/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.source.SessionWindows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.hitorro.jvssql.TestSupport.MAPPER;
import static com.hitorro.jvssql.TestSupport.run;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SESSION windows: verify boundary computation with a gap threshold,
 * per-key session isolation, and end-to-end aggregation over the emitted
 * session_start / session_end columns.
 */
class SessionWindowsTest {

    private static Type sessionedType() throws Exception {
        String typeJson = "{\"name\":\"events\",\"fields\":["
            + "{\"name\":\"user\",\"type\":\"core_string\"},"
            + "{\"name\":\"qty\",\"type\":\"core_long\"},"
            + "{\"name\":\"event_time\",\"type\":\"core_long\"},"
            + "{\"name\":\"session_start\",\"type\":\"core_long\"},"
            + "{\"name\":\"session_end\",\"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS row(long ts, String user, long qty) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"user\":\"" + user + "\",\"qty\":" + qty + ",\"event_time\":" + ts + "}"));
    }

    @Test
    void sessionize_computesSessionBoundariesWithGap() throws Exception {
        // gap = 30 seconds. Two users with distinct session patterns.
        // user A: t=0, t=10 (same session), t=100 (new session — gap>30)
        // user B: t=5 (own session), t=200 (own session)
        var events = List.of(
            row(0,       "A", 1),
            row(10_000,  "A", 2),
            row(100_000, "A", 3),
            row(5_000,   "B", 10),
            row(200_000, "B", 20)
        ).iterator();

        var sessioned = SessionWindows.sessionize(events, "user", "event_time", 30_000);
        var engine = JvsSqlEngine.builder()
            .registerStream("s", sessioned, sessionedType())
            .build();

        var rows = run(engine.compile(
            "SELECT \"user\", session_start, session_end, COUNT(*) AS n, SUM(qty) AS total "
          + "FROM s GROUP BY \"user\", session_start, session_end ORDER BY \"user\", session_start"));

        // Expected sessions:
        //   A [0, 10]     n=2, total=3
        //   A [100k,100k] n=1, total=3
        //   B [5k, 5k]    n=1, total=10
        //   B [200k,200k] n=1, total=20
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).get("user").asText()).isEqualTo("A");
        assertThat(rows.get(0).get("session_start").asLong()).isEqualTo(0);
        assertThat(rows.get(0).get("session_end").asLong()).isEqualTo(10_000L);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(2L);
        assertThat(rows.get(0).get("total").asLong()).isEqualTo(3L);
        assertThat(rows.get(1).get("session_start").asLong()).isEqualTo(100_000L);
        assertThat(rows.get(2).get("user").asText()).isEqualTo("B");
        assertThat(rows.get(2).get("session_start").asLong()).isEqualTo(5_000L);
        assertThat(rows.get(3).get("session_start").asLong()).isEqualTo(200_000L);
    }

    @Test
    void sessionize_outOfOrderInputSortsCorrectly() throws Exception {
        // Same session (A, t=0..25) but events arrive out-of-order.
        var events = List.of(
            row(25_000, "A", 3),
            row(0,      "A", 1),
            row(10_000, "A", 2)
        ).iterator();
        var sessioned = SessionWindows.sessionize(events, "user", "event_time", 30_000);
        var engine = JvsSqlEngine.builder()
            .registerStream("s", sessioned, sessionedType()).build();
        var rows = run(engine.compile(
            "SELECT session_start, session_end, COUNT(*) AS n FROM s "
          + "GROUP BY session_start, session_end"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("session_start").asLong()).isEqualTo(0);
        assertThat(rows.get(0).get("session_end").asLong()).isEqualTo(25_000L);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(3L);
    }

    @Test
    void sessionizeNested_producesNestedSessionObject() throws Exception {
        var events = List.of(
            row(0, "A", 1),
            row(5_000, "A", 2)
        ).iterator();
        var sessioned = SessionWindows.sessionizeNested(events, "user", "event_time", 30_000);
        // The nested "session" field isn't declared on our simpler type here,
        // so we access it via JPATH() to prove the augmented shape lands correctly.
        String typeJson = "{\"name\":\"events\",\"fields\":["
            + "{\"name\":\"user\",\"type\":\"core_string\"},"
            + "{\"name\":\"qty\",\"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));

        var engine = JvsSqlEngine.builder()
            .registerStream("s", sessioned, t).build();
        var rows = run(engine.compile(
            "SELECT \"user\", "
          + "  JPATH('session.start') AS win_start, "
          + "  JPATH('session.end')   AS win_end, "
          + "  COUNT(*) AS n "
          + "FROM s GROUP BY \"user\", JPATH('session.start'), JPATH('session.end')"));
        assertThat(rows).hasSize(1);
        JsonNode row = rows.get(0);
        assertThat(row.get("win_start").asLong()).isEqualTo(0L);
        assertThat(row.get("win_end").asLong()).isEqualTo(5000L);
    }
}
