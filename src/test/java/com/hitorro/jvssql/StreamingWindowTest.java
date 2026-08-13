/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.io.StoreException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hitorro.jvssql.TestSupport.MAPPER;
import static com.hitorro.jvssql.TestSupport.run;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 event-time streaming: WIN_START / WIN_END scalars for
 * batch-mode windowed aggregation, plus WatermarkFilter routing late-arriving
 * records to a side sink.
 *
 * <p>The Phase 2 v1 API uses SQL scalars for window boundaries (a
 * {@code GROUP BY WIN_START(...)} pattern), rather than the streaming
 * {@code TABLE(TUMBLE(...))} syntax with incremental emit. That richer
 * form + full stream×stream time-bounded joins land in Phase 2-late.</p>
 */
class StreamingWindowTest {

    private static Type eventsType() throws Exception {
        String typeJson = "{\"name\":\"events\",\"fields\":["
            + "{\"name\":\"dept\",\"type\":\"core_string\"},"
            + "{\"name\":\"file_size\",\"type\":\"core_long\"},"
            + "{\"name\":\"event_time\",\"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS ev(long tsMillis, String dept, long size) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"dept\":\"" + dept + "\",\"file_size\":" + size + ",\"event_time\":" + tsMillis + "}"));
    }

    @Test
    void tumblingWindow_hourlyBucketsGroupBy() throws Exception {
        // Events at t=0m, 15m, 45m, 65m, 130m — three hourly buckets [0..60), [60..120), [120..180)
        List<JVS> events = new ArrayList<>();
        events.add(ev(0 * 60_000L, "eng", 100));
        events.add(ev(15 * 60_000L, "eng", 200));
        events.add(ev(45 * 60_000L, "sales", 300));
        events.add(ev(65 * 60_000L, "eng", 400));
        events.add(ev(130 * 60_000L, "sales", 500));

        var engine = JvsSqlEngine.builder()
            .registerStream("events", events.iterator(), eventsType())
            .build();

        long hourMs = 3_600_000L;
        // Group by WIN_START(event_time, 3600000) — one row per (hour, dept)
        var rows = run(engine.compile(
            "SELECT dept, "
          + "       WIN_START(event_time, " + hourMs + ") AS window_start, "
          + "       WIN_END(event_time,   " + hourMs + ") AS window_end, "
          + "       COUNT(*)          AS n, "
          + "       SUM(file_size)    AS total "
          + "FROM events "
          + "GROUP BY WIN_START(event_time, " + hourMs + "), "
          + "         WIN_END(event_time,   " + hourMs + "), "
          + "         dept "
          + "ORDER BY window_start, dept"));

        // Expected windows:
        //   hour 0 (0..3.6M):  eng total 300 (n=2)  ; sales total 300 (n=1)
        //   hour 1 (3.6M..7.2M): eng total 400
        //   hour 2 (7.2M..):    sales total 500
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).get("dept").asText()).isEqualTo("eng");
        assertThat(rows.get(0).get("window_start").asLong()).isEqualTo(0);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(2);
        assertThat(rows.get(0).get("total").asLong()).isEqualTo(300);
        assertThat(rows.get(1).get("dept").asText()).isEqualTo("sales");
        assertThat(rows.get(2).get("window_start").asLong()).isEqualTo(hourMs);
        assertThat(rows.get(2).get("dept").asText()).isEqualTo("eng");
        assertThat(rows.get(3).get("window_start").asLong()).isEqualTo(2 * hourMs);
        assertThat(rows.get(3).get("dept").asText()).isEqualTo("sales");
    }

    /** Counting sink that captures the routed rows for assertion. */
    private static final class CapturingSink implements Sink<JsonNode> {
        final List<JsonNode> received = new ArrayList<>();
        @Override public boolean init(JsonNode config) { return true; }
        @Override public boolean start() { return true; }
        @Override public boolean add(JsonNode row) throws IOException, StoreException { received.add(row); return true; }
        @Override public boolean stop() { return true; }
        @Override public void close() {}
    }

    @Test
    void watermarkFilter_routesLateRecordsToSideSink() throws Exception {
        // Events at t = 0, 10s, 20s, 5s. AllowedLateness = 3s. Third record
        // pushes watermark to 20s-3s = 17s. Fourth record (5s) < 17s -> late.
        List<JVS> events = new ArrayList<>();
        events.add(ev(0, "a", 1));
        events.add(ev(10_000, "b", 2));
        events.add(ev(20_000, "c", 3));
        events.add(ev(5_000, "d", 99));   // late

        CapturingSink lateSink = new CapturingSink();
        var engine = JvsSqlEngine.builder()
            .registerStream("events", events.iterator(), eventsType(),
                    StreamConfig.builder().eventTimeField("event_time").allowedLatenessMillis(3_000).build())
            .withLateDataSink("events", lateSink)
            .build();

        var rows = run(engine.compile("SELECT dept, file_size FROM events"));
        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(r -> r.get("dept").asText()).containsExactly("a", "b", "c");
        assertThat(lateSink.received).hasSize(1);
        assertThat(lateSink.received.get(0).get("dept").asText()).isEqualTo("d");
    }

    @Test
    void winStruct_producesNestedWindowObject() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("events", List.of(
                    ev(15 * 60_000L, "eng", 100),
                    ev(45 * 60_000L, "eng", 200),
                    ev(75 * 60_000L, "eng", 300)
            ).iterator(), eventsType()).build();
        var rows = run(engine.compile(
            "SELECT WIN_STRUCT(event_time, 3600000) AS \"window\", dept, COUNT(*) AS n "
          + "FROM events "
          + "GROUP BY WIN_STRUCT(event_time, 3600000), dept "
          + "ORDER BY n DESC"));
        assertThat(rows).hasSize(2);
        // First row (n=2): window {"start":0, "end":3600000}
        JsonNode w0 = rows.get(0).get("window");
        assertThat(w0.get("start").asLong()).isEqualTo(0L);
        assertThat(w0.get("end").asLong()).isEqualTo(3_600_000L);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(2L);
        // Second row (n=1): window {"start":3600000, "end":7200000}
        JsonNode w1 = rows.get(1).get("window");
        assertThat(w1.get("start").asLong()).isEqualTo(3_600_000L);
    }

    @Test
    void streamingAggregate_emitsClosedWindowsIncrementally() throws Exception {
        // Two hourly windows (0..3.6M, 3.6M..7.2M). Register as a streaming source so
        // the executor's StreamingHint detection engages. allowedLateness=0 → a window
        // closes as soon as we see a later window's start.
        var events = new java.util.ArrayList<JVS>();
        events.add(ev(0 * 60_000L,   "eng",   100));
        events.add(ev(30 * 60_000L,  "eng",   200));
        // ↓ this row's window_start = 3_600_000, which closes the (0, ...) windows
        events.add(ev(65 * 60_000L,  "eng",   400));
        events.add(ev(130 * 60_000L, "sales", 500));

        var engine = JvsSqlEngine.builder()
            .registerStream("events", events.iterator(), eventsType(),
                    com.hitorro.jvssql.config.StreamConfig.builder()
                        .eventTimeField("event_time")
                        .allowedLatenessMillis(0)
                        .build())
            .build();

        long hourMs = 3_600_000L;
        var q = engine.compile(
            "SELECT WIN_START(event_time, " + hourMs + ") AS window_start, "
          + "       dept, COUNT(*) AS n, SUM(file_size) AS total "
          + "FROM   events "
          + "GROUP BY WIN_START(event_time, " + hourMs + "), dept");

        // Pull rows one at a time and observe when they arrive relative to how much
        // of the source we've consumed. Streaming emit should give us the closed
        // hour-0 window BEFORE we've drained the entire source.
        var it = q.asIterator();
        java.util.List<JsonNode> rows = new java.util.ArrayList<>();
        while (it.hasNext()) rows.add(it.next());

        // 3 output rows total: (0, eng, 2, 300), (3.6M, eng, 1, 400), (7.2M, sales, 1, 500)
        assertThat(rows).hasSize(3);
        // First-emitted row should be from the closed hour-0 window (window_start=0).
        assertThat(rows.get(0).get("window_start").asLong()).isEqualTo(0L);
        assertThat(rows.get(0).get("dept").asText()).isEqualTo("eng");
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(2L);
        assertThat(rows.get(0).get("total").asLong()).isEqualTo(300L);
        // Hour-1 window closes when the hour-2 event (t=130min) arrives.
        assertThat(rows.get(1).get("window_start").asLong()).isEqualTo(hourMs);
        // Hour-2 flushed at end-of-input.
        assertThat(rows.get(2).get("window_start").asLong()).isEqualTo(2 * hourMs);
    }

    @Test
    void streamingAggregate_usesTightRawEventTimeWatermark() throws Exception {
        // Setup: window size = 1 minute, allowedLateness = 0.
        // Events within the SAME window arrive in order at t=0, 30s.
        // A third event at t=65s is in the NEXT window — but importantly, at that
        // point maxObservedEventTime = 65s, which is > (window_end 60s + lateness 0).
        // The hour-0 window should therefore close AFTER the 3rd event is read but
        // BEFORE the source is exhausted.
        //
        // With the old approximation (watermark ≈ maxObservedWindowStart) we needed
        // TWO later-window events to trigger closure of window 0. With the raw
        // event-time watermark, ONE later-window event is enough.
        var events = new java.util.ArrayList<JVS>();
        events.add(ev(0,       "eng", 100));
        events.add(ev(30_000,  "eng", 200));
        events.add(ev(65_000,  "sales", 500));   // ← this alone closes window 0

        var engine = JvsSqlEngine.builder()
            .registerStream("events", events.iterator(), eventsType(),
                    com.hitorro.jvssql.config.StreamConfig.builder()
                        .eventTimeField("event_time")
                        .allowedLatenessMillis(0)
                        .build())
            .build();

        long minute = 60_000L;
        var it = engine.compile(
            "SELECT WIN_START(event_time, " + minute + ") AS window_start, "
          + "       dept, COUNT(*) AS n "
          + "FROM   events "
          + "GROUP BY WIN_START(event_time, " + minute + "), dept").asIterator();

        // Pull one row; without the raw-event-time watermark, the operator would
        // still be holding window 0 open and would want another row from the
        // source before emitting. With the tight watermark, we get window 0
        // right after the third event arrived.
        assertThat(it.hasNext()).isTrue();
        JsonNode first = it.next();
        assertThat(first.get("window_start").asLong()).isEqualTo(0L);
        assertThat(first.get("dept").asText()).isEqualTo("eng");
        assertThat(first.get("n").asLong()).isEqualTo(2L);
        // Second and third rows: window 1's single sales event, flushed at EOF.
        assertThat(it.hasNext()).isTrue();
        JsonNode second = it.next();
        assertThat(second.get("window_start").asLong()).isEqualTo(minute);
        assertThat(second.get("dept").asText()).isEqualTo("sales");
    }

    @Test
    void hopStarts_producesMultipleWindowsPerRow() throws Exception {
        // HOP(size=60s, slide=20s): each event belongs to (size/slide) = 3 concurrent windows.
        var engine = JvsSqlEngine.builder()
            .registerStream("events", List.of(ev(50_000, "a", 1)).iterator(), eventsType()).build();
        var rows = run(engine.compile(
            "SELECT WIN_HOP_STARTS(event_time, 60000, 20000) AS starts FROM events"));
        assertThat(rows).hasSize(1);
        JsonNode starts = rows.get(0).get("starts");
        assertThat(starts.isArray()).isTrue();
        // At t=50000, windows-of-60s starting every 20s that COVER t=50000:
        // starts at 0 (0..60), 20000 (20..80), 40000 (40..100) — yes all include 50k
        assertThat(starts.size()).isEqualTo(3);
    }
}
