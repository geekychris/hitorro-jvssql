/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.source.SessionWindows;

import java.util.List;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;

/**
 * SESSION windows: gap-based grouping via
 * {@link SessionWindows#sessionize}. Each session is a contiguous run of
 * events for the same key where consecutive event times are within
 * {@code gap} ms of each other; a longer gap starts a new session.
 *
 * <p>The helper preprocesses the input stream by adding
 * {@code session_start} and {@code session_end} columns; users then
 * aggregate over those with a regular SQL {@code GROUP BY}.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example14_SessionWindows"}</p>
 */
public final class Example14_SessionWindows {

    /** Type declaring the augmented columns SessionWindows.sessionize adds. */
    private static Type sessionedType() throws Exception {
        String typeJson = "{\"name\":\"clicks\",\"fields\":["
            + "{\"name\":\"user\",     \"type\":\"core_string\"},"
            + "{\"name\":\"page\",     \"type\":\"core_string\"},"
            + "{\"name\":\"event_time\",\"type\":\"core_long\"},"
            + "{\"name\":\"session_start\",\"type\":\"core_long\"},"
            + "{\"name\":\"session_end\",  \"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS click(long ts, String user, String page) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"user\":\"" + user + "\",\"page\":\"" + page + "\",\"event_time\":" + ts + "}"));
    }

    public static void main(String[] args) throws Exception {
        // Website clickstream: two users, several sessions each. gap = 5 minutes.
        // Alice's clicks:
        //   t=0     /home    ─┐
        //   t=30s   /product  │ session 1
        //   t=90s   /checkout─┘
        //   t=600s  /home     ─ session 2 (600s > 90s + gap of 300s)
        //   t=650s  /product  ─ (same session)
        //
        // Bob's clicks:
        //   t=100   /home
        //   t=1000  /checkout — different session (gap>300s)
        long s = 1000L;   // millis per second
        var events = List.of(
            click(0*s,     "alice", "/home"),
            click(30*s,    "alice", "/product"),
            click(90*s,    "alice", "/checkout"),
            click(600*s,   "alice", "/home"),
            click(650*s,   "alice", "/product"),
            click(100*s,   "bob",   "/home"),
            click(1000*s,  "bob",   "/checkout")
        ).iterator();

        long gap = 5 * 60 * s;   // 5-minute inactivity gap
        var sessioned = SessionWindows.sessionize(events, "user", "event_time", gap);

        var engine = JvsSqlEngine.builder()
            .registerStream("clicks", sessioned, sessionedType())
            .build();

        printQuery("Sessions per user with click counts + duration",
            engine.compile(
                "SELECT \"user\", "
              + "       session_start, "
              + "       session_end, "
              + "       (session_end - session_start) AS duration_ms, "
              + "       COUNT(*) AS clicks "
              + "FROM   clicks "
              + "GROUP BY \"user\", session_start, session_end "
              + "ORDER BY \"user\", session_start"));
    }
}
