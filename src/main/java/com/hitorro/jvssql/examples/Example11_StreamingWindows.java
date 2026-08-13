/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.config.StreamConfig;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.io.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;

/**
 * Phase 2 event-time windowing: aggregate a stream by time buckets using
 * the {@code WIN_START} / {@code WIN_END} scalar functions on a
 * {@code GROUP BY}, plus late-data routing through the WatermarkFilter.
 *
 * <p>Phase 2 v1 uses scalar functions for bucketing rather than the
 * {@code TABLE(TUMBLE(...))} streaming SQL syntax with incremental emit.
 * That richer form + full stream × stream time-bounded joins land in
 * Phase 2-late.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example11_StreamingWindows"}</p>
 */
public final class Example11_StreamingWindows {

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

    /** Sink that prints any late-arrival rows the engine routes to it. */
    static final class LatePrintSink implements Sink<JsonNode> {
        @Override public boolean init(JsonNode c) { return true; }
        @Override public boolean start() { return true; }
        @Override public boolean add(JsonNode row) throws IOException, StoreException {
            System.out.println("(late-data sink) dropped: " + row);
            return true;
        }
        @Override public boolean stop() { return true; }
        @Override public void close() {}
    }

    public static void main(String[] args) throws Exception {
        long hourMs = 3_600_000L;

        // ── Example A: batch-mode tumbling window aggregation ────────────
        List<JVS> events = new ArrayList<>();
        events.add(ev(0L,                "eng",   100));
        events.add(ev(15 * 60_000L,      "eng",   200));
        events.add(ev(45 * 60_000L,      "sales", 300));
        events.add(ev(65 * 60_000L,      "eng",   400));
        events.add(ev(130 * 60_000L,     "sales", 500));
        var engine = JvsSqlEngine.builder()
            .registerStream("events", events.iterator(), eventsType())
            .build();
        printQuery("hourly tumbling window per (window, dept)",
            engine.compile(
                "SELECT dept, "
              + "       WIN_START(event_time, " + hourMs + ") AS window_start, "
              + "       WIN_END(event_time,   " + hourMs + ") AS window_end, "
              + "       COUNT(*)       AS n, "
              + "       SUM(file_size) AS total "
              + "FROM   events "
              + "GROUP BY WIN_START(event_time, " + hourMs + "), "
              + "         WIN_END(event_time,   " + hourMs + "), dept "
              + "ORDER BY window_start, dept"));

        // ── Example B: WatermarkFilter drops late records into a side sink ────────────
        List<JVS> events2 = new ArrayList<>();
        events2.add(ev(0L,        "a", 1));
        events2.add(ev(10_000L,   "b", 2));
        events2.add(ev(20_000L,   "c", 3));
        events2.add(ev(5_000L,    "d", 99));   // out of order — becomes late
        var engine2 = JvsSqlEngine.builder()
            .registerStream("events", events2.iterator(), eventsType(),
                    StreamConfig.builder()
                        .eventTimeField("event_time")
                        .allowedLatenessMillis(3_000)
                        .build())
            .withLateDataSink("events", new LatePrintSink())
            .build();
        printQuery("watermark filter — 'd' is late (event_time=5s but watermark ≥ 17s)",
            engine2.compile("SELECT dept, event_time FROM events ORDER BY event_time"));
    }
}
