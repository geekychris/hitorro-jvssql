/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.config.StreamConfig;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;

/**
 * Real incremental streaming aggregate: closed windows are emitted as soon as
 * the watermark advances past their end, without waiting for the source to
 * fully drain. Consumers pulling from {@code asIterator()} see hourly window
 * results appear as later hours' events start arriving.
 *
 * <p>Detection is automatic — when a query's GROUP BY column is derived from
 * {@code WIN_START(event_time, size)} and the source stream declares an event
 * time via {@link StreamConfig#eventTimeField}, the executor swaps in the
 * {@link com.hitorro.jvssql.exec.StreamingAggregate} operator. State is bounded
 * to currently-open windows only.</p>
 *
 * <p>This example uses a slow-drip source that produces one event every
 * 250ms. Watch the output: hour-0 rows print BEFORE the source is done —
 * they land as soon as an hour-1 event arrives and closes the hour-0 window.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example16_StreamingAggregate"}</p>
 */
public final class Example16_StreamingAggregate {

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

    private static JVS ev(long ts, String dept, long size) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"dept\":\"" + dept + "\",\"file_size\":" + size + ",\"event_time\":" + ts + "}"));
    }

    /** A source that slowly emits one event every 250ms so we can visually observe streaming emit. */
    private static Iterator<JVS> slowSource(long delayMs, JVS... events) {
        return new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < events.length; }
            @Override public JVS next() {
                if (!hasNext()) throw new NoSuchElementException();
                if (i > 0) {
                    try { Thread.sleep(delayMs); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
                }
                JVS row = events[i++];
                System.out.println("  [source at t=" + (System.currentTimeMillis() % 100000) + "ms] emitting event_time="
                        + row.getJsonNode().get("event_time").asLong());
                return row;
            }
        };
    }

    public static void main(String[] args) throws Exception {
        long hourMs = 3_600_000L;
        JVS[] events = new JVS[] {
            ev(0 * 60_000L,   "eng",   100),
            ev(30 * 60_000L,  "eng",   200),
            ev(65 * 60_000L,  "eng",   400),   // ← this row's window_start=3.6M closes hour-0 windows
            ev(90 * 60_000L,  "sales", 50),
            ev(130 * 60_000L, "sales", 500),   // ← window_start=7.2M closes hour-1 windows
        };

        var engine = JvsSqlEngine.builder()
            .registerStream("events", slowSource(250, events), eventsType(),
                    StreamConfig.builder()
                        .eventTimeField("event_time")
                        .allowedLatenessMillis(0)
                        .build())
            .build();

        String sql =
              "SELECT WIN_START(event_time, " + hourMs + ") AS window_start, "
            + "       dept, COUNT(*) AS n, SUM(file_size) AS total "
            + "FROM   events "
            + "GROUP BY WIN_START(event_time, " + hourMs + "), dept";

        System.out.println("SQL: " + sql.replaceAll("\\s+", " ").trim());
        System.out.println();
        System.out.println("Pulling results (with timestamps so streaming emit is visible):");
        var it = engine.compile(sql).asIterator();
        while (it.hasNext()) {
            JsonNode row = it.next();
            long tNow = System.currentTimeMillis() % 100000;
            System.out.println("  [result at t=" + tNow + "ms] " + row);
        }
    }
}
