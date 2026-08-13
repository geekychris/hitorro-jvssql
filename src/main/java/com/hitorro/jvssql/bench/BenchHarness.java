/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.PreparedQuery;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Simple throughput benchmark harness for hitorro-jvssql.
 *
 * <p>Generates synthetic input (N rows), runs each of several representative
 * queries against it, and reports rows/sec throughput. Not a formal
 * micro-benchmark — no JMH warmup or forking — but useful for spotting
 * regressions and understanding rough performance shape.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.bench.BenchHarness"}</p>
 *
 * <p>Override the row count with a system property:
 * {@code -Dbench.rows=100000}</p>
 */
public final class BenchHarness {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int N = Integer.getInteger("bench.rows", 100_000);

        System.out.println("hitorro-jvssql benchmarks — " + N + " rows/query, single-threaded");
        System.out.println("java.version = " + System.getProperty("java.version"));
        System.out.println("os.arch      = " + System.getProperty("os.arch"));
        System.out.println("processors   = " + Runtime.getRuntime().availableProcessors());
        System.out.println();

        // Warmup — JIT compile the hot paths on a small run.
        System.out.println("Warmup...");
        for (int i = 0; i < 3; i++) runOne("warmup", () -> synthEvents(1_000),
                "SELECT dept, COUNT(*) FROM events GROUP BY dept", false);
        System.out.println();

        // Actual benchmarks.
        run("filter",              N, "SELECT dept FROM events WHERE file_size > 500");
        run("filter+project",      N, "SELECT dept, UPPER(dept) AS d, file_size / 1024 AS kb "
                                   + "FROM events WHERE file_size BETWEEN 100 AND 10000");
        run("group-by",            N, "SELECT dept, COUNT(*) AS n, SUM(file_size) AS total, "
                                   + "AVG(file_size) AS mean FROM events GROUP BY dept");
        run("group-by (10 keys)",  N, "SELECT dept, filename_prefix, COUNT(*) AS n, "
                                   + "SUM(file_size) AS total FROM events "
                                   + "GROUP BY dept, filename_prefix");
        run("order by",            N, "SELECT dept, file_size FROM events ORDER BY file_size DESC LIMIT 100");
        run("distinct",            N, "SELECT COUNT(DISTINCT dept) AS n FROM events");
        run("windowed agg",        N, "SELECT WIN_START(event_time, 3600000) AS w, dept, COUNT(*) AS n "
                                   + "FROM events GROUP BY WIN_START(event_time, 3600000), dept");
    }

    private static void run(String label, int N, String sql) throws Exception {
        runOne(label, () -> synthEvents(N), sql, true);
    }

    private static void runOne(String label, Supplier<Iterator<JVS>> src, String sql, boolean print) throws Exception {
        Type type = eventsType();
        var engine = JvsSqlEngine.builder()
            .registerStream("events", src.get(), type).build();
        PreparedQuery q = engine.compile(sql);

        long t0 = System.nanoTime();
        long rowsIn = countRows(src.get());   // regenerate for the counter
        var it = q.asIterator();
        long resultRows = 0;
        while (it.hasNext()) { it.next(); resultRows++; }
        long dt = System.nanoTime() - t0;

        if (print) {
            double secs = dt / 1e9;
            long rate = (long) (rowsIn / secs);
            System.out.printf("  %-22s  in=%d rows  out=%d rows  time=%.3fs  rate=%,d rows/s%n",
                    label, rowsIn, resultRows, secs, rate);
        }
    }

    private static long countRows(Iterator<JVS> it) {
        long n = 0;
        while (it.hasNext()) { it.next(); n++; }
        return n;
    }

    // -- data -----------------------------------------------------------------

    private static Type eventsType() throws Exception {
        String typeJson = "{\"name\":\"events\",\"fields\":["
            + "{\"name\":\"dept\",       \"type\":\"core_string\"},"
            + "{\"name\":\"filename_prefix\",\"type\":\"core_string\"},"
            + "{\"name\":\"file_size\",  \"type\":\"core_long\"},"
            + "{\"name\":\"event_time\", \"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static Iterator<JVS> synthEvents(int N) {
        String[] depts = {"eng", "sales", "finance", "marketing", "ops"};
        String[] prefixes = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        return new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < N; }
            @Override public JVS next() {
                if (i >= N) throw new NoSuchElementException();
                int idx = i++;
                String dept = depts[idx % depts.length];
                String prefix = prefixes[idx % prefixes.length];
                long fileSize = 100L + (idx * 37L) % 1_000_000L;
                long eventTime = idx * 1000L;    // one event per second → 24 buckets in a day
                try {
                    return new JVS(MAPPER.readTree(
                        "{\"dept\":\"" + dept + "\","
                      + "\"filename_prefix\":\"" + prefix + "\","
                      + "\"file_size\":" + fileSize + ","
                      + "\"event_time\":" + eventTime + "}"));
                } catch (Exception e) { throw new RuntimeException(e); }
            }
        };
    }
}
