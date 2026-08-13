/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jvssql.JvsSqlEngine;

import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;

/**
 * User-defined aggregate (UDAF) — register a Java class with
 * {@code init/add/result} methods and call it from GROUP BY like any built-in.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example10_UserAggregate"}</p>
 */
public final class Example10_UserAggregate {

    /**
     * A UDAF that returns the joined string of values in the group. Uses a
     * mutable StringBuilder as the accumulator — the engine keeps a reference
     * to it across all rows in the group.
     */
    public static class Concat {
        public Object init() { return new StringBuilder(); }
        public Object add(Object acc, Object v) {
            if (v == null) return acc;
            StringBuilder sb = (StringBuilder) acc;
            if (sb.length() > 0) sb.append(", ");
            sb.append(v);
            return sb;
        }
        public Object result(Object acc) { return acc.toString(); }
    }

    /** A UDAF that returns the harmonic mean of a group's numeric values. */
    public static class HarmonicMean {
        public static class Acc {
            double sumReciprocals;
            long n;
        }
        public Object init() { return new Acc(); }
        public Object add(Object accBox, Object v) {
            Acc acc = (Acc) accBox;
            if (v == null) return acc;
            double d = v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
            if (d == 0) return acc;
            acc.sumReciprocals += 1.0 / d;
            acc.n++;
            return acc;
        }
        public Object result(Object accBox) {
            Acc acc = (Acc) accBox;
            return acc.n == 0 ? null : acc.n / acc.sumReciprocals;
        }
    }

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerAggregate("CONCAT_FILES", Concat.class)
            .registerAggregate("HARMONIC_MEAN", HarmonicMean.class)
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"dept\":\"eng\",   \"file_size\":100}"),
                jvs("{\"filename\":\"b.pdf\", \"dept\":\"eng\",   \"file_size\":200}"),
                jvs("{\"filename\":\"c.pdf\", \"dept\":\"eng\",   \"file_size\":800}"),
                jvs("{\"filename\":\"d.pdf\", \"dept\":\"sales\", \"file_size\":50}"),
                jvs("{\"filename\":\"e.pdf\", \"dept\":\"sales\", \"file_size\":500}")
            ), docsType()).build();

        printQuery("CONCAT_FILES + HARMONIC_MEAN alongside SUM/AVG built-ins",
            engine.compile(
                "SELECT dept, "
              + "       CONCAT_FILES(filename) AS files, "
              + "       SUM(file_size) AS total, "
              + "       AVG(file_size) AS mean, "
              + "       HARMONIC_MEAN(file_size) AS harmonic "
              + "FROM docs GROUP BY dept"));
    }
}
