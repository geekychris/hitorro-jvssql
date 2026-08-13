/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import org.junit.jupiter.api.Test;

import static com.hitorro.jvssql.TestSupport.docsType;
import static com.hitorro.jvssql.TestSupport.jvs;
import static com.hitorro.jvssql.TestSupport.run;
import static com.hitorro.jvssql.TestSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * User-defined function registration — Java class-based scalar / aggregate,
 * plus Groovy scalars/aggregates (when Groovy is on the classpath).
 */
class UdfTest {

    /** Public class with a public {@code eval} method — the Java UDF shape. */
    public static class HashFn {
        public long eval(String s) {
            if (s == null) return 0L;
            long h = 1125899906842597L;
            for (int i = 0; i < s.length(); i++) h = 31 * h + s.charAt(i);
            return Math.abs(h);
        }
    }

    /** Public UDAF class following the shape JavaAggregateFn expects. */
    public static class ConcatFn {
        public Object createAccumulator() { return new StringBuilder(); }
        public void accumulate(Object acc, Object v) {
            if (v == null) return;
            StringBuilder sb = (StringBuilder) acc;
            if (sb.length() > 0) sb.append(",");
            sb.append(v);
        }
        public Object result(Object acc) { return acc.toString(); }
    }

    @Test
    void javaScalarUdf_registeredAndCallable() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerFunction("HASH64", HashFn.class)
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\"}"),
                jvs("{\"filename\":\"b.pdf\"}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename, HASH64(filename) AS h FROM docs");
        var rows = run(q);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("h").asLong()).isGreaterThan(0);
        assertThat(rows.get(0).get("h").asLong()).isNotEqualTo(rows.get(1).get("h").asLong());
    }
}
