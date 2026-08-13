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

    /**
     * Public UDAF class. Uses Calcite's init/add/result convention. Note:
     * accumulator must be a mutable holder so add() can update it in place;
     * the wrapper doesn't observe the return value of add().
     */
    public static class StringConcatFn {
        public Object init() { return new StringBuilder(); }
        public Object add(Object accBox, Object v) {
            if (v == null) return accBox;
            StringBuilder sb = (StringBuilder) accBox;
            if (sb.length() > 0) sb.append(",");
            sb.append(v);
            return sb;
        }
        public Object result(Object acc) { return acc.toString(); }
    }

    @Test
    void javaAggregateUdf_worksInGroupBy() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerAggregate("STR_CONCAT", StringConcatFn.class)
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"dept\":\"eng\"}"),
                jvs("{\"filename\":\"b.pdf\", \"dept\":\"eng\"}"),
                jvs("{\"filename\":\"c.pdf\", \"dept\":\"sales\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT dept, STR_CONCAT(filename) AS files FROM docs GROUP BY dept"));
        assertThat(rows).hasSize(2);
        var eng = rows.stream().filter(r -> "eng".equals(r.get("dept").asText())).findFirst().orElseThrow();
        var sales = rows.stream().filter(r -> "sales".equals(r.get("dept").asText())).findFirst().orElseThrow();
        assertThat(eng.get("files").asText()).isIn("a.pdf,b.pdf", "b.pdf,a.pdf");
        assertThat(sales.get("files").asText()).isEqualTo("c.pdf");
    }

    @Test
    void groovyScalarUdf_endToEnd() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerGroovyFunction("SHOUT", 1, "arg1.toString().toUpperCase() + '!'")
            .registerGroovyFunction("SUM_LEN", 2, "arg1.toString().length() + arg2.toString().length()")
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"dept\":\"eng\"}"),
                jvs("{\"filename\":\"b.pdf\", \"dept\":\"sales\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT SHOUT(filename) AS loud, SUM_LEN(filename, dept) AS total_len FROM docs"));
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("loud").asText()).isEqualTo("A.PDF!");
        // 'a.pdf' (5) + 'eng' (3) = 8
        assertThat(rows.get(0).get("total_len").asLong()).isEqualTo(8L);
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
