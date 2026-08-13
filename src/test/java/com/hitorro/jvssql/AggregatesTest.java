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
 * Tests for GROUP BY + built-in aggregates + HAVING.
 * <p>COUNT(*) / COUNT(expr) / SUM / AVG / MIN / MAX all against a small
 * three-department dataset so the arithmetic is easy to eyeball.</p>
 */
class AggregatesTest {

    @Test
    void countStar_noGroupBy() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\"}"),
                jvs("{\"filename\":\"b\"}"),
                jvs("{\"filename\":\"c\"}")
            ), docsType()).build();
        var rows = run(engine.compile("SELECT COUNT(*) AS n FROM docs"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(3L);
    }

    @Test
    void sumMinMaxAvg_groupBy() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\", \"file_size\":100}"),
                jvs("{\"dept\":\"eng\", \"file_size\":200}"),
                jvs("{\"dept\":\"eng\", \"file_size\":300}"),
                jvs("{\"dept\":\"sales\", \"file_size\":1000}"),
                jvs("{\"dept\":\"sales\", \"file_size\":2000}")
            ), docsType()).build();
        var q = engine.compile(
            "SELECT dept, COUNT(*) AS n, SUM(file_size) AS total, MIN(file_size) AS lo, "
          + "MAX(file_size) AS hi, AVG(file_size) AS mean "
          + "FROM docs GROUP BY dept");
        var rows = run(q);
        assertThat(rows).hasSize(2);
        // Order depends on group iteration; find each dept
        var eng = rows.stream().filter(r -> "eng".equals(r.get("dept").asText())).findFirst().orElseThrow();
        var sales = rows.stream().filter(r -> "sales".equals(r.get("dept").asText())).findFirst().orElseThrow();
        assertThat(eng.get("n").asLong()).isEqualTo(3L);
        assertThat(eng.get("total").asLong()).isEqualTo(600L);
        assertThat(eng.get("lo").asLong()).isEqualTo(100L);
        assertThat(eng.get("hi").asLong()).isEqualTo(300L);
        assertThat(eng.get("mean").asDouble()).isEqualTo(200.0);
        assertThat(sales.get("n").asLong()).isEqualTo(2L);
        assertThat(sales.get("total").asLong()).isEqualTo(3000L);
    }

    @Test
    void groupBy_multipleColumns() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\", \"classification\":\"public\",   \"file_size\":100}"),
                jvs("{\"dept\":\"eng\", \"classification\":\"internal\", \"file_size\":200}"),
                jvs("{\"dept\":\"eng\", \"classification\":\"internal\", \"file_size\":300}"),
                jvs("{\"dept\":\"sales\", \"classification\":\"public\", \"file_size\":50}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT dept, classification, COUNT(*) AS n FROM docs GROUP BY dept, classification"));
        assertThat(rows).hasSize(3);   // (eng,public), (eng,internal), (sales,public)
    }

    @Test
    void having_filtersGroups() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\", \"file_size\":100}"),
                jvs("{\"dept\":\"eng\", \"file_size\":200}"),
                jvs("{\"dept\":\"sales\", \"file_size\":1000}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT dept, SUM(file_size) AS total FROM docs GROUP BY dept HAVING SUM(file_size) > 500"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("dept").asText()).isEqualTo("sales");
    }

    @Test
    void countStar_emptyInput_yieldsZeroForNoGroupCase() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(), docsType()).build();
        var rows = run(engine.compile("SELECT COUNT(*) AS n FROM docs"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("n").asLong()).isEqualTo(0L);
    }
}
