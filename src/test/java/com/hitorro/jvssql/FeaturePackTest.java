/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.hitorro.jvssql.TestSupport.docsType;
import static com.hitorro.jvssql.TestSupport.jvs;
import static com.hitorro.jvssql.TestSupport.run;
import static com.hitorro.jvssql.TestSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * COUNT(DISTINCT), set operations (UNION / INTERSECT / EXCEPT), EXPLAIN,
 * and prepared-statement parameters.
 */
class FeaturePackTest {

    @Test
    void countDistinct_deDupesWithinGroup() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\",   \"author\":\"chris\"}"),
                jvs("{\"dept\":\"eng\",   \"author\":\"chris\"}"),
                jvs("{\"dept\":\"eng\",   \"author\":\"alex\"}"),
                jvs("{\"dept\":\"sales\", \"author\":\"pat\"}"),
                jvs("{\"dept\":\"sales\", \"author\":\"pat\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT dept, COUNT(DISTINCT author) AS distinct_authors, COUNT(*) AS total "
          + "FROM docs GROUP BY dept ORDER BY dept"));
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("dept").asText()).isEqualTo("eng");
        assertThat(rows.get(0).get("distinct_authors").asLong()).isEqualTo(2L);
        assertThat(rows.get(0).get("total").asLong()).isEqualTo(3L);
        assertThat(rows.get(1).get("distinct_authors").asLong()).isEqualTo(1L);
    }

    @Test
    void unionAll_concatenatesInputs() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("a", stream(
                jvs("{\"filename\":\"x\", \"file_size\":1}"),
                jvs("{\"filename\":\"y\", \"file_size\":2}")
            ), docsType())
            .registerStream("b", stream(
                jvs("{\"filename\":\"y\", \"file_size\":2}"),
                jvs("{\"filename\":\"z\", \"file_size\":3}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT filename FROM a UNION ALL SELECT filename FROM b ORDER BY filename"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("x", "y", "y", "z");
    }

    @Test
    void unionDistinct_dedupsAcrossInputs() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("a", stream(
                jvs("{\"filename\":\"x\"}"), jvs("{\"filename\":\"y\"}")
            ), docsType())
            .registerStream("b", stream(
                jvs("{\"filename\":\"y\"}"), jvs("{\"filename\":\"z\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT filename FROM a UNION SELECT filename FROM b"));
        assertThat(rows).hasSize(3);
    }

    @Test
    void intersect_keepsRowsInBothInputs() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("a", stream(
                jvs("{\"filename\":\"x\"}"), jvs("{\"filename\":\"y\"}"), jvs("{\"filename\":\"z\"}")
            ), docsType())
            .registerStream("b", stream(
                jvs("{\"filename\":\"y\"}"), jvs("{\"filename\":\"z\"}"), jvs("{\"filename\":\"w\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT filename FROM a INTERSECT SELECT filename FROM b"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactlyInAnyOrder("y", "z");
    }

    @Test
    void except_dropsRowsAlsoInRight() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("a", stream(
                jvs("{\"filename\":\"x\"}"), jvs("{\"filename\":\"y\"}"), jvs("{\"filename\":\"z\"}")
            ), docsType())
            .registerStream("b", stream(
                jvs("{\"filename\":\"y\"}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT filename FROM a EXCEPT SELECT filename FROM b"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactlyInAnyOrder("x", "z");
    }

    @Test
    void explain_returnsPlanText() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(jvs("{\"filename\":\"a\"}")), docsType()).build();
        var q = engine.compile("SELECT filename FROM docs WHERE file_size > 100");
        String plan = q.explain();
        assertThat(plan).contains("LogicalProject").contains("LogicalFilter").contains("docs");
    }

    @Test
    void preparedParameters_bindAndRun() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"file_size\":100}"),
                jvs("{\"filename\":\"b\", \"file_size\":500}"),
                jvs("{\"filename\":\"c\", \"file_size\":2000}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename FROM docs WHERE file_size > ?");
        q.bind(1, 400);
        List<JsonNode> rows = run(q);
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("b", "c");
    }
}
