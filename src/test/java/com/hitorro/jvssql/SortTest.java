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
 * Tests for ORDER BY (in-memory Phase 1 impl). External-merge with spill
 * lands in a follow-up task on Phase 1.
 */
class SortTest {

    @Test
    void orderBy_singleColumn_asc() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"c\", \"file_size\":300}"),
                jvs("{\"filename\":\"a\", \"file_size\":100}"),
                jvs("{\"filename\":\"b\", \"file_size\":200}")
            ), docsType()).build();
        var rows = run(engine.compile("SELECT filename FROM docs ORDER BY file_size"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("a", "b", "c");
    }

    @Test
    void orderBy_singleColumn_desc() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"c\", \"file_size\":300}"),
                jvs("{\"filename\":\"a\", \"file_size\":100}"),
                jvs("{\"filename\":\"b\", \"file_size\":200}")
            ), docsType()).build();
        var rows = run(engine.compile("SELECT filename FROM docs ORDER BY file_size DESC"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("c", "b", "a");
    }

    @Test
    void orderBy_twoColumns() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\", \"filename\":\"b\"}"),
                jvs("{\"dept\":\"sales\", \"filename\":\"a\"}"),
                jvs("{\"dept\":\"eng\", \"filename\":\"a\"}")
            ), docsType()).build();
        var rows = run(engine.compile("SELECT dept, filename FROM docs ORDER BY dept ASC, filename ASC"));
        assertThat(rows).extracting(r -> r.get("dept").asText() + ":" + r.get("filename").asText())
                        .containsExactly("eng:a", "eng:b", "sales:a");
    }

    @Test
    void orderBy_limitOffset() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"file_size\":1}"),
                jvs("{\"filename\":\"b\", \"file_size\":2}"),
                jvs("{\"filename\":\"c\", \"file_size\":3}"),
                jvs("{\"filename\":\"d\", \"file_size\":4}"),
                jvs("{\"filename\":\"e\", \"file_size\":5}")
            ), docsType()).build();
        var rows = run(engine.compile("SELECT filename FROM docs ORDER BY file_size DESC LIMIT 2 OFFSET 1"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("d", "c");
    }

    @Test
    void orderBy_afterGroupBy() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"a\", \"file_size\":100}"),
                jvs("{\"dept\":\"b\", \"file_size\":50}"),
                jvs("{\"dept\":\"c\", \"file_size\":300}")
            ), docsType()).build();
        var rows = run(engine.compile(
            "SELECT dept, SUM(file_size) AS total FROM docs GROUP BY dept ORDER BY total DESC"));
        assertThat(rows).extracting(r -> r.get("dept").asText())
                        .containsExactly("c", "a", "b");
    }
}
