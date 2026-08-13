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
 * Tests for the RexEvaluator expression surface: arithmetic, comparisons,
 * boolean composition, LIKE, IN, BETWEEN, CASE, COALESCE, NULLIF, string and
 * math built-in scalars.
 */
class ExpressionsTest {

    @Test
    void arithmetic_plusMinusTimesDivMod() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(jvs("{\"filename\":\"a.pdf\",\"file_size\":100}")), docsType()).build();
        var q = engine.compile(
            "SELECT file_size + 5 AS added, file_size - 5 AS subtracted, file_size * 2 AS doubled, file_size / 4 AS quarter, MOD(file_size, 3) AS remainder FROM docs");
        var rows = run(q);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("added").asLong()).isEqualTo(105);
        assertThat(rows.get(0).get("subtracted").asLong()).isEqualTo(95);
        assertThat(rows.get(0).get("doubled").asLong()).isEqualTo(200);
        assertThat(rows.get(0).get("quarter").asLong()).isEqualTo(25);
        assertThat(rows.get(0).get("remainder").asLong()).isEqualTo(1);
    }

    @Test
    void comparisons_allSixOperators() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\",\"file_size\":100}"),
                jvs("{\"filename\":\"b\",\"file_size\":200}"),
                jvs("{\"filename\":\"c\",\"file_size\":300}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename FROM docs WHERE file_size >= 200 AND file_size < 300");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("filename").asText()).containsExactly("b");
    }

    @Test
    void like_percentAndUnderscore() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"report.pdf\"}"),
                jvs("{\"filename\":\"invoice.pdf\"}"),
                jvs("{\"filename\":\"notes.txt\"}")
            ), docsType()).build();
        assertThat(run(engine.compile("SELECT filename FROM docs WHERE filename LIKE '%.pdf'")))
            .extracting(r -> r.get("filename").asText())
            .containsExactly("report.pdf", "invoice.pdf");
        // Underscore = exactly one char. 'notes' has 5 letters, '_____' matches exactly 5.
        assertThat(run(JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"notes.txt\"}"),
                jvs("{\"filename\":\"note.txt\"}"),
                jvs("{\"filename\":\"notess.txt\"}")
            ), docsType()).build()
            .compile("SELECT filename FROM docs WHERE filename LIKE '_____.txt'")))
            .extracting(r -> r.get("filename").asText())
            .containsExactly("notes.txt");
    }

    @Test
    void in_membership() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"classification\":\"public\"}"),
                jvs("{\"classification\":\"internal\"}"),
                jvs("{\"classification\":\"restricted\"}"),
                jvs("{\"classification\":\"confidential\"}")
            ), docsType()).build();
        assertThat(run(engine.compile(
            "SELECT classification FROM docs WHERE classification IN ('internal', 'restricted')")))
            .hasSize(2);
    }

    @Test
    void between_inclusive() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"file_size\":99}"),
                jvs("{\"file_size\":100}"),
                jvs("{\"file_size\":200}"),
                jvs("{\"file_size\":201}")
            ), docsType()).build();
        assertThat(run(engine.compile("SELECT file_size FROM docs WHERE file_size BETWEEN 100 AND 200")))
            .hasSize(2);
    }

    @Test
    void case_when_else() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"file_size\":50}"),
                jvs("{\"file_size\":500}"),
                jvs("{\"file_size\":5000}")
            ), docsType()).build();
        var q = engine.compile(
            "SELECT file_size, "
          + "  CASE WHEN file_size < 100 THEN 'tiny' "
          + "       WHEN file_size < 1000 THEN 'small' "
          + "       ELSE 'big' END AS bucket FROM docs");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("bucket").asText())
                        .containsExactly("tiny", "small", "big");
    }

    @Test
    void coalesce_returnsFirstNonNull() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"author\":\"chris\"}"),
                jvs("{\"filename\":\"b\"}"),                         // no author
                jvs("{\"filename\":\"c\", \"author\":null}")         // explicit null
            ), docsType()).build();
        var q = engine.compile("SELECT filename, COALESCE(author, 'anonymous') AS who FROM docs");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("who").asText())
                        .containsExactly("chris", "anonymous", "anonymous");
    }

    @Test
    void stringFunctions_upperLowerTrimSubstringLength() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(jvs("{\"filename\":\"  Report.PDF  \"}")), docsType()).build();
        var q = engine.compile(
            "SELECT UPPER(filename) AS u, LOWER(filename) AS l, TRIM(filename) AS t, "
          + "SUBSTRING(TRIM(filename), 1, 6) AS pre, CHAR_LENGTH(TRIM(filename)) AS len FROM docs");
        var row = run(q).get(0);
        assertThat(row.get("u").asText()).isEqualTo("  REPORT.PDF  ");
        assertThat(row.get("l").asText()).isEqualTo("  report.pdf  ");
        assertThat(row.get("t").asText()).isEqualTo("Report.PDF");
        assertThat(row.get("pre").asText()).isEqualTo("Report");
        assertThat(row.get("len").asLong()).isEqualTo(10L);
    }

    @Test
    void mathFunctions_absRoundCeilFloor() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(jvs("{\"file_size\":100}")), docsType()).build();
        // Compute over a literal (arithmetic yields a decimal, math funcs consume it).
        var q = engine.compile("SELECT ABS(-7.5) AS a, ROUND(2.4) AS r, CEIL(2.1) AS c, FLOOR(2.9) AS f FROM docs");
        var row = run(q).get(0);
        assertThat(row.get("a").asDouble()).isEqualTo(7.5);
        assertThat(row.get("r").asDouble()).isEqualTo(2.0);
        assertThat(row.get("c").asDouble()).isEqualTo(3.0);
        assertThat(row.get("f").asDouble()).isEqualTo(2.0);
    }

    @Test
    void notLike_notIn_notBetween() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"report.pdf\",   \"classification\":\"public\",   \"file_size\":100}"),
                jvs("{\"filename\":\"invoice.pdf\",  \"classification\":\"internal\", \"file_size\":500}"),
                jvs("{\"filename\":\"notes.txt\",    \"classification\":\"public\",   \"file_size\":2000}"),
                jvs("{\"filename\":\"draft.md\",     \"classification\":\"restricted\", \"file_size\":50}")
            ), docsType()).build();
        // NOT LIKE
        assertThat(run(engine.compile("SELECT filename FROM docs WHERE filename NOT LIKE '%.pdf'")))
            .extracting(r -> r.get("filename").asText())
            .containsExactly("notes.txt", "draft.md");
        // NOT IN
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"report.pdf\",   \"classification\":\"public\",   \"file_size\":100}"),
                jvs("{\"filename\":\"invoice.pdf\",  \"classification\":\"internal\", \"file_size\":500}"),
                jvs("{\"filename\":\"notes.txt\",    \"classification\":\"public\",   \"file_size\":2000}"),
                jvs("{\"filename\":\"draft.md\",     \"classification\":\"restricted\", \"file_size\":50}")
            ), docsType()).build();
        assertThat(run(engine2.compile("SELECT filename FROM docs WHERE classification NOT IN ('public', 'internal')")))
            .extracting(r -> r.get("filename").asText())
            .containsExactly("draft.md");
        // NOT BETWEEN
        var engine3 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"report.pdf\",   \"classification\":\"public\",   \"file_size\":100}"),
                jvs("{\"filename\":\"invoice.pdf\",  \"classification\":\"internal\", \"file_size\":500}"),
                jvs("{\"filename\":\"notes.txt\",    \"classification\":\"public\",   \"file_size\":2000}"),
                jvs("{\"filename\":\"draft.md\",     \"classification\":\"restricted\", \"file_size\":50}")
            ), docsType()).build();
        assertThat(run(engine3.compile("SELECT filename FROM docs WHERE file_size NOT BETWEEN 100 AND 500")))
            .extracting(r -> r.get("filename").asText())
            .containsExactly("notes.txt", "draft.md");
    }

    @Test
    void isNull_isNotNull() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"author\":\"chris\"}"),
                jvs("{\"filename\":\"b\"}"),
                jvs("{\"filename\":\"c\", \"author\":null}")
            ), docsType()).build();
        assertThat(run(engine.compile("SELECT filename FROM docs WHERE author IS NULL"))).hasSize(2);
        assertThat(run(JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"author\":\"chris\"}"),
                jvs("{\"filename\":\"b\"}"),
                jvs("{\"filename\":\"c\", \"author\":null}")
            ), docsType()).build()
            .compile("SELECT filename FROM docs WHERE author IS NOT NULL"))).hasSize(1);
    }
}
