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
 * Verifies transparent dotted-path support — writing {@code metadata.experimental} in SQL
 * should behave the same as writing {@code JPATH('metadata.experimental')} because
 * {@link JpathRewriter} rewrites the identifier before validation.
 *
 * <p>Also pins down two edge cases we care about:</p>
 * <ul>
 *   <li>A qualified reference to a top-level column ({@code docs.filename})
 *       still resolves through Calcite natively — the rewriter must not touch it.</li>
 *   <li>Reading via a dotted path must NOT mutate the source JVS document —
 *       Propaccess is called with {@code NeverCreate}. Regression guard for
 *       the earlier {@code AlwaysCreate} bug.</li>
 * </ul>
 */
class TransparentPathTest {

    @Test
    void dottedPathInSelect_worksWithoutJpath() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"metadata\":{\"experimental\":\"yes\"}}"),
                jvs("{\"filename\":\"b.pdf\", \"metadata\":{\"experimental\":\"no\"}}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename, metadata.experimental AS x FROM docs");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("x").asText())
                        .containsExactly("yes", "no");
    }

    @Test
    void dottedPathInWhere_worksWithoutJpath() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"metadata\":{\"experimental\":\"yes\"}}"),
                jvs("{\"filename\":\"b.pdf\", \"metadata\":{\"experimental\":\"no\"}}"),
                jvs("{\"filename\":\"c.pdf\", \"metadata\":{\"experimental\":\"yes\"}}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename FROM docs WHERE metadata.experimental = 'yes'");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("a.pdf", "c.pdf");
    }

    @Test
    void qualifiedTopLevelColumn_stillResolvesNatively() throws Exception {
        // docs.filename is table.column — must NOT be rewritten to JPATH('filename').
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\"}"),
                jvs("{\"filename\":\"b.pdf\"}")
            ), docsType()).build();
        var q = engine.compile("SELECT docs.filename FROM docs");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("a.pdf", "b.pdf");
    }

    @Test
    void tableAliasedDottedPath_stripsAliasBeforeJpath() throws Exception {
        // With alias `d`, `d.metadata.experimental` should read metadata.experimental from the row —
        // the alias is stripped before the path is joined for JPATH.
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"metadata\":{\"experimental\":\"yes\"}}")
            ), docsType()).build();
        var q = engine.compile("SELECT d.filename, d.metadata.experimental AS x FROM docs d");
        var rows = run(q);
        assertThat(rows.get(0).get("x").asText()).isEqualTo("yes");
    }

    @Test
    void readingAbsentDottedPath_doesNotMutateSourceRow() throws Exception {
        // Regression guard: earlier RowProjector used PAContext.AlwaysCreate which
        // silently auto-created missing intermediate nodes. NeverCreate is required.
        var row = jvs("{\"filename\":\"a.pdf\"}");
        String before = row.getJsonNode().toString();

        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(row), docsType()).build();
        // Reference a path that does not exist. Result should be null, source untouched.
        var q = engine.compile("SELECT filename, metadata.experimental AS x FROM docs");
        var rows = run(q);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("x").isNull()).isTrue();
        assertThat(row.getJsonNode().toString())
                .as("source row must not be mutated by a read of an absent path")
                .isEqualTo(before);
    }
}
