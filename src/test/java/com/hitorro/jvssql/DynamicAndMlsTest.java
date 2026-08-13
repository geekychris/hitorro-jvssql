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
 * JPATH('path') for undeclared JVS fields, and MLS(field, 'lang') for the
 * multi-language string envelope.
 */
class DynamicAndMlsTest {

    @Test
    void dynamic_readsUndeclaredField() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"metadata\":{\"experimental\":\"yes\"}}"),
                jvs("{\"filename\":\"b.pdf\", \"metadata\":{\"experimental\":\"no\"}}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename, JPATH('metadata.experimental') AS x FROM docs");
        var rows = run(q);
        assertThat(rows).extracting(r -> r.get("x").asText())
                        .containsExactly("yes", "no");
    }

    @Test
    void dynamic_absentPathIsNull() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\"}")
            ), docsType()).build();
        var q = engine.compile("SELECT JPATH('does.not.exist') AS x FROM docs");
        var rows = run(q);
        assertThat(rows.get(0).get("x").isNull()).isTrue();
    }

    @Test
    void mls_extractsLanguageText() throws Exception {
        // MLS envelope shape: {"mls": [{"lang":"en","text":"..."}, {"lang":"fr","text":"..."}]}
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a\", \"content\":{\"mls\":["
                    + "{\"lang\":\"en\",\"text\":\"Hello world\"},"
                    + "{\"lang\":\"fr\",\"text\":\"Bonjour le monde\"}"
                    + "]}}"),
                jvs("{\"filename\":\"b\", \"content\":{\"mls\":["
                    + "{\"lang\":\"en\",\"text\":\"Goodbye\"}"
                    + "]}}")
            ), docsType()).build();
        var q = engine.compile("SELECT filename, MLS(content, 'en') AS en, MLS(content, 'fr') AS fr FROM docs");
        var rows = run(q);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("en").asText()).isEqualTo("Hello world");
        assertThat(rows.get(0).get("fr").asText()).isEqualTo("Bonjour le monde");
        assertThat(rows.get(1).get("en").asText()).isEqualTo("Goodbye");
        // French absent on second row → NULL
        assertThat(rows.get(1).get("fr").isNull()).isTrue();
    }

    @Test
    void mls_missingEnvelopeIsNull() throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(jvs("{\"filename\":\"a\"}")), docsType()).build();
        var q = engine.compile("SELECT MLS(content, 'en') AS body FROM docs");
        var rows = run(q);
        assertThat(rows.get(0).get("body").isNull()).isTrue();
    }
}
