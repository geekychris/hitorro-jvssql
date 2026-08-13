/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke test for Phase 1: Scan + Filter + Project against a small
 * in-memory JVS iterator. Covers the plumbing: type→RelDataType, JvsSchema
 * registration, Calcite parse+validate+plan, executor walk.
 */
class BasicSelectTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Trivial type: three fields, all string, mimicking a filename catalog row.
     */
    private static Type demoType() throws Exception {
        String typeJson = """
        {
          "name": "demo",
          "fields": [
            {"name": "filename",       "type": "core_string"},
            {"name": "classification", "type": "core_string"},
            {"name": "file_size",      "type": "core_long"}
          ]
        }
        """;
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS jvs(String json) throws Exception {
        return new JVS(MAPPER.readTree(json));
    }

    private static Iterator<JVS> stream(JVS... rows) {
        return Arrays.asList(rows).iterator();
    }

    @Test
    void selectStarLike_projectAllColumns() throws Exception {
        Iterator<JVS> docs = stream(
            jvs("{\"filename\": \"a.pdf\", \"classification\": \"public\",   \"file_size\": 100}"),
            jvs("{\"filename\": \"b.pdf\", \"classification\": \"internal\", \"file_size\": 500}"),
            jvs("{\"filename\": \"c.pdf\", \"classification\": \"internal\", \"file_size\": 2000}")
        );
        JvsSqlEngine engine = JvsSqlEngine.builder()
            .registerStream("docs", docs, demoType())
            .build();

        PreparedQuery q = engine.compile("SELECT filename, classification, file_size FROM docs");
        List<JsonNode> rows = toList(q);

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).get("filename").asText()).isEqualTo("a.pdf");
        assertThat(rows.get(2).get("file_size").asLong()).isEqualTo(2000L);
    }

    @Test
    void whereClause_filtersRows() throws Exception {
        Iterator<JVS> docs = stream(
            jvs("{\"filename\": \"a.pdf\", \"classification\": \"public\",   \"file_size\": 100}"),
            jvs("{\"filename\": \"b.pdf\", \"classification\": \"internal\", \"file_size\": 500}"),
            jvs("{\"filename\": \"c.pdf\", \"classification\": \"internal\", \"file_size\": 2000}")
        );
        JvsSqlEngine engine = JvsSqlEngine.builder()
            .registerStream("docs", docs, demoType())
            .build();

        PreparedQuery q = engine.compile(
            "SELECT filename FROM docs WHERE file_size > 400 AND classification = 'internal'");
        List<JsonNode> rows = toList(q);

        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("b.pdf", "c.pdf");
    }

    private static List<JsonNode> toList(PreparedQuery q) {
        List<JsonNode> out = new java.util.ArrayList<>();
        var it = q.asIterator();
        while (it.hasNext()) out.add(it.next());
        return out;
    }
}
