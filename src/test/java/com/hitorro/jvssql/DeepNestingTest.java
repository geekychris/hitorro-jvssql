/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.util.json.JsonMappers;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.List;

import static com.hitorro.jvssql.TestSupport.run;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Values nested 600 levels deep behave as on Jackson 2 (limit 1000). On Jackson 3,
 * {@code JsonNode.toString()} throws past 500 levels, so the set operations' dedup keys and the
 * arithmetic error message must not use it.
 */
class DeepNestingTest {

    static String nested(int depth) {
        return "[".repeat(depth) + "]".repeat(depth);
    }

    static Type type() throws Exception {
        Type t = new Type();
        t.init(JsonMappers.json().readTree("{\"name\":\"t\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},{\"name\":\"deep\",\"type\":\"core_string\"},"
                + "{\"name\":\"n\",\"type\":\"core_long\"}]}"));
        return t;
    }

    static JVS row(String id, String deep) throws Exception {
        return new JVS(JsonMappers.json().readTree("{\"id\":\"" + id + "\",\"deep\":" + deep + ",\"n\":" + deep + "}"));
    }

    static JvsSqlEngine engine() throws Exception {
        String d = nested(600);
        return JvsSqlEngine.builder()
                .registerStream("a", List.of(row("x", d), row("y", "1"), row("x", d)).iterator(), type())
                .registerStream("b", List.of(row("x", d), row("z", "2")).iterator(), type())
                .build();
    }

    static List<String> ids(List<JsonNode> rows) {
        return rows.stream().map(r -> r.get("id").stringValue()).toList();
    }

    @Test
    void setOperationsDeduplicateDeepRows() throws Exception {
        assertThat(ids(run(engine().compile("SELECT id, deep FROM a UNION SELECT id, deep FROM b"))))
                .containsExactly("x", "y", "z");
        assertThat(ids(run(engine().compile("SELECT id, deep FROM a INTERSECT SELECT id, deep FROM b"))))
                .containsExactly("x");
        assertThat(ids(run(engine().compile("SELECT id, deep FROM a EXCEPT SELECT id, deep FROM b"))))
                .containsExactly("y");
        List<JsonNode> union = run(engine().compile("SELECT id, deep FROM a UNION SELECT id, deep FROM b"));
        assertThat(JsonMappers.json().writeValueAsString(union.get(0).get("deep"))).isEqualTo(nested(600));
    }

    @Test
    void arithmeticOnDeepValueIsAJvsSqlException() throws Exception {
        var q = engine().compile("SELECT n + 1 AS m FROM a WHERE id = 'x'");
        assertThatThrownBy(() -> run(q))
                .isInstanceOf(JvsSqlException.class)
                .hasMessageContaining("arithmetic on non-numeric values: " + nested(600) + " + 1");
    }
}
