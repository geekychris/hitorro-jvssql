/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Shared fixtures for the JVS-SQL test suite. Keeps every test class small and
 * focused: define a type once, spin up rows with {@link #jvs}, run a query with
 * {@link #run}.
 */
final class TestSupport {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private TestSupport() {}

    /**
     * A small "docs" type used across most tests: filename (string), classification
     * (string), file_size (long), dept (string). MLS field {@code content} for MLS tests.
     */
    static Type docsType() throws Exception {
        String typeJson = "{"
            + "\"name\": \"docs\","
            + "\"fields\": ["
            + "  {\"name\": \"filename\",       \"type\": \"core_string\"},"
            + "  {\"name\": \"classification\", \"type\": \"core_string\"},"
            + "  {\"name\": \"file_size\",      \"type\": \"core_long\"},"
            + "  {\"name\": \"dept\",           \"type\": \"core_string\"},"
            + "  {\"name\": \"author\",         \"type\": \"core_string\"},"
            + "  {\"name\": \"content\",        \"type\": \"core_string\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    /**
     * Parse a JSON literal into a JVS document. Handles the common test pattern:
     * {@code jvs("{\"filename\": \"a.pdf\", \"file_size\": 100}")}.
     */
    static JVS jvs(String json) throws Exception {
        return new JVS(MAPPER.readTree(json));
    }

    /** Wrap a variable-arity list of JVS docs as an iterator. */
    static Iterator<JVS> stream(JVS... rows) {
        return Arrays.asList(rows).iterator();
    }

    /** Fully drain a PreparedQuery into a List. */
    static List<JsonNode> run(PreparedQuery q) {
        List<JsonNode> out = new ArrayList<>();
        var it = q.asIterator();
        while (it.hasNext()) out.add(it.next());
        return out;
    }
}
