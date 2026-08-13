/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.PreparedQuery;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Small helpers shared by the runnable {@code com.hitorro.jvssql.examples} programs.
 * Kept intentionally minimal so the examples themselves read as end-to-end recipes.
 */
final class ExampleSupport {

    private ExampleSupport() {}

    static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectMapper PRETTY = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    /** Standard "docs" type used by most examples. */
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

    /** Reference-table type used by join examples. */
    static Type usersType() throws Exception {
        String typeJson = "{"
            + "\"name\": \"users\","
            + "\"fields\": ["
            + "  {\"name\": \"email\",  \"type\": \"core_string\"},"
            + "  {\"name\": \"name\",   \"type\": \"core_string\"},"
            + "  {\"name\": \"tenure\", \"type\": \"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    static JVS jvs(String json) throws Exception {
        return new JVS(MAPPER.readTree(json));
    }

    static Iterator<JVS> stream(JVS... rows) {
        return Arrays.asList(rows).iterator();
    }

    /** Print each result row from a query, pretty-formatted. */
    static void printQuery(String label, PreparedQuery q) {
        System.out.println();
        System.out.println("── " + label + " ──");
        System.out.println("SQL: " + q.sql().replaceAll("\\s+", " ").trim());
        System.out.println("Results:");
        var it = q.asIterator();
        int i = 0;
        while (it.hasNext()) {
            JsonNode row = it.next();
            try {
                System.out.println("  " + (++i) + ") " + PRETTY.writeValueAsString(row).replace("\n", "\n     "));
            } catch (Exception e) {
                System.out.println("  " + (++i) + ") " + row);
            }
        }
        if (i == 0) System.out.println("  (no rows)");
    }
}
