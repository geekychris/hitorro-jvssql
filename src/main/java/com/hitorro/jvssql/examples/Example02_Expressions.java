/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jvssql.JvsSqlEngine;

import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;

/**
 * Every expression form the engine understands out of the box: arithmetic,
 * CASE, COALESCE, LIKE, IN, BETWEEN, and the string/math scalar library.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example02_Expressions"}</p>
 */
public final class Example02_Expressions {

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"annual-report.pdf\", \"file_size\":50000,   \"dept\":\"finance\"}"),
                jvs("{\"filename\":\"notes.txt\",         \"file_size\":200,     \"dept\":\"eng\"}"),
                jvs("{\"filename\":\"invoice.pdf\",       \"file_size\":800,     \"dept\":\"finance\"}"),
                jvs("{\"filename\":\"handbook.pdf\",      \"file_size\":1500000, \"dept\":\"hr\"}"),
                jvs("{\"filename\":\"README.md\",         \"file_size\":5000,    \"dept\":\"eng\"}")
            ), docsType()).build();

        // CASE bucketing on file_size
        printQuery("size bucket (CASE)",
            engine.compile(
                "SELECT filename, file_size, "
              + "  CASE WHEN file_size < 1000 THEN 'tiny' "
              + "       WHEN file_size < 100000 THEN 'medium' "
              + "       ELSE 'huge' END AS bucket "
              + "FROM docs"));

        // Multi-op arithmetic + string function
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"annual-report.pdf\", \"file_size\":50000}")
            ), docsType()).build();
        printQuery("computed columns (arithmetic + string funcs)",
            engine2.compile(
                "SELECT UPPER(filename) AS f_upper, "
              + "       CHAR_LENGTH(filename) AS name_len, "
              + "       file_size / 1024 AS size_kb, "
              + "       file_size * 8 AS size_bits "
              + "FROM docs"));

        // LIKE, IN, BETWEEN
        var engine3 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"annual-report.pdf\", \"file_size\":50000,   \"dept\":\"finance\"}"),
                jvs("{\"filename\":\"notes.txt\",         \"file_size\":200,     \"dept\":\"eng\"}"),
                jvs("{\"filename\":\"invoice.pdf\",       \"file_size\":800,     \"dept\":\"finance\"}"),
                jvs("{\"filename\":\"handbook.pdf\",      \"file_size\":1500000, \"dept\":\"hr\"}"),
                jvs("{\"filename\":\"README.md\",         \"file_size\":5000,    \"dept\":\"eng\"}")
            ), docsType()).build();
        printQuery("LIKE + IN + BETWEEN",
            engine3.compile(
                "SELECT filename FROM docs "
              + "WHERE filename LIKE '%.pdf' "
              + "  AND dept IN ('finance', 'hr') "
              + "  AND file_size BETWEEN 500 AND 100000"));
    }
}
