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
 * GROUP BY + built-in aggregates + HAVING. Everything a data engineer
 * expects from a small OLAP query — COUNT, SUM, AVG, MIN, MAX.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example03_Aggregates"}</p>
 */
public final class Example03_Aggregates {

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"dept\":\"eng\",     \"file_size\":100}"),
                jvs("{\"filename\":\"b.pdf\", \"dept\":\"eng\",     \"file_size\":200}"),
                jvs("{\"filename\":\"c.pdf\", \"dept\":\"eng\",     \"file_size\":500}"),
                jvs("{\"filename\":\"d.pdf\", \"dept\":\"sales\",   \"file_size\":1000}"),
                jvs("{\"filename\":\"e.pdf\", \"dept\":\"sales\",   \"file_size\":2000}"),
                jvs("{\"filename\":\"f.pdf\", \"dept\":\"finance\", \"file_size\":50}")
            ), docsType()).build();

        printQuery("counts + size stats per department",
            engine.compile(
                "SELECT dept, "
              + "       COUNT(*)         AS n_docs, "
              + "       SUM(file_size)   AS total_bytes, "
              + "       MIN(file_size)   AS smallest, "
              + "       MAX(file_size)   AS largest, "
              + "       AVG(file_size)   AS avg_bytes "
              + "FROM docs "
              + "GROUP BY dept"));

        // HAVING: only departments totalling > 500 bytes
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\",     \"file_size\":100}"),
                jvs("{\"dept\":\"eng\",     \"file_size\":200}"),
                jvs("{\"dept\":\"eng\",     \"file_size\":500}"),
                jvs("{\"dept\":\"sales\",   \"file_size\":1000}"),
                jvs("{\"dept\":\"sales\",   \"file_size\":2000}"),
                jvs("{\"dept\":\"finance\", \"file_size\":50}")
            ), docsType()).build();
        printQuery("only 'big' departments (HAVING SUM > 500)",
            engine2.compile(
                "SELECT dept, SUM(file_size) AS total FROM docs "
              + "GROUP BY dept HAVING SUM(file_size) > 500"));
    }
}
