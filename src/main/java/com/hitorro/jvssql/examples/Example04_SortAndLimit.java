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
 * ORDER BY / LIMIT / OFFSET, plus ORDER BY chained after a GROUP BY.
 * Sorting is in-memory in Phase 1; external-merge with BaseFile spill is a
 * follow-up task in the same phase.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example04_SortAndLimit"}</p>
 */
public final class Example04_SortAndLimit {

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"gamma.pdf\", \"file_size\":300}"),
                jvs("{\"filename\":\"alpha.pdf\", \"file_size\":100}"),
                jvs("{\"filename\":\"delta.pdf\", \"file_size\":400}"),
                jvs("{\"filename\":\"beta.pdf\",  \"file_size\":200}")
            ), docsType()).build();

        printQuery("largest three, desc by size",
            engine.compile("SELECT filename, file_size FROM docs ORDER BY file_size DESC LIMIT 3"));

        // ORDER BY after GROUP BY
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"dept\":\"eng\",     \"file_size\":100}"),
                jvs("{\"dept\":\"eng\",     \"file_size\":200}"),
                jvs("{\"dept\":\"sales\",   \"file_size\":1000}"),
                jvs("{\"dept\":\"finance\", \"file_size\":50}")
            ), docsType()).build();
        printQuery("departments by total size, largest first",
            engine2.compile(
                "SELECT dept, SUM(file_size) AS total FROM docs "
              + "GROUP BY dept ORDER BY total DESC"));
    }
}
