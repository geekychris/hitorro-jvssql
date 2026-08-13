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
 * The absolute minimum: register one stream, run a SELECT with a WHERE.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example01_BasicSelect"}</p>
 * <p>Or from IntelliJ, right-click the class and Run.</p>
 */
public final class Example01_BasicSelect {

    public static void main(String[] args) throws Exception {
        var docs = stream(
            jvs("{\"filename\":\"report.pdf\",   \"classification\":\"public\",   \"file_size\":1500}"),
            jvs("{\"filename\":\"invoice.pdf\",  \"classification\":\"internal\", \"file_size\":800}"),
            jvs("{\"filename\":\"secret.pdf\",   \"classification\":\"restricted\",\"file_size\":50}"),
            jvs("{\"filename\":\"notes.txt\",    \"classification\":\"public\",   \"file_size\":200}")
        );

        var engine = JvsSqlEngine.builder()
            .registerStream("docs", docs, docsType())
            .build();

        // Simplest SELECT: pull every row's filename and classification.
        printQuery("all rows",
            engine.compile("SELECT filename, classification FROM docs"));

        // Rebuild the engine because the previous query consumed the source iterator.
        // In real use, wrap the source in a re-openable/caching iterator upstream if you
        // need multiple executions against the same data.
        var docs2 = stream(
            jvs("{\"filename\":\"report.pdf\",   \"classification\":\"public\",   \"file_size\":1500}"),
            jvs("{\"filename\":\"invoice.pdf\",  \"classification\":\"internal\", \"file_size\":800}"),
            jvs("{\"filename\":\"secret.pdf\",   \"classification\":\"restricted\",\"file_size\":50}"),
            jvs("{\"filename\":\"notes.txt\",    \"classification\":\"public\",   \"file_size\":200}")
        );
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", docs2, docsType())
            .build();

        // WHERE clause with a boolean composition.
        printQuery("public and larger than 100 bytes",
            engine2.compile(
                "SELECT filename, file_size FROM docs "
              + "WHERE classification = 'public' AND file_size > 100"));
    }
}
