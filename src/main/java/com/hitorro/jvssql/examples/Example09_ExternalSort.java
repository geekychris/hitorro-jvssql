/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.util.basefile.fs.file.FileFileSystem;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;

/**
 * External-merge sort with BaseFile spill: sort a large-enough input that the
 * in-memory buffer overflows and rows are written as sorted runs to a spill
 * directory, then k-way merged on read.
 *
 * <p>The engine spills automatically when the buffer exceeds
 * {@code memoryBudgetMB * 5000} rows (5000 rows/MB is a rough default).
 * Set an explicit {@link com.hitorro.jvssql.JvsSqlEngine.Builder#withSpillDirectory}
 * for durable/S3/HDFS spill; the OS temp directory is the fallback.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example09_ExternalSort"}</p>
 */
public final class Example09_ExternalSort {

    public static void main(String[] args) throws Exception {
        int N = 20_000;
        List<JVS> rows = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            rows.add(new JVS(MAPPER.readTree("{\"filename\":\"file-" + i + "\",\"file_size\":" + i + "}")));
        }
        Collections.shuffle(rows, new Random(0));

        var spillDir = FileFileSystem.Root.getFile(
            Files.createTempDirectory("jvssql-example-spill-").toAbsolutePath().toString());

        // Set a very tight memory budget so we spill even on 20k rows.
        var engine = JvsSqlEngine.builder()
            .withSpillDirectory(spillDir)
            .withMemoryBudgetMB(1)               // ~5,000 rows in memory then spill
            .registerStream("docs", rows.iterator(), docsType())
            .build();

        System.out.println("Sorting " + N + " rows through a 1MB memory budget "
            + "(rows spill to " + spillDir.getAbsolutePath() + ").");

        printQuery("top 10 by file_size DESC (spilled sort)",
            engine.compile("SELECT filename, file_size FROM docs ORDER BY file_size DESC LIMIT 10"));
    }
}
