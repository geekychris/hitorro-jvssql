/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.file.FileFileSystem;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;
import static com.hitorro.jvssql.examples.ExampleSupport.usersType;

/**
 * Stream × reference-table JOIN. The classic dimension-lookup pattern:
 * documents flow past as a stream, we enrich each row by looking up its
 * author against a static reference table loaded from a BaseFile.
 *
 * <p>Reference tables are loaded from a JSON array or NDJSON file at engine
 * build time and hash-indexed on the join key when the query executes.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example08_ReferenceJoin"}</p>
 */
public final class Example08_ReferenceJoin {

    public static void main(String[] args) throws Exception {
        // Write a small users reference table to a temp file. In real use this
        // would be an S3 file, a checked-in config file, or anything BaseFile
        // can point at.
        Path tmp = Files.createTempFile("users-", ".json");
        Files.writeString(tmp, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"alex@example.com\",  \"name\":\"Alex\",  \"tenure\":3},"
            + "{\"email\":\"pat@hitorro.com\",   \"name\":\"Pat\",   \"tenure\":1}"
            + "]");
        BaseFile usersFile = FileFileSystem.Root.getFile(tmp.toAbsolutePath().toString());

        // INNER join: only rows that match on both sides
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"pat@hitorro.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"stranger@nowhere.com\"}")
            ), docsType())
            .registerReferenceTable("users", usersFile, usersType())
            .build();
        printQuery("INNER JOIN: only rows with matching users",
            engine.compile(
                "SELECT d.filename, u.name, u.tenure "
              + "FROM docs d JOIN users u ON d.author = u.email"));

        // LEFT join: keep unmatched left rows, null on right
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"pat@hitorro.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"stranger@nowhere.com\"}")
            ), docsType())
            .registerReferenceTable("users", usersFile, usersType())
            .build();
        printQuery("LEFT JOIN: unmatched left rows kept with NULL right",
            engine2.compile(
                "SELECT d.filename, COALESCE(u.name, '(unknown)') AS author_name "
              + "FROM docs d LEFT JOIN users u ON d.author = u.email"));

        // Join + filter + aggregation
        var engine3 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\", \"file_size\":100}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"chris@hitorro.com\", \"file_size\":200}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"alex@example.com\",  \"file_size\":500}"),
                jvs("{\"filename\":\"d.pdf\", \"author\":\"pat@hitorro.com\",   \"file_size\":50}")
            ), docsType())
            .registerReferenceTable("users", usersFile, usersType())
            .build();
        printQuery("Enrichment + aggregation: total bytes authored per senior user (tenure ≥ 5)",
            engine3.compile(
                "SELECT u.name, COUNT(*) AS n_docs, SUM(d.file_size) AS total_bytes "
              + "FROM docs d JOIN users u ON d.author = u.email "
              + "WHERE u.tenure >= 5 "
              + "GROUP BY u.name"));

        Files.deleteIfExists(tmp);
    }
}
