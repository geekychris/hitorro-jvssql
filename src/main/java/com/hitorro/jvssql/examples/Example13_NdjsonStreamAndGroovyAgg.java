/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.source.StreamSources;
import com.hitorro.util.basefile.fs.file.FileFileSystem;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;

/**
 * Two final Phase 2-late add-ons:
 * <ul>
 *   <li>{@link StreamSources#ndjson} — turn a large NDJSON file (local disk /
 *       S3 / HDFS via BaseFile) into a lazy {@code Iterator<JVS>} you can
 *       register as a stream, no full-materialize required</li>
 *   <li>Groovy aggregate UDF — declare {@code init/accum/result} closures
 *       inline and use the aggregate in a normal GROUP BY</li>
 * </ul>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example13_NdjsonStreamAndGroovyAgg"}</p>
 */
public final class Example13_NdjsonStreamAndGroovyAgg {

    private static Type ordersType() throws Exception {
        String typeJson = "{\"name\":\"orders\",\"fields\":["
            + "{\"name\":\"customer\",\"type\":\"core_string\"},"
            + "{\"name\":\"product\", \"type\":\"core_string\"},"
            + "{\"name\":\"qty\",     \"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    public static void main(String[] args) throws Exception {
        // Write a small NDJSON file — in real use this would be your log file,
        // Kafka dump, or historical event archive on any BaseFile-backed store.
        Path tmp = Files.createTempFile("orders-", ".ndjson");
        Files.writeString(tmp,
              "{\"customer\":\"alice\", \"product\":\"apple\",   \"qty\":3}\n"
            + "{\"customer\":\"alice\", \"product\":\"banana\",  \"qty\":2}\n"
            + "{\"customer\":\"bob\",   \"product\":\"apple\",   \"qty\":5}\n"
            + "{\"customer\":\"bob\",   \"product\":\"cherry\",  \"qty\":1}\n"
            + "{\"customer\":\"alice\", \"product\":\"cherry\",  \"qty\":4}\n"
            + "{\"customer\":\"carol\", \"product\":\"banana\",  \"qty\":6}\n");

        var bf = FileFileSystem.Root.getFile(tmp.toAbsolutePath().toString());

        var engine = JvsSqlEngine.builder()
            // Groovy UDAF: comma-joined DISTINCT sorted products per customer.
            // Closure syntax: each MUST start with -> so shell.evaluate returns a Closure.
            .registerGroovyAggregate("PRODUCTS_LIST",
                "init:   { -> [] as Set }\n"
              + "accum:  { acc, v -> if (v) acc << v.toString(); acc }\n"
              + "result: { acc -> acc.sort().join(', ') }")
            // Register the NDJSON file as a streaming source.
            .registerStream("orders", StreamSources.ndjson(bf), ordersType())
            .build();

        printQuery("Total qty + distinct products per customer",
            engine.compile(
                "SELECT customer, "
              + "       SUM(qty)             AS total_qty, "
              + "       COUNT(*)             AS n_lines, "
              + "       PRODUCTS_LIST(product) AS products "
              + "FROM   orders "
              + "GROUP BY customer "
              + "ORDER BY total_qty DESC"));

        Files.deleteIfExists(tmp);
    }
}
