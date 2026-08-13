/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.PreparedQuery;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.io.StoreException;

import java.io.IOException;

import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;

/**
 * Demonstrates the push-based API. Instead of pulling results with
 * {@code asIterator()}, supply a {@code Sink<JsonNode>} to
 * {@code PreparedQuery.execute(sink)} — the engine pushes rows into your
 * sink until the source is exhausted.
 *
 * <p>Use this shape when you want to compose with hitorro-streams pipeline
 * primitives (async-via-ThreadedQueue, batched writes, etc.), or when the
 * downstream is naturally a consumer (Kafka producer, HTTP writer).</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example07_PushToSink"}</p>
 */
public final class Example07_PushToSink {

    /** A tiny Sink that just prints each row it receives. */
    static final class PrintSink implements Sink<JsonNode> {
        private long count = 0;
        @Override public boolean init(com.fasterxml.jackson.databind.JsonNode config) { return true; }
        @Override public boolean start() { System.out.println("(sink: start)"); return true; }
        @Override public boolean add(JsonNode row) throws IOException, StoreException {
            System.out.println("(sink: row " + (++count) + ") " + row);
            return true;
        }
        @Override public boolean stop() { System.out.println("(sink: stop; total=" + count + ")"); return true; }
        @Override public void close() {}
    }

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"file_size\":100}"),
                jvs("{\"filename\":\"b.pdf\", \"file_size\":500}"),
                jvs("{\"filename\":\"c.pdf\", \"file_size\":2000}")
            ), docsType()).build();

        PreparedQuery q = engine.compile(
            "SELECT filename, file_size FROM docs WHERE file_size > 200 ORDER BY file_size DESC");

        q.execute(new PrintSink());
    }
}
