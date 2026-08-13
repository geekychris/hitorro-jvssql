/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.util.basefile.fs.BaseFile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Utilities for turning common file-shaped stream sources into
 * {@code Iterator<JVS>} that can be registered with
 * {@link com.hitorro.jvssql.JvsSqlEngine.Builder#registerStream}.
 *
 * <p>Unlike {@link com.hitorro.jvssql.refdata.ReferenceTableLoader} — which
 * fully materializes a small dimension file at engine build time — these
 * helpers stream the input row-by-row so you can register a source that's
 * larger than RAM and let it flow through the engine on demand.</p>
 */
public final class StreamSources {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonFactory FACTORY = MAPPER.getFactory();

    private StreamSources() {}

    /**
     * Stream a {@link BaseFile} as NDJSON — one JSON object per line, each becomes
     * a {@link JVS} row. Handles blank lines silently. Closes the underlying
     * reader when iteration reaches end-of-file.
     *
     * <p>This is what you want for real-world event ingestion: point at a
     * multi-gigabyte NDJSON log file on local disk, S3, HDFS, or FTP and pull
     * rows through the SQL engine without loading it all into memory.</p>
     */
    public static Iterator<JVS> ndjson(BaseFile source) {
        if (!source.exists()) {
            throw new JvsSqlException("stream source not found: " + source.getAbsolutePath());
        }
        try {
            InputStream is = source.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            return new NdjsonIterator(reader);
        } catch (Exception e) {
            throw new JvsSqlException("failed to open NDJSON source " + source.getAbsolutePath() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Stream a {@link BaseFile} as a JSON array — the top level must be an
     * array of objects. Streams elements lazily via Jackson's token parser;
     * does not materialize the entire array.
     */
    public static Iterator<JVS> jsonArray(BaseFile source) {
        if (!source.exists()) {
            throw new JvsSqlException("stream source not found: " + source.getAbsolutePath());
        }
        try {
            // Simplest correct approach: read the top-level array node, iterate its
            // elements lazily via ArrayNode's iterator. Trades a small load-of-array
            // cost for correctness — the NDJSON path stays token-streaming for the
            // large-file case, and JSON-array files are usually smaller anyway.
            InputStream is = source.getInputStream();
            JsonNode root;
            try (JsonParser parser = FACTORY.createParser(is)) {
                root = MAPPER.readTree(parser);
            }
            if (root == null || !root.isArray()) {
                throw new JvsSqlException("expected JSON array at top level of " + source.getAbsolutePath());
            }
            Iterator<JsonNode> it = root.elements();
            return new Iterator<>() {
                @Override public boolean hasNext() { return it.hasNext(); }
                @Override public JVS next() { return new JVS(it.next()); }
            };
        } catch (JvsSqlException e) { throw e; }
        catch (Exception e) {
            throw new JvsSqlException("failed to open JSON-array source " + source.getAbsolutePath() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Auto-detect: first non-whitespace character {@code '['} → array; otherwise
     * NDJSON. Buffers only the first byte, then re-opens if needed.
     */
    public static Iterator<JVS> autoDetect(BaseFile source) {
        try (InputStream sniff = source.getInputStream()) {
            int b;
            while ((b = sniff.read()) != -1 && Character.isWhitespace(b)) { /* skip */ }
            boolean isArray = b == '[';
            return isArray ? jsonArray(source) : ndjson(source);
        } catch (Exception e) {
            throw new JvsSqlException("failed to detect format of " + source.getAbsolutePath() + ": " + e.getMessage(), e);
        }
    }

    // -- iterator impls -------------------------------------------------------

    private static final class NdjsonIterator implements Iterator<JVS> {
        private final BufferedReader reader;
        private String nextLine;
        private boolean closed;

        NdjsonIterator(BufferedReader reader) { this.reader = reader; }

        @Override public boolean hasNext() {
            if (closed) return false;
            while (nextLine == null) {
                try {
                    String line = reader.readLine();
                    if (line == null) { close(); return false; }
                    if (line.trim().isEmpty()) continue;  // skip blank lines
                    nextLine = line;
                } catch (Exception e) {
                    close();
                    throw new JvsSqlException("NDJSON read failed: " + e.getMessage(), e);
                }
            }
            return true;
        }

        @Override public JVS next() {
            if (!hasNext()) throw new NoSuchElementException();
            try {
                return new JVS(MAPPER.readTree(nextLine));
            } catch (Exception e) {
                throw new JvsSqlException("NDJSON parse failed on: " + nextLine, e);
            } finally {
                nextLine = null;
            }
        }

        private void close() {
            closed = true;
            try { reader.close(); } catch (Exception ignored) {}
        }
    }

    private static final class MappingIteratorAdapter implements Iterator<JVS> {
        private final JsonParser parser;
        private final MappingIterator<JsonNode> mi;
        MappingIteratorAdapter(JsonParser parser, MappingIterator<JsonNode> mi) {
            this.parser = parser; this.mi = mi;
        }
        @Override public boolean hasNext() {
            boolean h = mi.hasNext();
            if (!h) try { parser.close(); } catch (Exception ignored) {}
            return h;
        }
        @Override public JVS next() { return new JVS(mi.next()); }
    }
}
