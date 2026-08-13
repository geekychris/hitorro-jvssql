/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.file.FileFileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * ORDER BY implementation that spills to a {@link BaseFile} directory when the
 * in-memory buffer overflows.
 *
 * <p>Algorithm:</p>
 * <ol>
 *   <li>Read up to {@code memoryBudgetRows} rows into an in-memory buffer.</li>
 *   <li>If the source is drained before we hit the budget → sort the buffer in place
 *       and return an iterator over it. No spill.</li>
 *   <li>Otherwise sort the current buffer, write it as a sorted "run" (NDJSON) to a
 *       fresh spill file, clear the buffer, and continue reading.</li>
 *   <li>Once the source is drained, k-way merge the current buffer + all spill files
 *       using a min-heap keyed by the sort comparator.</li>
 * </ol>
 *
 * <p>Spill files land in {@code EngineConfig.spillDir()} (a {@link BaseFile}), which
 * can be local disk, S3, HDFS, etc. If unset, they land under the OS temp directory.
 * Spill files are deleted on iterator close or JVM shutdown.</p>
 *
 * <p>Serialization format: one JSON object per line (NDJSON). Trivial to inspect
 * and portable across BaseFile backends.</p>
 */
public final class ExternalMergeSort {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ExternalMergeSort() {}

    /**
     * Sort {@code source} according to {@code comparator}, spilling to {@code spillDir}
     * (or the OS temp dir if null) once the in-memory buffer reaches {@code memoryBudgetRows}.
     */
    public static Iterator<JsonNode> sort(Iterator<JsonNode> source,
                                          Comparator<JsonNode> comparator,
                                          int memoryBudgetRows,
                                          BaseFile spillDir) {
        List<JsonNode> buffer = new ArrayList<>(Math.min(memoryBudgetRows, 1024));
        List<BaseFile> spillFiles = new ArrayList<>();
        try {
            while (source.hasNext()) {
                buffer.add(source.next());
                if (buffer.size() >= memoryBudgetRows) {
                    buffer.sort(comparator);
                    spillFiles.add(writeRun(buffer, spillDir, spillFiles.size()));
                    buffer.clear();
                }
            }
            if (spillFiles.isEmpty()) {
                // Fully in-memory case — most queries land here.
                buffer.sort(comparator);
                return buffer.iterator();
            }
            // Merge the final in-memory buffer + all spilled runs.
            buffer.sort(comparator);
            List<Iterator<JsonNode>> runs = new ArrayList<>(spillFiles.size() + 1);
            runs.add(buffer.iterator());
            for (BaseFile f : spillFiles) runs.add(readRun(f));
            return new MergeIterator(runs, comparator, spillFiles);
        } catch (Exception e) {
            // Clean up on failure.
            for (BaseFile f : spillFiles) try { f.delete(); } catch (Exception ignored) {}
            throw new JvsSqlException("external sort failed: " + e.getMessage(), e);
        }
    }

    // -- run I/O -------------------------------------------------------------

    private static BaseFile writeRun(List<JsonNode> sortedBuffer, BaseFile spillDir, int runIndex) throws IOException {
        BaseFile dir = spillDir != null ? spillDir : defaultSpillDir();
        BaseFile spillFile = dir.getChild("jvssql-spill-" + System.nanoTime() + "-" + runIndex + ".ndjson");
        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(spillFile.getOutputStream(), StandardCharsets.UTF_8))) {
            for (JsonNode row : sortedBuffer) {
                w.write(MAPPER.writeValueAsString(row));
                w.write('\n');
            }
        }
        return spillFile;
    }

    private static Iterator<JsonNode> readRun(BaseFile spillFile) throws IOException {
        BufferedReader r = new BufferedReader(
                new InputStreamReader(spillFile.getInputStream(), StandardCharsets.UTF_8));
        return new Iterator<>() {
            private String next;
            private boolean closed;
            @Override public boolean hasNext() {
                if (closed) return false;
                if (next != null) return true;
                try {
                    next = r.readLine();
                    if (next == null) { closeQuietly(); return false; }
                    return true;
                } catch (IOException e) {
                    closeQuietly();
                    throw new JvsSqlException("spill read failed: " + e.getMessage(), e);
                }
            }
            @Override public JsonNode next() {
                if (!hasNext()) throw new NoSuchElementException();
                try { return MAPPER.readTree(next); }
                catch (IOException e) { throw new JvsSqlException("spill parse failed: " + e.getMessage(), e); }
                finally { next = null; }
            }
            private void closeQuietly() {
                closed = true;
                try { r.close(); } catch (IOException ignored) {}
            }
        };
    }

    /**
     * Best-effort default spill directory: {@code $TMPDIR/jvssql-spill/}. Created if missing.
     * Real deployments should configure a durable {@code EngineConfig.spillDir(...)}.
     */
    private static BaseFile defaultSpillDir() {
        String tmp = System.getProperty("java.io.tmpdir");
        java.io.File dir = new java.io.File(tmp, "jvssql-spill");
        dir.mkdirs();
        return FileFileSystem.Root.getFile(dir.getAbsolutePath());
    }

    // -- k-way merge ---------------------------------------------------------

    private static final class MergeIterator implements Iterator<JsonNode> {
        private final PriorityQueue<Node> heap;
        private final List<BaseFile> spillFiles;
        private final Comparator<JsonNode> comparator;

        MergeIterator(List<Iterator<JsonNode>> runs, Comparator<JsonNode> comparator, List<BaseFile> spillFiles) {
            this.comparator = comparator;
            this.spillFiles = spillFiles;
            this.heap = new PriorityQueue<>(runs.size(), Comparator.comparing(n -> n.row, comparator));
            for (Iterator<JsonNode> r : runs) {
                if (r.hasNext()) heap.add(new Node(r.next(), r));
            }
        }

        @Override public boolean hasNext() {
            if (!heap.isEmpty()) return true;
            // Best-effort cleanup at end of iteration.
            for (BaseFile f : spillFiles) try { f.delete(); } catch (Exception ignored) {}
            return false;
        }

        @Override public JsonNode next() {
            if (!hasNext()) throw new NoSuchElementException();
            Node n = heap.poll();
            JsonNode out = n.row;
            if (n.source.hasNext()) heap.add(new Node(n.source.next(), n.source));
            return out;
        }

        private static final class Node {
            final JsonNode row;
            final Iterator<JsonNode> source;
            Node(JsonNode row, Iterator<JsonNode> source) { this.row = row; this.source = source; }
        }
    }
}
