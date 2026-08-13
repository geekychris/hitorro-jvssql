/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.util.basefile.fs.file.FileFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hitorro.jvssql.TestSupport.MAPPER;
import static com.hitorro.jvssql.TestSupport.docsType;
import static com.hitorro.jvssql.TestSupport.run;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * External-merge sort: force spill by shrinking memory budget so that all but
 * a handful of rows go to disk, then verify the final sorted order is correct.
 * Uses hitorro-basefile's local FS impl for the spill directory.
 */
class ExternalSortTest {

    @Test
    void sortSpillsAndReturnsCorrectOrder(@TempDir Path tmpDir) throws Exception {
        int N = 5000;
        List<Long> sizes = new ArrayList<>(N);
        for (int i = 0; i < N; i++) sizes.add((long) i);
        Collections.shuffle(sizes, new Random(42));

        List<JVS> rows = new ArrayList<>(N);
        for (long s : sizes) {
            rows.add(new JVS(MAPPER.readTree(
                "{\"filename\":\"f" + s + "\",\"file_size\":" + s + "}")));
        }

        var spillDir = FileFileSystem.Root.getFile(tmpDir.toAbsolutePath().toString());
        // Memory budget = 1 MB → ~5000 rows/MB = 5000 row budget. But we want to force
        // multiple spill runs, so we set a very small budget and let the engine spill.
        var engine = JvsSqlEngine.builder()
            .withSpillDirectory(spillDir)
            .withMemoryBudgetMB(1)          // budget * rowsPerMB in Executor = ~5k; we push 5k
            .registerStream("docs", rows.iterator(), docsType())
            .build();
        var out = run(engine.compile("SELECT file_size FROM docs ORDER BY file_size"));
        assertThat(out).hasSize(N);
        for (int i = 0; i < N; i++) {
            assertThat(out.get(i).get("file_size").asLong()).isEqualTo(i);
        }
    }

    @Test
    void sortDescWithLimit(@TempDir Path tmpDir) throws Exception {
        List<JVS> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(new JVS(MAPPER.readTree("{\"filename\":\"f\",\"file_size\":" + i + "}")));
        }
        Collections.shuffle(rows, new Random(1));
        var engine = JvsSqlEngine.builder()
            .withSpillDirectory(FileFileSystem.Root.getFile(tmpDir.toAbsolutePath().toString()))
            .registerStream("docs", rows.iterator(), docsType())
            .build();
        var out = run(engine.compile("SELECT file_size FROM docs ORDER BY file_size DESC LIMIT 5"));
        assertThat(out).extracting(r -> r.get("file_size").asLong())
                       .containsExactly(99L, 98L, 97L, 96L, 95L);
    }
}
