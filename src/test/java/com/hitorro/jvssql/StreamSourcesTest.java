/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.hitorro.jvssql.source.StreamSources;
import com.hitorro.util.basefile.fs.file.FileFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.hitorro.jvssql.TestSupport.docsType;
import static com.hitorro.jvssql.TestSupport.run;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * File-shaped stream sources — {@link StreamSources#ndjson}, {@code jsonArray},
 * {@code autoDetect}. Verifies each ingests correctly and closes cleanly when
 * driven end-to-end through the SQL engine.
 */
class StreamSourcesTest {

    @Test
    void ndjsonSource_streamsRows(@TempDir Path tmp) throws Exception {
        Path p = tmp.resolve("events.ndjson");
        Files.writeString(p,
              "{\"filename\":\"a\",\"file_size\":100}\n"
            + "\n"                                      // blank line silently skipped
            + "{\"filename\":\"b\",\"file_size\":200}\n"
            + "{\"filename\":\"c\",\"file_size\":300}\n");
        var bf = FileFileSystem.Root.getFile(p.toAbsolutePath().toString());
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", StreamSources.ndjson(bf), docsType())
            .build();
        var rows = run(engine.compile(
            "SELECT filename FROM docs WHERE file_size >= 200 ORDER BY file_size"));
        assertThat(rows).extracting(r -> r.get("filename").asText())
                        .containsExactly("b", "c");
    }

    @Test
    void jsonArraySource_streamsRows(@TempDir Path tmp) throws Exception {
        Path p = tmp.resolve("events.json");
        Files.writeString(p, "["
            + "{\"filename\":\"a\",\"file_size\":100},"
            + "{\"filename\":\"b\",\"file_size\":200},"
            + "{\"filename\":\"c\",\"file_size\":300}"
            + "]");
        var bf = FileFileSystem.Root.getFile(p.toAbsolutePath().toString());
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", StreamSources.jsonArray(bf), docsType())
            .build();
        assertThat(run(engine.compile("SELECT COUNT(*) AS n FROM docs")).get(0).get("n").asLong())
            .isEqualTo(3L);
    }

    @Test
    void autoDetect_worksForBothShapes(@TempDir Path tmp) throws Exception {
        // NDJSON
        Path a = tmp.resolve("a.dat");
        Files.writeString(a, "{\"filename\":\"x\",\"file_size\":1}\n");
        var engA = JvsSqlEngine.builder()
            .registerStream("docs", StreamSources.autoDetect(FileFileSystem.Root.getFile(a.toAbsolutePath().toString())), docsType())
            .build();
        assertThat(run(engA.compile("SELECT COUNT(*) AS n FROM docs")).get(0).get("n").asLong()).isEqualTo(1L);

        // JSON array
        Path b = tmp.resolve("b.dat");
        Files.writeString(b, "[{\"filename\":\"y\",\"file_size\":2},{\"filename\":\"z\",\"file_size\":3}]");
        var engB = JvsSqlEngine.builder()
            .registerStream("docs", StreamSources.autoDetect(FileFileSystem.Root.getFile(b.toAbsolutePath().toString())), docsType())
            .build();
        assertThat(run(engB.compile("SELECT COUNT(*) AS n FROM docs")).get(0).get("n").asLong()).isEqualTo(2L);
    }
}
