/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.file.FileFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.hitorro.jvssql.TestSupport.docsType;
import static com.hitorro.jvssql.TestSupport.jvs;
import static com.hitorro.jvssql.TestSupport.run;
import static com.hitorro.jvssql.TestSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reference-table loading (JSON array + NDJSON formats), plus INNER and LEFT
 * hash joins between a streaming input and a reference table.
 */
class ReferenceTableAndJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Type usersType() throws Exception {
        String typeJson = "{"
            + "\"name\": \"users\","
            + "\"fields\": ["
            + "  {\"name\": \"email\",     \"type\": \"core_string\"},"
            + "  {\"name\": \"name\",      \"type\": \"core_string\"},"
            + "  {\"name\": \"tenure\",    \"type\": \"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static BaseFile writeUsersFile(Path tmpDir, String contents) throws Exception {
        Path p = tmpDir.resolve("users.json");
        Files.writeString(p, contents);
        return FileFileSystem.Root.getFile(p.toAbsolutePath().toString());
    }

    @Test
    void innerJoin_jsonArrayReferenceTable(@TempDir Path tmpDir) throws Exception {
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"alex@example.com\",  \"name\":\"Alex\",  \"tenure\":3}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"alex@example.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"stranger@nowhere.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name, u.tenure "
          + "FROM docs d "
          + "JOIN users u ON d.author = u.email"));
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.get("filename").asText() + ":" + r.get("name").asText())
                        .containsExactlyInAnyOrder("a.pdf:Chris", "b.pdf:Alex");
    }

    @Test
    void leftJoin_nullPadsMissingRightSide(@TempDir Path tmpDir) throws Exception {
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"stranger@nowhere.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name "
          + "FROM docs d "
          + "LEFT JOIN users u ON d.author = u.email"));
        assertThat(rows).hasSize(2);
        var byFile = rows.stream().collect(
            java.util.stream.Collectors.toMap(r -> r.get("filename").asText(), r -> r.get("name")));
        assertThat(byFile.get("a.pdf").asText()).isEqualTo("Chris");
        assertThat(byFile.get("b.pdf").isNull()).isTrue();
    }

    @Test
    void ndjsonFormat_alsoWorks(@TempDir Path tmpDir) throws Exception {
        var ref = writeUsersFile(tmpDir,
              "{\"email\":\"a@x.com\", \"name\":\"A\", \"tenure\":1}\n"
            + "{\"email\":\"b@x.com\", \"name\":\"B\", \"tenure\":2}\n"
            + "{\"email\":\"c@x.com\", \"name\":\"C\", \"tenure\":3}\n");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"z.pdf\", \"author\":\"b@x.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name FROM docs d JOIN users u ON d.author = u.email"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("name").asText()).isEqualTo("B");
    }

    @Test
    void joinWithProjectionAndFilter(@TempDir Path tmpDir) throws Exception {
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"alex@example.com\",  \"name\":\"Alex\",  \"tenure\":3},"
            + "{\"email\":\"pat@hitorro.com\",   \"name\":\"Pat\",   \"tenure\":1}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"pat@hitorro.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"alex@example.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name, u.tenure "
          + "FROM docs d JOIN users u ON d.author = u.email "
          + "WHERE u.tenure >= 5"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("name").asText()).isEqualTo("Chris");
    }
}
