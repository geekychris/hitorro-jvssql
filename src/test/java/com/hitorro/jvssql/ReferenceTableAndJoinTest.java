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
import java.util.List;

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
    void rightJoin_nullPadsMissingLeftSide(@TempDir Path tmpDir) throws Exception {
        // 3 users in the reference table, only 1 has a matching doc. RIGHT JOIN
        // should surface all 3 users with null-padded left (doc) side for the
        // two unmatched users.
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"alex@example.com\",  \"name\":\"Alex\",  \"tenure\":3},"
            + "{\"email\":\"other@nowhere.com\", \"name\":\"Other\", \"tenure\":1}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name "
          + "FROM docs d "
          + "RIGHT JOIN users u ON d.author = u.email"));
        assertThat(rows).hasSize(3);
        var byUserName = rows.stream().collect(
            java.util.stream.Collectors.toMap(r -> r.get("name").asText(), r -> r.get("filename")));
        assertThat(byUserName.get("Chris").asText()).isEqualTo("a.pdf");
        assertThat(byUserName.get("Alex").isNull()).as("Alex has no matching doc").isTrue();
        assertThat(byUserName.get("Other").isNull()).as("Other has no matching doc").isTrue();
    }

    @Test
    void fullJoin_preservesBothUnmatchedSides(@TempDir Path tmpDir) throws Exception {
        // 2 docs, 2 users. Only chris@ has both. Total = 3 rows: matched Chris,
        // orphan doc, orphan user.
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"lonely@nowhere.com\", \"name\":\"Lonely\", \"tenure\":1}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"orphan.pdf\", \"author\":\"stranger@nowhere.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name "
          + "FROM docs d "
          + "FULL JOIN users u ON d.author = u.email"));
        assertThat(rows).hasSize(3);
        long matched = rows.stream()
                .filter(r -> !r.get("filename").isNull() && !r.get("name").isNull())
                .count();
        long docOnly = rows.stream()
                .filter(r -> !r.get("filename").isNull() && r.get("name").isNull())
                .count();
        long userOnly = rows.stream()
                .filter(r -> r.get("filename").isNull() && !r.get("name").isNull())
                .count();
        assertThat(matched).as("1 matched pair (Chris + a.pdf)").isEqualTo(1);
        assertThat(docOnly).as("1 doc with no matching user (orphan.pdf)").isEqualTo(1);
        assertThat(userOnly).as("1 user with no matching doc (Lonely)").isEqualTo(1);
    }

    @Test
    void rightJoin_multipleLeftMatches_allSurface(@TempDir Path tmpDir) throws Exception {
        // Two docs share the same author — RIGHT JOIN should surface both matches
        // for that user, plus null-padded rows for unmatched users.
        var ref = writeUsersFile(tmpDir, "["
            + "{\"email\":\"chris@hitorro.com\", \"name\":\"Chris\", \"tenure\":10},"
            + "{\"email\":\"alex@example.com\",  \"name\":\"Alex\",  \"tenure\":3}"
            + "]");
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"chris@hitorro.com\"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"chris@hitorro.com\"}")
            ), docsType())
            .registerReferenceTable("users", ref, usersType())
            .build();
        var rows = run(engine.compile(
            "SELECT d.filename, u.name "
          + "FROM docs d "
          + "RIGHT JOIN users u ON d.author = u.email"));
        // 2 chris matches + 1 unmatched Alex = 3 rows
        assertThat(rows).hasSize(3);
        long chrisRows = rows.stream()
                .filter(r -> "Chris".equals(r.get("name").asText()))
                .count();
        long alexRows = rows.stream()
                .filter(r -> "Alex".equals(r.get("name").asText()))
                .count();
        assertThat(chrisRows).isEqualTo(2);
        assertThat(alexRows).isEqualTo(1);
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
    void refreshReferenceTable_picksUpNewFileContents(@TempDir Path tmpDir) throws Exception {
        // Reference file starts with one user, we query twice against a fresh stream,
        // then rewrite the file and refresh — third query sees the new contents.
        var refFile = writeUsersFile(tmpDir, "[{\"email\":\"a@x.com\",\"name\":\"OldA\",\"tenure\":1}]");
        var engine = JvsSqlEngine.builder()
            .registerReferenceTable("users", refFile, usersType(),
                    com.hitorro.jvssql.config.RefreshPolicy.onDemand())
            .registerStream("docs", stream(
                jvs("{\"filename\":\"one.pdf\",\"author\":\"a@x.com\"}")
            ), docsType())
            .build();
        var out1 = run(engine.compile("SELECT u.name FROM docs d JOIN users u ON d.author = u.email"));
        assertThat(out1.get(0).get("name").asText()).isEqualTo("OldA");

        // Rewrite the reference file and refresh.
        Files.writeString(java.nio.file.Path.of(refFile.getAbsolutePath()),
                "[{\"email\":\"a@x.com\",\"name\":\"NewA\",\"tenure\":99}]");
        engine.refreshReferenceTable("users");

        // Re-register stream (single-use), then re-query — should see NewA.
        var engine2 = JvsSqlEngine.builder()
            .registerReferenceTable("users", refFile, usersType(),
                    com.hitorro.jvssql.config.RefreshPolicy.onDemand())
            .registerStream("docs", stream(
                jvs("{\"filename\":\"two.pdf\",\"author\":\"a@x.com\"}")
            ), docsType())
            .build();
        var out2 = run(engine2.compile("SELECT u.name FROM docs d JOIN users u ON d.author = u.email"));
        assertThat(out2.get(0).get("name").asText()).isEqualTo("NewA");
        engine.close();
        engine2.close();
    }

    @Test
    void intervalJoin_streamXstreamWithinTimeBound() throws Exception {
        // orders and shipments streamed side-by-side; join if same order_id AND shipment
        // event_time is within 5 minutes of the order event_time.
        // We drive this as INTERVAL '5' MINUTE (300000 ms). Setup:
        //   order o1 at t=100
        //   order o2 at t=1_000_000
        //   ship  o1 at t=200        → matches (100 ± 300k)
        //   ship  o1 at t=400_000    → outside 5-min window, no match
        //   ship  o2 at t=1_000_500  → matches
        String ordersJson = "{\"name\":\"orders\",\"fields\":["
            + "{\"name\":\"order_id\",\"type\":\"core_string\"},"
            + "{\"name\":\"o_ts\",   \"type\":\"core_long\"}"
            + "]}";
        String shipmentsJson = "{\"name\":\"shipments\",\"fields\":["
            + "{\"name\":\"order_id\",\"type\":\"core_string\"},"
            + "{\"name\":\"s_ts\",   \"type\":\"core_long\"}"
            + "]}";
        var oType = new com.hitorro.jsontypesystem.Type(); oType.init(TestSupport.MAPPER.readTree(ordersJson));
        var sType = new com.hitorro.jsontypesystem.Type(); sType.init(TestSupport.MAPPER.readTree(shipmentsJson));

        var orders = List.of(
            new com.hitorro.jsontypesystem.JVS(TestSupport.MAPPER.readTree("{\"order_id\":\"o1\",\"o_ts\":100}")),
            new com.hitorro.jsontypesystem.JVS(TestSupport.MAPPER.readTree("{\"order_id\":\"o2\",\"o_ts\":1000000}"))
        ).iterator();
        var shipments = List.of(
            new com.hitorro.jsontypesystem.JVS(TestSupport.MAPPER.readTree("{\"order_id\":\"o1\",\"s_ts\":200}")),
            new com.hitorro.jsontypesystem.JVS(TestSupport.MAPPER.readTree("{\"order_id\":\"o1\",\"s_ts\":400000}")),
            new com.hitorro.jsontypesystem.JVS(TestSupport.MAPPER.readTree("{\"order_id\":\"o2\",\"s_ts\":1000500}"))
        ).iterator();

        var engine = JvsSqlEngine.builder()
            .registerStream("orders", orders, oType)
            .registerStream("shipments", shipments, sType)
            .build();

        // 300000 ms = 5 minutes tolerance
        var rows = run(engine.compile(
            "SELECT o.order_id, o.o_ts, s.s_ts "
          + "FROM   orders o JOIN shipments s "
          + "  ON   o.order_id = s.order_id "
          + "  AND  s.s_ts BETWEEN o.o_ts - 300000 AND o.o_ts + 300000 "
          + "ORDER BY o.o_ts, s.s_ts"));

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("order_id").asText()).isEqualTo("o1");
        assertThat(rows.get(0).get("s_ts").asLong()).isEqualTo(200);
        assertThat(rows.get(1).get("order_id").asText()).isEqualTo("o2");
        assertThat(rows.get(1).get("s_ts").asLong()).isEqualTo(1_000_500);
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
