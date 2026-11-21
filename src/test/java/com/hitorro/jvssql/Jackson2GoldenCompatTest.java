/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.PreparedQuery;
import com.hitorro.jvssql.exec.ExternalMergeSort;
import com.hitorro.jvssql.refdata.ReferenceTableLoader;
import com.hitorro.jvssql.source.SessionWindows;
import com.hitorro.jvssql.source.StreamSources;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.file.FileFileSystem;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Pins what hitorro-jvssql produces and accepts to 3.0.2 on Jackson 2: query result rows (JSON text
 * and the node type of every value) for 33 queries over clean and wrongly typed data, the external
 * sort's spill files, the NDJSON / JSON-array / auto-detect stream sources and the reference-table
 * loader on valid and invalid input, and the session-window operators.
 *
 * <p>{@code jackson2-golden.txt} was produced by running this same program against the 3.0.2 sources
 * on Jackson 2.18 (only the package of the Jackson imports and the mapper differed).</p>
 */
class Jackson2GoldenCompatTest {
  static ObjectMapper M;
  static StringBuilder OUT = new StringBuilder();

  static void p(String k, Object v) { OUT.append(k).append('=').append(v).append('\n'); }

  /** Typed rendering of a tree: node type and number type of every value. */
  static String sig(JsonNode n) {
    if (n == null) return "<java-null>";
    if (n.isObject()) {
      StringBuilder b = new StringBuilder("{");
      for (Map.Entry<String, JsonNode> e : n.properties()) {
        if (b.length() > 1) b.append(',');
        b.append(e.getKey()).append(':').append(sig(e.getValue()));
      }
      return b.append('}').toString();
    }
    if (n.isArray()) {
      StringBuilder b = new StringBuilder("[");
      for (JsonNode c : n) { if (b.length() > 1) b.append(','); b.append(sig(c)); }
      return b.append(']').toString();
    }
    if (n.isNumber()) return n.numberType() + "(" + n.toString() + ")";
    return n.getNodeType() + "(" + n.toString() + ")";
  }

  static String err(Throwable t) {
    StringBuilder b = new StringBuilder("!");
    b.append(t.getClass().getSimpleName());
    return b.toString();
  }

  static Type type(String json) throws Exception { Type t = new Type(); t.init(M.readTree(json)); return t; }
  static JVS jvs(String json) throws Exception { return new JVS(M.readTree(json)); }

  static final String DOCS_TYPE = "{\"name\":\"docs\",\"fields\":["
      + "{\"name\":\"filename\",\"type\":\"core_string\"},"
      + "{\"name\":\"classification\",\"type\":\"core_string\"},"
      + "{\"name\":\"file_size\",\"type\":\"core_long\"},"
      + "{\"name\":\"dept\",\"type\":\"core_string\"},"
      + "{\"name\":\"author\",\"type\":\"core_string\"},"
      + "{\"name\":\"content\",\"type\":\"core_string\"},"
      + "{\"name\":\"score\",\"type\":\"core_double\"},"
      + "{\"name\":\"event_time\",\"type\":\"core_long\"}"
      + "]}";

  static final String[] DOCS = {
    "{\"filename\":\"a.pdf\",\"classification\":\"public\",\"file_size\":100,\"dept\":\"eng\",\"author\":\"x@h.com\",\"score\":1.5,\"event_time\":0,\"metadata\":{\"experimental\":\"yes\",\"n\":3}}",
    "{\"filename\":\"b.txt\",\"classification\":\"internal\",\"file_size\":250,\"dept\":\"eng\",\"author\":null,\"score\":2.25,\"event_time\":900000,\"metadata\":{\"experimental\":\"no\"}}",
    "{\"filename\":\"c.pdf\",\"classification\":\"restricted\",\"file_size\":\"300\",\"dept\":\"sales\",\"score\":\"7\",\"event_time\":2700000}",
    "{\"filename\":\"d.doc\",\"classification\":\"public\",\"file_size\":3.7,\"dept\":\"sales\",\"author\":\"y@h.com\",\"score\":1.10,\"event_time\":3900000,\"tags\":[\"t1\",\"t2\"]}",
    "{\"filename\":\"e.pdf\",\"file_size\":12345678901,\"dept\":\"ops\",\"author\":\"x@h.com\",\"score\":1e3,\"event_time\":7800000,\"content\":{\"mls\":[{\"lang\":\"en\",\"text\":\"hello\"},{\"lang\":\"fr\",\"text\":\"bonjour\"}]}}",
    "{\"filename\":\"f\",\"classification\":true,\"file_size\":null,\"dept\":\"ops\",\"score\":null,\"event_time\":\"1970-01-01T02:30:00Z\"}",
    "{\"filename\":7,\"file_size\":\"abc\",\"dept\":{\"x\":1},\"score\":12345678901234567890,\"event_time\":7900000}",
    "{\"filename\":\"g.pdf\",\"classification\":\"internal\",\"file_size\":0,\"dept\":\"eng\",\"score\":-0.0,\"event_time\":8000000,\"metadata\":{\"experimental\":\"yes\"}}"
  };

  static final String[] QUERIES = {
    "SELECT * FROM docs",
    "SELECT filename, classification, file_size, dept, author, score, event_time FROM docs",
    "SELECT filename, file_size + 5 AS added, file_size * 2 AS doubled, file_size / 4 AS quarter, MOD(file_size, 3) AS remainder FROM docs",
    "SELECT filename, score * 2 AS s2, score + 0.1 AS s3, -score AS neg FROM docs",
    "SELECT filename FROM docs WHERE file_size > 100",
    "SELECT filename FROM docs WHERE file_size BETWEEN 100 AND 300",
    "SELECT filename FROM docs WHERE filename LIKE '%.pdf'",
    "SELECT filename FROM docs WHERE classification IN ('public', 'internal')",
    "SELECT filename FROM docs WHERE author IS NULL",
    "SELECT filename, COALESCE(author, 'anonymous') AS who FROM docs",
    "SELECT filename, CASE WHEN file_size > 200 THEN 'big' ELSE 'small' END AS sz FROM docs",
    "SELECT filename, NULLIF(dept, 'eng') AS d FROM docs",
    "SELECT UPPER(filename) AS u, LOWER(filename) AS l, TRIM(filename) AS t FROM docs",
    "SELECT ABS(-7.5) AS a, ROUND(2.4) AS r, CEIL(2.1) AS c, FLOOR(2.9) AS f FROM docs",
    "SELECT filename, metadata.experimental AS x FROM docs",
    "SELECT filename, JPATH('metadata.n') AS n, JPATH('tags') AS tags, JPATH('dept') AS d FROM docs",
    "SELECT filename, MLS(content, 'en') AS en, MLS(content, 'fr') AS fr FROM docs",
    "SELECT filename, HASH64(filename) AS h FROM docs",
    "SELECT dept, COUNT(*) AS n, SUM(file_size) AS total, MIN(file_size) AS lo, MAX(file_size) AS hi, AVG(file_size) AS av FROM docs GROUP BY dept",
    "SELECT dept, SUM(score) AS s, AVG(score) AS a, MIN(score) AS lo, MAX(score) AS hi FROM docs GROUP BY dept",
    "SELECT dept, COUNT(DISTINCT author) AS da, COUNT(*) AS total FROM docs GROUP BY dept",
    "SELECT classification, COUNT(*) AS n FROM docs GROUP BY classification",
    "SELECT filename FROM docs ORDER BY file_size",
    "SELECT filename FROM docs ORDER BY file_size DESC",
    "SELECT filename, score FROM docs ORDER BY score",
    "SELECT filename, dept FROM docs ORDER BY dept, filename",
    "SELECT filename FROM docs ORDER BY classification DESC, filename LIMIT 3 OFFSET 1",
    "SELECT dept, WIN_START(event_time, 3600000) AS ws, WIN_END(event_time, 3600000) AS we, COUNT(*) AS n FROM docs GROUP BY WIN_START(event_time, 3600000), WIN_END(event_time, 3600000), dept",
    "SELECT WIN_STRUCT(event_time, 3600000) AS \"window\", dept, COUNT(*) AS n FROM docs GROUP BY WIN_STRUCT(event_time, 3600000), dept",
    "SELECT filename, WIN_HOP_STARTS(event_time, 3600000, 1800000) AS starts FROM docs",
    "SELECT filename FROM docs UNION SELECT dept FROM docs",
    "SELECT d.filename, u.name, u.tenure FROM docs d JOIN users u ON d.author = u.email",
    "SELECT d.filename, u.name FROM docs d LEFT JOIN users u ON d.author = u.email"
  };

  static final String USERS_TYPE = "{\"name\":\"users\",\"fields\":["
      + "{\"name\":\"email\",\"type\":\"core_string\"},"
      + "{\"name\":\"name\",\"type\":\"core_string\"},"
      + "{\"name\":\"tenure\",\"type\":\"core_long\"}]}";

  static final int[] CLEAN = {0, 1, 4, 7};

  static List<JVS> docs(boolean clean) throws Exception {
    List<JVS> l = new ArrayList<>();
    if (clean) { for (int i : CLEAN) l.add(jvs(DOCS[i])); }
    else { for (String d : DOCS) l.add(jvs(d)); }
    return l;
  }

  static BaseFile bf(Path p) { return FileFileSystem.Root.getFile(p.toAbsolutePath().toString()); }

  static String drainJvs(Iterator<JVS> it) {
    StringBuilder b = new StringBuilder();
    try {
      while (it.hasNext()) b.append(sig(it.next().getJsonNode())).append(';');
    } catch (Throwable t) { b.append(err(t)); }
    return b.toString();
  }

  @Test
  void outputMatchesJackson2(@TempDir Path dir) throws Exception {
    Map<String, String> golden = new LinkedHashMap<>();
    try (InputStream in = getClass().getResourceAsStream("jackson2-golden.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    Map<String, String> actual = new LinkedHashMap<>();
    for (String line : generate(dir).split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    List<String> diffs = new ArrayList<>();
    for (String k : golden.keySet()) {
      if (!Objects.equals(golden.get(k), actual.get(k))) diffs.add(k + "\n  jackson2: " + golden.get(k) + "\n  now:      " + actual.get(k));
    }
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden): " + actual.get(k));
    Assertions.assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    Assertions.assertEquals(774, golden.size());
  }

  static String generate(Path dir) throws Exception {
    M = com.hitorro.util.json.JsonMappers.json();
    OUT = new StringBuilder();
    Path users = dir.resolve("users.json");
    Files.writeString(users, "[{\"email\":\"x@h.com\",\"name\":\"Xa\",\"tenure\":5},{\"email\":\"y@h.com\",\"name\":\"Yo\",\"tenure\":2.5}]");

    for (int qi = 0; qi < QUERIES.length * 2; qi++) {
      boolean clean = qi < QUERIES.length;
      String k = (clean ? "clean.q" : "all.q") + (qi % QUERIES.length);
      p(k + ".sql", QUERIES[qi % QUERIES.length]);
      try {
        JvsSqlEngine eng = JvsSqlEngine.builder()
            .registerStream("docs", docs(clean).iterator(), type(DOCS_TYPE))
            .registerReferenceTable("users", bf(users), type(USERS_TYPE))
            .build();
        PreparedQuery q = eng.compile(QUERIES[qi % QUERIES.length]);
        Iterator<JsonNode> it = q.asIterator();
        int i = 0;
        while (it.hasNext()) {
          JsonNode r = it.next();
          p(k + ".row" + i + ".json", M.writeValueAsString(r));
          p(k + ".row" + i + ".sig", sig(r));
          i++;
        }
        p(k + ".rows", i);
      } catch (Throwable t) { p(k + ".error", err(t)); }
    }

    // External sort spill files: written and read back by ExternalMergeSort.
    Path spill = dir.resolve("spill");
    Files.createDirectories(spill);
    List<JsonNode> rows = new ArrayList<>();
    for (String d : DOCS) rows.add(M.readTree(d));
    Comparator<JsonNode> byName = Comparator.comparing(n -> n.get("filename").toString());
    Iterator<JsonNode> sorted = ExternalMergeSort.sort(rows.iterator(), byName, 3, bf(spill));
    List<Path> runs = new ArrayList<>();
    try (var s = Files.list(spill)) { s.forEach(runs::add); }
    runs.sort(Comparator.comparing(x -> x.getFileName().toString().replaceAll(".*-(\\d+)\\.ndjson$", "$1")));
    for (int i = 0; i < runs.size(); i++) p("spill.run" + i, Files.readString(runs.get(i)).replace("\n", "\\n"));
    StringBuilder sb = new StringBuilder();
    while (sorted.hasNext()) sb.append(sig(sorted.next())).append(';');
    p("spill.merged", sb);

    // Source readers.
    String[][] files = {
      {"ndjson.ok", "{\"a\":1}\n\n{\"a\":2.50,\"b\":\"x\"}\n   \n{\"a\":null}\n"},
      {"ndjson.trailing", "{\"a\":1} junk\n{\"a\":2}\n"},
      {"ndjson.twovalues", "{\"a\":1}{\"a\":2}\n"},
      {"ndjson.bad", "{\"a\":1}\n{\"a\":\n{\"a\":3}\n"},
      {"ndjson.scalar", "1\n\"s\"\nnull\n[1,2]\n"},
      {"array.ok", "[{\"a\":1},{\"a\":2.50},{\"a\":12345678901234567890}]"},
      {"array.trailing", "[{\"a\":1}] trailing"},
      {"array.notarray", "{\"a\":1}"},
      {"array.bad", "[{\"a\":1},"},
      {"array.empty", "[]"},
      {"empty", ""}
    };
    for (String[] f : files) {
      Path fp = dir.resolve(f[0] + ".txt");
      Files.writeString(fp, f[1]);
      try { p("ndjson." + f[0], drainJvs(StreamSources.ndjson(bf(fp)))); } catch (Throwable t) { p("ndjson." + f[0], err(t)); }
      try { p("jsonArray." + f[0], drainJvs(StreamSources.jsonArray(bf(fp)))); } catch (Throwable t) { p("jsonArray." + f[0], err(t)); }
      try { p("autoDetect." + f[0], drainJvs(StreamSources.autoDetect(bf(fp)))); } catch (Throwable t) { p("autoDetect." + f[0], err(t)); }
      for (String ext : new String[]{".json", ".ndjson", ".jsonl"}) {
        Path rp = dir.resolve(f[0] + ext);
        Files.writeString(rp, f[1]);
        try {
          StringBuilder b = new StringBuilder();
          for (JVS j : ReferenceTableLoader.load(bf(rp))) b.append(sig(j.getJsonNode())).append(';');
          p("refload" + ext + "." + f[0], b);
        } catch (Throwable t) { p("refload" + ext + "." + f[0], err(t)); }
      }
    }

    // Session windows.
    String[] evs = {
      "{\"user\":\"u1\",\"event_time\":1000,\"qty\":1}",
      "{\"user\":\"u1\",\"event_time\":5000,\"qty\":2}",
      "{\"user\":\"u2\",\"event_time\":\"1970-01-01T00:00:03Z\",\"qty\":3}",
      "{\"user\":\"u1\",\"event_time\":100000,\"qty\":4}",
      "{\"user\":7,\"event_time\":2000.9,\"qty\":5}",
      "{\"user\":null,\"event_time\":3000,\"qty\":6}"
    };
    for (String mode : new String[]{"sessionize", "sessionizeIncremental", "sessionizeNested"}) {
      List<JVS> l = new ArrayList<>();
      for (String e : evs) l.add(jvs(e));
      try {
        Iterator<JVS> it = switch (mode) {
          case "sessionize" -> SessionWindows.sessionize(l.iterator(), "user", "event_time", 30000);
          case "sessionizeIncremental" -> SessionWindows.sessionizeIncremental(l.iterator(), "user", "event_time", 30000);
          default -> SessionWindows.sessionizeNested(l.iterator(), "user", "event_time", 30000);
        };
        p("session." + mode, drainJvs(it));
      } catch (Throwable t) { p("session." + mode, err(t)); }
    }
    return OUT.toString();
  }
}
