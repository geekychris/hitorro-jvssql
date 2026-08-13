/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;

import java.util.List;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;

/**
 * Two Phase 2 add-ons combined:
 * <ul>
 *   <li>Groovy scalar UDFs — inline scripts that become SQL functions</li>
 *   <li>{@code WIN_STRUCT(event_time, size)} — nested {"start":…, "end":…} object
 *       for tumbling-window results (matches the shape the engine promised
 *       for streaming aggregation output)</li>
 * </ul>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example12_GroovyAndNestedWindow"}</p>
 */
public final class Example12_GroovyAndNestedWindow {

    private static Type eventsType() throws Exception {
        String typeJson = "{\"name\":\"events\",\"fields\":["
            + "{\"name\":\"dept\",\"type\":\"core_string\"},"
            + "{\"name\":\"file_size\",\"type\":\"core_long\"},"
            + "{\"name\":\"event_time\",\"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS ev(long ts, String dept, long size) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"dept\":\"" + dept + "\",\"file_size\":" + size + ",\"event_time\":" + ts + "}"));
    }

    public static void main(String[] args) throws Exception {
        // Groovy scalar — normalize an email address inline
        var engine = JvsSqlEngine.builder()
            .registerGroovyFunction("NORMALIZE_EMAIL", 1,
                "arg1.toString().trim().toLowerCase().replaceAll(/\\s+/, '')")
            .registerGroovyFunction("HAS_DOMAIN", 2,
                "arg1.toString().endsWith('@' + arg2.toString())")
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\" Chris@Hitorro.COM \"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"alex@example.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"pat @ hitorro . com\"}")
            ), docsType()).build();

        // Groovy scalars return ANY at the SQL level, so combine them with LIKE/=
        // when you need a WHERE clause; Calcite can't type-check an ANY as boolean.
        printQuery("Groovy inline UDFs applied in projection + WHERE",
            engine.compile(
                "SELECT filename, "
              + "       NORMALIZE_EMAIL(author) AS clean_email, "
              + "       HAS_DOMAIN(NORMALIZE_EMAIL(author), 'hitorro.com') AS is_internal "
              + "FROM docs "
              + "WHERE NORMALIZE_EMAIL(author) LIKE '%@hitorro.com'"));

        // WIN_STRUCT — nested window bounds in the result
        long hourMs = 3_600_000L;
        var engine2 = JvsSqlEngine.builder()
            .registerStream("events", List.of(
                ev(15 * 60_000L, "eng",   100),
                ev(45 * 60_000L, "eng",   200),
                ev(75 * 60_000L, "sales", 300),
                ev(90 * 60_000L, "sales", 400),
                ev(130 * 60_000L, "eng",  500)
            ).iterator(), eventsType()).build();

        printQuery("Nested {\"window\":{\"start\":..., \"end\":...}} result via WIN_STRUCT",
            engine2.compile(
                "SELECT WIN_STRUCT(event_time, " + hourMs + ") AS \"window\", "
              + "       dept, "
              + "       SUM(file_size) AS total, "
              + "       COUNT(*) AS n "
              + "FROM   events "
              + "GROUP BY WIN_STRUCT(event_time, " + hourMs + "), dept"));
    }
}
