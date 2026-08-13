/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jvssql.JvsSqlEngine;

import static com.hitorro.jvssql.examples.ExampleSupport.docsType;
import static com.hitorro.jvssql.examples.ExampleSupport.jvs;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;
import static com.hitorro.jvssql.examples.ExampleSupport.stream;

/**
 * Register a Java class as a scalar SQL function. The class must have a
 * public no-arg constructor and exactly one public {@code eval(...)}
 * method. Argument types + return type drive Calcite's type checking.
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example06_JavaUdf"}</p>
 */
public final class Example06_JavaUdf {

    /** A scalar UDF: normalize an email address (lowercase, trim). */
    public static class NormalizeEmail {
        public String eval(String s) {
            if (s == null) return null;
            return s.trim().toLowerCase().replaceAll("\\s+", "");
        }
    }

    /** A scalar UDF: FPHash-style fingerprint of a string. */
    public static class Fingerprint {
        public long eval(String s) {
            if (s == null) return 0L;
            long h = 1469598103934665603L;
            for (int i = 0; i < s.length(); i++) h = (h ^ s.charAt(i)) * 1099511628211L;
            return Math.abs(h);
        }
    }

    public static void main(String[] args) throws Exception {
        var engine = JvsSqlEngine.builder()
            .registerFunction("NORMALIZE_EMAIL", NormalizeEmail.class)
            .registerFunction("FPRINT", Fingerprint.class)
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"  Chris@Hitorro.COM  \"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"alex@example.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"chris @ hitorro . com\"}")
            ), docsType()).build();

        printQuery("normalize + fingerprint author emails",
            engine.compile(
                "SELECT filename, "
              + "       NORMALIZE_EMAIL(author) AS normalized, "
              + "       FPRINT(NORMALIZE_EMAIL(author)) AS fingerprint "
              + "FROM docs"));

        // UDFs compose naturally with predicates too:
        var engine2 = JvsSqlEngine.builder()
            .registerFunction("NORMALIZE_EMAIL", NormalizeEmail.class)
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"author\":\"  Chris@Hitorro.COM  \"}"),
                jvs("{\"filename\":\"b.pdf\", \"author\":\"alex@example.com\"}"),
                jvs("{\"filename\":\"c.pdf\", \"author\":\"chris @ hitorro . com\"}")
            ), docsType()).build();
        printQuery("filter by normalized email",
            engine2.compile(
                "SELECT filename FROM docs WHERE NORMALIZE_EMAIL(author) LIKE '%@hitorro.com'"));
    }
}
