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
 * The two JVS-specific SQL scalars:
 * <ul>
 *   <li>{@code JPATH('some.dotted.path')} — escape hatch for reading fields
 *       that aren't declared on the JVS Type</li>
 *   <li>{@code MLS(content, 'en')} — first-class accessor for a multi-language
 *       string envelope (JVS's {@code core_mls} type)</li>
 * </ul>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example05_DynamicAndMls"}</p>
 */
public final class Example05_DynamicAndMls {

    public static void main(String[] args) throws Exception {
        // JPATH — reach undeclared fields by dotted path.
        var engine = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"a.pdf\", \"metadata\":{\"exp\":\"yes\", \"ttl\":42}}"),
                jvs("{\"filename\":\"b.pdf\", \"metadata\":{\"exp\":\"no\",  \"ttl\":7}}"),
                jvs("{\"filename\":\"c.pdf\"}")  // no metadata at all
            ), docsType()).build();
        printQuery("read undeclared metadata via JPATH()",
            engine.compile(
                "SELECT filename, "
              + "       JPATH('metadata.exp') AS experimental, "
              + "       JPATH('metadata.ttl') AS ttl "
              + "FROM docs"));

        // MLS — pull the requested language out of an mls envelope.
        var engine2 = JvsSqlEngine.builder()
            .registerStream("docs", stream(
                jvs("{\"filename\":\"welcome.md\", \"content\":{\"mls\":["
                    + "{\"lang\":\"en\",\"text\":\"Hello world\"},"
                    + "{\"lang\":\"fr\",\"text\":\"Bonjour le monde\"},"
                    + "{\"lang\":\"de\",\"text\":\"Hallo Welt\"}"
                    + "]}}"),
                jvs("{\"filename\":\"goodbye.md\", \"content\":{\"mls\":["
                    + "{\"lang\":\"en\",\"text\":\"Goodbye\"},"
                    + "{\"lang\":\"es\",\"text\":\"Adios\"}"
                    + "]}}")
            ), docsType()).build();
        printQuery("MLS accessor with fallback via COALESCE",
            engine2.compile(
                "SELECT filename, "
              + "       MLS(content, 'en') AS body_en, "
              + "       MLS(content, 'fr') AS body_fr, "
              + "       COALESCE(MLS(content, 'fr'), MLS(content, 'en'), '(missing)') AS fr_or_en "
              + "FROM docs"));
    }
}
