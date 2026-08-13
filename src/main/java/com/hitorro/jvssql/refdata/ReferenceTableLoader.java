/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.refdata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.util.basefile.fs.BaseFile;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads a reference table's rows from a {@link BaseFile}. Supports two on-disk
 * shapes automatically — chosen by the first non-whitespace character:
 * <ul>
 *   <li><b>JSON array</b> — {@code [ {...}, {...}, ... ]}</li>
 *   <li><b>NDJSON</b> — one JSON object per line: {@code {...}\n{...}\n...}</li>
 * </ul>
 *
 * <p>Each element becomes a {@link JVS} row. Order is preserved.</p>
 *
 * <p>Phase 1 loads the whole file into memory at engine-build time. For very
 * large reference tables a follow-up can add on-demand hash-index building
 * that streams the file, but the common case (dimension tables sized in the
 * thousands, not millions) fits comfortably in RAM.</p>
 */
public final class ReferenceTableLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonFactory FACTORY = MAPPER.getFactory();

    private ReferenceTableLoader() {}

    /** Read all rows from {@code source}. Detects JSON array vs NDJSON. */
    public static List<JVS> load(BaseFile source) {
        if (!source.exists()) {
            throw new JvsSqlException("reference table file not found: " + source.getAbsolutePath());
        }
        try (InputStream in = source.getInputStream()) {
            byte[] all = in.readAllBytes();
            int i = 0;
            while (i < all.length && Character.isWhitespace(all[i])) i++;
            boolean isArray = i < all.length && all[i] == '[';
            return isArray ? readArray(all) : readNdjson(all);
        } catch (Exception e) {
            throw new JvsSqlException("failed to load reference table from "
                    + source.getAbsolutePath() + ": " + e.getMessage(), e);
        }
    }

    private static List<JVS> readArray(byte[] bytes) throws Exception {
        JsonNode arr = MAPPER.readTree(bytes);
        if (!arr.isArray()) {
            throw new JvsSqlException("expected JSON array at top level, got " + arr.getNodeType());
        }
        List<JVS> out = new ArrayList<>(arr.size());
        for (JsonNode row : arr) out.add(new JVS(row));
        return out;
    }

    private static List<JVS> readNdjson(byte[] bytes) throws Exception {
        // Jackson's MappingIterator streams JSON values back-to-back.
        List<JVS> out = new ArrayList<>();
        try (JsonParser parser = FACTORY.createParser(bytes);
             MappingIterator<JsonNode> it = MAPPER.readValues(parser, JsonNode.class)) {
            while (it.hasNext()) {
                JsonNode node = it.next();
                // Skip blank lines / non-object elements silently — user probably meant to filter these out.
                if (node != null && !node.isNull()) out.add(new JVS(node));
            }
        } catch (Exception e) {
            // If the parser hit EOF cleanly it may still throw during close on trailing whitespace — swallow.
            if (out.isEmpty()) throw e;
        }
        return out;
    }
}
