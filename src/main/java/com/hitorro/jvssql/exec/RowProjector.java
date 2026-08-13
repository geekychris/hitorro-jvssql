/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.util.json.keys.propaccess.PAContext;
import com.hitorro.util.json.keys.propaccess.Propaccess;

/**
 * Reads a single top-level field of a JVS document by name.
 *
 * <p>Phase 1 handles bare identifiers (top-level fields) — Phase 1-late extends
 * this to dotted paths, {@code DYNAMIC('path')}, and {@code MLS(content, 'en')}
 * via a proper expression-evaluator built off Calcite's {@code RexNode}.</p>
 */
public final class RowProjector {

    private final String[] columnNames;

    public RowProjector(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] columnNames() { return columnNames; }

    /**
     * Read one column value from a JVS doc as a raw {@link JsonNode}, or {@code null}
     * if absent. Uses {@link Propaccess#get} with
     * {@link PAContext#NeverCreate} — reads must never mutate the input row.
     *
     * <p><b>Design decision:</b> earlier code passed {@link PAContext#AlwaysCreate},
     * which caused Propaccess to auto-create missing intermediate objects/arrays as
     * a side effect of a plain read. That was invisible in most tests (the returned
     * value is the same) but corrupts the source JVS for anyone else holding a
     * reference — including a caching iterator upstream. If the executor ever needs
     * write-through semantics, do it in an explicit setter, not here.</p>
     */
    public JsonNode read(JVS row, String columnName) {
        try {
            Propaccess p = new Propaccess(columnName);
            return p.get(row, row.getJsonNode(), PAContext.NeverCreate);
        } catch (Exception e) {
            return null;
        }
    }
}
