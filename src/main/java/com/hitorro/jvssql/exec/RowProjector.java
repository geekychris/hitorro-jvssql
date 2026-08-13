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
     * if absent. Uses {@link Propaccess#get} to navigate top-level and dotted paths.
     */
    public JsonNode read(JVS row, String columnName) {
        try {
            Propaccess p = new Propaccess(columnName);
            return p.get(row, row.getJsonNode(), PAContext.AlwaysCreate);
        } catch (Exception e) {
            return null;
        }
    }
}
