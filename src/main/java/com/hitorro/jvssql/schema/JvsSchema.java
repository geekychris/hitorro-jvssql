/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.schema;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Calcite {@link org.apache.calcite.schema.Schema} that holds all streams and
 * reference tables registered with the engine. Each entry is a {@link JvsTable}.
 *
 * <p>Table names are the identifier a caller uses in SQL:
 * {@code SELECT ... FROM docs} → the {@code JvsTable} registered as {@code "docs"}.</p>
 */
public final class JvsSchema extends AbstractSchema {

    private final Map<String, Table> tables = new LinkedHashMap<>();

    public void addTable(String name, JvsTable table) {
        if (tables.putIfAbsent(name, table) != null) {
            throw new IllegalArgumentException("table already registered: " + name);
        }
    }

    public boolean hasTable(String name) { return tables.containsKey(name); }

    public JvsTable getJvsTable(String name) {
        Table t = tables.get(name);
        return t instanceof JvsTable jt ? jt : null;
    }

    @Override
    protected Map<String, Table> getTableMap() { return tables; }
}
