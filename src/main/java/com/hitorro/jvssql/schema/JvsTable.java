/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.schema;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Calcite {@link org.apache.calcite.schema.Table} backed by an
 * {@code Iterator<JVS>}. Its row type is computed from the JVS
 * {@link Type} it was registered with, and its rows are drawn lazily from
 * the {@link Supplier} handed in at registration.
 *
 * <p>The supplier is invoked each time Calcite requests iteration — this lets
 * a stream be re-read for {@code EXPLAIN} / multi-execution scenarios if the
 * supplier is capable of restarting (e.g. a re-openable file source). Most
 * true stream sources will supply the same iterator once; second-execution
 * behaviour is caller-defined.</p>
 */
public final class JvsTable extends AbstractTable {

    private final String name;
    private final Type jvsType;
    private final StreamConfig streamConfig;
    private final Supplier<Iterator<JVS>> source;
    private final boolean isReferenceTable;
    /** Materialized rows for reference tables — supports re-iteration across query executions. */
    private volatile java.util.List<JVS> referenceRows;

    public JvsTable(String name, Type jvsType, StreamConfig streamConfig,
                    Supplier<Iterator<JVS>> source, boolean isReferenceTable) {
        this.name = name;
        this.jvsType = jvsType;
        this.streamConfig = streamConfig;
        this.source = source;
        this.isReferenceTable = isReferenceTable;
    }

    public String getName() { return name; }
    public Type jvsType() { return jvsType; }
    public StreamConfig streamConfig() { return streamConfig; }
    public boolean isReferenceTable() { return isReferenceTable; }

    /** Direct access to the materialized rows of a reference table (for HashJoin build phase). */
    public java.util.List<JVS> referenceRows() { return referenceRows; }

    /** Called at engine build time for reference tables — swaps in the loaded snapshot atomically. */
    public void setReferenceRows(java.util.List<JVS> rows) { this.referenceRows = rows; }

    /**
     * Fresh iterator from the source. Reference tables re-iterate their loaded snapshot
     * on every call; streaming sources consume the underlying supplier once.
     */
    public Iterator<JVS> openIterator() {
        if (isReferenceTable && referenceRows != null) return referenceRows.iterator();
        return source.get();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return TypeToRelDataType.asRowType(jvsType, typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        // Phase 1: no cardinality estimate. Cost-based optimizer uses defaults.
        // Reference tables can get richer stats in Phase 1-late once we have real row counts.
        return Statistics.UNKNOWN;
    }
}
