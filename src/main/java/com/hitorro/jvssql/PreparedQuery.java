/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.hitorro.jvssql.exec.Executor;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.apache.calcite.rel.RelNode;

import java.util.Iterator;

/**
 * A compiled SQL query. Reusable: call {@link #asIterator()} or {@link #execute(Sink)}
 * to run it against the streams and reference tables registered on the owning engine.
 *
 * <p>Both APIs wrap the same underlying operator pipeline. Executing more than once
 * against a single-consumption stream will fail on the second call — most true stream
 * sources can only be read once. Batch iterators that support re-iteration are the
 * responsibility of the caller (wrap in a caching iterator upstream).</p>
 */
public final class PreparedQuery {

    private final JvsSqlEngine engine;
    private final String sql;
    private final RelNode plan;

    PreparedQuery(JvsSqlEngine engine, String sql, RelNode plan) {
        this.engine = engine;
        this.sql = sql;
        this.plan = plan;
    }

    public String sql() { return sql; }

    /** Pull results one at a time. Backpressure = natural: the engine stalls when the consumer is slow. */
    public AbstractIterator<JsonNode> asIterator() {
        Iterator<JsonNode> raw = new Executor(plan, engine.functions()).execute();
        return new IteratorAdapter(raw);
    }

    /**
     * Push results into the caller-supplied sink until the plan is exhausted.
     * Handles {@link Sink#start()} / {@link Sink#stop()} lifecycle.
     */
    public void execute(Sink<JsonNode> sink) {
        try {
            sink.init(JsonNodeFactory.instance.objectNode());
            sink.start();
            Iterator<JsonNode> it = new Executor(plan, engine.functions()).execute();
            while (it.hasNext()) {
                sink.accept(it.next());
            }
            sink.stop();
        } catch (Exception e) {
            throw new JvsSqlException("execute() failed: " + sql, e);
        }
    }

    // Bridges a plain Iterator<JsonNode> into the AbstractIterator required by the
    // hitorro-streams pipeline framework.
    private static final class IteratorAdapter extends AbstractIterator<JsonNode> {
        private final Iterator<JsonNode> src;
        IteratorAdapter(Iterator<JsonNode> src) { this.src = src; }
        @Override public boolean hasNext() { return src.hasNext(); }
        @Override public JsonNode next()  { return src.next(); }
    }
}
