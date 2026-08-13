/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.core.AggregateCall;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Streaming aggregate operator — maintains per-window state and emits closed
 * windows as soon as the watermark advances past their end. Consumers pulling
 * from the resulting iterator see rows for completed windows immediately,
 * without waiting for the source to be fully drained.
 *
 * <p>Used automatically when a query's GROUP BY contains a
 * {@code WIN_START(event_time, size)} column and the underlying table is a
 * registered streaming source. Falls back to the batch hash-aggregate for
 * pure batch queries.</p>
 *
 * <h3>Watermark model</h3>
 * <p>We approximate the watermark from observed data: {@code watermark ≈
 * maxObservedWindowStart}. A window with {@code window_start = ws} is
 * closable when {@code ws + windowSize + allowedLateness ≤
 * maxObservedWindowStart}. This works for well-formed streams whose event
 * times only cross window boundaries occasionally, and matches the semantics
 * of {@link WatermarkFilter} which drops out-of-order records outside the
 * allowed lateness before they reach the aggregate.</p>
 *
 * <h3>Memory bound</h3>
 * <p>State holds only currently-open windows — {@code O(active_windows × distinct_group_keys_per_window)}
 * — vs the batch aggregate's {@code O(total_windows × distinct_group_keys)}.
 * For a query grouping by (hour, dept) over a day of events, memory drops
 * from {@code 24 × depts} groups to typically {@code 1–2 × depts}.</p>
 */
public final class StreamingAggregate implements Iterator<JsonNode> {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private final Iterator<JsonNode> upstream;
    private final int[] groupCols;
    private final int windowStartGroupIdx;   // position within groupCols of the window_start column
    private final long windowSize;
    private final long allowedLateness;
    private final Function<AggregateCall, AggregateFn> aggResolver;
    private final List<AggregateCall> aggCalls;
    private final List<String> outNames;

    /** window_start -> (group_key -> state). Sorted so we can find closable windows in O(log n). */
    private final NavigableMap<Long, Map<List<JsonNode>, GroupState>> perWindow = new TreeMap<>();
    private final Deque<JsonNode> emitQueue = new ArrayDeque<>();
    private long maxObservedWindowStart = Long.MIN_VALUE;
    private boolean flushed;

    public StreamingAggregate(Iterator<JsonNode> upstream,
                              int[] groupCols,
                              int windowStartGroupIdx,
                              long windowSize,
                              long allowedLateness,
                              List<AggregateCall> aggCalls,
                              List<String> outNames,
                              Function<AggregateCall, AggregateFn> aggResolver) {
        this.upstream = upstream;
        this.groupCols = groupCols;
        this.windowStartGroupIdx = windowStartGroupIdx;
        this.windowSize = windowSize;
        this.allowedLateness = allowedLateness;
        this.aggCalls = aggCalls;
        this.outNames = outNames;
        this.aggResolver = aggResolver;
    }

    @Override public boolean hasNext() {
        while (emitQueue.isEmpty()) {
            if (upstream.hasNext()) {
                JsonNode row = upstream.next();
                accumulateOne(row);
                closeReadyWindows();
            } else if (!flushed) {
                flushAllWindows();
                flushed = true;
                if (emitQueue.isEmpty()) return false;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override public JsonNode next() {
        if (!hasNext()) throw new NoSuchElementException();
        return emitQueue.poll();
    }

    // -- state maintenance ---------------------------------------------------

    private void accumulateOne(JsonNode row) {
        List<JsonNode> key = new ArrayList<>(groupCols.length);
        for (int c : groupCols) key.add(nthField(row, c));
        JsonNode wsNode = key.get(windowStartGroupIdx);
        if (wsNode == null || wsNode.isNull() || !wsNode.isNumber()) {
            // Row with no window — skip. (Shouldn't happen if WIN_START is well-defined.)
            return;
        }
        long ws = wsNode.asLong();
        maxObservedWindowStart = Math.max(maxObservedWindowStart, ws);

        Map<List<JsonNode>, GroupState> forThisWindow =
                perWindow.computeIfAbsent(ws, k -> new LinkedHashMap<>());
        GroupState g = forThisWindow.computeIfAbsent(key, k -> new GroupState(aggCalls, aggResolver));
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall ac = aggCalls.get(i);
            Object argValue = ac.getArgList().isEmpty()
                    ? Boolean.TRUE
                    : RexEvaluator.unwrap(nthField(row, ac.getArgList().get(0)));
            g.fns[i].accumulate(g.accs[i], argValue);
        }
    }

    private void closeReadyWindows() {
        if (maxObservedWindowStart == Long.MIN_VALUE) return;
        // A window at ws is closable when ws + windowSize + allowedLateness ≤ maxObservedWindowStart.
        // Equivalently ws ≤ maxObservedWindowStart - windowSize - allowedLateness.
        long cutoff = maxObservedWindowStart - windowSize - allowedLateness;
        var it = perWindow.headMap(cutoff, true).entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            emitWindow(entry.getKey(), entry.getValue());
            it.remove();
        }
    }

    private void flushAllWindows() {
        for (var entry : perWindow.entrySet()) {
            emitWindow(entry.getKey(), entry.getValue());
        }
        perWindow.clear();
    }

    private void emitWindow(long windowStart, Map<List<JsonNode>, GroupState> groups) {
        for (var e : groups.entrySet()) {
            ObjectNode row = F.objectNode();
            List<JsonNode> keyTuple = e.getKey();
            for (int i = 0; i < groupCols.length; i++) {
                JsonNode v = keyTuple.get(i);
                row.set(outNames.get(i), v == null ? F.nullNode() : v);
            }
            GroupState g = e.getValue();
            for (int i = 0; i < aggCalls.size(); i++) {
                Object v = g.fns[i].result(g.accs[i]);
                row.set(outNames.get(groupCols.length + i), RexEvaluator.wrap(v));
            }
            emitQueue.add(row);
        }
    }

    private static JsonNode nthField(JsonNode row, int col) {
        int j = 0;
        for (Iterator<String> it = row.fieldNames(); it.hasNext(); ) {
            String n = it.next();
            if (j++ == col) return row.get(n);
        }
        return null;
    }

    private static final class GroupState {
        final AggregateFn[] fns;
        final Object[] accs;
        GroupState(List<AggregateCall> calls, Function<AggregateCall, AggregateFn> resolver) {
            fns = new AggregateFn[calls.size()];
            accs = new Object[calls.size()];
            for (int i = 0; i < calls.size(); i++) {
                fns[i] = resolver.apply(calls.get(i));
                accs[i] = fns[i].createAccumulator();
            }
        }
    }
}
