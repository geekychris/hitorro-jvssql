/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.schema.JvsTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Executes a Calcite {@link RelNode} plan against registered streams, producing an
 * {@code Iterator<JsonNode>} of result rows.
 *
 * <p>Phase 1 supports:</p>
 * <ul>
 *   <li>{@link TableScan} — reads from a registered {@link JvsTable}</li>
 *   <li>{@link Filter} — WHERE (and HAVING when placed above Aggregate)</li>
 *   <li>{@link Project} — SELECT projection with expressions</li>
 *   <li>{@link Aggregate} — GROUP BY + COUNT/SUM/AVG/MIN/MAX + UDAF</li>
 *   <li>{@link Sort} — ORDER BY (in-memory; external-merge with spill is next)</li>
 *   <li>{@link Values} — VALUES (literal rows)</li>
 * </ul>
 *
 * <p>Expression evaluation goes through {@link RexEvaluator}, which handles
 * arithmetic, string/math built-ins, LIKE/IN/BETWEEN, CASE/COALESCE/NULLIF/CAST,
 * DYNAMIC(), MLS(), and any user-registered UDF.</p>
 */
public final class Executor {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private final RelNode plan;
    private final FunctionRegistry functions;
    private final com.hitorro.jvssql.config.EngineConfig engineConfig;
    private final java.util.Map<String, com.hitorro.util.core.iterator.sinks.Sink<JsonNode>> lateDataSinks;
    private final RexEvaluator rex;
    /** Per-execution watermark state; updated by WatermarkFilter, read by StreamingAggregate. */
    private final WatermarkTracker watermarkTracker = new WatermarkTracker();

    public Executor(RelNode plan, FunctionRegistry functions, com.hitorro.jvssql.config.EngineConfig engineConfig,
                    java.util.Map<String, com.hitorro.util.core.iterator.sinks.Sink<JsonNode>> lateDataSinks) {
        this(plan, functions, engineConfig, lateDataSinks, java.util.Map.of());
    }

    public Executor(RelNode plan, FunctionRegistry functions, com.hitorro.jvssql.config.EngineConfig engineConfig,
                    java.util.Map<String, com.hitorro.util.core.iterator.sinks.Sink<JsonNode>> lateDataSinks,
                    java.util.Map<Integer, Object> paramBindings) {
        this.plan = plan;
        this.functions = functions;
        this.engineConfig = engineConfig;
        this.lateDataSinks = lateDataSinks == null ? java.util.Map.of() : lateDataSinks;
        this.rex = new RexEvaluator(functions, paramBindings == null ? java.util.Map.of() : paramBindings);
    }

    public Iterator<JsonNode> execute() {
        return build(plan);
    }

    // -- plan walk ------------------------------------------------------------

    private Iterator<JsonNode> build(RelNode node) {
        if (node instanceof TableScan scan) return buildScan(scan);
        if (node instanceof Filter filter)  return buildFilter(filter);
        if (node instanceof Project proj)   return buildProject(proj);
        if (node instanceof Aggregate agg)  return buildAggregate(agg);
        if (node instanceof Sort sort)      return buildSort(sort);
        if (node instanceof Values vals)    return buildValues(vals);
        if (node instanceof Join join)      return buildJoin(join);
        if (node instanceof org.apache.calcite.rel.core.Union u)     return buildUnion(u);
        if (node instanceof org.apache.calcite.rel.core.Intersect i) return buildSetOp(i, false);
        if (node instanceof org.apache.calcite.rel.core.Minus m)     return buildSetOp(m, true);
        throw new JvsSqlException("Phase 1 does not yet support operator: "
                + node.getClass().getSimpleName());
    }

    // -- set operations -------------------------------------------------------

    private Iterator<JsonNode> buildUnion(org.apache.calcite.rel.core.Union union) {
        // UNION ALL: concat all inputs. UNION (all=false): concat + dedup.
        java.util.List<Iterator<JsonNode>> parts = new java.util.ArrayList<>();
        for (RelNode child : union.getInputs()) parts.add(build(child));
        Iterator<JsonNode> concat = new Iterator<>() {
            int idx = 0;
            @Override public boolean hasNext() {
                while (idx < parts.size() && !parts.get(idx).hasNext()) idx++;
                return idx < parts.size();
            }
            @Override public JsonNode next() {
                if (!hasNext()) throw new NoSuchElementException();
                return parts.get(idx).next();
            }
        };
        if (union.all) return concat;
        // UNION (dedup): materialize into a LinkedHashSet keyed by textual repr.
        java.util.LinkedHashMap<String, JsonNode> seen = new java.util.LinkedHashMap<>();
        while (concat.hasNext()) {
            JsonNode row = concat.next();
            seen.putIfAbsent(row.toString(), row);
        }
        return seen.values().iterator();
    }

    private Iterator<JsonNode> buildSetOp(org.apache.calcite.rel.core.SetOp op, boolean isMinus) {
        // INTERSECT / EXCEPT (ALL variants dedup ratios differ; we implement set semantics).
        java.util.LinkedHashMap<String, JsonNode> left = new java.util.LinkedHashMap<>();
        Iterator<JsonNode> l = build(op.getInputs().get(0));
        while (l.hasNext()) { JsonNode r = l.next(); left.putIfAbsent(r.toString(), r); }
        for (int i = 1; i < op.getInputs().size(); i++) {
            java.util.HashSet<String> rightKeys = new java.util.HashSet<>();
            Iterator<JsonNode> r = build(op.getInputs().get(i));
            while (r.hasNext()) rightKeys.add(r.next().toString());
            if (isMinus) {
                left.keySet().removeAll(rightKeys);   // EXCEPT: drop rows also in right
            } else {
                left.keySet().retainAll(rightKeys);   // INTERSECT: keep only shared
            }
        }
        return left.values().iterator();
    }

    // -- scan -----------------------------------------------------------------

    private Iterator<JsonNode> buildScan(TableScan scan) {
        JvsTable table = scan.getTable().unwrap(JvsTable.class);
        if (table == null) {
            throw new JvsSqlException("scan of non-JVS table: " + scan.getTable());
        }
        Iterator<JVS> src = table.openIterator();
        List<String> cols = scan.getRowType().getFieldNames();
        RowProjector projector = new RowProjector(cols.toArray(new String[0]));
        // Wrap with a watermark-aware source if the stream config declares an event-time field.
        // Records more than allowedLateness behind the current watermark are routed to the
        // late-data sink (if any) or silently dropped.
        var sc = table.streamConfig();
        if (sc != null && sc.isStreaming()) {
            com.hitorro.util.core.iterator.sinks.Sink<JsonNode> lateSink = lateDataSinks.get(table.getName());
            src = new WatermarkFilter(src, sc.eventTimeField(), sc.allowedLatenessMillis(),
                                       lateSink, watermarkTracker);
        }
        return new ScanIterator(src, projector, functions);
    }

    /**
     * Watermark-aware source wrapper. Maintains {@code maxEventTime - allowedLateness}
     * as the current watermark; drops (or side-outputs) records whose event time is
     * below the watermark.
     */
    private static final class WatermarkFilter implements Iterator<JVS> {
        private final Iterator<JVS> upstream;
        private final com.hitorro.util.json.keys.propaccess.Propaccess eventTimePath;
        private final long allowedLatenessMillis;
        private final com.hitorro.util.core.iterator.sinks.Sink<JsonNode> lateSink;
        private final WatermarkTracker tracker;
        private long maxObservedEventTime = Long.MIN_VALUE;
        private JVS nextRow;
        WatermarkFilter(Iterator<JVS> upstream, String eventTimeField, long allowedLatenessMillis,
                        com.hitorro.util.core.iterator.sinks.Sink<JsonNode> lateSink,
                        WatermarkTracker tracker) {
            this.upstream = upstream;
            this.eventTimePath = new com.hitorro.util.json.keys.propaccess.Propaccess(eventTimeField);
            this.allowedLatenessMillis = allowedLatenessMillis;
            this.lateSink = lateSink;
            this.tracker = tracker;
        }
        @Override public boolean hasNext() {
            while (nextRow == null && upstream.hasNext()) {
                JVS candidate = upstream.next();
                long ts = extractEventTime(candidate);
                long watermark = maxObservedEventTime == Long.MIN_VALUE
                        ? Long.MIN_VALUE : maxObservedEventTime - allowedLatenessMillis;
                if (ts < watermark) {
                    // Late record.
                    if (lateSink != null) {
                        try { lateSink.add(candidate.getJsonNode()); } catch (Exception ignored) {}
                    }
                    continue;
                }
                maxObservedEventTime = Math.max(maxObservedEventTime, ts);
                tracker.observe(ts);
                nextRow = candidate;
            }
            return nextRow != null;
        }
        @Override public JVS next() {
            if (!hasNext()) throw new NoSuchElementException();
            JVS out = nextRow; nextRow = null; return out;
        }
        private long extractEventTime(JVS row) {
            try {
                JsonNode v = eventTimePath.get(row, row.getJsonNode(),
                        com.hitorro.util.json.keys.propaccess.PAContext.AlwaysCreate);
                if (v == null || v.isNull()) return 0L;
                if (v.isNumber()) return v.asLong();
                return java.time.Instant.parse(v.asText()).toEpochMilli();
            } catch (Exception e) {
                return 0L;
            }
        }
    }

    private static final class ScanIterator implements Iterator<JsonNode> {
        private final Iterator<JVS> src;
        private final RowProjector projector;
        private final FunctionRegistry functions;
        ScanIterator(Iterator<JVS> src, RowProjector projector, FunctionRegistry functions) {
            this.src = src; this.projector = projector; this.functions = functions;
        }
        @Override public boolean hasNext() { return src.hasNext(); }
        @Override public JsonNode next() {
            if (!src.hasNext()) throw new NoSuchElementException();
            JVS row = src.next();
            // Bind so downstream operators can call JPATH/MLS. The binding stays live
            // through Filter/Project/Aggregate expression evaluation and is replaced on
            // the next Scan.next() call. Single-threaded pull semantics make this safe.
            functions.bindRow(row);
            ObjectNode out = F.objectNode();
            for (String col : projector.columnNames()) {
                JsonNode v = projector.read(row, col);
                out.set(col, v == null ? F.nullNode() : v);
            }
            return out;
        }
    }

    // -- filter ---------------------------------------------------------------

    private Iterator<JsonNode> buildFilter(Filter filter) {
        Iterator<JsonNode> upstream = build(filter.getInput());
        RexNode condition = filter.getCondition();
        return new FilterIterator(upstream, condition, rex);
    }

    private static final class FilterIterator implements Iterator<JsonNode> {
        private final Iterator<JsonNode> src;
        private final RexNode condition;
        private final RexEvaluator rex;
        private JsonNode next;
        FilterIterator(Iterator<JsonNode> src, RexNode condition, RexEvaluator rex) {
            this.src = src; this.condition = condition; this.rex = rex;
        }
        @Override public boolean hasNext() {
            while (next == null && src.hasNext()) {
                JsonNode row = src.next();
                if (rex.evalBool(condition, row)) next = row;
            }
            return next != null;
        }
        @Override public JsonNode next() {
            if (!hasNext()) throw new NoSuchElementException();
            JsonNode out = next; next = null; return out;
        }
    }

    // -- project --------------------------------------------------------------

    private Iterator<JsonNode> buildProject(Project proj) {
        Iterator<JsonNode> upstream = build(proj.getInput());
        List<RexNode> exprs = proj.getProjects();
        List<String> outNames = proj.getRowType().getFieldNames();
        return new ProjectIterator(upstream, exprs, outNames, rex);
    }

    private static final class ProjectIterator implements Iterator<JsonNode> {
        private final Iterator<JsonNode> src;
        private final List<RexNode> exprs;
        private final List<String> outNames;
        private final RexEvaluator rex;
        ProjectIterator(Iterator<JsonNode> src, List<RexNode> exprs, List<String> outNames, RexEvaluator rex) {
            this.src = src; this.exprs = exprs; this.outNames = outNames; this.rex = rex;
        }
        @Override public boolean hasNext() { return src.hasNext(); }
        @Override public JsonNode next() {
            JsonNode row = src.next();
            ObjectNode out = F.objectNode();
            for (int i = 0; i < exprs.size(); i++) {
                JsonNode v = rex.eval(exprs.get(i), row);
                out.set(outNames.get(i), v == null ? F.nullNode() : v);
            }
            return out;
        }
    }

    // -- aggregate ------------------------------------------------------------

    private Iterator<JsonNode> buildAggregate(Aggregate agg) {
        ImmutableBitSet groupSet = agg.getGroupSet();
        int[] groupCols = groupSet.toArray();
        List<AggregateCall> aggCalls = agg.getAggCallList();
        List<String> outNames = agg.getRowType().getFieldNames();
        List<String> inputNames = agg.getInput().getRowType().getFieldNames();

        // Try streaming path: if any group column is derived from WIN_START(event_time, size)
        // AND the source is a registered streaming stream, use per-window incremental emit.
        StreamingHint hint = detectStreamingWindow(agg, groupCols);
        Iterator<JsonNode> upstream = build(agg.getInput());
        if (hint != null) {
            return new StreamingAggregate(
                    upstream, groupCols, hint.groupIdx, hint.windowSize, hint.allowedLateness,
                    aggCalls, outNames, this::aggregateFor, watermarkTracker);
        }

        // Bucketize by group-key tuple.
        Map<List<JsonNode>, GroupState> groups = new LinkedHashMap<>();
        boolean sawRow = false;
        while (upstream.hasNext()) {
            sawRow = true;
            JsonNode row = upstream.next();
            List<JsonNode> key = new ArrayList<>(groupCols.length);
            for (int c : groupCols) key.add(nthField(row, c));
            GroupState g = groups.computeIfAbsent(key, k -> new GroupState(aggCalls));
            for (int i = 0; i < aggCalls.size(); i++) {
                AggregateCall ac = aggCalls.get(i);
                Object argValue = extractAggArg(ac, row, inputNames);
                g.fns[i].accumulate(g.accs[i], argValue);
            }
        }

        // GROUP BY () with no rows still yields one row (e.g. SELECT COUNT(*) FROM t WHERE false → 0).
        if (groups.isEmpty() && groupCols.length == 0 && !aggCalls.isEmpty()) {
            groups.put(List.of(), new GroupState(aggCalls));
        }
        // With group cols and no rows: no output rows.

        // Materialize group -> result row.
        List<JsonNode> outRows = new ArrayList<>(groups.size());
        for (Map.Entry<List<JsonNode>, GroupState> e : groups.entrySet()) {
            ObjectNode row = F.objectNode();
            int idx = 0;
            for (int c : groupCols) {
                JsonNode v = e.getKey().get(idx++);
                row.set(outNames.get(row.size()), v == null ? F.nullNode() : v);
            }
            GroupState g = e.getValue();
            for (int i = 0; i < aggCalls.size(); i++) {
                Object v = g.fns[i].result(g.accs[i]);
                row.set(outNames.get(groupCols.length + i), RexEvaluator.wrap(v));
            }
            outRows.add(row);
        }
        return outRows.iterator();
    }

    private Object extractAggArg(AggregateCall ac, JsonNode row, List<String> inputNames) {
        // COUNT(*) has no arg → passes a non-null sentinel so count() increments.
        // DISTINCT semantics are handled by AggregateOps.distinctOf wrapper in aggregateFor.
        List<Integer> argList = ac.getArgList();
        if (argList.isEmpty()) return Boolean.TRUE;
        int col = argList.get(0);
        JsonNode v = nthField(row, col);
        return RexEvaluator.unwrap(v);
    }

    private static JsonNode nthField(JsonNode row, int col) {
        int j = 0;
        for (Iterator<String> it = row.fieldNames(); it.hasNext(); ) {
            String n = it.next();
            if (j++ == col) return row.get(n);
        }
        return null;
    }

    private final class GroupState {
        final AggregateFn[] fns;
        final Object[] accs;
        GroupState(List<AggregateCall> calls) {
            fns = new AggregateFn[calls.size()];
            accs = new Object[calls.size()];
            for (int i = 0; i < calls.size(); i++) {
                fns[i] = aggregateFor(calls.get(i));
                accs[i] = fns[i].createAccumulator();
            }
        }
    }

    /** Hint that the query looks like a windowed streaming aggregate. */
    private record StreamingHint(int groupIdx, long windowSize, long allowedLateness) {}

    /**
     * Detect whether an Aggregate can be executed as a streaming operator:
     * <ul>
     *   <li>Its input is a Project whose i-th output (that participates in GROUP BY)
     *       is a call to {@code WIN_START(event_time_col, size_literal)}, and</li>
     *   <li>The underlying TableScan is a registered streaming source
     *       (StreamConfig.isStreaming()).</li>
     * </ul>
     * <p>Returns null if either check fails — the caller falls back to batch aggregate.</p>
     */
    private StreamingHint detectStreamingWindow(Aggregate agg, int[] groupCols) {
        RelNode input = agg.getInput();
        if (!(input instanceof Project proj)) return null;
        List<RexNode> exprs = proj.getProjects();
        for (int gi = 0; gi < groupCols.length; gi++) {
            RexNode e = exprs.get(groupCols[gi]);
            if (e instanceof org.apache.calcite.rex.RexCall call
                    && "WIN_START".equalsIgnoreCase(call.getOperator().getName())
                    && call.getOperands().size() == 2
                    && call.getOperands().get(1) instanceof org.apache.calcite.rex.RexLiteral sizeLit) {
                Long size = sizeLit.getValueAs(Long.class);
                if (size == null || size <= 0) continue;
                // Walk down to find the underlying TableScan for stream config.
                Long lateness = findStreamAllowedLateness(input);
                if (lateness == null) continue;   // input isn't a streaming source
                return new StreamingHint(gi, size, lateness);
            }
        }
        return null;
    }

    /**
     * Walk a plan looking for a TableScan of a JvsTable whose StreamConfig is streaming;
     * returns its allowedLateness or {@code null} if none found.
     */
    private Long findStreamAllowedLateness(RelNode node) {
        if (node instanceof TableScan scan) {
            com.hitorro.jvssql.schema.JvsTable t = scan.getTable().unwrap(com.hitorro.jvssql.schema.JvsTable.class);
            if (t == null || t.streamConfig() == null || !t.streamConfig().isStreaming()) return null;
            return t.streamConfig().allowedLatenessMillis();
        }
        for (RelNode child : node.getInputs()) {
            Long l = findStreamAllowedLateness(child);
            if (l != null) return l;
        }
        return null;
    }

    /** Resolve an aggregate call: built-in first, then FunctionRegistry (user UDAFs).
     *  Wraps in {@link AggregateOps#distinctOf} when the call is DISTINCT. */
    private AggregateFn aggregateFor(AggregateCall call) {
        String name = call.getAggregation().getName().toUpperCase();
        AggregateFn base = switch (name) {
            case "COUNT" -> call.getArgList().isEmpty() ? AggregateOps.countStar() : AggregateOps.count();
            case "SUM", "SUM0" -> AggregateOps.sum();
            case "AVG"   -> AggregateOps.avg();
            case "MIN"   -> AggregateOps.min();
            case "MAX"   -> AggregateOps.max();
            default -> functions.getAggregate(name);
        };
        if (base == null) throw new JvsSqlException("aggregate not registered: " + name);
        return call.isDistinct() ? AggregateOps.distinctOf(base) : base;
    }

    // -- sort -----------------------------------------------------------------

    private Iterator<JsonNode> buildSort(Sort sort) {
        Iterator<JsonNode> upstream = build(sort.getInput());
        RelCollation collation = sort.getCollation();
        List<RelFieldCollation> fields = collation.getFieldCollations();
        int offset = sort.offset == null ? 0 : ((org.apache.calcite.rex.RexLiteral) sort.offset).getValueAs(Integer.class);
        int fetch = sort.fetch == null ? Integer.MAX_VALUE : ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValueAs(Integer.class);

        // ORDER BY with no fields (rare — pure LIMIT/OFFSET on unordered input) → just paginate.
        if (fields.isEmpty()) return sliceIterator(upstream, offset, fetch);

        // External-merge sort. Runs stay in memory up to memoryBudgetRows, then spill.
        // Rough conversion: memoryBudgetMB -> row count via a small per-row assumption.
        int rowsPerMB = 5_000;
        int memoryBudgetRows = Math.max(1_000, engineConfig.memoryBudgetMB() * rowsPerMB);
        Iterator<JsonNode> sorted = ExternalMergeSort.sort(
                upstream, sortComparator(fields), memoryBudgetRows, engineConfig.spillDir());
        return sliceIterator(sorted, offset, fetch);
    }

    private static Iterator<JsonNode> sliceIterator(Iterator<JsonNode> src, int offset, int fetch) {
        // Advance past the offset.
        for (int i = 0; i < offset && src.hasNext(); i++) src.next();
        if (fetch == Integer.MAX_VALUE) return src;
        return new Iterator<>() {
            int remaining = fetch;
            @Override public boolean hasNext() { return remaining > 0 && src.hasNext(); }
            @Override public JsonNode next() {
                if (!hasNext()) throw new NoSuchElementException();
                remaining--;
                return src.next();
            }
        };
    }

    private static Comparator<JsonNode> sortComparator(List<RelFieldCollation> fields) {
        return (r1, r2) -> {
            for (RelFieldCollation fc : fields) {
                JsonNode a = nthField(r1, fc.getFieldIndex());
                JsonNode b = nthField(r2, fc.getFieldIndex());
                int c = compareNullsHandled(a, b, fc);
                if (c != 0) return fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? -c : c;
            }
            return 0;
        };
    }

    private static int compareNullsHandled(JsonNode a, JsonNode b, RelFieldCollation fc) {
        boolean aNull = a == null || a.isNull();
        boolean bNull = b == null || b.isNull();
        if (aNull && bNull) return 0;
        if (aNull) return fc.nullDirection == RelFieldCollation.NullDirection.LAST ? 1 : -1;
        if (bNull) return fc.nullDirection == RelFieldCollation.NullDirection.LAST ? -1 : 1;
        if (a.isNumber() && b.isNumber()) return a.decimalValue().compareTo(b.decimalValue());
        return a.asText().compareTo(b.asText());
    }

    // -- hash join ------------------------------------------------------------
    //
    // Phase 1 supports INNER and LEFT equijoins between a streaming left side and
    // a fully-materialized right side. We materialize the right side into a HashMap
    // keyed by the composite join-key tuple, then iterate the left side probing
    // that map. The right side is typically a reference-table scan; anything else
    // is legal too (Sort/Filter/Project on top of a scan) but the whole right
    // subtree is drained into memory at the start of iteration.

    private Iterator<JsonNode> buildJoin(Join join) {
        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new JvsSqlException("Phase 1 supports INNER and LEFT joins only; got " + joinType);
        }
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty() || info.rightKeys.isEmpty()) {
            throw new JvsSqlException("Phase 1 join requires at least one equijoin key. Condition was: "
                    + join.getCondition());
        }

        // Non-equi residual: everything Calcite couldn't push into the hash-key tuple.
        // This is what makes interval joins work:
        //   ON o.id = s.id AND s.ts BETWEEN o.ts - INTERVAL AND o.ts + INTERVAL
        // The equality becomes hash keys; the BETWEEN becomes the residual, evaluated
        // on the combined (left+right) row after each hash match.
        var cluster = join.getCluster();
        org.apache.calcite.rex.RexNode residual = info.getRemaining(cluster.getRexBuilder());
        // Convert isAlwaysTrue → null so we skip the eval per row.
        if (residual != null && residual.isAlwaysTrue()) residual = null;

        // Build side: fully drain the right input into a hash-index by right-key tuple.
        Iterator<JsonNode> rightIt = build(join.getRight());
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int rightFieldCount = join.getRight().getRowType().getFieldCount();
        List<String> outNames = join.getRowType().getFieldNames();

        java.util.Map<List<JsonNode>, List<JsonNode>> hash = new java.util.HashMap<>();
        int[] rightKeyCols = info.rightKeys.stream().mapToInt(Integer::intValue).toArray();
        while (rightIt.hasNext()) {
            JsonNode rightRow = rightIt.next();
            List<JsonNode> key = keyTuple(rightRow, rightKeyCols);
            hash.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(rightRow);
        }

        // Probe side: iterate left, look up matches, then apply the residual.
        Iterator<JsonNode> leftIt = build(join.getLeft());
        int[] leftKeyCols = info.leftKeys.stream().mapToInt(Integer::intValue).toArray();
        return new HashJoinIterator(leftIt, hash, leftKeyCols, leftFieldCount, rightFieldCount,
                outNames, joinType == JoinRelType.LEFT, residual, rex);
    }

    private static List<JsonNode> keyTuple(JsonNode row, int[] keyCols) {
        List<JsonNode> key = new java.util.ArrayList<>(keyCols.length);
        for (int c : keyCols) key.add(nthField(row, c));
        return key;
    }

    private static final class HashJoinIterator implements Iterator<JsonNode> {
        private final Iterator<JsonNode> left;
        private final java.util.Map<List<JsonNode>, List<JsonNode>> hash;
        private final int[] leftKeyCols;
        private final int leftFieldCount;
        private final int rightFieldCount;
        private final List<String> outNames;
        private final boolean isLeftJoin;
        private final org.apache.calcite.rex.RexNode residual;   // may be null
        private final RexEvaluator rex;
        private JsonNode nextOut;
        private JsonNode pendingLeft;
        private Iterator<JsonNode> pendingRightMatches;
        private boolean pendingLeftMatched;   // for LEFT-join fallback

        HashJoinIterator(Iterator<JsonNode> left,
                         java.util.Map<List<JsonNode>, List<JsonNode>> hash,
                         int[] leftKeyCols, int leftFieldCount, int rightFieldCount,
                         List<String> outNames, boolean isLeftJoin,
                         org.apache.calcite.rex.RexNode residual, RexEvaluator rex) {
            this.left = left; this.hash = hash;
            this.leftKeyCols = leftKeyCols;
            this.leftFieldCount = leftFieldCount;
            this.rightFieldCount = rightFieldCount;
            this.outNames = outNames;
            this.isLeftJoin = isLeftJoin;
            this.residual = residual;
            this.rex = rex;
        }

        @Override public boolean hasNext() {
            while (nextOut == null) {
                // Continue draining right-side matches for the current left row.
                while (pendingRightMatches != null && pendingRightMatches.hasNext()) {
                    JsonNode candidate = combine(pendingLeft, pendingRightMatches.next());
                    if (residual == null || rex.evalBool(residual, candidate)) {
                        pendingLeftMatched = true;
                        nextOut = candidate;
                        return true;
                    }
                    // residual rejected this pair — try the next right-side match.
                }
                // No more right matches for the current left row. For LEFT joins, if
                // none of the right candidates passed the residual, emit the null-padded row.
                if (pendingLeft != null && isLeftJoin && !pendingLeftMatched) {
                    nextOut = combine(pendingLeft, null);
                    pendingLeft = null;
                    pendingRightMatches = null;
                    return true;
                }
                pendingLeft = null;
                pendingRightMatches = null;
                pendingLeftMatched = false;

                if (!left.hasNext()) return false;
                JsonNode leftRow = left.next();
                List<JsonNode> key = keyTuple(leftRow, leftKeyCols);
                List<JsonNode> matches = hash.get(key);
                if (matches == null || matches.isEmpty()) {
                    if (isLeftJoin) {
                        nextOut = combine(leftRow, null);
                        return true;
                    }
                    continue;   // INNER: skip
                }
                pendingLeft = leftRow;
                pendingRightMatches = matches.iterator();
            }
            return true;
        }

        @Override public JsonNode next() {
            if (!hasNext()) throw new NoSuchElementException();
            JsonNode out = nextOut; nextOut = null; return out;
        }

        private JsonNode combine(JsonNode leftRow, JsonNode rightRow) {
            ObjectNode out = F.objectNode();
            // Left fields first, then right — matches Calcite's join row type.
            int lf = leftFieldCount;
            int rf = rightFieldCount;
            for (int i = 0; i < lf; i++) {
                JsonNode v = nthField(leftRow, i);
                out.set(outNames.get(i), v == null ? F.nullNode() : v);
            }
            for (int j = 0; j < rf; j++) {
                JsonNode v = rightRow == null ? F.nullNode() : nthField(rightRow, j);
                out.set(outNames.get(lf + j), v == null ? F.nullNode() : v);
            }
            return out;
        }
    }

    // -- values ---------------------------------------------------------------

    private Iterator<JsonNode> buildValues(Values vals) {
        List<String> outNames = vals.getRowType().getFieldNames();
        List<JsonNode> rows = new ArrayList<>();
        for (var tuple : vals.tuples) {
            ObjectNode row = F.objectNode();
            for (int i = 0; i < tuple.size(); i++) {
                row.set(outNames.get(i), RexEvaluator.literalAsJson(tuple.get(i)));
            }
            rows.add(row);
        }
        return rows.iterator();
    }
}
