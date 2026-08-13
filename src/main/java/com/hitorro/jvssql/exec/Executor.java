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
    private final RexEvaluator rex;

    public Executor(RelNode plan, FunctionRegistry functions, com.hitorro.jvssql.config.EngineConfig engineConfig) {
        this.plan = plan;
        this.functions = functions;
        this.engineConfig = engineConfig;
        this.rex = new RexEvaluator(functions);
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
        throw new JvsSqlException("Phase 1 does not yet support operator: "
                + node.getClass().getSimpleName());
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
        return new ScanIterator(src, projector, functions);
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
        Iterator<JsonNode> upstream = build(agg.getInput());
        ImmutableBitSet groupSet = agg.getGroupSet();
        int[] groupCols = groupSet.toArray();
        List<AggregateCall> aggCalls = agg.getAggCallList();
        List<String> outNames = agg.getRowType().getFieldNames();
        List<String> inputNames = agg.getInput().getRowType().getFieldNames();

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
        List<Integer> argList = ac.getArgList();
        if (ac.isDistinct()) {
            // Phase-1-late: DISTINCT aggregation. Fold via unique-key set per group.
            throw new JvsSqlException("COUNT(DISTINCT ...) not yet supported in Phase 1");
        }
        if (argList.isEmpty()) {
            // COUNT(*)
            return Boolean.TRUE;
        }
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

    /** Resolve an aggregate call: built-in first, then FunctionRegistry (user UDAFs). */
    private AggregateFn aggregateFor(AggregateCall call) {
        String name = call.getAggregation().getName().toUpperCase();
        AggregateFn builtin = switch (name) {
            case "COUNT" -> call.getArgList().isEmpty() ? AggregateOps.countStar() : AggregateOps.count();
            case "SUM", "SUM0" -> AggregateOps.sum();
            case "AVG"   -> AggregateOps.avg();
            case "MIN"   -> AggregateOps.min();
            case "MAX"   -> AggregateOps.max();
            default -> null;
        };
        if (builtin != null) return builtin;
        AggregateFn user = functions.getAggregate(name);
        if (user != null) return user;
        throw new JvsSqlException("aggregate not registered: " + name);
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
        if (info.leftKeys.isEmpty() || info.rightKeys.isEmpty() || !info.isEqui()) {
            throw new JvsSqlException("Phase 1 supports equijoins only. Condition was: " + join.getCondition());
        }

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

        // Probe side: iterate left, look up matches.
        Iterator<JsonNode> leftIt = build(join.getLeft());
        int[] leftKeyCols = info.leftKeys.stream().mapToInt(Integer::intValue).toArray();
        return new HashJoinIterator(leftIt, hash, leftKeyCols, leftFieldCount, rightFieldCount,
                outNames, joinType == JoinRelType.LEFT);
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
        private JsonNode nextOut;
        private JsonNode pendingLeft;
        private Iterator<JsonNode> pendingRightMatches;

        HashJoinIterator(Iterator<JsonNode> left,
                         java.util.Map<List<JsonNode>, List<JsonNode>> hash,
                         int[] leftKeyCols, int leftFieldCount, int rightFieldCount,
                         List<String> outNames, boolean isLeftJoin) {
            this.left = left; this.hash = hash;
            this.leftKeyCols = leftKeyCols;
            this.leftFieldCount = leftFieldCount;
            this.rightFieldCount = rightFieldCount;
            this.outNames = outNames;
            this.isLeftJoin = isLeftJoin;
        }

        @Override public boolean hasNext() {
            while (nextOut == null) {
                // Continue emitting a still-open cross-product of the current left row.
                if (pendingRightMatches != null && pendingRightMatches.hasNext()) {
                    nextOut = combine(pendingLeft, pendingRightMatches.next());
                    return true;
                }
                pendingRightMatches = null;
                if (!left.hasNext()) return false;
                JsonNode leftRow = left.next();
                List<JsonNode> key = keyTuple(leftRow, leftKeyCols);
                List<JsonNode> matches = hash.get(key);
                if (matches == null || matches.isEmpty()) {
                    if (isLeftJoin) {
                        // LEFT JOIN preserves the left row with NULLs on the right side.
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
