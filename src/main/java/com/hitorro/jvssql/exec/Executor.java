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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Walks a Calcite {@link RelNode} plan and produces an {@code Iterator<JsonNode>}.
 *
 * <p>Phase 1 handles {@link TableScan}, {@link Filter}, and {@link Project}. It supports
 * a small predicate/expression subset — enough to prove the end-to-end plumbing:</p>
 * <ul>
 *   <li>Column references (RexInputRef)</li>
 *   <li>Literals (RexLiteral)</li>
 *   <li>Binary comparisons: =, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=</li>
 *   <li>AND / OR / NOT</li>
 * </ul>
 *
 * <p>Everything else — aggregates, sorts, joins, richer expressions, DYNAMIC(), MLS() —
 * is added incrementally in subsequent Phase 1 tasks.</p>
 */
public final class Executor {

    private final RelNode plan;

    public Executor(RelNode plan) { this.plan = plan; }

    public Iterator<JsonNode> execute() {
        return build(plan);
    }

    // -- plan walk ------------------------------------------------------------

    private Iterator<JsonNode> build(RelNode node) {
        if (node instanceof TableScan scan) return buildScan(scan);
        if (node instanceof Filter filter)  return buildFilter(filter);
        if (node instanceof Project proj)   return buildProject(proj);
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
        return new ScanIterator(src, projector);
    }

    private static final class ScanIterator implements Iterator<JsonNode> {
        private final Iterator<JVS> src;
        private final RowProjector projector;
        ScanIterator(Iterator<JVS> src, RowProjector projector) {
            this.src = src; this.projector = projector;
        }
        @Override public boolean hasNext() { return src.hasNext(); }
        @Override public JsonNode next() {
            if (!src.hasNext()) throw new NoSuchElementException();
            JVS row = src.next();
            ObjectNode out = JsonNodeFactory.instance.objectNode();
            for (String col : projector.columnNames()) {
                JsonNode v = projector.read(row, col);
                out.set(col, v == null ? JsonNodeFactory.instance.nullNode() : v);
            }
            return out;
        }
    }

    // -- filter ---------------------------------------------------------------

    private Iterator<JsonNode> buildFilter(Filter filter) {
        Iterator<JsonNode> upstream = build(filter.getInput());
        RexNode condition = filter.getCondition();
        return new FilterIterator(upstream, condition);
    }

    private static final class FilterIterator implements Iterator<JsonNode> {
        private final Iterator<JsonNode> src;
        private final RexNode condition;
        private JsonNode next;
        FilterIterator(Iterator<JsonNode> src, RexNode condition) {
            this.src = src; this.condition = condition;
        }
        @Override public boolean hasNext() {
            while (next == null && src.hasNext()) {
                JsonNode row = src.next();
                if (evalBool(condition, row)) next = row;
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
        return new ProjectIterator(upstream, exprs, outNames);
    }

    private static final class ProjectIterator implements Iterator<JsonNode> {
        private final Iterator<JsonNode> src;
        private final List<RexNode> exprs;
        private final List<String> outNames;
        ProjectIterator(Iterator<JsonNode> src, List<RexNode> exprs, List<String> outNames) {
            this.src = src; this.exprs = exprs; this.outNames = outNames;
        }
        @Override public boolean hasNext() { return src.hasNext(); }
        @Override public JsonNode next() {
            JsonNode row = src.next();
            ObjectNode out = JsonNodeFactory.instance.objectNode();
            for (int i = 0; i < exprs.size(); i++) {
                JsonNode v = eval(exprs.get(i), row);
                out.set(outNames.get(i), v == null ? JsonNodeFactory.instance.nullNode() : v);
            }
            return out;
        }
    }

    // -- expression eval ------------------------------------------------------
    //
    // Minimal RexNode interpreter. Every additional operator becomes a case in
    // evalCall(). For Phase 1-late we replace this with a proper visitor and a
    // per-column type-driven coercion table (RexInterpreter is stricter than
    // what we want, but it's a good reference).

    private static JsonNode eval(RexNode node, JsonNode row) {
        if (node instanceof RexInputRef ref) {
            String colName = row instanceof ObjectNode ? nthFieldName(row, ref.getIndex()) : null;
            return colName == null ? null : row.get(colName);
        }
        if (node instanceof RexLiteral lit) return literalAsJson(lit);
        if (node instanceof RexCall call) return evalCall(call, row);
        throw new JvsSqlException("unsupported RexNode kind: " + node.getKind() + " (" + node.getClass().getSimpleName() + ")");
    }

    private static boolean evalBool(RexNode node, JsonNode row) {
        JsonNode v = eval(node, row);
        return v != null && !v.isNull() && v.asBoolean();
    }

    private static JsonNode evalCall(RexCall call, JsonNode row) {
        SqlKind kind = call.getKind();
        JsonNodeFactory f = JsonNodeFactory.instance;
        switch (kind) {
            case AND: {
                for (RexNode op : call.getOperands()) {
                    if (!evalBool(op, row)) return f.booleanNode(false);
                }
                return f.booleanNode(true);
            }
            case OR: {
                for (RexNode op : call.getOperands()) {
                    if (evalBool(op, row)) return f.booleanNode(true);
                }
                return f.booleanNode(false);
            }
            case NOT: return f.booleanNode(!evalBool(call.getOperands().get(0), row));
            case EQUALS:                 return f.booleanNode(cmp(call, row) == 0);
            case NOT_EQUALS:             return f.booleanNode(cmp(call, row) != 0);
            case LESS_THAN:              return f.booleanNode(cmp(call, row) <  0);
            case LESS_THAN_OR_EQUAL:     return f.booleanNode(cmp(call, row) <= 0);
            case GREATER_THAN:           return f.booleanNode(cmp(call, row) >  0);
            case GREATER_THAN_OR_EQUAL:  return f.booleanNode(cmp(call, row) >= 0);
            case IS_NULL:                {
                JsonNode v = eval(call.getOperands().get(0), row);
                return f.booleanNode(v == null || v.isNull());
            }
            case IS_NOT_NULL: {
                JsonNode v = eval(call.getOperands().get(0), row);
                return f.booleanNode(v != null && !v.isNull());
            }
            default:
                throw new JvsSqlException("Phase 1 does not yet support expression: " + kind);
        }
    }

    private static int cmp(RexCall call, JsonNode row) {
        JsonNode a = eval(call.getOperands().get(0), row);
        JsonNode b = eval(call.getOperands().get(1), row);
        if (a == null || a.isNull() || b == null || b.isNull()) return -2;
        if (a.isNumber() && b.isNumber()) return Double.compare(a.asDouble(), b.asDouble());
        return a.asText().compareTo(b.asText());
    }

    private static JsonNode literalAsJson(RexLiteral lit) {
        JsonNodeFactory f = JsonNodeFactory.instance;
        Object v = lit.getValue();
        if (v == null) return f.nullNode();
        if (v instanceof BigDecimal bd) return f.numberNode(bd);
        if (v instanceof Boolean b) return f.booleanNode(b);
        if (v instanceof Number n) return f.numberNode(n.doubleValue());
        // String literals are wrapped in an NlsString (charset + collation + value); use
        // getValueAs(String.class) to strip the wrapping down to the raw text.
        String s = lit.getValueAs(String.class);
        return s == null ? f.nullNode() : f.textNode(s);
    }

    private static String nthFieldName(JsonNode row, int i) {
        int j = 0;
        for (java.util.Iterator<String> it = row.fieldNames(); it.hasNext(); ) {
            String n = it.next();
            if (j++ == i) return n;
        }
        return null;
    }
}
