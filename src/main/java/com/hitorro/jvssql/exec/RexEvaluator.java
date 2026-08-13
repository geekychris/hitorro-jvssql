/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.hitorro.jvssql.JvsSqlException;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Evaluates a Calcite {@link RexNode} against a {@link JsonNode} row and returns a
 * {@link JsonNode} value (or {@code null} for SQL NULL).
 *
 * <p>Rows are Jackson {@code ObjectNode}s produced by the scan operator with field
 * names matching the SELECT/FROM row type. Column references ({@link RexInputRef})
 * resolve by ordinal against the row's field iteration order — the scan is
 * responsible for inserting fields in row-type order.</p>
 *
 * <h3>Supported operator set (Phase 1)</h3>
 * <ul>
 *   <li>Column refs, literals</li>
 *   <li>Boolean: AND, OR, NOT, IS NULL, IS NOT NULL</li>
 *   <li>Comparisons: =, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=</li>
 *   <li>Arithmetic: +, -, *, /, MOD, unary -</li>
 *   <li>Predicates: LIKE, NOT LIKE, IN, NOT IN, BETWEEN, NOT BETWEEN</li>
 *   <li>Conditional: CASE WHEN, COALESCE, NULLIF, CAST</li>
 *   <li>String: UPPER, LOWER, TRIM, SUBSTRING, CHAR_LENGTH, CONCAT (via ||), REPLACE</li>
 *   <li>Math: ABS, ROUND, CEIL, FLOOR, MOD</li>
 *   <li>Built-ins: CURRENT_TIMESTAMP</li>
 * </ul>
 *
 * <p>User-defined scalar functions (Java + Groovy) are dispatched via {@link ScalarFn}
 * looked up by operator name from {@link FunctionRegistry}. DYNAMIC and MLS are two
 * such built-in scalars.</p>
 */
public final class RexEvaluator {

    private final FunctionRegistry functions;
    private final java.util.Map<Integer, Object> paramBindings;

    public RexEvaluator(FunctionRegistry functions) {
        this(functions, java.util.Map.of());
    }

    public RexEvaluator(FunctionRegistry functions, java.util.Map<Integer, Object> paramBindings) {
        this.functions = functions;
        this.paramBindings = paramBindings == null ? java.util.Map.of() : paramBindings;
    }

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    /** Evaluate a boolean-typed expression. NULL → false. */
    public boolean evalBool(RexNode node, JsonNode row) {
        JsonNode v = eval(node, row);
        return v != null && !v.isNull() && v.asBoolean(false);
    }

    /** Evaluate an expression; returns a JsonNode (possibly {@link com.fasterxml.jackson.databind.node.NullNode}) or {@code null} for SQL NULL. */
    public JsonNode eval(RexNode node, JsonNode row) {
        if (node instanceof RexInputRef ref) {
            return columnByIndex(row, ref.getIndex());
        }
        if (node instanceof RexLiteral lit) {
            return literalAsJson(lit);
        }
        if (node instanceof RexCall call) {
            return evalCall(call, row);
        }
        if (node instanceof org.apache.calcite.rex.RexDynamicParam dp) {
            // Resolve ?-parameter binding set via PreparedQuery.bind(pos, value).
            Object v = paramBindings.get(dp.getIndex());
            return wrap(v);
        }
        throw new JvsSqlException("unsupported RexNode: " + node.getKind() + " (" + node.getClass().getSimpleName() + ")");
    }

    private JsonNode evalCall(RexCall call, JsonNode row) {
        SqlKind kind = call.getKind();
        List<RexNode> ops = call.getOperands();

        // Boolean ops handled first for short-circuiting.
        switch (kind) {
            case AND: {
                for (RexNode op : ops) if (!evalBool(op, row)) return F.booleanNode(false);
                return F.booleanNode(true);
            }
            case OR: {
                for (RexNode op : ops) if (evalBool(op, row)) return F.booleanNode(true);
                return F.booleanNode(false);
            }
            case NOT: return F.booleanNode(!evalBool(ops.get(0), row));
            case IS_NULL: {
                JsonNode v = eval(ops.get(0), row);
                return F.booleanNode(v == null || v.isNull());
            }
            case IS_NOT_NULL: {
                JsonNode v = eval(ops.get(0), row);
                return F.booleanNode(v != null && !v.isNull());
            }
            case IS_TRUE: {
                JsonNode v = eval(ops.get(0), row);
                return F.booleanNode(v != null && !v.isNull() && v.asBoolean(false));
            }
            case IS_FALSE: {
                JsonNode v = eval(ops.get(0), row);
                return F.booleanNode(v != null && !v.isNull() && !v.asBoolean(false));
            }
            default: /* fall through */
        }

        switch (kind) {
            case EQUALS:                return F.booleanNode(cmp(ops, row) == 0);
            case NOT_EQUALS:            return F.booleanNode(cmp(ops, row) != 0);
            case LESS_THAN:             return F.booleanNode(cmp(ops, row) < 0);
            case LESS_THAN_OR_EQUAL:    return F.booleanNode(cmp(ops, row) <= 0);
            case GREATER_THAN:          return F.booleanNode(cmp(ops, row) > 0);
            case GREATER_THAN_OR_EQUAL: return F.booleanNode(cmp(ops, row) >= 0);

            case PLUS:                  return arith(ops, row, "+");
            case MINUS:                 return arith(ops, row, "-");
            case TIMES:                 return arith(ops, row, "*");
            case DIVIDE:                return arith(ops, row, "/");
            case MOD:                   return arith(ops, row, "%");
            case MINUS_PREFIX: {
                JsonNode v = eval(ops.get(0), row);
                if (v == null || v.isNull() || !v.isNumber()) return null;
                return F.numberNode(v.decimalValue().negate());
            }

            case LIKE: {
                JsonNode s = eval(ops.get(0), row);
                JsonNode p = eval(ops.get(1), row);
                if (s == null || s.isNull() || p == null || p.isNull()) return null;
                return F.booleanNode(likeMatch(s.asText(), p.asText()));
            }

            case IN: {
                JsonNode probe = eval(ops.get(0), row);
                if (probe == null || probe.isNull()) return null;
                for (int i = 1; i < ops.size(); i++) {
                    JsonNode elem = eval(ops.get(i), row);
                    if (elem != null && !elem.isNull() && compareValues(probe, elem) == 0) {
                        return F.booleanNode(true);
                    }
                }
                return F.booleanNode(false);
            }

            case BETWEEN: {
                // Calcite normalizes BETWEEN to AND(>=, <=) at planning, but tolerate raw form.
                JsonNode v = eval(ops.get(0), row);
                JsonNode lo = eval(ops.get(1), row);
                JsonNode hi = eval(ops.get(2), row);
                if (v == null || v.isNull() || lo == null || lo.isNull() || hi == null || hi.isNull()) return null;
                return F.booleanNode(compareValues(v, lo) >= 0 && compareValues(v, hi) <= 0);
            }

            case CASE: {
                // Alternating WHEN/THEN pairs then optional ELSE.
                int i = 0;
                while (i + 1 < ops.size()) {
                    if (evalBool(ops.get(i), row)) return eval(ops.get(i + 1), row);
                    i += 2;
                }
                return i < ops.size() ? eval(ops.get(i), row) : F.nullNode();
            }

            case COALESCE: {
                for (RexNode op : ops) {
                    JsonNode v = eval(op, row);
                    if (v != null && !v.isNull()) return v;
                }
                return F.nullNode();
            }

            case NULLIF: {
                JsonNode a = eval(ops.get(0), row);
                JsonNode b = eval(ops.get(1), row);
                if (a == null || a.isNull()) return F.nullNode();
                if (b != null && !b.isNull() && compareValues(a, b) == 0) return F.nullNode();
                return a;
            }

            case CAST: {
                // For Phase 1, tolerate CAST as a passthrough — the JsonNode already carries the value.
                // Full CAST semantics (numeric ↔ string, string → date) is Phase 1-late.
                return eval(ops.get(0), row);
            }

            // String / date / concat / etc.
            default: /* fall through */
        }

        // Named function dispatch. Covers UPPER / LOWER / TRIM / CONCAT ('||') / SUBSTRING /
        // CHAR_LENGTH / REPLACE / ABS / ROUND / CEIL / FLOOR / DYNAMIC / MLS and any UDF.
        String opName = call.getOperator().getName();
        ScalarFn fn = functions.getScalar(opName);
        if (fn != null) {
            Object[] argValues = new Object[ops.size()];
            for (int i = 0; i < ops.size(); i++) {
                argValues[i] = unwrap(eval(ops.get(i), row));
            }
            Object result = fn.call(argValues);
            return wrap(result);
        }

        throw new JvsSqlException("unsupported operator: " + opName + " (SqlKind=" + kind + ")");
    }

    // -- comparison / arithmetic helpers -------------------------------------

    private int cmp(List<RexNode> ops, JsonNode row) {
        JsonNode a = eval(ops.get(0), row);
        JsonNode b = eval(ops.get(1), row);
        return compareValues(a, b);
    }

    /**
     * Three-valued comparison. Returns Integer.MIN_VALUE for NULLs; callers should NOT use
     * the return value to satisfy SQL comparison predicates when either side is NULL
     * (those return NULL, treated as false by WHERE).
     */
    public static int compareValues(JsonNode a, JsonNode b) {
        if (a == null || a.isNull() || b == null || b.isNull()) return Integer.MIN_VALUE;
        if (a.isNumber() && b.isNumber()) {
            return a.decimalValue().compareTo(b.decimalValue());
        }
        if (a.isBoolean() && b.isBoolean()) return Boolean.compare(a.booleanValue(), b.booleanValue());
        return a.asText().compareTo(b.asText());
    }

    private JsonNode arith(List<RexNode> ops, JsonNode row, String op) {
        JsonNode a = eval(ops.get(0), row);
        JsonNode b = eval(ops.get(1), row);
        if (a == null || a.isNull() || b == null || b.isNull()) return null;
        // String CONCAT is an operator overload of PLUS in some dialects; Calcite emits ||
        // as a call to CONCAT — handled by function dispatch. Here we handle numeric only.
        if (!a.isNumber() || !b.isNumber()) {
            throw new JvsSqlException("arithmetic on non-numeric values: " + a + " " + op + " " + b);
        }
        BigDecimal x = a.decimalValue();
        BigDecimal y = b.decimalValue();
        return switch (op) {
            case "+" -> F.numberNode(x.add(y));
            case "-" -> F.numberNode(x.subtract(y));
            case "*" -> F.numberNode(x.multiply(y));
            case "/" -> {
                if (y.signum() == 0) throw new JvsSqlException("division by zero");
                yield F.numberNode(x.divide(y, java.math.MathContext.DECIMAL64));
            }
            case "%" -> F.numberNode(x.remainder(y));
            default -> throw new JvsSqlException("bad arithmetic op: " + op);
        };
    }

    /** SQL LIKE: {@code %} = any run, {@code _} = single char. Anchor implicit. */
    static boolean likeMatch(String s, String pattern) {
        StringBuilder re = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%' -> re.append(".*");
                case '_' -> re.append('.');
                case '\\', '.', '^', '$', '(', ')', '[', ']', '{', '}', '+', '*', '?', '|' ->
                        re.append('\\').append(c);
                default  -> re.append(c);
            }
        }
        re.append("$");
        return Pattern.compile(re.toString(), Pattern.DOTALL).matcher(s).matches();
    }

    // -- literals + column refs ----------------------------------------------

    private JsonNode columnByIndex(JsonNode row, int index) {
        if (!(row.isObject())) return null;
        int j = 0;
        for (Iterator<String> it = row.fieldNames(); it.hasNext(); ) {
            String n = it.next();
            if (j++ == index) return row.get(n);
        }
        return null;
    }

    static JsonNode literalAsJson(RexLiteral lit) {
        Object v = lit.getValue();
        if (v == null) return F.nullNode();
        if (v instanceof BigDecimal bd) return F.numberNode(bd);
        if (v instanceof Boolean b) return F.booleanNode(b);
        if (v instanceof Number n) return F.numberNode(n.doubleValue());
        // Enum SYMBOL literals (TRIM's BOTH/LEADING/TRAILING, EXTRACT unit, etc.) can't
        // be coerced to String by getValueAs — fall back to toString().
        if (v instanceof Enum<?> e) return F.textNode(e.name());
        String s = lit.getValueAs(String.class);
        if (s == null) return F.nullNode();
        // Calcite pads CHAR literals to a common width when they meet (e.g. in CASE branches).
        // JVS is a JSON world where VARCHAR semantics is the norm — strip trailing spaces on
        // CHAR literals so 'small'/'tiny'/'big' don't come out as 'small'/'tiny '/'big  '.
        if (lit.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
            int end = s.length();
            while (end > 0 && s.charAt(end - 1) == ' ') end--;
            s = s.substring(0, end);
        }
        return F.textNode(s);
    }

    // -- Java <-> JsonNode bridging for UDF dispatch --------------------------

    /** Unwrap a JsonNode into a Java value the UDF layer expects. */
    static Object unwrap(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (n.isBoolean()) return n.booleanValue();
        if (n.isNumber()) {
            if (n.isIntegralNumber()) return n.longValue();
            return n.doubleValue();
        }
        if (n.isTextual()) return n.textValue();
        // For arrays/objects, hand the JsonNode over verbatim — UDFs that want raw JSON get it.
        return n;
    }

    static JsonNode wrap(Object v) {
        if (v == null) return F.nullNode();
        if (v instanceof JsonNode jn) return jn;
        if (v instanceof Boolean b) return F.booleanNode(b);
        if (v instanceof Integer i) return F.numberNode(i);
        if (v instanceof Long l) return F.numberNode(l);
        if (v instanceof Float f) return F.numberNode(f);
        if (v instanceof Double d) return F.numberNode(d);
        if (v instanceof BigDecimal bd) return F.numberNode(bd);
        if (v instanceof Number n) return F.numberNode(n.doubleValue());
        if (v instanceof List<?> list) {
            var arr = F.arrayNode();
            for (Object o : list) arr.add(wrap(o));
            return arr;
        }
        return F.textNode(v.toString());
    }

    /** Collect all row-column references, used to prune unused columns from scans. */
    public static List<Integer> referencedColumns(RexNode node) {
        List<Integer> out = new ArrayList<>();
        collectRefs(node, out);
        return out;
    }

    private static void collectRefs(RexNode node, List<Integer> out) {
        if (node instanceof RexInputRef r) out.add(r.getIndex());
        else if (node instanceof RexCall c) for (RexNode op : c.getOperands()) collectRefs(op, out);
    }
}
