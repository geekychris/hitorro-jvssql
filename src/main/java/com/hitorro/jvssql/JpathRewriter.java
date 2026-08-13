/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transparent dotted-path support: rewrites multi-part identifiers like
 * {@code metadata.exp} into {@code JPATH('metadata.exp')} so authors can navigate
 * nested JVS content in {@code SELECT}, {@code WHERE}, {@code GROUP BY}, etc.
 * without reaching for the JPATH escape hatch by hand.
 *
 * <h3>Design decision — read this before "improving" it</h3>
 *
 * The engine's declared table schema is flat (one Calcite column per top-level
 * JVS field). Calcite's validator therefore rejects any multi-part identifier
 * whose first component is a scalar column — {@code metadata.exp} is read as
 * "field {@code exp} of column {@code metadata}" and only validates if the
 * column is a ROW/STRUCT. We *could* declare every JVS column as a nested
 * ROW type and let Calcite navigate it natively; that was the alternative
 * (approach B in the design conversation). It is strictly more powerful — the
 * validator can type-check nested access, the optimizer can push predicates
 * through struct field access — but requires:
 *
 *   1. Walking every registered {@link com.hitorro.jsontypesystem.Type} and
 *      recursively mapping nested groups / MLS envelopes to Calcite ROW types.
 *   2. Handling arrays as ARRAY-of-ROW, including cases where the array element
 *      shape is heterogeneous (JVS permits this; Calcite does not).
 *   3. Teaching every operator (Filter/Project/Aggregate) to unwrap the row
 *      structure back into JsonNode values when calling into the executor.
 *   4. Reconciling with dynamic/undeclared JVS fields — data that isn't in the
 *      Type at all. JPATH still needs to exist for that case.
 *
 * The current approach (rewrite to JPATH at parse-tree time) is a much smaller
 * intervention: one {@link SqlShuttle} pass, no schema changes, no executor
 * changes. Its cost is that the validator can't type-check the *contents* of a
 * JPATH argument, and predicates involving JPATH are opaque to the planner.
 * That is fine for Phase 1-late — if we ever need column-level statistics or
 * predicate pushdown into nested fields, revisit and do approach B properly.
 *
 * <h3>Rules</h3>
 * <ul>
 *   <li>A single-part identifier (bare column name) is left untouched.</li>
 *   <li>A two-part identifier is left untouched iff its first part is a
 *       known table name or alias from the FROM clause
 *       (e.g. {@code docs.title} where {@code docs} is a registered table).</li>
 *   <li>Any other multi-part identifier is rewritten to
 *       {@code JPATH('joined.parts')}. If the first part IS a table alias,
 *       the alias is stripped from the path before joining.</li>
 * </ul>
 *
 * <h3>Known limits (deferred)</h3>
 * <ul>
 *   <li>Array indexing chains (e.g. {@code title.mls[0].text}) — Calcite parses
 *       these as an {@code ITEM} call wrapping identifiers, and this shuttle
 *       does not currently reassemble the mixed identifier+ITEM tree into a
 *       single JPATH string. Users must still write {@code JPATH('title.mls[0].text')}
 *       for that shape. Fix would be a second visitor that recognizes the
 *       specific {@code DOT/ITEM} pattern.</li>
 *   <li>JOIN with the same column name on both sides could confuse the alias
 *       detection if a user names a JVS field the same as a table alias.
 *       Documented, not fixed.</li>
 * </ul>
 */
final class JpathRewriter {

    private JpathRewriter() {}

    /**
     * Return a copy of {@code root} with dotted identifiers rewritten to JPATH calls.
     * The input node is not mutated.
     */
    static SqlNode rewrite(SqlNode root) {
        Set<String> tableNames = new LinkedHashSet<>();
        collectTableNames(root, tableNames);
        Shuttle s = new Shuttle(tableNames);
        SqlNode out = root.accept(s);
        return out == null ? root : out;
    }

    /** Walks the tree collecting table names and aliases from any FROM clause found. */
    private static void collectTableNames(SqlNode node, Set<String> out) {
        if (node == null) return;
        if (node instanceof SqlSelect sel) {
            addFrom(sel.getFrom(), out);
            // recurse into WITH / subqueries in FROM etc. — SqlShuttle covers the
            // expression tree but not deeply nested selects for name collection.
            if (sel.getSelectList() != null) {
                for (SqlNode s : sel.getSelectList()) collectTableNames(s, out);
            }
            collectTableNames(sel.getWhere(), out);
        } else if (node instanceof SqlCall call) {
            for (SqlNode op : call.getOperandList()) collectTableNames(op, out);
        } else if (node instanceof SqlNodeList list) {
            for (SqlNode n : list) collectTableNames(n, out);
        }
    }

    private static void addFrom(SqlNode from, Set<String> out) {
        if (from == null) return;
        if (from instanceof SqlIdentifier id) {
            out.add(lastPart(id).toLowerCase());
        } else if (from instanceof SqlJoin join) {
            addFrom(join.getLeft(), out);
            addFrom(join.getRight(), out);
        } else if (from instanceof SqlCall call
                && call.getOperator() == SqlStdOperatorTable.AS
                && call.operandCount() >= 2) {
            addFrom(call.operand(0), out);
            if (call.operand(1) instanceof SqlIdentifier alias) {
                out.add(alias.getSimple().toLowerCase());
            }
        }
    }

    private static String lastPart(SqlIdentifier id) {
        List<String> parts = id.names;
        return parts.get(parts.size() - 1);
    }

    private static final class Shuttle extends SqlShuttle {
        private final Set<String> tableNames;

        Shuttle(Set<String> tableNames) { this.tableNames = tableNames; }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            List<String> parts = id.names;
            if (parts.size() < 2) return id;

            String first = parts.get(0).toLowerCase();
            boolean firstIsTable = tableNames.contains(first);

            // table.column stays as-is; Calcite handles it natively.
            if (firstIsTable && parts.size() == 2) return id;

            // Strip the table alias if it prefixes a deeper path.
            List<String> pathParts = firstIsTable ? parts.subList(1, parts.size()) : parts;
            String path = String.join(".", pathParts);

            SqlParserPos pos = id.getParserPosition();
            SqlIdentifier jpath = new SqlIdentifier("JPATH", pos);
            SqlNode literal = org.apache.calcite.sql.SqlLiteral.createCharString(path, pos);
            return new SqlBasicCall(
                    new org.apache.calcite.sql.SqlUnresolvedFunction(
                            jpath,
                            null, null, null, null,
                            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                    List.of(literal),
                    pos);
        }
    }
}
