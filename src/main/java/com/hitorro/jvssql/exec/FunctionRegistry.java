/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.util.json.keys.propaccess.PAContext;
import com.hitorro.util.json.keys.propaccess.Propaccess;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Name-to-implementation registry for scalar and aggregate functions.
 *
 * <p>Populates itself with the SQL standard scalar library on construction so
 * common SQL like {@code UPPER(x)}, {@code SUBSTRING(x, 1, 3)},
 * {@code COALESCE(...)}, {@code MOD(a,b)}, and the JVS-specific
 * {@code DYNAMIC('path')} / {@code MLS(field, 'lang')} all Just Work.
 * User-registered functions (Java class or Groovy script) go into the same
 * map via {@link #putScalar}/{@link #putAggregate}.</p>
 *
 * <p>Names are case-insensitive (Calcite uppercases the operator name).</p>
 */
public final class FunctionRegistry {

    private final Map<String, ScalarFn> scalars = new HashMap<>();
    private final Map<String, AggregateFn> aggregates = new HashMap<>();

    /** Bound to a specific engine's session state (e.g. current row for DYNAMIC access). */
    private final ThreadLocal<JVS> currentJvsRow = new ThreadLocal<>();

    public FunctionRegistry() {
        registerBuiltInScalars();
    }

    // -- public registration --------------------------------------------------

    public void putScalar(String name, ScalarFn fn) { scalars.put(name.toUpperCase(), fn); }
    public void putAggregate(String name, AggregateFn fn) { aggregates.put(name.toUpperCase(), fn); }

    public ScalarFn getScalar(String name) { return scalars.get(name.toUpperCase()); }
    public AggregateFn getAggregate(String name) { return aggregates.get(name.toUpperCase()); }

    /** Called by scans/executor to make the current JVS document available for DYNAMIC/MLS. */
    public void bindRow(JVS row) { currentJvsRow.set(row); }
    public void unbindRow() { currentJvsRow.remove(); }
    JVS currentRow() { return currentJvsRow.get(); }

    // -- built-in scalars -----------------------------------------------------

    private void registerBuiltInScalars() {
        // String
        putScalar("UPPER",       args -> str(args[0]) == null ? null : str(args[0]).toUpperCase());
        putScalar("LOWER",       args -> str(args[0]) == null ? null : str(args[0]).toLowerCase());
        putScalar("TRIM",        args -> {
            // Calcite emits TRIM as (flag, chars, input): flag ∈ {BOTH, LEADING, TRAILING},
            // chars is what to trim (usually ' '). Two-arg / one-arg forms also possible.
            String input; String chars; String flag;
            if (args.length == 3) { flag = str(args[0]); chars = str(args[1]); input = str(args[2]); }
            else if (args.length == 2) { flag = "BOTH"; chars = str(args[0]); input = str(args[1]); }
            else { flag = "BOTH"; chars = " "; input = str(args[0]); }
            if (input == null) return null;
            if (chars == null || chars.isEmpty()) chars = " ";
            int lo = 0, hi = input.length();
            if ("BOTH".equals(flag) || "LEADING".equals(flag))
                while (lo < hi && chars.indexOf(input.charAt(lo)) >= 0) lo++;
            if ("BOTH".equals(flag) || "TRAILING".equals(flag))
                while (hi > lo && chars.indexOf(input.charAt(hi - 1)) >= 0) hi--;
            return input.substring(lo, hi);
        });
        putScalar("CHAR_LENGTH", args -> str(args[0]) == null ? null : (long) str(args[0]).length());
        putScalar("CHARACTER_LENGTH", args -> str(args[0]) == null ? null : (long) str(args[0]).length());
        putScalar("LENGTH",      args -> str(args[0]) == null ? null : (long) str(args[0]).length());
        putScalar("||",          args -> {
            StringBuilder sb = new StringBuilder();
            for (Object a : args) sb.append(a == null ? "" : a);
            return sb.toString();
        });
        putScalar("CONCAT",      args -> {
            StringBuilder sb = new StringBuilder();
            for (Object a : args) sb.append(a == null ? "" : a);
            return sb.toString();
        });
        putScalar("SUBSTRING", args -> {
            String s = str(args[0]);
            if (s == null) return null;
            int from1 = (int) lng(args[1]);  // SQL uses 1-based indexing
            int from = Math.max(0, from1 - 1);
            if (args.length == 2) return s.substring(Math.min(from, s.length()));
            int len = (int) lng(args[2]);
            int to = Math.min(from + Math.max(0, len), s.length());
            return s.substring(Math.min(from, s.length()), to);
        });
        putScalar("REPLACE", args -> {
            String s = str(args[0]); String from = str(args[1]); String to = str(args[2]);
            if (s == null || from == null || to == null) return null;
            return s.replace(from, to);
        });
        putScalar("POSITION", args -> {
            String needle = str(args[0]); String hay = str(args[1]);
            if (needle == null || hay == null) return null;
            int i = hay.indexOf(needle);
            return (long) (i < 0 ? 0 : i + 1);
        });

        // Math
        putScalar("ABS",   args -> args[0] == null ? null : Math.abs(dbl(args[0])));
        putScalar("ROUND", args -> args[0] == null ? null : (double) Math.round(dbl(args[0])));
        putScalar("CEIL",  args -> args[0] == null ? null : Math.ceil(dbl(args[0])));
        putScalar("CEILING", args -> args[0] == null ? null : Math.ceil(dbl(args[0])));
        putScalar("FLOOR", args -> args[0] == null ? null : Math.floor(dbl(args[0])));
        putScalar("MOD",   args -> args[0] == null || args[1] == null ? null : lng(args[0]) % lng(args[1]));
        putScalar("POWER", args -> args[0] == null || args[1] == null ? null : Math.pow(dbl(args[0]), dbl(args[1])));
        putScalar("SQRT",  args -> args[0] == null ? null : Math.sqrt(dbl(args[0])));

        // JVS-specific
        putScalar("JPATH", args -> {
            // Reads a JVS dotted path from the currently-bound row.
            String path = str(args[0]);
            JVS row = currentRow();
            if (row == null || path == null) return null;
            try {
                Propaccess p = new Propaccess(path);
                JsonNode v = p.get(row, row.getJsonNode(), PAContext.AlwaysCreate);
                if (v == null || v.isNull()) return null;
                return RexEvaluator.unwrap(v);
            } catch (Exception e) {
                return null;
            }
        });

        putScalar("MLS", args -> {
            // Extract the text for a given language from an MLS envelope.
            // The first arg is expected to be a JsonNode (raw MLS structure); the
            // second is the language code ('en', 'fr', ...). One-arg form uses "en".
            Object envelope = args[0];
            String lang = args.length > 1 ? str(args[1]) : "en";
            if (envelope == null || lang == null) return null;
            JsonNode env = envelope instanceof JsonNode jn ? jn : null;
            if (env == null) return null;
            // Standard JVS MLS shape: {"mls": [{"lang": "en", "text": "..."}, ...]}
            JsonNode mls = env.get("mls");
            if (mls == null || !mls.isArray()) return null;
            for (JsonNode item : mls) {
                JsonNode l = item.get("lang");
                if (l != null && lang.equalsIgnoreCase(l.asText())) {
                    JsonNode t = item.get("text");
                    return t == null ? null : t.asText();
                }
            }
            return null;
        });

        // Windowing scalars — compute bucket boundaries for tumbling / hopping windows.
        // For Phase 2 batch-mode windowed aggregation: use these to GROUP BY on the
        // window start/end. Phase 2-late adds the streaming TABLE(TUMBLE(...)) syntax
        // with incremental emit as watermarks advance.
        putScalar("WIN_START", args -> {
            Long ts = numToLong(args[0]);
            Long size = numToLong(args[1]);
            if (ts == null || size == null || size <= 0) return null;
            return Math.floorDiv(ts, size) * size;
        });
        putScalar("WIN_END", args -> {
            Long ts = numToLong(args[0]);
            Long size = numToLong(args[1]);
            if (ts == null || size == null || size <= 0) return null;
            return Math.floorDiv(ts, size) * size + size;
        });
        putScalar("WIN_HOP_STARTS", args -> {
            // Returns the list of window-start times a given event belongs to under
            // a HOP(size, slide) schedule. Users can UNNEST to duplicate rows across windows.
            Long ts = numToLong(args[0]);
            Long size = numToLong(args[1]);
            Long slide = numToLong(args[2]);
            if (ts == null || size == null || slide == null || size <= 0 || slide <= 0) return null;
            java.util.List<Long> out = new java.util.ArrayList<>();
            long firstStart = Math.floorDiv(ts - size + slide, slide) * slide;
            for (long s = firstStart; s <= ts; s += slide) {
                if (ts < s + size) out.add(s);
            }
            return out;
        });

        putScalar("WIN_STRUCT", args -> {
            Long ts = numToLong(args[0]);
            Long size = numToLong(args[1]);
            if (ts == null || size == null || size <= 0) return null;
            long start = Math.floorDiv(ts, size) * size;
            var node = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
            node.put("start", start);
            node.put("end", start + size);
            return node;
        });

        putScalar("MLS_LANGS", args -> {
            Object envelope = args[0];
            if (!(envelope instanceof JsonNode env)) return null;
            JsonNode mls = env.get("mls");
            if (mls == null || !mls.isArray()) return null;
            java.util.List<String> out = new java.util.ArrayList<>();
            for (JsonNode item : mls) {
                JsonNode l = item.get("lang");
                if (l != null) out.add(l.asText());
            }
            return out;
        });
    }

    // -- small coercion helpers ----------------------------------------------

    private static Long numToLong(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        try { return Long.parseLong(o.toString()); } catch (Exception e) { return null; }
    }

    private static String str(Object o) { return o == null ? null : o.toString(); }
    private static long   lng(Object o) {
        if (o == null) return 0L;
        if (o instanceof Number n) return n.longValue();
        return Long.parseLong(o.toString());
    }
    private static double dbl(Object o) {
        if (o == null) return 0.0;
        if (o instanceof Number n) return n.doubleValue();
        if (o instanceof BigDecimal bd) return bd.doubleValue();
        return Double.parseDouble(o.toString());
    }
}
