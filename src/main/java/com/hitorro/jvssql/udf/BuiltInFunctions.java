/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

/**
 * Reflection targets that Calcite's validator inspects to type-check calls to
 * DYNAMIC() and MLS(). The bodies are never executed — the {@code Executor} sees
 * the RexCall for these operators and dispatches to
 * {@link com.hitorro.jvssql.exec.FunctionRegistry} by name.
 */
public final class BuiltInFunctions {

    private BuiltInFunctions() {}

    /** JPATH(path) — reads a JVS dotted path from the current row (escape hatch for undeclared fields).
     *  Return type is ANY; caller should treat the result as untyped.
     *  <p>Named {@code JPATH} rather than {@code DYNAMIC} because DYNAMIC is a SQL:2016 reserved word.</p> */
    public static Object JPATH(String path) { throw new UnsupportedOperationException("dispatched by executor"); }

    /** MLS(mls_envelope, lang) — extracts the {@code lang} text from an MLS envelope. */
    public static String MLS(Object envelope, String lang) { throw new UnsupportedOperationException("dispatched by executor"); }

    /** MLS_LANGS(mls_envelope) — returns list of languages present in the envelope. */
    public static Object MLS_LANGS(Object envelope) { throw new UnsupportedOperationException("dispatched by executor"); }
}
