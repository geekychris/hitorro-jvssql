/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

/**
 * Reflection targets Calcite's validator uses to type-check Groovy UDF calls.
 * Each static method has a fixed arity; when a Groovy scalar is registered we
 * pick the stub whose arity matches the closure's parameter count and hand
 * that reflected method to {@code ScalarFunctionImpl.create}. Bodies are
 * never executed — the executor dispatches by function name via the
 * {@link com.hitorro.jvssql.exec.FunctionRegistry} at runtime.
 *
 * <p>Return type is {@code Object} → Calcite treats it as ANY, which matches
 * the untyped nature of Groovy closures. Users who want stricter typing
 * should register a Java UDF instead.</p>
 */
public final class GroovyStubs {

    private GroovyStubs() {}

    // Return String rather than Object so Calcite maps it to VARCHAR (a real SqlTypeName)
    // instead of OTHER — LIKE and comparisons work naturally. At runtime the actual
    // value (Boolean / Long / etc.) is preserved by our JsonNode-based dispatch.
    public static String stub0() { throw dispatched(); }
    public static String stub1(Object a) { throw dispatched(); }
    public static String stub2(Object a, Object b) { throw dispatched(); }
    public static String stub3(Object a, Object b, Object c) { throw dispatched(); }
    public static String stub4(Object a, Object b, Object c, Object d) { throw dispatched(); }
    public static String stub5(Object a, Object b, Object c, Object d, Object e) { throw dispatched(); }

    private static UnsupportedOperationException dispatched() {
        return new UnsupportedOperationException(
            "GroovyStubs.stub* are for Calcite validation only — runtime dispatch goes through FunctionRegistry");
    }

    /** Reflect a stub of the requested arity. */
    public static java.lang.reflect.Method forArity(int arity) {
        String name = "stub" + arity;
        for (java.lang.reflect.Method m : GroovyStubs.class.getMethods()) {
            if (name.equals(m.getName())) return m;
        }
        throw new IllegalArgumentException("no Groovy UDF stub for arity " + arity
            + " (max is 5; register a Java UDF for higher arity)");
    }
}
