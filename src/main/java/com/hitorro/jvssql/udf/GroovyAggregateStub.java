/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

/**
 * Reflection target Calcite's validator uses to type-check calls to Groovy
 * aggregate functions. Calcite {@code AggregateFunctionImpl.create(cls)}
 * inspects the class for three named methods:
 * <ul>
 *   <li>{@code init()} — creates an accumulator</li>
 *   <li>{@code add(acc, value)} — folds a value into the accumulator</li>
 *   <li>{@code result(acc)} — extracts the final aggregate value</li>
 * </ul>
 *
 * <p>Bodies are never executed — the executor sees an AggregateCall with our
 * registered name and looks up the real UDAF via
 * {@link com.hitorro.jvssql.exec.FunctionRegistry}. All Groovy aggregates
 * share this stub class; each registered <b>name</b> in the schema serves it
 * back to Calcite so calls type-check as this signature.</p>
 */
public final class GroovyAggregateStub {

    public static Object init() {
        throw new UnsupportedOperationException("dispatched by executor");
    }

    public static Object add(Object acc, Object value) {
        throw new UnsupportedOperationException("dispatched by executor");
    }

    public static Object result(Object acc) {
        throw new UnsupportedOperationException("dispatched by executor");
    }
}
