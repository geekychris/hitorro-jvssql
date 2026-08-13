/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

/**
 * Runtime shape of a scalar function callable from SQL. Args come in as Java
 * values (String, Long, Double, Boolean, {@code null}, or a raw
 * {@link com.fasterxml.jackson.databind.JsonNode} for structured types).
 * The return value is coerced back to a JsonNode by {@link RexEvaluator#wrap}.
 */
@FunctionalInterface
public interface ScalarFn {
    Object call(Object[] args);
}
