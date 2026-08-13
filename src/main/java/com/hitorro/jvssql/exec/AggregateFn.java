/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

/**
 * Runtime shape of an aggregate function (UDAF). One accumulator instance per
 * group; {@link #accumulate} is called for each row in the group;
 * {@link #result} produces the final value once the group is closed.
 */
public interface AggregateFn {
    /** Fresh accumulator. Called once per group. */
    Object createAccumulator();

    /** Fold a row's aggregated value into the accumulator. */
    void accumulate(Object accumulator, Object value);

    /** Extract the final aggregate value. */
    Object result(Object accumulator);
}
