/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

import java.math.BigDecimal;

/**
 * Built-in {@link AggregateFn}s for the SQL standard aggregate library.
 * COUNT, SUM, AVG, MIN, MAX. UDAFs go into the same registry.
 *
 * <p>Numeric aggregates use {@link BigDecimal} internally to avoid double-precision
 * drift; the final {@link com.fasterxml.jackson.databind.node.NumericNode} is emitted
 * with whatever precision the accumulator built up.</p>
 */
public final class AggregateOps {

    private AggregateOps() {}

    public static AggregateFn count() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new long[]{0L}; }
            @Override public void accumulate(Object acc, Object v) {
                // COUNT(*) passes null; COUNT(expr) only counts non-null.
                if (v != null) ((long[]) acc)[0]++;
            }
            @Override public Object result(Object acc) { return ((long[]) acc)[0]; }
        };
    }

    /** COUNT(*) — counts every row including NULLs. */
    public static AggregateFn countStar() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new long[]{0L}; }
            @Override public void accumulate(Object acc, Object v) { ((long[]) acc)[0]++; }
            @Override public Object result(Object acc) { return ((long[]) acc)[0]; }
        };
    }

    public static AggregateFn sum() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new SumAcc(); }
            @Override public void accumulate(Object acc, Object v) {
                if (v == null) return;
                SumAcc s = (SumAcc) acc;
                s.total = s.total.add(toDecimal(v));
                s.n++;
            }
            @Override public Object result(Object acc) {
                SumAcc s = (SumAcc) acc;
                return s.n == 0 ? null : s.total;
            }
        };
    }

    public static AggregateFn avg() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new SumAcc(); }
            @Override public void accumulate(Object acc, Object v) {
                if (v == null) return;
                SumAcc s = (SumAcc) acc;
                s.total = s.total.add(toDecimal(v));
                s.n++;
            }
            @Override public Object result(Object acc) {
                SumAcc s = (SumAcc) acc;
                if (s.n == 0) return null;
                return s.total.divide(BigDecimal.valueOf(s.n), java.math.MathContext.DECIMAL64);
            }
        };
    }

    public static AggregateFn min() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new Object[]{null}; }
            @Override public void accumulate(Object acc, Object v) {
                if (v == null) return;
                Object[] holder = (Object[]) acc;
                if (holder[0] == null || cmp(v, holder[0]) < 0) holder[0] = v;
            }
            @Override public Object result(Object acc) { return ((Object[]) acc)[0]; }
        };
    }

    public static AggregateFn max() {
        return new AggregateFn() {
            @Override public Object createAccumulator() { return new Object[]{null}; }
            @Override public void accumulate(Object acc, Object v) {
                if (v == null) return;
                Object[] holder = (Object[]) acc;
                if (holder[0] == null || cmp(v, holder[0]) > 0) holder[0] = v;
            }
            @Override public Object result(Object acc) { return ((Object[]) acc)[0]; }
        };
    }

    // -- helpers --------------------------------------------------------------

    private static class SumAcc {
        BigDecimal total = BigDecimal.ZERO;
        long n = 0;
    }

    private static BigDecimal toDecimal(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        if (v instanceof Long l) return BigDecimal.valueOf(l);
        if (v instanceof Integer i) return BigDecimal.valueOf(i);
        if (v instanceof Double d) return BigDecimal.valueOf(d);
        if (v instanceof Number n) return new BigDecimal(n.toString());
        return new BigDecimal(v.toString());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int cmp(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return toDecimal(a).compareTo(toDecimal(b));
        }
        if (a instanceof Comparable ca && b instanceof Comparable) {
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}
