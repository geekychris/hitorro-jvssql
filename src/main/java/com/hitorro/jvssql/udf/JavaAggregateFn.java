/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.exec.AggregateFn;

import java.lang.reflect.Method;

/**
 * Wraps a Java class as an {@link AggregateFn}. The class must have three
 * public methods (matching Calcite's {@code AggregateFunctionImpl} convention):
 * <ul>
 *   <li>{@code Object init()}</li>
 *   <li>{@code Object add(Object acc, Object value)} — returns the (possibly-new) accumulator</li>
 *   <li>{@code Object result(Object acc)}</li>
 * </ul>
 * <p>Or if it implements {@link AggregateFn} directly, we just instantiate it.</p>
 */
public final class JavaAggregateFn {

    private JavaAggregateFn() {}

    public static AggregateFn wrap(Class<?> cls) {
        try {
            Object instance = cls.getDeclaredConstructor().newInstance();
            if (instance instanceof AggregateFn a) return a;
            Method init = cls.getMethod("init");
            Method add = findMethod(cls, "add", 2);
            Method result = cls.getMethod("result", Object.class);
            return new AggregateFn() {
                @Override public Object createAccumulator() {
                    return invoke(init, instance);
                }
                @Override public void accumulate(Object acc, Object v) {
                    // Users may either mutate acc in place (returning it) or return a new one.
                    // Since our AggregateFn interface holds acc by reference in the executor,
                    // we can't swap the reference — so require mutate-in-place semantics for
                    // wrapped classes. Users can wrap immutable acc in a Holder.
                    invoke(add, instance, acc, v);
                }
                @Override public Object result(Object acc) {
                    return invoke(result, instance, acc);
                }
            };
        } catch (Exception e) {
            throw new JvsSqlException("cannot wrap UDAF class " + cls.getName() + ": " + e.getMessage(), e);
        }
    }

    private static Method findMethod(Class<?> cls, String name, int arity) throws NoSuchMethodException {
        for (Method m : cls.getMethods()) {
            if (name.equals(m.getName()) && m.getParameterCount() == arity) return m;
        }
        throw new NoSuchMethodException(cls.getName() + "." + name + "/" + arity);
    }

    private static Object invoke(Method m, Object instance, Object... args) {
        try {
            return m.invoke(instance, args);
        } catch (Exception e) {
            throw new JvsSqlException("UDAF invoke failed: " + m + " — " + e.getMessage(), e);
        }
    }
}
