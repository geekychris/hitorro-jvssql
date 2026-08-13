/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.exec.AggregateFn;

import java.lang.reflect.Method;

/**
 * Wraps a Java class as an {@link AggregateFn}. The class must have three
 * public methods:
 * <ul>
 *   <li>{@code Object createAccumulator()}</li>
 *   <li>{@code void accumulate(Object acc, Object value)}</li>
 *   <li>{@code Object result(Object acc)}</li>
 * </ul>
 *
 * <p>Or if it implements {@link AggregateFn} directly, we just instantiate it.</p>
 */
public final class JavaAggregateFn {

    private JavaAggregateFn() {}

    public static AggregateFn wrap(Class<?> cls) {
        try {
            Object instance = cls.getDeclaredConstructor().newInstance();
            if (instance instanceof AggregateFn a) return a;
            Method create = cls.getMethod("createAccumulator");
            Method accum = findMethod(cls, "accumulate", 2);
            Method result = cls.getMethod("result", Object.class);
            return new AggregateFn() {
                @Override public Object createAccumulator() {
                    return invoke(create, instance);
                }
                @Override public void accumulate(Object acc, Object v) {
                    invoke(accum, instance, acc, v);
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
