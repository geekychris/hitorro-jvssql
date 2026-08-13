/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.exec.ScalarFn;

import java.lang.reflect.Method;

/**
 * Reflects on a user-provided Java class to find its {@code eval} method,
 * then wraps it as a {@link ScalarFn}. The class must have a public
 * no-arg constructor and exactly one public method named {@code eval}.
 *
 * <p>Argument arity/type-mapping: whatever the reflected method accepts.
 * Callers wire the class name into Calcite's schema so the validator sees
 * the same method signature.</p>
 */
public final class JavaScalarFn {

    private JavaScalarFn() {}

    public static ScalarFn wrap(Class<?> cls) {
        Method eval = findEval(cls);
        try {
            final Object instance = cls.getDeclaredConstructor().newInstance();
            final Class<?>[] paramTypes = eval.getParameterTypes();
            return args -> {
                try {
                    Object[] converted = coerceArgs(args, paramTypes);
                    return eval.invoke(instance, converted);
                } catch (Exception e) {
                    throw new JvsSqlException("UDF " + cls.getSimpleName() + ".eval failed: " + e.getMessage(), e);
                }
            };
        } catch (Exception e) {
            throw new JvsSqlException("cannot instantiate UDF class " + cls.getName(), e);
        }
    }

    private static Method findEval(Class<?> cls) {
        for (Method m : cls.getMethods()) {
            if ("eval".equals(m.getName())) return m;
        }
        throw new JvsSqlException("UDF class " + cls.getName() + " has no public eval() method");
    }

    private static Object[] coerceArgs(Object[] args, Class<?>[] paramTypes) {
        Object[] out = new Object[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            Object v = i < args.length ? args[i] : null;
            out[i] = coerce(v, paramTypes[i]);
        }
        return out;
    }

    private static Object coerce(Object v, Class<?> target) {
        if (v == null) return null;
        if (target.isInstance(v)) return v;
        if (target == String.class) return v.toString();
        if (target == long.class || target == Long.class) return toLong(v);
        if (target == int.class || target == Integer.class) return (int) toLong(v);
        if (target == double.class || target == Double.class) return toDouble(v);
        if (target == float.class || target == Float.class) return (float) toDouble(v);
        if (target == boolean.class || target == Boolean.class) return toBool(v);
        return v; // pass-through — reflection may still succeed via widening
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }
    private static double toDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }
    private static boolean toBool(Object v) {
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString());
    }
}
