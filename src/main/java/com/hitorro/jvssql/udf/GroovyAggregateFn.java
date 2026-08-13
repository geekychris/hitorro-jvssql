/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.exec.AggregateFn;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles a Groovy aggregate specification. The script defines three closures
 * (init / accum / result) as key: value pairs. Example:
 *
 * <pre>{@code
 * init:   { [] as List }
 * accum:  { acc, v -> acc.add(v); acc }
 * result: { acc -> acc.join(',') }
 * }</pre>
 */
public final class GroovyAggregateFn {

    private GroovyAggregateFn() {}

    public static AggregateFn compile(String script) {
        String initSrc = extract(script, "init");
        String accumSrc = extract(script, "accum");
        String resultSrc = extract(script, "result");
        Object initClosure = compileClosure(initSrc);
        Object accumClosure = compileClosure(accumSrc);
        Object resultClosure = compileClosure(resultSrc);
        return new AggregateFn() {
            @Override public Object createAccumulator() { return callClosure(initClosure); }
            @Override public void accumulate(Object acc, Object v) { callClosure(accumClosure, acc, v); }
            @Override public Object result(Object acc) { return callClosure(resultClosure, acc); }
        };
    }

    private static String extract(String script, String key) {
        Pattern p = Pattern.compile("(?m)^\\s*" + key + "\\s*:\\s*(\\{[\\s\\S]*?\\})\\s*$");
        Matcher m = p.matcher(script);
        if (!m.find()) throw new JvsSqlException("Groovy UDAF is missing '" + key + ":' closure");
        return m.group(1);
    }

    private static Object compileClosure(String closureSrc) {
        try {
            Class<?> shellCls = Class.forName("groovy.lang.GroovyShell");
            Object shell = shellCls.getDeclaredConstructor().newInstance();
            return shellCls.getMethod("evaluate", String.class).invoke(shell, closureSrc);
        } catch (ClassNotFoundException e) {
            throw new JvsSqlException("Groovy is not on the classpath — add org.apache.groovy:groovy to use registerGroovyAggregate()", e);
        } catch (Exception e) {
            throw new JvsSqlException("cannot compile Groovy UDAF closure: " + closureSrc + " — " + e.getMessage(), e);
        }
    }

    private static Object callClosure(Object closure, Object... args) {
        try {
            java.lang.reflect.Method call = closure.getClass().getMethod("call", Object[].class);
            return call.invoke(closure, (Object) args);
        } catch (Exception e) {
            throw new JvsSqlException("Groovy UDAF closure invocation failed: " + e.getMessage(), e);
        }
    }
}
