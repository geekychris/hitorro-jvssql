/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.udf;

import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.jvssql.exec.ScalarFn;

/**
 * Compiles a Groovy script snippet into a {@link ScalarFn}. Args are bound
 * into the script as {@code arg}, {@code arg1..argN}. The script's return
 * value becomes the function result.
 *
 * <p>Groovy is an optional {@code provided} dependency; if it's not on the
 * classpath at runtime, {@link #compile(String, int)} throws with a clear
 * message. Arity must be declared at registration so Calcite's validator
 * can type-check calls (Groovy closures don't carry SQL-compatible metadata).</p>
 */
public final class GroovyScalarFn {

    private GroovyScalarFn() {}

    /** Backwards-compatible one-arg form. */
    public static ScalarFn compile(String script) { return compile(script, 1); }

    public static ScalarFn compile(String script, int arity) {
        // Reflect-load Groovy so hitorro-jvssql compiles even when groovy is absent.
        try {
            Class<?> shellCls = Class.forName("groovy.lang.GroovyShell");
            Class<?> bindingCls = Class.forName("groovy.lang.Binding");
            Class<?> compiledScriptCls = Class.forName("groovy.lang.Script");
            Object shell = shellCls.getDeclaredConstructor().newInstance();
            Object parsed = shellCls.getMethod("parse", String.class).invoke(shell, script);
            return args -> {
                try {
                    Object binding = bindingCls.getDeclaredConstructor().newInstance();
                    java.lang.reflect.Method setVar = bindingCls.getMethod("setVariable", String.class, Object.class);
                    for (int i = 0; i < args.length; i++) {
                        setVar.invoke(binding, "arg" + (i + 1), args[i]);
                        if (i == 0) setVar.invoke(binding, "arg", args[i]);
                    }
                    compiledScriptCls.getMethod("setBinding", bindingCls).invoke(parsed, binding);
                    return compiledScriptCls.getMethod("run").invoke(parsed);
                } catch (Exception e) {
                    throw new JvsSqlException("groovy scalar UDF failed: " + e.getMessage(), e);
                }
            };
        } catch (ClassNotFoundException e) {
            throw new JvsSqlException("Groovy is not on the classpath — add org.apache.groovy:groovy to your project to use registerGroovyFunction()", e);
        } catch (Exception e) {
            throw new JvsSqlException("cannot compile Groovy UDF: " + e.getMessage(), e);
        }
    }
}
