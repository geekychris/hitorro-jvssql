/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

/** Thrown for parse, validation, planning, or runtime failures in the JVS SQL engine. */
public class JvsSqlException extends RuntimeException {
    public JvsSqlException(String msg) { super(msg); }
    public JvsSqlException(String msg, Throwable cause) { super(msg, cause); }
}
