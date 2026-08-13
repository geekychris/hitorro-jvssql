/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.config;

import java.time.Duration;

/**
 * When to reload a reference table from its backing {@code BaseFile}.
 *
 * <ul>
 *   <li>{@link #once()} — load at engine build; never reload.</li>
 *   <li>{@link #every(Duration)} — reload on a schedule. In-flight queries continue
 *       against the old snapshot; the swap is atomic.</li>
 *   <li>{@link #onDemand()} — load at build; reload only when the caller invokes
 *       {@code JvsSqlEngine.refreshReferenceTable(name)}.</li>
 * </ul>
 */
public final class RefreshPolicy {

    public enum Kind { ONCE, SCHEDULED, ON_DEMAND }

    private final Kind kind;
    private final Duration interval;

    private RefreshPolicy(Kind kind, Duration interval) {
        this.kind = kind;
        this.interval = interval;
    }

    public Kind kind() { return kind; }
    public Duration interval() { return interval; }

    public static RefreshPolicy once() {
        return new RefreshPolicy(Kind.ONCE, null);
    }

    public static RefreshPolicy every(Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        return new RefreshPolicy(Kind.SCHEDULED, interval);
    }

    public static RefreshPolicy onDemand() {
        return new RefreshPolicy(Kind.ON_DEMAND, null);
    }
}
