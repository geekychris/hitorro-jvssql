/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.exec;

/**
 * Shared per-execution watermark state. {@link com.hitorro.jvssql.exec.Executor}
 * creates one instance per query run and hands it to any scan and streaming
 * operator that needs to coordinate on the current event-time watermark.
 *
 * <p>The scan's {@code WatermarkFilter} updates {@link #maxObservedEventTime}
 * as each row flows through; downstream operators like {@link StreamingAggregate}
 * read from here instead of approximating from group-column values.</p>
 *
 * <p>Not thread-safe — we rely on the single-threaded pull-execution model where
 * upstream and downstream operators run in sequence on the consumer's thread.</p>
 */
public final class WatermarkTracker {

    private long maxObservedEventTime = Long.MIN_VALUE;

    public long maxObservedEventTime() { return maxObservedEventTime; }

    public void observe(long eventTimeMillis) {
        if (eventTimeMillis > maxObservedEventTime) {
            maxObservedEventTime = eventTimeMillis;
        }
    }

    /** True when at least one event time has been observed. */
    public boolean isReady() { return maxObservedEventTime != Long.MIN_VALUE; }
}
