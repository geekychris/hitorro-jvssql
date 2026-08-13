/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.config;

/**
 * Per-stream configuration: event-time attribute, allowed lateness, optional
 * user-supplied watermark strategy. Given when a stream is registered with
 * {@code JvsSqlEngine.Builder.registerStream}.
 *
 * <p>Phase 1 uses only the event-time field name (dotted JVS path). Phase 2 wires
 * the field + allowedLateness into the WatermarkAssigner runtime operator.</p>
 */
public final class StreamConfig {

    private final String eventTimeField;
    private final long allowedLatenessMillis;

    private StreamConfig(Builder b) {
        this.eventTimeField = b.eventTimeField;
        this.allowedLatenessMillis = b.allowedLatenessMillis;
    }

    /** JVS dotted path to the event-time field, or {@code null} for non-streaming (batch) input. */
    public String eventTimeField() { return eventTimeField; }

    /** How out-of-order records may arrive relative to the current watermark before being late. */
    public long allowedLatenessMillis() { return allowedLatenessMillis; }

    public boolean isStreaming() { return eventTimeField != null; }

    public static Builder builder() { return new Builder(); }

    /** Shortcut for {@code builder().eventTimeField(path).build()}. */
    public static StreamConfig eventTime(String path) {
        return builder().eventTimeField(path).build();
    }

    /** Batch mode (no event time, no watermarks). The default for finite iterators. */
    public static StreamConfig batch() {
        return builder().build();
    }

    public static final class Builder {
        private String eventTimeField;
        private long allowedLatenessMillis = 0L;

        public Builder eventTimeField(String path) { this.eventTimeField = path; return this; }
        public Builder allowedLatenessMillis(long ms) { this.allowedLatenessMillis = ms; return this; }
        public StreamConfig build() { return new StreamConfig(this); }
    }
}
