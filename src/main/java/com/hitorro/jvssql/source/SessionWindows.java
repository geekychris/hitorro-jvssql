/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jvssql.JvsSqlException;
import com.hitorro.util.json.keys.propaccess.PAContext;
import com.hitorro.util.json.keys.propaccess.Propaccess;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * SESSION-window preprocessing: takes an input stream of {@link JVS} rows and
 * produces an augmented stream where each row carries two additional fields
 * — {@code session_start} and {@code session_end} — identifying the
 * session it belongs to. Users then aggregate over those columns with a
 * normal SQL {@code GROUP BY}.
 *
 * <h3>Session semantics</h3>
 * <p>A session is a contiguous run of events for the same <b>key</b> where
 * consecutive event times differ by no more than <b>gap</b> milliseconds.
 * As soon as a longer gap appears, a new session begins.</p>
 *
 * <h3>Algorithm</h3>
 * <p>Implemented as a single-pass buffer-then-emit: all input rows are
 * consumed, sorted by (key, event_time), and walked to compute session
 * boundaries. Memory is O(input row count). For very large streams a
 * follow-up can add an incremental variant that emits closed sessions
 * as watermarks advance — see the streaming-aggregate task.</p>
 *
 * <p>The augmented row's {@code session_start} equals the earliest event
 * time in the session; {@code session_end} equals the latest event time
 * (both inclusive). Downstream aggregators read them like any other
 * top-level field via {@code GROUP BY session_start, session_end}.</p>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * Iterator<JVS> sessioned = SessionWindows.sessionize(events, "user", "event_time", 30_000);
 * engine.registerStream("sessioned_events", sessioned, augmentedType);
 * engine.compile("SELECT user, session_start, session_end, COUNT(*), SUM(qty) " +
 *                "FROM sessioned_events GROUP BY user, session_start, session_end");
 * }</pre>
 */
public final class SessionWindows {

    private SessionWindows() {}

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    /**
     * @param source    input rows (any order — will be buffered + sorted)
     * @param keyPath   JVS dotted path identifying the "user" / session key
     * @param timePath  JVS dotted path to the event-time field (millis)
     * @param gapMillis inactivity gap beyond which a new session starts
     */
    public static Iterator<JVS> sessionize(Iterator<JVS> source, String keyPath, String timePath, long gapMillis) {
        Propaccess keyP = new Propaccess(keyPath);
        Propaccess timeP = new Propaccess(timePath);
        // Bucket by key so we can process each key's timeline independently.
        Map<String, List<Row>> perKey = new HashMap<>();
        while (source.hasNext()) {
            JVS jvs = source.next();
            String key = extractString(jvs, keyP);
            long ts = extractLong(jvs, timeP);
            perKey.computeIfAbsent(key, k -> new ArrayList<>()).add(new Row(ts, jvs));
        }
        // Assign session bounds per key.
        List<JVS> out = new ArrayList<>();
        for (Map.Entry<String, List<Row>> e : perKey.entrySet()) {
            List<Row> rows = e.getValue();
            rows.sort(Comparator.comparingLong(r -> r.ts));
            long sessionStart = -1;
            long sessionEnd = -1;
            List<Row> current = new ArrayList<>();
            for (Row r : rows) {
                if (current.isEmpty()) {
                    sessionStart = r.ts;
                    sessionEnd = r.ts;
                    current.add(r);
                } else if (r.ts - sessionEnd > gapMillis) {
                    // Flush the current session.
                    emitSession(current, sessionStart, sessionEnd, out);
                    current.clear();
                    sessionStart = r.ts;
                    sessionEnd = r.ts;
                    current.add(r);
                } else {
                    sessionEnd = r.ts;
                    current.add(r);
                }
            }
            if (!current.isEmpty()) {
                emitSession(current, sessionStart, sessionEnd, out);
            }
        }
        return out.iterator();
    }

    private static void emitSession(List<Row> rows, long start, long end, List<JVS> out) {
        for (Row r : rows) {
            JsonNode orig = r.jvs.getJsonNode();
            ObjectNode augmented = orig.isObject()
                    ? ((ObjectNode) orig.deepCopy())
                    : F.objectNode().set("_", orig);
            augmented.put("session_start", start);
            augmented.put("session_end",   end);
            out.add(new JVS(augmented));
        }
    }

    private static String extractString(JVS jvs, Propaccess p) {
        try {
            JsonNode v = p.get(jvs, jvs.getJsonNode(), PAContext.AlwaysCreate);
            return v == null || v.isNull() ? null : v.asText();
        } catch (Exception e) {
            return null;
        }
    }

    private static long extractLong(JVS jvs, Propaccess p) {
        try {
            JsonNode v = p.get(jvs, jvs.getJsonNode(), PAContext.AlwaysCreate);
            if (v == null || v.isNull()) return 0L;
            if (v.isNumber()) return v.asLong();
            // Fall back to ISO-8601 parse for string timestamps.
            return java.time.Instant.parse(v.asText()).toEpochMilli();
        } catch (Exception e) {
            throw new JvsSqlException("cannot read event_time at " + p + ": " + e.getMessage(), e);
        }
    }

    private record Row(long ts, JVS jvs) {}

    /**
     * Incremental sessionization: emits closed sessions as soon as the observed
     * event-time watermark passes {@code last_seen_ts_for_this_key + gap}. Memory
     * is O(open sessions across all keys), not O(input rows). Requires input to
     * be roughly in-order — heavily out-of-order inputs should use
     * {@link #sessionize} instead.
     *
     * <p>Each closed session emits its buffered rows augmented with
     * {@code session_start} and {@code session_end}, preserving original row
     * order within the session.</p>
     */
    public static Iterator<JVS> sessionizeIncremental(Iterator<JVS> source, String keyPath, String timePath, long gapMillis) {
        Propaccess keyP = new Propaccess(keyPath);
        Propaccess timeP = new Propaccess(timePath);
        return new Iterator<>() {
            // Per-key open session state: rows buffered so far, session_start, latest_ts.
            final Map<String, OpenSession> open = new HashMap<>();
            final java.util.ArrayDeque<JVS> emitQueue = new java.util.ArrayDeque<>();
            long maxObserved = Long.MIN_VALUE;
            boolean sourceExhausted = false;

            @Override public boolean hasNext() {
                while (emitQueue.isEmpty()) {
                    if (!sourceExhausted && source.hasNext()) {
                        JVS jvs = source.next();
                        String k = extractString(jvs, keyP);
                        long ts = extractLong(jvs, timeP);
                        maxObserved = Math.max(maxObserved, ts);
                        OpenSession sess = open.get(k);
                        if (sess == null) {
                            open.put(k, new OpenSession(ts, ts, jvs));
                        } else if (ts - sess.latestTs > gapMillis) {
                            emitClosed(k, sess);
                            open.put(k, new OpenSession(ts, ts, jvs));
                        } else {
                            sess.latestTs = ts;
                            sess.rows.add(jvs);
                        }
                        // Sweep other keys' sessions that have gone stale since we advanced the watermark.
                        java.util.Iterator<Map.Entry<String, OpenSession>> it = open.entrySet().iterator();
                        while (it.hasNext()) {
                            var e = it.next();
                            if (maxObserved - e.getValue().latestTs > gapMillis && !k.equals(e.getKey())) {
                                emitClosed(e.getKey(), e.getValue());
                                it.remove();
                            }
                        }
                    } else if (!sourceExhausted) {
                        sourceExhausted = true;
                    } else if (!open.isEmpty()) {
                        // Flush all remaining open sessions.
                        for (var e : open.entrySet()) emitClosed(e.getKey(), e.getValue());
                        open.clear();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            @Override public JVS next() {
                if (!hasNext()) throw new NoSuchElementException();
                return emitQueue.poll();
            }

            private void emitClosed(String key, OpenSession sess) {
                for (JVS r : sess.rows) {
                    JsonNode orig = r.getJsonNode();
                    ObjectNode augmented = orig.isObject()
                            ? ((ObjectNode) orig.deepCopy())
                            : F.objectNode().set("_", orig);
                    augmented.put("session_start", sess.startTs);
                    augmented.put("session_end",   sess.latestTs);
                    emitQueue.add(new JVS(augmented));
                }
            }
        };
    }

    private static final class OpenSession {
        long startTs;
        long latestTs;
        final List<JVS> rows = new ArrayList<>();
        OpenSession(long startTs, long latestTs, JVS first) {
            this.startTs = startTs;
            this.latestTs = latestTs;
            rows.add(first);
        }
    }

    /** Convenience: adds a nested {@code {"session":{"start":..., "end":...}}} object
     *  instead of flat {@code session_start} / {@code session_end} columns. */
    public static Iterator<JVS> sessionizeNested(Iterator<JVS> source, String keyPath, String timePath, long gapMillis) {
        Iterator<JVS> flat = sessionize(source, keyPath, timePath, gapMillis);
        return new Iterator<>() {
            @Override public boolean hasNext() { return flat.hasNext(); }
            @Override public JVS next() {
                JVS jvs = flat.next();
                ObjectNode obj = (ObjectNode) jvs.getJsonNode();
                long start = obj.remove("session_start").asLong();
                long end   = obj.remove("session_end").asLong();
                ObjectNode session = F.objectNode();
                session.put("start", start);
                session.put("end",   end);
                obj.set("session", session);
                return new JVS(obj);
            }
        };
    }
}
