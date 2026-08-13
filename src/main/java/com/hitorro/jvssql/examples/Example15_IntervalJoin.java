/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.examples;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;

import java.util.List;

import static com.hitorro.jvssql.examples.ExampleSupport.MAPPER;
import static com.hitorro.jvssql.examples.ExampleSupport.printQuery;

/**
 * Stream × stream time-bounded interval JOIN. Two event streams are joined
 * on an equality key AND a temporal window — a match only counts if the
 * right event's timestamp is within {@code ±window} of the left event's
 * timestamp.
 *
 * <p>Under the hood: the equality becomes a hash-join key; the {@code BETWEEN}
 * predicate becomes a residual condition evaluated on the combined row after
 * each hash match. Rows that fail the residual don't emit; for LEFT joins the
 * left row still emits with NULL on the right if no right row survives.</p>
 *
 * <p><b>Run:</b> {@code mvn -pl hitorro-jvssql exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.Example15_IntervalJoin"}</p>
 */
public final class Example15_IntervalJoin {

    private static Type ordersType() throws Exception {
        String typeJson = "{\"name\":\"orders\",\"fields\":["
            + "{\"name\":\"order_id\", \"type\":\"core_string\"},"
            + "{\"name\":\"customer\", \"type\":\"core_string\"},"
            + "{\"name\":\"o_ts\",     \"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static Type shipmentsType() throws Exception {
        String typeJson = "{\"name\":\"shipments\",\"fields\":["
            + "{\"name\":\"order_id\", \"type\":\"core_string\"},"
            + "{\"name\":\"tracking\", \"type\":\"core_string\"},"
            + "{\"name\":\"s_ts\",     \"type\":\"core_long\"}"
            + "]}";
        Type t = new Type();
        t.init(MAPPER.readTree(typeJson));
        return t;
    }

    private static JVS order(String id, String customer, long ts) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"order_id\":\"" + id + "\",\"customer\":\"" + customer + "\",\"o_ts\":" + ts + "}"));
    }

    private static JVS ship(String id, String tracking, long ts) throws Exception {
        return new JVS(MAPPER.readTree(
            "{\"order_id\":\"" + id + "\",\"tracking\":\"" + tracking + "\",\"s_ts\":" + ts + "}"));
    }

    public static void main(String[] args) throws Exception {
        // Orders and shipments arrive on separate streams. Match a shipment to an
        // order only if the shipment happened within 5 minutes of the order.
        long minute = 60_000L;

        var orders = List.of(
            order("o1", "alice", 100_000),           // t = 100s
            order("o2", "bob",   200_000),           // t = 200s
            order("o3", "carol", 5_000_000)          // t = 5000s
        ).iterator();

        var shipments = List.of(
            ship("o1", "TRK-A1", 120_000),           // ← matches o1 (+20s)
            ship("o1", "TRK-A2", 800_000),           // outside window; ignored
            ship("o2", "TRK-B1", 250_000),           // ← matches o2 (+50s)
            ship("o3", "TRK-C1", 5_100_000),         // ← matches o3 (+100s)
            ship("o3", "TRK-C2", 5_400_000)          // outside 5-min window; ignored
        ).iterator();

        var engine = JvsSqlEngine.builder()
            .registerStream("orders",    orders,    ordersType())
            .registerStream("shipments", shipments, shipmentsType())
            .build();

        printQuery("Interval JOIN — orders matched to shipments within ±5 min",
            engine.compile(
                "SELECT o.order_id, o.customer, s.tracking, "
              + "       o.o_ts AS order_ts, s.s_ts AS ship_ts, "
              + "       (s.s_ts - o.o_ts) AS lag_ms "
              + "FROM   orders o "
              + "JOIN   shipments s "
              + "  ON   o.order_id = s.order_id "
              + "  AND  s.s_ts BETWEEN o.o_ts - " + (5 * minute) + " "
              + "                  AND o.o_ts + " + (5 * minute) + " "
              + "ORDER BY o.o_ts, s.s_ts"));

        // Same but LEFT JOIN — orders without a valid shipment show up with NULL right side
        var orders2 = List.of(
            order("o1", "alice", 100_000),
            order("o2", "bob",   200_000),
            order("o4", "dave",  9_000_000)          // no shipment
        ).iterator();
        var shipments2 = List.of(
            ship("o1", "TRK-A1", 120_000),
            ship("o2", "TRK-B1", 250_000)
        ).iterator();
        var engine2 = JvsSqlEngine.builder()
            .registerStream("orders",    orders2,    ordersType())
            .registerStream("shipments", shipments2, shipmentsType())
            .build();

        printQuery("LEFT interval join — unshipped orders retained with NULL tracking",
            engine2.compile(
                "SELECT o.order_id, o.customer, "
              + "       COALESCE(s.tracking, '(not shipped)') AS tracking "
              + "FROM   orders o "
              + "LEFT JOIN shipments s "
              + "  ON   o.order_id = s.order_id "
              + "  AND  s.s_ts BETWEEN o.o_ts - " + (5 * minute) + " "
              + "                  AND o.o_ts + " + (5 * minute) + " "
              + "ORDER BY o.o_ts"));
    }
}
