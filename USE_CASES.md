# hitorro-jvssql — Use Cases

Where this engine fits, where it doesn't, and what real workloads look
like against it.

## Where hitorro-jvssql fits

The sweet spot is **structured-JSON analytics inside a JVM process where
the shape of the data is known and the query is dynamic**. If you find
yourself writing `.stream().filter().collect(Collectors.groupingBy(...))`
against a stream of Jackson `JsonNode`s and wishing you had SQL, this is
the tool.

**Concretely, it fits when:**

- Input rows are JVS documents (or JSON that fits a JVS type)
- Users or callers should be able to express queries in SQL, not code
- You want to run the query inside the same JVM as the data (no cluster,
  no wire protocol)
- Windowed aggregation, joins, and sorted output are useful

**It doesn't fit when:**

- You're serving thousands of concurrent queries per second against a
  shared datastore → use a real database (Postgres, DuckDB, ClickHouse)
- You need distributed execution across nodes → Flink, Spark SQL, Trino
- Your data is columnar and cardinality is >>100M rows → DuckDB, Polars,
  ClickHouse
- Queries are essentially fixed and you can hand-code the pipeline →
  hand-code it; SQL is only worth the overhead when queries vary

The engine is a component you embed in a larger app. It doesn't manage
storage, replication, or query queueing.

## Use case: real-time event enrichment

**Scenario:** an ingestion pipeline sees a stream of raw event JSON.
Each event has a customer identifier. You want to enrich each event with
the customer's segment/tenure/name, filter to high-value customers, and
route the result to Kafka.

```java
BaseFile customers = /* your customer dimension file, refreshed hourly */;

JvsSqlEngine engine = JvsSqlEngine.builder()
    .registerStream("events", eventStreamIter, eventType,
        StreamConfig.eventTime("ts"))
    .registerReferenceTable("customers", customers, customerType,
        RefreshPolicy.every(Duration.ofHours(1)))
    .registerFunction("NORMALIZE_EMAIL", NormalizeEmail.class)
    .build();

PreparedQuery q = engine.compile("""
    SELECT e.ts, e.action, c.name, c.segment,
           NORMALIZE_EMAIL(c.email) AS email
    FROM   events e
    JOIN   customers c ON e.customer_id = c.id
    WHERE  c.segment IN ('enterprise', 'strategic')
    """);

q.execute(kafkaSink);   // pushes JsonNode rows into your Kafka producer
```

**Why this fits:** streaming source, dimension enrichment via a
periodically-refreshed reference table, custom UDF for domain-specific
normalization, push-based output that composes with any `Sink`. Runs as
long as `eventStreamIter.hasNext()`.

## Use case: sessionization for product analytics

**Scenario:** clickstream analytics. You want per-session metrics —
number of pageviews, session duration, conversion detection — with
"session" defined as ≥30 minutes of inactivity.

```java
Iterator<JVS> clicks = StreamSources.ndjson(clickstreamFile);

Iterator<JVS> sessioned = SessionWindows.sessionize(
    clicks, /*key*/ "user_id", /*time*/ "ts", /*gap*/ 30 * 60_000L);

JvsSqlEngine engine = JvsSqlEngine.builder()
    .registerStream("sessions", sessioned, sessionedType)
    .build();

PreparedQuery q = engine.compile("""
    SELECT user_id, session_start,
           (session_end - session_start) / 1000 AS duration_s,
           COUNT(*)               AS pageviews,
           SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS conversions
    FROM   sessions
    GROUP BY user_id, session_start, session_end
    ORDER BY duration_s DESC
    """);
```

**Why this fits:** SESSION windows aren't in most embedded SQL engines;
here you get them via a preprocessing helper that plugs into the same
`GROUP BY` machinery. Sort spills to disk automatically if the session
count exceeds memory.

## Use case: hourly aggregation with tight latency

**Scenario:** you're computing metrics per hour per department, and you
want each hour's row to be emitted as soon as that hour "closes" — not
at end-of-stream. Downstream consumers pull from `PreparedQuery.asIterator()`
and react to each row.

```java
JvsSqlEngine engine = JvsSqlEngine.builder()
    .registerStream("events", eventStream, eventType,
        StreamConfig.builder()
            .eventTimeField("event_time")
            .allowedLatenessMillis(60_000)  // tolerate 1 minute of skew
            .build())
    .withLateDataSink("events", laterecords)
    .build();

var it = engine.compile("""
    SELECT WIN_START(event_time, 3600000) AS window_start,
           dept, COUNT(*) AS n, SUM(bytes) AS total
    FROM   events
    GROUP BY WIN_START(event_time, 3600000), dept
    """).asIterator();

while (it.hasNext()) {
    JsonNode hourResult = it.next();
    dashboardWebsocket.send(hourResult);  // arrives per-window, not per-day
}
```

**Why this fits:** `StreamingAggregate` (auto-detected because the group
is `WIN_START(event_time, ...)` and the stream has an event-time attr)
holds only currently-open windows. Watermark is the max observed
`event_time`; hour 9's row lands the instant the first event with
`event_time ≥ 10:00:00 + 1min` arrives. Late data goes to the side sink.

## Use case: order-shipment interval join

**Scenario:** two event streams — orders and shipments. Match them if
they share `order_id` AND the shipment happens within ±5 minutes of the
order.

```java
var engine = JvsSqlEngine.builder()
    .registerStream("orders",    orderStream,    orderType)
    .registerStream("shipments", shipmentStream, shipmentType)
    .build();

engine.compile("""
    SELECT o.order_id, o.customer, s.tracking,
           (s.ts - o.ts) AS lag_ms
    FROM   orders o
    JOIN   shipments s
      ON   o.order_id = s.order_id
      AND  s.ts BETWEEN o.ts - 300000 AND o.ts + 300000
    """);
```

**Why this fits:** the SQL is what you'd expect from Flink SQL, and it
lowers to a hash join with a residual — the equality becomes hash keys,
the `BETWEEN` becomes a residual condition evaluated on each combined
row. Small right side stays memory-resident; large right side + tight
latency would need a real streaming symmetric hash join, which is out of
scope for the current engine.

## Use case: multi-language content querying

**Scenario:** you're storing product descriptions with an MLS
(multi-language-string) envelope; queries need to project the English
text (with French fallback) into result rows.

```java
engine.compile("""
    SELECT product_id,
           COALESCE(MLS(description, 'en'),
                    MLS(description, 'fr'),
                    '(no translation)') AS description_text,
           MLS_LANGS(description) AS available_langs
    FROM   products
    WHERE  MLS(description, 'en') LIKE '%organic%'
    """);
```

**Why this fits:** `MLS` is first-class in the engine and knows the JVS
`core_mls` envelope shape. You get the accessor without writing your
own JSON traversal.

## Use case: schema-driven ETL

**Scenario:** you're transforming a stream of source records into a
different shape for a downstream consumer. Some fields are declared on
the type, some are dynamic experimental fields you want to preserve.

```java
engine.compile("""
    SELECT id,
           UPPER(TRIM(name))                   AS name_normalized,
           file_size / 1024                    AS size_kb,
           JPATH('extras.experimental_tier')   AS tier,
           CASE WHEN file_size > 1000000 THEN 'large'
                WHEN file_size > 1000    THEN 'medium'
                ELSE 'small' END               AS bucket
    FROM   docs
    WHERE  classification = 'public'
      AND  JPATH('extras.published') = 'true'
    """);
```

**Why this fits:** `JPATH()` is the escape hatch for undeclared fields.
Declared fields are typechecked at compile time (typos error out);
`JPATH('any.path')` opts into dynamic access with `null` returned on
missing paths. Best of both worlds.

## Use case: quick historical reporting from a file

**Scenario:** you have a day of NDJSON events on disk (or S3 via
BaseFile). Ad-hoc report — top 10 senders by bytes yesterday.

```java
BaseFile events = FileFileSystem.Root.getFile("/var/log/events-2026-08-11.ndjson");

var engine = JvsSqlEngine.builder()
    .registerStream("events", StreamSources.ndjson(events), eventType)
    .withSpillDirectory(spillDir)
    .withMemoryBudgetMB(2048)
    .build();

var rows = engine.compile("""
    SELECT sender, SUM(bytes) AS total, COUNT(*) AS n
    FROM   events
    WHERE  status = 'ok'
    GROUP BY sender
    ORDER BY total DESC LIMIT 10
    """).asIterator();
```

**Why this fits:** NDJSON reader is lazy, sort spills to disk if
`SUM(bytes)` group cardinality is high. No cluster, no service to run,
just a JVM process reading a file.

## Use case: computing UDFs that carry business logic

**Scenario:** your finance team has business rules that don't fit in
standard SQL — e.g., "revenue category" based on nested product metadata.
You write the rule once in Groovy and use it everywhere.

```java
engine = JvsSqlEngine.builder()
    .registerGroovyFunction("REVENUE_CATEGORY", 1, """
        def price = arg1?.price ?: 0
        def type = arg1?.type ?: 'unknown'
        if (type == 'subscription') return 'ARR'
        if (price > 10_000) return 'enterprise'
        if (price > 100)    return 'mid'
        return 'consumer'
    """)
    .registerStream("orders", orderStream, orderType).build();

engine.compile("""
    SELECT REVENUE_CATEGORY(product) AS category,
           SUM(amount) AS total_revenue,
           COUNT(*)    AS n_orders
    FROM   orders
    GROUP BY REVENUE_CATEGORY(product)
    """);
```

**Why this fits:** rules live where the rule owner can edit them (config,
DB, wiki) as a Groovy string, not in compiled Java. Runtime dispatch
means updates don't require a redeploy of the query engine.

## When NOT to use hitorro-jvssql

**Use a real OLAP database (DuckDB, ClickHouse, StarRocks) if:**
- Your data is columnar or fits nicely into a table
- You have >100M rows to query
- You need parallel query execution
- You want indexes, statistics, and true cost-based optimization

**Use Flink / Beam / Kafka Streams if:**
- You need distributed execution across nodes
- You need exactly-once processing guarantees against Kafka
- You have state that must survive process restarts
- You need to scale beyond a single JVM's memory

**Use Spark SQL if:**
- Your workload is batch over large datasets that don't fit in memory
- You need YARN/Kubernetes orchestration
- You've got existing Spark infrastructure

**Use raw Java streams if:**
- The query is fixed (won't change without a code change)
- Users don't need to author or modify SQL
- Performance is the top priority and every reflection call matters

**hitorro-jvssql's niche is small:** you have a stream of JVS documents,
you want SQL for flexibility, you want it inside your JVM with no
cluster, and you're OK with single-node throughput. That's a real niche
— it's what a lot of type-aware document pipelines need — but it's not
"a database".

## Performance rules of thumb

| Workload | Expected throughput |
|---|---|
| Simple SELECT/WHERE over NDJSON on local disk | ~500k rows/s per core |
| GROUP BY with 5-10 aggregates | ~200k rows/s |
| Hash join, right side is 10k-row ref table | ~250k left rows/s |
| ORDER BY, in-memory | ~150k rows/s |
| ORDER BY, spilling | ~50k rows/s (I/O bound) |
| Streaming aggregate over 1M active windows | comfortably fits in <1GB heap |

These are rough numbers on modest hardware. Add UDFs, wide rows, deep
nested JSON access → expect 2-4× slowdown. If throughput becomes the
bottleneck, add a benchmark for your exact query shape and measure.

## Integration patterns

**As a query service** — one long-lived `JvsSqlEngine` per data domain,
compile queries per user request, pull results via `asIterator()`.
Prepared queries are cheap; keeping the engine alive avoids
reregistering functions and reference tables on every request.

**As an ETL step** — build a fresh engine per job, register the sources
you need for that job, compile once, `.execute(sink)` to push results
downstream. Close the engine (`engine.close()`) when done to stop any
scheduled reference-table refreshers.

**As a validation layer** — register your domain type, run queries
against synthetic test data, assert the shape/values of results. The
same query that runs in production can be tested against fixture
iterators — no separate mock layer.

**Behind a REST endpoint** — accept a SQL string, `compile(sql)`,
`.execute(streamingHttpSink)` to stream results as newline-delimited
JSON. Compile errors → HTTP 400 with a clear message.

## Reading the sources

If you want to understand a specific operator, the code is small and
readable:

- `Executor.java` — 500 lines, one class per operator
- `RexEvaluator.java` — 300 lines, one method per SQL operator family
- `FunctionRegistry.java` — 200 lines, all built-in scalar functions
- `StreamingAggregate.java` — 180 lines, the entire streaming path
- `SessionWindows.java` — 130 lines, the whole session helper

Start with `Executor.build(RelNode)` and follow the dispatch. Each
operator's implementation fits on one screen.
