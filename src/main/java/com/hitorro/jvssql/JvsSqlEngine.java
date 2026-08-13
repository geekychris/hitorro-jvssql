/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.EngineConfig;
import com.hitorro.jvssql.config.RefreshPolicy;
import com.hitorro.jvssql.config.StreamConfig;
import com.hitorro.jvssql.exec.AggregateFn;
import com.hitorro.jvssql.exec.FunctionRegistry;
import com.hitorro.jvssql.exec.ScalarFn;
import com.hitorro.jvssql.schema.JvsSchema;
import com.hitorro.jvssql.schema.JvsTable;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Top-level entry point. Build with {@link #builder()}, register streams and
 * reference tables + optional UDFs, then {@link #compile(String)} to get a
 * {@link PreparedQuery} you execute repeatedly.
 *
 * <p>Phase 1 supports: SELECT with WHERE and column projection (with aliases and
 * expressions). Subsequent phases add hash aggregate, external sort, hash join
 * against reference tables, and — Phase 2+ — event-time windows.</p>
 *
 * <pre>{@code
 * JvsSqlEngine engine = JvsSqlEngine.builder()
 *     .withSpillDirectory(spillDir)
 *     .registerStream("docs", jvsIterator, docType)
 *     .build();
 *
 * PreparedQuery q = engine.compile("SELECT filename FROM docs WHERE file_size > 1000");
 *
 * // pull:
 * AbstractIterator<JsonNode> rows = q.asIterator();
 * while (rows.hasNext()) { ... }
 *
 * // push:
 * q.execute(mySinkOfJsonNode);
 * }</pre>
 */
public final class JvsSqlEngine {

    private final EngineConfig engineConfig;
    private final JvsSchema schema;
    private final FunctionRegistry functions;
    private final Map<String, Class<?>> userJavaScalars;
    private final Map<String, Class<?>> userJavaAggregates;
    private final Map<String, Integer> userGroovyScalars;   // name → arity
    private final java.util.Set<String> userGroovyAggregates;
    private final Map<String, com.hitorro.jvssql.refdata.RefTableSpec> refSpecs;
    private final Map<String, Sink<com.fasterxml.jackson.databind.JsonNode>> lateDataSinks;
    private final java.util.concurrent.ScheduledExecutorService refresher;

    private JvsSqlEngine(EngineConfig engineConfig, JvsSchema schema, FunctionRegistry functions,
                         Map<String, Class<?>> userJavaScalars,
                         Map<String, Class<?>> userJavaAggregates,
                         Map<String, Integer> userGroovyScalars,
                         java.util.Set<String> userGroovyAggregates,
                         Map<String, com.hitorro.jvssql.refdata.RefTableSpec> refSpecs,
                         Map<String, Sink<com.fasterxml.jackson.databind.JsonNode>> lateDataSinks) {
        this.engineConfig = engineConfig;
        this.schema = schema;
        this.functions = functions;
        this.userJavaScalars = userJavaScalars;
        this.userJavaAggregates = userJavaAggregates;
        this.userGroovyScalars = userGroovyScalars;
        this.userGroovyAggregates = userGroovyAggregates;
        this.refSpecs = refSpecs;
        this.lateDataSinks = lateDataSinks;
        // Spin up a small scheduler if any reference table needs periodic refresh.
        boolean anyScheduled = refSpecs.values().stream()
                .anyMatch(s -> s.policy().kind() == com.hitorro.jvssql.config.RefreshPolicy.Kind.SCHEDULED);
        if (anyScheduled) {
            this.refresher = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "jvssql-ref-refresher");
                t.setDaemon(true);
                return t;
            });
            for (var e : refSpecs.entrySet()) {
                var s = e.getValue();
                if (s.policy().kind() != com.hitorro.jvssql.config.RefreshPolicy.Kind.SCHEDULED) continue;
                long intervalMs = s.policy().interval().toMillis();
                refresher.scheduleWithFixedDelay(
                        () -> { try { refreshReferenceTable(s.name()); } catch (Exception ignored) {} },
                        intervalMs, intervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        } else {
            this.refresher = null;
        }
    }

    public EngineConfig config() { return engineConfig; }
    public JvsSchema schema() { return schema; }
    public FunctionRegistry functions() { return functions; }
    public Map<String, Sink<com.fasterxml.jackson.databind.JsonNode>> lateDataSinks() { return lateDataSinks; }

    /**
     * Parse, validate, plan, optimize the SQL. Returns a reusable prepared query.
     *
     * @throws JvsSqlException on parse, validation, or planning errors — including
     *         strict-with-escape violations (unknown identifier not wrapped in DYNAMIC()).
     */
    public PreparedQuery compile(String sql) {
        try {
            RelNode plan = doCompile(sql);
            return new PreparedQuery(this, sql, plan);
        } catch (JvsSqlException e) {
            throw e;
        } catch (Exception e) {
            throw new JvsSqlException("failed to compile SQL: " + sql, e);
        }
    }

    /**
     * Reload a reference table registered with a manual or scheduled refresh policy.
     * Swap is atomic — in-flight queries continue against the old snapshot until
     * their next execution starts. No-op for tables registered with
     * {@link RefreshPolicy#once()}.
     */
    public void refreshReferenceTable(String name) {
        var spec = refSpecs.get(name);
        if (spec == null) {
            throw new JvsSqlException("no reference table registered under name: " + name);
        }
        if (spec.policy().kind() == RefreshPolicy.Kind.ONCE) {
            return;    // silently ignore — user picked once()
        }
        java.util.List<JVS> reloaded = com.hitorro.jvssql.refdata.ReferenceTableLoader.load(spec.source());
        var table = schema.getJvsTable(name);
        if (table == null) {
            throw new JvsSqlException("reference table disappeared from schema: " + name);
        }
        table.setReferenceRows(reloaded);   // atomic swap of the volatile field
    }

    /**
     * Shut down the background refresh scheduler (if any). Idempotent. In-flight
     * queries continue to completion.
     */
    public void close() {
        if (refresher != null) refresher.shutdownNow();
    }

    // -- internals -----------------------------------------------------------

    private RelNode doCompile(String sql) throws Exception {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        SchemaPlus jvsSchema = rootSchema.add("jvs", schema);
        // Register built-in JVS-specific functions on the jvs schema — the
        // CalciteCatalogReader default path is "jvs" so unqualified calls resolve here.
        // Registering on both root and jvs would trip a Calcite duplicate-candidate assertion.
        for (var m : com.hitorro.jvssql.udf.BuiltInFunctions.class.getDeclaredMethods()) {
            if (java.lang.reflect.Modifier.isStatic(m.getModifiers()) && java.lang.reflect.Modifier.isPublic(m.getModifiers())) {
                jvsSchema.add(m.getName(), org.apache.calcite.schema.impl.ScalarFunctionImpl.create(m));
            }
        }
        for (var e : userJavaScalars.entrySet()) {
            jvsSchema.add(e.getKey().toUpperCase(),
                    org.apache.calcite.schema.impl.ScalarFunctionImpl.create(e.getValue(), "eval"));
        }
        for (var e : userJavaAggregates.entrySet()) {
            jvsSchema.add(e.getKey().toUpperCase(),
                    org.apache.calcite.schema.impl.AggregateFunctionImpl.create(e.getValue()));
        }
        // Groovy scalars: register a per-arity stub with Calcite so the validator
        // accepts calls. Runtime dispatch goes through FunctionRegistry.
        for (var e : userGroovyScalars.entrySet()) {
            java.lang.reflect.Method stub = com.hitorro.jvssql.udf.GroovyStubs.forArity(e.getValue());
            jvsSchema.add(e.getKey().toUpperCase(),
                    org.apache.calcite.schema.impl.ScalarFunctionImpl.create(stub));
        }
        // Groovy aggregates: all share the GroovyAggregateStub signature at plan
        // time; runtime dispatch goes through FunctionRegistry.getAggregate(name).
        for (String name : userGroovyAggregates) {
            jvsSchema.add(name.toUpperCase(),
                    org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
                            com.hitorro.jvssql.udf.GroovyAggregateStub.class));
        }
        // TODO Phase 1-late: aggregates + Groovy dispatch via a synthetic Java method
        // signature that the validator can consume.

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        props.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), "UNCHANGED");
        props.setProperty(CalciteConnectionProperty.QUOTED_CASING.camelName(), "UNCHANGED");
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(props);

        CalciteCatalogReader catalog = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                List.of("jvs"),
                typeFactory,
                connectionConfig);

        SqlParser.Config parserCfg = SqlParser.config()
                .withCaseSensitive(false)
                .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED)
                .withQuotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED);
        SqlParser parser = SqlParser.create(sql, parserCfg);
        SqlNode sqlNode = parser.parseQuery();

        // Transparent dotted-path support: rewrite multi-part identifiers such as
        // metadata.exp into JPATH('metadata.exp') calls before validation. See
        // JpathRewriter for the design rationale and known limits.
        sqlNode = JpathRewriter.rewrite(sqlNode);

        // Chain the standard SQL operators with our own catalog reader — the reader
        // exposes schema-registered functions (JPATH, MLS, user UDFs) to the validator.
        org.apache.calcite.sql.SqlOperatorTable opTable =
                org.apache.calcite.sql.util.SqlOperatorTables.chain(
                        SqlStdOperatorTable.instance(),
                        catalog);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                opTable,
                catalog,
                typeFactory,
                SqlValidator.Config.DEFAULT.withConformance(org.apache.calcite.sql.validate.SqlConformanceEnum.LENIENT));
        SqlNode validated = validator.validate(sqlNode);

        RelOptCluster cluster = RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(), rexBuilder);
        SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, viewName, schemaPath, viewPath) -> null,
                validator, catalog, cluster,
                org.apache.calcite.sql2rel.StandardConvertletTable.INSTANCE,
                SqlToRelConverter.CONFIG);
        RelNode logicalPlan = converter.convertQuery(validated, false, true).rel;

        // Phase 1 skips the Volcano optimizer sweep and hands the logical plan
        // straight to the executor, which handles Scan + Filter + Project. As soon
        // as aggregate / sort / join operators land we'll register their planner rules
        // and turn the optimizer on.
        return logicalPlan;
    }

    public static Builder builder() { return new Builder(); }

    // -- builder --------------------------------------------------------------

    public static final class Builder {
        private EngineConfig.Builder cfg = EngineConfig.builder();
        private final JvsSchema schema = new JvsSchema();
        private final FunctionRegistry functions = new FunctionRegistry();
        private final Map<String, Class<?>> userJavaScalars = new LinkedHashMap<>();
        private final Map<String, Class<?>> userJavaAggregates = new LinkedHashMap<>();
        private final Map<String, Integer> userGroovyScalars = new LinkedHashMap<>();
        private final java.util.Set<String> userGroovyAggregates = new java.util.LinkedHashSet<>();
        private final Map<String, com.hitorro.jvssql.refdata.RefTableSpec> refSpecs = new LinkedHashMap<>();
        private final Map<String, Sink<com.fasterxml.jackson.databind.JsonNode>> lateDataSinks = new LinkedHashMap<>();

        public Builder withSpillDirectory(BaseFile dir) { cfg.spillDir(dir); return this; }
        public Builder withMemoryBudgetMB(int mb) { cfg.memoryBudgetMB(mb); return this; }
        public Builder strictTypes(boolean strict) { cfg.strictTypes(strict); return this; }

        /** Register a batch input source — no event time, no watermarks. */
        public Builder registerStream(String name, Iterator<JVS> iter, Type type) {
            return registerStream(name, iter, type, StreamConfig.batch());
        }

        /**
         * Register a streaming input source. If {@link StreamConfig#eventTimeField()}
         * is set, the engine will assign watermarks as records arrive; otherwise the
         * source is treated as a finite batch.
         *
         * <p>The iterator is stored by reference. It is consumed once per query
         * execution — if multiple queries need to iterate the same source, wrap it
         * in a caching iterator upstream.</p>
         */
        public Builder registerStream(String name, Iterator<JVS> iter, Type type, StreamConfig sc) {
            Supplier<Iterator<JVS>> supplier = new SingleUseSupplier(iter);
            schema.addTable(name, new JvsTable(name, type, sc, supplier, false));
            return this;
        }

        /** Reference table with default (load-once) refresh policy. */
        public Builder registerReferenceTable(String name, BaseFile source, Type type) {
            return registerReferenceTable(name, source, type, RefreshPolicy.once());
        }

        /**
         * Reference table (dimension table) loaded from a BaseFile, JOIN-target only.
         *
         * <p>Reads the file at build time. Supports two on-disk shapes automatically:
         * JSON array or NDJSON (one JSON object per line). Rows are materialized in
         * memory and hash-indexed by the join key when the query is executed.</p>
         *
         * <p>Refresh policy {@link RefreshPolicy#every(java.time.Duration)} /
         * {@link RefreshPolicy#onDemand()} rebuilds the snapshot atomically — in-flight
         * queries continue against the old snapshot until the next execution starts.
         * The scheduled reloader is registered here; manual refresh flows through
         * {@link JvsSqlEngine#refreshReferenceTable}.</p>
         */
        public Builder registerReferenceTable(String name, BaseFile source, Type type, RefreshPolicy policy) {
            java.util.List<JVS> loaded = com.hitorro.jvssql.refdata.ReferenceTableLoader.load(source);
            Supplier<Iterator<JVS>> supplier = java.util.Collections::emptyIterator;   // unused for ref tables
            JvsTable t = new JvsTable(name, type, StreamConfig.batch(), supplier, true);
            t.setReferenceRows(loaded);
            schema.addTable(name, t);
            refSpecs.put(name, new com.hitorro.jvssql.refdata.RefTableSpec(name, source, policy));
            return this;
        }

        /** Route records that are too late (beyond {@code allowedLateness}) to this sink instead of dropping them. */
        public Builder withLateDataSink(String streamName, Sink<com.fasterxml.jackson.databind.JsonNode> sink) {
            lateDataSinks.put(streamName, sink);
            return this;
        }

        /**
         * Register a Java scalar function callable from SQL. The class must have a
         * public no-arg constructor and a single public {@code eval(...)} method
         * whose parameters and return are one of: primitive, boxed primitive, String,
         * or {@link com.fasterxml.jackson.databind.JsonNode} (for raw JSON access).
         *
         * <p>Example: {@code public class Hash64 { public long eval(String s) { ... } }}
         * then {@code .registerFunction("hash64", Hash64.class)} lets you write
         * {@code SELECT hash64(filename) FROM docs}.</p>
         */
        public Builder registerFunction(String name, Class<?> functionClass) {
            functions.putScalar(name, com.hitorro.jvssql.udf.JavaScalarFn.wrap(functionClass));
            userJavaScalars.put(name, functionClass);   // for Calcite validator registration at compile time
            return this;
        }

        /**
         * Register a Java aggregate function (UDAF). The class must implement
         * {@link com.hitorro.jvssql.exec.AggregateFn} or have method-shape
         * {@code createAccumulator()}, {@code accumulate(acc, v)}, {@code result(acc)}.
         */
        public Builder registerAggregate(String name, Class<?> aggClass) {
            functions.putAggregate(name, com.hitorro.jvssql.udf.JavaAggregateFn.wrap(aggClass));
            userJavaAggregates.put(name, aggClass);
            return this;
        }

        /**
         * Register a Groovy-defined scalar. The script body receives an implicit
         * argument variable per positional arg — the first as {@code arg} (or
         * {@code arg1}), the second as {@code arg2}, etc. Whatever the script
         * returns is the function's result.
         *
         * <p>Example: {@code .registerGroovyFunction("normalize_email", "arg.toString().toLowerCase().trim()")}</p>
         */
        /** Groovy scalar UDF with one argument (bound as {@code arg} / {@code arg1}). */
        public Builder registerGroovyFunction(String name, String groovyScript) {
            return registerGroovyFunction(name, 1, groovyScript);
        }

        /**
         * Groovy scalar UDF with a given argument count. Args are bound as
         * {@code arg1..argN} (also {@code arg} for the first). Arity is declared
         * so Calcite's validator can type-check calls; the return type is ANY.
         */
        public Builder registerGroovyFunction(String name, int arity, String groovyScript) {
            functions.putScalar(name, com.hitorro.jvssql.udf.GroovyScalarFn.compile(groovyScript, arity));
            userGroovyScalars.put(name, arity);
            return this;
        }

        /**
         * Register a Groovy-defined aggregate. Script defines three closures.
         * Each closure MUST start with an explicit arrow ({@code ->}) so
         * {@code shell.evaluate} returns a Closure (bare {@code {...}} blocks are
         * evaluated eagerly by Groovy):
         *
         * <pre>{@code
         * init:   { -> [] as List }
         * accum:  { acc, v -> acc << v; acc }
         * result: { acc -> acc.sort().join(',') }
         * }</pre>
         *
         * <p>The aggregate takes a single argument at the SQL level; the Groovy
         * {@code accum} closure receives the accumulator and the SQL argument
         * for each row in the group.</p>
         */
        public Builder registerGroovyAggregate(String name, String groovyScript) {
            functions.putAggregate(name, com.hitorro.jvssql.udf.GroovyAggregateFn.compile(groovyScript));
            userGroovyAggregates.add(name);
            return this;
        }

        public JvsSqlEngine build() {
            return new JvsSqlEngine(cfg.build(), schema, functions, userJavaScalars, userJavaAggregates,
                    userGroovyScalars, userGroovyAggregates, refSpecs, lateDataSinks);
        }
    }

    /**
     * Wraps a caller-supplied iterator so Calcite/executor code can request "the source"
     * without knowing whether it's already been consumed. First call returns the iterator;
     * subsequent calls throw — matching the reality of a live stream that can't be replayed.
     */
    private static final class SingleUseSupplier implements Supplier<Iterator<JVS>> {
        private volatile Iterator<JVS> once;
        SingleUseSupplier(Iterator<JVS> it) { this.once = it; }
        @Override public Iterator<JVS> get() {
            Iterator<JVS> it = once;
            if (it == null) throw new JvsSqlException("stream source already consumed");
            once = null;
            return it;
        }
    }
}
