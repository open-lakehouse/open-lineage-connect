# spark-openlineage-plugin

An Apache Spark 4 plugin that observes SQL / DataFrame / Structured Streaming
jobs, extracts source / sink lineage from the analyzed `LogicalPlan`, and
emits `lineage.v1.RunEvent` protobuf messages to the Go `open-lineage-service`
over ConnectRPC.

Status: **functional** — the `SparkPlugin`, `QueryExecutionListener`,
`StreamingQueryListener`, executor-side task metrics capture, driver-side
aggregation, and the ConnectRPC emitter are all implemented and covered by a
76-test sbt suite (14 suites, including an end-to-end smoke test against a
MockWebServer). See `../research/spark4-lineage-extraction.md` for the
architecture rationale and `../research/spark-openlineage-architecture.md`
for the consolidated design doc.

## Requirements

| Tool   | Version                                  |
|--------|------------------------------------------|
| Spark  | 4.1.1 (Scala 2.13, classic or Connect)   |
| Scala  | 2.13.14 (Spark 4 dropped 2.12)           |
| JDK    | 17 (21 also supported)                   |
| sbt    | 1.10+                                    |
| buf    | 1.40+ (only to regenerate protos)        |

## Build

From the repo root:

```bash
make spark-plugin-build    # → target/scala-2.13/spark-openlineage-plugin-assembly-*.jar
make spark-plugin-test
```

Or directly:

```bash
cd spark-openlineage-plugin
sbt assembly
sbt test
```

The assembly shades `com.google.protobuf.**`, `com.connectrpc.**`,
`okhttp3.**`, `okio.**`, `kotlin.**`, and `kotlinx.**` under
`com.openlakehouse.shaded.*` so the plugin jar is safe to drop next to any
downstream that pins its own versions of those libraries.

## Quickstart

### 1. Classic `spark-submit`

```bash
spark-submit \
  --jars spark-openlineage-plugin-assembly-0.1.0-SNAPSHOT.jar \
  --conf spark.plugins=com.openlakehouse.lineage.LineagePlugin \
  --conf spark.openlineage.serviceUrl=http://open-lineage-service:8090 \
  --conf spark.openlineage.namespace=prod-warehouse \
  my-app.jar
```

### 2. Spark Connect

The plugin must load on the **Connect server JVM**, not the client. Client
config keys (Python / Scala / Go) that set `spark.plugins` are ignored:

```bash
$SPARK_HOME/sbin/start-connect-server.sh \
  --jars spark-openlineage-plugin-assembly-0.1.0-SNAPSHOT.jar \
  --conf spark.plugins=com.openlakehouse.lineage.LineagePlugin \
  --conf spark.openlineage.serviceUrl=http://open-lineage-service:8090 \
  --conf spark.openlineage.namespace=prod-warehouse
```

Clients connect to the Connect server exactly as before; lineage extraction
happens server-side off of the same `LogicalPlan` the Connect service
compiles from each ExecutePlanRequest, so every user session is covered
without client-side changes.

### 3. Databricks / cluster UI

Put the assembly jar on DBFS or UC Volumes and set on the cluster:

| Spark conf                                | Value                               |
|-------------------------------------------|-------------------------------------|
| `spark.plugins`                           | `com.openlakehouse.lineage.LineagePlugin` |
| `spark.openlineage.serviceUrl`            | `http://open-lineage-service:8090`  |
| `spark.openlineage.namespace`             | `<env>-<workspace>`                 |

## Configuration reference

| Key                                               | Default     | Description                                                  |
|---------------------------------------------------|-------------|--------------------------------------------------------------|
| `spark.openlineage.serviceUrl`                    | *(unset)*   | `http(s)://host:port` of `open-lineage-service`; when unset the plugin loads but routes events to a Noop sink. |
| `spark.openlineage.namespace`                     | `default`   | Logical namespace for datasets and jobs.                    |
| `spark.openlineage.jobName`                       | *(derived)* | Explicit override for the OpenLineage job name.             |
| `spark.openlineage.emit.taskMetrics`              | `true`      | Ship per-task metrics from executors to driver and fold them into the `COMPLETE` event's facets. |
| `spark.openlineage.emit.columnLineage`            | `false`     | Walk the analyzed `LogicalPlan` to derive a typed `ColumnLineageDatasetFacet` per output dataset (DIRECT + INDIRECT transformations and dataset-level deps, mirroring OpenLineage 1-2-0). Off by default; flip to `true` to populate `OutputDataset.column_lineage` on every `RunEvent`. Extraction is wrapped in `try/catch NonFatal` and a failure leaves lineage attached without column lineage. |
| `spark.openlineage.emit.streaming.progressEveryN` | `1`         | Throttle `onQueryProgress` to every Nth batch (1 = every batch). |
| `spark.openlineage.queueSize`                     | `1024`      | Bounded queue in front of the transport worker; overflow is dropped with a log line. |
| `spark.openlineage.batchFlushMs`                  | `250`       | Max time an event waits in the queue before being flushed.  |
| `spark.openlineage.disabled`                      | `false`     | Kill-switch — plugin loads but registers no listeners.      |
| `spark.openlineage.failOpen`                      | `true`      | On transport error, log and continue. Setting this to `false` is currently advisory — all error paths already continue. |

### Job-name derivation (`spark.openlineage.jobName`)

Precedence is documented in `JobNaming.derive`:

1. The explicit `spark.openlineage.jobName` config, if set.
2. The `SparkListenerSQLExecutionStart.description` (a human label Spark
   attaches to SQL executions, e.g. `count at MyJob.scala:42`).
3. `<appName>.plan_<short-sha1(analyzed-plan-canonical)>`.

Names and namespaces are sanitised to a conservative character class so they
round-trip cleanly through URLs / table names downstream.

## What gets emitted

### Batch queries

For every `QueryExecution` that reaches `QueryExecutionListener.onSuccess` or
`onFailure` the plugin emits two events that share a `runId`:

1. `START`  — with `job.namespace` / `job.name`, `run.runId`, inputs & outputs
              extracted from the analyzed `LogicalPlan`.
2. `COMPLETE` (or `FAIL`) — with the shared `runId`, duration facet, error
              facets on failure, and aggregated executor task metrics
              (`records_read`, `bytes_read`, `records_written`, `bytes_written`,
              `executor_run_time_ms`, `executor_cpu_time_ns`, `jvm_gc_time_ms`,
              `peak_execution_memory_bytes`, etc.).

### Structured Streaming

`LineageStreamingListener` hooks `StreamingQueryListener`:

- `onQueryStarted`     → `START`
- `onQueryProgress`    → `RUNNING` (rate-limited by `progressEveryN`, with
                          per-batch progress facets + parsed source/sink
                          `DatasetRef`s from the `SourceProgress` /
                          `SinkProgress` descriptions)
- `onQueryTerminated`  → `COMPLETE` on clean shutdown, `FAIL` if the
                          termination carried an exception

### Column lineage

When `spark.openlineage.emit.columnLineage=true`, `ColumnLineageExtractor`
walks the analyzed `LogicalPlan` once per query and produces a typed
`ColumnLineageDatasetFacet` for each output dataset. The facet follows the
OpenLineage 1-2-0 schema and rides on the `OutputDataset.column_lineage`
proto field (sibling of the existing generic `facets` struct). Coverage:

- **DIRECT / IDENTITY / TRANSFORMATION / AGGREGATION** classified per
  output column from `Project`, `Aggregate`, `Window`, `Generate`, and
  `SubqueryAlias` nodes.
- **INDIRECT / FILTER / SORT / JOIN / GROUP_BY / WINDOW** dependencies
  collected from `Filter.condition`, `Sort.order`, `Join.condition`,
  `Aggregate.groupingExpressions`, and window partition / order specs;
  also surfaced on the top-level `dataset` deps array.
- **`masking=true`** flagged when the projection routes through `md5`,
  `sha1`, `sha2`, `mask`, or `regexp_replace`.

Extraction is best-effort: any `NonFatal` failure inside the extractor is
logged and the run event still ships, just without the typed facet
attached.

### Dataset identity

`DatasetRef(namespace, name, sourceType)` is how every source/sink is keyed.
`QueryPlanVisitor` understands DSv1 `LogicalRelation`, `HiveTableRelation`,
DSv2 `DataSourceV2Relation` / `NamedRelation`, `InsertIntoHadoopFsRelationCommand`,
`V2WriteCommand`, `CreateTableAsSelect`, and (reflectively, without a hard
dependency) Delta / Iceberg write commands. See `COMPAT.md` for the exact
Catalyst surface we read on Spark 4.1.1.

## Fail-open policy

Every listener callback and every transport hop is wrapped in
`try/catch NonFatal` with a log line. The plugin will **never** fail a Spark
job. If the `open-lineage-service` is down:

- Events queue up to `queueSize` (default 1024).
- Once full, new events are dropped and a `dropped` counter is logged.
- The emitter keeps retrying on transient HTTP failures (connect reset, 408,
  429, 5xx) with exponential backoff.
- On `shutdown()` the worker gets a bounded grace period to flush, then is
  interrupted and the final counts (`sent=…, failed=…, dropped=…`) are logged.

## Directory layout

```
spark-openlineage-plugin/
  build.sbt
  COMPAT.md                                # Spark 4.1.1 Catalyst surface we target
  README.md
  project/
    build.properties
    plugins.sbt
  src/
    main/
      scala/com/openlakehouse/lineage/
        LineagePlugin.scala                # SparkPlugin entrypoint
        LineageConfig.scala                # spark.openlineage.* config
        common/ExecutorTaskMetrics.scala   # wire payload: executors → driver
        driver/
          LineageDriverPlugin.scala
          LineageQueryListener.scala       # QueryExecutionListener
          QueryPlanVisitor.scala           # LogicalPlan → DatasetRefs
          ReflectiveSinkMatchers.scala     # Delta / Iceberg / Hive writes
          DatasetRef.scala
          RunContext.scala
          JobNaming.scala
          FacetEncoder.scala
          RunEventBuilder.scala
          EventSink.scala                  # Noop / InMemory / Broadcast
          TaskMetricsAggregator.scala      # driver-side rollup by executionId
        executor/
          LineageExecutorPlugin.scala      # onTaskSucceeded / onTaskFailed
        streaming/
          LineageStreamingListener.scala
          StreamingSourceParser.scala
        transport/
          ConnectRpcClient.scala           # OkHttp → Connect unary
          LineageServiceClient.scala       # typed wrapper
          ConnectRpcEventSink.scala        # bounded-queue worker
      generated/                           # buf-generated Java protos (committed)
    test/
      scala/com/openlakehouse/lineage/
        ...                                # 14 suites, 76 tests
      scala/org/apache/spark/sql/streaming/
        StreamingTestHelpers.scala         # ducks under private[spark] ctors
```

## Comparison to `io.openlineage:openlineage-spark`

| Feature                                   | This plugin                                        | `io.openlineage:openlineage-spark` |
|-------------------------------------------|----------------------------------------------------|------------------------------------|
| Wire format                               | `lineage.v1.*` protobuf over ConnectRPC            | OpenLineage JSON spec over HTTP    |
| Spark version                             | 4.x only (2.13)                                    | 2.4 / 3.x / 4.x                    |
| Column-level lineage                      | Yes — typed `ColumnLineageDatasetFacet` (1-2-0 spec) | Yes                                |
| Custom facets                             | v2 (not yet)                                       | Yes, extensible API                |
| Integrates with this repo's storage       | Yes — `open-lineage-service` → Rust → Delta        | No                                 |

Pick this plugin when you want to feed the ConnectRPC / protobuf pipeline
already present in this repo. Pick the upstream one if you need the JSON
spec, column-level lineage, or Spark ≤ 3.x coverage today.

## Development tips

- **Regenerate protos:** `buf generate` at the repo root writes both Go
  server stubs and the Java message classes under
  `spark-openlineage-plugin/src/main/generated/`. The generated files are
  committed so CI runners without `buf` can still build.
- **`private[spark]` test helpers:** Spark 4 stopped shipping test jars, so
  any listener events with `private[spark]` ctors (e.g.
  `QueryProgressEvent`) are constructed via the
  `org.apache.spark.sql.streaming.StreamingTestHelpers` shim.
- **Logging in background threads:** `ConnectRpcEventSink` uses SLF4J
  directly rather than Spark's `Logging` trait to sidestep Log4j2 init
  issues observed in sbt's test forks.
