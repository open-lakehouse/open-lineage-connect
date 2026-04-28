# Spark OpenLineage Plugin — Consolidated Architecture

> File: `spark-openlineage-architecture.md` (31 chars). Companion to
> [`spark4-lineage-extraction.md`](./spark4-lineage-extraction.md) (the *plan
> extraction* design) and [`../spark-openlineage-plugin/COMPAT.md`](../spark-openlineage-plugin/COMPAT.md)
> (the live Catalyst API reference).

This document captures the **complete** architecture of the
`spark-openlineage-plugin` as it exists today: the end-to-end data flow from
executor tasks, through driver aggregation, through the ConnectRPC transport,
into the Go `open-lineage-service`. Where the sibling `spark4-lineage-extraction.md`
document focuses on *"how do we pull `DatasetRef`s out of a `LogicalPlan`,"*
this one answers:

- How does a Spark job turn into a stream of `RunEvent` protobufs?
- Which pieces run on which JVM, and why?
- What happens when something goes wrong (network flap, slow downstream,
  executor crash, Connect server shutdown)?
- What is the test strategy, and what invariants does each suite hold?

---

## 1. The pipeline, end to end

```
 ┌──────────────────── Spark driver JVM ──────────────────────────┐
 │                                                                 │
 │   LineageDriverPlugin.init(sc, ctx)                             │
 │         │                                                       │
 │         ├── LineageQueryListener     ─── onSuccess / onFailure ─┤
 │         │       │                                               │
 │         │       ├─ QueryPlanVisitor(qe.analyzed)                │
 │         │       │    └─ Seq[DatasetRef] inputs / outputs        │
 │         │       │                                               │
 │         │       ├─ RunContext (runId, JobRef, status, facets)   │
 │         │       ├─ RunEventBuilder → Lineage.RunEvent (proto)   │
 │         │       └─ EventSink.send(RunEvent)  ──────────┐        │
 │         │                                              │        │
 │         ├── LineageStreamingListener                   │        │
 │         │       │                                      │        │
 │         │       └─ onQueryStarted / onQueryProgress /  │        │
 │         │          onQueryTerminated                   │        │
 │         │          → same RunEvent pipeline ───────────┤        │
 │         │                                              │        │
 │         ├── TaskMetricsAggregator                      │        │
 │         │       │                                      │        │
 │         │       └─ merged into terminal RunEvent facets│        │
 │         │                                              │        │
 │         └── receive(ExecutorTaskMetrics)  ←─── plugin RPC ──┐   │
 │                                                         │   │   │
 │   ConnectRpcEventSink (worker thread)  ◄────────────────┘   │   │
 │     │                                                       │   │
 │     ├─ bounded ArrayBlockingQueue[RunEvent]  (queueSize=1024)│   │
 │     ├─ drains batches every batchFlushMs (=250ms)            │   │
 │     ├─ OkHttp POST /lineage.v1.LineageService/IngestBatch    │   │
 │     │      Content-Type: application/proto                   │   │
 │     └─ retries on 408/429/5xx with exponential backoff       │   │
 │                                                              │   │
 └──────────────────────────────────────────────────────────────┼───┘
                                                                │
                              ┌─── open-lineage-service (Go) ───┘
                              │     gRPC/Connect IngestBatch
                              │     → Delta Lake (via Rust sidecar)
                              ▼
                          storage graph

 ┌──────────── Spark executor JVM(s) ─────────────┐
 │                                                 │
 │   LineageExecutorPlugin.init(ctx, extraConf)    │
 │         │                                       │
 │         ├── onTaskSucceeded / onTaskFailed      │
 │         │     │                                 │
 │         │     └─ ExecutorTaskMetrics payload    │
 │         │        (records/bytes r+w, CPU, GC,   │
 │         │         memory, executionId, host…)   │
 │         │                                       │
 │         └─ PluginContext.send(payload) ─────────┼──── over Spark RPC ─→ driver
 │                                                 │
 └─────────────────────────────────────────────────┘
```

The driver is the sole network actor that speaks to the lineage service.
Executors only ever talk to the driver, and they do so over Spark's built-in
plugin RPC — no new ports, no new transport, no new auth plane.

---

## 2. Component inventory and responsibilities

| Layer                 | File                                       | Responsibility                                                                                   |
|-----------------------|--------------------------------------------|--------------------------------------------------------------------------------------------------|
| Entry                 | `LineagePlugin`                            | `SparkPlugin` entry; returns driver + executor halves.                                           |
| Config                | `LineageConfig`                            | Typed view of `spark.openlineage.*`; conservative defaults, fail-open.                           |
| Driver                | `LineageDriverPlugin`                      | Registers listeners, owns the `EventSink`, receives executor RPCs.                               |
| Driver                | `LineageQueryListener`                     | `QueryExecutionListener` — translates batch queries into `RunEvent`s.                            |
| Driver                | `LineageStreamingListener`                 | `StreamingQueryListener` — translates streaming queries into `RunEvent`s.                        |
| Driver                | `QueryPlanVisitor` + `ReflectiveSinkMatchers` | `LogicalPlan` → `Seq[DatasetRef]` for inputs / outputs.                                        |
| Driver                | `RunContext`, `JobRef`, `RunStatus`        | Immutable per-run state.                                                                         |
| Driver                | `JobNaming`                                | Derives deterministic job names with a documented precedence.                                    |
| Driver                | `FacetEncoder`                             | Scala `Map` → `google.protobuf.Struct` (used for run / dataset facets).                          |
| Driver                | `ColumnLineageExtractor`                   | Walks analyzed `LogicalPlan`; produces typed `ColumnLineageDatasetFacet` per output dataset (DIRECT + INDIRECT + dataset deps). |
| Driver                | `ColumnLineageEncoder`                     | Scala `ColumnLineageFacet` → generated Java proto builder.                                       |
| Driver                | `RunEventBuilder`                          | `RunContext` → `Lineage.RunEvent` protobuf (pure function); attaches column-lineage facet.       |
| Driver                | `EventSink` (`Noop`, `InMemory`, `Broadcast`, `ConnectRpcEventSink`) | Abstract destination for `RunEvent`s; `ConnectRpc*` is the production impl.                 |
| Driver                | `TaskMetricsAggregator`                    | Aggregates `ExecutorTaskMetrics` by SQL `executionId` for terminal-event enrichment.             |
| Shared                | `ExecutorTaskMetrics`                      | Serializable wire payload between executors and driver.                                          |
| Executor              | `LineageExecutorPlugin`                    | Captures `TaskMetrics` on task completion; sends via `PluginContext.send`.                       |
| Transport             | `ConnectRpcClient`                         | OkHttp-backed Connect unary client; maps HTTP status → Connect error codes.                      |
| Transport             | `LineageServiceClient`                     | Typed wrapper binding `ConnectRpcClient` to `lineage.v1.LineageService` method paths.            |
| Transport             | `ConnectRpcEventSink`                      | Bounded-queue + single worker thread; the only place sync network I/O happens.                   |
| Streaming             | `StreamingSourceParser`                    | `SourceProgress` / `SinkProgress` description strings → `DatasetRef`s.                           |
| Tests                 | `StreamingTestHelpers` (`org.apache.spark.*`) | Ducks under `private[spark]` ctors for `QueryProgressEvent` / `StreamingQueryProgress`.      |

---

## 3. Driver ↔ executor communication

### Why Spark's plugin RPC, not a dedicated channel

- `PluginContext.send(msg)` on the executor → `DriverPlugin.receive(msg)` on
  the driver. Serialization, retries under transient driver loss, and
  back-pressure are Spark's problem, not ours.
- The executor never materializes a `Lineage.RunEvent`. It emits plain Scala
  case classes (`ExecutorTaskMetrics`) and lets the driver handle proto
  construction. This keeps the proto runtime and the ConnectRPC shaded
  universe confined to the driver classpath.
- In Spark Connect, both halves of this exchange still happen inside the
  Connect server process and its executor pool. No API changes.

### Payload shape

`ExecutorTaskMetrics` is deliberately a narrow, Serializable, versionable
struct — not a protobuf. We avoid pinning generated Java proto classes on the
executor classpath because the assembly shades them, and shading is
driver-side only. Adding fields is backwards-compatible (old executors send a
subset, driver tolerates).

### Driver-side rollup

`TaskMetricsAggregator` keys by `(executionId)` (when present) and uses
`LongAdder` counters + an `AtomicLong` peak to avoid contention on the hot
callback path. Orphaned metrics (no `executionId` — e.g. metadata tasks) fall
into a separate bucket that's never attached to a `RunEvent` but is queryable
for diagnostics.

Rollups are **snapshotted once**, at terminal-event construction time, then
**removed from the map**. This keeps the map bounded by the number of
*in-flight* executions, not by historical load.

---

## 4. The transport layer

### Why a hand-rolled Connect client

ConnectRPC's only official Scala-compatible client is `connect-kotlin-okhttp`,
whose method signatures are `suspend fun`. Calling Kotlin suspend functions
from Scala is possible but awkward: every call becomes
`client.emit(req, ContinuationImpl(...))`, errors surface as
`kotlin.coroutines.Continuation`-wrapped exceptions, and the thread model is
opaque from the caller's side.

Connect's unary wire format, on the other hand, is trivial:

```
POST {serviceBase}/{FQN.service}/{MethodName} HTTP/1.1
Content-Type: application/proto

<proto serialized request body>
```

Responses are either:

- HTTP 200 with the proto response body, OR
- Non-200 with a JSON `{ "code": "...", "message": "..." }`.

So `ConnectRpcClient` is ~150 LoC of OkHttp that:

1. Serializes the request proto.
2. POSTs to the service path.
3. On 200, parses the response proto.
4. On non-200, maps the HTTP status to a Connect error code string
   (`"resource_exhausted"` for 429, `"unavailable"` for 503, etc.) and throws
   `ConnectRpcException`.

No Kotlin on the critical path.

### Why an in-process queue + dedicated worker

Listener callbacks run on Spark threads. Spark will not wait for us. Any
synchronous `POST` inside `onSuccess` would gate Spark's execution progress
on `open-lineage-service`'s tail latency — unacceptable.

`ConnectRpcEventSink` buffers events in an `ArrayBlockingQueue`. Exactly one
worker thread drains that queue in batches, issues the `IngestBatch` call,
and retries on transient failures. The `send()` call never blocks:

- Queue has room  → enqueue, return immediately.
- Queue is full   → drop the event, increment `dropped`, log once per
                    drop-burst. Spark job is never throttled.

### Shutdown is bounded

`close()` sets a `draining` flag and then joins the worker with a timeout:

- Worker observes `draining && queue.isEmpty` → exits cleanly.
- Worker is mid-flight on an HTTP call → finishes it, then exits.
- Timeout (default 2s) elapsed → interrupts the worker. We log the `sent /
  failed / dropped` totals so operators can tell what got dropped.

We do *not* drain to completion unconditionally. A downstream outage must not
delay SparkContext.stop() by minutes.

### Retries

`ConnectRpcEventSink` does a modest exponential backoff with a hard cap on
attempts per batch. The batch is dropped once the cap is hit; failing a
single batch never shuts down the worker — the next batch tries again.

Retriable classes: `timeout`, `unavailable` (5xx), `resource_exhausted`
(429), `aborted`. Everything else (400, 401, 403, ...) is non-retriable and
logged at WARN.

---

## 5. Streaming lineage

Structured Streaming does not expose a `LogicalPlan` in a form we can visit
directly from a listener — the plan is logical *within* a single microbatch
and the listener only sees `StreamingQueryProgress` snapshots. So
`LineageStreamingListener` has to work from `SourceProgress.description`
strings like:

- `FileStreamSource[file:/data/events/*.parquet]`
- `KafkaV2[Subscribe[orders,payments]]`
- `DeltaSource[s3a://warehouse/deltas/orders]`

`StreamingSourceParser` is a small, regex-driven parser with a `java.net.URI`
fallback for anything URI-shaped and a "preserve the raw description" branch
for everything else. We never throw; an unrecognized description becomes a
`DatasetRef` named after the raw description with `sourceType = "unknown"`.

### Event cadence

- `onQueryStarted`     → **START**    event, once per `runId`.
- `onQueryProgress`    → **RUNNING**  event, rate-limited to every Nth batch
                          (`spark.openlineage.emit.streaming.progressEveryN`).
                          Inputs and outputs are re-computed on each emitted
                          progress so dataset-set changes (e.g. new Kafka
                          partitions) are captured.
- `onQueryTerminated`  → **COMPLETE** if termination was clean,
                          **FAIL** if `exception.isDefined` on the event.
                          Error facets include `errorClass` /
                          `errorClassOnException` when Spark populates them.

### Why a separate listener

`QueryExecutionListener.onSuccess` fires for ad-hoc DataFrames but not for
the long-lived streaming `QueryExecution` — streaming queries emit progress
events instead of a terminal success. Trying to shoehorn both flows through a
single listener makes the "is this a batch run or a streaming tick" branching
very messy. Two small listeners are cleaner.

---

## 6. Fail-open operational properties

Every listener callback and every transport hop is wrapped in
`try/catch NonFatal`. The plugin will **never** fail a user's Spark job. The
ordered list of things that go wrong and what happens:

| Failure                                       | Outcome                                                                                    |
|-----------------------------------------------|--------------------------------------------------------------------------------------------|
| Plugin class not on classpath                 | Spark logs a plugin-load error. Job proceeds. No lineage.                                   |
| `LineageConfig.fromSparkConf` throws          | `LineageDriverPlugin.init` logs at ERROR, returns empty config map. Executors load inert.   |
| `spark.openlineage.disabled=true`             | Init short-circuits. No listeners registered. Zero network traffic.                         |
| `spark.openlineage.serviceUrl` unset          | Listeners still register; events route to `EventSink.Noop`. Same code paths exercised, just no network. |
| `QueryPlanVisitor` hits an unknown node       | Returns `Seq.empty`. Event emitted with missing inputs/outputs. Operator sees a lineage gap, not a crash. |
| `ReflectiveSinkMatchers` class not loadable   | Silently inert for that matcher. No log spam.                                               |
| `ConnectRpcEventSink` queue full              | Event dropped. Counter incremented. Logged once per burst at WARN.                          |
| `open-lineage-service` 5xx / timeout          | Retried with backoff. After cap, batch dropped, worker continues.                           |
| `open-lineage-service` 4xx                    | Logged at WARN. Not retried. Next batch tried.                                              |
| Executor crashes mid-task                     | `onTaskFailed` fires, metrics payload goes out with `failed=true`. Worst case the payload is also lost, in which case the aggregator has no data for that task. Event still emits. |
| SparkContext stop with pending events         | `close()` waits up to 2s for worker to drain, then interrupts. Final counts logged.         |
| Log4j2 init NPE in background thread          | `ConnectRpcEventSink` uses SLF4J directly rather than Spark's `Logging` trait to dodge this. |

No failure path loops, and no failure path logs at FATAL. The worst
observable outcome is "silently missing lineage edges," which is trivially
debuggable via the emitted `sent / failed / dropped` counters.

---

## 7. Dimensional concerns

### Memory

- Aggregator map: bounded by in-flight `executionId`s. Each bucket holds a
  handful of `LongAdder`s + an `AtomicLong`. O(few KB / query).
- Event queue: `queueSize` × `sizeof(RunEvent)`. Default 1024 × ~4 KB = ~4 MB
  steady-state ceiling per driver. Tunable via `spark.openlineage.queueSize`.
- `RunContext`: one per in-flight query. Case-class-cheap.

### CPU

- `QueryPlanVisitor` walks `LogicalPlan` once per terminal. Linear in plan
  size; typical queries < 10 ms.
- `FacetEncoder.encodeMixed` is the heaviest per-event path at ~100 µs.
- Worker thread runs at most once per `batchFlushMs` under quiescence.

### Network

- Driver → `open-lineage-service`: at most one in-flight POST at a time
  (single worker), batched up to `queueSize` events per POST.
- No executor → service traffic.

---

## 8. Deployment modes

### Classic `spark-submit`

```bash
spark-submit \
  --jars spark-openlineage-plugin-assembly-*.jar \
  --conf spark.plugins=com.openlakehouse.lineage.LineagePlugin \
  --conf spark.openlineage.serviceUrl=http://open-lineage-service:8090 \
  ...
```

Driver loads the plugin, executors get the config map back from
`DriverPlugin.init`, each executor's `ExecutorPlugin.init` reads it.

### Spark Connect

Plugin must be on the **Connect server's** `spark.plugins`, not the client.
The Connect server *is* a Spark driver: it owns the `SparkContext`, runs the
analyzer, and has the listener surface we rely on. The client is stateless.
Setting plugin config on a Connect client is a no-op (Spark doesn't ship that
config to the server).

The `LineagePluginSmokeSpec` test exercises this exact lifecycle — a real
`SparkSession` loading the plugin via `spark.plugins` and emitting real
protobuf over HTTP to a `MockWebServer`. Because the plugin does not call any
Connect-specific APIs (we only use `SparkSession`, `SparkContext`, and the
standard listener surfaces), this classic-mode test is a valid proxy for
Connect coverage.

### Databricks (DBR 16+)

DBR 16+ bundles Spark 4.x and Scala 2.13. The plugin jar can go on DBFS or a
UC Volume; the configs are the same. The namespace key
(`spark.openlineage.namespace`) is typically set to
`<env>-<workspace-id>`.

---

## 9. Test strategy

14 suites, 76 tests, all green in `sbt test` (~16s):

| Suite                            | Shape                                          | What it protects                                          |
|----------------------------------|------------------------------------------------|-----------------------------------------------------------|
| `LineageConfigSpec`              | Unit                                           | Config parsing / defaults / bounds clamping.              |
| `JobNamingSpec`                  | Unit                                           | Name precedence, determinism, sanitisation rules.         |
| `FacetEncoderSpec`               | Unit                                           | Struct encoding (strings, mixed, nested, empties).        |
| `RunEventBuilderSpec`            | Unit                                           | Proto conversion, run-facet injection, dataset mapping.   |
| `EventSinkSpec`                  | Unit w/ concurrency                            | Noop / InMemory / Broadcast behaviour.                    |
| `TaskMetricsAggregatorSpec`      | Unit w/ concurrency                            | Per-execution rollup, orphan handling, thread safety.     |
| `QueryPlanVisitorSpec`           | Integration (real SparkSession)                | Source extraction across plan shapes.                     |
| `QueryPlanVisitorSinksSpec`      | Integration (real SparkSession)                | Sink extraction across plan shapes.                       |
| `LineageQueryListenerSpec`       | Integration (real SparkSession)                | End-to-end batch listener behaviour incl. failure paths and metric folding. |
| `StreamingSourceParserSpec`      | Unit                                           | Description-string parsing (file/kafka/delta/unknown).    |
| `LineageStreamingListenerSpec`   | Unit (synthetic events via `StreamingTestHelpers`) | Streaming START/RUNNING/COMPLETE/FAIL cadence.       |
| `ConnectRpcClientSpec`           | Integration (MockWebServer)                    | Wire format, headers, error-code mapping.                 |
| `ConnectRpcEventSinkSpec`        | Integration (MockWebServer)                    | Backpressure, batching, retries, graceful drain.          |
| `LineagePluginSmokeSpec`         | End-to-end (real SparkSession + MockWebServer) | Plugin loads via `spark.plugins`, protobuf lands on HTTP. |

### Why mix unit + integration

Unit tests catch regressions cheaply. Integration tests against real
`SparkSession` catch Catalyst-surface drift — the kind of failure we care
about most on Spark minor upgrades. MockWebServer integration tests catch
wire-format mistakes that would be silent in unit mocks.

### The `private[spark]` workaround

Spark 4 dropped the convenient `spark-testing-base`-style artifacts for
Connect-era APIs, and several listener events (notably `QueryProgressEvent`)
have `private[spark]` constructors. Rather than use reflection at every test
call site, we added a tiny helper file under
`src/test/scala/org/apache/spark/sql/streaming/StreamingTestHelpers.scala`.
Because it sits in Spark's package namespace, `private[spark]` members are
visible. This is a documented, test-only pattern.

---

## 10. Build and release

### sbt / JDK / Scala

- `scalaVersion := "2.13.14"` (no `crossScalaVersions` — Spark 4 dropped 2.12).
- `javacOptions ++= Seq("-source", "17", "-target", "17")`.
- `scalacOptions += "-release:17"`.
- Test fork sets `--add-opens` for `java.base/java.nio`, `java.base/java.util`,
  etc. to satisfy Spark's reflective access on JDK 17.

### Dependencies

- `spark-core`, `spark-sql`, `spark-catalyst`, `spark-hive`, `spark-connect` —
  all `% Provided`. Consumers supply Spark.
- `com.google.protobuf:protobuf-java:4.34.1` — matches the
  `buf.build/protocolbuffers/java` remote plugin's target. **Shaded.**
- `build.buf:protovalidate:0.7.0` — required so generated proto code
  referencing `build.buf.validate.*` resolves. **Shaded.**
- `com.connectrpc:connect-kotlin:0.7.2` + `connect-kotlin-okhttp` — we carry
  the jars because `Lineage.java` references `com.connectrpc.Code`, but we
  never call `ProtocolClient`. **Shaded.**
- `com.squareup.okhttp3:okhttp:4.12.0` — the transport. **Shaded.**
- `io.delta:delta-spark:4.0.0` — `% Test` only, for reflective-matcher
  integration.
- `com.squareup.okhttp3:mockwebserver` — `% Test` only.

### Shading

```scala
ShadeRule.rename(
  "com.google.protobuf.**" -> "com.openlakehouse.shaded.protobuf.@1",
  "com.connectrpc.**"      -> "com.openlakehouse.shaded.connectrpc.@1",
  "okhttp3.**"             -> "com.openlakehouse.shaded.okhttp3.@1",
  "okio.**"                -> "com.openlakehouse.shaded.okio.@1",
  "kotlin.**"              -> "com.openlakehouse.shaded.kotlin.@1",
  "kotlinx.**"             -> "com.openlakehouse.shaded.kotlinx.@1"
).inAll
```

Plugin JAR is drop-in next to DBR / stock Spark / user apps that carry their
own conflicting versions of any of these libraries.

### Code generation

`buf generate` at the repo root produces:

- `gen/**` Go stubs for the service.
- `spark-openlineage-plugin/src/main/generated/**` Java message classes.

The generated Java is **committed** so CI runners without `buf` (or a BSR
token) can build the plugin. The only time you need to run `buf generate` is
after editing `proto/lineage/v1/lineage.proto`.

---

## 11. What we built — phased recap

This is the order the work actually went in, so a future reader can trace
why each file exists:

| Phase                                   | Core deliverables                                                                                                       |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| **Scaffold**                            | `build.sbt` for Spark 4.1.1 / Scala 2.13 / JDK 17; `LineagePlugin`, `LineageConfig`.                                   |
| **Plan extraction**                     | `DatasetRef`, `QueryPlanVisitor`, `ReflectiveSinkMatchers`, `COMPAT.md`. Integration tests against a real SparkSession.|
| **Run model**                           | `RunContext`, `JobRef`, `RunStatus`, `JobNaming`, `FacetEncoder`, `RunEventBuilder`.                                   |
| **Event sink abstractions**             | `EventSink` trait + Noop / InMemory / Broadcast impls.                                                                  |
| **Batch listener**                      | `LineageQueryListener` + `DeferredListenerRegistrar`. Full START/COMPLETE/FAIL lifecycle.                               |
| **Transport**                           | `ConnectRpcClient` (OkHttp), `LineageServiceClient`, `ConnectRpcEventSink`.                                             |
| **Executor → driver metrics**           | `ExecutorTaskMetrics`, `LineageExecutorPlugin`, `TaskMetricsAggregator`; wired into terminal-event facets.              |
| **Streaming lineage**                   | `StreamingSourceParser`, `LineageStreamingListener`; deferred registration for SparkSessions created after init.        |
| **Smoke / Connect-equivalent coverage** | `LineagePluginSmokeSpec`, `StreamingTestHelpers`.                                                                       |
| **Docs**                                | `README.md` operator quickstart; `spark4-lineage-extraction.md`; this file.                                             |

---

## 12. Shipped since v1 + future work

### Shipped

1. **Column-level lineage.** Implemented end-to-end behind
   `spark.openlineage.emit.columnLineage=true` (off by default).
   `ColumnLineageExtractor` walks the analyzed plan and produces a typed
   `ColumnLineageDatasetFacet` (OpenLineage 1-2-0 spec) for each output
   dataset:
   - Seeds at `LogicalRelation` / `HiveTableRelation` /
     `DataSourceV2Relation` leaves by indexing every output
     `AttributeReference.exprId` to its `(DatasetRef, fieldName)`.
   - Propagates lineage through `Project`, `Aggregate`, `Window`,
     `Generate`, `SubqueryAlias`, and `Union`, classifying each output
     column as `DIRECT/IDENTITY`, `DIRECT/TRANSFORMATION`,
     `DIRECT/AGGREGATION`, or flagging `masking=true` when the
     transformation routes through `md5/sha1/sha2/mask/regexp_replace`.
   - Collects INDIRECT dependencies (`FILTER`, `SORT`, `JOIN`,
     `GROUP_BY`, `WINDOW`) from `Filter.condition`, `Sort.order`,
     `Join.condition`, `Aggregate.groupingExpressions`,
     `Window.partitionSpec` / `orderSpec` — surfaced both per-field
     and on the top-level `dataset` deps array.
   - The facet rides on `OutputDataset.column_lineage` (sibling of the
     existing generic `facets` struct). `ColumnLineageEncoder` translates
     Scala case classes to the generated Java proto builders.
   - Wired into both `LineageQueryListener` (one extraction per query,
     reused across START / COMPLETE / FAIL) and
     `LineageStreamingListener` (microbatch path looks up
     `IncrementalExecution.analyzed` reflectively). Extraction is
     wrapped in `try/catch NonFatal` — a failure logs and ships the
     event without column lineage.
   - The Go converter parses the equivalent JSON facet from external
     OpenLineage senders and lifts it into the same typed proto field.
   - The Rust table-service writes a per-event `column_lineage_json`
     column to Delta (see `open-lakehouse-table-service/src/writer/schema.rs`).

### Future work

Explicitly out of scope for this cut:

1. **User-defined facets.** `spark.openlineage.facets.run.*` /
   `spark.openlineage.facets.job.*` → injected into the relevant facet
   structs. Mechanically simple, just not wired.
2. **Spark Connect true-E2E test.** Today's smoke test is a classic-mode
   proxy. A CI job that actually starts a Connect server would close the
   remaining coverage gap; it's deferred because starting a Connect server
   in CI is non-trivial to do portably.
3. **Hive Metastore coverage in CI.** `HiveTableRelation` is handled in
   code, but we don't stand up a Hive-enabled Spark in CI.
4. **TLS + mTLS transport.** `ConnectRpcClient.defaultOkHttp()` is plain
   HTTP. Wiring an `SSLContext` is a one-method change but needs an
   operator story for cert material.
5. **Retry policy tuning.** Current backoff is intentionally modest; real
   production load may want jittered backoff and per-endpoint rate limiting.

---

## 13. Pointers

- **Extraction strategy:** [`spark4-lineage-extraction.md`](./spark4-lineage-extraction.md)
- **Live Catalyst API reference:** [`../spark-openlineage-plugin/COMPAT.md`](../spark-openlineage-plugin/COMPAT.md)
- **Operator quickstart:** [`../spark-openlineage-plugin/README.md`](../spark-openlineage-plugin/README.md)
- **Protobuf source of truth:** [`../proto/lineage/v1/lineage.proto`](../proto/lineage/v1/lineage.proto)
- **Plan checked during implementation:** [`../.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md`](../.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md)
