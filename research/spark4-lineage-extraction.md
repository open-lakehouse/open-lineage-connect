# Spark 4.1.1 Lineage Extraction — Design Research

> File name: `spark4-lineage-extraction.md` (28 chars). Companion to the living API reference at [`spark-openlineage-plugin/COMPAT.md`](../spark-openlineage-plugin/COMPAT.md) and the checked plan at [`.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md`](../.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md).

This document captures the research and trade-off decisions behind the Spark OpenLineage plugin that ships in `spark-openlineage-plugin/`. It is intentionally narrative rather than a spec: the spec is the plan; this is the why.

Audience: future contributors needing to understand which options we evaluated and which we rejected, so we don't re-derive the same answers on every Spark upgrade.

---

## 1. Problem

We want a Spark-side lineage producer that emits OpenLineage-shaped `RunEvent`s, `JobEvent`s, and `DatasetEvent`s (as defined in [`proto/lineage/v1/lineage.proto`](../proto/lineage/v1/lineage.proto)) to the Go `open-lineage-service`, across three Spark deployment modes:

1. Classic Spark 4.x (driver + executors in one JVM topology).
2. Spark Connect, where user code runs in a client JVM and query planning/execution happens on a remote Spark Connect server.
3. Spark Structured Streaming.

The producer must be tolerant to misconfiguration, forward-compatible across Spark minor releases, and not add failure modes to user jobs (i.e. lineage emission should never crash the Spark application).

---

## 2. Architecture decisions

### 2.1 Driver aggregates; executors don't talk to the service directly

**Decision:** The driver is the sole process that serializes and emits `RunEvent`/`JobEvent`/`DatasetEvent` protobufs over ConnectRPC. Executors push small summary messages to the driver via Spark's built-in plugin RPC (`PluginContext.send`/`ask`).

**Alternatives considered:**

| Option | Rejected because |
|---|---|
| Each executor opens its own ConnectRPC client to the lineage service | N × executors concurrent connections, N × auth token rotations, N × retries on partial cluster failures. Network-complex; each executor would need outbound reachability to the lineage service, which is a hard ask in Kubernetes / Databricks networking. |
| Driver runs a ConnectRPC **server** that executors connect to | Requires opening an additional ingress port on the driver, firewall config, TLS material distribution, and a lifecycle story for graceful shutdown. Duplicates what `PluginContext` already gives us for free. |
| **Chosen:** Driver is aggregator + client; executors push via `PluginContext.send` | Zero additional network plane. Spark already serializes driver↔executor plugin messages through its internal RPC. Drivers already have outbound connectivity — Spark talks to the external shuffle service, YARN/K8s API, metrics sinks, etc. |

**Implication for the proto layer:** Executors never construct protobuf `RunEvent`s. They send plain Scala case classes over Spark RPC. The driver is the only place that needs the generated Java proto classes on its classpath. This matters for shading: we only shade `com.google.protobuf` and `com.connectrpc` on the driver side's dependencies — executor messages stay JVM-native.

### 2.2 Plan inspection via listeners, not `QueryPlanner` subclassing

**Decision:** Use `QueryExecutionListener` (for batch), `SparkListener` (for job/stage/task lifecycle), and `StreamingQueryListener` (for streaming progress) on the driver. Do **not** subclass or replace Spark's `QueryPlanner`.

**Why:** `QueryExecutionListener` gives you `QueryExecution` on both success and failure, with `qe.analyzed` (resolved plan) available synchronously. That's the same hook the Databricks OpenLineage integration and the upstream `io.openlineage:openlineage-spark` library use.

`QueryPlanner` is the physical planner (`SparkPlanner`), invoked during logical→physical conversion. Hooking it would force us to intercept every query without a signal for "this query finished." It's also an internal API with less stability than the listener interfaces.

### 2.3 Plugin architecture: `SparkPlugin` with Driver + Executor components

**Decision:** Register a top-level `com.openlakehouse.lineage.LineagePlugin` via `spark.plugins`, returning a `DriverPlugin` (registers listeners) and an `ExecutorPlugin` (sends task-summary messages to the driver).

**Why:**
- `SparkPlugin` was promoted to `@DeveloperApi` in Spark 3.x and is the canonical extension point in 4.x.
- `DriverPlugin.init(SparkContext, PluginContext): java.util.Map[String,String]` returns a config map that Spark automatically ships to every executor's `ExecutorPlugin.init(PluginContext, Map)`. Perfect for our "driver tells executors where to send things" bootstrap (`lineage.driver.host`, etc. — though we use native RPC we may still want to pass a run ID down).
- `PluginContext.send(msg)` routes a message from the executor to the driver's `DriverPlugin.receive(msg)`. Spark handles the serialization, retry, and partial-failure semantics.

No alternative survived contact with the constraint "work in Spark Connect without opening ports."

### 2.4 Spark Connect: plugin loads on the Connect server JVM

**Decision:** In Spark Connect deployments, the plugin JAR and `spark.plugins` configuration must be set on the **Connect server**, not the client. The client code never loads our plugin.

**Why it works that way:** The Connect server is where `QueryExecution` instances actually get constructed. The client just ships a plan proto over gRPC. Listeners registered on the client receive nothing meaningful.

**Operational note:** Documented in [`spark-openlineage-plugin/README.md`](../spark-openlineage-plugin/README.md). The plan includes a `spark-connect-compat` follow-up to add an integration test that actually exercises this.

---

## 3. Protobuf and code generation

### 3.1 Why we generate Java, not Scala protos

- **Language compatibility:** Java proto classes are directly usable from Scala. ScalaPB would introduce a second incompatible representation of the same messages, and the Go/Rust sidecars are already consuming the `protocolbuffers/go` variant.
- **Ecosystem:** `com.google.protobuf` 4.x is more aggressive about runtime version validation (`RuntimeVersion.validateProtobufGencodeVersion(...)` in the generated code). Sticking with Google's official Java bindings means we inherit bug fixes and security updates without waiting for a third-party codegen.

### 3.2 Why we dropped the generated `connect-kotlin` client stubs

The `buf.build/connectrpc/kotlin` plugin produces Kotlin classes whose client methods are `suspend fun`. Calling `suspend fun` from Scala is mechanically possible (the Kotlin compiler lowers `suspend` to a synthetic `Continuation` parameter) but awkward: every call becomes `client.emit(req, ContinuationImpl(...))`, and suspend-thrown errors surface as platform-specific exception types.

**Resolution:** We generate pure-Java proto messages and call `com.connectrpc.ProtocolClient` directly from Scala. `ProtocolClient` is Kotlin but is trivially Java-callable — it exposes plain `unary(request, method, headers): ResponseMessage<R>` methods that don't use `suspend`. We lose the tiny bit of type-safe method-name binding that the generated stubs give you, but that's 3 lines of boilerplate per RPC. Worth it to avoid a Kotlin compiler plugin in the sbt build.

### 3.3 Why protobuf-java 4.x (and what that broke)

The `buf.build/protocolbuffers/java` remote plugin emits code targeting `protobuf-java 4.x` (the `GeneratedFile` base class + `RuntimeVersion.validateProtobufGencodeVersion` call in static init). Pinning `protobuf-java` to 3.x in our build caused `ClassNotFoundException: com.google.protobuf.GeneratedFile` at class load.

**Resolution:** Bumped to `protobuf-java 4.34.1` to match the generator. We **do** still have to think about protobuf runtime coexistence on Spark's classpath — Spark 4.1.1 ships its own shaded protobuf for some subsystems (e.g. Kubernetes client). Our `sbt-assembly` shading rule relocates `com.google.protobuf.**` into `com.openlakehouse.shaded.protobuf.**` so the plugin uses its own copy regardless of what else is on the cluster.

### 3.4 Why we pull in `build.buf:protovalidate`

The `.proto` files under `proto/lineage/v1/` carry `buf.validate.field` annotations (e.g. `string run_id = 1 [(buf.validate.field).string.uuid = true];`). The Java gencode references `build.buf.validate.ValidateProto` directly in descriptor registration. Without the `protovalidate` artifact on the classpath, `Lineage.java` won't compile.

We don't actually *run* validation in the Spark plugin — the Go service is the source of truth for that. We just need the type to resolve. A future cleanup might generate the plugin's Java output with validation stripped, but the dependency is ~2 MB and shaded alongside the rest of the proto runtime, so this is a low-priority optimisation.

---

## 4. Catalyst plan extraction strategy

This section condenses [`COMPAT.md`](../spark-openlineage-plugin/COMPAT.md) into the rules-of-thumb that drive the implementation of `QueryPlanVisitor`.

### 4.1 Read the *analyzed* plan, not the optimized one

We hook `QueryExecutionListener.onSuccess(..., qe: QueryExecution, ...)` and consume `qe.analyzed`. Rationale:

- **Optimization rewrites paths.** A `LogicalRelation` with a catalog table can be rewritten into a `FileSourceScanExec` after optimization; the optimized plan loses the `CatalogTable` identity we want.
- **Analysis resolves names.** `UnresolvedRelation` becomes `LogicalRelation`/`HiveTableRelation`/`DataSourceV2Relation`. We get the catalog-qualified name "for free" without re-implementing Spark's name resolution.
- **Analysis preserves `Option[CatalogTable]`.** For path-based reads (`spark.read.parquet("...")`) `catalogTable` is `None`, so we fall through to `HadoopFsRelation.location.rootPaths` — also still intact at this stage.

### 4.2 Prefer the strongest identity available

Every extractor follows the same fallback chain:

```
catalog-qualified table identifier
  → catalog name + namespace + table name (DSv2)
  → filesystem scheme + authority + path (HadoopFsRelation)
  → JDBC URL + dbtable option (SaveIntoDataSourceCommand)
  → provider class name (last resort; never silently dropped)
```

This means an event for the same physical table will converge on the same `(namespace, name)` key regardless of which code path the user took to read it, which is essential for cross-run lineage stitching in the downstream graph.

### 4.3 One accessor handles the whole V2 write family

`V2WriteCommand` (trait) declares `.table: NamedRelation` and `.query: LogicalPlan`. All of `AppendData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`, and `ReplaceData` implement it. A single `case c: V2WriteCommand => fromV2WriteCommand(c)` covers the whole family, and `NamedRelation.name(): String` is the identity.

`CreateTableAsSelect` and `ReplaceTableAsSelect` do **not** extend `V2WriteCommand` — they extend `V2CreateTableAsSelectPlan` and carry their target as `.name: LogicalPlan` which resolves to a `ResolvedIdentifier`. Two extra cases, `unapply(ResolvedIdentifier)` gives us `(CatalogPlugin, Identifier)` cleanly.

### 4.4 Reflective fallback for connector-specific commands

Delta, Iceberg, and Hudi in Spark 4.x all lower writes through `V2WriteCommand` in the vast majority of cases — so our main extractor already covers them. But we keep a small `ReflectiveSinkMatchers` module that tries `Class.forName` for:

- `org.apache.spark.sql.delta.commands.WriteIntoDelta` (pre-v2 Delta path)
- `org.apache.spark.sql.delta.commands.MergeIntoCommand` (`MERGE INTO` as of delta-spark 4.x)
- `org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand` (in `spark-hive`, not `spark-sql`)
- `org.apache.iceberg.spark.source.SparkWrite$CommitCommand` (rare edge cases)

If the class can't be loaded, the matcher is silently inert — no dependency, no crash. This gives us belt-and-braces coverage for users running older connector versions or hitting code paths that haven't migrated to V2 yet.

### 4.5 No throwing, ever

Every extractor returns `Option[DatasetRef]` or a `Try`-wrapped fallback. The principle is that a misconfigured plan, an unexpected node shape, or a broken reflective lookup must never propagate out of the listener: the worst acceptable outcome is a missing lineage edge, not a user job crash. This is why the fallback chain terminates in a "best effort" `DatasetRef(namespace = "unknown", name = <class-name>)` rather than `None`.

---

## 5. Spark 4.1.1 specifics

### 5.1 Scala 2.13 only

Spark 4.x dropped Scala 2.12. That lets us simplify `build.sbt` (no `crossScalaVersions`), drop `CanBuildFrom`-era workarounds, and rely on Scala 2.13 standard library (`Seq.distinct`, `LazyList`, etc.).

We pinned `scalaVersion := "2.13.14"` to match what Databricks Runtime (DBR) 16 ships. Our published JAR should be loadable on DBR 15.x or stock Apache Spark 4.x, but 2.13.14 is what CI validates against.

### 5.2 JDK 17

Spark 4.x requires JDK 17 minimum. We set `javacOptions ++= Seq("-source", "17", "-target", "17")` and `scalacOptions += "-release:17"`. Test fork gets the reflective-access `--add-opens` flags (`java.base/java.nio=ALL-UNNAMED`, etc.) that Spark needs to warm up on modern JDKs.

### 5.3 Spark Connect is a first-class deployment mode

The plan includes a separate `spark-connect-compat` phase because Spark Connect flips two assumptions we had:

1. **Where the plugin lives.** On the Connect server, not the client. The README documents the config.
2. **What `SparkContext` is available.** The Connect server is a real Spark driver — the usual `SparkContext` is fine. But the client has no meaningful `SparkContext`. Plugin code must run server-side.

No code changes are needed in `LineagePlugin.scala` itself; the deployment story is what differs.

### 5.4 `delta-spark 4.0.0` as test dep only

We depend on `io.delta:delta-spark_2.13:4.0.0` at `% Test` scope so we can write Delta integration tests that exercise the reflective matchers end-to-end. It is **not** a runtime dep — users without Delta on their classpath won't pull it in.

---

## 6. What we're not yet solving

Out of scope for this initial cut, explicitly parked:

1. **Column-level lineage.** Requires walking `plan.expressions` and tracking `AttributeReference` lineage through `Project`/`Aggregate`/`Join` — a much bigger lift. Tracked for a future phase once the dataset-level graph is working end-to-end.
2. **Structured Streaming microbatch-granular facets.** We emit events on `onQueryProgress` but don't yet break out per-batch dataset diffs. `WriteToMicroBatchDataSource` extraction lands the scaffolding.
3. **User-facing facet customization.** Users may want to attach `OwnershipFacet`, `DocumentationFacet`, etc. Ownable through Spark conf (`spark.openlineage.facets.*`) but not wired yet.
4. **Hive Metastore-only deployments.** Works in principle (we handle `HiveTableRelation`), but we haven't stood up a Hive-enabled Spark in CI. Also parked.
5. **Buf Schema Registry auth in CI.** `buf generate` requires BSR login. We commit the generated Java to git (`spark-openlineage-plugin/src/main/generated/**`) so CI builds don't need `buf` or a BSR token. Slightly more repo noise; large net win for build reliability.

---

## 7. Alternatives considered and rejected

### 7.1 Fork the upstream `io.openlineage:openlineage-spark` library

**Rejected.** Upstream is Scala 2.12/2.13 cross-built and has a strong opinion about emitting to the OpenLineage HTTP endpoint (or a handful of transports). Rewiring it to emit our ConnectRPC protobufs would fight the library's architecture at every step, and we'd inherit a lot of code paths we don't need. Writing our own visitor is ~500 LoC; forking would be a maintenance tax forever.

### 7.2 Use `SparkListener.onJobEnd` only and skip `QueryExecutionListener`

**Rejected.** `SparkListener` gives you the *physical* execution graph (stages, tasks) but no `LogicalPlan`. You can work backwards from `SparkListenerSQLExecutionStart.executionId` → `SQLAppStatusStore.executionInfo(id).description`, but the description is a rendered string, not a plan. `QueryExecutionListener` is the cleaner, intended surface.

### 7.3 Emit OpenLineage HTTP POSTs directly

**Rejected** for two reasons:

1. Our downstream is a ConnectRPC Go service, not an OpenLineage-compatible HTTP endpoint. Translating to OpenLineage JSON on the way out and back to protobuf in Go would double-serialize and introduce a schema-drift surface.
2. ConnectRPC over HTTP/2 gives us trailers, deadlines, and typed errors without writing that logic ourselves.

### 7.4 Deploy the plugin as a Spark SQL extension (`spark.sql.extensions`) instead

**Rejected.** `SparkSessionExtensions` are for injecting logical-plan rules, function registrations, and custom parsers — they run *during* query compilation. A lineage producer wants to observe *after* execution, and across multiple query sessions. Wrong tool for the job.

---

## 8. Key file references

| File | Role |
|---|---|
| `proto/lineage/v1/lineage.proto` | Source of truth for event shape. Shared with Go service and Rust sidecar. |
| `buf.gen.yaml` | Generates Go (`gen/`), Java (`spark-openlineage-plugin/src/main/generated/`). |
| `spark-openlineage-plugin/build.sbt` | Scala 2.13 / Spark 4.1.1 / JDK 17 build. Shading rules for protobuf + connectrpc + okhttp + kotlin. |
| `spark-openlineage-plugin/COMPAT.md` | Live reference: Spark 4.1.1 Catalyst node classes + accessor signatures. |
| `spark-openlineage-plugin/src/main/scala/com/openlakehouse/lineage/LineagePlugin.scala` | `SparkPlugin` entry point. |
| `spark-openlineage-plugin/src/main/scala/com/openlakehouse/lineage/driver/QueryPlanVisitor.scala` | The core extraction logic. |
| `spark-openlineage-plugin/src/main/scala/com/openlakehouse/lineage/driver/ReflectiveSinkMatchers.scala` | Connector-specific fallback matcher for Delta/Iceberg/Hive/Hudi. |
| `.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md` | The checked plan driving implementation. |

---

## 9. Validation status

As of 2026-04-22:

- `buf generate` → Java proto classes commit cleanly, 23k LoC of generated code compiles.
- `sbt compile` passes with zero warnings on Scala 2.13.14 + JDK 17.
- `sbt test` runs **76/76 tests green in ~16s across 14 suites**:
  - `LineageConfigSpec` — configuration parsing
  - `QueryPlanVisitorSpec` — source extraction (parquet paths, json paths, catalog tables, joins, self-joins, empty plans, dedup helper)
  - `QueryPlanVisitorSinksSpec` — sink extraction (parquet writes, `saveAsTable`, `INSERT INTO`, CTAS, read-only plans, reflective fallback inert when no connector classpath)
  - `JobNamingSpec` — job-name precedence + sanitisation
  - `FacetEncoderSpec` — struct encoding (strings, mixed types, nested, errors)
  - `RunEventBuilderSpec` — proto conversion + run-facet injection
  - `EventSinkSpec` — Noop / InMemory / Broadcast behaviour under concurrency
  - `TaskMetricsAggregatorSpec` — per-execution rollup + orphan handling + thread safety
  - `LineageQueryListenerSpec` — batch listener end-to-end with real SparkSession (START/COMPLETE, FAIL path, disabled short-circuit, task-metrics folding)
  - `ConnectRpcClientSpec` — Connect unary wire format over MockWebServer
  - `ConnectRpcEventSinkSpec` — backpressure, batching, retry/backoff, graceful drain
  - `StreamingSourceParserSpec` — FileStream / Kafka / Delta / unknown description parsing
  - `LineageStreamingListenerSpec` — START / RUNNING (rate-limited) / COMPLETE / FAIL lifecycle
  - `LineagePluginSmokeSpec` — **end-to-end:** real SparkSession with `spark.plugins`, plugin loads, runs a query, protobuf lands on a MockWebServer

Reviewed against Spark 4.1.1 jar signatures via `javap -p`. Every class name and accessor referenced in the visitor has been verified against the actual `spark-catalyst_2.13-4.1.1.jar` / `spark-sql_2.13-4.1.1.jar` / `spark-sql-api_2.13-4.1.1.jar` published on Maven Central.

For the full system architecture (transport, executor metrics pipeline, streaming lineage, fail-open operational story, and how the phases fit together) see the companion document [`spark-openlineage-architecture.md`](./spark-openlineage-architecture.md).
