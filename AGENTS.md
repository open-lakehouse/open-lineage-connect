# Agents Guide — open-lineage-service

Lessons learned and context for AI agents working in this repo.

## Repository Layout

- The Go lineage service lives under `services/lineage/` (holds `cmd/`, `internal/`, `gen/`, `go.mod`, `go.sum`, `vendor/`). The Go module path is unchanged: `github.com/open-lakehouse/open-lineage-service`.
- The Rust table service lives under `crates/table-service/` and is a member of the Cargo **workspace** defined by the root `Cargo.toml`. The shared `Cargo.lock` and `.cargo/config.toml` live at the repo root, and the workspace `target/` dir is also at the repo root.
- `proto/` (source protos) and `resources/` (shared test fixtures, used by both the Go and Rust test suites) stay at the repo root.

## Buf / Proto Code Generation

- The proto lives in `proto/lineage/v1/lineage.proto`. Generated Go code lands in `services/lineage/gen/`.
- The `.proto` file **must** include a `go_package` option matching the Go module path, e.g. `github.com/open-lakehouse/open-lineage-service/gen/lineage/v1;lineagev1`. Without it, `buf generate` fails with a cryptic `protoc-gen-connect-go` error about import paths.
- Well-known types (`google.protobuf.Timestamp`, `Struct`) are bundled with buf — do **not** add `buf.build/googleapis/googleapis` to `buf.yaml` deps. buf will warn about unused deps if you do.
- After changing the proto, always `rm -rf services/lineage/gen && buf generate` to avoid stale files.
- **Protobuf gencode/runtime pin (Spark plugin):** the committed Java gencode under `spark-openlineage-plugin/src/main/generated/` is stamped with a `RuntimeVersion` (currently `4.35.0`). `protobufVersion` in `spark-openlineage-plugin/build.sbt` is the *runtime* and **must be ≥ the gencode version**, or every suite that touches a generated message aborts at class-init with `ProtobufRuntimeVersionException: Detected incompatible Protobuf Gencode/Runtime versions ... runtime version cannot be older than the linked gencode version`. When you regenerate the Java protos, bump `protobufVersion` to match.

## ConnectRPC Patterns

- `connect.UnaryInterceptorFunc` is the simplest way to create an interceptor when you only need unary support (no streaming RPCs in this service). It satisfies `connect.Interceptor`.
- ConnectRPC clients accept `connect.WithProtoJSON()` to switch from binary protobuf to JSON encoding. Tests should exercise both to ensure the service handles each codec correctly.
- For integration tests, `httptest.NewServer` + the generated `NewLineageServiceClient` is the canonical pattern. No need for `h2c` in tests — `httptest` with HTTP/1.1 works fine for the Connect protocol.

## Coverage Gotchas

- `go tool cover -func` reports coverage across **all** packages, including generated code (`services/lineage/gen/`) and `services/lineage/cmd/server/main.go`. These drag the total percentage down dramatically even when hand-written code is at 100%.
- When evaluating coverage, focus on the `services/lineage/internal/` packages. Generated code and the `main()` entrypoint are excluded from the 80% target.
- Closures returned by interceptor constructors are only counted as covered when exercised **within the same package's tests**. Cross-package integration tests (e.g. service tests calling through the interceptor) do not contribute to the interceptor package's coverage number. Add in-package integration tests if the interceptor coverage looks low.

## Docker

- The Go service `Dockerfile` (`services/lineage/Dockerfile`) uses a two-stage build: a `golang` builder for compilation, `alpine:3.21` for the runtime image. The binary is statically linked (`CGO_ENABLED=0`). Its docker build context is `services/lineage/` (set in `docker-compose.yaml`), with a dedicated `services/lineage/.dockerignore`.
- Default port is `8090`, configurable via `PORT` env var.
- **`.dockerignore` is mandatory.** The root `.dockerignore` guards the Rust build (`crates/table-service/Dockerfile`, whose build context is the repo root). Without it, `COPY` would pull in `target/` (~9 GB of Rust artifacts), the Spark plugin's sbt target (~100 MB), `.git/`, `demo/results/`, etc. — yielding a 10 GB+ build context that routinely fills the Docker daemon's disk and corrupts BuildKit's containerd metadata store. Symptom: `failed to solve: write /var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db: input/output error`. Recovery: `just docker-recover` (followed by a Docker Desktop / Colima restart if I/O errors persist).

## Rust Sidecar (crates/table-service)

- The Rust table-service lives in `crates/table-service/` and is a Cargo workspace member (root `Cargo.toml`). The crate/package name is `table-service` (so the lib crate is `table_service` and the built binary is `target/release/table-service`).
- It uses `connectrpc` (Anthropic's connect-rust) for RPC and `deltalake` for Delta Lake writes. As of the iceberg-sink work it also uses `iceberg` + `iceberg-catalog-rest` for Apache Iceberg writes via Lakekeeper.
- Proto code generation uses `connectrpc-build` in `build.rs`, reading from `proto-export/` (a vendored copy of all protos with buf deps resolved).
- Run `make proto-export` from the repo root before building the Rust project. This runs `buf export` to produce the self-contained proto tree.
- `make rust-build` and `make rust-test` are the canonical build/test targets. They invoke `cargo build -p table-service` / `cargo test -p table-service` from the repo root (the workspace handles path resolution; `build.rs` still resolves `proto-export/` relative to the crate dir).
- **Cargo target dir gotcha**: every shell invocation that escalates to `required_permissions: ["all"]` lands in a fresh sandbox cache (`/var/folders/.../cursor-sandbox-cache/<hash>/cargo-target`), forcing a full rebuild of the deltalake+iceberg+datafusion dep tree (~75s clean). The workspace already places `target/` at the repo root; prefer running cargo with `required_permissions: ["full_network"]` (not `["all"]`) so builds reuse that root `target/` and stay incremental (~1–4s). If you must use `["all"]`, export `CARGO_TARGET_DIR="$(pwd)/target"` first.

## Iceberg Sink (writer/iceberg.rs)

- Sinks are pluggable via the `TableSink` trait (`writer/sink.rs`). `TABLE_SINKS=delta,iceberg` fan-outs every `WriteBatch` RPC to both sinks; ordering matters and the first failure short-circuits the RPC with that sink's error prefix.
- The Iceberg sink talks to **any** REST catalog by default — `Lakekeeper` is the project-blessed open-source choice, but Polaris / Tabular / Nessie also work. Production wiring goes through `IcebergSink::from_config(&IcebergConfig)`.
- **Embedded tests use `iceberg::memory::MemoryCatalog`** (no Docker, no network). `tests/iceberg_integration_test.rs` carries `#[ignore]`'d tests that talk to a real Lakekeeper brought up via `just stack-up-iceberg`. Run with `just test-iceberg-integration`.
- **Field-id gotcha**: `iceberg::arrow::arrow_schema_to_schema` requires every Arrow field to already carry `PARQUET:field_id` metadata, which our canonical `arrow_schema()` deliberately doesn't. We therefore hand-roll `iceberg_schema()` natively (via `iceberg::spec::Schema::builder`) and rebind incoming RecordBatches onto the field-id-tagged Arrow schema produced by `iceberg::arrow::schema_to_arrow_schema(iceberg_schema())` before handing them off to the writer.
- **Timezone gotcha**: Delta uses tz string `"UTC"`; Iceberg's `Timestamptz` materializes to `"+00:00"` (per `iceberg::arrow::UTC_TIME_ZONE`). The append path runs an `arrow::compute::cast` per column to bridge the two — for tz-only changes this is a metadata edit, not a data rewrite.
- **Partitioning v1**: `RecordBatchPartitionSplitter` in iceberg-rust 0.7 is `pub(crate)`, so we ship our own minimal fanout in `IcebergSink::split_by_partition`. Supported: single- or multi-column **identity-transform** partitions on **Utf8/string** columns (covers `event_kind`). Anything else returns a clear error at write time.
- **File-name uniqueness**: `DefaultFileNameGenerator` only auto-uniques *within* one writer instance. Each `append` call must construct its own writer with a per-call unique `suffix` (we use unix-nanos + group-index) or the `fast_append` transaction will reject the second write with `Cannot add files that are already referenced by table`.
- **Crate version pinning**: `iceberg = 0.7` and `iceberg-catalog-rest = 0.7` because they're the last release line that still pins `arrow/parquet 55.x`, matching what `deltalake = 0.28` pulls in. Bumping iceberg ≥0.8 also requires bumping deltalake (or vendoring an arrow-version bridge).

## Event Forwarding (Go → Rust)

- The Go service has an `OnEvent` callback mechanism on `LineageService` that fires after every `StoreEvent` call.
- The `services/lineage/internal/forwarder` package implements an async batching client that buffers events and sends them to the Rust sidecar via `WriteBatch` RPC.
- Forwarding is enabled when `TABLE_SERVICE_URL` is set (e.g. `http://localhost:8091`).
- The forwarder never blocks the caller — events are enqueued to a buffered channel and flushed in the background.

## Docker Compose / ECS

- `docker-compose.yaml` at the repo root runs both services plus MinIO for S3-compatible testing.
- `ecs/task-definition.json` is a Fargate task template with both containers sharing `localhost` via `awsvpc` networking.

## Spark Plugin Config Reference (`spark.openlineage.*`)

Parsed in `spark-openlineage-plugin/.../LineageConfig.scala`. Beyond the core
keys (`serviceUrl`, `namespace`, `authToken`, `jobName`, `emit.*`, `queueSize`,
`batchFlushMs`, `disabled`, `failOpen`):

- **User-defined facets:** `spark.openlineage.facets.run.<key>=<value>` and
  `spark.openlineage.facets.job.<key>=<value>` inject operator facets onto
  `Run.facets` / `Job.facets`. Built-in run facets win on key collision; empty
  values are dropped. Run facets are merged in both listeners; job facets are
  encoded by `RunEventBuilder.buildJob`.
- **TLS / mTLS:** `spark.openlineage.tls.trustStorePath|trustStorePassword|trustStoreType`
  (pin a private-CA/self-signed lineage-service cert) and `…tls.keyStorePath|keyStorePassword|keyStoreType`
  (client cert for mTLS). Store type defaults to `PKCS12`. `ConnectRpcClient.okHttpForTls`
  builds the `SSLContext`; `buildSink` uses it whenever any trust/key material is set.
- **Retry/backoff:** `spark.openlineage.retry.maxRetries` (default 2),
  `retry.backoffMs` (default 200, exponential), `retry.jitterFactor` (default
  0.5, symmetric jitter in `[0,1]`). The pure math lives in
  `transport/RetryBackoff.scala` (jitter via an injectable `rng` for
  deterministic tests).
- **Rate limiting:** `spark.openlineage.rateLimit.minIntervalMs` (default 0 =
  off) spaces flushes to the single lineage endpoint; logic in
  `transport/RateLimiter.scala` (injectable clock + sleeper for tests).

## Follow-ups / Parked Work

Historic "future work" items (see `research/spark-openlineage-architecture.md`
§12 and `research/spark4-lineage-extraction.md` §6). Status:

- **Lakekeeper bearer token (Iceberg sink)** — DONE. Threaded via
  `iceberg::build_rest_props` (sourced from `ICEBERG_TOKEN`), covered by
  `build_rest_props_*` unit tests plus the `#[ignore]`d
  `iceberg_integration_test::auth_token_*` live test.
- **Plugin ConnectRPC emitter** — DONE (was already wired in `buildSink`;
  selection covered by `LineageDriverPluginSpec`).
- **User-defined facets** — DONE (`spark.openlineage.facets.run/job.*`).
- **TLS + mTLS transport** — DONE (`spark.openlineage.tls.*`).
- **Retry policy tuning (jitter + rate limiting)** — DONE
  (`spark.openlineage.retry.*` / `rateLimit.*`).
- **Hive Metastore coverage** — DONE via a deterministic unit test
  (`QueryPlanVisitorHiveSpec` constructs a `HiveTableRelation` directly, no
  metastore needed). A Hive-enabled-Spark CI job remains optional.
- **Spark Connect true E2E** — DONE as opt-in: `make spark-connect-e2e`
  (`scripts/spark-connect-e2e.sh`) boots a real Connect server with the plugin
  and drives it from a remote session; runs in CI only via the manual
  `spark-connect-e2e` job in `.github/workflows/ci.yml`. The in-JVM
  `LineagePluginSmokeSpec` remains the fast default-path check.

Note: `.cursor/plans/spark_openlineage_plugin_2a538db4.plan.md` and
`rust_sidecar_table_service_adec9bc5.plan.md` do exist (the `Glob` tool skips
`.cursor/`), so the references to them in `build.sbt`/research docs are live, not
dangling.

## Project Conventions

- Service implementations go in `services/lineage/internal/service/`.
- Interceptors go in `services/lineage/internal/interceptor/`.
- Test files sit alongside their source (`foo.go` -> `foo_test.go`).
- Table-driven tests are preferred. Name format: `TestFunctionName_Scenario`.
