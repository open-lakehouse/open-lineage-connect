# open-lineage-connect

End-to-end [OpenLineage](https://openlineage.io/) pipeline for an open lakehouse:

- a **Go ingest service** that accepts events over REST and ConnectRPC,
- a **Rust table-service sidecar** that persists every event into a Delta Lake
  table (local FS, S3, or Unity Catalog), and
- an **Apache Spark 4 plugin** that observes SQL / DataFrame / Structured
  Streaming jobs and emits typed `lineage.v1.RunEvent` protobufs (including
  full column-level lineage following the OpenLineage 1-2-0 spec).

## Architecture

```
              ┌────────────────────┐         ┌──────────────────────┐
              │  Spark plugin      │         │  External producers  │
              │  (Scala 2.13)      │         │  (Airflow, dbt, …)   │
              └──────────┬─────────┘         └─────────┬────────────┘
                         │ ConnectRPC (proto)          │ OpenLineage JSON
                         ▼                              ▼
                ┌───────────────────────────────────────────────────┐
                │  open-lineage-service (Go, :8090)                 │
                │   • REST: POST /lineage, /lineage/batch           │
                │   • ConnectRPC: IngestEvent / IngestBatch / …     │
                │   • Auth (Bearer), validation, batching           │
                └────────────────────────────┬──────────────────────┘
                                             │ ConnectRPC
                                             ▼
                ┌───────────────────────────────────────────────────┐
                │  open-lakehouse-table-service (Rust, :8091)       │
                │   • TableWriterService.WriteEvent / WriteBatch    │
                │   • Arrow + deltalake → Delta Lake table          │
                │   • Local FS / S3 / Unity Catalog volumes         │
                └────────────────────────────┬──────────────────────┘
                                             ▼
                                    ┌─────────────────┐
                                    │  Delta table    │
                                    │  (events log)   │
                                    └─────────────────┘
```

The Go service is the network entry point. Whenever it ingests an event it
fires a non-blocking `OnEvent` callback; the `internal/forwarder` package
batches those callbacks and ships them to the Rust sidecar over ConnectRPC.
The Rust sidecar converts each event into an Arrow `RecordBatch` (with a
nullable `column_lineage_json` column for the typed
`ColumnLineageDatasetFacet`) and appends to Delta.

| Component | Language | Default port | Source dir |
|-----------|----------|--------------|------------|
| Ingest service | Go 1.22+ | `8090` | [`cmd/`, `internal/`](./internal) |
| Table-service sidecar | Rust 1.88+ | `8091` | [`open-lakehouse-table-service/`](./open-lakehouse-table-service) |
| Spark plugin | Scala 2.13 / Spark 4.x | n/a | [`spark-openlineage-plugin/`](./spark-openlineage-plugin) |

## Prerequisites

- Go 1.22+
- Rust 1.88+ (only if you build / run the table-service locally)
- [buf](https://buf.build/docs/installation) CLI (for proto code generation)
- Docker (for the all-in-one `docker compose` workflow)
- sbt 1.10+ and JDK 17 (only if you build the Spark plugin)
- [just](https://github.com/casey/just) (for scripted demo orchestration)
- Spark 4.x with `spark-submit` on `PATH` (for the E2E Spark demo)

## Quick start

### Run just the Go service

```bash
# generate Go code from protos
make generate

# run the server (default :8090)
make build
PORT=8090 ./open-lineage-service

# or run directly
go run ./cmd/server
```

### Run the full stack (Go ingest + Rust table-service + MinIO) with Docker Compose

```bash
make docker-compose-up        # builds both images, starts both services + MinIO
curl -sf http://localhost:8090/health
curl -sf http://localhost:8091/health
make docker-compose-down      # tears it down and removes the Delta volume
```

`docker-compose.yaml` wires the Go service's `TABLE_SERVICE_URL` to the Rust
sidecar so events flow through to Delta automatically. MinIO is included for
S3-backend integration tests; see the [Rust table-service](#rust-table-service)
section below for the storage backends supported.

If your environment requires a Cargo mirror/proxy for Rust dependency downloads,
set `CRATES_PROXY` in a local `.env` file (Docker Compose reads it automatically):

```bash
cp .env.example .env
# then set CRATES_PROXY from ~/.cargo/config.toml
```

`CRATES_PROXY` accepts either `https://...` or `sparse+https://...`.
You can also set `SPARK_HOME` in `.env`; `just run-e2e-demo` uses
`$SPARK_HOME/bin/spark-submit` when present.
If Maven Central is blocked, set `SPARK_MAVEN_REPOSITORIES` in `.env` and the
demo will pass it through to `spark-submit --repositories`.
If ConnectRPC auth is enabled on the Go service, set
`OPENLINEAGE_AUTH_TOKEN` (defaults to `valid-token` in the local demo template).

### Run the E2E Spark demo with Just

`just run-e2e-demo` spins up the stack, runs a Spark job that joins the demo
`orders` and `users` Delta tables, and then copies lineage table artifacts into
`demo/results`.

```bash
# default output=deltalake
just run-e2e-demo

# explicit deltalake output
just run-e2e-demo output=deltalake

# reserved for a follow-up PR (exits non-zero)
just run-e2e-demo output=iceberg
```

Expected artifacts after a successful Delta run:

- `demo/results/deltalake/spark-output/user_orders` (joined output Delta table)
- `demo/results/deltalake/lineage-events` (lineage events table files copied from `table-service:/data/events`)

## Endpoints

### REST (OpenLineage-native JSON)

These endpoints accept the standard [OpenLineage spec](https://openlineage.io/spec/2-0-2/OpenLineage.json) JSON format -- no auth header required on the REST path.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/lineage` | Ingest a single RunEvent, JobEvent, or DatasetEvent |
| `POST` | `/lineage/batch` | Ingest a JSON array of mixed event types |
| `GET`  | `/health` | Service readiness check (returns `200` only when dependencies are ready; when `TABLE_SERVICE_URL` is set this includes sidecar `/health`) |

### ConnectRPC

All RPCs require an `Authorization: Bearer valid-token` header.

| RPC | Description |
|-----|-------------|
| `IngestEvent` | Ingest a single RunEvent (proto wrapper) |
| `IngestBatch` | Ingest multiple RunEvents in one call |
| `IngestOpenLineageBatch` | Ingest a batch of typed OpenLineageEvents (RunEvent, JobEvent, DatasetEvent) |
| `QueryLineage` | Query lineage graph for a job (stub) |

---

## Sending Events with `curl` (REST API)

The REST endpoints accept standard OpenLineage JSON. Event type is auto-detected from the payload: `run` + `job` fields indicate a RunEvent, `dataset` alone indicates a DatasetEvent, and `job` without `run` indicates a JobEvent.

### Ingest a single RunEvent

```bash
curl -s -X POST http://localhost:8090/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "eventTime": "2025-06-01T12:00:00Z",
    "producer": "https://github.com/open-lakehouse/open-lineage-service",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    "eventType": "START",
    "run": { "runId": "run-abc-123" },
    "job": { "namespace": "my-scheduler", "name": "etl.transform_orders" },
    "inputs":  [{ "namespace": "warehouse", "name": "raw.orders" }],
    "outputs": [{ "namespace": "warehouse", "name": "curated.orders" }]
  }'
```

### Ingest a JobEvent

```bash
curl -s -X POST http://localhost:8090/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "eventTime": "2024-06-15T12:00:00Z",
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    "job": {
      "namespace": "airflow-prod",
      "name": "dag_weekly_report.generate_summary",
      "facets": {
        "sql": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
          "query": "SELECT region, SUM(revenue) FROM sales GROUP BY region"
        }
      }
    },
    "inputs":  [{ "namespace": "bigquery", "name": "project.dataset.sales" }],
    "outputs": [{ "namespace": "bigquery", "name": "project.dataset.weekly_summary" }]
  }'
```

### Ingest a DatasetEvent

```bash
curl -s -X POST http://localhost:8090/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "eventTime": "2024-06-15T08:00:00Z",
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    "dataset": {
      "namespace": "s3://data-lake",
      "name": "raw/clickstream/2024-06-15",
      "facets": {
        "schema": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
          "fields": [
            { "name": "event_id", "type": "STRING" },
            { "name": "user_id", "type": "STRING" }
          ]
        }
      }
    }
  }'
```

### Ingest a batch of mixed events

```bash
curl -s -X POST http://localhost:8090/lineage/batch \
  -H "Content-Type: application/json" \
  -d '[
    {
      "eventTime": "2025-06-01T12:00:00Z",
      "producer": "https://github.com/open-lakehouse/open-lineage-service",
      "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
      "eventType": "COMPLETE",
      "run": { "runId": "run-abc-123" },
      "job": { "namespace": "my-scheduler", "name": "etl.transform_orders" }
    },
    {
      "eventTime": "2024-06-15T12:00:00Z",
      "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
      "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
      "job": { "namespace": "airflow-prod", "name": "dag_weekly_report.generate_summary" }
    },
    {
      "eventTime": "2024-06-15T08:00:00Z",
      "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
      "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
      "dataset": { "namespace": "s3://data-lake", "name": "raw/clickstream/2024-06-15" }
    }
  ]'
```

Expected response:

```json
{
  "status": "success",
  "summary": { "received": 3, "successful": 3, "failed": 0 }
}
```

You can also send example files directly:

```bash
curl -s -X POST http://localhost:8090/lineage \
  -H "Content-Type: application/json" \
  -d @resources/examples/lineage/single/run-event.json

curl -s -X POST http://localhost:8090/lineage/batch \
  -H "Content-Type: application/json" \
  -d @resources/examples/lineage/batch/mixed-event-batch.json
```

---

## Sending Events with `buf curl` (ConnectRPC)

[`buf curl`](https://buf.build/docs/reference/cli/buf/curl/) uses the local proto schema (`--schema .`) to encode typed Protobuf requests. All ConnectRPC endpoints require the `Authorization: Bearer valid-token` header.

### Ingest a single RunEvent

```bash
buf curl --schema . --protocol connect \
  --header "Authorization: Bearer valid-token" \
  --data '{
    "event": {
      "eventType": "START",
      "eventTime": "2025-06-01T12:00:00Z",
      "run":  { "runId": "run-abc-123" },
      "job":  { "namespace": "my-scheduler", "name": "etl.transform_orders" },
      "inputs":  [{ "namespace": "warehouse", "name": "raw.orders" }],
      "outputs": [{ "namespace": "warehouse", "name": "curated.orders" }],
      "producer": "https://github.com/open-lakehouse/open-lineage-service"
    }
  }' \
  http://0.0.0.0:8090/lineage.v1.LineageService/IngestEvent
```

Expected response:

```json
{"status": "ok"}
```

### Ingest a batch of RunEvents

```bash
buf curl --schema . --protocol connect \
  --header "Authorization: Bearer valid-token" \
  --data '{
    "events": [
      {
        "eventType": "START",
        "eventTime": "2025-06-01T12:00:00Z",
        "run": { "runId": "run-abc-123" },
        "job": { "namespace": "my-scheduler", "name": "etl.transform_orders" },
        "producer": "https://github.com/open-lakehouse/open-lineage-service"
      },
      {
        "eventType": "COMPLETE",
        "eventTime": "2025-06-01T12:05:00Z",
        "run": { "runId": "run-abc-123" },
        "job": { "namespace": "my-scheduler", "name": "etl.transform_orders" },
        "outputs": [{ "namespace": "warehouse", "name": "curated.orders" }],
        "producer": "https://github.com/open-lakehouse/open-lineage-service"
      }
    ]
  }' \
  http://0.0.0.0:8090/lineage.v1.LineageService/IngestBatch
```

Expected response:

```json
{"ingested": 2}
```

### Ingest a typed OpenLineage batch (mixed event types)

```bash
buf curl --schema . --protocol connect \
  --header "Authorization: Bearer valid-token" \
  --data '{
    "events": [
      {
        "runEvent": {
          "eventType": "START",
          "eventTime": "2025-06-01T12:00:00Z",
          "run": { "runId": "run-abc-123" },
          "job": { "namespace": "my-scheduler", "name": "etl.transform_orders" },
          "producer": "https://github.com/open-lakehouse/open-lineage-service"
        }
      },
      {
        "jobEvent": {
          "eventTime": "2024-06-15T12:00:00Z",
          "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
          "job": { "namespace": "airflow-prod", "name": "dag_weekly_report.generate_summary" }
        }
      },
      {
        "datasetEvent": {
          "eventTime": "2024-06-15T08:00:00Z",
          "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
          "dataset": { "namespace": "s3://data-lake", "name": "raw/clickstream/2024-06-15" }
        }
      }
    ]
  }' \
  http://0.0.0.0:8090/lineage.v1.LineageService/IngestOpenLineageBatch
```

Expected response:

```json
{"status": "success", "summary": {"received": 3, "successful": 3}}
```

### Query lineage for a job

```bash
buf curl --schema . --protocol connect \
  --header "Authorization: Bearer valid-token" \
  --data '{
    "jobNamespace": "my-scheduler",
    "jobName": "etl.transform_orders",
    "depth": 1
  }' \
  http://0.0.0.0:8090/lineage.v1.LineageService/QueryLineage
```

## Rust table-service

The [`open-lakehouse-table-service`](./open-lakehouse-table-service) sidecar
is the persistence half of the pipeline. It exposes a ConnectRPC service
(`table.v1.TableWriterService`) over HTTP/1.1 with a single hot path
(`WriteBatch`) plus a unary fallback (`WriteEvent`), and writes every event
into a Delta Lake table.

### Build / test

```bash
make rust-build           # buf export → cargo build
make rust-test            # buf export → cargo test (8 integration tests)
make docker-build-table   # build only the table-service Docker image
```

`make proto-export` is the prerequisite step: it runs `buf export` so that
`connectrpc-build` in the Rust crate can compile the proto tree without
needing `buf` at build time. `make rust-build` and `make rust-test` invoke it
automatically.

### Configuration (env vars)

| Variable | Default | Description |
|----------|---------|-------------|
| `TABLE_SERVICE_PORT` | `8091` | TCP port the service listens on. |
| `DELTA_STORAGE` | `local` | One of `local`, `s3`, or `unity`. |
| `DELTA_TABLE_PATH` | `/data/events` | Local path or `s3://bucket/prefix` URL for the Delta table. |
| `DELTA_PARTITION_COLS` | `event_kind` | Comma-separated list of partition columns (`event_kind`, `event_type`, …). |
| `RUST_LOG` | *(unset)* | Standard `tracing-subscriber` filter (e.g. `info`, `open_lakehouse_table_service=debug`). |

S3 / Unity Catalog credentials are picked up from standard environment
variables when `DELTA_STORAGE=s3` or `unity`:

| Variable | Used by |
|----------|---------|
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `s3` backend |
| `AWS_ENDPOINT_URL`, `AWS_S3_ALLOW_UNSAFE_RENAME` | `s3` backend (MinIO) |
| `UNITY_CATALOG_URL`, `UNITY_CATALOG_TOKEN` | `unity` backend |

### Delta schema

Each event is stored as one row in the Delta table. Column lineage piggybacks
on a nullable `column_lineage_json` column whose JSON shape mirrors the
OpenLineage 1-2-0 `ColumnLineageDatasetFacet` so downstream consumers can
read it without re-deriving the schema.

| Column | Type | Notes |
|--------|------|-------|
| `event_kind` | `STRING NOT NULL` | `run` \| `job` \| `dataset` |
| `event_type` | `STRING` | RunEvent: `START` / `RUNNING` / `COMPLETE` / `FAIL` / `ABORT` / `OTHER`; null for job/dataset events. |
| `event_time` | `TIMESTAMP(UTC) NOT NULL` | Microsecond precision. |
| `producer` | `STRING NOT NULL` | OpenLineage `producer` URI. |
| `schema_url` | `STRING` | OpenLineage `schemaURL`. |
| `run_id`, `job_namespace`, `job_name` | `STRING` | Set on RunEvents and JobEvents respectively. |
| `dataset_namespace`, `dataset_name` | `STRING` | Set on DatasetEvents. |
| `facets_json`, `inputs_json`, `outputs_json` | `STRING` | JSON copies of the typed facet/dataset arrays. |
| `column_lineage_json` | `STRING` | Per-event JSON of every input/output's typed `ColumnLineageDatasetFacet`. Null when no dataset on the event carries column lineage. |
| `raw_json` | `STRING` | Original event JSON when the producer was the REST endpoint, otherwise null. |

The default partition column is `event_kind`, which keeps run/job/dataset
streams cleanly separated on disk; override with `DELTA_PARTITION_COLS`.

## Testing

```bash
# Go: run tests with coverage
make coverage

# Rust: 8 integration tests (Arrow conversion + Delta round-trip + JSON fixture)
make rust-test

# Spark plugin: 100 tests across 16 suites (sbt + JDK 17 required)
make spark-plugin-test
```

Go tests exercise every RPC over both binary Protobuf and JSON transports,
the REST ingestion endpoints with all three event types, batch
partial-failure handling, auth scenarios, and the Go → Rust forwarder.

## Project Layout

```
proto/                                      # protobuf source of truth
  lineage/v1/lineage.proto                  # OpenLineage event + service definitions
  table/v1/table_writer.proto               # Rust table-service writer RPCs
gen/                                        # buf-generated Go code

cmd/server/main.go                          # Go server entrypoint
internal/ingest/converter.go                # OpenLineage JSON → proto conversion
internal/ingest/handler.go                  # REST ingestion (POST /lineage, /lineage/batch)
internal/interceptor/auth.go                # Bearer-token authorization interceptor
internal/interceptor/validate.go            # protovalidate interceptor
internal/service/lineage.go                 # in-memory service implementation
internal/forwarder/forwarder.go             # async batched Go → Rust forwarder

open-lakehouse-table-service/               # Rust sidecar (Cargo crate)
  src/main.rs                               # axum + ConnectRPC bootstrap
  src/service.rs                            # TableWriterService impl
  src/writer/schema.rs                      # Arrow schema + per-event serialization
  src/writer/delta.rs                       # deltalake writer wrapper
  src/config.rs                             # env-var configuration
  build.rs                                  # connectrpc-build (consumes proto-export/)
  tests/integration_test.rs                 # Delta round-trip + column-lineage fixture
  Dockerfile

spark-openlineage-plugin/                   # Spark 4 plugin (Scala 2.13)
  src/main/scala/com/openlakehouse/lineage/ # driver + executor + streaming + transport
  src/test/scala/...                        # 100 unit + integration tests
  README.md, COMPAT.md                      # quickstart + Catalyst API surface

ecs/task-definition.json                    # Fargate template (both services as one task)
docker-compose.yaml                         # Go + Rust + MinIO local stack
resources/examples/                         # example JSON payloads (incl. column-lineage)
resources/specs/openapi.json                # OpenLineage OpenAPI specification
research/                                   # design docs (Spark architecture, lineage extraction)
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `generate` | Run `buf generate` (regenerates Go + Java protos). |
| `proto-export` | `buf export` the proto tree into `open-lakehouse-table-service/proto-export` (consumed by Rust at build time). |
| `build` | `go build ./...`. |
| `test` / `coverage` | Run Go tests with coverage profile / print per-function coverage. |
| `lint` | Run `buf lint`. |
| `rust-build` / `rust-test` | Build / test the Rust table-service (auto-runs `proto-export`). |
| `spark-plugin-build` / `spark-plugin-test` / `spark-plugin-clean` | Build / test / clean the Spark plugin via sbt. |
| `docker-build` / `docker-run` | Build / run the Go service Docker image. |
| `docker-build-table` | Build the Rust table-service Docker image. |
| `docker-compose-up` / `docker-compose-down` | Start / stop the full local stack (Go + Rust + MinIO). |
| `clean` | Remove `gen/`, `coverage.out`, `proto-export/`, and run `sbt clean`. |
