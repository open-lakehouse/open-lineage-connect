# open-lineage-service

A Go service that ingests [OpenLineage](https://openlineage.io/) events via two complementary interfaces:

- **REST API** (`POST /lineage`, `POST /lineage/batch`) -- accepts standard OpenLineage JSON directly from any producer, no protobuf tooling required.
- **ConnectRPC** -- typed Protobuf RPCs over Connect, gRPC, or gRPC-Web for internal service-to-service communication.

Both interfaces share the same `LineageService` backend and support `RunEvent`, `JobEvent`, and `DatasetEvent` payloads.

## Prerequisites

- Go 1.22+
- [buf](https://buf.build/docs/installation) CLI (for proto code generation)

## Quick Start

```bash
# generate Go code from protos
make generate

# run the server (default :8090)
make build
PORT=8090 ./open-lineage-service

# or run directly
go run ./cmd/server
```

## Endpoints

### REST (OpenLineage-native JSON)

These endpoints accept the standard [OpenLineage spec](https://openlineage.io/spec/2-0-2/OpenLineage.json) JSON format -- no auth header required on the REST path.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/lineage` | Ingest a single RunEvent, JobEvent, or DatasetEvent |
| `POST` | `/lineage/batch` | Ingest a JSON array of mixed event types |

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

## Testing

```bash
# run tests with coverage
make coverage
```

Tests exercise every RPC over both binary Protobuf and JSON transports, the REST ingestion endpoints with all three event types, batch partial-failure handling, and auth scenarios.

## Project Layout

```
proto/lineage/v1/lineage.proto     # service + message definitions
gen/                               # buf-generated Go code
cmd/server/main.go                 # server entrypoint
internal/ingest/converter.go       # OpenLineage JSON -> proto conversion
internal/ingest/handler.go         # REST ingestion handler (POST /lineage, /lineage/batch)
internal/interceptor/auth.go       # authorization interceptor
internal/interceptor/validate.go   # protovalidate interceptor
internal/service/lineage.go        # in-memory service implementation
resources/specs/openapi.json       # OpenLineage OpenAPI specification
resources/examples/                # example JSON payloads for each event type
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `generate` | Run `buf generate` |
| `build` | `go build ./...` |
| `test` | Run tests with coverage profile |
| `coverage` | Print per-function coverage |
| `lint` | Run `buf lint` |
| `docker-build` | Build Docker image |
| `docker-run` | Build and run in Docker |
| `clean` | Remove generated code and coverage files |
