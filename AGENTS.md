# Agents Guide — open-lineage-service

Lessons learned and context for AI agents working in this repo.

## Buf / Proto Code Generation

- The proto lives in `proto/lineage/v1/lineage.proto`. Generated Go code lands in `gen/`.
- The `.proto` file **must** include a `go_package` option matching the Go module path, e.g. `github.com/open-lakehouse/open-lineage-service/gen/lineage/v1;lineagev1`. Without it, `buf generate` fails with a cryptic `protoc-gen-connect-go` error about import paths.
- Well-known types (`google.protobuf.Timestamp`, `Struct`) are bundled with buf — do **not** add `buf.build/googleapis/googleapis` to `buf.yaml` deps. buf will warn about unused deps if you do.
- After changing the proto, always `rm -rf gen && buf generate` to avoid stale files.

## ConnectRPC Patterns

- `connect.UnaryInterceptorFunc` is the simplest way to create an interceptor when you only need unary support (no streaming RPCs in this service). It satisfies `connect.Interceptor`.
- ConnectRPC clients accept `connect.WithProtoJSON()` to switch from binary protobuf to JSON encoding. Tests should exercise both to ensure the service handles each codec correctly.
- For integration tests, `httptest.NewServer` + the generated `NewLineageServiceClient` is the canonical pattern. No need for `h2c` in tests — `httptest` with HTTP/1.1 works fine for the Connect protocol.

## Coverage Gotchas

- `go tool cover -func` reports coverage across **all** packages, including generated code (`gen/`) and `cmd/server/main.go`. These drag the total percentage down dramatically even when hand-written code is at 100%.
- When evaluating coverage, focus on the `internal/` packages. Generated code and the `main()` entrypoint are excluded from the 80% target.
- Closures returned by interceptor constructors are only counted as covered when exercised **within the same package's tests**. Cross-package integration tests (e.g. service tests calling through the interceptor) do not contribute to the interceptor package's coverage number. Add in-package integration tests if the interceptor coverage looks low.

## Docker

- The `Dockerfile` uses a two-stage build: `golang:1.24-alpine` for compilation, `alpine:3.21` for the runtime image. The binary is statically linked (`CGO_ENABLED=0`).
- Default port is `8090`, configurable via `PORT` env var.

## Rust Sidecar (open-lakehouse-table-service)

- The Rust table-service lives in `open-lakehouse-table-service/` as a subdirectory.
- It uses `connectrpc` (Anthropic's connect-rust) for RPC and `deltalake` for Delta Lake writes.
- Proto code generation uses `connectrpc-build` in `build.rs`, reading from `proto-export/` (a vendored copy of all protos with buf deps resolved).
- Run `make proto-export` from the repo root before building the Rust project. This runs `buf export` to produce the self-contained proto tree.
- `make rust-build` and `make rust-test` are the canonical build/test targets.

## Event Forwarding (Go → Rust)

- The Go service has an `OnEvent` callback mechanism on `LineageService` that fires after every `StoreEvent` call.
- The `internal/forwarder` package implements an async batching client that buffers events and sends them to the Rust sidecar via `WriteBatch` RPC.
- Forwarding is enabled when `TABLE_SERVICE_URL` is set (e.g. `http://localhost:8091`).
- The forwarder never blocks the caller — events are enqueued to a buffered channel and flushed in the background.

## Docker Compose / ECS

- `docker-compose.yaml` at the repo root runs both services plus MinIO for S3-compatible testing.
- `ecs/task-definition.json` is a Fargate task template with both containers sharing `localhost` via `awsvpc` networking.

## Project Conventions

- Service implementations go in `internal/service/`.
- Interceptors go in `internal/interceptor/`.
- Test files sit alongside their source (`foo.go` -> `foo_test.go`).
- Table-driven tests are preferred. Name format: `TestFunctionName_Scenario`.
