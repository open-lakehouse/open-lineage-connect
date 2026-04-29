# Stage 1: Export protos with all buf-managed dependencies resolved.
FROM bufbuild/buf:latest AS proto
WORKDIR /workspace
COPY buf.yaml buf.lock buf.gen.yaml ./
COPY proto/ proto/
RUN buf export --output=proto-export

# Stage 2: Build the Rust binary.
FROM rust:1-bookworm AS build
ARG CRATES_PROXY=
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY open-lakehouse-table-service/ .
COPY --from=proto /workspace/proto-export/ proto-export/
RUN if [ -n "${CRATES_PROXY}" ]; then \
      proxy_url="${CRATES_PROXY}"; \
      case "${proxy_url}" in \
        sparse+*) ;; \
        *) proxy_url="sparse+${proxy_url}" ;; \
      esac; \
      mkdir -p /usr/local/cargo; \
      printf '[source.crates-io]\nreplace-with = "proxy"\n\n[source.proxy]\nregistry = "%s"\n' "${proxy_url}" > /usr/local/cargo/config.toml; \
      echo "Using CRATES_PROXY for cargo registry: ${proxy_url}"; \
    fi
RUN cargo build --release

# Stage 3: Minimal runtime image.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/open-lakehouse-table-service /table-service
ENV TABLE_SERVICE_PORT=8091
EXPOSE 8091
ENTRYPOINT ["/table-service"]
