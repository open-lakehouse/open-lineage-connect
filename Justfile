set shell := ["bash", "-euo", "pipefail", "-c"]

spark_namespace := "demo-local"
delta_spark_package := "io.delta:delta-spark_2.13:4.2.0"

default:
  @just --list

run-e2e-demo output="deltalake":
  @selected_output="{{output}}"; \
  selected_output="${selected_output#output=}"; \
  case "$selected_output" in \
    deltalake) just run-e2e-demo-deltalake ;; \
    iceberg) just run-e2e-demo-iceberg ;; \
    *) \
      echo "Unsupported output '$selected_output'."; \
      echo "Valid options: deltalake, iceberg"; \
      exit 1 ;; \
  esac

run-e2e-demo-deltalake:
  @results_root="demo/results/deltalake"; \
  rm -rf "$results_root"; \
  mkdir -p "$results_root/spark-output" "$results_root/lineage-events"; \
  trap 'just stack-down' EXIT; \
  just stack-up; \
  just wait-healthy; \
  just spark-job "$results_root/spark-output"; \
  sleep 3; \
  just collect-results "$results_root/lineage-events"; \
  echo "E2E demo complete. Results written to $results_root"

run-e2e-demo-iceberg:
  @results_root="demo/results/iceberg"; \
  rm -rf "$results_root"; \
  mkdir -p "$results_root/spark-output" "$results_root/lineage-events"; \
  trap 'just stack-down-iceberg' EXIT; \
  just stack-up-iceberg; \
  just wait-healthy-iceberg; \
  just spark-job "$results_root/spark-output"; \
  echo "Waiting 10s for the async forwarder to flush events to the Iceberg sink..."; \
  sleep 10; \
  just collect-results-iceberg "$results_root/lineage-events"; \
  just verify-iceberg-results "$results_root/lineage-events"; \
  echo "E2E iceberg demo complete. Results written to $results_root"

stack-up:
  @docker compose up --build -d

stack-down:
  @docker compose down -v

# Bring the stack up with the Iceberg / Lakekeeper override applied. The
# table-service will fan events out to BOTH delta and iceberg sinks.
stack-up-iceberg:
  @docker compose -f docker-compose.yaml -f docker-compose.iceberg.yaml up --build -d

stack-down-iceberg:
  @docker compose -f docker-compose.yaml -f docker-compose.iceberg.yaml down -v

# Wipe the Rust debug build artifacts (workspace-root `target/debug`) without
# touching the rest of `target/`. The debug profile is what `cargo test` and
# unqualified `cargo build` produce; on this codebase it grows to ~9 GB because
# of the deltalake + iceberg + datafusion dep tree. Cargo will rebuild
# incrementally on the next invocation. (The Cargo workspace puts the shared
# target dir at the repo root, not under the crate.)
cargo-clean:
  @before=$(du -sh target 2>/dev/null | awk '{print $1}'); \
  rm -rf target/debug \
         target/tmp; \
  after=$(du -sh target 2>/dev/null | awk '{print $1}'); \
  echo "Reclaimed: target/debug (was $before -> now $after)"

# Recover a Docker daemon that's gotten into a bad state — usually after a
# disk-pressure / I/O-error event in BuildKit's containerd store. Symptom:
# `failed to solve: write /var/lib/containerd/.../meta.db: input/output error`.
# Wipes build/image/container/volume cache (forces re-pull on next run) but
# does NOT touch the host's `target/` or other workspace files.
docker-recover:
  @echo "Stopping any compose stacks…"; \
  docker compose -f docker-compose.yaml -f docker-compose.iceberg.yaml down -v --remove-orphans 2>/dev/null || true; \
  docker compose -f docker-compose.yaml down -v --remove-orphans 2>/dev/null || true; \
  echo "Pruning builder cache…"; \
  docker builder prune -af 2>/dev/null || true; \
  echo "Pruning system (images / volumes / networks / build cache)…"; \
  docker system prune -af --volumes 2>/dev/null || true; \
  echo ""; \
  echo "Docker recovery done. If you still see I/O errors:"; \
  echo "  • Restart Docker Desktop or Colima."; \
  echo "  • Free disk space on the host (df -h /)."; \
  echo "  • If on Colima: 'colima stop && colima delete && colima start'"; \
  echo ""; \
  df -h / | head -2

wait-healthy:
  @for i in $(seq 1 60); do \
    if curl -sf "http://localhost:8090/health" >/dev/null 2>&1 && \
       curl -sf "http://localhost:8091/health" >/dev/null 2>&1; then \
      echo "Services are healthy."; \
      exit 0; \
    fi; \
    if [ "$i" -eq 60 ]; then \
      echo "Services did not become healthy in time."; \
      exit 1; \
    fi; \
    sleep 1; \
  done

# Wait until lineage-service, table-service, AND lakekeeper are all healthy.
# Lakekeeper bootstrap runs as a one-shot — by the time `lineage-service`
# starts (depends_on: bootstrap completed), the warehouse already exists.
wait-healthy-iceberg:
  @for i in $(seq 1 90); do \
    if curl -sf "http://localhost:8090/health" >/dev/null 2>&1 && \
       curl -sf "http://localhost:8091/health" >/dev/null 2>&1 && \
       curl -sf "http://localhost:8181/health" >/dev/null 2>&1; then \
      echo "Services are healthy (lineage + table + lakekeeper)."; \
      exit 0; \
    fi; \
    if [ "$i" -eq 90 ]; then \
      echo "Services did not become healthy in time."; \
      docker compose -f docker-compose.yaml -f docker-compose.iceberg.yaml ps; \
      exit 1; \
    fi; \
    sleep 2; \
  done

# Pull every parquet / metadata.json / manifest avro under the `iceberg/`
# prefix out of MinIO and onto the host. We can't `docker compose cp` from
# the minio server directly because MinIO stores each object as a directory
# wrapped in an `xl.meta` envelope — only an S3 client (`mc`) reproduces the
# logical filenames. Instead of carrying a separate `mc` image, we re-run
# the existing `minio-setup` one-shot with an extra bind-mount to ferry
# files out via `mc mirror`.
collect-results-iceberg lineage_iceberg_dir:
  @mkdir -p "{{lineage_iceberg_dir}}"; \
  out_abs="$(cd "{{lineage_iceberg_dir}}" && pwd)"; \
  docker compose -f docker-compose.yaml -f docker-compose.iceberg.yaml run --rm \
    --no-deps -T -v "$out_abs:/out" --entrypoint sh minio-setup -c "\
      mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && \
      listing=\$(mc ls --recursive local/lineage-events/iceberg/ 2>/dev/null || true); \
      if [ -n \"\$listing\" ]; then \
        mc mirror --overwrite --quiet local/lineage-events/iceberg/ /out/ >/dev/null && \
        echo 'Copied iceberg files to {{lineage_iceberg_dir}}'; \
      else \
        echo 'No iceberg/ objects found in MinIO (table may be empty).'; \
      fi"

# Smoke-test the e2e iceberg path. Asserts that at least one Iceberg
# `metadata.json` and one Parquet data file were materialized under the
# given directory. Fails loudly with a non-zero exit code if either is
# missing — the e2e demo target chains this so a silent regression in the
# Iceberg sink can never look like a passing run.
verify-iceberg-results lineage_iceberg_dir:
  @echo "Verifying iceberg results in {{lineage_iceberg_dir}}..."; \
  if [ ! -d "{{lineage_iceberg_dir}}" ]; then \
    echo "FAIL: {{lineage_iceberg_dir}} does not exist"; \
    exit 1; \
  fi; \
  metadata_count=$(find "{{lineage_iceberg_dir}}" -type f -name '*.metadata.json' | wc -l | tr -d ' '); \
  parquet_count=$(find "{{lineage_iceberg_dir}}" -type f -name '*.parquet' | wc -l | tr -d ' '); \
  manifest_count=$(find "{{lineage_iceberg_dir}}" -type f -name '*.avro' | wc -l | tr -d ' '); \
  if [ "$metadata_count" -lt 1 ]; then \
    echo "FAIL: no Iceberg *.metadata.json files found under {{lineage_iceberg_dir}}"; \
    find "{{lineage_iceberg_dir}}" -maxdepth 6 -type f | head -50; \
    exit 1; \
  fi; \
  if [ "$parquet_count" -lt 1 ]; then \
    echo "FAIL: no *.parquet data files found under {{lineage_iceberg_dir}}"; \
    find "{{lineage_iceberg_dir}}" -maxdepth 6 -type f | head -50; \
    exit 1; \
  fi; \
  echo "OK: iceberg has $metadata_count metadata.json file(s), $manifest_count manifest avro file(s), and $parquet_count parquet data file(s)"; \
  echo ""; \
  echo "Layout:"; \
  find "{{lineage_iceberg_dir}}" -maxdepth 6 -type f | sort | sed 's|^|  |'

# Run the cargo iceberg integration test against the live Lakekeeper stack
# brought up by `stack-up-iceberg`. The test is gated on LAKEKEEPER_URL —
# without that env var it is skipped automatically.
test-iceberg-integration:
  @LAKEKEEPER_URL=http://localhost:8181/catalog \
    AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    AWS_ENDPOINT_URL=http://localhost:9000 \
      cargo test -p table-service --test iceberg_integration -- --nocapture --ignored

spark-job spark_output_dir:
  @if [ -f .env ]; then \
    set -a; \
    . ./.env; \
    set +a; \
  fi; \
  spark_submit_bin="spark-submit"; \
  if [ -n "${SPARK_HOME:-}" ]; then \
    spark_submit_bin="${SPARK_HOME}/bin/spark-submit"; \
  fi; \
  if [ ! -x "$spark_submit_bin" ] && ! command -v "$spark_submit_bin" >/dev/null 2>&1; then \
    echo "spark-submit was not found. Set SPARK_HOME or add spark-submit to PATH."; \
    exit 1; \
  fi; \
  repositories_arg=(); \
  if [ -n "${SPARK_MAVEN_REPOSITORIES:-}" ]; then \
    repositories_arg=(--repositories "${SPARK_MAVEN_REPOSITORIES}"); \
  fi; \
  make spark-plugin-build; \
  plugin_jar=$(ls -1 spark-openlineage-plugin/target/scala-2.13/spark-openlineage-plugin-assembly-*.jar | sort | tail -n 1); \
  orders_path="$(pwd)/demos/datasets/delta/orders"; \
  users_path="$(pwd)/demos/datasets/delta/users"; \
  user_orders_path="$(pwd)/{{spark_output_dir}}/user_orders"; \
  openlineage_auth_token="${OPENLINEAGE_AUTH_TOKEN:-valid-token}"; \
  rm -rf "$user_orders_path"; \
  DEMO_ORDERS_PATH="$orders_path" \
  DEMO_USERS_PATH="$users_path" \
  DEMO_USER_ORDERS_PATH="$user_orders_path" \
  "$spark_submit_bin" \
    "${repositories_arg[@]}" \
    --packages "{{delta_spark_package}}" \
    --jars "$plugin_jar" \
    --conf spark.plugins=com.openlakehouse.lineage.LineagePlugin \
    --conf spark.openlineage.serviceUrl=http://localhost:8090 \
    --conf spark.openlineage.namespace="{{spark_namespace}}" \
    --conf spark.openlineage.authToken="$openlineage_auth_token" \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    demos/jobs/user_orders_join.py

collect-results lineage_output_dir:
  @mkdir -p "{{lineage_output_dir}}"; \
  if docker compose exec -T table-service test -d /data/events; then \
    docker compose cp table-service:/data/events/. "{{lineage_output_dir}}"; \
  else \
    echo "Expected lineage table path /data/events was not found in table-service."; \
    docker compose exec -T table-service ls -la /data || true; \
    exit 1; \
  fi; \
  echo "Copied lineage table files to {{lineage_output_dir}}"
