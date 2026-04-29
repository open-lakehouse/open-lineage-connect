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
  @echo "output=iceberg is not implemented yet. Follow-up PR will add Iceberg REST Catalog support."
  @exit 1

stack-up:
  @docker compose up --build -d

stack-down:
  @docker compose down -v

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
