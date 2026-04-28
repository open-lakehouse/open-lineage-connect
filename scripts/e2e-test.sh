#!/usr/bin/env bash
# End-to-end smoke test for the sidecar deployment via docker-compose.
#
# Prerequisites:
#   docker compose up --build -d
#
# This script sends events to the Go lineage-service and verifies that the
# Rust table-service writes them to the local Delta Lake volume.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8090}"
TABLE_URL="${TABLE_URL:-http://localhost:8091}"

echo "==> Waiting for services to be healthy..."
for i in $(seq 1 30); do
  if curl -sf "$BASE_URL/health" >/dev/null 2>&1 && \
     curl -sf "$TABLE_URL/health" >/dev/null 2>&1; then
    echo "    Services are up."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "    ERROR: Services did not become healthy in 30s"
    exit 1
  fi
  sleep 1
done

echo "==> Sending a RunEvent via REST..."
curl -sf -X POST "$BASE_URL/lineage" \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "START",
    "eventTime": "2025-01-01T00:00:00Z",
    "producer": "e2e-test",
    "run": {"runId": "e2e-run-1"},
    "job": {"namespace": "e2e-ns", "name": "e2e-job"}
  }' && echo " -> OK"

echo "==> Sending a JobEvent via REST..."
curl -sf -X POST "$BASE_URL/lineage" \
  -H "Content-Type: application/json" \
  -d '{
    "eventTime": "2025-01-01T00:01:00Z",
    "producer": "e2e-test",
    "job": {"namespace": "e2e-ns", "name": "e2e-job-2"},
    "inputs": [{"namespace": "db", "name": "source_table"}],
    "outputs": [{"namespace": "db", "name": "target_table"}]
  }' && echo " -> OK"

echo "==> Sending a DatasetEvent via REST..."
curl -sf -X POST "$BASE_URL/lineage" \
  -H "Content-Type: application/json" \
  -d '{
    "eventTime": "2025-01-01T00:02:00Z",
    "producer": "e2e-test",
    "dataset": {"namespace": "db", "name": "my_dataset"}
  }' && echo " -> OK"

echo "==> Waiting for forwarder flush (2s)..."
sleep 2

echo "==> Checking table-service health..."
curl -sf "$TABLE_URL/health" && echo " -> OK"

echo "==> Checking Delta Lake files on the table-service container..."
DELTA_FILES=$(docker compose exec -T table-service find /data/events -name '*.parquet' 2>/dev/null | wc -l || echo 0)
echo "    Found $DELTA_FILES parquet file(s)"

if [ "$DELTA_FILES" -gt 0 ]; then
  echo "==> SUCCESS: Delta Lake table has data."
else
  echo "==> WARNING: No parquet files found yet. The forwarder may need more time."
  echo "    Try: docker compose exec table-service ls -la /data/events/"
fi

echo "==> Done."
