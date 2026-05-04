#!/bin/sh
# Bootstrap Lakekeeper for the open-lineage-connect demo stack.
#
# Idempotently:
#   1. Calls `/management/v1/bootstrap` (no-op if already bootstrapped).
#   2. Creates a warehouse named $WAREHOUSE_NAME pointing at the MinIO
#      bucket $MINIO_BUCKET under prefix $MINIO_KEY_PREFIX, using the
#      MinIO root credentials passed in via env.
#
# This script is invoked by the `lakekeeper-bootstrap` service in
# docker-compose.iceberg.yaml. It uses `curl` (no jq dependency) to keep
# the bootstrap container small.
set -eu

LAKEKEEPER_URL="${LAKEKEEPER_URL:-http://lakekeeper:8181}"
WAREHOUSE_NAME="${WAREHOUSE_NAME:-lineage}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-lineage-events}"
MINIO_KEY_PREFIX="${MINIO_KEY_PREFIX:-iceberg}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "lakekeeper-bootstrap: target=$LAKEKEEPER_URL warehouse=$WAREHOUSE_NAME"

# Wait until the management API answers — the `serve` boot races the
# healthcheck on slow CI runners.
for i in $(seq 1 30); do
  if curl -sf "$LAKEKEEPER_URL/management/v1/info" >/dev/null 2>&1; then
    break
  fi
  echo "lakekeeper-bootstrap: waiting for $LAKEKEEPER_URL ($i/30)"
  sleep 2
done

# Step 1: bootstrap. Returns 204 first time, 409/400 if already bootstrapped.
status=$(curl -s -o /tmp/bootstrap.out -w '%{http_code}' \
  -X POST "$LAKEKEEPER_URL/management/v1/bootstrap" \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}')
case "$status" in
  20[0-9])
    echo "lakekeeper-bootstrap: bootstrap OK ($status)"
    ;;
  400|409)
    echo "lakekeeper-bootstrap: already bootstrapped ($status), continuing"
    ;;
  *)
    echo "lakekeeper-bootstrap: bootstrap FAILED ($status)"
    cat /tmp/bootstrap.out || true
    exit 1
    ;;
esac

# Step 2: ensure the warehouse exists. List warehouses, skip create if found.
list_status=$(curl -s -o /tmp/wh-list.out -w '%{http_code}' \
  "$LAKEKEEPER_URL/management/v1/warehouse")
if [ "$list_status" = "200" ] && grep -q "\"name\":\"$WAREHOUSE_NAME\"" /tmp/wh-list.out; then
  echo "lakekeeper-bootstrap: warehouse '$WAREHOUSE_NAME' already present"
  exit 0
fi

cat >/tmp/wh-create.json <<EOF
{
  "warehouse-name": "$WAREHOUSE_NAME",
  "storage-profile": {
    "type": "s3",
    "bucket": "$MINIO_BUCKET",
    "key-prefix": "$MINIO_KEY_PREFIX",
    "endpoint": "$MINIO_ENDPOINT",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "minio",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "$MINIO_ROOT_USER",
    "aws-secret-access-key": "$MINIO_ROOT_PASSWORD"
  }
}
EOF

create_status=$(curl -s -o /tmp/wh-create.out -w '%{http_code}' \
  -X POST "$LAKEKEEPER_URL/management/v1/warehouse" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/wh-create.json)
case "$create_status" in
  20[0-9])
    echo "lakekeeper-bootstrap: warehouse '$WAREHOUSE_NAME' created"
    ;;
  409)
    echo "lakekeeper-bootstrap: warehouse '$WAREHOUSE_NAME' already exists"
    ;;
  *)
    echo "lakekeeper-bootstrap: warehouse create FAILED ($create_status)"
    cat /tmp/wh-create.out || true
    exit 1
    ;;
esac

echo "lakekeeper-bootstrap: done"
