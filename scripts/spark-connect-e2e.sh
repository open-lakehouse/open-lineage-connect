#!/usr/bin/env bash
#
# Spark Connect end-to-end test for the OpenLineage plugin.
#
# This is the realization of the previously-deferred `spark-connect-compat`
# follow-up: instead of the in-JVM smoke test (classic mode), it boots a *real*
# Spark Connect server with the plugin on its classpath and drives it from a
# remote PySpark session over `sc://`. It asserts that the driver inside the
# Connect server emits OpenLineage RunEvents to the configured service URL.
#
# To stay self-contained (no full lineage stack required) it points the plugin
# at a tiny local HTTP capture server that records every POST. A non-empty
# capture proves the plugin's transport fired from within the Connect server.
#
# Requirements: bash, java 17, python3, sbt (for the plugin assembly), and
# either $SPARK_HOME pointing at a Spark 4.1.1 distribution or network access
# to download one.
#
# Usage:
#   scripts/spark-connect-e2e.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_VERSION="${SPARK_VERSION:-4.1.1}"
SPARK_HADOOP="${SPARK_HADOOP:-3}"
CONNECT_PORT="${CONNECT_PORT:-15002}"
CAPTURE_PORT="${CAPTURE_PORT:-18099}"
WORK_DIR="$(mktemp -d)"
CAPTURE_FILE="${WORK_DIR}/captured.log"

CAPTURE_PID=""
CONNECT_STARTED=0

log() { printf '\n=== %s ===\n' "$*"; }

cleanup() {
  log "Cleaning up"
  if [[ "${CONNECT_STARTED}" == "1" && -n "${SPARK_HOME:-}" ]]; then
    "${SPARK_HOME}/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
  fi
  if [[ -n "${CAPTURE_PID}" ]]; then
    kill "${CAPTURE_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK_DIR}" || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Build the plugin assembly jar.
# ---------------------------------------------------------------------------
log "Building plugin assembly"
make -C "${REPO_ROOT}" spark-plugin-build
PLUGIN_JAR="$(find "${REPO_ROOT}/spark-openlineage-plugin/target" -name 'spark-openlineage-plugin-assembly-*.jar' | head -n1)"
if [[ -z "${PLUGIN_JAR}" ]]; then
  echo "ERROR: could not locate the assembled plugin jar" >&2
  exit 1
fi
echo "Plugin jar: ${PLUGIN_JAR}"

# ---------------------------------------------------------------------------
# 2. Ensure a Spark distribution with the Connect server scripts.
# ---------------------------------------------------------------------------
if [[ -z "${SPARK_HOME:-}" ]]; then
  SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP}"
  log "Downloading ${SPARK_TGZ}"
  curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}.tgz" \
    -o "${WORK_DIR}/spark.tgz"
  tar -xzf "${WORK_DIR}/spark.tgz" -C "${WORK_DIR}"
  export SPARK_HOME="${WORK_DIR}/${SPARK_TGZ}"
fi
echo "SPARK_HOME=${SPARK_HOME}"

# ---------------------------------------------------------------------------
# 3. Start a tiny HTTP capture server standing in for open-lineage-service.
#    It records every request body and returns an empty 200 so the plugin's
#    transport treats the POST as delivered.
# ---------------------------------------------------------------------------
log "Starting capture server on :${CAPTURE_PORT}"
CAPTURE_FILE="${CAPTURE_FILE}" python3 - "${CAPTURE_PORT}" <<'PY' &
import http.server, os, sys
port = int(sys.argv[1])
path = os.environ["CAPTURE_FILE"]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        _ = self.rfile.read(n)
        with open(path, "a") as f:
            f.write(self.path + "\n")
        self.send_response(200)
        self.send_header("content-type", "application/proto")
        self.end_headers()
        self.wfile.write(b"")
    def log_message(self, *a):  # quiet
        pass
http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
PY
CAPTURE_PID=$!
sleep 2

# ---------------------------------------------------------------------------
# 4. Start the Spark Connect server with the plugin registered.
# ---------------------------------------------------------------------------
log "Starting Spark Connect server on :${CONNECT_PORT}"
"${SPARK_HOME}/sbin/start-connect-server.sh" \
  --jars "${PLUGIN_JAR}" \
  --packages "org.apache.spark:spark-connect_2.13:${SPARK_VERSION}" \
  --conf "spark.connect.grpc.binding.port=${CONNECT_PORT}" \
  --conf "spark.plugins=com.openlakehouse.lineage.LineagePlugin" \
  --conf "spark.openlineage.serviceUrl=http://127.0.0.1:${CAPTURE_PORT}" \
  --conf "spark.openlineage.namespace=connect-e2e" \
  --conf "spark.openlineage.batchFlushMs=100"
CONNECT_STARTED=1
sleep 15

# ---------------------------------------------------------------------------
# 5. Drive the server from a remote session and run a query that produces
#    lineage (read + write).
# ---------------------------------------------------------------------------
log "Running remote query via sc://localhost:${CONNECT_PORT}"
OUT_DIR="${WORK_DIR}/out" CONNECT_PORT="${CONNECT_PORT}" python3 - <<'PY'
import os
from pyspark.sql import SparkSession

port = os.environ["CONNECT_PORT"]
out = os.environ["OUT_DIR"]
spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
df = spark.range(100).selectExpr("id", "id * 2 as doubled")
df.write.mode("overwrite").parquet(out)
print("rows written:", spark.read.parquet(out).count())
spark.stop()
PY

# Give the async emitter time to flush from the server JVM.
sleep 5

# ---------------------------------------------------------------------------
# 6. Verify the plugin emitted at least one event from inside the Connect
#    server.
# ---------------------------------------------------------------------------
log "Verifying captured lineage events"
if [[ -s "${CAPTURE_FILE}" ]] && grep -q "/lineage.v1.LineageService/" "${CAPTURE_FILE}"; then
  echo "PASS: Spark Connect server emitted lineage events:"
  sort "${CAPTURE_FILE}" | uniq -c
  exit 0
fi

echo "FAIL: no lineage events captured from the Connect server" >&2
echo "--- connect server logs (tail) ---" >&2
tail -n 100 "${SPARK_HOME}"/logs/*connect*.out 2>/dev/null >&2 || true
exit 1
