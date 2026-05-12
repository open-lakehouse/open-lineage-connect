#!/usr/bin/env bash
UC_SERVER="${UC_SERVER:-http://localhost:9000}"
CATALOG="${CATALOG:-demo}"
SCHEMA="${SCHEMA:-results}"
DELTA_BASE="${DELTA_BASE:-file:///delta}"

json_header=(-H "Content-Type: application/json")

# Create catalog
curl -sS -X POST "${UC_SERVER}/api/2.1/unity-catalog/catalogs" "${json_header[@]}" \
  -d "{\"name\":\"${CATALOG}\"}"

# Create schema
curl -sS -X POST "${UC_SERVER}/api/2.1/unity-catalog/schemas" "${json_header[@]}" \
  -d "{\"name\":\"${SCHEMA}\",\"catalog_name\":\"${CATALOG}\"}"

# Register processed graph tables
for t in jobs job_inputs job_outputs; do
  curl -sS -X POST "${UC_SERVER}/api/2.1/unity-catalog/tables" "${json_header[@]}" \
    -d "{\"name\":\"${t}\",\"catalog_name\":\"${CATALOG}\",\"schema_name\":\"${SCHEMA}\",\"table_type\":\"EXTERNAL\",\"data_source_format\":\"DELTA\",\"storage_location\":\"${DELTA_BASE}/flattened_lineage_tables/${t}\"}"
done