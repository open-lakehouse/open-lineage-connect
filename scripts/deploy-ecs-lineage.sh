#!/usr/bin/env bash
# Deploy the open-lineage stack to ECS Fargate via terraform/ecs-lineage.
#
# Flow:
#   1. Source repo-root .env and export TF_VAR_* for Terraform.
#   2. Build + push both images to ECR (skip with DEPLOY_SKIP_BUILD=1).
#   3. terraform init + apply.
#   4. Wait for the service to stabilize and print outputs.
#
# Usage: scripts/deploy-ecs-lineage.sh [lineage_tag] [table_tag]
#   tags default to $LINEAGE_IMAGE_TAG / $TABLE_IMAGE_TAG, then v0.1.0.
#
# Knobs:
#   DEPLOY_AUTO_APPROVE=1   skip the interactive terraform apply confirmation
#   DEPLOY_SKIP_BUILD=1     reuse already-pushed images
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
TF_DIR="terraform/ecs-lineage"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

LINEAGE_TAG="${1:-${LINEAGE_IMAGE_TAG:-v0.1.0}}"
TABLE_TAG="${2:-${TABLE_IMAGE_TAG:-v0.1.0}}"
REGION="${AWS_REGION:-us-west-2}"

# ----- Map .env -> TF_VAR_* --------------------------------------------------
export TF_VAR_aws_region="$REGION"
export TF_VAR_lineage_image_tag="$LINEAGE_TAG"
export TF_VAR_table_image_tag="$TABLE_TAG"
[[ -n "${OPEN_LINEAGE_NAME_PREFIX:-}" ]] && export TF_VAR_name_prefix="$OPEN_LINEAGE_NAME_PREFIX"
[[ -n "${LINEAGE_ECR_REPO:-}" ]] && export TF_VAR_lineage_ecr_repo_name="$LINEAGE_ECR_REPO"
[[ -n "${TABLE_ECR_REPO:-}" ]] && export TF_VAR_table_ecr_repo_name="$TABLE_ECR_REPO"
[[ -n "${DELTA_STORAGE:-}" ]] && export TF_VAR_delta_storage="$DELTA_STORAGE"
[[ -n "${DELTA_BUCKET:-}" ]] && export TF_VAR_delta_bucket="$DELTA_BUCKET"
[[ -n "${DELTA_TABLE_PATH:-}" ]] && export TF_VAR_delta_table_path="$DELTA_TABLE_PATH"
[[ -n "${UNITY_CATALOG_URL:-}" ]] && export TF_VAR_unity_catalog_url="$UNITY_CATALOG_URL"
[[ -n "${UC_CATALOG:-}" ]] && export TF_VAR_uc_catalog="$UC_CATALOG"
[[ -n "${UC_SCHEMA:-}" ]] && export TF_VAR_uc_schema="$UC_SCHEMA"
[[ -n "${UC_TABLE:-}" ]] && export TF_VAR_uc_table="$UC_TABLE"
[[ -n "${LINEAGE_AUTH_MODE:-}" ]] && export TF_VAR_lineage_auth_mode="$LINEAGE_AUTH_MODE"
[[ -n "${LINEAGE_DOMAIN_NAME:-}" ]] && export TF_VAR_domain_name="$LINEAGE_DOMAIN_NAME"
[[ -n "${LINEAGE_HOSTED_ZONE_ID:-}" ]] && export TF_VAR_hosted_zone_id="$LINEAGE_HOSTED_ZONE_ID"
[[ -n "${LINEAGE_CERT_DOMAIN_NAME:-}" ]] && export TF_VAR_cert_domain_name="$LINEAGE_CERT_DOMAIN_NAME"
# Sensitive values flow via TF_VAR_* only (never CLI args).
export TF_VAR_unity_catalog_token="${UNITY_CATALOG_TOKEN:-}"
export TF_VAR_lineage_auth_token="${LINEAGE_AUTH_TOKEN:-}"

# ----- Build + push images ---------------------------------------------------
if [[ "${DEPLOY_SKIP_BUILD:-0}" != "1" ]]; then
  bash "$ROOT/scripts/ecr-push-lineage.sh" "$LINEAGE_TAG"
  bash "$ROOT/scripts/ecr-push-table.sh" "$TABLE_TAG"
else
  echo "==> DEPLOY_SKIP_BUILD=1; reusing already-pushed images"
fi

# ----- Terraform -------------------------------------------------------------
echo "==> terraform init"
terraform -chdir="$TF_DIR" init -input=false

APPLY_ARGS=(-input=false)
[[ "${DEPLOY_AUTO_APPROVE:-0}" == "1" ]] && APPLY_ARGS+=(-auto-approve)

echo "==> terraform apply"
terraform -chdir="$TF_DIR" apply "${APPLY_ARGS[@]}"

# ----- Wait for the service to stabilize -------------------------------------
CLUSTER="$(terraform -chdir="$TF_DIR" output -raw cluster_name)"
SERVICE="$(terraform -chdir="$TF_DIR" output -raw service_name)"
echo "==> Waiting for ECS service ${SERVICE} to stabilize"
aws ecs wait services-stable --cluster "$CLUSTER" --services "$SERVICE" --region "$REGION"

echo "==> Done. Outputs:"
terraform -chdir="$TF_DIR" output
