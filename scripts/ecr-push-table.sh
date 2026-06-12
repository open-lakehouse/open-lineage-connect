#!/usr/bin/env bash
# Build and push the Rust table-service image to ECR (linux/arm64, to match the
# Fargate ARM64 task definition). The build context is the repo root because
# the Dockerfile needs buf.yaml, proto/, and Cargo.lock. Creates the repository
# if it does not exist.
#
# Usage: scripts/ecr-push-table.sh [tag]
#   tag defaults to $TABLE_IMAGE_TAG, then v0.1.0.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TAG="${1:-${TABLE_IMAGE_TAG:-v0.1.0}}"
REGION="${AWS_REGION:-us-west-2}"
REPO="${TABLE_ECR_REPO:-table-service}"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO}"

echo "==> Ensuring ECR repo ${REPO} exists in ${REGION}"
aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 ||
  aws ecr create-repository --repository-name "$REPO" --region "$REGION" >/dev/null

echo "==> Logging in to ${REGISTRY}"
aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

# Forward an optional Cargo registry proxy used behind corporate firewalls.
CRATES_PROXY_ARG=()
if [[ -n "${CRATES_PROXY:-}" ]]; then
  CRATES_PROXY_ARG=(--build-arg "CRATES_PROXY=${CRATES_PROXY}")
fi

echo "==> Building + pushing ${IMAGE}:${TAG} (and :latest) [linux/arm64]"
docker buildx build \
  --platform linux/arm64 \
  --build-arg "SERVICE_VERSION=${TAG}" \
  "${CRATES_PROXY_ARG[@]}" \
  -f crates/table-service/Dockerfile \
  -t "${IMAGE}:${TAG}" \
  -t "${IMAGE}:latest" \
  --push \
  .

echo "==> Pushed ${IMAGE}:${TAG}"
