#!/usr/bin/env bash
# Build and push the Go lineage-service image to ECR (linux/arm64, to match the
# Fargate ARM64 task definition). Creates the repository if it does not exist.
#
# Usage: scripts/ecr-push-lineage.sh [tag]
#   tag defaults to $LINEAGE_IMAGE_TAG, then v0.1.0.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Load .env so AWS_REGION / repo names / tags are available.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TAG="${1:-${LINEAGE_IMAGE_TAG:-v0.1.0}}"
REGION="${AWS_REGION:-us-west-2}"
REPO="${LINEAGE_ECR_REPO:-lineage-service}"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO}"

echo "==> Ensuring ECR repo ${REPO} exists in ${REGION}"
aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 ||
  aws ecr create-repository --repository-name "$REPO" --region "$REGION" >/dev/null

echo "==> Logging in to ${REGISTRY}"
aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

echo "==> Building + pushing ${IMAGE}:${TAG} (and :latest) [linux/arm64]"
docker buildx build \
  --platform linux/arm64 \
  --build-arg "VERSION=${TAG}" \
  -t "${IMAGE}:${TAG}" \
  -t "${IMAGE}:latest" \
  --push \
  services/lineage

echo "==> Pushed ${IMAGE}:${TAG}"
