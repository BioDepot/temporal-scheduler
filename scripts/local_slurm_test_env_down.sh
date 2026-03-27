#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deployment"
CLUSTER_DIR="${SLURM_DOCKER_CLUSTER_DIR:-/mnt/pikachu/slurm-docker-cluster}"

docker rm -f "${LOCAL_SLURM_WORKER_CONTAINER_NAME:-local-slurm-worker}" >/dev/null 2>&1 || true
docker compose --env-file "${DEPLOY_DIR}/.env" -f "${DEPLOY_DIR}/docker-compose.yml" down

if [ -d "${CLUSTER_DIR}" ]; then
  make -C "${CLUSTER_DIR}" down
fi
