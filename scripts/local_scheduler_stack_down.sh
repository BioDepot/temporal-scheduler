#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deployment"
ENV_FILE="${SCHEDULER_LOCAL_ENV:-${DEPLOY_DIR}/.env.local}"
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.local.yml"

if [ ! -f "${ENV_FILE}" ]; then
  ENV_FILE="${DEPLOY_DIR}/.env.local.example"
fi

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" down
