#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deployment"
ENV_FILE="${SCHEDULER_LOCAL_ENV:-${DEPLOY_DIR}/.env.local}"

if [ ! -f "${ENV_FILE}" ]; then
  ENV_FILE="${DEPLOY_DIR}/.env.local.example"
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

SCHED_STORAGE_DIR="${SCHED_STORAGE_DIR:-/tmp/temporal-scheduler-local}"
BWB_SCHEDULER_IMAGE="${BWB_SCHEDULER_IMAGE:-bwb-scheduler:local}"
SSH_DOCKER_PORT="${SSH_DOCKER_PORT:-22}"
SSH_DOCKER_SSH_HOME="${SSH_DOCKER_SSH_HOME:-${HOME}/.ssh}"
SSH_DOCKER_WORKER_NAME="${SSH_DOCKER_WORKER_NAME:-temporal-scheduler-local-ssh-docker-worker}"
SSH_DOCKER_NETWORK="${SSH_DOCKER_NETWORK:-temporal-scheduler-local_temporal-scheduler-local}"

required=(
  SSH_DOCKER_HOST
  SSH_DOCKER_USER
  SSH_DOCKER_STORAGE_DIR
)

for key in "${required[@]}"; do
  if [ -z "${!key:-}" ]; then
    echo "Missing required environment variable ${key}." >&2
    exit 2
  fi
done

mkdir -p "${SCHED_STORAGE_DIR}"

CONFIG_PATH="${SCHED_STORAGE_DIR}/ssh-docker-worker-config.json"
python3 - <<PY
import json
from pathlib import Path

payload = {
    "executors": {
        "ssh_docker": {
            "ip_addr": "${SSH_DOCKER_HOST}",
            "user": "${SSH_DOCKER_USER}",
            "storage_dir": "${SSH_DOCKER_STORAGE_DIR}",
            "port": int("${SSH_DOCKER_PORT}"),
            "gpu_device": "${SSH_DOCKER_GPU_DEVICE:-}",
        }
    }
}
path = Path("${CONFIG_PATH}")
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
print(path)
PY

docker rm -f "${SSH_DOCKER_WORKER_NAME}" >/dev/null 2>&1 || true

docker run -d \
  --name "${SSH_DOCKER_WORKER_NAME}" \
  --restart unless-stopped \
  --network "${SSH_DOCKER_NETWORK}" \
  -e "MINIO_ENDPOINT_URL=${MINIO_ENDPOINT_URL}" \
  -e "MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}" \
  -e "MINIO_SECRET_KEY=${MINIO_SECRET_KEY}" \
  -e "SCHED_STORAGE_DIR=${SCHED_STORAGE_DIR}" \
  -e "TEMPORAL_ENDPOINT_URL=temporal:7233" \
  -v "${SCHED_STORAGE_DIR}:${SCHED_STORAGE_DIR}" \
  -v "${CONFIG_PATH}:/tmp/ssh-docker-config.json:ro" \
  -v "${SSH_DOCKER_SSH_HOME}:/root/.ssh:rw" \
  "${BWB_SCHEDULER_IMAGE}" \
  bash -c "sleep 5 && poetry run python bwb/scheduling_service/worker.py ssh-docker --config /tmp/ssh-docker-config.json"

echo "SSH Docker scheduler worker is starting."
echo "Queue: ${SSH_DOCKER_USER}@${SSH_DOCKER_HOST}:${SSH_DOCKER_PORT}"
echo "Container: ${SSH_DOCKER_WORKER_NAME}"
