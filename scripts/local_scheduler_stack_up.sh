#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deployment"
ENV_FILE="${SCHEDULER_LOCAL_ENV:-${DEPLOY_DIR}/.env.local}"
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.local.yml"

if [ ! -f "${ENV_FILE}" ]; then
  cp "${DEPLOY_DIR}/.env.local.example" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.local.example"
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

SCHED_STORAGE_DIR="${SCHED_STORAGE_DIR:-/tmp/temporal-scheduler-local}"
BWB_SCHEDULER_IMAGE="${BWB_SCHEDULER_IMAGE:-bwb-scheduler:local}"
SCHEDULER_API_PORT="${SCHEDULER_API_PORT:-8000}"

mkdir -p "${SCHED_STORAGE_DIR}"

if [ "${SKIP_SCHEDULER_IMAGE_BUILD:-false}" != "true" ]; then
  docker build -t "${BWB_SCHEDULER_IMAGE}" -f "${DEPLOY_DIR}/Dockerfile" "${REPO_ROOT}"
fi

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d

python3 - <<PY
import time
import urllib.request

url = "http://localhost:${SCHEDULER_API_PORT}/openapi.json"
deadline = time.time() + int("${SCHEDULER_API_WAIT_SECONDS:-180}")
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            if response.status == 200:
                print("Scheduler API is ready.")
                break
    except Exception:
        time.sleep(2)
else:
    raise SystemExit("Scheduler API did not become ready in time.")
PY

python3 - <<'PY'
import subprocess
import time

cmd = [
    "docker",
    "exec",
    "temporal-scheduler-local-admin-tools",
    "tctl",
    "namespace",
    "describe",
    "default",
]
deadline = time.time() + 180
while time.time() < deadline:
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode == 0:
        print("Temporal default namespace is ready.")
        break
    time.sleep(2)
else:
    raise SystemExit("Temporal default namespace did not become ready in time.")
PY

echo "Local scheduler stack is up."
echo "Scheduler API: http://localhost:${SCHEDULER_API_PORT}"
echo "Temporal UI:  http://localhost:${TEMPORAL_UI_PORT:-8080}"
