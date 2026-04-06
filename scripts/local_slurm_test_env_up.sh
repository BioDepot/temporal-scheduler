#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deployment"

if [ ! -f "${DEPLOY_DIR}/.env" ]; then
  cp "${DEPLOY_DIR}/.env.example" "${DEPLOY_DIR}/.env"
fi

SCHED_STORAGE_DIR="$(grep '^SCHED_STORAGE_DIR=' "${DEPLOY_DIR}/.env" | cut -d= -f2-)"
mkdir -p "${SCHED_STORAGE_DIR}"

# Auto-shift the Go SLURM backend host port if the default (8765) is in use.
GO_SLURM_HOST_PORT=$(python3 - <<'PY'
import socket, sys
port = 8765
while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if s.connect_ex(("127.0.0.1", port)) != 0:
            print(port)
            break
    port += 1
    if port > 8865:
        sys.exit("No free port found in range 8765-8865")
PY
)
export GO_SLURM_HOST_PORT
# Persist in .env so run_local_docker_slurm_worker.sh picks it up.
if grep -q '^GO_SLURM_HOST_PORT=' "${DEPLOY_DIR}/.env"; then
  sed -i "s|^GO_SLURM_HOST_PORT=.*|GO_SLURM_HOST_PORT=${GO_SLURM_HOST_PORT}|" "${DEPLOY_DIR}/.env"
else
  echo "GO_SLURM_HOST_PORT=${GO_SLURM_HOST_PORT}" >> "${DEPLOY_DIR}/.env"
fi
if [ "${GO_SLURM_HOST_PORT}" != "8765" ]; then
  echo "Port 8765 in use — go-slurm-backend will listen on host port ${GO_SLURM_HOST_PORT}"
fi

bash "${SCRIPT_DIR}/setup_local_slurm_host.sh"
docker build -t bwb-scheduler -f "${DEPLOY_DIR}/Dockerfile" "${REPO_ROOT}"
docker build -t go-slurm-backend -f "${DEPLOY_DIR}/Dockerfile.go-slurm-backend" "${REPO_ROOT}"
docker compose --env-file "${DEPLOY_DIR}/.env" -f "${DEPLOY_DIR}/docker-compose.yml" up -d
bash "${SCRIPT_DIR}/run_local_docker_slurm_worker.sh" --detach

python3 - <<'PY'
import json
import time
import urllib.request

url = "http://localhost:8000/openapi.json"
deadline = time.time() + 180
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

deadline = time.time() + 180
cmd = [
    "docker",
    "exec",
    "temporal-admin-tools",
    "tctl",
    "namespace",
    "describe",
    "default",
]
while time.time() < deadline:
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode == 0:
        print("Temporal default namespace is ready.")
        break
    time.sleep(2)
else:
    raise SystemExit("Temporal default namespace did not become ready in time.")
PY
