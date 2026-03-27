#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_ENV="${REPO_ROOT}/deployment/.env"
DEPLOY_ENV_EXAMPLE="${REPO_ROOT}/deployment/.env.example"
CONTAINER_NAME="${LOCAL_SLURM_WORKER_CONTAINER_NAME:-local-slurm-worker}"
WORKER_CONFIG="${LOCAL_SLURM_WORKER_CONFIG:-bwb/scheduling_service/test_workflows/test_scheme_local_docker_slurm_config.json}"
DETACH=false

usage() {
  cat <<'EOF'
Usage: bash scripts/run_local_docker_slurm_worker.sh [--detach] [--stop]

Starts the local Slurm worker in a Docker container using host networking so it
can reach the local Slurm controller at localhost:3022.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --detach)
      DETACH=true
      shift
      ;;
    --stop)
      docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
      exit 0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ ! -f "${DEPLOY_ENV}" ]; then
  cp "${DEPLOY_ENV_EXAMPLE}" "${DEPLOY_ENV}"
fi

if [ ! -d "${HOME}/.ssh" ]; then
  echo "Missing ${HOME}/.ssh, cannot provide SSH credentials to the Slurm worker." >&2
  exit 1
fi

SCHED_STORAGE_DIR="$(grep '^SCHED_STORAGE_DIR=' "${DEPLOY_ENV}" | cut -d= -f2-)"
MINIO_ACCESS_KEY="$(grep '^MINIO_ACCESS_KEY=' "${DEPLOY_ENV}" | cut -d= -f2-)"
MINIO_SECRET_KEY="$(grep '^MINIO_SECRET_KEY=' "${DEPLOY_ENV}" | cut -d= -f2-)"

if [ -z "${SCHED_STORAGE_DIR}" ]; then
  echo "SCHED_STORAGE_DIR is not set in ${DEPLOY_ENV}" >&2
  exit 1
fi

mkdir -p "${SCHED_STORAGE_DIR}"
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

RUN_ARGS=(
  --network host
  --name "${CONTAINER_NAME}"
  -v "${HOME}/.ssh:/root/.ssh:ro"
  -v "${SCHED_STORAGE_DIR}:/data/sched_storage"
  -e DEBUG_MODE=false
  -e MINIO_ENDPOINT_URL=http://localhost:9000
  -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}"
  -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY}"
  -e SCHED_STORAGE_DIR=/data/sched_storage
  -e TEMPORAL_ENDPOINT_URL=localhost:7233
)

if [ "${DETACH}" = true ]; then
  docker run -d "${RUN_ARGS[@]}" bwb-scheduler bash -lc \
    "while true; do /root/.cache/pypoetry/virtualenvs/bwb-scheduler-9TtSrW0h-py3.11/bin/python bwb/scheduling_service/worker.py slurm --config ${WORKER_CONFIG} && exit 0; echo 'local-slurm-worker: retrying after startup failure' >&2; sleep 5; done"
else
  docker run --rm "${RUN_ARGS[@]}" bwb-scheduler bash -lc \
    "while true; do /root/.cache/pypoetry/virtualenvs/bwb-scheduler-9TtSrW0h-py3.11/bin/python bwb/scheduling_service/worker.py slurm --config ${WORKER_CONFIG} && exit 0; echo 'local-slurm-worker: retrying after startup failure' >&2; sleep 5; done"
fi
