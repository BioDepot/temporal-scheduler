#!/usr/bin/env bash
set -euo pipefail

SSH_DOCKER_WORKER_NAME="${SSH_DOCKER_WORKER_NAME:-temporal-scheduler-local-ssh-docker-worker}"
docker rm -f "${SSH_DOCKER_WORKER_NAME}" >/dev/null 2>&1 || true
echo "Stopped ${SSH_DOCKER_WORKER_NAME}."
