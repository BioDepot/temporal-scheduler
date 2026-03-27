#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFEST="${1:-benchmarks/test_scheme_local_docker_slurm_manifest.json}"
KEEP_UP="${KEEP_LOCAL_SLURM_TEST_ENV_UP:-false}"

bash "${SCRIPT_DIR}/local_slurm_test_env_up.sh"

cleanup() {
  if [ "${KEEP_UP}" != "true" ]; then
    bash "${SCRIPT_DIR}/local_slurm_test_env_down.sh"
  fi
}

trap cleanup EXIT

cd "${REPO_ROOT}"
python3 -m bwb_scheduler.benchmark_harness "${MANIFEST}" \
  --wait-for-api-seconds 120 \
  --poll-seconds 10 \
  --timeout-seconds 7200
