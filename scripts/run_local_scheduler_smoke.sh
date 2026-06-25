#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cleanup() {
  if [ "${KEEP_LOCAL_SCHEDULER_STACK_UP:-false}" != "true" ]; then
    bash "${SCRIPT_DIR}/local_scheduler_stack_down.sh"
  fi
}
trap cleanup EXIT

bash "${SCRIPT_DIR}/local_scheduler_stack_up.sh"

python3 -m bwb_scheduler.benchmark_harness \
  "${REPO_ROOT}/benchmarks/local_echo_manifest.json" \
  --wait-for-api-seconds "${SCHEDULER_API_WAIT_SECONDS:-120}" \
  --poll-seconds "${SCHEDULER_SMOKE_POLL_SECONDS:-2}" \
  --timeout-seconds "${SCHEDULER_SMOKE_TIMEOUT_SECONDS:-300}" \
  --stop-on-timeout
