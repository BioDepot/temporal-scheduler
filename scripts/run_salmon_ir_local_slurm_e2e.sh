#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OWS_PATH="${1:-${REPO_ROOT}/star_salmon_short/star_salmon_short.ows}"
RUN_ID="${RUN_ID:-}"
GENERATED_DIR="${REPO_ROOT}/benchmarks/generated"
CONFIG_PATH="${REPO_ROOT}/bwb/scheduling_service/test_workflows/test_scheme_local_docker_slurm_config.json"

mkdir -p "${GENERATED_DIR}"

WORKFLOW_BASENAME="$(basename "${OWS_PATH}" .ows)"
WORKFLOW_JSON_PATH="${GENERATED_DIR}/${WORKFLOW_BASENAME}.decoded.json"
MANIFEST_PATH="${GENERATED_DIR}/${WORKFLOW_BASENAME}.local_slurm_manifest.json"

DECODE_ARGS=(
  -m
  bwb_scheduler.ir_to_scheduler
  "${OWS_PATH}"
  --workflow
  salmon
  --output
  "${WORKFLOW_JSON_PATH}"
)

if [ -n "${RUN_ID}" ]; then
  DECODE_ARGS+=(--run-id "${RUN_ID}")
fi

python3 "${DECODE_ARGS[@]}"

python3 - <<PY
import json
from pathlib import Path

manifest = {
    "api_base_url": "http://localhost:8000",
    "benchmarks": [
        {
            "name": "salmon-ir-local-docker-slurm",
            "workflow_path": str(Path("${WORKFLOW_JSON_PATH}").resolve()),
            "config_path": str(Path("${CONFIG_PATH}").resolve()),
            "register_worker": False,
            "iterations": 1,
        }
    ],
}
Path("${MANIFEST_PATH}").write_text(json.dumps(manifest, indent=2) + "\n")
PY

bash "${SCRIPT_DIR}/run_local_slurm_e2e.sh" "${MANIFEST_PATH}"
