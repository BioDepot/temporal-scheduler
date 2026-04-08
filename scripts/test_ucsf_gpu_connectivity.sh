#!/usr/bin/env bash
# Smoke test: verify SSH, rsync, Docker, and GPU connectivity to the
# UCSF remote GPU target.  Does NOT require Temporal or the scheduler
# to be running — this is a standalone pre-flight check.
#
# Usage:
#   bash scripts/test_ucsf_gpu_connectivity.sh [config.json]
#
# Default config: bwb/scheduling_service/test_workflows/ucsf_gpu_cellbender_config.json
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG="${1:-${REPO_ROOT}/bwb/scheduling_service/test_workflows/ucsf_gpu_cellbender_config.json}"

if [[ ! -f "${CONFIG}" ]]; then
  echo "FAIL: config not found: ${CONFIG}"
  exit 1
fi

IP_ADDR=$(python3 -c "import json,sys; c=json.load(open('${CONFIG}')); print(c['executors']['ssh_docker']['ip_addr'])")
USER=$(python3 -c "import json,sys; c=json.load(open('${CONFIG}')); print(c['executors']['ssh_docker']['user'])")
PORT=$(python3 -c "import json,sys; c=json.load(open('${CONFIG}')); print(c['executors']['ssh_docker'].get('port', 22))")
STORAGE_DIR=$(python3 -c "import json,sys; c=json.load(open('${CONFIG}')); print(c['executors']['ssh_docker']['storage_dir'])")

SSH_TARGET="${USER}@${IP_ADDR}"
SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
if [[ "${PORT}" != "22" ]]; then
  SSH_OPTS="${SSH_OPTS} -p ${PORT}"
fi

PASS=0
FAIL=0
check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "  PASS  ${label}"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  ${label}"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== UCSF GPU target connectivity test ==="
echo "Target: ${SSH_TARGET}:${PORT}"
echo "Storage: ${STORAGE_DIR}"
echo ""

echo "[1/6] SSH connectivity"
check "ssh echo" ssh ${SSH_OPTS} "${SSH_TARGET}" "echo __ok__"

echo "[2/6] rsync reachability"
RSYNC_SSH="ssh"
[[ "${PORT}" == "22" ]] || RSYNC_SSH="ssh -p ${PORT}"
check "rsync dry-run" rsync --dry-run -e "${RSYNC_SSH}" "${SSH_TARGET}:/dev/null" /dev/null

echo "[3/6] Remote storage writable"
PROBE="${STORAGE_DIR}/.connectivity_probe_$$"
check "touch+rm probe" ssh ${SSH_OPTS} "${SSH_TARGET}" "mkdir -p ${STORAGE_DIR} && touch ${PROBE} && rm -f ${PROBE}"

echo "[4/6] Docker available"
check "docker info" ssh ${SSH_OPTS} "${SSH_TARGET}" "docker info --format '{{.ID}}'"

echo "[5/6] NVIDIA GPU accessible"
GPU_OUT=$(ssh ${SSH_OPTS} "${SSH_TARGET}" "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader" 2>/dev/null || true)
if [[ -n "${GPU_OUT}" ]]; then
  echo "  PASS  nvidia-smi"
  echo "        GPUs: ${GPU_OUT}"
  PASS=$((PASS + 1))
else
  echo "  FAIL  nvidia-smi"
  FAIL=$((FAIL + 1))
fi

echo "[6/6] Disk space"
DF_OUT=$(ssh ${SSH_OPTS} "${SSH_TARGET}" "df -BG ${STORAGE_DIR} | tail -1 | awk '{print \$4}'" 2>/dev/null || true)
if [[ -n "${DF_OUT}" ]]; then
  echo "  PASS  disk space: ${DF_OUT} free"
  PASS=$((PASS + 1))
else
  echo "  FAIL  disk space check"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
echo "All pre-flight checks passed. Target is ready for SSH+Docker execution."
