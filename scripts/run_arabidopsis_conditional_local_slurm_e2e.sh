#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NEXTFLOW_UTILS_DIR="${NEXTFLOW_UTILS_DIR:-/mnt/pikachu/bwb-nextflow-utils}"
ROUNDTRIP_WORK_DIR="${ROUNDTRIP_WORK_DIR:-/tmp/rnaseq_arabidopsis_roundtrip}"
GENERATED_DIR="${REPO_ROOT}/benchmarks/generated"
MAKE_INDEX="${MAKE_INDEX:-true}"
USE_LOCAL_STORAGE="${USE_LOCAL_STORAGE:-true}"
RUN_ID="${RUN_ID:-rnaseq_arabidopsis_conditional}"
CONFIG_PATH="${REPO_ROOT}/bwb/scheduling_service/test_workflows/rnaseq_arabidopsis_local_docker_slurm_config.json"
OUTPUT_JSON="${GENERATED_DIR}/${RUN_ID}.scheduler.json"
MANIFEST_PATH="${GENERATED_DIR}/${RUN_ID}.local_slurm_manifest.json"
DEPLOY_ENV="${REPO_ROOT}/deployment/.env"
DEPLOY_ENV_EXAMPLE="${REPO_ROOT}/deployment/.env.example"

mkdir -p "${GENERATED_DIR}"

if [ ! -f "${DEPLOY_ENV}" ]; then
  cp "${DEPLOY_ENV_EXAMPLE}" "${DEPLOY_ENV}"
fi

python3 "${NEXTFLOW_UTILS_DIR}/scripts/run_rnaseq_arabidopsis_roundtrip.py" \
  --work-dir "${ROUNDTRIP_WORK_DIR}"

MAKE_INDEX_FLAG="--make-index"
if [ "${MAKE_INDEX}" != "true" ]; then
  MAKE_INDEX_FLAG="--no-make-index"
fi

LOCAL_STORAGE_FLAG="--use-local-storage"
if [ "${USE_LOCAL_STORAGE}" != "true" ]; then
  LOCAL_STORAGE_FLAG="--no-use-local-storage"
fi

python3 -m bwb_scheduler.nextflow_ir_to_scheduler \
  "${ROUNDTRIP_WORK_DIR}/outputs/rnaseq_arabidopsis.ir.json" \
  --output "${OUTPUT_JSON}" \
  --run-id "${RUN_ID}" \
  --config-path "${CONFIG_PATH}" \
  --manifest-output "${MANIFEST_PATH}" \
  "${LOCAL_STORAGE_FLAG}" \
  "${MAKE_INDEX_FLAG}"

SCHED_STORAGE_DIR="$(grep '^SCHED_STORAGE_DIR=' "${DEPLOY_ENV}" | cut -d= -f2-)"
if [ -z "${SCHED_STORAGE_DIR}" ]; then
  echo "SCHED_STORAGE_DIR is not set in ${DEPLOY_ENV}" >&2
  exit 1
fi

STAGE_DIR="${SCHED_STORAGE_DIR}/${RUN_ID}/singularity_data"
docker run --rm \
  -v "${SCHED_STORAGE_DIR}:/stage_root" \
  -v /mnt/pikachu/salmon_demo_work:/src:ro \
  alpine:3.18 \
  sh -lc "
    mkdir -p '/stage_root/${RUN_ID}/singularity_data' &&
    rm -rf '/stage_root/${RUN_ID}/singularity_data/athal_index' '/stage_root/${RUN_ID}/singularity_data/results' &&
    cp -f /src/DRR016125_1.fastq '/stage_root/${RUN_ID}/singularity_data/DRR016125_1.fastq' &&
    cp -f /src/DRR016125_2.fastq '/stage_root/${RUN_ID}/singularity_data/DRR016125_2.fastq' &&
    cp -f /src/Arabidopsis_thaliana.TAIR10.28.cdna.all.fa.gz '/stage_root/${RUN_ID}/singularity_data/Arabidopsis_thaliana.TAIR10.28.cdna.all.fa.gz' &&
    cp -a /src/athal_index '/stage_root/${RUN_ID}/singularity_data/athal_index'
  "

bash "${SCRIPT_DIR}/run_local_slurm_e2e.sh" "${MANIFEST_PATH}"
