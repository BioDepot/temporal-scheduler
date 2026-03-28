# Conditional Execution Runbook

## Goal

Implement and validate conditional execution for the Arabidopsis `FASTQ_SUBSAMPLE_FQ_SALMON` path so that the scheduler can:

1. preserve the upstream `make_index` condition contract from `bwb-nextflow-utils`
2. execute the conditional branch when `make_index=true`
3. skip the index-building branch and use a prebuilt Salmon index when `make_index=false`
4. run both cases through the existing local Temporal + Docker Slurm harness in this repo

## Current State

### Artifacts already in place

- Upstream Nextflow -> IR translator with conditional preservation:
  - `/mnt/pikachu/bwb-nextflow-utils/translators/nextflow_to_ir.py`
- Upstream IR -> BWB JSON translator with conditional metadata preservation:
  - `/mnt/pikachu/bwb-nextflow-utils/translators/ir_to_bwb.py`
- Arabidopsis round-trip fixture scripts:
  - `/mnt/pikachu/bwb-nextflow-utils/scripts/prepare_rnaseq_arabidopsis_fixture.py`
  - `/mnt/pikachu/bwb-nextflow-utils/scripts/run_rnaseq_arabidopsis_roundtrip.py`
- Local Arabidopsis demo data on this machine:
  - `/mnt/pikachu/salmon_demo_work/DRR016125_1.fastq`
  - `/mnt/pikachu/salmon_demo_work/DRR016125_2.fastq`
  - `/mnt/pikachu/salmon_demo_work/Arabidopsis_thaliana.TAIR10.28.cdna.all.fa.gz`
  - `/mnt/pikachu/salmon_demo_work/athal_index`
- Local scheduler harness in this repo:
  - `/mnt/pikachu/temporal-scheduler/scripts/local_slurm_test_env_up.sh`
  - `/mnt/pikachu/temporal-scheduler/scripts/local_slurm_test_env_down.sh`
  - `/mnt/pikachu/temporal-scheduler/scripts/run_local_slurm_e2e.sh`
  - `/mnt/pikachu/temporal-scheduler/bwb_scheduler/benchmark_harness.py`
- Scheduler JSON metadata contract for conditions:
  - `/mnt/pikachu/temporal-scheduler/bwb_scheduler/ir_to_scheduler.py`
  - `/mnt/pikachu/temporal-scheduler/README.md`
- Scheduler runtime condition evaluator and pruning:
  - `/mnt/pikachu/temporal-scheduler/bwb/scheduling_service/conditions.py`
  - `/mnt/pikachu/temporal-scheduler/bwb/scheduling_service/bwb_workflow.py`
- Arabidopsis IR -> scheduler decoder:
  - `/mnt/pikachu/temporal-scheduler/bwb_scheduler/nextflow_ir_to_scheduler.py`
- Arabidopsis local Slurm wrapper and config:
  - `/mnt/pikachu/temporal-scheduler/scripts/run_arabidopsis_conditional_local_slurm_e2e.sh`
  - `/mnt/pikachu/temporal-scheduler/bwb/scheduling_service/test_workflows/rnaseq_arabidopsis_local_docker_slurm_config.json`

### Artifacts verified during investigation

- The Arabidopsis round-trip runs successfully in `bwb-nextflow-utils`.
- Conditional mode produced:
  - `3` IR nodes
  - `2` IR edges
  - `3` BWB nodes
  - `2` BWB links
- Prebuilt-index mode produced:
  - `2` IR nodes
  - `1` IR edge
  - `2` BWB nodes
  - `1` BWB link
- Generated outputs were written to:
  - `/tmp/rnaseq_arabidopsis_roundtrip_conditional/outputs`
  - `/tmp/rnaseq_arabidopsis_roundtrip_prebuilt/outputs`

### Tests and contracts already in place

- `bwb-nextflow-utils` contract tests:
  - `/mnt/pikachu/bwb-nextflow-utils/tests/test_nextflow_to_ir.py`
  - `/mnt/pikachu/bwb-nextflow-utils/tests/test_ir_to_bwb.py`
- Scheduler-side metadata tests:
  - `/mnt/pikachu/temporal-scheduler/tests/test_ir_to_scheduler.py`
  - `/mnt/pikachu/temporal-scheduler/tests/test_graph_manager.py`
- Existing local Slurm end-to-end harness:
  - proven already for the scheduler’s existing test workflow

### Validation status

- The scheduler now evaluates conditions at workflow start, prunes inactive links, and marks inactive nodes skipped in workflow status.
- The Arabidopsis decoder is committed and produces scheduler JSON plus an optional benchmark manifest from the upstream IR.
- The local Slurm wrapper is committed and stages the Arabidopsis inputs automatically.
- The local conditional execution path has been validated end to end for `make_index=true`.
- Latest successful benchmark artifact:
  - `/mnt/pikachu/temporal-scheduler/benchmarks/results/benchmark_run_20260328T074511Z.json`

### Remaining gap

- The `make_index=false` local harness path still needs an explicit live validation run and artifact capture. The modeling is in place, but only the `make_index=true` branch has been re-proven end to end on the local Slurm harness.

## Recommended Execution Contract

Use the existing metadata contract already aligned with `bwb-nextflow-utils`:

- top-level `conditions`
- optional `condition_ref` on nodes
- optional `condition_ref` on links
- optional inline `condition` object on nodes or links

Add one runtime-only extension so the scheduler can evaluate the conditions:

- top-level `condition_context`

Recommended shape:

```json
{
  "conditions": [
    {
      "id": "cond_make_index",
      "type": "nextflow_if",
      "expr": "make_index",
      "source_language": "nextflow",
      "status": "source_preserved"
    }
  ],
  "condition_context": {
    "make_index": true
  }
}
```

### Evaluation rules

- Support only simple boolean expressions first:
  - `make_index`
  - `!make_index`
  - `true`
  - `false`
- If a referenced variable is missing from `condition_context`, fail fast.
- If a node’s `condition_ref` evaluates to `false`, mark the node skipped and never schedule it.
- If a link’s `condition_ref` evaluates to `false`, remove that link before graph execution.
- If a skipped node is the source of a link, that link must not participate in input propagation.

### Why this contract is sufficient

- It preserves the upstream Nextflow/IR vocabulary instead of inventing a new conditional model.
- It is backward compatible for existing workflows because `conditions` and `condition_context` are optional.
- It fits the current scheduler behavior where linked inputs override static node parameters:
  - if the conditional link into `salmon_quant.index` is active, it should override the node’s static `index` parameter
  - if the conditional link is inactive, `salmon_quant` can fall back to the static prebuilt-index path in `parameters`

## Implemented Design

### 1. Scheduler condition evaluator

Implemented in:
- `/mnt/pikachu/temporal-scheduler/bwb/scheduling_service/conditions.py`

Responsibilities:

- parse top-level `conditions`
- read top-level `condition_context`
- evaluate supported expressions into `condition_id -> bool`
- expose helper functions for:
  - `node_is_active(node, condition_results)`
  - `link_is_active(link, condition_results)`

### 2. Workflow preprocessing before scheduling

Implemented in:
- `/mnt/pikachu/temporal-scheduler/bwb/scheduling_service/bwb_workflow.py`

Responsibilities:

- deep-copy the input scheme
- evaluate all conditions once at workflow start
- remove inactive links
- mark inactive nodes as skipped
- ensure skipped nodes are not started

Recommended behavior:

- retain skipped nodes in workflow status output
- add a status string such as:
  - `Skipped by condition cond_make_index=false`

### 3. Static fallback inputs for downstream nodes

For the Arabidopsis case, `salmon_quant` must always have a usable `index` value in `parameters`.

Required modeling:

- `salmon_quant.parameters.index = "/mnt/pikachu/salmon_demo_work/athal_index"` or the scheduler-visible equivalent
- conditional link:
  - `salmon_index.index -> salmon_quant.index`
  - guarded by `cond_make_index`

Runtime result:

- when `make_index=true`, the active link overrides the static parameter
- when `make_index=false`, the link is inactive and the static parameter remains in use

### 4. Arabidopsis decoder

Implemented in:
- `/mnt/pikachu/temporal-scheduler/bwb_scheduler/nextflow_ir_to_scheduler.py`

Current scope:

- fixed mapping for:
  - `salmon_index`
  - `fq_subsample`
  - `salmon_quant`
- populate:
  - scheduler `nodes`
  - scheduler `links`
  - top-level `conditions`
  - top-level `condition_context`
- generate two runtime variants from the same logical workflow:
  - conditional runtime variant with `make_index=true`
  - conditional runtime variant with `make_index=false`

### 5. Local benchmark manifests and config

Committed artifacts in this repo:

- local Docker Slurm config for the new node ids
- generated benchmark manifests written under `benchmarks/generated/` by the decoder or wrapper

Recommended generated outputs:

- `benchmarks/generated/rnaseq_arabidopsis.conditional.scheduler.json`
- `benchmarks/generated/rnaseq_arabidopsis.prebuilt.scheduler.json`
- `benchmarks/generated/rnaseq_arabidopsis.conditional.local_slurm_manifest.json`
- `benchmarks/generated/rnaseq_arabidopsis.prebuilt.local_slurm_manifest.json`

If the decoder is deterministic and the artifacts are small, also add stable fixtures under `tests/fixtures` or `benchmarks/`.

## Tests And Contracts

### Unit tests in this repo

Current tests cover:

- condition expression evaluation
- unresolved condition variables failing fast
- node skipping when `condition_ref` is false
- link pruning when `condition_ref` is false
- downstream node keeping static fallback input when a conditional link is pruned
- workflow status reporting for skipped nodes
- Arabidopsis decoder output containing:
  - `conditions`
  - `condition_context`
  - conditional node/link refs
  - fallback `index` parameter on `salmon_quant`

### Contract tests against upstream artifacts

Use the generated Arabidopsis IR from `bwb-nextflow-utils` and assert:

- `workflow.conditions == [{"id": "cond_make_index", ...}]`
- `salmon_index.condition_ref == "cond_make_index"`
- `salmon_index.index -> salmon_quant.index` edge carries `condition_ref`

These contract tests should remain structural and not require live execution.

### Local harness end-to-end tests

Run both execution modes through the local Docker Slurm harness:

1. `make_index=true`
2. `make_index=false`

Acceptance criteria:

- `make_index=true`
  - `salmon_index`, `fq_subsample`, and `salmon_quant` all run
  - the conditioned edge from `salmon_index.index` to `salmon_quant.index` is active
  - workflow finishes successfully
- `make_index=false`
  - `salmon_index` is reported skipped
  - `fq_subsample` and `salmon_quant` run
  - `salmon_quant` uses the prebuilt index path from static parameters
  - workflow finishes successfully

### Regression that mattered in practice

The live local Slurm path was blocked by the scheduler’s Slurm completion loop. The fixes that unblocked the end-to-end run were:
- normalize Temporal-stringified Slurm job IDs back to integers before matching them
- ignore step records such as `123.batch` when deciding whether the top-level Slurm job is terminal
- remove handled jobs from both `outstanding_jobs` and `outstanding_requests` in the child poller before signaling the parent workflow
- keep the local harness in shared-filesystem mode with `use_local_storage=true`

## Arabidopsis Local Harness Procedure

### Step 1. Generate upstream IR and BWB artifacts

Conditional variant:

```bash
python3 /mnt/pikachu/bwb-nextflow-utils/scripts/run_rnaseq_arabidopsis_roundtrip.py \
  --work-dir /tmp/rnaseq_arabidopsis_roundtrip_conditional
```

Prebuilt-index structural variant:

```bash
python3 /mnt/pikachu/bwb-nextflow-utils/scripts/run_rnaseq_arabidopsis_roundtrip.py \
  --work-dir /tmp/rnaseq_arabidopsis_roundtrip_prebuilt \
  --assume-prebuilt-index
```

Expected outputs:

- `/tmp/rnaseq_arabidopsis_roundtrip_conditional/outputs/rnaseq_arabidopsis.ir.json`
- `/tmp/rnaseq_arabidopsis_roundtrip_conditional/outputs/rnaseq_arabidopsis.bwb.json`
- `/tmp/rnaseq_arabidopsis_roundtrip_prebuilt/outputs/rnaseq_arabidopsis.ir.json`
- `/tmp/rnaseq_arabidopsis_roundtrip_prebuilt/outputs/rnaseq_arabidopsis.bwb.json`

### Step 2. Decode IR into scheduler JSON

Current decoder usage:

```bash
python3 -m bwb_scheduler.nextflow_ir_to_scheduler \
  /tmp/rnaseq_arabidopsis_roundtrip_conditional/outputs/rnaseq_arabidopsis.ir.json \
  --make-index \
  --output benchmarks/generated/rnaseq_arabidopsis.conditional.scheduler.json \
  --manifest-output benchmarks/generated/rnaseq_arabidopsis.conditional.local_slurm_manifest.json

python3 -m bwb_scheduler.nextflow_ir_to_scheduler \
  /tmp/rnaseq_arabidopsis_roundtrip_conditional/outputs/rnaseq_arabidopsis.ir.json \
  --no-make-index \
  --output benchmarks/generated/rnaseq_arabidopsis.prebuilt.scheduler.json \
  --manifest-output benchmarks/generated/rnaseq_arabidopsis.prebuilt.local_slurm_manifest.json
```

Implemented decoder behavior:

- preserve `conditions`
- populate `condition_context.make_index`
- attach `condition_ref` to the scheduler node and link
- set static fallback `salmon_quant.parameters.index`

### Step 3. Generate benchmark manifests

Recommended manifest shape:

```json
{
  "api_base_url": "http://localhost:8000",
  "benchmarks": [
    {
      "name": "rnaseq-arabidopsis-conditional",
      "workflow_path": "/abs/path/to/rnaseq_arabidopsis.conditional.scheduler.json",
      "config_path": "/abs/path/to/rnaseq_arabidopsis.local_docker_slurm_config.json",
      "register_worker": false,
      "iterations": 1
    }
  ]
}
```

Create one manifest per mode. The decoder can write these directly with `--manifest-output`.

### Step 4. Start the local scheduler + Slurm harness

```bash
bash /mnt/pikachu/temporal-scheduler/scripts/local_slurm_test_env_up.sh
```

### Step 5. Run both benchmarks

```bash
python3 -m bwb_scheduler.benchmark_harness \
  benchmarks/generated/rnaseq_arabidopsis.conditional.local_slurm_manifest.json \
  --wait-for-api-seconds 120 \
  --poll-seconds 10 \
  --timeout-seconds 7200

python3 -m bwb_scheduler.benchmark_harness \
  benchmarks/generated/rnaseq_arabidopsis.prebuilt.local_slurm_manifest.json \
  --wait-for-api-seconds 120 \
  --poll-seconds 10 \
  --timeout-seconds 7200
```

### Step 6. Tear down

```bash
bash /mnt/pikachu/temporal-scheduler/scripts/local_slurm_test_env_down.sh
```

### Step 4a. One-shot wrapper

For the default local conditional path, use:

```bash
bash scripts/run_arabidopsis_conditional_local_slurm_e2e.sh
```

Useful options:
- `MAKE_INDEX=true` or `MAKE_INDEX=false`
- `RUN_ID=my-run`
- `ROUNDTRIP_WORK_DIR=/tmp/custom_roundtrip`
- `KEEP_LOCAL_SLURM_TEST_ENV_UP=true`

## Readiness Summary

### Ready now

- upstream conditional IR and BWB metadata contract
- Arabidopsis input data and prebuilt index
- local Temporal + Docker Slurm execution harness
- scheduler runtime condition evaluation and status reporting
- Arabidopsis IR -> scheduler decoder
- committed Arabidopsis local Slurm config and one-shot wrapper
- end-to-end conditional execution on the local harness for `make_index=true`

### Still to prove live

- end-to-end conditional execution on the local harness for `make_index=false`

## Recommended Next Implementation Order

1. run and capture the `MAKE_INDEX=false` local Slurm artifact
2. tighten status assertions in the end-to-end harness around skipped nodes
3. broaden the decoder beyond the current Arabidopsis salmon subset if more workflows are needed
