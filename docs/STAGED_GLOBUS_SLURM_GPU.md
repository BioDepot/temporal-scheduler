# Staged Globus, Slurm, and GPU Workflows

`StagedSlurmGpuWorkflow` provides one durable Temporal execution across four
independently routed stages:

1. optional Globus stage-in;
2. an optional raw Slurm batch script;
3. optional Globus stage-back;
4. an optional SSH/Docker job, with optional CUDA and GPU free-memory gating;
5. optional Globus publication of the GPU outputs.

The orchestration worker does not execute Slurm or GPU work itself. Temporal
routes the Slurm activities to the configured HPC queue and starts the existing
`RemoteDockerWorkflow` as a child on the configured SSH/Docker queue.

## Safety and retry contract

- Every Globus request requires a caller-generated `submission_id`. Globus
  uses that ID to make activity retries safe against duplicate transfer
  submission.
- Globus endpoint IDs can be restricted with the worker allowlist. Paths must
  be absolute and cannot contain `..` traversal.
- Transfers default to checksum synchronization, checksum verification, and
  modification-time preservation.
- Raw Slurm submission is attempted once because `sbatch` is not naturally
  idempotent. Subsequent polling and output retrieval are retried.
- Omitting `slurm` is an explicit recovery mode for resuming GPU and publish
  stages from already validated local inputs. Record the preceding Slurm job
  and stage-back task in the scientific provenance; the result reports the
  Slurm stage as `SKIPPED`.
- Omitting `gpu` supports CPU-only staged workflows such as an nf-core launch
  on Slurm. The result reports the GPU stage as `SKIPPED` and publication can
  proceed after stage-back.
- A CUDA job can set `min_gpu_free_mb`. A busy GPU is a retryable preflight
  condition until `gpu_wait_timeout_seconds`; absent SSH, Docker, or CUDA
  support fails without retry.
- Stage and external task IDs are available from the workflow status query.

## Workers

Start one worker for each role against the same Temporal server:

```bash
export TEMPORAL_ENDPOINT_URL=localhost:7233

python bwb/scheduling_service/worker.py staged-pipeline \
  --config bwb/scheduling_service/test_workflows/staged_pipeline_worker_config.example.json

python bwb/scheduling_service/worker.py slurm \
  --config /path/to/site-config.json

python bwb/scheduling_service/worker.py ssh-docker \
  --config /path/to/site-config.json
```

The Globus CLI runs on the orchestration worker and must already be logged in.
The Slurm and SSH/Docker queue names are `user@host:port` unless explicitly
overridden in a request.

## API

Submit a request to:

```text
POST /start_staged_slurm_gpu_workflow
```

Poll it with:

```text
POST /staged_slurm_gpu_workflow_status
{"workflow_id": "...", "run_id": "..."}
```

The request schema is illustrated by
`bwb/scheduling_service/test_workflows/staged_slurm_gpu_req.example.json`.
Site-specific endpoint IDs, queues, filesystem paths, and scripts must be
filled in before submission.

The Slurm `script` is the batch body. Scheduler directives are generated from
`slurm.job.config`; do not include a second `#SBATCH` header in the body.
Scientific run records and concrete payloads belong in the provenance
repository, not in this reusable scheduler repository.
