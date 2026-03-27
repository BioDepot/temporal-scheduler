# BWB-Schedule docker-compose files

## How to start
1. Copy `deployment/.env.example` to `deployment/.env`, then update the keys `WORKER_RAM`, `WORKER_CPUS`, and `WORKER_GPUS` if needed.
    ```
    cp deployment/.env.example deployment/.env
    ```
2. Build the `bwb-scheduler` docker image.
   (WIP: If we upload the `bwb-scheduler` image to docker hub, then this step can be ignored.)
   ```bash
   docker build -t bwb-scheduler -f deployment/Dockerfile .
   ```

3. Execute docker-compose.
    ```bash
    cd deployment
    docker compose up
    ```

## Optional Setting 
- running with minio instance
    ```bash
    docker compose --profile minio up
    ```
  
## Example
```commandline
curl -H "Content-Type: application/json" --data @bwb/scheduling_service/test_workflows/test_scheme_req.json http://localhost:8000/start_workflow && echo
```

You'll get response w/ key `workflow_id`. Now add a worker to process it. Be sure that the values given here for worker resources
(RAM, CPUs, GPUs) accord with those in the `.env` file for the docker-compose.
```commandline
curl -H "Content-Type: application/json" --data '{"workflow_id": "[WORKFLOW_ID]", "worker_queue": "worker1", "worker_cpus": [WORKER_CPUS], "worker_gpus": [WORKER_GPUS], "worker_mem_mb": [WORKER_MEM_MB]}' http://localhost:8000/add_worker_to_workflow && echo
```

## Notes
- Default queue name for worker is `worker1`. This will aim to use no more than 60% of your machine's RAM.
- The worker containers may need to restart a few times, because the temporal docker containers will register as up
before they're actually functional. Idk how to fix this outside of rewriting the tepmoral docker container code,
so this is probably good enough for now.
- Q: What does `/dyncamicconfig` directory do? 
  
    A: This directory contains the initial configuration for the Temporal database. It can be modified if needed.
    Source: https://github.com/temporalio/docker-compose/tree/main/dynamicconfig

## Benchmarking
Once the stack is up, you can run the benchmark harness against the scheduler API.

Quick start:
```bash
python3 -m bwb_scheduler.benchmark_harness benchmarks/salmon_demo_manifest.json --wait-for-api-seconds 120
```

This writes a timestamped result file under `benchmarks/results/` containing:
- workflow IDs and run IDs
- elapsed wall-clock time per iteration
- worker resources used for the run
- final scheduler status payload returned by `/workflow_status`

Manifest notes:
- Use `request_path` when the JSON file already matches the API contract and contains `workflow_def`.
- Use `workflow_path` for raw scheduler workflow JSON; the harness will wrap it as `{"workflow_def": ...}` automatically.
- Use `config_path` alongside `workflow_path` when benchmarking a workflow with a separate executor config.

Example manifest shape:
```json
{
  "api_base_url": "http://localhost:8000",
  "default_worker": {
    "queue": "worker1",
    "cpus": 8,
    "mem_mb": 16000,
    "gpus": 0
  },
  "benchmarks": [
    {
      "name": "salmon-demo",
      "request_path": "../bwb/scheduling_service/test_workflows/test_scheme_req.json",
      "iterations": 3
    }
  ]
}
```

### SLURM benchmark runs
For workflows that use the SLURM executor, start the scheduler stack as above, then run a separate SLURM worker process from the repo root:

```bash
bash scripts/run_slurm_worker.sh bwb/scheduling_service/test_workflows/test_scheme_slurm_config.json
```

Then run the SLURM benchmark manifest:

```bash
python3 -m bwb_scheduler.benchmark_harness benchmarks/test_scheme_slurm_manifest.json --wait-for-api-seconds 120
```

Notes:
- The SLURM worker uses the config file's `executors.slurm` SSH settings.
- Benchmarks with `"register_worker": false` skip `/add_worker_to_workflow`; this is the correct mode for SLURM-only executor runs.
- If your SSH setup needs a password, set `SSH_PASSWORD` in the environment before starting the SLURM worker.

### Local Docker Slurm host
For a same-machine test target, use:
- `bwb/scheduling_service/test_workflows/test_scheme_local_docker_slurm_config.json`
- `benchmarks/test_scheme_local_docker_slurm_manifest.json`
- `bash scripts/run_local_docker_slurm_worker.sh --detach`
- `bash scripts/setup_local_slurm_host.sh`

This local mode expects a Slurm controller reachable at `root@localhost:3022` with a shared work dir at `/data/temporal_scheduler`.

For an on-demand local test environment:

```bash
bash scripts/local_slurm_test_env_up.sh
python3 -m bwb_scheduler.benchmark_harness benchmarks/test_scheme_local_docker_slurm_manifest.json --wait-for-api-seconds 120 --poll-seconds 10 --timeout-seconds 7200
bash scripts/local_slurm_test_env_down.sh
```

Or use the one-shot wrapper:

```bash
bash scripts/run_local_slurm_e2e.sh
```

To exercise the full salmon workflow path from `.ows` IR through scheduler JSON conversion and local Slurm execution:

```bash
bash scripts/run_salmon_ir_local_slurm_e2e.sh
```

By default this wrapper reads `star_salmon_short/star_salmon_short.ows`, writes the decoded workflow plus a generated benchmark manifest under `benchmarks/generated/`, then calls `run_local_slurm_e2e.sh` to run the workflow to completion.

Useful variants:
- `bash scripts/run_salmon_ir_local_slurm_e2e.sh /path/to/workflow.ows`
- `RUN_ID=my-run bash scripts/run_salmon_ir_local_slurm_e2e.sh`
- `KEEP_LOCAL_SLURM_TEST_ENV_UP=true bash scripts/run_salmon_ir_local_slurm_e2e.sh`

Notes:
- `local_slurm_test_env_up.sh` sets up the local Docker Slurm host, builds `bwb-scheduler`, starts the compose stack, and launches the local Slurm worker container.
- `local_slurm_test_env_down.sh` stops the worker container, tears down the compose stack, and stops the local Docker Slurm cluster.
- Set `KEEP_LOCAL_SLURM_TEST_ENV_UP=true` when calling `run_local_slurm_e2e.sh` if you want to leave the environment running after the benchmark finishes.
