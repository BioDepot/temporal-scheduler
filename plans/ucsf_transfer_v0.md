# UCSF Transfer/Staging v0 — Design Note

## Supported runtime transfer paths

The v0 UCSF transfer story provides **two execution paths**, both using
**ssh/rsync** as the first-class transfer mechanism:

1. **SSH+Docker (primary — matches actual UCSF GPU target, standalone only)**
   For standalone GPU workstations with Docker + NVIDIA runtime but no SLURM.
   This is the honest path for 10.159.4.53.  Currently runs single-command
   jobs via `RemoteDockerWorkflow` or `scripts/run_remote_docker.py`.
   Not yet integrated into the BWB DAG executor.

2. **SLURM+Singularity (existing — improved)**
   For HPC clusters with SLURM job scheduling. GPU sbatch support added
   (`--gres=gpu:N`), rsync error reporting improved.

## UCSF GPU target: verified topology

```
Host: 10.159.4.53
User: lhhung
GPUs: 2x NVIDIA RTX 6000 Ada Generation (49 GB VRAM each)
Disk: 1.4 TB free on /home
Runtime: Docker 28.5 with NVIDIA runtime
No SLURM, no Singularity
```

Pre-flight connectivity verified by `scripts/test_ucsf_gpu_connectivity.sh`:
SSH, rsync, Docker, nvidia-smi, storage writability, disk space — all pass.

## SSH+Docker data flow (primary path)

```
Scheduler host                          10.159.4.53 (GPU workstation)
(local storage)                         Docker + NVIDIA runtime

 /mnt/pikachu/ucsf-*-corrected/         /home/lhhung/ucsf_remote_cellbender/
 └── sample/downstream/                  └── <job-id>/work/
     └── unfiltered_counts.h5ad              ├── unfiltered_counts.h5ad  ← rsync up
                                             ├── .numba/
                                             ├── .matplotlib/
                                             └── output/
                                                 └── cellbender_counts.h5  ← rsync back
```

**Lifecycle** (implemented in `SshDockerActivity`):

1. **validate_connectivity** — pre-flight: SSH, rsync, Docker, GPU, storage,
   disk space
2. **setup_remote_volumes** — `mkdir -p` staging dirs on remote
3. **upload_inputs** — `rsync -az --stats` input files to remote work dir
4. **run_remote_docker** — SSH exec:
   `docker run --rm --gpus device=N -v work:work cellbender:0.3.2 cellbender remove-background --cuda ...`
5. **download_outputs** — `rsync -az --stats` output dir back to local
6. **cleanup_remote** (optional) — `rm -rf` remote staging dir

This matches the proven pattern from `STAR-suite/scripts/run_remote_cellbender_rsync.sh`.

### SSH+Docker config

```json
{
    "executors": {
        "ssh_docker": {
            "ip_addr": "10.159.4.53",
            "user": "lhhung",
            "port": 22,
            "storage_dir": "/home/lhhung/ucsf_remote_cellbender",
            "gpu_device": ""
        }
    }
}
```

### Integration status

**SSH+Docker is standalone.**  `RemoteDockerWorkflow` runs single-command
Docker jobs end-to-end (validate → stage → run → fetch), but it is **not
wired into the BWB DAG executor** (`AbstractExecutor` subclass).  Wiring
SSH+Docker into multi-node BWB DAGs is a follow-on task.

GPU selection uses Docker's `--gpus device=N` flag directly.  No separate
`--runtime` config is needed — `--gpus` implies the NVIDIA runtime.

### How to run a remote Docker job

The SSH+Docker path is generic — it works for any Docker image that reads
inputs from a work directory and writes outputs to a subdirectory.  The
staging contract is:

- Inputs are rsync'd to `/work/<name>` on the remote host.
- The container's working directory is `/work`.
- The container writes outputs to `/work/output/`.
- `/work/output/` is rsync'd back to the local output directory.

There are two ways to use it:

#### With Temporal (proper path — retry, observability)

Start the worker (run from repo root):
```bash
python bwb/scheduling_service/worker.py ssh-docker \
    --config bwb/scheduling_service/test_workflows/ucsf_gpu_cellbender_config.json
```

Then start a `RemoteDockerWorkflow` via Temporal client with a
`RemoteDockerJobParams` payload.  The workflow sequences all six activity
steps (validate → setup → upload → run → download → cleanup) and gets
Temporal retry and observability for free.

Example `RemoteDockerJobParams` (conceptual — adapt paths to your data):
```python
RemoteDockerJobParams(
    image="biodepot/cellbender:0.3.2",
    cmd=["cellbender", "remove-background",
         "--input", "/work/unfiltered_counts.h5ad",
         "--output", "/work/output/cellbender_counts.h5",
         "--cpu-threads", "8", "--cuda"],
    input_files={
        "/mnt/pikachu/.../unfiltered_counts.h5ad": "unfiltered_counts.h5ad",
    },
    local_output_dir="/local/cellbender_out",
    use_gpu=True,
    gpu_device="0",
    env={"NUMBA_CACHE_DIR": "/work/.numba", "MPLCONFIGDIR": "/work/.matplotlib"},
)
```

#### Without Temporal (standalone script)

```bash
python scripts/run_remote_docker.py \
    --remote-host 10.159.4.53 --remote-user lhhung \
    --image biodepot/cellbender:0.3.2 \
    --input unfiltered_counts.h5ad=/path/to/unfiltered_counts.h5ad \
    --output-dir /local/cellbender_out \
    --gpu --gpu-device 0 \
    --env NUMBA_CACHE_DIR=/work/.numba \
    --env MPLCONFIGDIR=/work/.matplotlib \
    -- cellbender remove-background \
       --input /work/unfiltered_counts.h5ad \
       --output /work/output/cellbender_counts.h5 \
       --cpu-threads 8 --cuda
```

The `--input` flag is repeatable (`NAME=LOCAL_PATH`).  Everything after
`--` is the container command.  Use `--dry-run` to validate connectivity
without running anything.

Both paths work for any image, not just CellBender.

## SLURM path data flow (improved)

```
Scheduler host                          SLURM cluster (e.g. NSF Bridges2)

 local volumes                           remote storage_dir/
 ├── singularity_data/                   ├── <bucket>/singularity_tmp/<uuid>/
 └── images/*.sif                        │   └── output/
                                         ├── <bucket>/singularity_data/
                                         ├── images/*.sif
                                         └── slurm/
```

1. **Pre-flight validation** — SSH, rsync transfer endpoint, remote storage,
   disk space (new: runs before first transfer in `setup_volumes`)
2. **Upload** — `rsync -av --stats` inputs + SIF to remote
3. **sbatch** — submit with `--gres=gpu:N` (new: GPU support)
4. **Poll** — sacct-based SLURM job status polling
5. **Download** — `rsync -av --stats` outputs back

### SLURM config

```json
{
    "executors": {
        "slurm": {
            "ip_addr": "bridges2.psc.edu",
            "user": "mckeever",
            "port": 22,
            "transfer_addr": "data.bridges2.psc.edu",
            "transfer_port": 22,
            "storage_dir": "/ocean/projects/.../sched_storage",
            "go_backend_url": "http://localhost:8765"
        }
    },
    "configs": {
        "cellbender": {
            "executor": "slurm",
            "partition": "GPU-shared",
            "gpus": 1,
            "mem": "32GB",
            "cpus_per_task": 8,
            "time": "04:00:00"
        }
    }
}
```

## Assumptions about remote targets

### SSH+Docker target (10.159.4.53)
- Reachable via SSH with key-based auth (verified)
- `rsync` available on both sides (verified)
- Docker with NVIDIA runtime installed (verified)
- Remote `storage_dir` writable by `user` (verified)
- GPUs accessible via `docker run --gpus` (verified)
- No job scheduler — single-user, single-job model

### SLURM target (when used)
- Same SSH/rsync requirements
- Singularity available on compute nodes
- GPUs allocatable via `--gres=gpu:N`
- `sacct` available for job polling
- Optional separate transfer endpoint

## What's now diagnosable

Before this work, rsync failures produced only `"rsync failed"` with no
context. Now:

- **Exit code classification:** rsync exit codes mapped to categories
  (SSH connection failed, permission denied, disk full, timeout, partial
  transfer, etc.)
- **Stderr capture:** Last 500 chars included in error messages
- **File path context:** Local path, remote path, direction, host:port
- **Transfer timing:** Elapsed time logged for success and failure
- **Pre-flight validation:** SSH, rsync, Docker/SLURM, GPU, storage, disk
  checked before any data transfer
- **Non-retryable classification:** Config errors (bad key, wrong host,
  permissions) marked non-retryable
- **Error aggregation:** Multi-file transfers report per-file failure context
- **Docker execution errors:** Remote Docker failures include exit code,
  image name, command, stderr

## What remains out of scope

- **Globus transfer** — not v0
- **MinIO/S3 as primary path** — secondary mode, not UCSF target
- **Incremental/resumable rsync** — v0 uses full-sync
- **Multi-hop/bastion SSH** — assumes direct reachability
- **GPU scheduling/queuing on workstation** — single-user model, no queue
- **BWB DAG integration for SSH+Docker** — `RemoteDockerWorkflow` handles
  single-command jobs; wiring SSH+Docker into the multi-node BWB DAG
  executor (`AbstractExecutor` subclass) is a follow-on
- **Workflow IR transport hints** — config stays in executor config
- **Bandwidth throttling** — rsync runs without caps
