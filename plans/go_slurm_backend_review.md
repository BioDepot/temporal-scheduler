# Go SLURM Backend Extraction — Review Summary

> Branch: `feature/go-slurm-scheduler`
> Date: 2026-04-06

## 1. What Was Built

A Go HTTP sidecar (`go-slurm-backend`) that takes over the SLURM execution
path from the Python scheduler, and a Python shim that routes existing
Temporal activities through it.

The Python Temporal scheduler is unchanged at the workflow/orchestration
layer.  Only the SLURM backend operations are re-implemented.

---

## 2. Repository Layout

```
go_slurm_backend/
  api/types.go               shared request/response types
  sshutil/auth.go            SSH agent + key-scan auth
  sshutil/exec.go            SSHExecutor (persistent conn, auto-reconnect)
  rsyncfs/ssh_rsync.go       rsync-over-SSH upload/download
  sbatch/container_cmd.go    FormSingularityCmd / FormSingularityCmdPrefix
  sbatch/writer.go           WriteSbatchFile, RandID
  sbatch/writer_test.go
  polling/sacct.go           RunSacct, AwaitFileExistence, JOB_CODES
  polling/jobs.go            StartJob, GetOutputs, processRawCmdOutputs
  polling/sacct_test.go
  server/server.go           HTTP handlers for all 6 endpoints + connection pool
  server/server_test.go
  main.go                    entry point (-addr flag, default :8765)
  go.mod / go.sum

deployment/
  Dockerfile.go-slurm-backend   multi-stage Go 1.22 build → alpine runtime
  docker-compose.yml             go-slurm-backend service (host networking)

scripts/
  setup_local_slurm_host.sh     refreshes known_hosts on cluster restart
  local_slurm_test_env_up.sh    auto-shifts GO_SLURM_HOST_PORT on conflict
  run_local_docker_slurm_worker.sh  passes GO_SLURM_BACKEND_URL to worker

bwb/scheduling_service/executors/
  slurm_activities_go_shim.py   GoSlurmActivity — Python shim
  slurm_activities.py           original Paramiko implementation (unchanged)

bwb/scheduling_service/
  worker.py                  updated to select Go vs Paramiko at startup

tests/
  test_go_slurm_shim.py     12 unit tests for GoSlurmActivity

bwb_scheduler/
  benchmark_harness.py       added progress logging to poll loop
```

---

## 3. Phase 1 — Go Package Extraction

Source: extracted from `/mnt/pikachu/bwb_scheduler` (old Go scheduler).

### SSH (`sshutil/`)

`auth.go` — `BuildAuthMethods()` tries SSH agent first (`SSH_AUTH_SOCK`),
then scans `~/.ssh/` for unencrypted private keys.  `GetHostKeyCallback()`
reads `~/.ssh/known_hosts`.

`exec.go` — `SSHExecutor` wraps a persistent `*ssh.Client`.  On session
failure it resets and reconnects once.  The original code had a latent
deadlock (held `RLock`, called `Close` which needed a write lock); fixed
by using a single `sync.Mutex` throughout.

### rsync (`rsyncfs/`)

`ssh_rsync.go` — `Upload` and `Download` run rsync via `sh -c`.  Uses
`transfer_addr` when set (NSF cluster pattern), falls back to `ip_addr`.
Port is configurable; defaults to 22.

**Phase 4 fix:** removed dead `bytes.Buffer` assignments from `rsyncCmd`
that caused `exec: Stdout already set` errors when callers used
`CombinedOutput()`.

### sbatch (`sbatch/`)

`container_cmd.go` — `FormSingularityCmd` / `FormSingularityCmdPrefix`
build a Singularity `exec` invocation with bind mounts and
`SINGULARITYENV_*` env vars.  Extracted verbatim from
`bwb_scheduler/parsing/parse_workflow_cmds.go`.

`writer.go` — `WriteSbatchFile` writes the `#!/bin/bash` + `#SBATCH`
directives from `SlurmJobConfig`, then either the pre-built `RawCmd` (if
set) or the output of `FormSingularityCmd`.  Supports GPU via
`#SBATCH --gpus` — a gap that the Python code marks `# TODO`.

### polling (`polling/`)

`sacct.go` — `RunSacct` queries `sacct -j ... -o JobID,State,ExitCode -n -P`,
parses the pipe-delimited output, skips `.batch` and `.extern` step records,
and annotates each result with `Done / Failed / Fatal` from `JOB_CODES`.
`AwaitFileExistence` retries once after 5 s to absorb LUSTRE propagation lag.

`jobs.go` — `StartJob` creates a remote tmp dir, writes a local sbatch
script, rsync-uploads it, runs `sbatch --parsable`, and returns a `SlurmJob`
descriptor.  `GetOutputs` reads stdout/stderr files and all files under
`TmpOutputHostPath`, runs them through `processRawCmdOutputs`, then
removes the remote files.  Both functions tolerate empty path fields (for
failed-job partial reads).

---

## 4. Phase 2 — HTTP Server

`server/server.go` registers seven routes on `http.NewServeMux`:

| Method | Path | Go function |
|--------|------|-------------|
| GET | `/healthz` | returns `{"status":"ok"}` |
| POST | `/setup_login_node_volumes` | SSH `mkdir -p` for each dir in `Dirs[]` |
| POST | `/upload_to_slurm_login_node` | `rsyncfs.Upload` |
| POST | `/start_slurm_job` | mkdir `ExtraDirs`, then `polling.StartJob` |
| POST | `/poll_slurm` | `polling.RunSacct`; empty `job_ids` sends keepalive |
| POST | `/get_slurm_outputs` | `polling.GetOutputs` for each job |
| POST | `/download_from_slurm_login_node` | `rsyncfs.Download` |

Errors are returned as `{"error": "..."}` with HTTP 200 (the caller
inspects the field).

`main.go` starts the server on `-addr` (default `:8765`).

---

## 5. Phase 3 — Python Shim

### `GoSlurmActivity` (`slurm_activities_go_shim.py`)

Drop-in replacement for `SlurmActivity`.  Same six `@activity.defn` method
names; no Paramiko dependency.  Uses `requests.post` wrapped in
`asyncio.to_thread`.

Backend URL precedence: `GO_SLURM_BACKEND_URL` env var →
`slurm_config["go_backend_url"]` → `http://localhost:8765`.  Env var
takes priority so the port-shifting logic in test harnesses works even
when the config JSON hard-codes a port.

#### Activity mapping

| Temporal activity | Shim behaviour |
|-------------------|---------------|
| `setup_login_node_volumes` | Computes the standard dir list (`workflow_dir`, `slurm/`, `images/`, `tmp/`, volume dirs), POSTs to `/setup_login_node_volumes`, returns the volumes dict |
| `start_slurm_job` | Creates per-job `cnt_name`, updates `/tmp` volume, calls `get_container_cmd` to build the full Singularity command, POSTs to `/start_slurm_job` with `raw_cmd` and `extra_dirs=[output_dir]`; returns `SlurmCmdObj{tmp_dir=volumes["/tmp"]}` |
| `poll_slurm` | Extracts job IDs, POSTs to `/poll_slurm`, maps `SacctResult` → `SlurmCmdResult`; double-confirms COMPLETED (same logic as old code) |
| `get_slurm_outputs` | For failed jobs: reads only `err_path`. For succeeded jobs: reads stdout + `tmp_dir/output/*` via `/get_slurm_outputs` |
| `upload_to_slurm_login_node` | Fans out one POST per file + SIF via `asyncio.gather` |
| `download_from_slurm_login_node` | Same pattern for downloads |

#### `RawCmd` design note

The Python command builder (`get_container_cmd`) and the Go `FormSingularityCmd`
produce slightly different singularity invocations (Python uses
`--writable-tmpfs --pwd /`; Go uses `--containall --cleanenv`).  Rather than
change command formatting, the shim passes the Python-built command as
`raw_cmd` to the Go sbatch writer, which writes it verbatim.  The Go path
using `FormSingularityCmd` is preserved for future callers that supply
structured `SlurmCmdTemplate` input.

#### `cmd_prefix` support

Fully plumbed: `slurm_config["cmd_prefix"]` → Python shim `_ssh_config` →
Go `SshConfig.CmdPrefix` → prepended to every SSH command in `SSHExecutor.Exec`.
Used on clusters requiring a wrapper (e.g. `docker exec slurmdbd`).

### `worker.py` change

`get_slurm_worker` now checks for `go_backend_url` in the config or
`GO_SLURM_BACKEND_URL` in the environment.  If either is present, it
instantiates `GoSlurmActivity` and skips all Paramiko code.  Otherwise it
falls through to the original Paramiko path unchanged.

---

## 6. Phase 4 — E2E Validation & Integration Fixes

### Bugs discovered and fixed during E2E testing

| Bug | Root cause | Fix |
|-----|-----------|-----|
| `rsyncfs: "Stdout already set"` | `rsyncCmd` pre-set `cmd.Stdout`/`cmd.Stderr` but callers used `CombinedOutput()` | Removed dead buffer assignments from `rsyncCmd` |
| Go backend can't reach SLURM at localhost:3022 | Container was on bridge network; `localhost` resolved to its own loopback | Switched to `network_mode: host` in docker-compose |
| Go backend can't find SIF images for rsync upload | `SCHED_STORAGE_DIR` not mounted into the sidecar | Added `${SCHED_STORAGE_DIR}:/data/sched_storage` volume |
| `knownhosts: key mismatch` on cluster restart | SLURM container regenerates SSH host keys; stale `known_hosts` entry remains | `setup_local_slurm_host.sh` now runs `ssh-keygen -R` + `ssh-keyscan` |
| `GO_SLURM_BACKEND_URL` env var ignored | Config JSON `go_backend_url` took precedence; port-shifting had no effect | Swapped URL resolution priority: env var first |
| Benchmark harness silent during runs | `poll_until_terminal` printed nothing; impossible to tell if workflow submitted | Added progress logging: workflow ID on submit, status+nodes on each poll tick |

### E2E results — all passing

| Test harness | Workflow | Result | Time |
|-------------|----------|--------|------|
| `test_scheme_local_docker_slurm` | 6-node salmon demo | Finished | 70 s |
| `salmon-IR` | conditional workflow (IR decoded) | Finished | 70 s |
| `arabidopsis-conditional` | 3-node with salmon index build | Finished | 90 s |

### Port conflict auto-resolution

`local_slurm_test_env_up.sh` now detects if port 8765 is already in use
(e.g. by another service) and shifts `GO_SLURM_HOST_PORT` to the next free
port in 8765–8865.  The shifted port is:

- Persisted to `deployment/.env` as `GO_SLURM_HOST_PORT`
- Passed to docker-compose via the `-addr` flag on the Go binary
- Passed to the Python worker container as `GO_SLURM_BACKEND_URL`

---

## 7. Tests

### Go unit tests (`go test ./...`)

```
go-slurm-backend/sbatch       — 4 tests: directives, job config overrides,
                                 GPU flag, RandID uniqueness
go-slurm-backend/polling      — 6 tests: sacct parsing, field mapping,
                                 .batch/.extern filtering, malformed records,
                                 AwaitFileExistence (live-sleep skipped)
go-slurm-backend/server       — 4 tests: healthz, malformed JSON → 400,
                                 unknown route → 404, Content-Type header
```

### Python shim unit tests (`tests/test_go_slurm_shim.py`)

12 tests using `unittest.mock.patch` on `requests.post`:

- `setup_login_node_volumes`: dirs check, error propagation
- `start_slurm_job`: raw_cmd set, extra_dirs, job_config, tmp_dir mapping
- `poll_slurm`: done, running, empty, failed
- `get_slurm_outputs`: success path (tmp_output_host_path), failed path
- `upload_to_slurm_login_node`: count per file, elideable skip
- `download_from_slurm_login_node`: path mapping

---

## 8. Activation

To route a SLURM worker through the Go backend, start the sidecar:

```bash
cd go_slurm_backend
go build -o go-slurm-backend .
./go-slurm-backend -addr :8765
```

Then add `"go_backend_url": "http://localhost:8765"` to the executor's
SLURM config JSON, or set `GO_SLURM_BACKEND_URL=http://localhost:8765`
before starting the Python worker:

```bash
python bwb/scheduling_service/worker.py slurm --config slurm_config.json
```

For the full Docker Compose stack:

```bash
bash scripts/local_slurm_test_env_up.sh
```

This builds both images (`bwb-scheduler` and `go-slurm-backend`), starts
all services, auto-shifts the Go backend port if needed, and waits for the
scheduler API and Temporal namespace to be ready.

The Paramiko path is still active for any worker that does not set either key.

---

## 9. Remaining Work

- **Connection pooling.** The server opens one `SSHExecutor` per HTTP request.
  For `poll_slurm` (called every 5 s) this is inefficient.  A per-config
  connection cache would reduce SSH handshake overhead.

---

## 10. Files Changed vs Master

| File | Change |
|------|--------|
| `go_slurm_backend/` (15 files, ~1 750 lines) | new |
| `deployment/Dockerfile.go-slurm-backend` | new |
| `deployment/docker-compose.yml` | added `go-slurm-backend` service |
| `deployment/.env.example` | added `GO_SLURM_HOST_PORT` |
| `bwb/scheduling_service/executors/slurm_activities_go_shim.py` | new |
| `bwb/scheduling_service/worker.py` | updated `get_slurm_worker` |
| `bwb/scheduling_service/test_workflows/*_slurm_config.json` (×2) | added `go_backend_url` |
| `tests/test_go_slurm_shim.py` | new (12 tests) |
| `scripts/setup_local_slurm_host.sh` | added known_hosts refresh |
| `scripts/local_slurm_test_env_up.sh` | added Go image build + port shift |
| `scripts/run_local_docker_slurm_worker.sh` | added `GO_SLURM_BACKEND_URL` passthrough |
| `bwb_scheduler/benchmark_harness.py` | added poll loop progress logging |
| `pyproject.toml` | added `requests` dependency |
| `poetry.lock` | regenerated |
