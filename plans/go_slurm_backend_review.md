# Go SLURM Backend Extraction — Review Summary

> Branch: `feature/go-slurm-scheduler`
> Commits: 3f59320, 0acec30, 61cc75a
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
  server/server.go           HTTP handlers for all 6 endpoints
  server/server_test.go
  main.go                    entry point (-addr flag, default :8765)
  go.mod / go.sum

bwb/scheduling_service/executors/
  slurm_activities_go_shim.py   GoSlurmActivity — new Python shim
  slurm_activities.py           original Paramiko implementation (unchanged)

bwb/scheduling_service/
  worker.py                  updated to select Go vs Paramiko at startup
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

Every request includes `ssh_config` in the body; the handler opens a fresh
`SSHExecutor`, calls the backend function, closes the connection, and returns
JSON.  Errors are returned as `{"error": "..."}` with HTTP 200 (the caller
inspects the field).

`main.go` starts the server on `-addr` (default `:8765`).

---

## 5. Phase 3 — Python Shim

### `GoSlurmActivity` (`slurm_activities_go_shim.py`)

Drop-in replacement for `SlurmActivity`.  Same six `@activity.defn` method
names; no Paramiko dependency.  Uses `requests.post` wrapped in
`asyncio.to_thread`.

Constructor reads SSH config from the slurm config dict.  Backend URL
precedence: `slurm_config["go_backend_url"]` → `GO_SLURM_BACKEND_URL` env
var → `http://localhost:8765`.

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

### `worker.py` change

`get_slurm_worker` now checks for `go_backend_url` in the config or
`GO_SLURM_BACKEND_URL` in the environment.  If either is present, it
instantiates `GoSlurmActivity` and skips all Paramiko code.  Otherwise it
falls through to the original Paramiko path unchanged.

---

## 6. Tests

```
go test ./...   (all pass)

go-slurm-backend/sbatch       — 4 tests: directives, job config overrides,
                                 GPU flag, RandID uniqueness
go-slurm-backend/polling      — 6 tests: sacct parsing, field mapping,
                                 .batch/.extern filtering, malformed records,
                                 AwaitFileExistence (live-sleep skipped)
go-slurm-backend/server       — 4 tests: healthz, malformed JSON → 400,
                                 unknown route → 404, Content-Type header
```

No Python unit tests were added in this PR; the shim is designed to be
exercised by the Phase 4 E2E harnesses.

---

## 7. Activation

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

The Paramiko path is still active for any worker that does not set either key.

---

## 8. Known Gaps / Phase 4

- **E2E validation not yet run.**  The existing harnesses should be the first
  test:
  - `scripts/run_local_slurm_e2e.sh`
  - `scripts/run_salmon_ir_local_slurm_e2e.sh`
  - `scripts/run_arabidopsis_conditional_local_slurm_e2e.sh`
- **Connection pooling.** The server opens one `SSHExecutor` per HTTP request.
  For `poll_slurm` (called every 5 s) this is inefficient.  A per-config
  connection cache would help.
- **Docker Compose wiring.** The `go-slurm-backend` binary is not yet
  built into the Docker image or added to `docker-compose.yml`.
- **Python unit tests** for the shim (mock HTTP server asserting correct
  payloads) would complete the test coverage story.
- **`cmd_prefix` plumbing in the shim.** `SshConfig.cmd_prefix` is passed
  through to the Go backend but is not yet wired in `GoSlurmActivity`'s
  SSH config dict.  Needs one-line fix before use on clusters that require a
  wrapper (e.g. `docker exec slurmdbd`).

---

## 9. Files Changed vs Master

| File | Change |
|------|--------|
| `go_slurm_backend/` (15 files, ~1 750 lines) | new |
| `bwb/scheduling_service/executors/slurm_activities_go_shim.py` | new |
| `bwb/scheduling_service/worker.py` | updated `get_slurm_worker` |
| `bwb/scheduling_service/executors/slurm_activities.py` | unchanged |
| `bwb/scheduling_service/executors/slurm_executor.py` | unchanged |
| `bwb/scheduling_service/executors/slurm_poller.py` | unchanged |
| `bwb/scheduling_service/bwb_workflow.py` | unchanged |
