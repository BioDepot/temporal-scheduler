"""SSH + Docker remote execution activity.

Supports GPU workstations reachable over SSH that run Docker (with NVIDIA
runtime) but do not have SLURM or Singularity.  This is the honest v0 UCSF
runtime path for remote CellBender.

Lifecycle for a single job:
  1. validate_connectivity  — pre-flight SSH/rsync/Docker/GPU checks
  2. setup_remote_volumes   — mkdir staging dirs on remote
  3. upload_inputs           — rsync input files to remote work dir
  4. run_remote_docker       — SSH exec: docker run --gpus … on remote
  5. download_outputs        — rsync output files back to local

All file transfer uses subprocess rsync (not paramiko), matching the
existing SLURM rsync path.  SSH commands also use subprocess so no
long-running paramiko connection is needed.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.executors.slurm_activities import classify_rsync_error


@dataclass
class SshDockerConfig:
    """Configuration for an SSH+Docker remote target."""
    ip_addr: str
    user: str
    storage_dir: str          # Remote staging root (e.g. /home/lhhung/ucsf_remote_cellbender)
    ssh_port: int = 22
    gpu_device: str = ""             # Empty = --gpus all; "0" or "0,1" = --gpus device=0,1


@dataclass
class SshDockerRunParams:
    """Parameters for a single remote Docker execution."""
    image: str                              # Docker image (e.g. biodepot/cellbender:0.3.2)
    cmd: List[str]                          # Command + args
    work_dir: str                           # Remote working directory (absolute)
    input_files: Dict[str, str]             # local_path -> remote_path
    output_dir: str                         # Remote output directory to rsync back
    local_output_dir: str                   # Local destination for outputs
    use_gpu: bool = True
    gpu_device: str = ""                    # Override per-job GPU pinning
    env: Optional[Dict[str, str]] = None    # Extra env vars for the container
    docker_volumes: Optional[Dict[str, str]] = None  # Extra host:container volume mounts
    timeout_seconds: int = 7200             # Max wall time for docker run


class SshDockerActivity:
    """Activity implementation for SSH+Docker remote execution with rsync staging."""

    def __init__(self, config: SshDockerConfig):
        self.config = config
        self.rsync_semaphore = asyncio.Semaphore(5)

    def _ssh_cmd_prefix(self) -> List[str]:
        parts = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
        ]
        if self.config.ssh_port != 22:
            parts.extend(["-p", str(self.config.ssh_port)])
        parts.append(f"{self.config.user}@{self.config.ip_addr}")
        return parts

    def _ssh_transport_flag(self) -> str:
        if self.config.ssh_port == 22:
            return "ssh"
        return f"ssh -p {self.config.ssh_port}"

    async def _exec_ssh(self, remote_cmd: str, timeout: int = 30) -> tuple:
        """Execute a command on the remote host. Returns (returncode, stdout, stderr)."""
        full_cmd = self._ssh_cmd_prefix() + [remote_cmd]
        process = await asyncio.create_subprocess_exec(
            *full_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            return (-1, "", f"SSH command timed out after {timeout}s: {remote_cmd}")
        return (
            process.returncode,
            stdout.decode("utf-8"),
            stderr.decode("utf-8"),
        )

    async def _rsync(self, local_path: str, remote_path: str, upload: bool):
        """rsync a file/directory with structured error reporting."""
        remote_full = f"{self.config.user}@{self.config.ip_addr}:{remote_path}"
        ssh_flag = self._ssh_transport_flag()
        direction = "upload" if upload else "download"

        if upload:
            # Ensure remote parent dir exists
            remote_parent = os.path.dirname(remote_path)
            await self._exec_ssh(f"mkdir -p {remote_parent}")
            rsync_cmd = f"rsync -az --stats -e '{ssh_flag}' {local_path} {remote_full}"
        else:
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            rsync_cmd = f"rsync -az --stats -e '{ssh_flag}' {remote_full} {local_path}"

        print(f"ssh-docker rsync {direction}: {rsync_cmd}")
        t0 = time.monotonic()

        async def _run():
            proc = await asyncio.create_subprocess_shell(
                rsync_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await proc.communicate()
            return proc.returncode, out.decode("utf-8"), err.decode("utf-8")

        if upload:
            rc, stdout, stderr = await _run()
        else:
            async with self.rsync_semaphore:
                rc, stdout, stderr = await _run()

        elapsed = time.monotonic() - t0

        if rc != 0:
            classification = classify_rsync_error(rc, stderr)
            stderr_tail = stderr[-500:] if len(stderr) > 500 else stderr
            error_msg = (
                f"rsync {direction} failed: {classification}\n"
                f"  exit_code={rc}\n"
                f"  local_path={local_path}\n"
                f"  remote_path={remote_path}\n"
                f"  remote_host={self.config.ip_addr}:{self.config.ssh_port}\n"
                f"  elapsed={elapsed:.1f}s\n"
                f"  stderr={stderr_tail.strip()}"
            )
            print(error_msg)
            raise ApplicationError(
                error_msg,
                non_retryable=(rc in (1, 3, 5, 255)),
            )

        print(f"ssh-docker rsync {direction} ok: {local_path} elapsed={elapsed:.1f}s")

    # ------------------------------------------------------------------
    # Activities
    # ------------------------------------------------------------------

    @activity.defn
    async def validate_connectivity(self, remote_storage_dir: str) -> str:
        """Pre-flight: check SSH, rsync, Docker, GPU availability."""
        remote_storage_dir = remote_storage_dir or self.config.storage_dir
        checks = []

        # 1. SSH
        rc, out, err = await self._exec_ssh("echo __ssh_ok__")
        if rc != 0 or "__ssh_ok__" not in out:
            raise ApplicationError(
                f"SSH check failed: {self.config.user}@{self.config.ip_addr}:"
                f"{self.config.ssh_port} rc={rc} err={err[:300]}",
                non_retryable=True,
            )
        checks.append("ssh")

        # 2. rsync reachability
        ssh_flag = self._ssh_transport_flag()
        rsync_probe = (
            f"rsync --dry-run -e '{ssh_flag}' "
            f"{self.config.user}@{self.config.ip_addr}:/dev/null /dev/null"
        )
        proc = await asyncio.create_subprocess_shell(
            rsync_probe,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode not in (0, 23):
            raise ApplicationError(
                f"rsync check failed: rc={proc.returncode} "
                f"err={stderr.decode()[:300]}",
                non_retryable=True,
            )
        checks.append("rsync")

        # 3. Docker available
        rc, out, err = await self._exec_ssh("docker info --format '{{.ID}}'")
        if rc != 0:
            raise ApplicationError(
                f"Docker not available on remote: {err[:300]}",
                non_retryable=True,
            )
        checks.append("docker")

        # 4. NVIDIA runtime / GPU
        rc, out, err = await self._exec_ssh(
            "docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi "
            "--query-gpu=name,memory.total --format=csv,noheader 2>/dev/null "
            "|| nvidia-smi --query-gpu=name,memory.total --format=csv,noheader",
            timeout=60,
        )
        if rc != 0:
            raise ApplicationError(
                f"GPU check failed on remote: {err[:300]}",
                non_retryable=True,
            )
        gpu_info = out.strip()
        checks.append(f"gpu=[{gpu_info}]")

        # 5. Remote storage writable
        probe = os.path.join(remote_storage_dir, ".scheduler_probe")
        rc, out, err = await self._exec_ssh(
            f"mkdir -p {remote_storage_dir} && touch {probe} && rm -f {probe}"
        )
        if rc != 0:
            raise ApplicationError(
                f"Remote storage {remote_storage_dir} not writable: {err[:300]}",
                non_retryable=True,
            )
        checks.append("storage_writable")

        # 6. Disk space
        rc, out, err = await self._exec_ssh(
            f"df -BG {remote_storage_dir} | tail -1 | awk '{{print $4}}'"
        )
        if rc == 0 and out.strip():
            try:
                avail_gb = int(out.strip().replace("G", ""))
                if avail_gb < 10:
                    print(f"WARNING: only {avail_gb}GB free on remote")
                checks.append(f"disk={avail_gb}GB")
            except ValueError:
                checks.append("disk=unknown")

        status = f"pre-flight ok: {', '.join(checks)}"
        print(status)
        return status

    @activity.defn
    async def setup_remote_volumes(self, job_id: str) -> Dict[str, str]:
        """Create remote staging directories for a job. Returns path dict."""
        base = os.path.join(self.config.storage_dir, job_id)
        work_dir = os.path.join(base, "work")
        output_dir = os.path.join(work_dir, "output")

        dirs_cmd = f"mkdir -p {work_dir} {output_dir}"
        rc, _, err = await self._exec_ssh(dirs_cmd)
        if rc != 0:
            raise ApplicationError(
                f"Failed creating remote dirs: {err[:300]}",
                non_retryable=True,
            )

        return {
            "base": base,
            "work": work_dir,
            "output": output_dir,
        }

    @activity.defn
    async def upload_inputs(self, file_map: Dict[str, str]) -> int:
        """rsync local files to remote. file_map: {local_path: remote_path}.

        Returns the number of files transferred.
        """
        errors = []
        for local_path, remote_path in file_map.items():
            try:
                await self._rsync(local_path, remote_path, upload=True)
            except ApplicationError as e:
                errors.append(f"  {local_path} -> {remote_path}: {e}")

        if errors:
            raise ApplicationError(
                f"upload_inputs failed for {len(errors)}/{len(file_map)} file(s):\n"
                + "\n".join(errors)
            )
        return len(file_map)

    @activity.defn
    async def run_remote_docker(self, params: SshDockerRunParams) -> str:
        """Execute a Docker container on the remote host over SSH.

        Returns the combined stdout+stderr of the container run.
        Raises ApplicationError on failure with diagnosable context.
        """
        docker_args = ["run", "--rm"]
        docker_args.extend(["--user", '"$(id -u):$(id -g)"'])
        docker_args.extend(["-v", f"{params.work_dir}:{params.work_dir}"])
        docker_args.extend(["-w", params.work_dir])

        # GPU flags
        gpu_device = params.gpu_device or self.config.gpu_device
        if params.use_gpu:
            if gpu_device:
                docker_args.extend(["--gpus", f'"device={gpu_device}"'])
            else:
                docker_args.extend(["--gpus", "all"])

        # Extra env vars
        if params.env:
            for k, v in params.env.items():
                docker_args.extend(["-e", f"{k}={v}"])

        # Extra volume mounts
        if params.docker_volumes:
            for host_path, container_path in params.docker_volumes.items():
                docker_args.extend(["-v", f"{host_path}:{container_path}"])

        # Image and command
        docker_args.append(params.image)
        docker_args.extend(params.cmd)

        full_docker_cmd = "docker " + " ".join(docker_args)
        print(f"ssh-docker run: {full_docker_cmd}")

        t0 = time.monotonic()
        rc, stdout, stderr = await self._exec_ssh(
            full_docker_cmd,
            timeout=params.timeout_seconds,
        )
        elapsed = time.monotonic() - t0

        if rc == -1 and "timed out" in stderr:
            raise ApplicationError(
                f"Remote Docker execution timed out after {params.timeout_seconds}s\n"
                f"  image={params.image}\n"
                f"  cmd={' '.join(params.cmd)}\n"
                f"  remote_host={self.config.ip_addr}",
                non_retryable=False,
            )

        if rc != 0:
            stderr_tail = stderr[-1000:] if len(stderr) > 1000 else stderr
            stdout_tail = stdout[-500:] if len(stdout) > 500 else stdout
            raise ApplicationError(
                f"Remote Docker execution failed (exit_code={rc})\n"
                f"  image={params.image}\n"
                f"  cmd={' '.join(params.cmd)}\n"
                f"  remote_host={self.config.ip_addr}\n"
                f"  elapsed={elapsed:.1f}s\n"
                f"  stdout_tail={stdout_tail.strip()}\n"
                f"  stderr={stderr_tail.strip()}",
                non_retryable=False,
            )

        print(f"ssh-docker run ok: elapsed={elapsed:.1f}s")
        return stdout + stderr

    @activity.defn
    async def download_outputs(self, params: List[str]) -> int:
        """rsync an entire remote directory back to local.

        params: [remote_dir, local_dir]
        Returns number of top-level items synced.
        """
        remote_dir, local_dir = params[0], params[1]
        # Trailing slash on remote means "contents of"
        remote_src = remote_dir.rstrip("/") + "/"
        local_dst = local_dir.rstrip("/") + "/"
        os.makedirs(local_dst, exist_ok=True)

        await self._rsync(local_dst, remote_src, upload=False)

        # Count what we got
        items = os.listdir(local_dst) if os.path.isdir(local_dst) else []
        print(f"ssh-docker download: {len(items)} items in {local_dst}")
        return len(items)

    @activity.defn
    async def cleanup_remote(self, remote_dir: str) -> None:
        """Remove a remote staging directory after successful completion."""
        rc, _, err = await self._exec_ssh(f"rm -rf {remote_dir}")
        if rc != 0:
            print(f"WARNING: cleanup of {remote_dir} failed: {err[:200]}")
