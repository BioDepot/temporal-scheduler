"""Temporal workflow for remote Docker execution over SSH.

Sequences the SshDockerActivity methods into a single workflow that can be
started via the Temporal client.  Works for any Docker image that reads
inputs from a work directory and writes outputs to a subdirectory.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

from bwb.scheduling_service.executors.ssh_docker_activity import (
    SshDockerActivity,
    SshDockerRunParams,
)


@dataclass
class RemoteDockerJobParams:
    """Parameters to start a remote Docker job."""
    image: str                              # Docker image
    cmd: List[str]                          # Command + args
    input_files: Dict[str, str]             # local_path -> remote_relative_path
    use_gpu: bool = True
    gpu_device: str = ""
    env: Optional[Dict[str, str]] = None
    timeout_seconds: int = 7200
    local_output_dir: str = ""              # Where to put results locally
    cleanup: bool = True                    # Remove remote staging after


@dataclass
class RemoteDockerJobResult:
    """Result of a completed remote Docker job."""
    success: bool
    job_id: str
    logs: str
    output_item_count: int
    local_output_dir: str
    error: str = ""


@workflow.defn
class RemoteDockerWorkflow:
    """Run a single Docker container on a remote GPU host, with rsync staging."""

    @workflow.run
    async def run(self, params: RemoteDockerJobParams) -> RemoteDockerJobResult:
        job_id = f"rdjob_{uuid.uuid4().hex[:8]}"

        # 1. Pre-flight validation
        await workflow.execute_activity(
            SshDockerActivity.validate_connectivity,
            "",  # uses the activity instance's config.storage_dir
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        # 2. Create remote staging dirs
        paths = await workflow.execute_activity(
            SshDockerActivity.setup_remote_volumes,
            job_id,
            start_to_close_timeout=timedelta(seconds=30),
        )
        work_dir = paths["work"]
        output_dir = paths["output"]

        # 3. Upload inputs — remap local paths to remote work dir
        remote_file_map = {}
        for local_path, rel_path in params.input_files.items():
            import os
            remote_file_map[local_path] = os.path.join(work_dir, rel_path)

        if remote_file_map:
            await workflow.execute_activity(
                SshDockerActivity.upload_inputs,
                remote_file_map,
                start_to_close_timeout=timedelta(seconds=3600),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )

        # 4. Run Docker on remote host
        run_params = SshDockerRunParams(
            image=params.image,
            cmd=params.cmd,
            work_dir=work_dir,
            input_files={},
            output_dir=output_dir,
            local_output_dir=params.local_output_dir,
            use_gpu=params.use_gpu,
            gpu_device=params.gpu_device,
            env=params.env,
            timeout_seconds=params.timeout_seconds,
        )
        logs = await workflow.execute_activity(
            SshDockerActivity.run_remote_docker,
            run_params,
            start_to_close_timeout=timedelta(seconds=params.timeout_seconds + 60),
        )

        # 5. Download outputs
        n_items = await workflow.execute_activity(
            SshDockerActivity.download_outputs,
            [output_dir, params.local_output_dir],
            start_to_close_timeout=timedelta(seconds=3600),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        # 6. Cleanup
        if params.cleanup:
            await workflow.execute_activity(
                SshDockerActivity.cleanup_remote,
                paths["base"],
                start_to_close_timeout=timedelta(seconds=30),
            )

        return RemoteDockerJobResult(
            success=True,
            job_id=job_id,
            logs=logs,
            output_item_count=n_items,
            local_output_dir=params.local_output_dir,
        )
