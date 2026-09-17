"""Temporal workflow joining Globus, Slurm, and GPU execution stages."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from bwb.scheduling_service.executors.globus_activity import (
        GlobusActivity,
        GlobusTaskStatus,
        GlobusTransferParams,
    )
    from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
    from bwb.scheduling_service.executors.ssh_docker_workflow import (
        RemoteDockerJobParams,
        RemoteDockerJobResult,
        RemoteDockerWorkflow,
    )
    from bwb.scheduling_service.scheduler_types import (
        CmdOutput,
        SlurmCmdResult,
        SlurmScriptJobParams,
    )


@dataclass
class SlurmScriptStage:
    task_queue: str
    job: SlurmScriptJobParams
    poll_interval_seconds: int = 15
    timeout_seconds: int = 86400


@dataclass
class GpuDockerStage:
    task_queue: str
    job: RemoteDockerJobParams


@dataclass
class StagedSlurmGpuParams:
    globus_task_queue: str
    slurm: SlurmScriptStage
    gpu: GpuDockerStage
    stage_in: Optional[GlobusTransferParams] = None
    stage_back: Optional[GlobusTransferParams] = None
    publish: Optional[GlobusTransferParams] = None


@dataclass
class StagedSlurmGpuResult:
    success: bool
    transfer_task_ids: Dict[str, str]
    slurm_job_id: int
    slurm_status: str
    gpu_result: RemoteDockerJobResult


@workflow.defn(sandboxed=False)
class StagedSlurmGpuWorkflow:
    """Run endpoint transfer, Slurm, stage-back, and GPU work durably."""

    def __init__(self) -> None:
        self.status = {
            "workflow_status": "Started",
            "current_stage": "initializing",
            "stages": {},
            "artifacts": {},
        }

    @workflow.query
    def get_status(self) -> dict:
        return self.status

    def _set_stage(self, name: str, state: str, **details) -> None:
        self.status["current_stage"] = name
        self.status["stages"][name] = {"state": state, **details}

    async def _run_globus(
        self,
        name: str,
        params: GlobusTransferParams,
        task_queue: str,
    ) -> GlobusTaskStatus:
        self._set_stage(name, "SUBMITTING")
        task = await workflow.execute_activity(
            GlobusActivity.submit_transfer,
            params,
            task_queue=task_queue,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        self._set_stage(name, "RUNNING", task_id=task.task_id)
        self.status["artifacts"][f"{name}_task_id"] = task.task_id
        deadline = workflow.now() + timedelta(seconds=params.timeout_seconds)
        while True:
            task_status = await workflow.execute_activity(
                GlobusActivity.get_task_status,
                task.task_id,
                task_queue=task_queue,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            self._set_stage(
                name,
                task_status.status,
                task_id=task.task_id,
                files=task_status.files,
                files_transferred=task_status.files_transferred,
                bytes_transferred=task_status.bytes_transferred,
                faults=task_status.faults,
            )
            if task_status.status == "SUCCEEDED":
                return task_status
            if task_status.status in {"FAILED", "CANCELED", "EXPIRED"}:
                raise RuntimeError(
                    f"Globus stage {name} failed: status={task_status.status} "
                    f"faults={task_status.faults} fatal_error={task_status.fatal_error}"
                )
            if workflow.now() >= deadline:
                raise RuntimeError(
                    f"Globus stage {name} timed out after {params.timeout_seconds}s"
                )
            await workflow.sleep(max(1, params.poll_interval_seconds))

    async def _run_slurm(self, params: SlurmScriptStage) -> tuple[SlurmCmdResult, CmdOutput]:
        self._set_stage("slurm", "SUBMITTING")
        job = await workflow.execute_activity(
            SlurmActivity.start_slurm_script_job,
            params.job,
            task_queue=params.task_queue,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        self.status["artifacts"]["slurm_job_id"] = str(job.job_id)
        self._set_stage("slurm", "RUNNING", job_id=job.job_id)
        deadline = workflow.now() + timedelta(seconds=params.timeout_seconds)
        while True:
            results = await workflow.execute_activity(
                SlurmActivity.poll_slurm,
                {str(job.job_id): job},
                task_queue=params.task_queue,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            result = results.get(job.job_id) or results.get(str(job.job_id))
            if result is not None:
                outputs = await workflow.execute_activity(
                    SlurmActivity.get_slurm_outputs,
                    result,
                    task_queue=params.task_queue,
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                self._set_stage(
                    "slurm",
                    result.status,
                    job_id=job.job_id,
                    exit_code=result.exit_code,
                )
                if result.failed or not outputs.success:
                    raise RuntimeError(
                        f"Slurm job {job.job_id} failed: status={result.status} "
                        f"exit_code={result.exit_code} logs={outputs.logs[-2000:]}"
                    )
                return result, outputs
            if workflow.now() >= deadline:
                raise RuntimeError(
                    f"Slurm job {job.job_id} timed out after {params.timeout_seconds}s"
                )
            await workflow.sleep(max(1, params.poll_interval_seconds))

    @workflow.run
    async def run(self, params: StagedSlurmGpuParams) -> StagedSlurmGpuResult:
        transfer_ids: Dict[str, str] = {}
        if params.stage_in is not None:
            status = await self._run_globus("stage_in", params.stage_in, params.globus_task_queue)
            transfer_ids["stage_in"] = status.task_id

        slurm_result, _ = await self._run_slurm(params.slurm)

        if params.stage_back is not None:
            status = await self._run_globus("stage_back", params.stage_back, params.globus_task_queue)
            transfer_ids["stage_back"] = status.task_id

        self._set_stage("gpu", "RUNNING", task_queue=params.gpu.task_queue)
        child_id = f"{workflow.info().workflow_id}-gpu"
        gpu_result = await workflow.execute_child_workflow(
            RemoteDockerWorkflow.run,
            params.gpu.job,
            id=child_id,
            task_queue=params.gpu.task_queue,
            parent_close_policy=ParentClosePolicy.TERMINATE,
        )
        self._set_stage(
            "gpu",
            "SUCCEEDED",
            job_id=gpu_result.job_id,
            output_item_count=gpu_result.output_item_count,
            local_output_dir=gpu_result.local_output_dir,
        )

        if params.publish is not None:
            status = await self._run_globus("publish", params.publish, params.globus_task_queue)
            transfer_ids["publish"] = status.task_id

        self.status["workflow_status"] = "Finished"
        self.status["current_stage"] = "complete"
        return StagedSlurmGpuResult(
            success=True,
            transfer_task_ids=transfer_ids,
            slurm_job_id=slurm_result.job_id,
            slurm_status=slurm_result.status,
            gpu_result=gpu_result,
        )
