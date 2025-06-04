import asyncio
import copy
import math
import os
from collections import defaultdict
from datetime import timedelta
from typing import Optional, DefaultDict, Dict

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ChildWorkflowHandle, ParentClosePolicy

from bwb.scheduling_service.executors.abstract_executor import AbstractExecutor
from bwb.scheduling_service.executors.local_activities import setup_volumes_on_worker, download_file_deps, sync_dir
from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
from bwb.scheduling_service.executors.slurm_poller import SlurmPoller
from bwb.scheduling_service.scheduler_types import CmdOutput, ResourceVector, WorkerResourceAlloc, SlurmPollerState, \
    InterWorkflowSlurmCmdRequest, SlurmContainerCmdParams, SlurmSetupVolumesParams, SlurmFileUploadParams, \
    SlurmFileDownloadParams, CmdQueueId, SyncDirParams, ContainerCmdParams, DownloadFileDepsParams, SetupVolumesParams


class SlurmExecutor(AbstractExecutor):
    def __init__(self, slurm_config):
        super().__init__()
        if "storage_dir" not in slurm_config:
            raise ApplicationError(
                "`storage_dir` unspecified in slurm config",
                non_retryable=True
            )
        self.storage_dir = slurm_config["storage_dir"]

        if "ip_addr" not in slurm_config:
            raise ApplicationError(
                "`ip_addr` unspecified in slurm config",
                non_retryable=True
            )
        self.ip_addr = slurm_config["ip_addr"]

        if "user" not in slurm_config:
            raise ApplicationError(
                "`user` unspecified in slurm config",
                non_retryable=True
            )
        self.user = slurm_config["user"]

        self.local_volumes = None
        self.remote_volumes = None
        self.child_workflow: Optional[ChildWorkflowHandle] = None

        # (Node ID, Cmd Queue ID) -> event that gets set when result is
        # available from polling. The main workflow will call a method to
        # set this once it gets a corresponding signal from the polling
        # child workflow. (I don't know of any better way to encapsulate
        # this.)
        self.cmd_result_event: DefaultDict[int, Dict[CmdQueueId, asyncio.Event]] = defaultdict(dict)
        # Same keys, just storing the results from signal to main workflow.
        self.cmd_results: DefaultDict[int, Dict[CmdQueueId, CmdOutput]] = defaultdict(dict)

        self.resource_reqs: DefaultDict[int, Dict[CmdQueueId, ResourceVector]] = defaultdict(dict)
        self.volumes_setup = False

    def child_workflow_is_started(self):
        return self.child_workflow is not None

    async def shutdown_child(self):
        if self.child_workflow is not None:
            self.child_workflow.cancel()
            await self.child_workflow

    def pass_resource_alloc(self, node_id: int, cmd_queue_id: CmdQueueId, alloc: WorkerResourceAlloc) -> None:
        pass

    def get_queue_id(self) -> str:
        return f"{self.user}@{self.ip_addr}"

    async def ensure_child_workflow_is_initiated(self):
        if self.child_workflow is None:
            queue_name = f"{self.user}@{self.ip_addr}"
            initial_poller_state = SlurmPollerState(
                self.ip_addr,
                self.user,
                {}, {}, {}
            )
            self.child_workflow: Optional[ChildWorkflowHandle] = await workflow.start_child_workflow(
                SlurmPoller.run,
                initial_poller_state,
                task_queue="scheduler-queue",
                parent_close_policy=ParentClosePolicy.TERMINATE
            )

    def pass_cmd_result(self, node_id: int, cmd_queue_id: CmdQueueId, cmd_result: CmdOutput) -> None:
        self.cmd_result_event[node_id][cmd_queue_id].set()
        self.cmd_results[node_id][cmd_queue_id] = cmd_result

    async def run_cmd(
        self, params: ContainerCmdParams, node_id: int, cmd_queue_id: CmdQueueId, queue_id: str
    ) -> CmdOutput:
        assert node_id in self.resource_reqs
        assert cmd_queue_id in self.resource_reqs[node_id]

        await self.ensure_child_workflow_is_initiated()
        image_dir = os.path.join(self.storage_dir, "images")
        cmd_params = SlurmContainerCmdParams(
            params.cmd,
            params.cmd_files,
            self.resource_reqs[node_id][cmd_queue_id],
            params.config,
            params.use_singularity,
            self.remote_volumes,
            image_dir
        )
        slurm_cmd_req = InterWorkflowSlurmCmdRequest(
            node_id,
            cmd_queue_id,
            cmd_params
        )
        self.cmd_result_event[node_id][cmd_queue_id] = asyncio.Event()
        await self.child_workflow.signal(SlurmPoller.submit_slurm_cmd, slurm_cmd_req)
        await self.cmd_result_event[node_id][cmd_queue_id].wait()
        assert node_id in self.cmd_results and cmd_queue_id in self.cmd_results[node_id]

        cmd_result = self.cmd_results[node_id][cmd_queue_id]

        # Read outputs activity doesn't have cmd files, so I just
        # do this here for now. TODO: Refactor.
        output_files = params.cmd_files.output_files
        for out_key, out_val in cmd_result.outputs.items():
            if out_key in output_files:
                if isinstance(out_val, list):
                    output_files[out_key] |= set(out_val)
                else:
                    output_files[out_key].add(out_val)
        cmd_result.output_files = output_files
        return cmd_result

    async def setup_volumes(self, params: SetupVolumesParams, queue_id: str):
        if not self.volumes_setup:
            print(f"Setting up volumes w/ queue_id {queue_id}")
            self.local_volumes = await workflow.execute_activity(
                setup_volumes_on_worker,
                params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=10000),
            )

            slurm_params = SlurmSetupVolumesParams(params.bucket_id, self.storage_dir)
            self.remote_volumes = await workflow.execute_activity(
                SlurmActivity.setup_login_node_volumes,
                slurm_params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=10000)
            )
            self.volumes_setup = True

    async def request_resource_allocation(
        self, req: ResourceVector, node_id: int, cmd_queue_id: CmdQueueId
    ) -> WorkerResourceAlloc:
        # Slurm workers will be instantiated with an SSH connection,
        # so the queue name must be uniquely determined by those
        updated_resource_req = ResourceVector(
            req.cpus,
            req.gpus,
            math.ceil(req.mem_mb * 1.2)
        )

        # This is a very specific constraint of the NSF cluster, which requires that
        # each job have a CPU-RAM ratio of at most 2. If this comes up when porting
        # to other clusters, we should formally encode this in the config.
        assert updated_resource_req.mem_mb / updated_resource_req.mem_mb < 2000
        self.resource_reqs[node_id][cmd_queue_id] = updated_resource_req
        return WorkerResourceAlloc(
            True,
            node_id,
            cmd_queue_id,
            self.get_queue_id(),
            updated_resource_req
        )

    async def release_resource_allocation(self, alloc: WorkerResourceAlloc):
        pass

    async def download_file_deps(self, params: DownloadFileDepsParams, queue_id: str,
                                 use_local_storage: bool) -> None:
        # The code to upload file deps to slurm login node uses rsync
        # on the worker node maintaining the connection to said login node.
        # This means that, if files are not already available on that node,
        # they need to be before we can upload them to slurm login node.
        # Later on, we'll fix this by just using a shared S3 bucket for
        # everything.
        if not use_local_storage:
            local_download_params = copy.deepcopy(params)
            local_download_params.volumes = self.local_volumes
            await workflow.execute_activity(
                download_file_deps,
                local_download_params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=10000)
            )

        file_upload_params = SlurmFileUploadParams(
            params.cmd_files,
            self.local_volumes,
            self.remote_volumes,
            params.sif_path,
            self.storage_dir,
            params.elideable_transfers
        )
        await workflow.execute_activity(
            SlurmActivity.upload_to_slurm_login_node,
            file_upload_params,
            task_queue=queue_id,
            start_to_close_timeout=timedelta(seconds=10000),
            retry_policy=RetryPolicy(
                maximum_attempts=2
            )
        )

    async def sync_dir(self, params: SyncDirParams, queue_id: str,
                       use_local_storage: bool) -> None:
        file_download_params = SlurmFileDownloadParams(
            params.output_files,
            self.local_volumes,
            self.remote_volumes,
            params.elideable_transfers
        )
        await workflow.execute_activity(
            SlurmActivity.download_from_slurm_login_node,
            file_download_params,
            task_queue=queue_id,
            start_to_close_timeout=timedelta(seconds=10000),
            retry_policy=RetryPolicy(
                maximum_attempts=2
            )
        )

        if not use_local_storage:
            local_upload_params = copy.deepcopy(params)
            local_upload_params.volumes = self.local_volumes
            await workflow.execute_activity(
                sync_dir,
                local_upload_params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=10000)
            )
