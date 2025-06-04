import asyncio
from collections import defaultdict
from datetime import timedelta, datetime
from typing import Dict, Optional, DefaultDict

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ChildWorkflowHandle, ParentClosePolicy

from bwb.scheduling_service.executors.abstract_executor import AbstractExecutor
from bwb.scheduling_service.executors.generic import get_scheduler_child_queue
from bwb.scheduling_service.executors.local_activities import setup_volumes_on_worker, run_workflow_cmd, download_file_deps, \
    sync_dir
from bwb.scheduling_service.executors.worker_poller import WorkerPoller
from bwb.scheduling_service.scheduler_types import WorkerResourceAlloc, ResourceVector, WorkerResourceRequest, \
    CmdOutput, CmdQueueId, SyncDirParams, ContainerCmdParams, DownloadFileDepsParams, SetupVolumesParams


class LocalExecutor(AbstractExecutor):
    def __init__(self):
        self.queue_assigned_events: Dict[int, asyncio.Event] = {}
        self.child_workflow: Optional[ChildWorkflowHandle] = None
        self.resource_allocs: DefaultDict[int, Dict[CmdQueueId, WorkerResourceAlloc]] = defaultdict(dict)
        self.alloc_recv_events: DefaultDict[int, Dict[CmdQueueId, bool]] = defaultdict(dict)

    def child_workflow_is_started(self):
        return self.child_workflow is not None

    async def ensure_child_workflow_is_initiated(self):
        if self.child_workflow is None:
            workflow_id = workflow.info().workflow_id

            # I would really prefer not to terminate child workflows once the parent
            # dies, but Temporal has some weird issue where calling "cancel" on a child
            # workflow doesn't work is that child workflow has been Continued-As-New.
            # I think this may be related to the issue described here
            # [https://temporal.io/blog/temporal-deep-dive-stress-testing], and it
            # seems like they've fixed this in other SDKs.
            # In any case, this is the only way I can guarantee that child workflows
            # get shutdown once the parent dies.
            self.child_workflow: Optional[ChildWorkflowHandle] = await workflow.start_child_workflow(
                WorkerPoller.run,
                id=get_scheduler_child_queue(workflow_id),
                task_queue="scheduler-queue",
                parent_close_policy=ParentClosePolicy.TERMINATE
            )

    async def setup_volumes(self, params: SetupVolumesParams, queue_id: str):
        return await workflow.execute_activity(
            setup_volumes_on_worker,
            params,
            task_queue=queue_id,
            start_to_close_timeout=timedelta(seconds=20)
        )

    async def request_resource_allocation(
        self, request: ResourceVector, node_id: int, cmd_queue_id: CmdQueueId
    ) -> WorkerResourceAlloc:
        await self.ensure_child_workflow_is_initiated()
        request = WorkerResourceRequest(
            node_id,
            cmd_queue_id,
            request
        )
        self.alloc_recv_events[node_id][cmd_queue_id] = False
        await self.child_workflow.signal(
            WorkerPoller.request_resources,
            request
        )
        await workflow.wait_condition(lambda: self.alloc_recv_events[node_id][cmd_queue_id])
        assert node_id in self.resource_allocs and cmd_queue_id in self.resource_allocs[node_id]
        return self.resource_allocs[node_id][cmd_queue_id]

    async def release_resource_allocation(self, alloc: WorkerResourceAlloc):
        await self.ensure_child_workflow_is_initiated()
        await self.child_workflow.signal(
            WorkerPoller.release_resource_allocation,
            alloc
        )

    async def download_file_deps(self, params: DownloadFileDepsParams, queue_id: str,
                                 use_local_storage: bool) -> None:
        if not use_local_storage:
            return await workflow.execute_activity(
                download_file_deps,
                params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=100000),
                heartbeat_timeout=timedelta(seconds=60)
            )

    async def sync_dir(self, params: SyncDirParams, queue_id: str,
                       use_local_storage: bool) -> None:
        if not use_local_storage:
            await workflow.execute_activity(
                sync_dir,
                params,
                task_queue=queue_id,
                start_to_close_timeout=timedelta(seconds=100000),
                heartbeat_timeout=timedelta(seconds=60)
            )

    def pass_cmd_result(self, node_id: int, cmd_queue_id: CmdQueueId, cmd_result: CmdOutput) -> None:
        pass

    def pass_resource_alloc(self, node_id: int, cmd_queue_id: CmdQueueId, alloc: WorkerResourceAlloc) -> None:
        self.resource_allocs[node_id][cmd_queue_id] = alloc
        self.alloc_recv_events[node_id][cmd_queue_id] = True

    async def run_cmd(
        self, params: ContainerCmdParams, node_id: int, cmd_queue_id: CmdQueueId, queue_id: str
    ) -> CmdOutput:
        now = datetime.now()
        time_string = now.strftime("%H:%M:%S")
        #print(f"[{time_string}] Local running cmd {node_id}, {cmd_queue_id}")
        return await workflow.execute_activity(
            run_workflow_cmd,
            params,
            start_to_close_timeout=timedelta(seconds=10000),
            task_queue=queue_id,
            retry_policy=RetryPolicy(
                maximum_attempts=2
            ),
            schedule_to_start_timeout=timedelta(seconds=20),
            heartbeat_timeout=timedelta(seconds=60)
        )

    async def shutdown_child(self):
        print("SHUTTING DOWN CHILD")
        if self.child_workflow is not None:
            self.child_workflow.cancel()
