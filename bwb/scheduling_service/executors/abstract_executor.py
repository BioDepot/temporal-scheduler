from abc import ABC, abstractmethod

from bwb.scheduling_service.scheduler_types import ResourceVector, WorkerResourceAlloc, CmdOutput, CmdQueueId, \
    SyncDirParams, ContainerCmdParams, DownloadFileDepsParams, SetupVolumesParams


# NOTE: Not all of these methods need to be implemented for every executor.
# If there are some methods which are unnecessary (e.g. executors without
# polling child workflows won't need `pass_cmd_result`), then just implement
# it as "pass" in the derived class.
class AbstractExecutor(ABC):
    @abstractmethod
    async def setup_volumes(self, params: SetupVolumesParams, queue_id: str):
        pass

    @abstractmethod
    def child_workflow_is_started(self):
        pass

    @abstractmethod
    async def shutdown_child(self):
        pass

    @abstractmethod
    async def request_resource_allocation(
        self, request: ResourceVector, node_id: int, cmd_queue_id: CmdQueueId
    ) -> WorkerResourceAlloc:
        pass

    @abstractmethod
    async def release_resource_allocation(self, alloc: WorkerResourceAlloc):
        pass

    @abstractmethod
    async def download_file_deps(self, params: DownloadFileDepsParams, queue_id: str,
                                 use_local_storage: bool) -> None:
        pass

    @abstractmethod
    async def sync_dir(self, params: SyncDirParams, queue_id: str,
                       use_local_storage: bool) -> None:
        pass

    @abstractmethod
    def pass_cmd_result(self, node_id: int, cmd_queue_id: CmdQueueId, cmd_result: CmdOutput) -> None:
        """
        One issue we have is that some executors can submit a job
        and wait for completion whereas others (like slurm) need
        to wait for a child workflow to do polling and signal back
        to the main workflow. The workaround, so that `run_cmd`
        can maintain a consistent interface, is to call this
        method from the main workflow once the signal is received;
        from the implementation, we can set some event on which
        `run_cmd` waits, so run_cmd can then continue and return
        whatever result the signal got. Signal will be named
        `handle_polling_result`.

        :param node_id: Node ID of signaled command.
        :param cmd_queue_id: CMD queue ID of signaled command.
        :param cmd_result: Result of signaled command.
        """
        pass

    @abstractmethod
    def pass_resource_alloc(self, node_id: int, cmd_queue_id: CmdQueueId, alloc: WorkerResourceAlloc) -> None:
        pass

    @abstractmethod
    async def run_cmd(
        self, params: ContainerCmdParams, node_id: int, cmd_queue_id: CmdQueueId, queue_id: str
    ) -> CmdOutput:
        pass
