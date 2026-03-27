from dataclasses import dataclass
from typing import List, Dict, Set, Optional


@dataclass
class CmdQueueId:
    cmd_set_id: int
    cmd_id: int

    def __hash__(self):
        return (self.cmd_set_id, self.cmd_id).__hash__()


@dataclass
class BwbWorkflowParams:
    scheme: dict
    config: dict
    use_singularity: bool


@dataclass
class ResourceVector:
    cpus: int
    gpus: int
    mem_mb: int


@dataclass
class WorkerResources:
    resources: ResourceVector
    queue_id: str


@dataclass
class WorkerResourceRequest:
    node_id: int
    cmd_queue_id: CmdQueueId
    request: ResourceVector


@dataclass
class WorkerResourceAlloc:
    active: bool
    node_id: int
    cmd_queue_id: CmdQueueId
    assigned_worker_queue: str
    assigned_resources: ResourceVector


@dataclass
class CmdFiles:
    input_files: Dict[str, Set[str]]
    deletable: Dict[str, Set[str]]
    output_files: Dict[str, Set[str]]


@dataclass
class ResourceRequest:
    workflow_id: str
    gpus: int
    cores: int
    ram_mb: int


@dataclass
class ResourceAllocation:
    job_id: int
    gpus: int
    cores: int
    ram_mb: int
    worker_queue: str | None


@dataclass
class CmdOutput:
    success: bool
    logs: str
    outputs: Optional[Dict[str, str]]
    output_files: Optional[Dict[str, Set[str]]]


@dataclass
class AssignWorkersParams:
    outstanding_requests: List[WorkerResourceRequest]
    worker_resources: Dict[str, WorkerResources]


@dataclass
class AssignWorkersResult:
    alllocations: List[WorkerResourceAlloc]
    revised_worker_resources: Dict[str, WorkerResources]


@dataclass
class WorkerPollerState:
    current_allocations: List[WorkerResourceAlloc]
    outstanding_requests: List[WorkerResourceRequest]
    # queue id -> current resources
    worker_total_resources: Dict[str, WorkerResources]
    # queue id -> current resources
    worker_current_resources: Dict[str, WorkerResources]
    # queue id -> is alive
    worker_liveliness: Dict[str, bool]


@dataclass
class InterWorkflowCmdResponse:
    node_id: int
    cmd_queue_id: CmdQueueId
    result: CmdOutput
    executor_type: str


@dataclass
class SlurmContainerCmdParams:
    cmd: str
    cmd_files: CmdFiles
    resource_req: ResourceVector
    config: dict
    use_singularity: bool
    volumes: dict
    image_dir: str


@dataclass
class SlurmCmdObj:
    job_id: int
    out_path: str
    err_path: str
    tmp_dir: str


@dataclass
class SlurmCmdResult:
    job_id: int
    out_path: str
    err_path: str
    tmp_dir: str
    exit_code: str
    status: str
    failed: bool
    preempted: bool


@dataclass
class SlurmSetupVolumesParams:
    bucket_id: str
    remote_storage_dir: str


@dataclass
class SlurmFileUploadParams:
    cmd_files: CmdFiles
    local_volumes: dict
    remote_volumes: dict
    sif_path: str | None
    remote_storage_dir: str
    elideable_xfers: Set[str]


@dataclass
class SlurmFileDownloadParams:
    output_files: Dict[str, List[str]]
    local_volumes: dict
    remote_volumes: dict
    elideable_xfers: Set[str]


@dataclass
class InterWorkflowSlurmCmdRequest:
    node_id: int
    cmd_queue_id: CmdQueueId
    request: SlurmContainerCmdParams


@dataclass
class SlurmPollerState:
    ip_addr: str
    user: str
    port: int
    outstanding_jobs: Dict[str, SlurmCmdObj]
    outstanding_requests: Dict[str, InterWorkflowSlurmCmdRequest]
    job_results: Dict[str, SlurmCmdResult]


@dataclass
class ImageBuildParams:
    img: str
    use_local_storage: bool
    bucket_id: str | None


@dataclass
class SyncDirParams:
    output_files: Dict[str, List[str]]
    elideable_transfers: Set[str]
    bucket_id: str


@dataclass
class ContainerCmdParams:
    cmd: str
    cmd_files: CmdFiles
    image: str
    bucket_id: str
    config: dict
    use_singularity: bool
    use_gpu: bool


@dataclass
class DownloadFileDepsParams:
    cmd_files: CmdFiles
    elideable_transfers: Set[str]
    bucket_id: str
    sif_path: str | None


@dataclass
class GenerateNodeCmdsParams:
    node: dict
    node_attrs: dict
    bucket_id: str
    image: str
    entrypoint: str
    use_singularity: bool
    use_local_storage: bool


@dataclass
class CmdObj:
    cmd: str
    cmd_files: CmdFiles


@dataclass
class SetupVolumesParams:
    bucket_id: str
