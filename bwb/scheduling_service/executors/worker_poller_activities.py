import asyncio
from collections import defaultdict
from typing import DefaultDict, Dict, List

from temporalio import activity

from bwb.scheduling_service.scheduler_types import AssignWorkersParams, AssignWorkersResult, ResourceVector, \
    WorkerResources, WorkerResourceAlloc, CmdQueueId


@activity.defn
async def assign_workers(params: AssignWorkersParams) -> AssignWorkersResult:
    open_reqs: DefaultDict[int, Dict[CmdQueueId, ResourceVector]] = defaultdict(dict)
    for request in params.outstanding_requests:
        request_node_id = request.node_id
        request_queue_id = request.cmd_queue_id
        open_reqs[request_node_id][request_queue_id] = request.request

    serviced_reqs: DefaultDict[int, Dict[CmdQueueId, str]] = defaultdict(dict)
    revised_worker_resources: Dict[str, WorkerResources] = {}
    #for worker_queue, worker in params.worker_resources.items():
    #    remaining_cpu = worker.resources.cpus
    #    remaining_gpu = worker.resources.gpus
    #    remaining_mem = worker.resources.mem_mb
    #    for node_id, requests_by_cmd_id in open_reqs.items():
    #        if node_id not in [5,3,0]:
    #            continue
    #        for cmd_id, request in requests_by_cmd_id.items():
    #            if node_id in serviced_reqs and cmd_id in serviced_reqs[node_id]:
    #                continue
    #            if request.cpus <= remaining_cpu and request.mem_mb <= remaining_mem and request.gpus <= remaining_gpu:
    #                serviced_reqs[node_id][cmd_id] = worker.queue_id
    #                remaining_cpu -= request.cpus
    #                remaining_gpu -= request.gpus
    #                remaining_mem -= request.mem_mb

    #        revised_worker_resources[worker.queue_id] = WorkerResources(
    #            resources=ResourceVector(remaining_cpu, remaining_gpu, remaining_mem),
    #            queue_id=worker_queue
    #        )

    for worker_queue, worker in params.worker_resources.items():
        remaining_cpu = worker.resources.cpus
        remaining_gpu = worker.resources.gpus
        remaining_mem = worker.resources.mem_mb
        if worker.queue_id in revised_worker_resources:
            remaining_cpu = revised_worker_resources[worker.queue_id].resources.cpus
            remaining_gpu = revised_worker_resources[worker.queue_id].resources.gpus
            remaining_mem = revised_worker_resources[worker.queue_id].resources.mem_mb

        for node_id, requests_by_cmd_id in open_reqs.items():
            for cmd_id, request in requests_by_cmd_id.items():
                if node_id in serviced_reqs and cmd_id in serviced_reqs[node_id]:
                    continue
                if request.cpus <= remaining_cpu and request.mem_mb <= remaining_mem and request.gpus <= remaining_gpu:
                    serviced_reqs[node_id][cmd_id] = worker.queue_id
                    remaining_cpu -= request.cpus
                    remaining_gpu -= request.gpus
                    remaining_mem -= request.mem_mb

            revised_worker_resources[worker.queue_id] = WorkerResources(
                resources=ResourceVector(remaining_cpu, remaining_gpu, remaining_mem),
                queue_id=worker_queue
            )

    allocs: List[WorkerResourceAlloc] = []
    for node_id, reqs_by_cmd_set in serviced_reqs.items():
        for cmd_set_id, queue_name in reqs_by_cmd_set.items():
            corresponding_req = open_reqs[node_id][cmd_set_id]
            allocs.append(WorkerResourceAlloc(
                True,
                node_id,
                cmd_set_id,
                queue_name,
                corresponding_req
            ))

    result = AssignWorkersResult(
        allocs,
        revised_worker_resources
    )
    return result


@activity.defn
async def heartbeat_activity() -> None:
    cancelled = False
    while not cancelled:
        try:
            activity.heartbeat()
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("Heartbeat activity cancelled")
            cancelled = True
