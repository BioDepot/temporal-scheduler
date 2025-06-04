import asyncio
from collections import defaultdict
from datetime import timedelta
from typing import Optional, DefaultDict, Dict, List

import temporalio.exceptions
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ExternalWorkflowHandle

from bwb.scheduling_service.executors.generic import get_worker_heartbeat_queue
from bwb.scheduling_service.executors.worker_poller_activities import heartbeat_activity, assign_workers
from bwb.scheduling_service.scheduler_types import WorkerPollerState, WorkerResourceAlloc, WorkerResourceRequest, \
    WorkerResources, AssignWorkersParams, AssignWorkersResult, CmdQueueId


@workflow.defn(sandboxed=False)
class WorkerPoller:
    @workflow.init
    def __init__(self, state: Optional[WorkerPollerState] = None):
        self.lock = asyncio.Lock()
        self.continuing = False
        self.current_allocations: DefaultDict[int, Dict[CmdQueueId, WorkerResourceAlloc]] = defaultdict(dict)
        self.outstanding_requests: DefaultDict[int, Dict[CmdQueueId, WorkerResourceRequest]] = defaultdict(dict)
        if state is not None:
            self.worker_total_resources = state.worker_total_resources
            self.worker_current_resources = state.worker_current_resources
            self.worker_is_alive = state.worker_liveliness
            self.heartbeat_handles = {}
            self.heartbeat_tasks = []
            self.worker_queue_by_heartbeat_activity_id = {}

            # If you're wondering why this is necessary, it's because JSON
            # (which temporal uses to transfer input args) can't encode dict
            # with dataclass as key.
            for alloc in state.current_allocations:
                node_id = int(alloc.node_id)
                cmd_queue_id = alloc.cmd_queue_id
                self.current_allocations[node_id][cmd_queue_id] = alloc
            for req in state.outstanding_requests:
                node_id = int(req.node_id)
                cmd_queue_id = req.cmd_queue_id
                self.outstanding_requests[node_id][cmd_queue_id] = req
        else:
            self.worker_total_resources: Dict[str, WorkerResources] = {}
            self.worker_current_resources: Dict[str, WorkerResources] = {}
            self.worker_is_alive = {}
            self.heartbeat_handles = {}
            self.heartbeat_tasks = []
            self.worker_queue_by_heartbeat_activity_id = {}

    async def handle_dead_worker(self, queue_id: str, depth):
        print(f"\nWorker queue {queue_id} failed heartbeat\n")

        if not self.continuing:
            print(f"Retrying heartbeat of worker queue {queue_id}")
            self.heartbeat_handles[queue_id].cancel()
            self.worker_is_alive[queue_id] = False
            await self.heartbeat_task(queue_id, depth)

    async def heartbeat_task(self, queue_id, depth=0):
        try:
            # Exponential backoff if worker is dead
            if depth > 0:
                sleep_time = min(1800, 4**depth)
                print(f"Waiting {sleep_time} seconds before retrying heartbeat on {queue_id}")
                await workflow.sleep(timedelta(seconds=sleep_time))

            print("Starting heartbeat")
            heartbeat_queue = get_worker_heartbeat_queue(queue_id)
            self.heartbeat_handles[queue_id] = workflow.start_activity(
                heartbeat_activity,
                task_queue=heartbeat_queue,
                schedule_to_start_timeout=timedelta(seconds=20),
                start_to_close_timeout=timedelta(seconds=100000),
                heartbeat_timeout=timedelta(seconds=20),
                retry_policy=RetryPolicy(
                    maximum_attempts=1
                )
            )

            # This handles case where heartbeat to worker previously
            # failed and worker is now marked as dead. We wait a bit
            # over 20 seconds (the schedule to start timeout), meaning
            # that, if the worker is still dead, an exception will be
            # thrown; if that is not the case, we can safely mark the
            # worker as alive again.
            if not self.worker_is_alive[queue_id]:
                await workflow.sleep(timedelta(seconds=25))
                print(f"Worker {queue_id} has resurrected.")
                self.worker_is_alive[queue_id] = True

            await self.heartbeat_handles[queue_id]
            # If the heartbeat handle successfully returns without raising
            # an error, this can mean the worker shut down gracefully. Idk
            # what the idiomatic temporal way is to allow both for graceful
            # cancellation of executors from the parent workflow but to
            # throw activity errors when the worker shuts down. In either
            # case, the above await should continue indefinitely (or until
            # this task is cancelled due to Continue-As-New).
            await self.handle_dead_worker(queue_id, depth+1)
        except asyncio.CancelledError:
            self.heartbeat_handles[queue_id].cancel()
            await self.heartbeat_handles[queue_id]
            return
        except (temporalio.exceptions.ActivityError, temporalio.exceptions.TimeoutError) as e:
            if self.worker_is_alive[queue_id]:
                print(f"Worker {queue_id} has died")
            await self.handle_dead_worker(queue_id, depth+1)

    async def start_heartbeats(self):
        for queue_id in self.worker_current_resources.keys():
            if queue_id not in self.heartbeat_handles:
                print("Creating heartbeat task")
                heartbeat_task = asyncio.Task(self.heartbeat_task(queue_id))
                self.heartbeat_tasks.append(heartbeat_task)

    def get_open_reqs(self) -> List[WorkerResourceRequest]:
        outstanding_requests = []
        for node_id, reqs_by_cmd_set in self.outstanding_requests.items():
            for cmd_set_id, req in reqs_by_cmd_set.items():
                outstanding_requests.append(req)
        return outstanding_requests

    def get_current_allocs(self) -> List[WorkerResourceAlloc]:
        current_allocs = []
        for node_id, reqs_by_cmd_set in self.current_allocations.items():
            for cmd_set_id, alloc in reqs_by_cmd_set.items():
                current_allocs.append(alloc)
        return current_allocs

    @workflow.run
    async def run(self, state: Optional[WorkerPollerState]=None):
        # This is the pattern recommended by docs here:
        # [https://temporal.io/blog/workflows-as-actors-is-it-really-possible#when-to-continue-as-new]
        cancelled = False
        try:
            requester_workflow: ExternalWorkflowHandle = workflow.get_external_workflow_handle(
                workflow.info().parent.workflow_id,
                run_id=workflow.info().parent.run_id
            )
            while workflow.info().get_current_history_length() < 10000:
                await self.start_heartbeats()
                living_workers = {}
                for worker_queue, worker_resources in self.worker_current_resources.items():
                    if self.worker_is_alive[worker_queue]:
                        living_workers[worker_queue] = worker_resources

                async with self.lock:
                    outstanding_requests = self.get_open_reqs()
                    worker_assignment_params = AssignWorkersParams(
                        outstanding_requests,
                        living_workers,
                    )
                    result: AssignWorkersResult = await workflow.execute_activity(
                        assign_workers,
                        worker_assignment_params,
                        task_queue="scheduler-queue",
                        start_to_close_timeout=timedelta(seconds=100)
                    )
                    for queue, current_resources in result.revised_worker_resources.items():
                        assert current_resources.resources.gpus >= 0
                        assert current_resources.resources.cpus >= 0
                        assert current_resources.resources.mem_mb >= 0
                        self.worker_current_resources[queue] = current_resources

                for alloc in result.alllocations:
                    node_id = alloc.node_id
                    cmd_queue_id = alloc.cmd_queue_id
                    del self.outstanding_requests[node_id][cmd_queue_id]
                    self.current_allocations[node_id][cmd_queue_id] = alloc
                    await requester_workflow.signal(
                        "handle_allocation",
                        alloc
                    )
                await asyncio.sleep(5)

        except temporalio.exceptions.CancelledError:
            cancelled = True
            print("GOT CANCELLED")
        except asyncio.CancelledError:
            cancelled = True
            print("GOT CANCELLED")
        finally:
            self.continuing = not cancelled
            #for task in self.heartbeat_tasks:
            #    if not task.done():
            #        print("Cancelling heartbeat task")
            #        task.cancel()
            #    else:
            #        print("Task already cancelled")
            #await asyncio.gather(*self.heartbeat_tasks, return_exceptions=True)
            if cancelled:
                return

        print("Creating new state")
        new_state = WorkerPollerState(
            self.get_current_allocs(),
            self.get_open_reqs(),
            self.worker_total_resources,
            self.worker_current_resources,
            self.worker_is_alive
        )
        workflow.continue_as_new(new_state)

    @workflow.signal
    async def request_resources(self, request: WorkerResourceRequest):
        node_id = request.node_id
        cmd_set_id = request.cmd_queue_id
        self.outstanding_requests[node_id][cmd_set_id] = request

    @workflow.signal
    async def release_resource_allocation(self, alloc: WorkerResourceAlloc):
        async with self.lock:
            node_id = alloc.node_id
            cmd_set_id = alloc.cmd_queue_id
            assert node_id in self.current_allocations
            assert cmd_set_id in self.current_allocations[node_id]
            worker_id = alloc.assigned_worker_queue
            #print(f"\tReceived release req {alloc}")
            #print(f"\tBefore: {self.worker_current_resources[worker_id]}")
            if self.current_allocations[node_id][cmd_set_id].active:
                self.worker_current_resources[worker_id].resources.cpus += alloc.assigned_resources.cpus
                self.worker_current_resources[worker_id].resources.gpus += alloc.assigned_resources.gpus
                self.worker_current_resources[worker_id].resources.mem_mb += alloc.assigned_resources.mem_mb

                total_cpus = self.worker_total_resources[worker_id].resources.cpus
                assert self.worker_current_resources[worker_id].resources.cpus <= total_cpus
                total_gpus = self.worker_total_resources[worker_id].resources.gpus
                assert self.worker_current_resources[worker_id].resources.gpus <= total_gpus
                total_mem = self.worker_total_resources[worker_id].resources.mem_mb
                assert self.worker_current_resources[worker_id].resources.mem_mb <= total_mem
            else:
                print(f"Received inactive resource alloc {node_id}, {cmd_set_id}")

            #print(f"\tAfter: {self.worker_current_resources[worker_id]}")
            self.current_allocations[node_id][cmd_set_id].active = False

    @workflow.signal
    async def add_worker(self, worker: WorkerResources):
        self.worker_total_resources[worker.queue_id] = worker
        self.worker_current_resources[worker.queue_id] = worker
        self.worker_is_alive[worker.queue_id] = True
