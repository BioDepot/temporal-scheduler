import temporalio
import asyncio
from datetime import timedelta
from typing import Dict

from temporalio import workflow
from temporalio.workflow import ExternalWorkflowHandle

from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
from bwb.scheduling_service.scheduler_types import SlurmPollerState, InterWorkflowSlurmCmdRequest, CmdOutput, \
    InterWorkflowCmdResponse, SlurmCmdObj, SlurmCmdResult


@workflow.defn(sandboxed=False)
class SlurmPoller:
    @workflow.init
    def __init__(self, poller_state: SlurmPollerState):
        self.outstanding_jobs: Dict[int, SlurmCmdObj] = {
            int(k): v for k, v in poller_state.outstanding_jobs.items()
        }
        self.outstanding_requests: Dict[int, InterWorkflowSlurmCmdRequest] = {
            int(k): v for k, v in poller_state.outstanding_requests.items()
        }
        self.job_results: Dict[int, SlurmCmdResult] = {
            int(k): v for k, v in poller_state.job_results.items()
        }
        self.ip_addr = poller_state.ip_addr
        self.user = poller_state.user
        self.port = getattr(poller_state, "port", 22)

    def get_queue_id(self) -> str:
        return f"{self.user}@{self.ip_addr}:{self.port}"

    @workflow.run
    async def run(self, poller_state: SlurmPollerState):
        try:
            queue_id = self.get_queue_id()
            requester_workflow: ExternalWorkflowHandle = workflow.get_external_workflow_handle(
                workflow.info().parent.workflow_id,
                run_id=workflow.info().parent.run_id
            )
            while workflow.info().get_current_history_length() < 10000:
                await asyncio.sleep(5)

                slurm_cmd_results: Dict[str, SlurmCmdResult] = await workflow.execute_activity(
                    SlurmActivity.poll_slurm,
                    self.outstanding_jobs,
                    task_queue=queue_id,
                    start_to_close_timeout=timedelta(seconds=10000),
                )
                if self.outstanding_jobs:
                    print(
                        f"SlurmPoller outstanding={sorted(self.outstanding_jobs.keys())} "
                        f"polled={list(slurm_cmd_results.keys())}"
                    )

                # Job ID will be serialized as str because it was turned
                # into JSON by temporal.
                for job_id_str, result in slurm_cmd_results.items():
                    job_id = int(job_id_str)
                    self.job_results[job_id] = result
                    corresponding_request = self.outstanding_requests[job_id]
                    del self.outstanding_jobs[job_id]
                    del self.outstanding_requests[job_id]

                    # I'm uncertain if we should put some upper-bound
                    # on the number of times we restart. As I understand it,
                    # jobs should not be indefinitely preempted.
                    if result.preempted:
                        await self.submit_slurm_cmd(corresponding_request)
                    else:
                        slurm_outputs: CmdOutput = await workflow.execute_activity(
                            SlurmActivity.get_slurm_outputs,
                            result,
                            task_queue=queue_id,
                            start_to_close_timeout=timedelta(seconds=10000),
                        )
                        if not slurm_outputs.success:
                            print(f"{corresponding_request.request.cmd} failed with error {slurm_outputs.logs}")
                            print(f"FULL RESPONSE: {slurm_outputs}")

                        response_to_slurm_req = InterWorkflowCmdResponse(
                            corresponding_request.node_id,
                            corresponding_request.cmd_queue_id,
                            slurm_outputs,
                            "slurm"
                        )
                        print(
                            f"SlurmPoller signaling result for job_id={job_id} "
                            f"node_id={corresponding_request.node_id} "
                            f"cmd_queue_id={corresponding_request.cmd_queue_id}"
                        )
                        await requester_workflow.signal("handle_polling_result", response_to_slurm_req)

            continuation_state = SlurmPollerState(
                self.ip_addr,
                self.user,
                self.port,
                self.outstanding_jobs,
                self.outstanding_requests,
                self.job_results
            )
            workflow.continue_as_new(continuation_state)
        
        except temporalio.exceptions.CancelledError:
            print("SLURM: GOT CANCELLED")
            return
        except asyncio.CancelledError:
            print("SLURM: GOT CANCELLED")
            return


    @workflow.signal
    async def submit_slurm_cmd(self, cmd_params: InterWorkflowSlurmCmdRequest):
        queue_name = self.get_queue_id()
        slurm_cmd_obj = await workflow.execute_activity(
            SlurmActivity.start_slurm_job,
            cmd_params.request,
            start_to_close_timeout=timedelta(seconds=10000),
            task_queue=queue_name
        )

        job_id = slurm_cmd_obj.job_id
        self.outstanding_jobs[job_id] = slurm_cmd_obj
        self.outstanding_requests[job_id] = cmd_params
        print(
            f"SlurmPoller submitted job_id={job_id} "
            f"node_id={cmd_params.node_id} cmd_queue_id={cmd_params.cmd_queue_id}"
        )
