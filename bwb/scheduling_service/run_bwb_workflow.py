import os
import uuid
import json
import asyncio
import traceback
import sys

from temporalio.client import Client
from dotenv import load_dotenv

from bwb.scheduling_service.worker import verify_env, get_worker, get_scheduler_worker, get_slurm_worker, \
    register_worker_with_workflow
from bwb.scheduling_service.bwb_workflow import BwbWorkflow
from bwb.scheduling_service.scheduler_types import BwbWorkflowParams


# Function to start BWB workflow given a scheme and temporal client.
# "scheme" is dictionary representing scheme graph in format generated
# by BWB client. See "test_scheme.json" for example. Returns
# workflow id and run ID.
async def start_scheme(scheme, config, client, task_queue):
    load_dotenv()

    params = BwbWorkflowParams(scheme, config, True)
    handle = await client.start_workflow(
        BwbWorkflow.run,
        params,
        task_queue=task_queue,
        id=str(uuid.uuid4())
    )

    return handle.id, handle.first_execution_run_id


# Simple test case based on "salmon_demo" workflow.
async def run_workflow(wf_path, config_path):
    with open(wf_path) as wf:
        scheme = json.load(wf)

    config = {}
    if config_path is not None:
        with open(config_path) as cf:
            config = json.load(cf)

    load_dotenv()
    temporal_ept = os.getenv("TEMPORAL_ENDPOINT_URL")
    if temporal_ept is None:
        print("FATAL: Missing `TEMPORAL_ENDPOINT_URL` in .env")
        exit(1)
    client = await Client.connect(temporal_ept)

    workers = []
    worker_tasks = []
    scheduler_queue, scheduler_worker = await get_scheduler_worker()
    workers.append(scheduler_worker)
    worker_tasks.append(asyncio.create_task(scheduler_worker.run()))

    worker_resources = None
    if "executors" in config:
        if "slurm" in config["executors"]:
            print("GETTING SLURM WORKER")
            slurm_queue, slurm_worker = await get_slurm_worker(config["executors"]["slurm"])
            workers.append(slurm_worker)
            worker_tasks.append(asyncio.create_task(slurm_worker.run()))
        if "local" in config["executors"]:
            # The main worker ("worker") subscribes to unique task queue and
            # gets assigned work. The "heartbeat_worker" subscrbes to a different
            # queue and accepts heartbeat executors from scheduler to track its
            # liveliness.
            worker_resources, heartbeat_worker, worker = await get_worker()
            workers.append(worker)
            worker_tasks.append(asyncio.create_task(worker.run()))
            workers.append(heartbeat_worker)
            worker_tasks.append(asyncio.create_task(heartbeat_worker.run()))
    else:
        worker_resources, heartbeat_worker, worker = await get_worker()
        workers.append(worker)
        worker_tasks.append(asyncio.create_task(worker.run()))
        workers.append(heartbeat_worker)
        worker_tasks.append(asyncio.create_task(heartbeat_worker.run()))

    try:
        handle_id, handle_run_id = await start_scheme(scheme, config, client, scheduler_queue)
        handle = client.get_workflow_handle(handle_id, run_id=handle_run_id)

        # Register local worker with workflow
        if worker_resources is not None:
            if not await register_worker_with_workflow(handle_id, handle, client, worker_resources):
                print("ERROR: Unable to register worker.")
                exit(1)
        await handle.result()
        print(f"Workflow {wf_path} completed")
    except Exception as e:
        print(traceback.format_exc())
        print(f"Encountered error {e}, shutting down gracefully")
    finally:
        for worker in workers:
            await worker.shutdown()
        try:
            for worker_task in worker_tasks:
                await worker_task
        except asyncio.CancelledError:
            print("Worker task cancelled, exiting")


if __name__ == "__main__":
    # This will exit if the .env file is misconfigured or if
    # certain dependencies are not installed.
    verify_env()

    if len(sys.argv) == 3:
        asyncio.run(run_workflow(sys.argv[1], sys.argv[2]))

    elif len(sys.argv) == 2:
        asyncio.run(run_workflow(sys.argv[1], None))
    else:
        print(f"USAGE: python3 run_bwb_workflow.py [WORKFLOW_JSON_PATH] [CONFIG_JSON_PATH]")
        print(f"(Config is optional.)")
        exit(1)
