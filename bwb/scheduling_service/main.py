import json
import os
import fastapi

from fastapi import Request
from fastapi.responses import JSONResponse
from temporalio.client import Client
from dotenv import load_dotenv

from bwb.scheduling_service.bwb_workflow import BwbWorkflow
from bwb.scheduling_service.run_bwb_workflow import start_scheme
from bwb.scheduling_service.worker import register_worker_with_workflow
from bwb.scheduling_service.scheduler_types import ResourceVector, WorkerResources

load_dotenv()
TEMPORAL_EPT = os.getenv("TEMPORAL_ENDPOINT_URL")
if TEMPORAL_EPT is None:
    print("FATAL: `TEMPORAL_ENDPOINT_URL` not defined in env")
    exit(1)

app = fastapi.FastAPI()


@app.post("/start_workflow")
async def start_workflow(req: Request):
    data = await req.json()
    client = await Client.connect(TEMPORAL_EPT)

    if "workflow_def" not in data:
        return JSONResponse(
            status_code=400,
            content={
                "message": "Missing required key `workflow_def`.",
            }
        )

    workflow_def = data["workflow_def"]
    config = {}
    if "config" in data:
        config = data["config"]

    workflow_id, run_id = await start_scheme(workflow_def, config, client, "scheduler-queue")

    return JSONResponse(
        status_code=200,
        content={
            "workflow_id": workflow_id,
            "run_id": run_id
        }
    )


@app.post("/add_worker_to_workflow")
async def add_worker(req: Request):
    client = await Client.connect(TEMPORAL_EPT)
    data = await req.json()
    required_keys = {
        "workflow_id", "worker_queue",
        "worker_cpus", "worker_mem_mb",
        "worker_gpus"
    }
    for key in required_keys:
        if key not in data:
            return JSONResponse(
                status_code=400,
                content={
                    "message": f"Missing required key {key}",
                }
            )

    worker_id = data["worker_queue"]
    workflow_id = data["workflow_id"]

    handle = client.get_workflow_handle(workflow_id)
    worker_resources = WorkerResources(
        resources=ResourceVector(
            data["worker_cpus"],
            data["worker_gpus"],
            data["worker_mem_mb"]
        ),
        queue_id=worker_id
    )

    # TODO: Have this timeout at some point if child workflow doesn't start in a long
    # time.
    if not await register_worker_with_workflow(workflow_id, handle, client, worker_resources):
        return JSONResponse(
            status_code=422,
            content={
                "message": f"Unable to register worker {worker_id} with child workflow of {workflow_id}.",
            }
        )

    return JSONResponse(
        status_code=200,
        content={
            "message": f"Successfully added worker {worker_id} to workflow {workflow_id}.",
        }
    )


@app.post("/stop_workflow")
async def stop_workflow(req: Request):
    client = await Client.connect(TEMPORAL_EPT)
    data = await req.json()
    workflow_id = data["workflow_id"]
    run_id = data["run_id"] if "run_id" in data else None

    try:
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        await handle.terminate()
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={
                "message": "Invalid workflow_id.",
            }
        )

    return JSONResponse(
        status_code=200,
        content={
            "message": f"Successfully stopped workflow {workflow_id}.",
        }
    )


@app.post("/workflow_status")
async def workflow_status(req: Request):
    client = await Client.connect(TEMPORAL_EPT)
    data = await req.json()
    if "workflow_id" not in data:
        return JSONResponse(
            status_code=400,
            content={
                "message": f"Missing required key workflow_id.",
            }
        )

    workflow_id = data["workflow_id"]
    run_id = None
    if "run_id" in data:
        run_id = data["run_id"]

    try:
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={
                "message": f"Failed to get workflow handle for id {workflow_id}.",
            }
        )

    status = await handle.query(BwbWorkflow.get_status)
    return status
