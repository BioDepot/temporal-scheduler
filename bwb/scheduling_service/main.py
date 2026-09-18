import json
import os
import uuid
from dataclasses import asdict, is_dataclass

import fastapi

from fastapi import Request
from fastapi.responses import JSONResponse
from temporalio.client import Client
from temporalio.service import RPCError
from dotenv import load_dotenv

from bwb_scheduler.resolved_payload import normalize_start_workflow_payload
from bwb.scheduling_service.bwb_workflow import BwbWorkflow
from bwb.scheduling_service.executors.ssh_docker_workflow import RemoteDockerJobParams, RemoteDockerWorkflow
from bwb.scheduling_service.executors.globus_activity import GlobusTransferItem, GlobusTransferParams
from bwb.scheduling_service.executors.staged_slurm_gpu_workflow import (
    GpuDockerStage,
    SlurmScriptStage,
    StagedSlurmGpuParams,
    StagedSlurmGpuWorkflow,
)
from bwb.scheduling_service.run_bwb_workflow import start_scheme
from bwb.scheduling_service.worker import register_worker_with_workflow
from bwb.scheduling_service.scheduler_types import (
    ResourceVector,
    SlurmScriptJobParams,
    WorkerResources,
)

load_dotenv()
TEMPORAL_EPT = os.getenv("TEMPORAL_ENDPOINT_URL")
if TEMPORAL_EPT is None:
    print("FATAL: `TEMPORAL_ENDPOINT_URL` not defined in env")
    exit(1)

app = fastapi.FastAPI()


def _workflow_status_from_description(description) -> dict:
    raw_status = description.status.name if description.status is not None else "UNKNOWN"
    status_map = {
        "RUNNING": "Running",
        "COMPLETED": "Finished",
        "FAILED": "Failed",
        "CANCELED": "Canceled",
        "TERMINATED": "Terminated",
        "CONTINUED_AS_NEW": "ContinuedAsNew",
        "TIMED_OUT": "TimedOut",
    }
    return {
        "workflow_status": status_map.get(raw_status, raw_status.title()),
        "node_statuses": {},
        "condition_results": {},
        "status_source": "describe",
    }


def _ssh_docker_config(data: dict) -> dict:
    config = data.get("ssh_docker") if isinstance(data.get("ssh_docker"), dict) else None
    if config is None and isinstance(data.get("config"), dict):
        raw_config = data["config"]
        if isinstance(raw_config.get("executors"), dict) and isinstance(raw_config["executors"].get("ssh_docker"), dict):
            config = raw_config["executors"]["ssh_docker"]
        elif isinstance(raw_config.get("ssh_docker"), dict):
            config = raw_config["ssh_docker"]
    if not isinstance(config, dict):
        raise ValueError("Missing ssh_docker config")
    for key in ("ip_addr", "user", "storage_dir"):
        if not str(config.get(key) or "").strip():
            raise ValueError(f"Missing ssh_docker config key `{key}`")
    return config


def _ssh_docker_queue(config: dict) -> str:
    return f"{config['user']}@{config['ip_addr']}:{int(config.get('port', 22))}"


def _remote_docker_job_params(data: dict) -> RemoteDockerJobParams:
    job = data.get("job") if isinstance(data.get("job"), dict) else data
    try:
        return RemoteDockerJobParams(
            image=str(job["image"]),
            cmd=[str(item) for item in job["cmd"]],
            input_files={str(key): str(value) for key, value in dict(job.get("input_files") or {}).items()},
            use_gpu=bool(job.get("use_gpu", False)),
            gpu_device=str(job.get("gpu_device") or ""),
            env={str(key): str(value) for key, value in dict(job.get("env") or {}).items()} or None,
            timeout_seconds=int(job.get("timeout_seconds") or 7200),
            local_output_dir=str(job.get("local_output_dir") or ""),
            cleanup=bool(job.get("cleanup", True)),
            min_gpu_free_mb=int(job.get("min_gpu_free_mb") or 0),
            gpu_wait_timeout_seconds=int(job.get("gpu_wait_timeout_seconds") or 7200),
        )
    except KeyError as exc:
        raise ValueError(f"Missing remote Docker job key `{exc.args[0]}`") from exc


def _jsonable_result(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return value
    return value


def _globus_transfer_params(data: dict | None) -> GlobusTransferParams | None:
    if data is None:
        return None
    items = [
        GlobusTransferItem(
            source_path=str(item["source_path"]),
            destination_path=str(item["destination_path"]),
            recursive=bool(item.get("recursive", False)),
        )
        for item in data.get("items") or []
    ]
    return GlobusTransferParams(
        source_endpoint_id=str(data["source_endpoint_id"]),
        destination_endpoint_id=str(data["destination_endpoint_id"]),
        items=items,
        label=str(data["label"]),
        submission_id=str(data["submission_id"]),
        sync_level=str(data.get("sync_level") or "checksum"),
        verify_checksum=bool(data.get("verify_checksum", True)),
        preserve_timestamp=bool(data.get("preserve_timestamp", True)),
        timeout_seconds=int(data.get("timeout_seconds") or 86400),
        poll_interval_seconds=int(data.get("poll_interval_seconds") or 15),
    )


def _staged_slurm_gpu_params(data: dict) -> StagedSlurmGpuParams:
    slurm = data.get("slurm")
    gpu = data.get("gpu")
    slurm_stage = None
    if slurm is not None:
        slurm_job = slurm["job"]
        resources = slurm_job.get("resources") or {}
        slurm_stage = SlurmScriptStage(
            task_queue=str(slurm["task_queue"]),
            job=SlurmScriptJobParams(
                script=str(slurm_job["script"]),
                resource_req=ResourceVector(
                    cpus=int(resources.get("cpus") or 1),
                    gpus=int(resources.get("gpus") or 0),
                    mem_mb=int(resources.get("mem_mb") or 1024),
                ),
                config=dict(slurm_job.get("config") or {}),
                name=str(slurm_job.get("name") or "slurm-script"),
            ),
            poll_interval_seconds=int(slurm.get("poll_interval_seconds") or 15),
            timeout_seconds=int(slurm.get("timeout_seconds") or 86400),
        )
    return StagedSlurmGpuParams(
        globus_task_queue=str(data.get("globus_task_queue") or data.get("task_queue") or "staged-slurm-gpu"),
        stage_in=_globus_transfer_params(data.get("stage_in")),
        slurm=slurm_stage,
        stage_back=_globus_transfer_params(data.get("stage_back")),
        gpu=(
            GpuDockerStage(
                task_queue=str(gpu["task_queue"]),
                job=_remote_docker_job_params(gpu),
            )
            if gpu is not None
            else None
        ),
        publish=_globus_transfer_params(data.get("publish")),
    )


@app.post("/start_workflow")
async def start_workflow(req: Request):
    data = normalize_start_workflow_payload(await req.json())
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

    use_singularity = bool(data.get("use_singularity", True))
    workflow_id, run_id = await start_scheme(
        workflow_def,
        config,
        client,
        "scheduler-queue",
        use_singularity=use_singularity,
    )

    return JSONResponse(
        status_code=200,
        content={
            "workflow_id": workflow_id,
            "run_id": run_id
        }
    )


@app.post("/start_ssh_docker_workflow")
async def start_ssh_docker_workflow(req: Request):
    data = await req.json()
    client = await Client.connect(TEMPORAL_EPT)
    try:
        config = _ssh_docker_config(data)
        params = _remote_docker_job_params(data)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"message": str(exc)})
    task_queue = str(data.get("task_queue") or _ssh_docker_queue(config))
    workflow_id = str(data.get("workflow_id") or f"ssh-docker-{uuid.uuid4().hex}")
    handle = await client.start_workflow(
        RemoteDockerWorkflow.run,
        params,
        task_queue=task_queue,
        id=workflow_id,
    )
    return JSONResponse(
        status_code=200,
        content={
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id,
            "task_queue": task_queue,
        },
    )


@app.post("/start_staged_slurm_gpu_workflow")
async def start_staged_slurm_gpu_workflow(req: Request):
    data = await req.json()
    client = await Client.connect(TEMPORAL_EPT)
    try:
        params = _staged_slurm_gpu_params(data)
    except (KeyError, TypeError, ValueError) as exc:
        return JSONResponse(status_code=400, content={"message": f"Invalid staged workflow payload: {exc}"})
    task_queue = str(data.get("task_queue") or "staged-slurm-gpu")
    workflow_id = str(data.get("workflow_id") or f"staged-slurm-gpu-{uuid.uuid4().hex}")
    handle = await client.start_workflow(
        StagedSlurmGpuWorkflow.run,
        params,
        task_queue=task_queue,
        id=workflow_id,
    )
    return JSONResponse(
        status_code=200,
        content={
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id,
            "task_queue": task_queue,
        },
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

    try:
        status = await handle.query(BwbWorkflow.get_status)
        status["status_source"] = "query"
        return status
    except RPCError as exc:
        description = await handle.describe()
        status = _workflow_status_from_description(description)
        status["query_error"] = str(exc)
        return status


@app.post("/ssh_docker_workflow_status")
async def ssh_docker_workflow_status(req: Request):
    client = await Client.connect(TEMPORAL_EPT)
    data = await req.json()
    if "workflow_id" not in data:
        return JSONResponse(status_code=400, content={"message": "Missing required key workflow_id."})

    workflow_id = data["workflow_id"]
    run_id = data["run_id"] if "run_id" in data else None
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    description = await handle.describe()
    status = _workflow_status_from_description(description)
    status["workflow_id"] = workflow_id
    status["run_id"] = run_id
    status["workflow_type"] = "RemoteDockerWorkflow"
    if status["workflow_status"] == "Finished":
        result = await handle.result()
        status["result"] = _jsonable_result(result)
    return status


@app.post("/staged_slurm_gpu_workflow_status")
async def staged_slurm_gpu_workflow_status(req: Request):
    client = await Client.connect(TEMPORAL_EPT)
    data = await req.json()
    if "workflow_id" not in data:
        return JSONResponse(status_code=400, content={"message": "Missing required key workflow_id."})
    workflow_id = data["workflow_id"]
    run_id = data.get("run_id")
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    description = await handle.describe()
    status = _workflow_status_from_description(description)
    status.update({"workflow_id": workflow_id, "run_id": run_id, "workflow_type": "StagedSlurmGpuWorkflow"})
    if status["workflow_status"] == "Running":
        status.update(await handle.query(StagedSlurmGpuWorkflow.get_status))
    elif status["workflow_status"] == "Finished":
        status["result"] = _jsonable_result(await handle.result())
    return status
