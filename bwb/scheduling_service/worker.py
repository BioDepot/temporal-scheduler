import os
import json
import math
import subprocess
import uuid
import shutil
import asyncio
import paramiko
import argparse
import psutil
import re

import temporalio.client
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker, FixedSizeSlotSupplier, WorkerTuner
from dotenv import load_dotenv
from typing import Tuple

from bwb.scheduling_service.executors.build_node_image_activity import build_node_img
from bwb.scheduling_service.executors.generate_cmds_activity import generate_node_cmds
from bwb.scheduling_service.scheduler_types import ResourceVector, WorkerResources
from bwb.scheduling_service.executors.local_activities import setup_volumes_on_worker, run_workflow_cmd, download_file_deps, \
    sync_dir
from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
from bwb.scheduling_service.bwb_workflow import BwbWorkflow
from bwb.scheduling_service.executors.slurm_poller import SlurmPoller
from bwb.scheduling_service.executors.generic import get_scheduler_child_queue, get_worker_heartbeat_queue
from bwb.scheduling_service.executors.worker_poller import WorkerPoller
from bwb.scheduling_service.executors.worker_poller_activities import assign_workers, heartbeat_activity


def verify_env():
    required_dotenv_keys = {
        "SCHED_STORAGE_DIR",
        "TEMPORAL_ENDPOINT_URL",
        "MINIO_ENDPOINT_URL",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY"
    }

    load_dotenv()
    for key in required_dotenv_keys:
        if os.getenv(key) is None:
            print(f"FAILING: Missing required .env key {key}.")
            if key.startswith("MINIO"):
                print("NOTE: You can leave the MINIO_* keys blank "
                      "at your own hazard if you intend to schedule "
                      "workflows using only local storage.")
            else:
                exit(1)

    minio_ept = os.getenv("MINIO_ENDPOINT_URL")
    if not minio_ept.startswith("http://") or minio_ept.startswith("https://"):
        print(f"`MINIO_ENDPOINT_URL` {minio_ept} is misformatted, must begin with http(s)://")
        exit(1)

    if shutil.which("singularity") is None:
        print("WARNING: `singularity` is not installed; you may proceed at "
              "your own hazard, but attempting to schedule singularity "
              "workflows on this worker will fail. It is strongly advised "
              "to install singularity CE > 3.0.")

    if shutil.which("docker") is None:
        print("WARNING: `docker` is not installed.")

    if shutil.which("rsync") is None:
        print("WARNING: `rsync` is not installed; you may proceed at "
              "your own hazard, but attempting to schedule SLURM "
              "workflows on this worker will fail. It is strongly advised "
              "to install rsync.")


async def get_worker(given_queue_name=None, cpus=None, ram_mb=None, gpus=0) -> Tuple[WorkerResources, Worker, Worker]:
    load_dotenv()
    temporal_ept = os.getenv("TEMPORAL_ENDPOINT_URL")
    print(f"Connecting to temporal endpoint {temporal_ept}")
    client = await Client.connect(temporal_ept)
    queue_name = given_queue_name
    if queue_name is None:
        queue_name = str(uuid.uuid4())
    print(f"Listening on queue {queue_name}")

    # We use a push-based system to make sure job RAM / CPU reqs
    # don't overwhelm worker, so we want to essentially disregard
    # the temporal scheduler's own rate-limiting decisions. We do
    # this by just setting really high slot numbers for (local)
    # activities.
    workflow_supplier = FixedSizeSlotSupplier(num_slots=5)
    activity_supplier = FixedSizeSlotSupplier(num_slots=1000)
    local_activity_supplier = FixedSizeSlotSupplier(num_slots=1000)
    tuner = WorkerTuner.create_composite(
        workflow_supplier=workflow_supplier,
        activity_supplier=activity_supplier,
        local_activity_supplier=local_activity_supplier
    )

    heartbeat_queue = get_worker_heartbeat_queue(queue_name)
    heartbeat_worker = Worker(
        client,
        task_queue=heartbeat_queue,
        workflows=[],
        activities=[
            heartbeat_activity,
        ],
        tuner=tuner
    )

    worker = Worker(
        client,
        task_queue=queue_name,
        workflows=[],
        activities=[
            setup_volumes_on_worker,
            download_file_deps,
            generate_node_cmds,
            build_node_img,
            run_workflow_cmd,
            sync_dir
        ],
        tuner=tuner
    )

    real_ram = ram_mb
    if real_ram is None:
        real_ram = math.floor((psutil.virtual_memory().total // 1024 ** 2) * 0.7)
        print(f"No RAM value specified, using 70% system total ({real_ram} MB).")
    real_cpus = cpus
    if real_cpus is None:
        real_cpus = math.floor(psutil.cpu_count() * 0.75)
        print(f"No CPU count specified, using all {real_cpus} of them.")

    resources = ResourceVector(real_cpus, gpus, real_ram)
    worker_resources = WorkerResources(resources=resources, queue_id=queue_name)
    return worker_resources, heartbeat_worker, worker


async def get_child_handle(workflow_id: str, handle: WorkflowHandle, client: Client) -> WorkflowHandle:
    while not await handle.query("child_workflow_started"):
        await asyncio.sleep(5)
    child_id = get_scheduler_child_queue(workflow_id)
    child_handle = client.get_workflow_handle(child_id)
    return child_handle


async def register_worker_with_workflow(workflow_id: str, handle: WorkflowHandle,
                                        client: Client, worker: WorkerResources) -> bool:
    try:
        # SLURM-only workflows don't use a local WorkerPoller child workflow,
        # so worker registration is a no-op for them.
        if not await handle.query("needs_worker_registration"):
            return True
        child_handle = await get_child_handle(workflow_id, handle, client)
    except temporalio.service.RPCError:
        return False
    await child_handle.signal("add_worker", worker)
    return True


async def get_scheduler_worker():
    load_dotenv()
    temporal_ept = os.getenv("TEMPORAL_ENDPOINT_URL")

    client = await Client.connect(temporal_ept)
    worker = Worker(
        client,
        task_queue="scheduler-queue",
        workflows=[BwbWorkflow, WorkerPoller, SlurmPoller],
        activities=[
            assign_workers,
            generate_node_cmds,
            build_node_img
        ]
    )
    print("Listening on `scheduler-queue`")
    return "scheduler-queue", worker


async def get_slurm_worker(slurm_config):
    if "ip_addr" not in slurm_config:
        print(f"ERROR: No key ip_addr in slurm config")
        exit(1)
    if "user" not in slurm_config:
        print(f"ERROR: No key user in slurm config")
        exit(1)
    if "storage_dir" not in slurm_config:
        print(f"ERROR: No storage_dir in slurm config")
        exit(1)

    # TODO: Add failure mode if key doesn't exist for user,
    # remove password as an env key.
    load_dotenv()
    ssh_password = os.getenv("SSH_PASSWORD")
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_port = int(slurm_config.get("port", 22))
    ssh_client.connect(slurm_config["ip_addr"],
                       port=ssh_port,
                       username=slurm_config["user"],
                       password=ssh_password)
    transport = ssh_client.get_transport()
    transport.set_keepalive(60)
    xfer_addr = None
    if "transfer_addr" in slurm_config:
        xfer_addr = slurm_config["transfer_addr"]

    slurm_activity = SlurmActivity(
        ssh_client,
        slurm_config["user"],
        slurm_config["ip_addr"],
        slurm_config["storage_dir"],
        xfer_addr,
        ssh_port=ssh_port,
        xfer_port=int(slurm_config.get("transfer_port", ssh_port))
    )

    slurm_queue = f"{slurm_config['user']}@{slurm_config['ip_addr']}:{ssh_port}"
    temporal_ept = os.getenv("TEMPORAL_ENDPOINT_URL")
    temporal_client = await Client.connect(temporal_ept)
    worker = Worker(
        temporal_client,
        task_queue=slurm_queue,
        workflows=[],
        activities=[
            setup_volumes_on_worker,
            download_file_deps,
            generate_node_cmds,
            build_node_img,
            run_workflow_cmd,
            sync_dir,
            slurm_activity.start_slurm_job,
            slurm_activity.poll_slurm,
            slurm_activity.get_slurm_outputs,
            slurm_activity.setup_login_node_volumes,
            slurm_activity.upload_to_slurm_login_node,
            slurm_activity.download_from_slurm_login_node
        ],
    )

    return slurm_queue, worker


def parse_ram_to_mb(ram_str):
    match = re.match(r'^(\d+)\s*(GB|MB|KB)$', ram_str, re.IGNORECASE)
    if not match:
        raise argparse.ArgumentTypeError("Invalid RAM format. Use '10 GB', '500 MB', etc.")
    value, unit = int(match.group(1)), match.group(2).upper()

    multiplier = {'KB': 1024 ** -1, 'MB': 1, 'GB': 1024}
    return value * multiplier[unit]


def check_system_resources(ram_mb, cpu_cores, gpus):
    available_ram_mb = psutil.virtual_memory().total // 1024 ** 2
    available_cpu_cores = psutil.cpu_count()

    if ram_mb is not None and ram_mb > available_ram_mb:
        print(f"FATAL: Requested RAM {ram_mb} MB exceeds available {available_ram_mb} MB.")
        return False
    if cpu_cores is not None and cpu_cores > available_cpu_cores:
        print(f"FATAL: Requested CPU cores {cpu_cores} exceeds available {available_cpu_cores}.")
        return False

    # TODO: Add GPU count verification code that works in a cross-platform way.
    if gpus > 0:
        gpu_docker_test = subprocess.run(
            "docker run --rm --gpus all nvidia/cuda:12.2.0-base-ubuntu22.04 nvidia-smi"
        )
        if gpu_docker_test.returncode != 0:
            print("FATAL: docker nvidia runtime not installed")
            return False

        # TODO: Add singularity GPU test.

    return True


async def run_scheduler_worker():
    scheduling_queue, worker = await get_scheduler_worker()
    await worker.run()

async def run_slurm_worker(config):
    slurm_queue, worker = await get_slurm_worker(config)
    await worker.run()


async def run_regular_worker(queue_name, ram, cpus, gpus):
    queue, heartbeat_worker, worker = await get_worker(queue_name, cpus, ram, gpus)
    worker_task = asyncio.create_task(worker.run())
    worker_heartbeat_task = asyncio.create_task(heartbeat_worker.run())
    await asyncio.gather(worker_task, worker_heartbeat_task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start Temporal Worker for BWB Scheduler.')
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    parser_sched_worker = subparsers.add_parser("scheduler", help="Run worker to perform scheduling.")
    parser_regular_worker = subparsers.add_parser("regular", help="Run regular worker.")
    parser_slurm_worker = subparsers.add_parser("slurm", help="Run regular worker.")

    parser_regular_worker.add_argument(
        '--ram',
        type=parse_ram_to_mb,
        help='Amount of RAM (e.g., "20 GB", "500 MB")'
    )
    parser_regular_worker.add_argument(
        '--cpu_cores',
        type=int,
        help='Number of CPU cores'
    )
    parser_regular_worker.add_argument(
        '--gpus',
        type=int,
        default=0,
        help='Number of GPUs'
    )
    parser_regular_worker.add_argument(
        '--queue',
        type=str,
        help='Queue to which this worker will listen'
    )
    parser_slurm_worker.add_argument(
        '--config',
        type=str,
        help='Config file to slurm'
    )

    args = parser.parse_args()
    if args.subcommand == "scheduler":
        verify_env()
        asyncio.run(run_scheduler_worker())
    elif args.subcommand == "regular":
        if not check_system_resources(args.ram, args.cpu_cores, args.gpus):
            exit(1)
        print("Requested resources are available.")
        asyncio.run(run_regular_worker(args.queue, args.ram, args.cpu_cores, args.gpus))
    elif args.subcommand == "slurm":
        with open(args.config, "r") as cf:
            config = json.load(cf)

        asyncio.run(run_slurm_worker(config["executors"]["slurm"]))
