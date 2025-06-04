import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from bwb_scheduler.config.scheduler_config import SchedulerConfig
from bwb_scheduler.workflow_stubs.main_activity_stub import (
    parse_workflow_json,
    execute_docker_command,
    execute_shell_command,
)
from bwb_scheduler.workflow_stubs.main_workflow_stub import (
    MainWorkflow,
    ParseJSONWorkflow,
)


async def run_worker(stop_event: asyncio.Event):
    client = await Client.connect(
        target_host=SchedulerConfig().get_temporal_connection_string(),
        namespace="default",
    )

    worker = Worker(
        client,
        task_queue="default",
        workflows=[MainWorkflow, ParseJSONWorkflow],
        activities=[parse_workflow_json, execute_docker_command, execute_shell_command],
    )
    async with worker:
        print("Worker running")
        await stop_event.wait()
        print("Worker done")


async def main():
    stop_worker = asyncio.Event()
    await run_worker(stop_worker)
    stop_worker.set()


def cli():
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

if __name__ == "__main__":
    asyncio.run(main())
