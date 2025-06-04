from unittest import TestCase
import asyncio

from temporalio.worker import Worker

from bwb_scheduler.models.workflow_definition import Workflow
from temporalio.client import Client as WorkflowClient

from bwb_scheduler.workflow_stubs.main_activity_stub import parse_workflow_json
from bwb_scheduler.workflow_stubs.main_workflow_stub import MainWorkflow


async def run_workflow():
    client = await WorkflowClient.connect("localhost:7233")
    handle = await client.start_workflow(
        MainWorkflow.run, "workflow", id="your name", task_queue="default"
    )
    # handle = await client.start_workflow(
    #     MainWorkflow, id="my-workflow-id", task_queue="my-task-queue"
    # )
    await handle.result()


async def run_worker(stop_event: asyncio.Event):
    client = await WorkflowClient.connect("127.0.0.1:7233", namespace="default")

    print("Worker launching")
    worker = Worker(
        client,
        task_queue="default",
        workflows=[MainWorkflow],
        activities=[
            parse_workflow_json,
        ],
    )
    async with worker:
        print("Worker running")
        await stop_event.wait()
        print("Worker done")


class TestWorkflowModels(TestCase):
    def test_workflow(self):
        example_data = {
            "workflow": "Project Development",
            "steps": [
                {
                    "name": "Initiation",
                    "stepName": "Initiation",
                    "execution": {
                        "name": "Initiation",
                        "type": "docker",
                        "docker_command": "echo 'Hello World!'",
                    },
                    "subSteps": [
                        {
                            "name": "Define Scope",
                            "execution": {
                                "name": "Define Scope",
                                "type": "shell",
                                "shell_command": "echo 'Hello World!'",
                            },
                        },
                        {
                            "name": "Stakeholder Analysis",
                            "execution": {
                                "name": "Stakeholder Analysis",
                                "type": "shell",
                                "shell_command": "echo 'Hello World!'",
                            },
                        },
                    ],
                },
            ],
        }

        # Create a Workflow instance from the example data
        # workflow_instance = Workflow(**example_data)
        stop_worker = asyncio.Event()
        # future1 = asyncio.create_task(run_workflow())
        # future2 = asyncio.create_task(run_worker(stop_worker))
        # future3 = asyncio.create_task(asyncio.gather((future1, future2)))

        asyncio.run(run_workflow())

        raise ValueError
