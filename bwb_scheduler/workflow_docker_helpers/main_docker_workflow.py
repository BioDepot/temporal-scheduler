import logging
from datetime import timedelta

from temporalio import workflow

from bwb_scheduler.workflow_docker_helpers.execute_docker_image import (
    execute_docker_image,
)
import msgpack


@workflow.defn
class DockerWorkflow:
    @workflow.run
    async def docker_workflow(self, image_name: str, stack_id, node_packed) -> None:
        node = msgpack.unpackb(node_packed)
        logging.error(f"Node: {node}, not processed yet")

        image_exists = await workflow.execute_child_workflow(
            id="check_docker_image_exists",
            workflow="CheckDockerImageExists",
            args=[image_name],
        )
        if not image_exists:
            raise Exception(f"Docker image does not exist: {image_name}")

        # Execute Docker image and stream logs
        logs = await workflow.execute_activity(
            execute_docker_image,
            args=(
                image_name,
                stack_id,
                node["node"]["name"],
            ),
            schedule_to_close_timeout=timedelta(seconds=5),
        )

        # Handle the logs as needed (e.g., print them)
        print(logs)
