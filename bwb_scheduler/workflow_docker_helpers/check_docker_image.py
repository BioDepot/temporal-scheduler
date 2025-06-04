import logging
from datetime import timedelta

from temporalio import activity, workflow

from bwb_scheduler.workflow_docker_helpers.pull_docker_image import pull_docker_image


@workflow.defn
class CheckDockerImageExists:
    @workflow.run
    async def check_docker_image_exists(self, image_name: str) -> bool:
        # Check if Docker image exists
        image_exists = await workflow.execute_activity(
            check_docker_image_exists,
            image_name,
            schedule_to_close_timeout=timedelta(seconds=5),
        )
        if not image_exists:
            logging.error(f"Docker image does not exist: {image_name}")

            image_pull = await workflow.execute_activity(
                pull_docker_image,
                image_name,
                schedule_to_close_timeout=timedelta(seconds=5),
            )
            if not image_pull:
                raise Exception(f"Error pulling Docker image: {image_name}")
        return True


@activity.defn
async def check_docker_image_exists(image_name: str) -> bool:
    import subprocess

    try:
        subprocess.run(["docker", "inspect", "--type=image", image_name], check=True)
        return True
    except subprocess.CalledProcessError:
        return False
