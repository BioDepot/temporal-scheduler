import logging
import subprocess

from temporalio import activity

from bwb_scheduler.models.workflow_definition import MainWorkflowDTO


@activity.defn(name="parse_workflow_json")
async def parse_workflow_json(workflow_json: str) -> dict:
    logging.info(f"Parsing workflow JSON: {workflow_json}")

    workflow_dto = MainWorkflowDTO.model_validate_json(workflow_json)
    logging.info(f"Workflow DTO: {workflow_dto}")

    return workflow_dto.dict()


@activity.defn(name="execute_shell_command")
async def execute_shell_command(shell_command: str) -> str:
    logging.info(f"Executing shell command: {shell_command}")
    result = subprocess.run(
        shell_command.split(),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.stdout:
        logging.info("STDOUT:\n%s", result.stdout)
    if result.stderr:
        logging.error("STDERR:\n%s", result.stderr)

    return shell_command


@activity.defn(name="execute_docker_command")
async def execute_docker_command(docker_command: str) -> str:
    logging.info(f"Executing docker command: {docker_command}")
    result = subprocess.run(
        docker_command.split(),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.stdout:
        logging.info("STDOUT:\n%s", result.stdout)
    if result.stderr:
        logging.error("STDERR:\n%s", result.stderr)

    return docker_command
