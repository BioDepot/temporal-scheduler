import asyncio
import logging
from datetime import timedelta

from temporalio import workflow

from bwb_scheduler.models.workflow_definition import (
    MainWorkflowDTO,
    SubStep,
    DockerExecution,
    ExecutionType,
)
from bwb_scheduler.workflow_stubs.main_activity_stub import (
    parse_workflow_json,
    execute_shell_command,
    execute_docker_command,
)


@workflow.defn
class ParseJSONWorkflow:
    @workflow.run
    async def run(self, workflow_json: str) -> dict:
        return await workflow.execute_activity(
            parse_workflow_json,
            workflow_json,
            schedule_to_close_timeout=timedelta(seconds=5),
        )


@workflow.defn
class MainWorkflow:
    @staticmethod
    def __add_execution__(execution: ExecutionType):
        if isinstance(execution, DockerExecution):
            return workflow.execute_activity(
                execute_docker_command,
                execution.docker_command,
                schedule_to_close_timeout=timedelta(seconds=5),
                activity_id=execution.name,
            )
        else:
            return workflow.execute_activity(
                execute_shell_command,
                execution.shell_command,
                schedule_to_close_timeout=timedelta(seconds=5),
                activity_id=execution.name,
            )

    def recursive_parse(self, step_dto: SubStep, futures=[]):
        print("parsing", step_dto.execution)
        futures.append(self.__add_execution__(step_dto.execution))

        local_futures = []
        for step in step_dto.subSteps or []:
            self.recursive_parse(step, local_futures)
        futures.append(asyncio.gather(*local_futures))

    @workflow.run
    async def run(self, name: str) -> str:
        models: MainWorkflowDTO = MainWorkflowDTO.model_validate(
            await workflow.execute_child_workflow(
                ParseJSONWorkflow.run, name, id="parsing-workflow-dto"
            )
        )

        futures = []
        for model in models.steps:
            for sub_step in model.subSteps:
                logging.info(sub_step.execution)
                self.recursive_parse(sub_step, futures)

        await asyncio.gather(*futures)
