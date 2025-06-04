from typing import List, Optional, Union, Literal

from pydantic import BaseModel


# Base model for an execution step
class ExecutionStep(BaseModel):
    name: str


# Specific model for a Docker execution step
class DockerExecution(ExecutionStep):
    docker_command: str
    type: str = Literal["docker"]


# Specific model for a Shell execution step
class ShellExecution(ExecutionStep):
    shell_command: str
    type: str = Literal["shell"]


# Union of different execution step types
ExecutionType = Union[DockerExecution, ShellExecution]


# Define the model for a SubStep
class SubStep(BaseModel):
    name: str
    subSteps: Optional[List["SubStep"]] = None
    execution: ExecutionType


# Enable recursive model by assigning the list of SubStep to itself
SubStep.model_rebuild()


# Define the model for a Step, which contains a list of SubSteps
class Step(BaseModel):
    stepName: str
    subSteps: List[SubStep]


# Define the top-level model for the Workflow
class MainWorkflowDTO(BaseModel):
    workflow: str
    steps: List[Step]
