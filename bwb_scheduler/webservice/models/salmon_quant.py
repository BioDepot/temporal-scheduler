from pydantic import BaseModel, Field


class SalmonQuantInputs(BaseModel):
    index: str = Field(
        None,
        title="index",
        description="index",
        handler="handleInputsindex",
    )
    trigger: str = Field(
        None,
        title="trigger",
        description="trigger",
        handler="handleInputstrigger",
    )
    mates1: str = Field(
        None,
        title="mates1",
        description="mates1",
        handler="handleInputsmates1",
    )
    mates2: str = Field(
        None,
        title="mates2",
        description="mates2",
        handler="handleInputsmates2",
    )
    unmatedReads: str = Field(
        None,
        title="unmatedReads",
        description="unmatedReads",
        handler="handleInputsunmatedReads",
    )
    outputDirs: str = Field(
        None,
        title="outputDirs",
        description="outputDirs",
        handler="handleInputsoutputDirs",
    )


class SalmonQuantOutputs(BaseModel):
    trigger: str = Field(
        None,
        title="trigger",
        description="trigger",
    )


class SalmonQuant(BaseModel):
    name: str = Field(
        "SalmonQuant",
        title="Name of the model",
        allow_mutation=False,
    )
    description: str = Field(
        "Alignment and quantification of reads from fastq files",
        title="Description of the model",
        allow_mutation=False,
    )

    priority: int = Field(
        4,
        title="Priority of the model",
        description="Priority of the model",
    )
    want_main_area: bool = Field(
        False,
        title="Want main area",
        description="Whether the widget wants the main area",
    )
    docker_image_name: str = Field(
        "biodepot/salmon",
        title="Docker image name",
        description="Docker image name",
    )
    docker_image_tag: str = Field(
        "latest",
        title="Docker image tag",
        description="Docker image tag",
    )

    inputs: SalmonQuantInputs = Field(
        None,
        title="SalmonQuantInputs",
        description="SalmonQuantInputs",
    )
    outputs: SalmonQuantOutputs = Field(
        None,
        title="SalmonQuantOutputs",
        description="SalmonQuantOutputs",
    )
