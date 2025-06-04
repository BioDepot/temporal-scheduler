from pydantic import BaseModel, Field


class OWdeseq2Inputs(BaseModel):
    Trigger: str = Field(
        None,
        title="Trigger",
        description="Trigger",
        handler="handleInputsTrigger",
    )
    countsFile: str = Field(
        None,
        title="countsFile",
        description="countsFile",
        handler="handleInputscountsFile",
    )


class OWdeseq2Outputs(BaseModel):
    topGenesFile: str = Field(
        None,
        title="topGenesFile",
        description="topGenesFile",
    )


class OWdeseq2(BaseModel):
    name: str = Field(
        "deseq2",
        title="Name of the model",
        allow_mutation=False,
    )
    description: str = Field(
        "Differential expression using DESeq2 package",
        title="Description of the model",
        allow_mutation=False,
    )

    priority: int = Field(
        14,
        title="Priority of the model",
        description="Priority of the model",
    )
    want_main_area: bool = Field(
        False,
        title="Want main area",
        description="Whether the widget wants the main area",
    )
    docker_image_name: str = Field(
        "biodepot/deseq2",
        title="Docker image name",
        description="Docker image name",
    )
    docker_image_tag: str = Field(
        "bioc-r_3.16-ubuntu-22.04-r-4.2.2__375472a0__186da45b__add50ffe",
        title="Docker image tag",
        description="Docker image tag",
    )

    inputs: OWdeseq2Inputs = Field(
        None,
        title="Inputs",
        description="Inputs",
    )
    outputs: OWdeseq2Outputs = Field(
        None,
        title="Outputs",
        description="Outputs",
    )
