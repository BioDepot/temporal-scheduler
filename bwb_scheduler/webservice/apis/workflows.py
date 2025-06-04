import uuid

import fastapi
import pydantic
from fastapi import HTTPException

from bwb_scheduler.webservice.models import MODEL_LIST
from bwb_scheduler.webservice.temporalio_client import run_worker

workflows_api_router = fastapi.APIRouter(tags=["workflows"])


@workflows_api_router.post("/workflows/{model_name}")
async def start_workflow(
    model_name, model_input_data: dict, stack_id: uuid.UUID = None
):
    try:
        base_model: pydantic.BaseModel = MODEL_LIST[model_name]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    model_instance = base_model.model_validate(model_input_data)
    print(model_instance)

    if stack_id is None:
        stack_id = uuid.uuid4()
    await run_worker(
        stack_id=stack_id,
        docker_image_name=model_instance.docker_image_name,
        docker_image_tag=model_instance.docker_image_tag,
    )
