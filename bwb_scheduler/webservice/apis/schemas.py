from fastapi import HTTPException

from bwb_scheduler.webservice.models import MODEL_LIST
import fastapi

schemas_api_router = fastapi.APIRouter(tags=["schemas"])


@schemas_api_router.get("/schemas/{model_name}")
def get_schema(model_name: str):
    try:
        base_model = MODEL_LIST[model_name]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    return base_model.schema()


@schemas_api_router.get("/schemas")
def list_schemas():
    return list(MODEL_LIST.keys())
