import logging
import random
import string
import sys
import time

import uvicorn
from fastapi import Request, FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette_context.middleware import RawContextMiddleware

from .apis import schemas_api_router, workflows_api_router

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id")

        # If not present, generate a new request ID
        if not request_id:
            request_id = "".join(random.choices(string.ascii_letters, k=16))

        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


class ProcessTimeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        request.state.start_time = start_time
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response


class ContextMiddleware(RawContextMiddleware):
    async def set_context(self, request: Request):
        return {
            "request_id": request.state.request_id,
            "start_time": request.state.start_time,
        }


def create_app(*args, **kwargs):
    app = FastAPI(*args, **kwargs, docs_url="/")

    # Add Middleware
    app.add_middleware(ContextMiddleware)
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(ProcessTimeMiddleware)

    return app


def start_app():
    sys.argv.insert(1, "bwb_scheduler.webservice.web_app:app")
    uvicorn.main()


app = create_app()
app.include_router(schemas_api_router, prefix="/api/v1")
app.include_router(workflows_api_router, prefix="/api/v1")
