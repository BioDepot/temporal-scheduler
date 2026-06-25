import asyncio

import pytest

pytest.importorskip("temporalio")

from bwb.scheduling_service.run_bwb_workflow import start_scheme


class _FakeHandle:
    id = "workflow-id"
    first_execution_run_id = "run-id"


class _FakeTemporalClient:
    def __init__(self):
        self.started = None

    async def start_workflow(self, workflow, params, task_queue, id):
        self.started = {
            "workflow": workflow,
            "params": params,
            "task_queue": task_queue,
            "id": id,
        }
        return _FakeHandle()


def test_start_scheme_passes_use_singularity_to_workflow_params():
    client = _FakeTemporalClient()

    workflow_id, run_id = asyncio.run(
        start_scheme(
            {"run_id": "local-echo-smoke", "use_local_storage": True, "nodes": [], "links": []},
            {},
            client,
            "scheduler-queue",
            use_singularity=False,
        )
    )

    assert workflow_id == "workflow-id"
    assert run_id == "run-id"
    assert client.started["task_queue"] == "scheduler-queue"
    assert client.started["params"].use_singularity is False
