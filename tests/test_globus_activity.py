import asyncio

import pytest
from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.executors.globus_activity import (
    GlobusActivity,
    GlobusTransferItem,
    GlobusTransferParams,
)


def _params() -> GlobusTransferParams:
    return GlobusTransferParams(
        source_endpoint_id="source-endpoint",
        destination_endpoint_id="destination-endpoint",
        items=[
            GlobusTransferItem(
                source_path="/mnt/pikachu/cardiac pilot/",
                destination_path="/ocean/project/cardiac pilot/",
                recursive=True,
            )
        ],
        label="cardiac-pilot-stage-in",
        submission_id="submission-123",
    )


def test_submit_transfer_uses_retry_safe_checksum_arguments(monkeypatch):
    activity = GlobusActivity(
        cli_path="/usr/bin/globus",
        allowed_endpoint_ids=["source-endpoint", "destination-endpoint"],
    )
    captured = {}

    async def fake_run(args, stdin="", timeout=300):
        captured.update(args=args, stdin=stdin, timeout=timeout)
        return {"task_id": "task-456"}

    monkeypatch.setattr(activity, "_run", fake_run)
    task = asyncio.run(activity.submit_transfer(_params()))

    assert task.task_id == "task-456"
    assert captured["args"][0] == "transfer"
    assert captured["args"][captured["args"].index("--submission-id") + 1] == "submission-123"
    assert captured["args"][captured["args"].index("--sync-level") + 1] == "checksum"
    assert "--verify-checksum" in captured["args"]
    assert "--preserve-mtime" in captured["args"]
    assert captured["stdin"].startswith("--recursive ")
    assert "'/mnt/pikachu/cardiac pilot/'" in captured["stdin"]


def test_submit_transfer_rejects_endpoint_outside_allowlist():
    activity = GlobusActivity(allowed_endpoint_ids=["source-endpoint"])

    with pytest.raises(ApplicationError, match="not in the worker allowlist"):
        asyncio.run(activity.submit_transfer(_params()))


def test_submit_transfer_rejects_parent_traversal():
    activity = GlobusActivity(
        allowed_endpoint_ids=["source-endpoint", "destination-endpoint"]
    )
    params = _params()
    params.items[0].source_path = "/mnt/pikachu/../secret"

    with pytest.raises(ApplicationError, match="parent traversal"):
        asyncio.run(activity.submit_transfer(params))


def test_task_status_parses_globus_response(monkeypatch):
    activity = GlobusActivity()

    async def fake_run(args, stdin="", timeout=300):
        return {
            "status": "SUCCEEDED",
            "files": 4,
            "files_transferred": 4,
            "files_skipped": 0,
            "bytes_transferred": 1024,
            "faults": 0,
        }

    monkeypatch.setattr(activity, "_run", fake_run)
    status = asyncio.run(activity.get_task_status("task-456"))

    assert status.status == "SUCCEEDED"
    assert status.files_transferred == 4
    assert status.bytes_transferred == 1024
