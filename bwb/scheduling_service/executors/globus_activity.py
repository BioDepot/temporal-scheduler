"""Durable Globus transfer activities for staged scheduler workflows."""

import asyncio
import json
import os
import shlex
from dataclasses import dataclass
from typing import List, Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError


@dataclass
class GlobusTransferItem:
    source_path: str
    destination_path: str
    recursive: bool = False


@dataclass
class GlobusTransferParams:
    source_endpoint_id: str
    destination_endpoint_id: str
    items: List[GlobusTransferItem]
    label: str
    submission_id: str
    sync_level: str = "checksum"
    verify_checksum: bool = True
    preserve_timestamp: bool = True
    timeout_seconds: int = 86400
    poll_interval_seconds: int = 15


@dataclass
class GlobusTask:
    task_id: str
    label: str


@dataclass
class GlobusTaskStatus:
    task_id: str
    status: str
    files: int = 0
    files_transferred: int = 0
    files_skipped: int = 0
    bytes_transferred: int = 0
    faults: int = 0
    fatal_error: str = ""


class GlobusActivity:
    """Invoke the authenticated Globus CLI with endpoint allowlisting."""

    def __init__(self, cli_path: str = "globus", allowed_endpoint_ids: Optional[List[str]] = None):
        self.cli_path = cli_path
        self.allowed_endpoint_ids = set(allowed_endpoint_ids or [])

    def _validate_endpoint(self, endpoint_id: str) -> None:
        if not endpoint_id:
            raise ApplicationError("Globus endpoint ID is empty", non_retryable=True)
        if self.allowed_endpoint_ids and endpoint_id not in self.allowed_endpoint_ids:
            raise ApplicationError(
                f"Globus endpoint {endpoint_id} is not in the worker allowlist",
                non_retryable=True,
            )

    @staticmethod
    def _validate_path(path: str) -> None:
        if not path.startswith("/") or "\x00" in path:
            raise ApplicationError(
                f"Globus paths must be absolute endpoint paths: {path!r}",
                non_retryable=True,
            )
        if ".." in path.split("/"):
            raise ApplicationError(
                f"Globus paths may not contain parent traversal: {path!r}",
                non_retryable=True,
            )

    async def _run(self, args: List[str], stdin: str = "", timeout: int = 300) -> dict:
        env = dict(os.environ)
        process = await asyncio.create_subprocess_exec(
            self.cli_path,
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin.encode("utf-8")),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise ApplicationError(
                f"Globus CLI timed out after {timeout}s: {' '.join(args[:3])}",
                non_retryable=False,
            ) from exc

        stdout_text = stdout.decode("utf-8", errors="replace")
        stderr_text = stderr.decode("utf-8", errors="replace")
        if process.returncode != 0:
            raise ApplicationError(
                f"Globus CLI failed with exit code {process.returncode}: "
                f"{stderr_text[-1000:].strip() or stdout_text[-1000:].strip()}",
                non_retryable=False,
            )
        try:
            return json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise ApplicationError(
                f"Globus CLI returned invalid JSON: {stdout_text[-1000:]}",
                non_retryable=True,
            ) from exc

    @activity.defn
    async def submit_transfer(self, params: GlobusTransferParams) -> GlobusTask:
        self._validate_endpoint(params.source_endpoint_id)
        self._validate_endpoint(params.destination_endpoint_id)
        if not params.items:
            raise ApplicationError("Globus transfer has no items", non_retryable=True)
        if not params.submission_id:
            raise ApplicationError(
                "Globus submission_id is required for retry-safe submission",
                non_retryable=True,
            )

        batch_lines = []
        for item in params.items:
            self._validate_path(item.source_path)
            self._validate_path(item.destination_path)
            prefix = "--recursive " if item.recursive else ""
            batch_lines.append(
                f"{prefix}{shlex.quote(item.source_path)} {shlex.quote(item.destination_path)}"
            )
        batch = "\n".join(batch_lines) + "\n"

        args = [
            "transfer",
            f"{params.source_endpoint_id}:",
            f"{params.destination_endpoint_id}:",
            "--batch", "-",
            "--format", "json",
            "--notify", "off",
            "--label", params.label,
            "--submission-id", params.submission_id,
            "--sync-level", params.sync_level,
        ]
        args.append("--verify-checksum" if params.verify_checksum else "--no-verify-checksum")
        if params.preserve_timestamp:
            args.append("--preserve-mtime")

        response = await self._run(args, stdin=batch)
        task_id = str(response.get("task_id") or "")
        if not task_id:
            raise ApplicationError(
                f"Globus transfer response did not include task_id: {response}",
                non_retryable=True,
            )
        return GlobusTask(task_id=task_id, label=params.label)

    @activity.defn
    async def get_task_status(self, task_id: str) -> GlobusTaskStatus:
        response = await self._run(["task", "show", task_id, "--format", "json"])
        fatal_error = response.get("fatal_error")
        if isinstance(fatal_error, dict):
            fatal_error = json.dumps(fatal_error, sort_keys=True)
        return GlobusTaskStatus(
            task_id=task_id,
            status=str(response.get("status") or "UNKNOWN").upper(),
            files=int(response.get("files") or 0),
            files_transferred=int(response.get("files_transferred") or 0),
            files_skipped=int(response.get("files_skipped") or 0),
            bytes_transferred=int(response.get("bytes_transferred") or 0),
            faults=int(response.get("faults") or 0),
            fatal_error=str(fatal_error or ""),
        )
