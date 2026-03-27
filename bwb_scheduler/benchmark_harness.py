from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TERMINAL_WORKFLOW_STATUSES = {"Finished", "Failed"}


@dataclass(frozen=True)
class WorkerSpec:
    queue: str
    cpus: int
    mem_mb: int
    gpus: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def load_start_workflow_payload(
    workflow_path: str | Path,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    payload = load_json(workflow_path)
    if "workflow_def" not in payload:
        payload = {"workflow_def": payload}

    if config_path is not None:
        payload["config"] = load_json(config_path)

    return payload


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest = load_json(path)
    if "benchmarks" not in manifest or not isinstance(manifest["benchmarks"], list):
        raise ValueError("Manifest must contain a `benchmarks` list")
    return manifest


def http_json_request(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc


def wait_for_scheduler_api(
    api_base_url: str,
    timeout_seconds: int = 180,
    poll_seconds: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    url = f"{api_base_url.rstrip('/')}/openapi.json"
    while time.monotonic() < deadline:
        try:
            http_json_request("GET", url, payload=None, timeout_seconds=10)
            return
        except Exception:
            time.sleep(poll_seconds)
    raise TimeoutError(f"Scheduler API did not become ready within {timeout_seconds} seconds")


def start_workflow(api_base_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    return http_json_request(
        "POST",
        f"{api_base_url.rstrip('/')}/start_workflow",
        payload=payload,
    )


def add_worker_to_workflow(
    api_base_url: str,
    workflow_id: str,
    worker: WorkerSpec,
) -> dict[str, Any]:
    payload = {
        "workflow_id": workflow_id,
        "worker_queue": worker.queue,
        "worker_cpus": worker.cpus,
        "worker_gpus": worker.gpus,
        "worker_mem_mb": worker.mem_mb,
    }
    return http_json_request(
        "POST",
        f"{api_base_url.rstrip('/')}/add_worker_to_workflow",
        payload=payload,
    )


def get_workflow_status(
    api_base_url: str,
    workflow_id: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"workflow_id": workflow_id}
    if run_id is not None:
        payload["run_id"] = run_id
    return http_json_request(
        "POST",
        f"{api_base_url.rstrip('/')}/workflow_status",
        payload=payload,
    )


def stop_workflow(
    api_base_url: str,
    workflow_id: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"workflow_id": workflow_id}
    if run_id is not None:
        payload["run_id"] = run_id
    return http_json_request(
        "POST",
        f"{api_base_url.rstrip('/')}/stop_workflow",
        payload=payload,
    )


def parse_worker_spec(raw: dict[str, Any]) -> WorkerSpec:
    return WorkerSpec(
        queue=raw["queue"],
        cpus=int(raw["cpus"]),
        mem_mb=int(raw["mem_mb"]),
        gpus=int(raw.get("gpus", 0)),
    )


def resolve_benchmark_payload(manifest_path: Path, benchmark: dict[str, Any]) -> dict[str, Any]:
    config_path = benchmark.get("config_path")
    resolved_config = None
    if config_path is not None:
        resolved_config = manifest_path.parent / config_path

    if "request_path" in benchmark:
        payload = load_json(manifest_path.parent / benchmark["request_path"])
        if resolved_config is not None:
            payload["config"] = load_json(resolved_config)
        return payload

    if "workflow_path" in benchmark:
        return load_start_workflow_payload(
            manifest_path.parent / benchmark["workflow_path"],
            config_path=resolved_config,
        )

    raise ValueError(
        "Each benchmark must define either `request_path` or `workflow_path`"
    )


def get_benchmark_worker(
    manifest: dict[str, Any],
    benchmark: dict[str, Any],
) -> WorkerSpec | None:
    register_worker = benchmark.get("register_worker", True)
    if not register_worker:
        return None

    raw_worker = benchmark.get("worker", manifest.get("default_worker"))
    if raw_worker is None:
        raise ValueError(
            f"Benchmark `{benchmark.get('name', '<unnamed>')}` requires a worker, "
            "but neither `worker` nor `default_worker` was provided."
        )
    return parse_worker_spec(raw_worker)


def poll_until_terminal(
    api_base_url: str,
    workflow_id: str,
    run_id: str | None,
    timeout_seconds: int,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while time.monotonic() < deadline:
        last_status = get_workflow_status(api_base_url, workflow_id, run_id)
        if last_status.get("workflow_status") in TERMINAL_WORKFLOW_STATUSES:
            return last_status
        time.sleep(poll_seconds)

    raise TimeoutError(
        f"Workflow {workflow_id} did not finish within {timeout_seconds} seconds"
    )


def run_single_benchmark_iteration(
    *,
    api_base_url: str,
    benchmark_name: str,
    payload: dict[str, Any],
    worker: WorkerSpec | None,
    poll_seconds: float,
    timeout_seconds: int,
    stop_on_timeout: bool,
) -> dict[str, Any]:
    started_at = utc_now_iso()
    monotonic_start = time.monotonic()
    workflow_start = start_workflow(api_base_url, payload)
    workflow_id = workflow_start["workflow_id"]
    run_id = workflow_start.get("run_id")
    if worker is not None:
        add_worker_to_workflow(api_base_url, workflow_id, worker)

    timeout_hit = False
    try:
        final_status = poll_until_terminal(
            api_base_url=api_base_url,
            workflow_id=workflow_id,
            run_id=run_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
    except TimeoutError:
        timeout_hit = True
        if stop_on_timeout:
            stop_workflow(api_base_url, workflow_id, run_id)
        final_status = {
            "workflow_status": "TimedOut",
            "node_statuses": {},
        }

    elapsed_seconds = round(time.monotonic() - monotonic_start, 3)
    return {
        "benchmark_name": benchmark_name,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "started_at_utc": started_at,
        "finished_at_utc": utc_now_iso(),
        "elapsed_seconds": elapsed_seconds,
        "timed_out": timeout_hit,
        "worker": None if worker is None else {
            "queue": worker.queue,
            "cpus": worker.cpus,
            "mem_mb": worker.mem_mb,
            "gpus": worker.gpus,
        },
        "final_status": final_status,
    }


def run_manifest(
    manifest_path: str | Path,
    *,
    api_base_url_override: str | None = None,
    wait_for_api_seconds: int = 0,
    poll_seconds: float = 5.0,
    timeout_seconds: int = 3600,
    stop_on_timeout: bool = False,
    output_dir: str | Path = "benchmarks/results",
) -> Path:
    manifest_file = Path(manifest_path)
    manifest = load_manifest(manifest_file)
    api_base_url = api_base_url_override or manifest.get("api_base_url", "http://localhost:8000")

    if wait_for_api_seconds > 0:
        wait_for_scheduler_api(api_base_url, timeout_seconds=wait_for_api_seconds)

    benchmark_results = []
    for benchmark in manifest["benchmarks"]:
        payload = resolve_benchmark_payload(manifest_file, benchmark)
        worker = get_benchmark_worker(manifest, benchmark)

        iterations = int(benchmark.get("iterations", 1))
        for iteration in range(iterations):
            result = run_single_benchmark_iteration(
                api_base_url=api_base_url,
                benchmark_name=benchmark["name"],
                payload=payload,
                worker=worker,
                poll_seconds=poll_seconds,
                timeout_seconds=timeout_seconds,
                stop_on_timeout=stop_on_timeout,
            )
            result["iteration"] = iteration + 1
            benchmark_results.append(result)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    result_file = output_path / f"benchmark_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    result_file.write_text(
        json.dumps(
            {
                "manifest_path": str(manifest_file),
                "api_base_url": api_base_url,
                "generated_at_utc": utc_now_iso(),
                "results": benchmark_results,
            },
            indent=2,
        )
        + "\n"
    )
    return result_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run benchmark workflows against the scheduler API."
    )
    parser.add_argument("manifest", help="Path to benchmark manifest JSON")
    parser.add_argument(
        "--api-base-url",
        default=None,
        help="Override the API base URL from the manifest.",
    )
    parser.add_argument(
        "--wait-for-api-seconds",
        type=int,
        default=0,
        help="Wait for the scheduler API before starting benchmarks.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=5.0,
        help="Polling interval for workflow status.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=3600,
        help="Per-workflow timeout.",
    )
    parser.add_argument(
        "--stop-on-timeout",
        action="store_true",
        help="Terminate the workflow if it times out.",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarks/results",
        help="Directory for benchmark result JSON files.",
    )
    args = parser.parse_args()

    result_file = run_manifest(
        args.manifest,
        api_base_url_override=args.api_base_url,
        wait_for_api_seconds=args.wait_for_api_seconds,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
        stop_on_timeout=args.stop_on_timeout,
        output_dir=args.output_dir,
    )
    print(result_file)


if __name__ == "__main__":
    main()
