from __future__ import annotations

from copy import deepcopy
from typing import Any


RESOLVED_WORKFLOW_SCHEMA_ID = "biodepot.resolved_workflow/v1"


def is_resolved_workflow_payload(payload: dict[str, Any]) -> bool:
    return "resolved_workflow" in payload or payload.get("schema") == RESOLVED_WORKFLOW_SCHEMA_ID


def _default_parameters(*, use_scheduler: bool, use_gpu: bool, iterate: bool, n_workers: int) -> dict[str, Any]:
    return {
        "useScheduler": use_scheduler,
        "useGpu": use_gpu,
        "iterate": iterate,
        "nWorkers": n_workers,
        "runMode": 2,
        "repeat": False,
        "exportGraphics": False,
        "controlAreaVisible": True,
        "__version__": 1,
        "inputConnectionsStore": {},
        "triggerReady": {},
        "runTriggers": [],
        "iterateSettings": {
            "iterableAttrs": [],
            "nWorkers": n_workers,
        },
        "optionsChecked": {},
    }


def _lower_resolved_node(node: dict[str, Any]) -> dict[str, Any]:
    launch = deepcopy(node.get("launch") or {})
    resources = deepcopy(node.get("resources") or {})
    scheduler_controls = deepcopy(node.get("scheduler_controls") or {})

    command = launch.get("command")
    if not isinstance(command, list) or not command:
        raise ValueError("resolved workflow nodes must define non-empty launch.command as a list of strings")

    if any(not isinstance(part, str) or not part.strip() for part in command):
        raise ValueError("resolved workflow launch.command entries must be non-empty strings")

    use_scheduler = bool(scheduler_controls.get("useScheduler", True))
    use_gpu = bool(scheduler_controls.get("useGpu", resources.get("gpus", 0) > 0))
    iterate = bool(scheduler_controls.get("iterate", False))
    n_workers = int(scheduler_controls.get("nWorkers", 1))

    lowered = {
        "id": int(node["id"]),
        "title": node.get("title", f"node-{node['id']}"),
        "description": node.get("description", node.get("title", f"node-{node['id']}")),
        "image_name": node["image_name"],
        "command": command,
        # Transitional compatibility path: no BWB arg rendering required.
        "arg_types": {},
        "parameters": _default_parameters(
            use_scheduler=use_scheduler,
            use_gpu=use_gpu,
            iterate=iterate,
            n_workers=n_workers,
        ),
        "required_parameters": [],
        "options_checked": {},
        "cores": int(resources.get("cores", 1)),
        "mem_mb": int(resources.get("mem_mb", 2048)),
        "gpus": int(resources.get("gpus", 0)),
        "slots": int(scheduler_controls.get("slots", 2)),
        "async": bool(node.get("async", False)),
        "barrier_for": node.get("barrier_for"),
        "iterate": iterate,
        "iterate_settings": deepcopy(
            node.get("iterate_settings")
            or {
                "iterableAttrs": [],
                "nWorkers": n_workers,
            }
        ),
        "static_env": deepcopy(launch.get("env") or {}),
        "port_mappings": deepcopy(node.get("port_mappings") or []),
        "port_vars": deepcopy(node.get("port_vars")),
    }

    if "input_files" in node:
        lowered["input_files"] = list(node["input_files"])
    if "output_files" in node:
        lowered["output_files"] = list(node["output_files"])
    if "deletable_files" in node:
        lowered["deletable_files"] = list(node["deletable_files"])

    return lowered


def _lower_link(link: dict[str, Any]) -> dict[str, Any]:
    """Map a v1 link onto v0 link shape.

    The v1 schema names link endpoints `source_output` / `sink_input`
    (typed output→input binding by name). The downstream Python scheduler
    graph code (graph_manager.py, ir_to_scheduler.py, ows_to_networkx_graph.py)
    consumes only `source_channel` / `sink_channel` (legacy BWB widget
    channel names). This shim renames the keys during lowering so v1
    payloads execute correctly through the existing executor, while
    preserving the cleaner v1 contract upstream.
    """
    out = deepcopy(link)
    if "source_output" in out:
        out["source_channel"] = out.pop("source_output")
    if "sink_input" in out:
        out["sink_channel"] = out.pop("sink_input")
    return out


def lower_resolved_workflow_to_workflow_def(resolved_workflow: dict[str, Any]) -> dict[str, Any]:
    nodes = resolved_workflow.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("resolved_workflow must contain a non-empty `nodes` list")

    links = resolved_workflow.get("links") or []
    if not isinstance(links, list):
        raise ValueError("resolved_workflow `links` must be a list when provided")

    return {
        "run_id": resolved_workflow["run_id"],
        "use_local_storage": bool(resolved_workflow.get("use_local_storage", True)),
        "nodes": [_lower_resolved_node(node) for node in nodes],
        "links": [_lower_link(link) for link in links],
    }


def normalize_start_workflow_payload(
    payload: dict[str, Any],
    *,
    config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = deepcopy(payload)

    if is_resolved_workflow_payload(normalized):
        resolved_workflow = normalized.get("resolved_workflow")
        if resolved_workflow is None:
            raise ValueError("resolved workflow payload must contain top-level `resolved_workflow`")
        normalized = {"workflow_def": lower_resolved_workflow_to_workflow_def(resolved_workflow)}
        if "config" in payload:
            normalized["config"] = deepcopy(payload["config"])
    elif "workflow_def" not in normalized:
        normalized = {"workflow_def": normalized}

    if config_override is not None:
        normalized["config"] = deepcopy(config_override)

    return normalized
