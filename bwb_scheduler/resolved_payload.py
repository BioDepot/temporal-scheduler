from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Any


RESOLVED_WORKFLOW_SCHEMA_ID = "biodepot.resolved_workflow/v1"


class ResolvedWorkflowAsyncWarning(UserWarning):
    """Raised when a v1 payload sets node-level `async` / `barrier_for`.

    These fields are lowered to the v0 `graph_manager` async-barrier
    implementation, which is an independent runtime-async engine — its
    behavior is NOT guaranteed to match another v1 scheduler's async
    (e.g. Patrick's Go scheduler). Per the 2026-06-07 deferred-expansion
    design note, v1's portable path is static pre-expansion; node-level
    async in v1 is a transitional accommodation, not the end state. The
    warning makes that boundary explicit at the lowering point rather
    than letting two divergent async engines silently disagree.
    """


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

    # Under the 2026-06-06 v1 correction, launch.command is an array of
    # shell command strings the executor joins with ' && ' inside a single
    # shell invocation. The existing Python scheduler / executor expects a
    # single shell command line, so emit a ["bash", "-c", "<joined>"] argv
    # at the v0 boundary. Each v1 entry is already a complete shell
    # command, so this is a join — not a render.
    command_v0 = ["bash", "-c", " && ".join(command)]

    # Image: v1 splits image_name / image_tag; v0 (the existing scheduler
    # path) carries the combined "name:tag" string. Re-glue here.
    image_name = node["image_name"]
    image_tag = node.get("image_tag")
    if image_tag:
        image_v0 = f"{image_name}:{image_tag}"
    else:
        image_v0 = image_name

    use_scheduler = bool(scheduler_controls.get("useScheduler", True))
    use_gpu = bool(scheduler_controls.get("useGpu", resources.get("gpus", 0) > 0))
    iterate = bool(scheduler_controls.get("iterate", False))
    n_workers = int(scheduler_controls.get("nWorkers", 1))

    lowered = {
        "id": int(node["id"]),
        "title": node.get("title", f"node-{node['id']}"),
        "description": node.get("description", node.get("title", f"node-{node['id']}")),
        "image_name": image_v0,
        "command": command_v0,
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
    # Under the 2026-06-06 v1 correction, nodes is an object keyed by the
    # decimal-string node id; iterate values in ascending-id order so the
    # lowered v0 list is stable / reproducible.
    nodes = resolved_workflow.get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        raise ValueError("resolved_workflow must contain a non-empty `nodes` object keyed by node id")

    links = resolved_workflow.get("links") or []
    if not isinstance(links, list):
        raise ValueError("resolved_workflow `links` must be a list when provided")

    nodes_in_order = [nodes[k] for k in sorted(nodes, key=lambda s: int(s))]

    # Flag node-level async / barrier_for. The bridge still passes these
    # through to the v0 graph_manager (which implements async-barrier
    # scatter-gather), so async v1 payloads continue to run on the Python
    # path — but the behavior is the v0 engine's, not a portable static
    # contract, and is not guaranteed to match another v1 scheduler. We
    # warn rather than reject so existing v0 async capability is not
    # broken; see the 2026-06-07 deferred-expansion design note.
    async_node_ids = sorted(
        int(n["id"])
        for n in nodes_in_order
        if n.get("async") or n.get("barrier_for") is not None
    )
    if async_node_ids:
        warnings.warn(
            "resolved_workflow/v1 payload sets node-level async/barrier_for on "
            f"node id(s) {async_node_ids}. These are lowered to the v0 "
            "graph_manager async-barrier engine, whose runtime-async semantics "
            "are NOT guaranteed to match another v1 scheduler (e.g. the Go "
            "scheduler). Per the 2026-06-07 deferred-expansion design note, the "
            "portable v1 path is static pre-expansion; node-level async in v1 is "
            "transitional. Pre-expand the fan-out into concrete nodes for "
            "scheduler-independent behavior.",
            ResolvedWorkflowAsyncWarning,
            stacklevel=2,
        )

    return {
        "run_id": resolved_workflow["run_id"],
        "use_local_storage": bool(resolved_workflow.get("use_local_storage", True)),
        "nodes": [_lower_resolved_node(node) for node in nodes_in_order],
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
