from __future__ import annotations

import argparse
import ast
import base64
import copy
import json
import pickle
import re
import xml.etree.ElementTree as ET

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


IR_PROPERTY_KEYS_TO_SKIP = {
    "inputConnectionsStore",
    "savedWidgetGeometry",
    "triggerReady",
}

SALMON_WIDGET_NAMES = {
    "downloadURL",
    "starIndex",
    "starAlign",
    "gffread",
    "Start",
    "10x_format_fa_gtf",
    "salmon",
    "Trimgalore",
    "S3_download",
}

DEFAULT_SALMON_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "bwb"
    / "scheduling_service"
    / "test_workflows"
    / "bulk_rna_async.json"
)


@dataclass(frozen=True)
class IRLink:
    source: int
    sink: int
    source_channel: str
    sink_channel: str
    enabled: bool = True


@dataclass(frozen=True)
class IRNode:
    id: int
    name: str
    title: str
    description: str
    project_name: str | None
    qualified_name: str | None
    position: str | None
    properties: dict[str, Any]


@dataclass(frozen=True)
class WorkflowIR:
    title: str
    description: str
    source_path: Path | None
    nodes: tuple[IRNode, ...]
    links: tuple[IRLink, ...]

    @property
    def nodes_by_name(self) -> dict[str, IRNode]:
        return {node.name: node for node in self.nodes}


class UnsupportedWorkflowError(ValueError):
    pass


def _link_key(link: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        link["source"],
        link["sink"],
        link["source_channel"],
        link["sink_channel"],
    )


def attach_condition_metadata(
    decoded_workflow: dict[str, Any],
    *,
    conditions: list[dict[str, Any]] | None = None,
    node_conditions: dict[int, dict[str, Any]] | None = None,
    link_conditions: dict[tuple[int, int, str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Attach optional conditional metadata to a scheduler workflow.

    This mirrors the minimal conditional vocabulary already used in the
    Workflow IR produced by bwb-nextflow-utils:
    - top-level ``conditions``
    - ``condition_ref`` / ``condition`` on nodes
    - ``condition_ref`` / ``condition`` on links

    The scheduler currently treats these fields as metadata only.
    """
    conditioned = copy.deepcopy(decoded_workflow)
    conditioned["conditions"] = copy.deepcopy(conditions or conditioned.get("conditions", []))

    normalized_node_conditions = node_conditions or {}
    for node in conditioned["nodes"]:
        metadata = normalized_node_conditions.get(node["id"])
        if metadata is None:
            continue
        if "condition_ref" in metadata:
            node["condition_ref"] = metadata["condition_ref"]
        if "condition" in metadata:
            node["condition"] = copy.deepcopy(metadata["condition"])

    if link_conditions:
        link_conditions_by_key = {
            _link_key(link): metadata for link, metadata in (
                (
                    {
                        "source": source,
                        "sink": sink,
                        "source_channel": source_channel,
                        "sink_channel": sink_channel,
                    },
                    metadata,
                )
                for (source, sink, source_channel, sink_channel), metadata in link_conditions.items()
            )
        }
    else:
        link_conditions_by_key = {}

    for link in conditioned["links"]:
        metadata = link_conditions_by_key.get(_link_key(link))
        if metadata is None:
            continue
        if "condition_ref" in metadata:
            link["condition_ref"] = metadata["condition_ref"]
        if "condition" in metadata:
            link["condition"] = copy.deepcopy(metadata["condition"])

    return conditioned


def _normalize_ir_value(value: Any) -> Any:
    if isinstance(value, OrderedDict):
        return {key: _normalize_ir_value(inner) for key, inner in value.items()}
    if isinstance(value, dict):
        return {key: _normalize_ir_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_ir_value(inner) for inner in value]
    if isinstance(value, bytes):
        raise TypeError("bytes values are not JSON serializable")
    return value


def _load_property_payload(raw_payload: str, payload_format: str) -> dict[str, Any]:
    if payload_format == "literal":
        payload = ast.literal_eval(raw_payload)
    elif payload_format == "pickle":
        payload = pickle.loads(base64.b64decode(raw_payload))
    else:
        raise ValueError(f"Unsupported node property format: {payload_format}")

    normalized_payload = {}
    for key, value in payload.items():
        if key in IR_PROPERTY_KEYS_TO_SKIP:
            continue
        normalized_payload[key] = _normalize_ir_value(value)
    return normalized_payload


def load_ows_ir(path: str | Path) -> WorkflowIR:
    ows_path = Path(path)
    root = ET.fromstring(ows_path.read_text())

    properties_by_node_id: dict[int, dict[str, Any]] = {}
    node_properties = root.find("node_properties")
    if node_properties is not None:
        for properties_element in node_properties:
            node_id = int(properties_element.get("node_id"))
            raw_payload = (properties_element.text or "").strip()
            payload_format = properties_element.get("format", "literal")
            properties_by_node_id[node_id] = _load_property_payload(raw_payload, payload_format)

    nodes = []
    for node_element in root.find("nodes") or []:
        node_id = int(node_element.get("id"))
        nodes.append(
            IRNode(
                id=node_id,
                name=node_element.get("name"),
                title=node_element.get("title"),
                description=node_element.get("title"),
                project_name=node_element.get("project_name"),
                qualified_name=node_element.get("qualified_name"),
                position=node_element.get("position"),
                properties=properties_by_node_id.get(node_id, {}),
            )
        )

    links = []
    for link_element in root.find("links") or []:
        links.append(
            IRLink(
                source=int(link_element.get("source_node_id")),
                sink=int(link_element.get("sink_node_id")),
                source_channel=link_element.get("source_channel"),
                sink_channel=link_element.get("sink_channel"),
                enabled=link_element.get("enabled", "true").lower() == "true",
            )
        )

    return WorkflowIR(
        title=root.get("title", ows_path.stem),
        description=root.get("description", ""),
        source_path=ows_path,
        nodes=tuple(sorted(nodes, key=lambda node: node.id)),
        links=tuple(links),
    )


def _sanitize_run_id(name: str) -> str:
    sanitized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return sanitized or "workflow"


def _load_salmon_template(path: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    template = json.loads(path.read_text())
    template_nodes = {
        node["description"]: node
        for node in template["nodes"]
        if node["description"] in SALMON_WIDGET_NAMES
    }
    if template_nodes.keys() != SALMON_WIDGET_NAMES:
        missing = sorted(SALMON_WIDGET_NAMES - template_nodes.keys())
        extra = sorted(template_nodes.keys() - SALMON_WIDGET_NAMES)
        raise UnsupportedWorkflowError(
            f"Salmon template mismatch. Missing={missing}, extra={extra}"
        )

    allowed_ids = {
        node["id"]
        for node in template["nodes"]
        if node["description"] in SALMON_WIDGET_NAMES
    }
    template_links = [
        link
        for link in template["links"]
        if link["source"] in allowed_ids and link["sink"] in allowed_ids
    ]
    return template_nodes, template_links


def _merge_scheduler_node(template_node: dict[str, Any], ir_node: IRNode) -> dict[str, Any]:
    merged_node = copy.deepcopy(template_node)
    merged_parameters = copy.deepcopy(merged_node["parameters"])
    merged_parameters.update(ir_node.properties)

    merged_node["id"] = ir_node.id
    merged_node["title"] = ir_node.title
    merged_node["description"] = ir_node.name
    merged_node["parameters"] = merged_parameters

    if "iterate" in merged_parameters:
        merged_node["iterate"] = merged_parameters["iterate"]
    if "iterateSettings" in merged_parameters:
        merged_node["iterate_settings"] = copy.deepcopy(merged_parameters["iterateSettings"])
    if "optionsChecked" in merged_parameters:
        merged_node["options_checked"] = copy.deepcopy(merged_parameters["optionsChecked"])
    if "numSlots" in merged_parameters:
        merged_node["slots"] = merged_parameters["numSlots"]
    if "end_async" in merged_parameters:
        merged_node["end_async"] = merged_parameters["end_async"]

    return merged_node


def decode_salmon_ir_to_scheduler_json(
    workflow_ir: WorkflowIR,
    run_id: str | None = None,
    use_local_storage: bool = True,
    template_path: str | Path = DEFAULT_SALMON_TEMPLATE_PATH,
) -> dict[str, Any]:
    ir_node_names = {node.name for node in workflow_ir.nodes}
    if ir_node_names != SALMON_WIDGET_NAMES:
        missing = sorted(SALMON_WIDGET_NAMES - ir_node_names)
        extra = sorted(ir_node_names - SALMON_WIDGET_NAMES)
        raise UnsupportedWorkflowError(
            f"Workflow is not the supported salmon graph. Missing={missing}, extra={extra}"
        )

    template_nodes, template_links = _load_salmon_template(Path(template_path))
    template_id_to_name = {node["id"]: node["description"] for node in template_nodes.values()}

    decoded_nodes = []
    for ir_node in workflow_ir.nodes:
        decoded_nodes.append(_merge_scheduler_node(template_nodes[ir_node.name], ir_node))

    name_to_runtime_id = {node.name: node.id for node in workflow_ir.nodes}
    decoded_links = []
    for template_link in template_links:
        source_name = template_id_to_name[template_link["source"]]
        sink_name = template_id_to_name[template_link["sink"]]
        decoded_links.append(
            {
                "source": name_to_runtime_id[source_name],
                "sink": name_to_runtime_id[sink_name],
                "source_channel": template_link["source_channel"],
                "sink_channel": template_link["sink_channel"],
            }
        )

    resolved_run_id = run_id
    if resolved_run_id is None:
        source_name = workflow_ir.source_path.stem if workflow_ir.source_path else workflow_ir.title
        resolved_run_id = _sanitize_run_id(source_name)

    return {
        "run_id": resolved_run_id,
        "use_local_storage": use_local_storage,
        "nodes": decoded_nodes,
        "links": decoded_links,
        "conditions": [],
    }


def decode_ows_to_scheduler_json(
    path: str | Path,
    workflow: str = "salmon",
    run_id: str | None = None,
    use_local_storage: bool = True,
) -> dict[str, Any]:
    workflow_ir = load_ows_ir(path)
    if workflow != "salmon":
        raise UnsupportedWorkflowError(f"Unsupported workflow decoder: {workflow}")
    return decode_salmon_ir_to_scheduler_json(
        workflow_ir=workflow_ir,
        run_id=run_id,
        use_local_storage=use_local_storage,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decode a workflow IR handoff into scheduler JSON."
    )
    parser.add_argument("input_path", help="Path to the source .ows workflow")
    parser.add_argument(
        "--workflow",
        default="salmon",
        help="Decoder to use. Only 'salmon' is supported currently.",
    )
    parser.add_argument("--run-id", default=None, help="Override the scheduler run_id")
    parser.add_argument(
        "--use-local-storage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Set the scheduler use_local_storage flag.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path. Defaults to stdout.",
    )
    args = parser.parse_args()

    decoded = decode_ows_to_scheduler_json(
        path=args.input_path,
        workflow=args.workflow,
        run_id=args.run_id,
        use_local_storage=args.use_local_storage,
    )
    rendered = json.dumps(decoded, indent=2)
    if args.output:
        Path(args.output).write_text(rendered + "\n")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
