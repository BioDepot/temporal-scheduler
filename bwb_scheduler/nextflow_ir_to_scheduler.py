from __future__ import annotations

import argparse
import copy
import json

from pathlib import Path
from typing import Any

from bwb_scheduler.ir_to_scheduler import UnsupportedWorkflowError


TEST_SCHEME_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "bwb"
    / "scheduling_service"
    / "test_workflows"
    / "test_scheme.json"
)


def _load_template_nodes() -> tuple[dict[str, Any], dict[str, Any]]:
    template = json.loads(TEST_SCHEME_TEMPLATE_PATH.read_text())
    index_node = next(node for node in template["nodes"] if node["title"] == "SalmonIndex")
    quant_node = next(node for node in template["nodes"] if node["title"] == "SalmonQuant")
    return copy.deepcopy(index_node), copy.deepcopy(quant_node)


def _load_ir_document(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _validate_arabidopsis_ir(ir_document: dict[str, Any]) -> list[dict[str, Any]]:
    workflow = ir_document.get("workflow", {})
    node_ids = {node["id"] for node in workflow.get("nodes", [])}
    required_ids = {"salmon_index", "salmon_quant"}
    if not required_ids.issubset(node_ids):
        raise UnsupportedWorkflowError(
            f"IR is missing required Arabidopsis Salmon nodes: {sorted(required_ids - node_ids)}"
        )

    conditions = workflow.get("conditions", [])
    if not any(condition.get("id") == "cond_make_index" for condition in conditions):
        raise UnsupportedWorkflowError("IR is missing required condition `cond_make_index`")
    return conditions


def _sanitize_run_id(name: str) -> str:
    return name.replace("-", "_")


def _make_root_node(
    *,
    trigger_value: str,
) -> dict[str, Any]:
    return {
        "id": 0,
        "mem_mb": 256,
        "cores": 1,
        "gpus": 0,
        "slots": 1,
        "async": False,
        "barrier_for": None,
        "title": "Arabidopsis Inputs",
        "description": "InputRouter",
        "parameters": {
            "trigger": trigger_value,
            "useGpu": False,
            "useScheduler": True,
        },
        "arg_types": {
            "trigger": {
                "flag": None,
                "label": "Trigger value",
                "type": "str",
            }
        },
        "image_name": "alpine:3.18",
        "command": ["true"],
        "port_mappings": [],
        "port_vars": None,
        "iterate": False,
        "iterate_settings": {
            "iterableAttrs": [],
            "nWorkers": 1,
        },
        "required_parameters": [],
        "options_checked": {},
        "static_env": {},
    }


def decode_nextflow_ir_to_scheduler_json(
    ir_document: dict[str, Any],
    *,
    run_id: str = "rnaseq_arabidopsis_conditional",
    use_local_storage: bool = False,
    make_index: bool = True,
    container_data_dir: str = "/data",
    sample_name: str = "DRR016125",
    transcript_fasta_name: str = "Arabidopsis_thaliana.TAIR10.28.cdna.all.fa.gz",
    generated_index_dir_name: str = "generated_index",
    prebuilt_index_dir_name: str = "athal_index",
    output_dir_name: str = "results/DRR016125_quant",
) -> dict[str, Any]:
    conditions = _validate_arabidopsis_ir(ir_document)
    index_node, quant_node = _load_template_nodes()

    root_node = _make_root_node(trigger_value=container_data_dir)

    index_node["id"] = 1
    index_node["title"] = "SALMON_INDEX"
    index_node["description"] = "salmon_index"
    index_node["parameters"]["index"] = f"{container_data_dir}/{generated_index_dir_name}"
    index_node["parameters"]["transcriptFasta"] = f"{container_data_dir}/{transcript_fasta_name}"
    index_node["condition_ref"] = "cond_make_index"
    index_node["input_files"] = ["transcriptFasta"]
    index_node["output_files"] = ["index"]

    quant_node["id"] = 2
    quant_node["title"] = "SALMON_QUANT"
    quant_node["description"] = "salmon_quant"
    quant_node["parameters"]["index"] = f"{container_data_dir}/{prebuilt_index_dir_name}"
    quant_node["parameters"]["mates1"] = [f"{container_data_dir}/{sample_name}_1.fastq"]
    quant_node["parameters"]["mates2"] = [f"{container_data_dir}/{sample_name}_2.fastq"]
    quant_node["parameters"]["outputDirs"] = [f"{container_data_dir}/{output_dir_name}"]
    quant_node["input_files"] = ["index", "mates1", "mates2"]
    quant_node["output_files"] = ["outputDirs"]

    return {
        "run_id": _sanitize_run_id(run_id),
        "use_local_storage": use_local_storage,
        "conditions": copy.deepcopy(conditions),
        "condition_context": {
            "make_index": make_index,
        },
        "nodes": [
            root_node,
            index_node,
            quant_node,
        ],
        "links": [
            {
                "source": 0,
                "sink": 1,
                "source_channel": "trigger",
                "sink_channel": "trigger",
            },
            {
                "source": 0,
                "sink": 2,
                "source_channel": "trigger",
                "sink_channel": "trigger",
            },
            {
                "source": 1,
                "sink": 2,
                "source_channel": "index",
                "sink_channel": "index",
                "condition_ref": "cond_make_index",
            },
        ],
    }


def write_manifest(
    workflow_path: Path,
    manifest_path: Path,
    config_path: Path,
    benchmark_name: str,
) -> None:
    manifest = {
        "api_base_url": "http://localhost:8000",
        "benchmarks": [
            {
                "name": benchmark_name,
                "workflow_path": str(workflow_path.resolve()),
                "config_path": str(config_path.resolve()),
                "register_worker": False,
                "iterations": 1,
            }
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decode the Arabidopsis conditional Nextflow IR into executable scheduler JSON."
    )
    parser.add_argument("input_path", help="Path to the Nextflow Workflow IR JSON")
    parser.add_argument("--output", required=True, help="Path to write scheduler JSON")
    parser.add_argument("--run-id", default="rnaseq_arabidopsis_conditional")
    parser.add_argument(
        "--use-local-storage",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Set the scheduler use_local_storage flag.",
    )
    parser.add_argument(
        "--make-index",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to execute the conditional Salmon index branch.",
    )
    parser.add_argument(
        "--container-data-dir",
        default="/data",
        help="Path to the staged Arabidopsis data inside the execution container.",
    )
    parser.add_argument(
        "--manifest-output",
        default=None,
        help="Optional output path for a benchmark manifest.",
    )
    parser.add_argument(
        "--config-path",
        default=(
            Path(__file__).resolve().parents[1]
            / "bwb"
            / "scheduling_service"
            / "test_workflows"
            / "rnaseq_arabidopsis_local_docker_slurm_config.json"
        ),
        help="Config path to embed in the optional benchmark manifest.",
    )
    args = parser.parse_args()

    ir_document = _load_ir_document(args.input_path)
    decoded = decode_nextflow_ir_to_scheduler_json(
        ir_document,
        run_id=args.run_id,
        use_local_storage=args.use_local_storage,
        make_index=args.make_index,
        container_data_dir=args.container_data_dir,
    )
    output_path = Path(args.output)
    output_path.write_text(json.dumps(decoded, indent=2) + "\n")

    if args.manifest_output:
        benchmark_name = (
            "rnaseq-arabidopsis-conditional"
            if args.make_index
            else "rnaseq-arabidopsis-prebuilt-index"
        )
        write_manifest(
            workflow_path=output_path,
            manifest_path=Path(args.manifest_output),
            config_path=Path(args.config_path),
            benchmark_name=benchmark_name,
        )


if __name__ == "__main__":
    main()
