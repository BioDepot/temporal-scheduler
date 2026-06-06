import json
from pathlib import Path

import pytest

from bwb_scheduler.resolved_payload import (
    RESOLVED_WORKFLOW_SCHEMA_ID,
    is_resolved_workflow_payload,
    lower_resolved_workflow_to_workflow_def,
    normalize_start_workflow_payload,
)


SALMON_RESOLVED_FIXTURE = Path(
    "/mnt/pikachu/bwb-nextflow-utils/tests/fixtures/simple_salmon/submission_payload.resolved_workflow_v1.json"
)


def _resolved_payload():
    # 2026-06-06 v1 correction shape: nodes is a dict keyed by string id;
    # image_name and image_tag are separate; launch.command is an array of
    # shell command strings.
    return {
        "schema": RESOLVED_WORKFLOW_SCHEMA_ID,
        "resolved_workflow": {
            "run_id": "resolved-0000",
            "use_local_storage": True,
            "nodes": {
                "0": {
                    "id": 0,
                    "title": "ResolvedEmit",
                    "description": "resolved smoke",
                    "image_name": "python",
                    "image_tag": "3.11-slim",
                    "launch": {
                        "command": ["python -c \"print('hello')\""],
                        "env": {"MESSAGE": "resolved-hello"},
                    },
                    "resources": {"cores": 2, "mem_mb": 1024, "gpus": 0},
                    "scheduler_controls": {
                        "useScheduler": True,
                        "useGpu": False,
                        "nWorkers": 1,
                        "slots": 1,
                    },
                }
            },
            "links": [],
        },
    }


def test_detects_resolved_payload():
    assert is_resolved_workflow_payload(_resolved_payload())


def test_lowers_resolved_node_to_stub_free_workflow_def():
    workflow_def = lower_resolved_workflow_to_workflow_def(_resolved_payload()["resolved_workflow"])

    assert workflow_def["run_id"] == "resolved-0000"
    assert workflow_def["use_local_storage"] is True
    assert len(workflow_def["nodes"]) == 1

    node = workflow_def["nodes"][0]
    # Per the 2026-06-06 v1 correction, launch.command is an array of shell
    # command strings; the bridge joins with ' && ' inside a bash -c wrap
    # for the existing v0 executor's single-argv expectation.
    assert node["command"] == ["bash", "-c", "python -c \"print('hello')\""]
    # image_name and image_tag are glued back to "name:tag" at the v0
    # boundary because the existing v0 executor carries the combined form.
    assert node["image_name"] == "python:3.11-slim"
    assert node["arg_types"] == {}
    assert node["required_parameters"] == []
    assert node["options_checked"] == {}
    assert node["static_env"] == {"MESSAGE": "resolved-hello"}
    assert node["cores"] == 2
    assert node["mem_mb"] == 1024
    assert node["gpus"] == 0
    assert node["parameters"]["useScheduler"] is True
    assert node["parameters"]["useGpu"] is False


def test_normalize_wraps_resolved_workflow_as_start_payload():
    payload = normalize_start_workflow_payload(_resolved_payload())

    assert set(payload.keys()) == {"workflow_def"}
    assert payload["workflow_def"]["run_id"] == "resolved-0000"
    assert payload["workflow_def"]["nodes"][0]["static_env"]["MESSAGE"] == "resolved-hello"


def test_normalize_preserves_existing_config_and_supports_override():
    payload = _resolved_payload()
    payload["config"] = {"executors": {"slurm": {"storage_dir": "/tmp/a"}}}

    normalized = normalize_start_workflow_payload(
        payload,
        config_override={"executors": {"slurm": {"storage_dir": "/tmp/b"}}},
    )

    assert normalized["config"]["executors"]["slurm"]["storage_dir"] == "/tmp/b"


# ── Link-name lowering tests ──────────────────────────────────────────
#
# v1 schema names link endpoints `source_output` / `sink_input` (typed
# output→input binding). The downstream Python scheduler graph code reads
# `source_channel` / `sink_channel`. The lowering shim in
# `_lower_link` must map between them so multi-node v1 payloads
# actually execute through the existing executor path.
#
# Without these tests, a regression in the shim would silently break
# every multi-node v1 fixture's link semantics — which is exactly what
# happened with the salmon resolved fixture before the shim landed.


def test_link_names_lowered_to_v0_channel_names():
    """Synthetic test: a v1 link with source_output / sink_input MUST
    lower to a v0 link with source_channel / sink_channel and no
    leftover v1 keys."""
    payload = _resolved_payload()
    payload["resolved_workflow"]["nodes"]["1"] = {
        "id": 1,
        "title": "Sink",
        "image_name": "python",
        "image_tag": "3.11-slim",
        "launch": {"command": ["true"]},
        "resources": {"cores": 1, "mem_mb": 512, "gpus": 0},
    }
    payload["resolved_workflow"]["links"].append(
        {
            "source": 0,
            "sink": 1,
            "source_output": "stdout",
            "sink_input": "stdin",
        }
    )

    workflow_def = lower_resolved_workflow_to_workflow_def(payload["resolved_workflow"])

    assert len(workflow_def["links"]) == 1
    link = workflow_def["links"][0]
    assert link["source_channel"] == "stdout", (
        "lowering shim did not map source_output → source_channel"
    )
    assert link["sink_channel"] == "stdin", (
        "lowering shim did not map sink_input → sink_channel"
    )
    assert "source_output" not in link, (
        "v1 link key source_output leaked into v0 output — shim must rename, not duplicate"
    )
    assert "sink_input" not in link, (
        "v1 link key sink_input leaked into v0 output — shim must rename, not duplicate"
    )


def test_salmon_resolved_v1_lowers_with_v0_channel_names():
    """Cross-repo regression test: load the salmon resolved fixture
    directly off disk from bwb-nextflow-utils and confirm every link
    lowers to v0 with source_channel / sink_channel and no v1 keys.
    Skip if the cross-repo path is not present (running in CI outside
    pikachu, sandbox, etc.)."""
    if not SALMON_RESOLVED_FIXTURE.exists():
        pytest.skip(f"Cross-repo fixture not present at {SALMON_RESOLVED_FIXTURE}")

    with open(SALMON_RESOLVED_FIXTURE) as fh:
        envelope = json.load(fh)

    assert envelope["schema"] == RESOLVED_WORKFLOW_SCHEMA_ID, (
        f"fixture declares unexpected schema id {envelope['schema']!r}"
    )

    workflow_def = lower_resolved_workflow_to_workflow_def(envelope["resolved_workflow"])

    assert workflow_def["links"], "salmon resolved fixture should have at least one link"
    for idx, link in enumerate(workflow_def["links"]):
        assert "source_channel" in link, (
            f"salmon link {idx} missing v0 source_channel after lowering: {link}"
        )
        assert "sink_channel" in link, (
            f"salmon link {idx} missing v0 sink_channel after lowering: {link}"
        )
        assert "source_output" not in link, (
            f"salmon link {idx} retained v1 source_output after lowering: {link}"
        )
        assert "sink_input" not in link, (
            f"salmon link {idx} retained v1 sink_input after lowering: {link}"
        )


def test_link_lowering_preserves_unrelated_keys():
    """The lowering shim should not strip legitimate v0 fields a payload
    might already carry (e.g. links explicitly already in v0 form, or
    future link metadata fields)."""
    workflow = {
        "run_id": "test-link-keys",
        "use_local_storage": True,
        "nodes": {
            "0": {
                "id": 0,
                "title": "A",
                "image_name": "alpine",
                "image_tag": "latest",
                "launch": {"command": ["true"]},
                "resources": {"cores": 1, "mem_mb": 512, "gpus": 0},
            },
            "1": {
                "id": 1,
                "title": "B",
                "image_name": "alpine",
                "image_tag": "latest",
                "launch": {"command": ["true"]},
                "resources": {"cores": 1, "mem_mb": 512, "gpus": 0},
            },
        },
        "links": [
            {
                "source": 0,
                "sink": 1,
                "source_output": "out",
                "sink_input": "in",
                "extra_metadata": "preserve_me",
            }
        ],
    }

    workflow_def = lower_resolved_workflow_to_workflow_def(workflow)
    link = workflow_def["links"][0]
    assert link["extra_metadata"] == "preserve_me", (
        "lowering shim discarded an unrelated key during link rewrite"
    )
