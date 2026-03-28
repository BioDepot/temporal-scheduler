from bwb_scheduler.nextflow_ir_to_scheduler import decode_nextflow_ir_to_scheduler_json


def _make_ir_document() -> dict:
    return {
        "workflow": {
            "conditions": [
                {
                    "id": "cond_make_index",
                    "type": "nextflow_if",
                    "expr": "make_index",
                    "source_language": "nextflow",
                    "status": "source_preserved",
                }
            ],
            "nodes": [
                {"id": "salmon_index"},
                {"id": "fq_subsample"},
                {"id": "salmon_quant"},
            ],
        }
    }


def test_decode_nextflow_ir_to_scheduler_json_preserves_condition_contract():
    decoded = decode_nextflow_ir_to_scheduler_json(_make_ir_document(), run_id="arabidopsis-demo")

    assert decoded["run_id"] == "arabidopsis_demo"
    assert decoded["condition_context"] == {"make_index": True}
    assert decoded["conditions"][0]["id"] == "cond_make_index"
    assert [node["id"] for node in decoded["nodes"]] == [0, 1, 2]
    assert decoded["nodes"][1]["condition_ref"] == "cond_make_index"
    assert decoded["nodes"][1]["input_files"] == ["transcriptFasta"]
    assert decoded["nodes"][1]["output_files"] == ["index"]
    assert decoded["nodes"][2]["parameters"]["index"] == "/data/athal_index"
    assert decoded["nodes"][2]["input_files"] == ["index", "mates1", "mates2"]
    assert decoded["nodes"][2]["output_files"] == ["outputDirs"]
    assert decoded["links"][-1] == {
        "source": 1,
        "sink": 2,
        "source_channel": "index",
        "sink_channel": "index",
        "condition_ref": "cond_make_index",
    }


def test_decode_nextflow_ir_to_scheduler_json_supports_prebuilt_index_mode():
    decoded = decode_nextflow_ir_to_scheduler_json(_make_ir_document(), make_index=False)

    assert decoded["condition_context"] == {"make_index": False}
    assert decoded["nodes"][1]["condition_ref"] == "cond_make_index"
    assert decoded["nodes"][2]["parameters"]["index"] == "/data/athal_index"
