from pathlib import Path

from bwb_scheduler.ir_to_scheduler import (
    attach_condition_metadata,
    decode_ows_to_scheduler_json,
    load_ows_ir,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SALMON_OWS_PATH = REPO_ROOT / "star_salmon_short" / "star_salmon_short.ows"


def _get_node(decoded_workflow: dict, title: str) -> dict:
    return next(node for node in decoded_workflow["nodes"] if node["title"] == title)


def test_load_ows_ir_decodes_literal_and_pickled_node_properties():
    workflow_ir = load_ows_ir(SALMON_OWS_PATH)

    assert workflow_ir.title == "star-salmon-short"
    assert [node.name for node in workflow_ir.nodes] == [
        "downloadURL",
        "starIndex",
        "starAlign",
        "gffread",
        "Start",
        "10x_format_fa_gtf",
        "salmon",
        "Trimgalore",
        "S3_download",
    ]

    download_node = workflow_ir.nodes_by_name["downloadURL"]
    assert download_node.properties["URL"] == [
        "https://ftp.ensembl.org/pub/release-98/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz",
        "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_32/gencode.v32.primary_assembly.annotation.gtf.gz",
    ]

    salmon_node = workflow_ir.nodes_by_name["salmon"]
    assert salmon_node.properties["outputdir"] == "/data/salmon-quant"
    assert salmon_node.properties["alignmentfiles"]["pattern"] == "**/*ome.out.bam"


def test_decode_salmon_ows_to_scheduler_json_uses_scheduler_templates():
    decoded = decode_ows_to_scheduler_json(SALMON_OWS_PATH)

    assert decoded["run_id"] == "star_salmon_short"
    assert decoded["use_local_storage"] is True
    assert decoded["conditions"] == []
    assert len(decoded["nodes"]) == 9
    assert len(decoded["links"]) == 35

    start_node = _get_node(decoded, "Start-index")
    assert start_node["description"] == "Start"
    assert start_node["parameters"]["genomegtfURLs"][0].endswith("release-98/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz")
    assert start_node["parameters"]["trimmeddir"] == "/data/startest/trimmed"

    trimgalore_node = _get_node(decoded, "Trimgalore")
    assert trimgalore_node["async"] is True
    assert trimgalore_node["parameters"]["outputDir"] == "/data/startest/trimmed"
    assert len(trimgalore_node["parameters"]["inputFiles"]) == 12

    salmon_node = _get_node(decoded, "salmon")
    assert salmon_node["parameters"]["outputdir"] == "/data/salmon-quant"
    assert salmon_node["parameters"]["transcriptome"] == "/data/startest/genome/transcriptome.fa"

    assert {
        "source": 7,
        "sink": 2,
        "source_channel": "trimmed_files",
        "sink_channel": "readFilesIn",
    } in decoded["links"]
    assert {
        "source": 2,
        "sink": 6,
        "source_channel": "transcriptome_bam_files",
        "sink_channel": "alignmentfiles",
    } in decoded["links"]


def test_attach_condition_metadata_preserves_scheduler_shape():
    decoded = decode_ows_to_scheduler_json(SALMON_OWS_PATH)

    conditioned = attach_condition_metadata(
        decoded,
        conditions=[
            {
                "id": "cond_make_index",
                "type": "nextflow_if",
                "expr": "make_index",
                "source_language": "nextflow",
                "status": "source_preserved",
            }
        ],
        node_conditions={
            1: {
                "condition_ref": "cond_make_index",
            }
        },
        link_conditions={
            (1, 2, "genomeDir", "genomeDir"): {
                "condition_ref": "cond_make_index",
                "condition": {
                    "expr": "make_index",
                    "status": "source_preserved",
                },
            }
        },
    )

    assert conditioned["conditions"] == [
        {
            "id": "cond_make_index",
            "type": "nextflow_if",
            "expr": "make_index",
            "source_language": "nextflow",
            "status": "source_preserved",
        }
    ]

    conditioned_index = _get_node(conditioned, "Make STAR indices")
    assert conditioned_index["condition_ref"] == "cond_make_index"

    conditioned_link = next(
        link
        for link in conditioned["links"]
        if link["source"] == 1
        and link["sink"] == 2
        and link["source_channel"] == "genomeDir"
        and link["sink_channel"] == "genomeDir"
    )
    assert conditioned_link["condition_ref"] == "cond_make_index"
    assert conditioned_link["condition"] == {
        "expr": "make_index",
        "status": "source_preserved",
    }
