from pathlib import Path

from bwb.scheduling_service.graph_manager import GraphManager
from bwb_scheduler.ir_to_scheduler import attach_condition_metadata, decode_ows_to_scheduler_json


REPO_ROOT = Path(__file__).resolve().parents[1]
SALMON_OWS_PATH = REPO_ROOT / "star_salmon_short" / "star_salmon_short.ows"


def test_graph_manager_allows_async_node_without_barrier():
    decoded = decode_ows_to_scheduler_json(SALMON_OWS_PATH)

    graph_manager = GraphManager(decoded["nodes"], decoded["links"])

    trimgalore_id = 7
    assert graph_manager.is_async[trimgalore_id] is True
    assert trimgalore_id not in graph_manager.async_barriers
    assert graph_manager.get_async_descendants(trimgalore_id) == set()
    assert graph_manager.get_async_ancestors(2) == set()


def test_graph_manager_ignores_condition_metadata():
    decoded = decode_ows_to_scheduler_json(SALMON_OWS_PATH)
    conditioned = attach_condition_metadata(
        decoded,
        conditions=[{"id": "cond_make_index", "expr": "make_index"}],
        node_conditions={1: {"condition_ref": "cond_make_index"}},
        link_conditions={
            (1, 2, "genomeDir", "genomeDir"): {"condition_ref": "cond_make_index"}
        },
    )

    graph_manager = GraphManager(conditioned["nodes"], conditioned["links"])

    assert graph_manager.predecessors[2] >= {1}
    assert graph_manager.is_async[1] is False
