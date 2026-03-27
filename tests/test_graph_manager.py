from pathlib import Path

from bwb.scheduling_service.graph_manager import GraphManager
from bwb_scheduler.ir_to_scheduler import decode_ows_to_scheduler_json


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
