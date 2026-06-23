from pathlib import Path

from bwb.scheduling_service.ancestor_list import AncestorList
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


def _node(node_id: int, *, parameters: dict | None = None, arg_types: dict | None = None) -> dict:
    return {
        "id": node_id,
        "parameters": {"useScheduler": True, **(parameters or {})},
        "arg_types": arg_types or {},
        "barrier_for": None,
        "async": False,
    }


def _ancestor_list(*, source_node: int, source_cmd_set: int, node_count: int = 2) -> AncestorList:
    ancestors = AncestorList(node_count)
    ancestors.set_node_cmd_set(source_node, source_cmd_set)
    return ancestors


def test_control_only_link_orders_without_value_binding():
    graph_manager = GraphManager(
        [
            _node(0),
            _node(1, arg_types={"trigger": {"type": "text"}}),
        ],
        [
            {
                "source": 0,
                "sink": 1,
                "source_channel": "__bwb_trigger_out",
                "sink_channel": "trigger",
                "control_only": True,
                "payload_semantics": "none",
            }
        ],
    )

    graph_manager.outputs[0][0] = {"__bwb_trigger_out": "must-not-bind"}
    graph_manager.inputs[0][0] = {}
    node_inputs = graph_manager.inputs_from_ancestors(
        1,
        _ancestor_list(source_node=0, source_cmd_set=0),
    )

    assert graph_manager.predecessors[1] == {0}
    assert 0 not in graph_manager.channels[1]
    assert "trigger" not in node_inputs


def test_data_link_still_binds_value():
    graph_manager = GraphManager(
        [
            _node(0),
            _node(1, arg_types={"payload": {"type": "text"}}),
        ],
        [
            {
                "source": 0,
                "sink": 1,
                "source_channel": "out",
                "sink_channel": "payload",
            }
        ],
    )

    graph_manager.outputs[0][0] = {"out": "value"}
    graph_manager.inputs[0][0] = {}
    node_inputs = graph_manager.inputs_from_ancestors(
        1,
        _ancestor_list(source_node=0, source_cmd_set=0),
    )

    assert graph_manager.predecessors[1] == {0}
    assert graph_manager.channels[1][0] == [("out", "payload")]
    assert node_inputs["payload"] == "value"
