from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.conditions import apply_conditions_to_workflow, evaluate_conditions


def test_evaluate_conditions_supports_simple_boolean_expressions():
    results = evaluate_conditions(
        [
            {"id": "cond_true", "expr": "make_index"},
            {"id": "cond_false", "expr": "!make_index"},
            {"id": "cond_literal_true", "expr": "true"},
            {"id": "cond_literal_false", "expr": "false"},
        ],
        {"make_index": True},
    )

    assert results == {
        "cond_true": True,
        "cond_false": False,
        "cond_literal_true": True,
        "cond_literal_false": False,
    }


def test_evaluate_conditions_fails_for_missing_context_variable():
    try:
        evaluate_conditions([{"id": "cond_missing", "expr": "make_index"}], {})
    except ApplicationError as exc:
        assert "condition_context" in str(exc)
    else:
        raise AssertionError("Expected ApplicationError for missing condition_context variable")


def test_apply_conditions_to_workflow_prunes_nodes_and_links_and_tracks_skips():
    scheme = {
        "run_id": "test",
        "use_local_storage": True,
        "conditions": [{"id": "cond_make_index", "expr": "make_index"}],
        "condition_context": {"make_index": False},
        "nodes": [
            {
                "id": 0,
                "title": "Start",
                "description": "Start",
                "parameters": {"useScheduler": True},
                "arg_types": {},
                "async": False,
                "barrier_for": None,
            },
            {
                "id": 1,
                "title": "SALMON_INDEX",
                "description": "salmon_index",
                "parameters": {"useScheduler": True},
                "arg_types": {},
                "async": False,
                "barrier_for": None,
                "condition_ref": "cond_make_index",
            },
            {
                "id": 2,
                "title": "SALMON_QUANT",
                "description": "salmon_quant",
                "parameters": {"useScheduler": True},
                "arg_types": {},
                "async": False,
                "barrier_for": None,
            },
        ],
        "links": [
            {"source": 0, "sink": 1, "source_channel": "trigger", "sink_channel": "trigger"},
            {"source": 0, "sink": 2, "source_channel": "trigger", "sink_channel": "trigger"},
            {
                "source": 1,
                "sink": 2,
                "source_channel": "index",
                "sink_channel": "index",
                "condition_ref": "cond_make_index",
            },
        ],
    }

    conditioned = apply_conditions_to_workflow(scheme)

    assert conditioned["_condition_results"] == {"cond_make_index": False}
    assert [node["id"] for node in conditioned["nodes"]] == [0, 2]
    assert conditioned["links"] == [
        {"source": 0, "sink": 2, "source_channel": "trigger", "sink_channel": "trigger"}
    ]
    assert conditioned["_skipped_nodes"][1]["status"] == "Skipped by condition cond_make_index=false"
