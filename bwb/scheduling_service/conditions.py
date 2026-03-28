from __future__ import annotations

import copy

from typing import Any

from temporalio.exceptions import ApplicationError


def _inline_condition_key(condition: dict[str, Any]) -> str:
    expr = condition.get("expr")
    if isinstance(expr, str) and expr.strip():
        return f"inline:{expr.strip()}"
    return f"inline:{repr(condition)}"


def _evaluate_expr(expr: str, context: dict[str, Any]) -> bool:
    normalized = expr.strip()
    lowered = normalized.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if normalized.startswith("!"):
        return not _evaluate_expr(normalized[1:].strip(), context)
    if normalized not in context:
        raise ApplicationError(
            f"Condition variable `{normalized}` not found in condition_context",
            non_retryable=True,
        )
    return bool(context[normalized])


def evaluate_conditions(
    conditions: list[dict[str, Any]] | None,
    condition_context: dict[str, Any] | None,
) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for condition in conditions or []:
        condition_id = condition.get("id")
        expr = condition.get("expr")
        if not isinstance(condition_id, str) or not condition_id.strip():
            raise ApplicationError("Condition is missing a valid `id`", non_retryable=True)
        if not isinstance(expr, str) or not expr.strip():
            raise ApplicationError(
                f"Condition `{condition_id}` is missing a valid `expr`",
                non_retryable=True,
            )
        results[condition_id] = _evaluate_expr(expr, condition_context or {})
    return results


def _condition_is_active(
    item: dict[str, Any],
    condition_results: dict[str, bool],
) -> tuple[bool, str | None]:
    if "condition_ref" in item:
        condition_ref = item["condition_ref"]
        if condition_ref not in condition_results:
            raise ApplicationError(
                f"Unknown condition_ref `{condition_ref}` in workflow definition",
                non_retryable=True,
            )
        return condition_results[condition_ref], condition_ref

    if "condition" in item:
        condition = item["condition"]
        if not isinstance(condition, dict):
            raise ApplicationError("Inline condition must be an object", non_retryable=True)
        inline_key = _inline_condition_key(condition)
        if inline_key not in condition_results:
            expr = condition.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                raise ApplicationError(
                    "Inline condition must include a non-empty `expr`",
                    non_retryable=True,
                )
            condition_results[inline_key] = _evaluate_expr(expr, {})
        return condition_results[inline_key], inline_key

    return True, None


def apply_conditions_to_workflow(scheme: dict[str, Any]) -> dict[str, Any]:
    conditioned_scheme = copy.deepcopy(scheme)
    conditions = conditioned_scheme.get("conditions", [])
    condition_context = conditioned_scheme.get("condition_context", {})
    condition_results = evaluate_conditions(conditions, condition_context)

    skipped_nodes: dict[int, dict[str, Any]] = {}
    active_nodes = []
    active_node_ids = set()
    for node in conditioned_scheme.get("nodes", []):
        is_active, condition_key = _condition_is_active(node, condition_results)
        if is_active:
            active_nodes.append(node)
            active_node_ids.add(node["id"])
            continue

        skipped_nodes[node["id"]] = {
            "id": node["id"],
            "title": node["title"],
            "description": node.get("description"),
            "condition_ref": node.get("condition_ref"),
            "condition_key": condition_key,
            "status": f"Skipped by condition {condition_key}=false",
        }

    active_links = []
    for link in conditioned_scheme.get("links", []):
        if link["source"] not in active_node_ids or link["sink"] not in active_node_ids:
            continue
        is_active, _ = _condition_is_active(link, condition_results)
        if is_active:
            active_links.append(link)

    conditioned_scheme["nodes"] = active_nodes
    conditioned_scheme["links"] = active_links
    conditioned_scheme["_condition_results"] = condition_results
    conditioned_scheme["_skipped_nodes"] = skipped_nodes
    return conditioned_scheme
