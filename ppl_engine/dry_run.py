"""Candidate-plan estimation only; never emits FastExpr or simulation keys."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping

from .config import simulation_budget_allocation


def _expand_templates(route: Mapping[str, Any], confidence: str) -> List[str]:
    templates = []
    for transform in route.get("transforms", []):
        name = str(transform["name"])
        windows = transform.get("windows")
        if windows:
            templates.extend([name] * len(windows))
        else:
            templates.append(name)
    limit = None
    if confidence == "LOW":
        limit = route.get("max_templates_low_confidence")
    if "max_templates" in route:
        limit = min(int(route["max_templates"]), int(limit)) if limit is not None else int(route["max_templates"])
    return templates[: int(limit)] if limit is not None else templates


def estimate_candidate_plan(discovery: Any, config: Any, operator_summary: Dict[str, Any]) -> Dict[str, Any]:
    routes = config.rules["candidate_routes"]
    reducers = config.rules["vector_reducers"]
    selected_datasets = [item for item in discovery.datasets if item["selected"]]
    discovery_pool = [item for item in discovery.datasets if item.get("in_discovery_pool", item["selected"])]
    selected_fields = [item for item in discovery.fields if item["selected"]]
    coverage_pass = [item for item in discovery.fields if item["coverage_pass"]]

    field_type_counts = Counter(item["field_type"] for item in selected_fields)
    class_counts = Counter(item["semantic_class"] for item in selected_fields)
    confidence_counts = Counter(item["classification_confidence"] for item in selected_fields)
    estimate_units_by_dataset = defaultdict(list)
    raw_estimate_by_dataset = Counter()
    raw_estimate_by_class = Counter()
    raw_estimate_by_transform = Counter()
    raw_estimate_by_reducer = Counter()

    for field in selected_fields:
        route = routes[field["semantic_class"]]
        templates = _expand_templates(route, field["classification_confidence"])
        field_reducers = reducers.get(field["field_type"], [])
        for reducer in field_reducers:
            for transform in templates:
                unit = {
                    "dataset_id": field["dataset_id"],
                    "field_id": field["field_id"],
                    "semantic_class": field["semantic_class"],
                    "transform": transform,
                    "vector_reducer": reducer,
                }
                estimate_units_by_dataset[field["dataset_id"]].append(unit)
                raw_estimate_by_dataset[field["dataset_id"]] += 1
                raw_estimate_by_class[field["semantic_class"]] += 1
                raw_estimate_by_transform[transform] += 1
                raw_estimate_by_reducer[reducer] += 1

    dataset_cap = int(config.plan["budgets"]["max_candidates_per_dataset"])
    kept_units = []
    dataset_cap_truncated = 0
    for dataset_id in sorted(estimate_units_by_dataset):
        units = estimate_units_by_dataset[dataset_id]
        kept_units.extend(units[:dataset_cap])
        dataset_cap_truncated += max(0, len(units) - dataset_cap)

    kept_by_dataset = Counter(item["dataset_id"] for item in kept_units)
    kept_by_class = Counter(item["semantic_class"] for item in kept_units)
    kept_by_transform = Counter(item["transform"] for item in kept_units)
    kept_by_reducer = Counter(item["vector_reducer"] for item in kept_units)
    estimate_total = len(kept_units)
    allocation = simulation_budget_allocation(config.plan)
    post_budget = allocation["max_new_simulation_posts"]
    initial_budget = allocation["initial_search_budget"]
    repair_budget = allocation["repair_reserve_budget"]
    initial_estimate = sum(raw_estimate_by_dataset.values())
    initial_selected = min(estimate_total, initial_budget)
    initial_truncated = max(0, initial_estimate - initial_selected)
    post_truncated = max(0, estimate_total - post_budget)
    unknown_count = class_counts.get("UNKNOWN", 0)
    warning_count = sum(1 for item in selected_fields if item.get("classification_warning"))
    high_ht_hints = {"INTRADAY", "PRICE_VOLUME", "MICROSTRUCTURE", "FLOW", "RETURN", "VOLATILITY"}
    low_hints = {"ANALYST", "FUNDAMENTAL"}
    selected_low = sum(1 for item in selected_datasets if item.get("dataset_semantic_hint") in low_hints)
    unselected_high = any(
        item.get("dataset_semantic_hint") in high_ht_hints and not item["selected"]
        for item in discovery_pool
    )
    preselection_review = (
        "DATASET_PRESELECTION_NEEDS_REVIEW"
        if selected_low > len(selected_datasets) / 2 and unselected_high
        else None
    )

    return {
        "dry_run_id": "dry_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
        "mode": "CANDIDATE_PLAN_ESTIMATE",
        "source": discovery.snapshot["source"],
        "discovery_snapshot_id": discovery.snapshot["snapshot_id"],
        "theme_settings_match": True,
        "dataset_exclusion_status": discovery.snapshot["exclusion_status"],
        "datasets_available": len(discovery.datasets),
        "datasets_in_discovery_pool": len(discovery_pool),
        "datasets_selected": len(selected_datasets),
        "dataset_discovery_pool": [
            {
                "dataset_id": item["dataset_id"],
                "score": item.get("dataset_preselection_score"),
                "stage_a_score": round(
                    sum(
                        float(value or 0)
                        for key, value in (item.get("preselection_components") or {}).items()
                        if key != "field_evidence"
                    ), 4
                ),
                "components": item.get("preselection_components"),
                "dataset_hint": item.get("dataset_semantic_hint"),
                "dataset_hint_source": item.get("dataset_hint_source"),
                "dataset_hint_confidence": item.get("dataset_hint_confidence"),
                "matched_text": item.get("dataset_hint_matched_text"),
                "field_evidence": item.get("field_evidence"),
                "selected": bool(item["selected"]),
            }
            for item in sorted(
                discovery_pool,
                key=lambda x: (-float(x.get("dataset_preselection_score") or 0), x["dataset_id"]),
            )
        ],
        "dataset_preselection_review": preselection_review,
        "fields_available": len(discovery.fields),
        "fields_coverage_pass": len(coverage_pass),
        "fields_selected": len(selected_fields),
        "field_type_counts": dict(sorted(field_type_counts.items())),
        "semantic_class_counts": dict(sorted(class_counts.items())),
        "classification_confidence_counts": dict(sorted(confidence_counts.items())),
        "classification_warning_count": warning_count,
        "unknown_classification_count": unknown_count,
        "candidate_estimate_raw_by_dataset": dict(sorted(raw_estimate_by_dataset.items())),
        "candidate_estimate_by_dataset": dict(sorted(kept_by_dataset.items())),
        "candidate_estimate_by_semantic_class": dict(sorted(kept_by_class.items())),
        "candidate_estimate_by_transform": dict(sorted(kept_by_transform.items())),
        "candidate_estimate_by_vector_reducer": dict(sorted(kept_by_reducer.items())),
        "candidate_estimate_before_dataset_cap": initial_estimate,
        "candidate_estimate_total": estimate_total,
        "max_new_simulation_posts": post_budget,
        "initial_search_budget": initial_budget,
        "repair_reserve_budget": repair_budget,
        "repair_budget_reserved": repair_budget,
        "unallocated_simulation_budget": allocation["unallocated_budget"],
        "initial_candidate_estimate": initial_estimate,
        "initial_candidates_selected": initial_selected,
        "initial_candidates_truncated": initial_truncated,
        "budget_utilization_pct": round((initial_selected / post_budget * 100), 2) if post_budget else None,
        "estimated_truncated_candidates": initial_truncated,
        "dataset_cap_truncated_candidates": dataset_cap_truncated,
        "new_post_budget_truncated_candidates": post_truncated,
        "initial_budget_truncated_candidates": max(0, estimate_total - initial_selected),
        "priority_truncation_required": bool(initial_truncated),
        "budget_status": "BUDGET_OVERFLOW" if initial_truncated else "WITHIN_BUDGET",
        "operator_registry_summary": operator_summary,
        "writes_formal_candidates": False,
    }
