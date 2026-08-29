"""Phase 3 offline FastExpr planning and diversity-aware initial selection."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    simulation_budget_allocation,
    validate_execution_hash_compatibility,
)
from .settings_contract import validate_full_simulation_settings



CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
FIELD_TOKEN_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
TRANSFORM_FAMILIES = {
    "raw": "RAW", "rank": "RANK", "zscore": "ZSCORE", "ts_mean": "TS_MEAN",
    "ts_delta": "TS_DELTA", "ts_rank": "TS_RANK", "ts_zscore": "TS_ZSCORE",
    "ts_std_dev": "TS_STD_DEV", "ts_quantile": "TS_QUANTILE",
    "ts_arg_min": "TS_ARG_EXTREME", "ts_arg_max": "TS_ARG_EXTREME",
}
EXTENSION_ALLOWED_OPERATORS = frozenset({
    "ts_std_dev", "ts_arg_min", "ts_arg_max", "ts_quantile",
})
EXTENSION_ALLOWED_WINDOWS = frozenset({22, 66})
EXTENSION_FIELD_TYPE = "MATRIX"
CACHE_ACTIONS = {
    "COMPLETE": ("CACHE_COMPLETE", "CACHE_RESTORE"),
    "RUNNING": ("RESUME_RUNNING", "RESUME_EXISTING"),
    "SUBMITTED": ("RESUME_SUBMITTED", "RESUME_EXISTING"),
    "ERROR": ("CACHE_ERROR", "RETRY_PER_V21_POLICY"),
    "AUTH_ERROR": ("CACHE_AUTH_ERROR", "RETRY_PER_V21_POLICY"),
    "INVALID": ("CACHE_INVALID", "STOP_INVALID"),
    "UNCERTAIN_SUBMISSION": ("CACHE_UNCERTAIN", "HOLD_UNCERTAIN"),
    "STALE_RUNNING": ("CACHE_STALE_RUNNING", "RESUME_EXISTING"),
    "REMOTE_NOT_FOUND": ("CACHE_REMOTE_NOT_FOUND", "HOLD_REMOTE_NOT_FOUND"),
}
SEMANTIC_PRIORITY = {"HIGH": 30.0, "MEDIUM": 20.0, "LOW": 10.0}
CONFIDENCE_PRIORITY = {"HIGH": 6.0, "MEDIUM": 4.0, "LOW": 2.0}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha(value: Any) -> str:
    material = value if isinstance(value, str) else _canonical_json(value)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def canonicalize_expression(expression: str) -> str:
    """Only normalize whitespace; never rewrite FastExpr algebra or arguments."""
    return re.sub(r"\s+", " ", str(expression).strip())


def build_expression(field_id: str, field_type: str, reducer: str, transform: str, window: Optional[int]) -> str:
    field_type = str(field_type).upper()
    reducer = str(reducer).upper()
    if field_type == "VECTOR":
        if reducer not in {"VEC_SUM", "VEC_AVG"}:
            raise ValueError(f"Unsupported VECTOR reducer: {reducer}")
        base = f"{reducer.lower()}({field_id})"
    elif field_type == "MATRIX":
        if reducer != "IDENTITY":
            raise ValueError(f"MATRIX reducer must be IDENTITY, got {reducer}")
        base = field_id
    else:
        raise ValueError(f"Unsupported field_type: {field_type}")
    name = str(transform).lower()
    if name == "raw":
        return base
    if name in {"rank", "zscore"}:
        return f"{name}({base})"
    if name in {
        "ts_mean", "ts_delta", "ts_rank", "ts_zscore", "ts_std_dev",
        "ts_arg_min", "ts_arg_max", "ts_quantile",
    }:
        if window is None:
            raise ValueError(f"{name} requires a window")
        return f"{name}({base}, {int(window)})"
    raise ValueError(f"Unsupported Phase 3 transform: {name}")


def expand_route(route: Mapping[str, Any], confidence: str) -> List[Tuple[str, Optional[int]]]:
    values: List[Tuple[str, Optional[int]]] = []
    for transform in route.get("transforms", []):
        name = str(transform["name"]).lower()
        windows = transform.get("windows")
        values.extend((name, int(w)) for w in windows) if windows else values.append((name, None))
    limit = route.get("max_templates_low_confidence") if confidence == "LOW" else None
    if route.get("max_templates") is not None:
        limit = min(int(limit), int(route["max_templates"])) if limit is not None else int(route["max_templates"])
    return values[: int(limit)] if limit is not None else values


def pp_operator_count(expression: str, excluded: Iterable[str]) -> Tuple[int, List[str]]:
    excluded_set = {str(x).lower() for x in excluded}
    calls = [name for name in CALL_RE.findall(expression) if name.lower() not in excluded_set]
    return len(calls), calls


def estimate_data_fields(expression: str, known_fields: Sequence[str], support_fields: Iterable[str] = ()) -> List[str]:
    support = set(support_fields)
    tokens = set(FIELD_TOKEN_RE.findall(expression))
    return sorted({field for field in known_fields if field in tokens and field not in support})


def evaluate_structure(pp_operator_count: int, data_field_count: int, *, operator_max: int = 8, field_max: int = 3) -> str:
    return "LOCAL_STRUCTURE_REJECTED" if pp_operator_count > operator_max or data_field_count > field_max else "ELIGIBLE"




def _is_known_v31_continuous_settings_payload_rejection(record: Mapping[str, Any]) -> bool:
    """Return True only for the D2E Continuous client-payload regression.

    HOTFIX3 intentionally reopens only a definitive HTTP 400 validation
    rejection that proves BRAIN did not create a Simulation.  Generic INVALID
    facts remain terminal and are never automatically re-POSTed.
    """
    if str(record.get("status") or "").upper() != "INVALID":
        return False
    try:
        http_status = int(record.get("last_http_status"))
    except (TypeError, ValueError):
        return False
    if http_status != 400 or record.get("simulation_url") or record.get("submitted_at"):
        return False
    error = str(record.get("error") or "")
    required_markers = (
        'method=POST',
        '/simulations',
        'status=400',
        'instrumentType',
        'This field is required.',
        'pasteurization',
        'unitHandling',
        'nanHandling',
        'language',
        'visualization',
    )
    return all(marker in error for marker in required_markers)

def classify_cache_read_only(alpha_db: Path, sim_key: str) -> Dict[str, Any]:
    """Read V2.1 cache facts without calling init_cache or mutating its schema."""
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute("SELECT * FROM alpha_results WHERE sim_key=?", (sim_key,)).fetchone()
    finally:
        connection.close()
    if row is None:
        return {"cache_classification": "CACHE_MISS", "execution_action": "NEW_SIMULATION_REQUIRED", "record": None}
    record = dict(row)
    status = str(record.get("status") or "ERROR").upper()
    classification, action = CACHE_ACTIONS.get(status, ("CACHE_ERROR", "RETRY_PER_V21_POLICY"))
    if _is_known_v31_continuous_settings_payload_rejection(record):
        # HTTP 400 validation is a definitive non-submission.  Re-open only
        # this exact infrastructure-caused INVALID after the payload builder is
        # fixed; ordinary INVALID responses remain STOP_INVALID.
        classification, action = "CACHE_INVALID_CLIENT_PAYLOAD_REJECTED", "RETRY_PER_V21_POLICY"
    if status in {"RUNNING", "SUBMITTED", "STALE_RUNNING"} and not record.get("simulation_url"):
        # A nonterminal fact without its remote identity is ambiguous.  It
        # must never fall into the generic retry/POST path.
        classification, action = "CACHE_UNCERTAIN", "HOLD_UNCERTAIN"
    return {"cache_classification": classification, "execution_action": action, "record": record}


def _validate_snapshots(discovery: Any, dry_run: Mapping[str, Any], config: Any) -> None:
    if not discovery or not dry_run:
        raise ConfigError("DISCOVERY_SNAPSHOT_REQUIRED")
    snapshot = discovery.snapshot
    settings = config.plan["simulation_settings"]
    mismatches = [
        key for key in ("region", "universe", "delay", "instrument_type")
        if str(snapshot.get(key)).upper() != str(settings.get(key)).upper()
    ]
    if dry_run.get("discovery_snapshot_id") != snapshot.get("snapshot_id"):
        mismatches.append("discovery_snapshot_id")
    compat = validate_execution_hash_compatibility(config, dry_run.get("execution_hash"))
    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:
        mismatches.append(f"execution_hash({compat['status']})")
    if mismatches:
        raise ConfigError("DISCOVERY_SNAPSHOT_MISMATCH: " + ", ".join(mismatches))


def _score_candidate(candidate: Mapping[str, Any], dataset_scores: Mapping[str, float], route_priority: str) -> float:
    coverage = float(candidate.get("coverage") or 0.0) * 10.0
    # Counts only provide a stable, very small tie-break and are not interpreted as alpha quality.
    crowding = -min(float(candidate.get("alpha_count") or 0), 10000.0) / 100000.0
    return round(
        float(dataset_scores.get(candidate["dataset_id"], 0.0))
        + SEMANTIC_PRIORITY.get(route_priority, 0.0)
        + CONFIDENCE_PRIORITY.get(candidate["classification_confidence"], 0.0)
        + coverage + crowding,
        6,
    )


def diversity_select(candidates: Sequence[Dict[str, Any]], budget: int, rules: Mapping[str, Any]) -> List[Dict[str, Any]]:
    if budget <= 0:
        return []
    dataset_cap = max(1, math.floor(budget * float(rules["max_dataset_fraction"])))
    semantic_cap = max(1, math.floor(budget * float(rules["max_semantic_class_fraction"])))
    unknown_cap = max(0, math.floor(budget * float(rules["max_unknown_fraction"])))
    field_cap = int(rules["max_initial_candidates_per_field"])
    family_window_cap = int(rules["max_windows_per_transform_family_per_field"])
    minimum = int(rules["min_candidates_per_dataset"])
    selected: List[Dict[str, Any]] = []
    used = set()
    by_dataset = Counter()
    by_semantic = Counter()
    by_field = Counter()
    by_field_family = Counter()

    def allowed(item: Mapping[str, Any]) -> bool:
        field_key = (item["dataset_id"], item["field_id"])
        family_key = (*field_key, item["vector_reducer"], item["transform_family"])
        limit = unknown_cap if item["semantic_class"] == "UNKNOWN" else semantic_cap
        return (
            by_dataset[item["dataset_id"]] < dataset_cap
            and by_semantic[item["semantic_class"]] < limit
            and by_field[field_key] < field_cap
            and by_field_family[family_key] < family_window_cap
        )

    def add(item: Dict[str, Any], reason: str) -> None:
        used.add(item["candidate_id"])
        selected.append(item)
        by_dataset[item["dataset_id"]] += 1
        by_semantic[item["semantic_class"]] += 1
        field_key = (item["dataset_id"], item["field_id"])
        by_field[field_key] += 1
        by_field_family[(*field_key, item["vector_reducer"], item["transform_family"])] += 1
        item["selection_reason"] = reason

    ordered = sorted(candidates, key=lambda x: (-x["initial_selection_score"], x["candidate_id"]))
    datasets = sorted({x["dataset_id"] for x in ordered})
    # First protect a small quota for every viable dataset.
    for dataset_id in datasets:
        for item in (x for x in ordered if x["dataset_id"] == dataset_id):
            if len(selected) >= budget or by_dataset[dataset_id] >= minimum:
                break
            if item["candidate_id"] not in used and allowed(item):
                add(item, "DATASET_MINIMUM_DIVERSITY")
    # Then globally fill without borrowing through any diversity cap.
    for item in ordered:
        if len(selected) >= budget:
            break
        if item["candidate_id"] not in used and allowed(item):
            add(item, "DIVERSITY_AWARE_SCORE")
    for rank, item in enumerate(selected, 1):
        item["selected_for_initial_search"] = True
        item["selection_rank"] = rank
    return selected


def _build_candidate_record(
    field: Mapping[str, Any], reducer: str, transform: str, window: Optional[int], *,
    route_priority: str, extension_metadata: Optional[Mapping[str, Any]],
    score_adjustment: float, known_fields: Sequence[str], dataset_scores: Mapping[str, float],
    config: Any, run_id: str, alpha_db: Path, machine_lib: Any, settings_plan: Mapping[str, Any],
    dry_run_id: str, snapshot_id: str, target_mode: str, pp_policy: Mapping[str, Any],
    pp_max: int, field_max: int,
) -> Dict[str, Any]:
    """Construct one V3 candidate for both route and extension generation."""
    raw = build_expression(field["field_id"], field["field_type"], reducer, transform, window)
    expression = canonicalize_expression(raw)
    candidate = {
        "expr": expression, "field": field["field_id"], "data_fields": [field["field_id"]],
        "dataset_id": field["dataset_id"], "dataset_ids": [field["dataset_id"]],
        "field_type": field["field_type"], "vector_op": None if reducer == "IDENTITY" else reducer.lower(),
        "operator": transform, "window": window, "decay": int(settings_plan["default_decay"]),
        "stage": "PPL_INITIAL",
    }
    annotated = machine_lib.annotate_candidate_strategy(candidate, target_mode)
    machine_lib.validate_candidate_context([annotated], dataset_id=field["dataset_id"], target_mode=target_mode)
    settings = validate_full_simulation_settings(
        machine_lib.build_settings(
            annotated,
            neutralization=settings_plan["neutralization"], region=settings_plan["region"],
            universe=settings_plan["universe"], delay=settings_plan["delay"],
            truncation=settings_plan["truncation"], test_period=settings_plan["test_period"],
        ),
        context="CANDIDATE_FACTORY",
    )
    settings_json = _canonical_json(settings)
    sim_key = machine_lib.simulation_key(expression, settings)
    transform_family = TRANSFORM_FAMILIES[transform]
    family = "/".join((field["dataset_id"], field["field_id"], reducer, "NORMAL", transform_family))
    context = {
        "runner_goal": config.plan["identity"]["runner_goal"], "target_mode": target_mode,
        "dataset_id": field["dataset_id"], "field_id": field["field_id"],
        "field_type": field["field_type"], "semantic_class": field["semantic_class"],
        "vector_reducer": reducer, "direction": "NORMAL", "transform_family": transform_family,
        "window": window, "discovery_snapshot_id": snapshot_id, "candidate_stage": "PPL_INITIAL",
    }
    if extension_metadata:
        context.update(dict(extension_metadata))
    context_fingerprint = _sha(context)
    candidate_id = "cand_" + _sha({"run_id": run_id, "sim_key": sim_key, "context": context_fingerprint})[:24]
    legacy_count, _ = machine_lib.count_unique_operators(expression)
    pp_count, _ = pp_operator_count(expression, pp_policy["exclude_functions"])
    data_fields = estimate_data_fields(expression, known_fields)
    structure = evaluate_structure(pp_count, len(data_fields), operator_max=pp_max, field_max=field_max)
    cache = classify_cache_read_only(alpha_db, sim_key)
    cached_record = cache["record"]
    simulation_status = str((cached_record or {}).get("status") or "NONE").upper()
    provenance = {
        **context, "classification_source": field["classification_source"],
        "classification_confidence": field["classification_confidence"],
        "classification_rule_id": field.get("classification_rule_id"),
    }
    item = {
        "candidate_id": candidate_id,
        "provenance_id": "prov_" + _sha({"run": run_id, "sim": sim_key, "context": context_fingerprint})[:24],
        "run_id": run_id, "expression": expression, "expression_raw": raw,
        "expression_canonical": expression, "expression_hash": _sha(expression), "sim_key": sim_key,
        "settings_json": settings_json, "settings_hash": _sha(settings_json),
        "context_fingerprint": context_fingerprint, "dataset_id": field["dataset_id"],
        "field_id": field["field_id"], "field_type": field["field_type"],
        "semantic_class": field["semantic_class"], "classification_source": field["classification_source"],
        "classification_confidence": field["classification_confidence"], "direction": "NORMAL",
        "transform_family": transform_family, "operator": transform, "window": window,
        "vector_reducer": reducer, "signal_family": family, "decay": annotated["decay"],
        "neutralization": settings_plan["neutralization"], "legacy_unique_operator_count": legacy_count,
        "pp_total_operator_count_estimate": pp_count,
        "pp_operator_estimator_version": int(pp_policy["estimator_version"]),
        "data_field_count_estimate": len(data_fields), "data_fields_used": data_fields,
        "discovery_snapshot_id": snapshot_id, "dry_run_snapshot_id": dry_run_id,
        "structure_status": structure, "cache_classification": cache["cache_classification"],
        "execution_action": cache["execution_action"], "simulation_status": simulation_status,
        "alpha_id": (cached_record or {}).get("alpha_id"),
        "available_result": cached_record if simulation_status == "COMPLETE" else None,
        "selected_for_initial_search": False, "selection_rank": None, "selection_reason": "NOT_SELECTED",
        "coverage": field.get("coverage"), "alpha_count": field.get("alphaCount"),
        "initial_selection_score": _score_candidate(field, dataset_scores, route_priority) + float(score_adjustment),
        "provenance": provenance, "v21_candidate": annotated,
        "_extension_priority": tuple((extension_metadata or {}).get("extension_priority") or ()),
    }
    return item


def _extension_sort_key(item: Mapping[str, Any]) -> Tuple[Any, ...]:
    provenance = dict(item.get("provenance") or {})
    return (
        tuple(-float(x) for x in item.get("_extension_priority") or ()),
        -float(item.get("initial_selection_score") or 0.0),
        str(item.get("dataset_id") or ""), str(item.get("field_id") or ""),
        str(provenance.get("extension_source") or ""),
        str(item.get("operator") or ""), int(item.get("window") or 0),
        str(item.get("expression_canonical") or item.get("expression") or ""),
        str(item.get("settings_hash") or ""), str(item.get("sim_key") or ""),
    )


def _round_robin_extension_items(items: Sequence[Dict[str, Any]], slots: int) -> List[Dict[str, Any]]:
    """Deterministically retain diverse dataset-field extension candidates."""
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        groups[(str(item.get("dataset_id") or ""), str(item.get("field_id") or ""))].append(item)
    for values in groups.values():
        values.sort(key=_extension_sort_key)
    out: List[Dict[str, Any]] = []
    while groups and len(out) < slots:
        ordered = sorted(groups, key=lambda key: (_extension_sort_key(groups[key][0]), key))
        for key in ordered:
            if len(out) >= slots:
                break
            out.append(groups[key].pop(0))
            if not groups[key]:
                del groups[key]
    return out


def _prune_extension_candidates(
    base: Sequence[Dict[str, Any]], core: Sequence[Dict[str, Any]], targeted: Sequence[Dict[str, Any]],
    *, existing_base_count: int = 0, existing_core_count: int = 0,
    existing_targeted_count: int = 0, existing_core_windows: Sequence[int] = (),
    eligible_incoming_base_count: Optional[int] = None,
    duplicate_skipped: int = 0, persisted_duplicate_skipped: int = 0,
    cache_only_base_excluded: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Apply the all-round 20% extension cap before incoming rows are persisted."""
    eligible_incoming = len(base) if eligible_incoming_base_count is None else int(eligible_incoming_base_count)
    base_count = int(existing_base_count) + eligible_incoming
    cap = math.floor(base_count * 0.20)
    existing_extension_count = int(existing_core_count) + int(existing_targeted_count)
    available = max(0, cap - existing_extension_count)
    core_items, targeted_items = list(core), list(targeted)
    existing_windows = {int(x) for x in existing_core_windows}
    mandatory: List[Dict[str, Any]] = []
    for window in (22, 66):
        if window in existing_windows:
            continue
        choices = [x for x in core_items if x.get("operator") == "ts_std_dev" and x.get("window") == window]
        if not choices:
            raise ConfigError("CANDIDATE_EXTENSION_CORE_WINDOWS_MISSING")
        mandatory.append(sorted(choices, key=_extension_sort_key)[0])
    if len(mandatory) > available:
        raise ConfigError("CANDIDATE_EXTENSION_POOL_CAP_INSUFFICIENT_FOR_CORE_WINDOWS")

    retained_core = list(mandatory)
    remaining_after_mandatory = available - len(retained_core)
    base_targeted_reserve = max(0, math.floor(cap * 0.25) - int(existing_targeted_count))
    targeted_reserved_capacity = min(remaining_after_mandatory, base_targeted_reserve)
    if targeted_items and remaining_after_mandatory > 0 and int(existing_targeted_count) == 0:
        targeted_reserved_capacity = max(1, targeted_reserved_capacity)
        targeted_reserved_capacity = min(remaining_after_mandatory, targeted_reserved_capacity)
    retained_targeted = _round_robin_extension_items(targeted_items, targeted_reserved_capacity)
    unused_targeted_capacity_returned_to_core = targeted_reserved_capacity - len(retained_targeted)
    retained_core.extend(_round_robin_extension_items(
        [x for x in core_items if x["candidate_id"] not in {y["candidate_id"] for y in retained_core}],
        remaining_after_mandatory - len(retained_targeted),
    ))
    retained = retained_core + retained_targeted
    retained_ids = {x["candidate_id"] for x in retained}
    report = {
        "base_count": base_count,
        "incoming_base_count": len(base),
        "eligible_incoming_base_count": eligible_incoming,
        "duplicate_skipped": int(duplicate_skipped),
        "persisted_duplicate_skipped": int(persisted_duplicate_skipped),
        "cache_only_base_excluded": int(cache_only_base_excluded),
        "persisted_base_count": int(existing_base_count),
        "persisted_extension_count": existing_extension_count,
        "core_generated": len(core_items),
        "core_retained": sum(x["candidate_id"] in retained_ids for x in core_items),
        "core_pruned": sum(x["candidate_id"] not in retained_ids for x in core_items),
        # Compatibility aliases for existing preview consumers.
        "core_restore_generated": len(core_items),
        "core_restore_retained": sum(x["candidate_id"] in retained_ids for x in core_items),
        "core_restore_pruned": sum(x["candidate_id"] not in retained_ids for x in core_items),
        "targeted_generated": len(targeted_items),
        "targeted_reserved_capacity": targeted_reserved_capacity,
        "targeted_retained": sum(x["candidate_id"] in retained_ids for x in targeted_items),
        "targeted_pruned": sum(x["candidate_id"] not in retained_ids for x in targeted_items),
        "unused_targeted_capacity_returned_to_core": unused_targeted_capacity_returned_to_core,
        "extension_cap": cap,
        "available_extension_capacity": available,
        "final_growth_ratio": ((existing_extension_count + len(retained)) / base_count) if base_count else 0.0,
        "total_growth_ratio": ((existing_extension_count + len(retained)) / base_count) if base_count else 0.0,
    }
    return list(retained), report


def _validate_extension_spec(spec: Mapping[str, Any]) -> Dict[str, Any]:
    field = dict(spec.get("field") or {})
    dataset_id = str(field.get("dataset_id") or "").strip()
    field_id = str(field.get("field_id") or "").strip()
    operator = str(spec.get("operator") or "").lower()
    try:
        window = int(spec.get("window"))
    except (TypeError, ValueError) as exc:
        raise ConfigError("CANDIDATE_EXTENSION_WINDOW_INVALID") from exc
    if not dataset_id or not field_id:
        raise ConfigError("CANDIDATE_EXTENSION_FIELD_IDENTITY_REQUIRED")
    if str(field.get("field_type") or "").upper() != EXTENSION_FIELD_TYPE:
        raise ConfigError("CANDIDATE_EXTENSION_MATRIX_ONLY")
    if operator not in EXTENSION_ALLOWED_OPERATORS:
        raise ConfigError("CANDIDATE_EXTENSION_OPERATOR_NOT_ALLOWED:" + operator)
    if window not in EXTENSION_ALLOWED_WINDOWS:
        raise ConfigError("CANDIDATE_EXTENSION_WINDOW_NOT_ALLOWED:" + str(window))
    field["dataset_id"] = dataset_id
    field["field_id"] = field_id
    return {**dict(spec), "field": field, "operator": operator, "window": window}


def generate_candidate_preview(
    discovery: Any,
    dry_run: Mapping[str, Any],
    config: Any,
    *,
    run_id: str,
    alpha_db: Path,
    machine_lib: Any,
    extension_specs: Sequence[Mapping[str, Any]] = (),
    extension_pool_state: Optional[Mapping[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    _validate_snapshots(discovery, dry_run, config)
    selected_fields = [field for field in discovery.fields if field.get("selected")]
    if not selected_fields:
        raise ConfigError("DISCOVERY_SNAPSHOT_REQUIRED: no selected fields")
    for field in selected_fields:
        if not str(field.get("dataset_id") or "").strip() or not str(field.get("field_id") or "").strip():
            raise ConfigError("DISCOVERY_SELECTED_FIELD_IDENTITY_REQUIRED")
    known_fields = [str(field["field_id"]) for field in discovery.fields]
    dataset_scores = {x["dataset_id"]: float(x.get("dataset_preselection_score") or 0) for x in discovery.datasets}
    settings_plan = config.plan["simulation_settings"]
    dry_run_id = dry_run["dry_run_id"]
    snapshot_id = discovery.snapshot["snapshot_id"]
    target_mode = config.target_mode
    pp_policy = config.rules["operator_count_policy"]["pp_estimator"]
    pp_max = int(config.rules["power_pool_base_presets"]["operator_count"]["preset_max"])
    field_max = int(config.rules["power_pool_base_presets"]["data_field_count"]["preset_max"])
    records: List[Dict[str, Any]] = []

    for field in selected_fields:
        route = config.rules["candidate_routes"][field["semantic_class"]]
        reducers = config.rules["vector_reducers"][field["field_type"]]
        for reducer_raw in reducers:
            reducer = "IDENTITY" if str(reducer_raw).lower() == "identity" else str(reducer_raw).upper()
            for transform, window in expand_route(route, field["classification_confidence"]):
                records.append(_build_candidate_record(
                    field, reducer, transform, window, route_priority=route.get("priority", "LOW"),
                    extension_metadata=None, score_adjustment=0.0, known_fields=known_fields,
                    dataset_scores=dataset_scores, config=config, run_id=run_id, alpha_db=alpha_db,
                    machine_lib=machine_lib, settings_plan=settings_plan, dry_run_id=dry_run_id,
                    snapshot_id=snapshot_id, target_mode=target_mode, pp_policy=pp_policy,
                    pp_max=pp_max, field_max=field_max,
                ))

    core_records: List[Dict[str, Any]] = []
    targeted_records: List[Dict[str, Any]] = []
    selected_keys = {(str(x["dataset_id"]), str(x["field_id"])) for x in selected_fields}
    for spec in extension_specs:
        validated_spec = _validate_extension_spec(spec)
        field = dict(validated_spec["field"])
        if (str(field.get("dataset_id") or ""), str(field.get("field_id") or "")) not in selected_keys:
            continue
        metadata = dict(validated_spec.get("metadata") or {})
        item = _build_candidate_record(
            field, "IDENTITY", str(validated_spec["operator"]), int(validated_spec["window"]),
            route_priority=str(validated_spec.get("route_priority") or "LOW"), extension_metadata=metadata,
            score_adjustment=float(validated_spec.get("score_adjustment") or 0.0), known_fields=known_fields,
            dataset_scores=dataset_scores, config=config, run_id=run_id, alpha_db=alpha_db,
            machine_lib=machine_lib, settings_plan=settings_plan, dry_run_id=dry_run_id,
            snapshot_id=snapshot_id, target_mode=target_mode, pp_policy=pp_policy,
            pp_max=pp_max, field_max=field_max,
        )
        (targeted_records if metadata.get("extension_source") == "TARGETED_OPERATOR_EXTENSION" else core_records).append(item)

    pool_state = dict(extension_pool_state or {})
    extension_enabled = bool(extension_specs) or bool(pool_state.get("extension_enabled"))
    if extension_enabled:
        persisted_sim_keys = {str(x) for x in pool_state.get("existing_sim_keys") or () if x}

        def dedupe(rows: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int]:
            kept: List[Dict[str, Any]] = []
            seen = set()
            duplicate_count = 0
            persisted_count = 0
            for row in sorted(rows, key=lambda x: (
                str(x.get("dataset_id") or ""), str(x.get("field_id") or ""),
                str(x.get("operator") or ""), int(x.get("window") or 0),
                str(x.get("expression_canonical") or ""), str(x.get("settings_hash") or ""),
                str(x.get("sim_key") or ""),
            )):
                sim_key = str(row.get("sim_key") or "")
                if sim_key in persisted_sim_keys:
                    persisted_count += 1
                    continue
                if sim_key in seen:
                    duplicate_count += 1
                    continue
                seen.add(sim_key)
                kept.append(row)
            return kept, duplicate_count, persisted_count

        records, base_duplicates, base_persisted_duplicates = dedupe(records)
        core_records, core_duplicates, core_persisted_duplicates = dedupe(core_records)
        targeted_records, targeted_duplicates, targeted_persisted_duplicates = dedupe(targeted_records)
        post_actions = {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"}
        eligible_incoming_base = sum(
            str(x.get("structure_status") or "") == "ELIGIBLE"
            and str(x.get("execution_action") or "") in post_actions
            for x in records
        )
        cache_only_base_excluded = len(records) - eligible_incoming_base
        retained_extensions, extension_report = _prune_extension_candidates(
            records, core_records, targeted_records,
            existing_base_count=int(pool_state.get("existing_base_count") or 0),
            existing_core_count=int(pool_state.get("existing_core_count") or 0),
            existing_targeted_count=int(pool_state.get("existing_targeted_count") or 0),
            existing_core_windows=pool_state.get("existing_core_windows") or (),
            eligible_incoming_base_count=eligible_incoming_base,
            duplicate_skipped=base_duplicates + core_duplicates + targeted_duplicates,
            persisted_duplicate_skipped=(base_persisted_duplicates + core_persisted_duplicates
                                         + targeted_persisted_duplicates),
            cache_only_base_excluded=cache_only_base_excluded,
        )
        records.extend(retained_extensions)
    else:
        extension_report = {
            "base_count": len(records), "incoming_base_count": len(records),
            "eligible_incoming_base_count": len(records), "extension_cap": 0,
            "core_generated": 0, "core_retained": 0, "core_pruned": 0,
            "targeted_generated": 0, "targeted_reserved_capacity": 0,
            "targeted_retained": 0, "targeted_pruned": 0,
            "duplicate_skipped": 0, "persisted_duplicate_skipped": 0,
            "cache_only_base_excluded": 0, "final_growth_ratio": 0.0,
        }

    eligible = [x for x in records if x["structure_status"] == "ELIGIBLE"]
    allocation = simulation_budget_allocation(config.plan)
    selected = diversity_select(eligible, allocation["initial_search_budget"], config.rules["initial_selection"])
    selected_ids = {x["candidate_id"] for x in selected}
    for item in records:
        if item["candidate_id"] not in selected_ids:
            item["selected_for_initial_search"] = False
    selected_cache = Counter(x["cache_classification"] for x in selected)
    selected_actions = Counter(x["execution_action"] for x in selected)
    report = {
        "mode": "CANDIDATE_EXECUTION_PREVIEW", "run_id": run_id,
        "discovery_snapshot_id": snapshot_id, "dry_run_snapshot_id": dry_run_id,
        "planned_candidates_total": len(records),
        "structure_rejected": len(records) - len(eligible), "eligible_initial_candidates": len(eligible),
        "selected_for_initial_search": len(selected),
        "truncated_by_initial_budget": max(0, len(eligible) - len(selected)),
        "candidate_count_by_dataset": dict(sorted(Counter(x["dataset_id"] for x in records).items())),
        "selected_count_by_dataset": dict(sorted(Counter(x["dataset_id"] for x in selected).items())),
        "selected_count_by_semantic_class": dict(sorted(Counter(x["semantic_class"] for x in selected).items())),
        "selected_count_by_field": dict(sorted(Counter(x["field_id"] for x in selected).items())),
        "selected_count_by_transform": dict(sorted(Counter(x["transform_family"] for x in selected).items())),
        "legacy_operator_count_distribution": dict(sorted(Counter(x["legacy_unique_operator_count"] for x in selected).items())),
        "pp_operator_count_distribution": dict(sorted(Counter(x["pp_total_operator_count_estimate"] for x in selected).items())),
        "cache_classification": dict(sorted(selected_cache.items())), "execution_actions": dict(sorted(selected_actions.items())),
        "required_new_posts": int(selected_actions.get("NEW_SIMULATION_REQUIRED", 0)),
        "initial_budget": allocation["initial_search_budget"],
        "initial_budget_consumed_if_executed": int(selected_actions.get("NEW_SIMULATION_REQUIRED", 0)),
        "initial_budget_remaining": allocation["initial_search_budget"] - int(selected_actions.get("NEW_SIMULATION_REQUIRED", 0)),
        "repair_reserve_budget": allocation["repair_reserve_budget"],
        "unused_initial_budget": allocation["initial_search_budget"] - len(selected),
        "duplicate_candidate_ids": len(records) - len({x["candidate_id"] for x in records}),
        "duplicate_sim_keys": len(records) - len({x["sim_key"] for x in records}),
        "snapshot_mismatch": False, "network_requests": 0, "simulation_posts": 0,
        "extension_preflight": extension_report,
    }
    return records, report
