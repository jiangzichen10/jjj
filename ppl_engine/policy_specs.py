"""Pure V3.1 Search/Repair policy normalization and identity helpers.

C5 separates mutable research policy from execution semantics.  This module
contains no durable store, sqlite connection, HTTP session, workflow transition
or remote side effect.  It turns the historical mixed V3 round-policy layout
into explicit Search and Repair policy sections while retaining legacy fallback
for old/sanitized policy snapshots.
"""
from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Dict, Mapping, Tuple

SEARCH_POLICY_SCHEMA = "V31_SEARCH_POLICY_V1"
REPAIR_POLICY_SCHEMA = "V31_REPAIR_POLICY_V1"

SEARCH_RANKING_KEYS: Tuple[str, ...] = (
    "prior_initial_scale", "prior_min_scale", "prior_half_life_attempts",
    "evidence_shrinkage_attempts", "exploration_evidence_fraction",
    "exploration_novelty_weight", "terminal_fail_penalty", "zero_viable_penalty",
    "zero_viable_min_attempts", "recent_zero_positive_min_attempts",
    "recent_zero_positive_penalty", "recent_zero_positive_consecutive_batches_cooldown",
    "exploit_min_combo_attempts", "exploit_min_combo_viable",
    "search_positive_sharpe_min", "search_strong_sharpe_min", "search_elite_sharpe_min",
    "max_unproven_dataset_fraction", "max_unproven_combo_per_batch",
    "dimension_weights", "stage_weights",
)

REPAIR_RANKING_KEYS: Tuple[str, ...] = (
    "repair_good_sharpe_min", "repair_elite_sharpe_min",
    "repair_turnover_near_max", "repair_turnover_mid_max",
    "direction_repair_positive_abs_sharpe_min",
    "direction_repair_strong_abs_sharpe_min",
    "direction_repair_elite_abs_sharpe_min",
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _legacy_search_ranking(policy: Mapping[str, Any]) -> Dict[str, Any]:
    adaptive = dict(policy.get("adaptive_ranking") or {})
    return {key: copy.deepcopy(adaptive[key]) for key in SEARCH_RANKING_KEYS if key in adaptive}


def _legacy_repair_ranking(policy: Mapping[str, Any]) -> Dict[str, Any]:
    adaptive = dict(policy.get("adaptive_ranking") or {})
    return {key: copy.deepcopy(adaptive[key]) for key in REPAIR_RANKING_KEYS if key in adaptive}


def normalize_search_policy(policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Return one explicit Search policy, with legacy fallback for old snapshots."""
    explicit = copy.deepcopy(dict(policy.get("search_policy") or {}))
    allocation = {
        "batch_size": int(policy.get("batch_size", 40)),
        "exploration_fraction": float(policy.get("exploration_fraction", 0.30)),
    }
    allocation.update(copy.deepcopy(dict(explicit.get("allocation") or {})))
    ranking = _legacy_search_ranking(policy)
    ranking.update(copy.deepcopy(dict(explicit.get("ranking") or {})))
    diversity = copy.deepcopy(dict(explicit.get("diversity") or {}))
    normalized = {
        "schema": str(explicit.get("schema") or SEARCH_POLICY_SCHEMA),
        "mode": str(explicit.get("mode") or "LEGACY_PARITY"),
        "allocation": allocation,
        "ranking": ranking,
        "diversity": diversity,
    }
    validate_search_policy(normalized)
    return normalized


def normalize_repair_policy(policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Return one explicit Repair policy, with legacy fallback for old snapshots."""
    explicit = copy.deepcopy(dict(policy.get("repair_policy") or {}))
    allocation = {"batch_size": int(policy.get("batch_size", 40))}
    allocation.update(copy.deepcopy(dict(explicit.get("allocation") or {})))
    ranking = _legacy_repair_ranking(policy)
    ranking.update(copy.deepcopy(dict(explicit.get("ranking") or {})))
    planning = {
        "normal_near_pass_repair_cap_per_family": int(policy.get("normal_near_pass_repair_cap_per_family", 1)),
        "strong_near_pass_repair_cap_per_family": int(policy.get("strong_near_pass_repair_cap_per_family", 2)),
    }
    planning.update(copy.deepcopy(dict(explicit.get("planning") or {})))
    normalized = {
        "schema": str(explicit.get("schema") or REPAIR_POLICY_SCHEMA),
        "mode": str(explicit.get("mode") or "LEGACY_PARITY"),
        "allocation": allocation,
        "ranking": ranking,
        "planning": planning,
    }
    validate_repair_policy(normalized)
    return normalized


def validate_search_policy(search: Mapping[str, Any]) -> None:
    if str(search.get("schema") or "") != SEARCH_POLICY_SCHEMA:
        raise ValueError("SEARCH_POLICY_SCHEMA_UNSUPPORTED")
    allocation = dict(search.get("allocation") or {})
    batch = int(allocation.get("batch_size", 0))
    if batch <= 0:
        raise ValueError("SEARCH_POLICY_BATCH_SIZE_INVALID")
    exploration = float(allocation.get("exploration_fraction", -1))
    if not 0.0 <= exploration <= 1.0:
        raise ValueError("SEARCH_POLICY_EXPLORATION_FRACTION_INVALID")
    ranking = dict(search.get("ranking") or {})
    if ranking:
        if float(ranking.get("prior_initial_scale", 0.30)) < float(ranking.get("prior_min_scale", 0.12)):
            raise ValueError("SEARCH_POLICY_PRIOR_SCALE_INVALID")
        if float(ranking.get("prior_half_life_attempts", 8.0)) <= 0 or float(ranking.get("evidence_shrinkage_attempts", 10.0)) <= 0:
            raise ValueError("SEARCH_POLICY_SHRINKAGE_INVALID")
        if not 0.0 <= float(ranking.get("exploration_evidence_fraction", 0.20)) <= 1.0:
            raise ValueError("SEARCH_POLICY_EVIDENCE_FRACTION_INVALID")
        if float(ranking.get("terminal_fail_penalty", 32.0)) < 0 or float(ranking.get("zero_viable_penalty", 8.0)) < 0:
            raise ValueError("SEARCH_POLICY_FAILURE_PENALTY_INVALID")
        if int(ranking.get("zero_viable_min_attempts", 4)) < 1 or int(ranking.get("exploit_min_combo_attempts", 2)) < 1:
            raise ValueError("SEARCH_POLICY_SAMPLE_INVALID")
        if int(ranking.get("recent_zero_positive_min_attempts", 8)) < 1:
            raise ValueError("SEARCH_POLICY_RECENT_SAMPLE_INVALID")
        if float(ranking.get("recent_zero_positive_penalty", 18.0)) < 0:
            raise ValueError("SEARCH_POLICY_RECENT_PENALTY_INVALID")
        if int(ranking.get("recent_zero_positive_consecutive_batches_cooldown", 2)) < 2:
            raise ValueError("SEARCH_POLICY_RECENT_COOLDOWN_INVALID")
        if int(ranking.get("exploit_min_combo_viable", 1)) < 1 or int(ranking.get("max_unproven_combo_per_batch", 4)) < 1:
            raise ValueError("SEARCH_POLICY_EXPLOIT_GATE_INVALID")
        pos = float(ranking.get("search_positive_sharpe_min", 1.60))
        strong = float(ranking.get("search_strong_sharpe_min", 2.00))
        elite = float(ranking.get("search_elite_sharpe_min", 3.00))
        if not (1.0 <= pos <= strong <= elite):
            raise ValueError("SEARCH_POLICY_SHARPE_BANDS_INVALID")
        if not 0.0 < float(ranking.get("max_unproven_dataset_fraction", 0.15)) <= 1.0:
            raise ValueError("SEARCH_POLICY_UNPROVEN_FRACTION_INVALID")
        weights = dict(ranking.get("dimension_weights") or {})
        if weights and abs(sum(float(x) for x in weights.values()) - 1.0) > 1e-9:
            raise ValueError("SEARCH_POLICY_DIMENSION_WEIGHTS_MUST_SUM_TO_ONE")


def validate_repair_policy(repair: Mapping[str, Any]) -> None:
    if str(repair.get("schema") or "") != REPAIR_POLICY_SCHEMA:
        raise ValueError("REPAIR_POLICY_SCHEMA_UNSUPPORTED")
    allocation = dict(repair.get("allocation") or {})
    if int(allocation.get("batch_size", 0)) <= 0:
        raise ValueError("REPAIR_POLICY_BATCH_SIZE_INVALID")
    ranking = dict(repair.get("ranking") or {})
    good = float(ranking.get("repair_good_sharpe_min", 2.00))
    elite = float(ranking.get("repair_elite_sharpe_min", 3.00))
    if not (1.0 <= good <= elite):
        raise ValueError("REPAIR_POLICY_SHARPE_BANDS_INVALID")
    near = float(ranking.get("repair_turnover_near_max", 0.80))
    mid = float(ranking.get("repair_turnover_mid_max", 1.00))
    if not (0.70 < near <= mid):
        raise ValueError("REPAIR_POLICY_TURNOVER_BANDS_INVALID")
    dpos = float(ranking.get("direction_repair_positive_abs_sharpe_min", 1.60))
    dstrong = float(ranking.get("direction_repair_strong_abs_sharpe_min", 2.00))
    delite = float(ranking.get("direction_repair_elite_abs_sharpe_min", 3.00))
    if not (1.0 <= dpos <= dstrong <= delite):
        raise ValueError("REPAIR_POLICY_DIRECTION_BANDS_INVALID")
    planning = dict(repair.get("planning") or {})
    normal = int(planning.get("normal_near_pass_repair_cap_per_family", 1))
    strong_cap = int(planning.get("strong_near_pass_repair_cap_per_family", 2))
    if normal < 0 or strong_cap < normal:
        raise ValueError("REPAIR_POLICY_FAMILY_CAPS_INVALID")


def effective_search_allocation(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_search_policy(policy)["allocation"]))


def effective_search_ranking(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_search_policy(policy)["ranking"]))


def effective_search_diversity(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_search_policy(policy)["diversity"]))


def effective_repair_allocation(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_repair_policy(policy)["allocation"]))


def effective_repair_ranking(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_repair_policy(policy)["ranking"]))


def effective_repair_planning(policy: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(normalize_repair_policy(policy)["planning"]))


def search_policy_hash(policy: Mapping[str, Any]) -> str:
    return _hash(normalize_search_policy(policy))


def repair_policy_hash(policy: Mapping[str, Any]) -> str:
    return _hash(normalize_repair_policy(policy))


def build_search_policy_payload(policy: Mapping[str, Any]) -> Dict[str, Any]:
    versions = dict(policy.get("policy_versions") or {})
    normalized = normalize_search_policy(policy)
    return {
        "policy_type": "SEARCH",
        "policy_version": str(versions.get("search") or ""),
        "policy_hash": _hash(normalized),
        "search_policy": normalized,
    }


def build_repair_policy_payload(policy: Mapping[str, Any]) -> Dict[str, Any]:
    versions = dict(policy.get("policy_versions") or {})
    normalized = normalize_repair_policy(policy)
    return {
        "policy_type": "REPAIR",
        "policy_version": str(versions.get("repair") or ""),
        "policy_hash": _hash(normalized),
        "repair_policy": normalized,
    }


def install_dedicated_policy_sections(policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a policy with explicit normalized Search/Repair sections installed."""
    out = copy.deepcopy(dict(policy))
    out["search_policy"] = normalize_search_policy(out)
    out["repair_policy"] = normalize_repair_policy(out)
    return out
