"""Pure V3.1 Search strategy logic.

This module contains Search scoring/allocation logic only.  It never receives a
RunnerStore, sqlite connection, HTTP session or machine_lib.  Engine code builds
an immutable fact snapshot, this strategy scores/selects it, and the engine
persists/executes the resulting decisions.

C4 intentionally preserves the proven V3.0.4o ranking behavior (parity mode)
while moving the actual research-allocation algorithm behind a strategy seam.
"""
from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence, Tuple

from .policy_specs import effective_search_allocation, effective_search_diversity, effective_search_ranking


def _window_key(value: Any) -> str:
    if value is None or str(value).strip() in {"", "None", "nan"}:
        return "NONE"
    try:
        f = float(value)
        return str(int(f)) if f.is_integer() else str(f)
    except (TypeError, ValueError):
        return str(value)


def _new_evidence_stat() -> Dict[str, int]:
    return {
        "attempts": 0,
        "signal_viable": 0,
        "local_pass": 0,
        "fixed_repairable": 0,
        "search_viable": 0,
        "search_strong": 0,
        "search_elite": 0,
        "repair_viable": 0,
        "repair_elite": 0,
        "terminal_fail": 0,
        "pretag_resolved": 0,
        "ppl_near_pass": 0,
        "ppl_strong_near_pass": 0,
        "ppl_success": 0,
    }


def evidence_viable_count(stat: Mapping[str, int]) -> int:
    if "search_viable" in stat:
        return int(stat.get("search_viable", 0))
    return max(
        int(stat.get("local_pass", 0)), int(stat.get("pretag_resolved", 0)),
        int(stat.get("ppl_near_pass", 0)), int(stat.get("ppl_strong_near_pass", 0)),
        int(stat.get("ppl_success", 0)),
    )


def stage_evidence_score(stat: Mapping[str, int], adaptive: Mapping[str, Any]) -> float:
    attempts = int(stat.get("attempts", 0))
    if attempts <= 0:
        return 0.0
    shrink = float(adaptive.get("evidence_shrinkage_attempts", 10.0))
    reliability = attempts / (attempts + shrink)
    weights = dict(adaptive.get("stage_weights") or {})
    raw = 0.0
    for name in (
        "signal_viable", "local_pass", "fixed_repairable",
        "search_viable", "search_strong", "search_elite",
        "repair_viable", "repair_elite", "pretag_resolved",
        "ppl_near_pass", "ppl_strong_near_pass", "ppl_success",
    ):
        raw += float(weights.get(name, 0.0)) * (int(stat.get(name, 0)) / attempts)
    terminal_fail = int(stat.get("terminal_fail", max(0, attempts - evidence_viable_count(stat))))
    raw -= float(adaptive.get("terminal_fail_penalty", 32.0)) * (terminal_fail / attempts)
    if attempts >= int(adaptive.get("zero_viable_min_attempts", 4)) and evidence_viable_count(stat) == 0:
        raw -= float(adaptive.get("zero_viable_penalty", 8.0))
    return round(reliability * raw, 6)


def adaptive_scores(
    row: Mapping[str, Any],
    evidence: Mapping[str, Mapping[Any, Mapping[str, int]]],
    policy: Mapping[str, Any],
) -> Dict[str, Any]:
    """Parity implementation of V3_RANK_005 Search scoring."""
    adaptive = effective_search_ranking(policy)
    dataset = str(row.get("dataset_id") or "UNKNOWN")
    operator = str(row.get("operator") or "UNKNOWN")
    window = _window_key(row.get("window"))
    keys = {
        "dataset": dataset,
        "operator": operator,
        "dataset_operator": (dataset, operator),
        "operator_window": (operator, window),
        "dataset_operator_window": (dataset, operator, window),
    }
    components: Dict[str, Dict[str, Any]] = {}
    dimension_weights = dict(adaptive.get("dimension_weights") or {})
    online = 0.0
    novelty = 0.0
    for dim, key in keys.items():
        stat = dict((evidence.get(dim) or {}).get(key) or _new_evidence_stat())
        score = stage_evidence_score(stat, adaptive)
        weight = float(dimension_weights.get(dim, 0.0))
        attempts = int(stat.get("attempts", 0))
        online += weight * score
        novelty += weight * (1.0 / math.sqrt(attempts + 1.0))
        components[dim] = {"key": key, "attempts": attempts, "score": score, **stat}

    combo_stat = components["dataset_operator_window"]
    combo_attempts = int(combo_stat.get("attempts", 0))
    combo_viable = evidence_viable_count(combo_stat)
    min_attempts = int(adaptive.get("exploit_min_combo_attempts", 2))
    min_viable = int(adaptive.get("exploit_min_combo_viable", 1))
    exploit_eligible = combo_attempts >= min_attempts and combo_viable >= min_viable
    if combo_attempts < min_attempts:
        exploit_gate_reason = "INSUFFICIENT_COMBO_EVIDENCE"
    elif combo_viable < min_viable:
        exploit_gate_reason = "NO_VIABLE_COMBO_EVIDENCE"
    else:
        exploit_gate_reason = "POSITIVE_COMBO_EVIDENCE"

    recent_rows = []
    for key, stat in (evidence.get("dataset_operator_window_batch") or {}).items():
        if not isinstance(key, tuple) or len(key) != 4:
            continue
        if tuple(key[:3]) != (dataset, operator, window):
            continue
        bno = int(key[3] or 0)
        if bno <= 0:
            continue
        recent_rows.append((bno, dict(stat)))
    recent_rows.sort(key=lambda x: x[0], reverse=True)
    recent_min_attempts = int(adaptive.get("recent_zero_positive_min_attempts", 8))
    zero_positive_streak = 0
    latest_recent = None
    for bno, stat in recent_rows:
        attempts = int(stat.get("attempts", 0))
        positives = evidence_viable_count(stat)
        if latest_recent is None:
            latest_recent = {"batch_no": bno, "attempts": attempts, "search_positive": positives}
        if attempts >= recent_min_attempts and positives == 0:
            zero_positive_streak += 1
            continue
        break
    recent_penalty = 0.0
    if latest_recent and int(latest_recent["attempts"]) >= recent_min_attempts and int(latest_recent["search_positive"]) == 0:
        recent_penalty = float(adaptive.get("recent_zero_positive_penalty", 18.0))
    cooldown_after = int(adaptive.get("recent_zero_positive_consecutive_batches_cooldown", 2))
    if zero_positive_streak >= cooldown_after:
        exploit_eligible = False
        exploit_gate_reason = "RECENT_ZERO_SEARCH_POSITIVE_COOLDOWN"

    prior_initial = float(adaptive.get("prior_initial_scale", 0.30))
    prior_min = float(adaptive.get("prior_min_scale", 0.12))
    half_life = float(adaptive.get("prior_half_life_attempts", 8.0))
    prior_scale = prior_min + (prior_initial - prior_min) / (1.0 + combo_attempts / half_life)
    base = float(row.get("initial_selection_score") or 0.0)
    exploit = prior_scale * base + online - recent_penalty
    explore = (
        prior_min * base
        + float(adaptive.get("exploration_evidence_fraction", 0.20)) * online
        + float(adaptive.get("exploration_novelty_weight", 10.0)) * novelty
    )
    return {
        "exploit": round(exploit, 6),
        "explore": round(explore, 6),
        "online_evidence": round(online, 6),
        "novelty": round(novelty, 6),
        "prior_scale": round(prior_scale, 6),
        "combo_scope": "DATASET_OPERATOR_WINDOW",
        "combo_attempts": combo_attempts,
        "combo_viable": combo_viable,
        "recent_zero_positive_penalty": round(recent_penalty, 6),
        "recent_zero_positive_streak": int(zero_positive_streak),
        "recent_window_batch": latest_recent,
        "exploit_eligible": bool(exploit_eligible),
        "exploit_gate_reason": exploit_gate_reason,
        "components": components,
    }


@dataclass(frozen=True)
class SearchSelectionResult:
    selected: Tuple[Mapping[str, Any], ...]
    paid_rep_by_family: Mapping[str, Mapping[str, Any]]
    exploit_pool_count: int
    unproven_pool_count: int
    batch_size: int
    extension_batch_cap: int
    selection_modes: Mapping[str, int]


def select_search_candidates(
    classified: Sequence[Mapping[str, Any]],
    *,
    protected_families: Sequence[str],
    active_datasets: Sequence[str],
    attempted_families: Sequence[str],
    initial_rules: Mapping[str, Any],
    policy: Mapping[str, Any],
    remaining: int,
    extension_batch_cap: int,
) -> SearchSelectionResult:
    """Pure parity allocator for already-classified candidate facts.

    Each row must carry ``_strategy_family_id`` and all ``round_*`` scoring/cache
    facts. No durable state or remote side effect is available here.
    """
    protected = set(str(x) for x in protected_families)
    active = set(str(x) for x in active_datasets)
    attempted = set(str(x) for x in attempted_families)
    rows = [dict(x) for x in classified]

    free = [x for x in rows if x.get("round_cache_action") in {"CACHE_RESTORE", "RESUME_EXISTING"}]
    per_family: Dict[str, Tuple[float, Dict[str, Any]]] = {}
    for row in rows:
        if row.get("round_cache_action") in {"CACHE_RESTORE", "RESUME_EXISTING"}:
            continue
        if not bool(row.get("_strategy_requires_new_post")):
            continue
        if active and str(row.get("dataset_id") or "") not in active:
            continue
        fid = str(row.get("_strategy_family_id") or "")
        if not fid or fid in protected or fid in attempted:
            continue
        score = float(row.get("round_exploit_score") or 0.0)
        current = per_family.get(fid)
        if current is None or score > current[0]:
            per_family[fid] = (score, row)
    misses = [v[1] for v in per_family.values()]

    if not free and not misses:
        return SearchSelectionResult((), {}, 0, 0, 0, int(extension_batch_cap), {})

    allocation = effective_search_allocation(policy)
    diversity = {**dict(initial_rules or {}), **effective_search_diversity(policy)}
    batch_size = min(int(allocation["batch_size"]), int(remaining))
    dataset_cap = max(1, math.floor(batch_size * float(diversity.get("max_dataset_fraction", 0.35))))
    semantic_cap = max(1, math.floor(batch_size * float(diversity.get("max_semantic_class_fraction", 0.50))))
    field_cap = int(diversity.get("max_initial_candidates_per_field", 4))
    counts = {
        "paid": {"dataset": Counter(), "semantic": Counter(), "field": Counter(), "combo": Counter()},
        "free": {"dataset": Counter(), "semantic": Counter(), "field": Counter(), "combo": Counter()},
    }
    adaptive_cfg = effective_search_ranking(policy)
    unproven_dataset_cap = max(1, math.floor(
        batch_size * float(adaptive_cfg.get("max_unproven_dataset_fraction", 0.15))
    ))
    unproven_combo_cap = int(adaptive_cfg.get("max_unproven_combo_per_batch", 4))
    extension_post_selected = 0

    def can_take(row: Mapping[str, Any], *, free_item: bool = False, mode: str | None = None) -> bool:
        ds_cap = 2 * dataset_cap if free_item else dataset_cap
        bucket = counts["free" if free_item else "paid"]
        dataset_key = row.get("dataset_id")
        combo_key = (row.get("dataset_id"), row.get("operator"))
        field_key = (row.get("dataset_id"), row.get("field_id"))
        if not free_item and mode == "EXPLORE" and not bool(row.get("round_exploit_eligible")):
            ds_cap = min(ds_cap, unproven_dataset_cap)
            if bucket["combo"][combo_key] >= unproven_combo_cap:
                return False
        if (
            not free_item
            and row.get("extension_source")
            and bool(row.get("_strategy_requires_new_post"))
            and extension_post_selected >= int(extension_batch_cap)
        ):
            return False
        return (
            bucket["dataset"][dataset_key] < ds_cap
            and bucket["semantic"][row.get("semantic_class")] < semantic_cap
            and bucket["field"][field_key] < field_cap
        )

    def take(pool: Sequence[Dict[str, Any]], limit: int, *, free_item: bool = False, mode: str | None = None):
        nonlocal extension_post_selected
        out = []
        for row in pool:
            if len(out) >= limit:
                break
            if not can_take(row, free_item=free_item, mode=mode):
                continue
            out.append(row)
            bucket = counts["free" if free_item else "paid"]
            bucket["dataset"].update([row.get("dataset_id")])
            bucket["semantic"].update([row.get("semantic_class")])
            bucket["field"].update([(row.get("dataset_id"), row.get("field_id"))])
            bucket["combo"].update([(row.get("dataset_id"), row.get("operator"))])
            if not free_item and row.get("extension_source") and bool(row.get("_strategy_requires_new_post")):
                extension_post_selected += 1
        return out

    def free_quality(row: Mapping[str, Any]):
        rec = row.get("round_cache_record") or {}
        action_rank = 0 if row.get("round_cache_action") == "CACHE_RESTORE" else 1
        return (
            action_rank,
            -float(rec.get("fitness") if rec.get("fitness") is not None else -1e9),
            -float(rec.get("sharpe") if rec.get("sharpe") is not None else -1e9),
            -float(row.get("round_adaptive_score") or 0.0),
            str(row.get("candidate_id") or ""),
        )

    free.sort(key=free_quality)
    misses.sort(key=lambda x: (-float(x.get("round_exploit_score") or 0.0), str(x.get("candidate_id") or "")))
    free_selected = take(free, int(allocation["batch_size"]), free_item=True, mode="FREE")
    exploration_n = min(batch_size, int(round(batch_size * float(allocation["exploration_fraction"]))))
    exploit_n = batch_size - exploration_n
    exploit_pool = [x for x in misses if bool(x.get("round_exploit_eligible"))]
    exploit = take(exploit_pool, exploit_n, mode="EXPLOIT")
    used = {str(x.get("candidate_id") or "") for x in exploit}
    exploratory_pool = sorted(
        (x for x in misses if str(x.get("candidate_id") or "") not in used),
        key=lambda x: (-float(x.get("round_explore_score") or 0.0), str(x.get("candidate_id") or "")),
    )
    exploration = take(exploratory_pool, exploration_n, mode="EXPLORE")
    used.update(str(x.get("candidate_id") or "") for x in exploration)
    backfill_pool = sorted(
        (
            x for x in exploit_pool
            if str(x.get("candidate_id") or "") not in used
            and not (x.get("extension_source") == "TARGETED_OPERATOR_EXTENSION" and not x.get("round_exploit_eligible"))
        ),
        key=lambda x: (-float(x.get("round_exploit_score") or 0.0), str(x.get("candidate_id") or "")),
    )
    backfill = take(backfill_pool, max(0, batch_size - len(exploit) - len(exploration)), mode="BACKFILL")
    for row in free_selected:
        row["round_selection_mode"] = "FREE_CACHE" if row.get("round_cache_action") == "CACHE_RESTORE" else "FREE_RESUME"
    for row in exploit:
        row["round_selection_mode"] = "EXPLOIT"
    for row in exploration:
        row["round_selection_mode"] = "EXPLORE"
    for row in backfill:
        row["round_selection_mode"] = "BACKFILL"
    selected = free_selected + exploit + exploration + backfill
    return SearchSelectionResult(
        selected=tuple(selected),
        paid_rep_by_family={fid: dict(item[1]) for fid, item in per_family.items()},
        exploit_pool_count=len(exploit_pool),
        unproven_pool_count=sum(1 for x in misses if not bool(x.get("round_exploit_eligible"))),
        batch_size=batch_size,
        extension_batch_cap=int(extension_batch_cap),
        selection_modes=dict(Counter(str(x.get("round_selection_mode") or "NONE") for x in selected)),
    )
