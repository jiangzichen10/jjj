"""Pure V3.1 Repair prioritization logic.

The engine remains responsible for creating/preflighting concrete repair plans,
reading durable state and performing remote work.  This module owns only the
research-policy decision of which repair opportunities are more valuable.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence, Tuple

from .policy_specs import effective_repair_ranking


def _float_or_none(value: Any):
    try:
        if value is None or str(value).strip().lower() in {"", "none", "nan", "null"}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def repair_value(item: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    adaptive = effective_repair_ranking(policy)
    fixed = dict((policy.get("ppl_classification") or {}).get("fixed_gates") or {})
    turnover_max = float(fixed.get("turnover_max", 0.70))
    good_min = float(adaptive.get("repair_good_sharpe_min", 2.00))
    elite_min = float(adaptive.get("repair_elite_sharpe_min", 3.00))
    near_max = float(adaptive.get("repair_turnover_near_max", 0.80))
    mid_max = float(adaptive.get("repair_turnover_mid_max", 1.00))
    sharpe = _float_or_none(item.get("sharpe"))
    turnover = _float_or_none(item.get("turnover"))
    if sharpe is None or turnover is None or turnover <= turnover_max or sharpe < good_min:
        return {"band": "ORDINARY_REPAIR", "score": 0, "sharpe": sharpe, "turnover": turnover}
    proximity = 2 if turnover <= near_max else 1 if turnover <= mid_max else 0
    if sharpe >= elite_min:
        return {"band": "ELITE_REPAIR", "score": 3 + proximity, "sharpe": sharpe, "turnover": turnover}
    return {"band": "GOOD_REPAIR", "score": 1 + proximity, "sharpe": sharpe, "turnover": turnover}


def direction_repair_value(item: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    adaptive = effective_repair_ranking(policy)
    fixed = dict((policy.get("ppl_classification") or {}).get("fixed_gates") or {})
    turnover_min = float(fixed.get("turnover_min", 0.01))
    turnover_max = float(fixed.get("turnover_max", 0.70))
    positive_min = float(adaptive.get("direction_repair_positive_abs_sharpe_min", 1.60))
    strong_min = float(adaptive.get("direction_repair_strong_abs_sharpe_min", 2.00))
    elite_min = float(adaptive.get("direction_repair_elite_abs_sharpe_min", 3.00))
    sharpe = _float_or_none(item.get("sharpe"))
    turnover = _float_or_none(item.get("turnover"))
    if sharpe is None or turnover is None or sharpe >= 0 or not (turnover_min <= turnover <= turnover_max):
        return {"band": "NOT_DIRECTION_REPAIR", "score": 0, "sharpe": sharpe, "turnover": turnover}
    magnitude = abs(sharpe)
    if magnitude < positive_min:
        return {"band": "NOT_DIRECTION_REPAIR", "score": 0, "sharpe": sharpe, "turnover": turnover}
    if magnitude >= elite_min:
        return {"band": "DIRECTION_REPAIR_ELITE", "score": 10, "sharpe": sharpe, "turnover": turnover}
    if magnitude >= strong_min:
        return {"band": "DIRECTION_REPAIR_STRONG", "score": 8, "sharpe": sharpe, "turnover": turnover}
    return {"band": "DIRECTION_REPAIR_POSITIVE", "score": 6, "sharpe": sharpe, "turnover": turnover}


def rank_repair_candidates(items: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> Tuple[Mapping[str, Any], ...]:
    """Return immutable parity-ranked repair opportunities.

    Concrete plan generation/preview is intentionally outside this function.
    """
    priority_rank = {"HIGH": 0, "MEDIUM": 1}
    ranked = []
    for raw in items:
        item = dict(raw)
        rv = repair_value(item, policy)
        item["round_repair_value_band"] = rv["band"]
        item["round_repair_value_score"] = int(rv["score"])
        ranked.append(item)
    ranked.sort(
        key=lambda x: (
            -int(x.get("round_repair_value_score") or 0),
            priority_rank.get(str(x.get("repair_priority")), 9),
            float(x.get("max_normalized_gap") if x.get("max_normalized_gap") is not None else 999),
            -float(x.get("sharpe") if x.get("sharpe") is not None else -999),
            str(x.get("candidate_id")),
        )
    )
    return tuple(ranked)


def rank_direction_repair_candidates(items: Sequence[Mapping[str, Any]]) -> Tuple[Mapping[str, Any], ...]:
    ranked = [dict(x) for x in items]
    ranked.sort(
        key=lambda x: (
            -int(x.get("round_direction_score") or 0),
            float(x.get("sharpe") if x.get("sharpe") is not None else 0.0),
            str(x.get("candidate_id")),
        )
    )
    return tuple(ranked)
