"""Near-Pass Rescue Engine for V2.2 Production research.

This module adds research/workflow classification on top of the existing
Phase-9.1 Check Parser facts. It NEVER overrides the platform raw_result,
never equates WARNING with FAIL, and never triggers a Simulation POST. It only
classifies, plans (deferred proposals), and ranks for manual review.

Key concepts:
  - threshold direction (MIN/MAX/UNRESOLVED) per check name
  - normalized gap so different-unit checks are comparable
  - Near-Pass classification: NORMAL / NEAR_PASS / STRONG_NEAR_PASS
  - Rescue Target per failure (HT_RETURNS_RATIO_NEAR_PASS, ...)
  - Rescue Priority (SETTINGS > WINDOW > SIGNAL_SHAPE > ADVANCED > SIBLING)
  - Repair Evidence Memory (aggregated from durable facts + external observations)
  - per-candidate rescue allowance + auto-stop rules
  - Manual Review escalation: P1 / P2 / P3
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .audit_log import audit_event
from .ppl_classifier import (
    REPAIRABLE_STATUSES, classify_ppl_candidate, load_ppl_classification_policy,
    load_ppl_classification_policy_for_config,
)
from .qualification_policy import (
    evaluate_ppl_qualification_compatibility,
    load_qualification_integration, load_qualification_policy_snapshot,
)
from .ppc_controlled_branch import (
    PPC_EVALUATED_OUTCOMES, PPC_TARGET_FAILURE, infer_ppc_branch_anchor,
    ppc_branch_config, ppc_branch_state,
)

# ---------------------------------------------------------------------------
# Configuration (defaults; overridable via rules["near_pass"])
# ---------------------------------------------------------------------------

DEFAULT_NEAR_PASS_CONFIG: Dict[str, Any] = {
    "near_pass_max_blockers": 2,
    "strong_near_pass_max_blockers": 1,
    "near_pass_normalized_gap_max": 0.10,
    "strong_near_pass_normalized_gap_max": 0.05,
    "normal_repair_max_attempts": 1,
    "near_pass_rescue_max_attempts": 3,
    "strong_near_pass_rescue_max_attempts": 5,
    # Used only when a legacy/synthetic check map lacks FITNESS. Live resolved
    # checks remain the preferred source of truth for STRONG_NEAR_PASS.
    "fitness_fallback_min": 1.0,
    # Correlation checks only block STRONG/NEAR when they exceed the live limit
    # severely (relative gap above this). A small overrun (e.g. 0.7112 vs 0.7)
    # is recorded but does not demote a strong near-pass Alpha.
    "correlation_severe_gap": 0.20,
    "neutralization_candidates_by_region": {
        "GLB": ["SUBINDUSTRY", "MARKET"],
    },
}

# Check names that are a "minimum threshold" (higher value is better; a FAIL
# means value < limit). These cover both the raw Live-API names and the
# canonical normalized names.
MIN_THRESHOLD_NAMES = {
    "LOW_SHARPE", "SHARPE", "LOW_FITNESS", "FITNESS",
    "LOW_SUB_UNIVERSE_SHARPE", "SUB_UNIVERSE", "LOW_2Y_SHARPE", "TWO_YEAR_SHARPE",
    "HIGH_TURNOVER_RETURNS_RATIO", "HT_HIGH_TURNOVER_RETURNS_RATIO",
}
# Check names that are a "maximum threshold" (lower value is better; a FAIL
# means value > limit).
MAX_THRESHOLD_NAMES = {
    "POWER_POOL_CORRELATION", "SELF_CORRELATION", "PROD_CORRELATION",
}

# PPL-specific hard checks for Near-Pass classification.  These mirror the
# current rules' Power Pool base/theme requirements and deliberately exclude
# generic quality diagnostics such as PROD_CORRELATION, SELF_CORRELATION,
# FITNESS and TWO_YEAR_SHARPE.  Those remain useful research signals but must
# never decide whether an Alpha is a PPL Near-Pass.
PPL_HARD_CHECKS = {
    "SHARPE", "LOW_SHARPE",
    "SUB_UNIVERSE", "LOW_SUB_UNIVERSE_SHARPE",
    "POWER_POOL_CORRELATION",
    "HIGH_TURNOVER",
    "HIGH_TURNOVER_RETURNS_RATIO", "HT_HIGH_TURNOVER_RETURNS_RATIO",
    "CLASSIFICATION_HIGH_TURNOVER", "MATCHES_CLASSIFICATION",
    "THEME_MATCH", "MATCHES_THEMES", "PURE_POWER_POOL_THEME",
}

QUALITY_DIAGNOSTIC_CHECKS = {
    "FITNESS", "LOW_FITNESS",
    "TWO_YEAR_SHARPE", "LOW_2Y_SHARPE",
    "SELF_CORRELATION", "PROD_CORRELATION", "POWER_POOL_SELF_CORRELATION",
}

PPL_CORRELATION_CHECKS = {"POWER_POOL_CORRELATION"}

# These checks depend on the user's manual PowerPoolSelected/theme tagging step.
# During PRE_TAG they are intentionally deferred: WARNING/FAIL/UNKNOWN must not
# become PPL Near-Pass blockers before the manual tag exists. They become hard
# checks again at POST_TAG.
PRETAG_DEFERRED_PPL_HARD_CHECKS = {
    "THEME_MATCH", "MATCHES_THEMES", "PURE_POWER_POOL_THEME",
}

# Rescue target mapping from a primary failure.
RESCUE_TARGET_BY_FAILURE = {
    "HT_RETURNS_RATIO_FAIL": "HT_RETURNS_RATIO_NEAR_PASS",
    "SHARPE_NEAR_PASS": "SHARPE_NEAR_PASS",
    "LOW_FITNESS": "FITNESS_NEAR_PASS",
    "SUB_UNIVERSE_FAIL": "SUB_UNIVERSE_NEAR_PASS",
    "PP_CORRELATION_FAIL": "CORRELATION_NEAR_PASS",
    "SELF_CORRELATION_FAIL": "CORRELATION_NEAR_PASS",
    "THEME_MATCH_UNKNOWN_CAUSE": "THEME_NEAR_PASS",
}

# Rescue priority tiers (lower number = tried first).
RESCUE_PRIORITY = {
    "SETTINGS_MICRO_TUNE": 1,        # NEUTRALIZATION / DECAY / TRUNCATION
    "WINDOW_MICRO_TUNE": 2,          # ts_mean 2 -> 3 (SAME_FAMILY_MICRO_TUNE)
    "SIGNAL_SHAPE_MICRO_TUNE": 3,    # rank / zscore
    "VERIFIED_ADVANCED_REPAIR": 4,   # ts_target_tvr_decay / hump
    "SAME_FIELD_SIBLING": 5,         # existing sibling candidate reuse
}

# Strategy -> priority tier.
STRATEGY_PRIORITY = {
    "NEUTRALIZATION_MICRO_TUNE": "SETTINGS_MICRO_TUNE",
    "DECAY_MICRO_TUNE": "SETTINGS_MICRO_TUNE",
    "TRUNCATION_MICRO_TUNE": "SETTINGS_MICRO_TUNE",
    "SAME_FAMILY_MICRO_TUNE": "WINDOW_MICRO_TUNE",
    "RANK_SIGNAL_SHAPE": "SIGNAL_SHAPE_MICRO_TUNE",
    "ZSCORE_SIGNAL_SHAPE": "SIGNAL_SHAPE_MICRO_TUNE",
    "HT_RATIO_TARGET_TVR": "VERIFIED_ADVANCED_REPAIR",
}

# Evidence sources that are external / manual research evidence. These are
# Priority Hints and Recommendation Evidence ONLY. They must never drive a
# Production auto state-machine resolution: no STOP_RESCUE_SUCCESS, no candidate
# lifecycle PASS, no Repair-plan EXECUTED, no budget change, no Finalist fact.
EXTERNAL_EVIDENCE_SOURCES = frozenset({
    "EXTERNAL_CONFIRMED_EVIDENCE",
    "MANUAL_OBSERVATION",
    "USER_CONFIRMED_EVIDENCE",
})


def evidence_source_is_external(source: Any) -> bool:
    """True only for external/manual research evidence sources.

    Absence of a source (None / "") means a SYSTEM fact and is trusted, so an
    unlabelled TARGET_PASS resolves as a system success.
    """
    return str(source or "").strip().upper() in EXTERNAL_EVIDENCE_SOURCES


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def near_pass_config(rules: Mapping[str, Any]) -> Dict[str, Any]:
    merged = dict(DEFAULT_NEAR_PASS_CONFIG)
    merged.update(rules.get("near_pass", {}) or {})
    return merged


# ---------------------------------------------------------------------------
# Threshold direction and normalized gap
# ---------------------------------------------------------------------------

def threshold_direction(normalized_name: str) -> str:
    """Return MIN / MAX / UNRESOLVED_THRESHOLD_DIRECTION for a check name."""
    if normalized_name in MIN_THRESHOLD_NAMES:
        return "MIN"
    if normalized_name in MAX_THRESHOLD_NAMES:
        return "MAX"
    return "UNRESOLVED_THRESHOLD_DIRECTION"


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        parsed = json.loads(str(value))
        return float(parsed) if isinstance(parsed, (int, float)) else None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def normalized_gap(raw_value: Any, raw_limit: Any, direction: str) -> Optional[float]:
    """Standardized gap. PASS <= 0; FAIL/WARNING > 0. None if unresolvable."""
    value = _as_float(raw_value)
    limit = _as_float(raw_limit)
    if value is None or limit is None or not limit:
        return None
    if direction == "MIN":
        return round((limit - value) / abs(limit), 6)
    if direction == "MAX":
        return round((value - limit) / abs(limit), 6)
    return None


def gap_summary(raw_value: Any, raw_limit: Any, direction: str) -> Dict[str, Any]:
    value = _as_float(raw_value)
    limit = _as_float(raw_limit)
    ng = normalized_gap(value, limit, direction)
    raw_gap = None
    if value is not None and limit is not None and direction in {"MIN", "MAX"}:
        raw_gap = round((limit - value) if direction == "MIN" else (value - limit), 6)
    return {
        "raw_value": value, "raw_limit": limit, "raw_gap": raw_gap,
        "normalized_gap": ng, "threshold_direction": direction,
    }


# ---------------------------------------------------------------------------
# Near-Pass classification
# ---------------------------------------------------------------------------

def _ppl_hard_blockers(
    checks: Mapping[str, Mapping[str, Any]], *, check_phase: str = "PRE_TAG"
) -> List[Dict[str, Any]]:
    """Return unsatisfied PPL hard checks for the requested check phase.

    Diagnostic-only checks are intentionally excluded.  Theme-match checks are
    tag-dependent: during PRE_TAG they are deferred because the user has not
    yet performed manual PowerPoolSelected.  At POST_TAG they become hard again.
    Other unquantifiable hard checks remain blockers.
    """
    phase = str(check_phase or "PRE_TAG").upper()
    blockers: List[Dict[str, Any]] = []
    for name in sorted(PPL_HARD_CHECKS):
        c = checks.get(name)
        if c is None:
            continue
        outcome = str(c.get("eligibility_outcome") or c.get("normalized_result") or "UNKNOWN").upper()
        if outcome in {"PASS", "NOT_APPLICABLE", "DEFERRED"}:
            continue
        if phase == "PRE_TAG" and name in PRETAG_DEFERRED_PPL_HARD_CHECKS:
            continue
        raw_result = str(c.get("raw_result") or "").upper()
        direction = threshold_direction(name)
        gap = gap_summary(c.get("raw_value_json"), c.get("raw_limit_json"), direction)
        item = {
            "check": name, "outcome": outcome, "raw_result": raw_result,
            "direction": direction, "is_ppl_hard": True,
            "is_ppl_correlation": name in PPL_CORRELATION_CHECKS, **gap,
        }
        if direction == "UNRESOLVED_THRESHOLD_DIRECTION" or gap["normalized_gap"] is None:
            item["ranked"] = False
            item["excluded_reason"] = (
                "UNRESOLVED_THRESHOLD_DIRECTION"
                if direction == "UNRESOLVED_THRESHOLD_DIRECTION"
                else "NO_RESOLVABLE_GAP"
            )
        else:
            item["ranked"] = True
        blockers.append(item)
    return blockers


def _ppl_deferred_checks(
    checks: Mapping[str, Mapping[str, Any]], *, check_phase: str = "PRE_TAG"
) -> List[Dict[str, Any]]:
    """Audit-only list of PPL checks deferred until manual tagging is complete."""
    phase = str(check_phase or "PRE_TAG").upper()
    if phase != "PRE_TAG":
        return []
    out: List[Dict[str, Any]] = []
    for name in sorted(PRETAG_DEFERRED_PPL_HARD_CHECKS):
        c = checks.get(name)
        if c is None:
            continue
        outcome = str(c.get("eligibility_outcome") or c.get("normalized_result") or "UNKNOWN").upper()
        if outcome in {"PASS", "NOT_APPLICABLE", "DEFERRED"}:
            continue
        out.append({
            "check": name,
            "outcome": outcome,
            "raw_result": str(c.get("raw_result") or "").upper(),
            "deferred": True,
            "deferred_reason": "PENDING_MANUAL_POWER_POOL_TAG",
            "required_phase": "POST_TAG",
        })
    return out


def _quality_diagnostics(checks: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, c in sorted(checks.items()):
        if name in PPL_HARD_CHECKS:
            continue
        outcome = str(c.get("eligibility_outcome") or c.get("normalized_result") or "UNKNOWN").upper()
        if outcome in {"PASS", "NOT_APPLICABLE", "DEFERRED"}:
            continue
        direction = threshold_direction(name)
        gap = gap_summary(c.get("raw_value_json"), c.get("raw_limit_json"), direction)
        out.append({
            "check": name, "outcome": outcome, "raw_result": str(c.get("raw_result") or "").upper(),
            "direction": direction, "is_ppl_hard": False, "diagnostic_only": True, **gap,
        })
    return out


def classify_near_pass(
    candidate: Mapping[str, Any], metrics: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]], rules: Mapping[str, Any],
    *, local_gate_status: Optional[str] = None, check_phase: str = "PRE_TAG",
) -> Dict[str, Any]:
    """Classify a candidate as STRONG_NEAR_PASS / NEAR_PASS / NORMAL.

    Prerequisites: Simulation COMPLETE, Local Gate PASS, a RESOLVED PRE-TAG
    check, and no structural INVALID. Blockers are counted only from current
    PPL hard checks. Generic diagnostics (for example PROD_CORRELATION or 2Y
    Sharpe) are recorded separately and never demote PPL Near-Pass.
    """
    cfg = near_pass_config(rules)
    all_blockers = _ppl_hard_blockers(checks, check_phase=check_phase)
    deferred_ppl_checks = _ppl_deferred_checks(checks, check_phase=check_phase)
    blockers = [b for b in all_blockers if b.get("ranked")]
    unquantified_hard_blockers = [b for b in all_blockers if not b.get("ranked")]
    quality_diagnostics = _quality_diagnostics(checks)
    blocker_count = len(blockers) + len(unquantified_hard_blockers)
    max_blocker_gap = max((b["normalized_gap"] for b in blockers if b["normalized_gap"] is not None), default=None)

    sim_complete = str(candidate.get("simulation_status") or "").upper() == "COMPLETE"
    structure_ok = str(candidate.get("structure_status") or "ELIGIBLE") != "INVALID"
    gate_pass = local_gate_status == "PASS"
    has_resolved_check = bool(checks)

    sharpe = _as_float(metrics.get("sharpe"))
    fitness = _as_float(metrics.get("fitness"))
    turnover = _as_float(metrics.get("turnover"))
    base = rules["power_pool_base_presets"]
    sharpe_pass = sharpe is not None and sharpe >= float(base["sharpe"]["preset_min"])
    fitness_check = checks.get("FITNESS") or checks.get("LOW_FITNESS")
    if fitness_check:
        fitness_pass = str(
            fitness_check.get("eligibility_outcome") or fitness_check.get("normalized_result") or ""
        ).upper() == "PASS"
    else:
        fitness_pass = fitness is not None and fitness >= float(cfg["fitness_fallback_min"])
    turnover_pass = (
        turnover is not None
        and float(base["turnover"]["preset_min"]) <= turnover <= float(base["turnover"]["preset_max"])
    )
    theme_turnover_pass = (
        turnover is not None
        and turnover >= float(rules["current_theme"]["local_preconditions"]["high_turnover"]["turnover"]["preset_min"])
    )

    classification = "NORMAL"
    reasons: List[str] = []
    if not sim_complete:
        reasons.append("SIMULATION_NOT_COMPLETE")
    elif not gate_pass:
        reasons.append("LOCAL_GATE_NOT_PASS")
    elif not has_resolved_check:
        reasons.append("NO_RESOLVED_PRE_TAG_CHECK")
    elif not structure_ok:
        reasons.append("STRUCTURAL_INVALID")
    elif blocker_count == 0:
        reasons.append("NO_BLOCKER")  # already passing every observed PPL hard check
    elif unquantified_hard_blockers:
        reasons.append("UNQUANTIFIED_PPL_HARD_BLOCKER")
    elif max_blocker_gap is None:
        reasons.append("NO_RESOLVABLE_GAP")
    else:
        strong_max = float(cfg["strong_near_pass_max_blockers"])
        near_max = float(cfg["near_pass_max_blockers"])
        strong_gap = float(cfg["strong_near_pass_normalized_gap_max"])
        near_gap = float(cfg["near_pass_normalized_gap_max"])
        if (
            blocker_count <= strong_max
            and max_blocker_gap <= strong_gap
            and sharpe_pass and turnover_pass and theme_turnover_pass
        ):
            classification = "STRONG_NEAR_PASS"
        elif blocker_count <= near_max and max_blocker_gap <= near_gap:
            classification = "NEAR_PASS"
        else:
            reasons.append("BLOCKER_COUNT_OR_GAP_EXCEEDS_NEAR_PASS")

    return {
        "classification": classification,
        "blocker_count": blocker_count,
        "blockers": blockers,
        "all_blockers": all_blockers,
        "unquantified_hard_blockers": unquantified_hard_blockers,
        "quality_diagnostics": quality_diagnostics,
        "deferred_ppl_checks": deferred_ppl_checks,
        "check_phase": str(check_phase or "PRE_TAG").upper(),
        "max_blocker_normalized_gap": max_blocker_gap,
        "reasons": reasons,
        "sharpe_pass": sharpe_pass,
        "fitness_pass": fitness_pass,
        "turnover_pass": turnover_pass,
        "theme_turnover_pass": theme_turnover_pass,
    }


def rescue_target(failure: str) -> str:
    return RESCUE_TARGET_BY_FAILURE.get(failure, "MULTI_GATE_NEAR_PASS")


def rescue_target_from_check(check_name: str) -> str:
    """Map a blocker check name to its rescue target (for audit logging only)."""
    c = (check_name or "").upper()
    if c in {"HIGH_TURNOVER_RETURNS_RATIO", "HT_HIGH_TURNOVER_RETURNS_RATIO"}:
        return "HT_RETURNS_RATIO_NEAR_PASS"
    if c in {"SHARPE", "LOW_SHARPE"}:
        return "SHARPE_NEAR_PASS"
    if c in {"FITNESS", "LOW_FITNESS"}:
        return "FITNESS_NEAR_PASS"
    if c in {"SUB_UNIVERSE", "LOW_SUB_UNIVERSE_SHARPE"}:
        return "SUB_UNIVERSE_NEAR_PASS"
    if c == "POWER_POOL_CORRELATION":
        return "CORRELATION_NEAR_PASS"
    if c in {"THEME_MATCH", "MATCHES_THEMES", "MATCHES_CLASSIFICATION", "PURE_POWER_POOL_THEME"}:
        return "THEME_NEAR_PASS"
    return "MULTI_GATE_NEAR_PASS"


# ---------------------------------------------------------------------------
# Repair Evidence Memory
# ---------------------------------------------------------------------------

def _evidence_from_repairs(store: Any, run_id: str, candidate_ids: Iterable[str]) -> List[Dict[str, Any]]:
    """Aggregate historical repair evidence from durable repair-plan + check facts."""
    ids = list(candidate_ids)
    if not ids:
        return []
    out = []
    with store.connect() as conn:
        marks = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""SELECT repair_plan_id,parent_candidate_id,target_failure,repair_type,repair_signature,
                       plan_status,consumed_posts,candidate_spec_json
                FROM ppl_repair_plans WHERE run_id=? AND parent_candidate_id IN ({marks})""",
            (run_id, *ids),
        ).fetchall()
    for r in rows:
        spec = json.loads(r["candidate_spec_json"]) if r["candidate_spec_json"] else {}
        child_verdict = None
        # Determine the child candidate_id from reuse/child linkage is not stored
        # here; verdict is computed by the caller via evaluate_ht_repair_outcome.
        out.append({
            "repair_plan_id": r["repair_plan_id"],
            "parent_candidate_id": r["parent_candidate_id"],
            "target_failure": r["target_failure"],
            "strategy": r["repair_type"],
            "repair_signature": r["repair_signature"],
            "plan_status": r["plan_status"],
            "consumed_posts": r["consumed_posts"],
            "parameter_change": _parameter_change(spec),
            "child_verdict": child_verdict,
        })
    return out


def _parameter_change(spec: Mapping[str, Any]) -> Dict[str, Any]:
    change: Dict[str, Any] = {}
    if spec.get("window_override") is not None:
        change["window"] = spec["window_override"]
    if spec.get("transform_family_override"):
        change["transform"] = spec["transform_family_override"]
    so = spec.get("settings_override") or {}
    for k in ("neutralization", "decay", "truncation"):
        if k in so:
            change[k] = so[k]
    if spec.get("expression_preview"):
        change["expression"] = spec["expression_preview"]
    return change


# ---------------------------------------------------------------------------
# Neutralization micro-tune candidate pool
# ---------------------------------------------------------------------------

def neutralization_candidates(config: Any) -> List[str]:
    """Ordered neutralization candidates for the current region (from config)."""
    rules = config.rules
    cfg = near_pass_config(rules)
    region = str(config.plan["simulation_settings"].get("region") or "GLB").upper()
    pool = cfg.get("neutralization_candidates_by_region", {}).get(region, [])
    # Filter to those the current theme/settings actually allow (at minimum keep
    # the current parent neutralization out of the change set).
    return list(dict.fromkeys(pool))


# ---------------------------------------------------------------------------
# Rescue planning (deferred proposals only)
# ---------------------------------------------------------------------------

def plan_neutralization_micro_tune(
    parent: Mapping[str, Any], failure: str, config: Any, *,
    current_neutralization: str, existing_changes: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    """Propose neutralization changes (expression unchanged, settings changed).

    Each proposal changes only settings.neutralization; the expression stays
    identical to the parent. A new sim_key is produced downstream because
    sim_key = canonical {expression, settings}.
    """
    proposals: List[Dict[str, Any]] = []
    tried = set(existing_changes)
    for target in neutralization_candidates(config):
        if target.upper() == current_neutralization.upper():
            continue
        if target.upper() in tried:
            continue
        proposals.append({
            "strategy": "NEUTRALIZATION_MICRO_TUNE",
            "priority": "SETTINGS_MICRO_TUNE",
            "priority_rank": RESCUE_PRIORITY["SETTINGS_MICRO_TUNE"],
            "repair_type": "NEUTRALIZATION_MICRO_TUNE",
            "change": {"neutralization": target},
            "settings_override": {"neutralization": target},
            "expression_unchanged": True,
            "target_failure": failure,
            "rescue_target": rescue_target(failure),
        })
    return proposals


# ---------------------------------------------------------------------------
# Manual review escalation
# ---------------------------------------------------------------------------

def manual_review_classify(
    classification: str, rescue_attempts: int, max_attempts: int, blockers: List[Dict[str, Any]],
    metrics: Mapping[str, Any], *, explicit_stop_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Escalate only after automatic rescue is actually exhausted/stopped.

    Before exhaustion there is deliberately no manual-review priority: the user
    asked to be alerted only after the system has tried its bounded rescue path.
    """
    sharpe = _as_float(metrics.get("sharpe"))
    max_gap = max((b["normalized_gap"] for b in blockers if b["normalized_gap"] is not None), default=None)
    n = len(blockers)
    exhausted = rescue_attempts >= max_attempts
    explicitly_stopped = explicit_stop_reason in {"STOP_AUTOMATIC_RESCUE", "STOP_FOR_SIDE_EFFECT"}
    if not exhausted and not explicitly_stopped:
        return {
            "manual_priority": None, "manual_review_reason": "AUTO_RESCUE_STILL_AVAILABLE",
            "blocker_count": n, "max_normalized_gap": max_gap,
        }
    if classification == "STRONG_NEAR_PASS":
        priority = "P1_MANUAL"
        reason = "STRONG_NEAR_PASS_RESCUE_EXHAUSTED"
    elif classification == "NEAR_PASS":
        priority = "P2_MANUAL"
        reason = "NEAR_PASS_RESCUE_EXHAUSTED"
    else:
        priority = "P3_ARCHIVE"
        reason = "LOW_VALUE_OR_MULTI_GATE_DEGRADED" if (sharpe is None or sharpe < 1.0) else "NORMAL_RESCUE_EXHAUSTED"
    return {
        "manual_priority": priority, "manual_review_reason": reason,
        "blocker_count": n, "max_normalized_gap": max_gap,
    }


def evaluate_rescue_stop(
    strategy_verdicts: List[Any], attempts: int, max_attempts: int,
) -> Optional[str]:
    """Determine auto-stop from SYSTEM-confirmed evidence only."""
    system_entries: List[Tuple[Optional[str], Optional[str]]] = []
    for entry in strategy_verdicts:
        if isinstance(entry, Mapping):
            strategy = entry.get("strategy")
            verdict = entry.get("verdict")
            source = entry.get("source")
        else:
            strategy = entry[0] if len(entry) > 0 else None
            verdict = entry[1] if len(entry) > 1 else None
            source = entry[2] if len(entry) > 2 else None
        if evidence_source_is_external(source):
            continue
        system_entries.append((strategy, verdict))

    if any(verdict == "TARGET_PASS" for _strategy, verdict in system_entries):
        return "STOP_RESCUE_SUCCESS"
    if attempts >= max_attempts:
        return "STOP_AUTOMATIC_RESCUE"
    if len(system_entries) >= 2:
        s1, v1 = system_entries[-2]
        s2, v2 = system_entries[-1]
        if v1 == "WORSE" and v2 == "WORSE" and s1 == s2:
            return "STOP_THIS_REPAIR_STRATEGY"
    if system_entries and system_entries[-1][1] == "WORSE":
        return "STOP_THIS_PARAMETER"
    return None


# ---------------------------------------------------------------------------
# Orchestration (read-only over durable facts)
# ---------------------------------------------------------------------------

def _resolved_check_map_by_candidate(store: Any, run_id: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    from .check_derived_repair import _check_map, _load_resolved_check_facts
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for fact in _load_resolved_check_facts(store, run_id):
        cid = fact["session"].get("candidate_id")
        if cid:
            out[cid] = _check_map(fact["results"])
    return out


def _resolved_check_rows_by_candidate(store: Any, run_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Latest resolved PRE_TAG rows, preserving raw platform check names.

    The legacy normalized map collapses unknown checks to UNKNOWN. V3.0.4b keeps
    the raw rows so dynamic HT_* theme signals remain distinguishable.
    """
    from .check_derived_repair import _load_resolved_check_facts
    out: Dict[str, List[Dict[str, Any]]] = {}
    for fact in _load_resolved_check_facts(store, run_id):
        cid = fact["session"].get("candidate_id")
        if cid:
            out[str(cid)] = [dict(r) for r in fact["results"]]
    return out


def _alpha_metrics_map(alpha_db: Path, sim_keys: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    from .check_derived_repair import _alpha_metrics
    return _alpha_metrics(alpha_db, list(dict.fromkeys(str(k) for k in sim_keys if k)))


def _local_gate_by_candidate(
    candidates: Mapping[str, Mapping[str, Any]], metrics_by_key: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Any],
) -> Dict[str, str]:
    """Local (simulation-level) gate via evaluate_local_pre_gate, NOT diagnosis.

    A check-derived diagnosis like HT_RETURNS_RATIO_FAIL is a PRE-TAG finding and
    must not be confused with the simulation-level local gate.
    """
    from .diagnosis import evaluate_local_pre_gate
    out: Dict[str, str] = {}
    for cid, cand in candidates.items():
        metrics = metrics_by_key.get(cand.get("sim_key")) or {}
        out[cid] = evaluate_local_pre_gate(cand, metrics, rules)["status"]
    return out


def load_external_evidence(path: Path) -> List[Dict[str, Any]]:
    """Load manually-recorded research evidence (source explicitly recorded)."""
    if not path or not Path(path).exists():
        return []
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return []
    return data.get("evidence", []) if isinstance(data, dict) else []




def _candidate_settings(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    raw = candidate.get("settings_json")
    if isinstance(raw, Mapping):
        return dict(raw)
    if raw:
        try:
            parsed = json.loads(str(raw))
            return dict(parsed) if isinstance(parsed, Mapping) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    return {}


def _matching_external_evidence(
    evidence: Iterable[Mapping[str, Any]], candidate: Mapping[str, Any], target_failure: str,
) -> List[Dict[str, Any]]:
    """Keep external research evidence scoped to this signal/failure context."""
    out: List[Dict[str, Any]] = []
    expr = str(candidate.get("expression") or "").strip()
    family = str(candidate.get("signal_family") or "").strip()
    for item in evidence:
        if item.get("target_failure") and str(item.get("target_failure")) != str(target_failure):
            continue
        if item.get("parent_expression") and str(item.get("parent_expression")).strip() != expr:
            continue
        if item.get("parent_signal_family") and family and str(item.get("parent_signal_family")).strip() != family:
            continue
        out.append(dict(item))
    return out

def build_rescue_context(store: Any, config: Any, alpha_db: Path, run_id: str) -> Dict[str, Any]:
    """Aggregate all durable facts needed for near-pass classification + rescue planning."""
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    checks_by_cid = _resolved_check_map_by_candidate(store, run_id)
    check_rows_by_cid = _resolved_check_rows_by_candidate(store, run_id)
    metrics_by_key = _alpha_metrics_map(alpha_db, [c.get("sim_key") for c in candidates.values()])
    gate_by_cid = _local_gate_by_candidate(candidates, metrics_by_key, config.rules)
    with store.connect() as conn:
        repair_rows = conn.execute(
            "SELECT repair_plan_id,parent_candidate_id,target_failure,repair_type,repair_signature,"
            "plan_status,consumed_posts,candidate_spec_json FROM ppl_repair_plans WHERE run_id=? ORDER BY created_at,repair_plan_id",
            (run_id,),
        ).fetchall()
        edge_rows = conn.execute(
            "SELECT parent_candidate_id,child_candidate_id,repair_type,repair_signature,"
            "before_json,after_json,delta_json,side_effect_verdict FROM ppl_repairs WHERE run_id=?",
            (run_id,),
        ).fetchall()
    edge_by_signature = {r["repair_signature"]: dict(r) for r in edge_rows}
    repairs_by_parent: Dict[str, List[Dict[str, Any]]] = {}
    repairs_by_root: Dict[str, List[Dict[str, Any]]] = {}
    for r in repair_rows:
        spec = json.loads(r["candidate_spec_json"]) if r["candidate_spec_json"] else {}
        edge = edge_by_signature.get(r["repair_signature"], {})
        entry = {
            "repair_plan_id": r["repair_plan_id"], "target_failure": r["target_failure"],
            "strategy": r["repair_type"], "repair_signature": r["repair_signature"],
            "plan_status": r["plan_status"], "consumed_posts": r["consumed_posts"],
            "parameter_change": _parameter_change(spec),
            "child_candidate_id": edge.get("child_candidate_id"),
            "system_verdict": edge.get("side_effect_verdict"),
            "before_json": edge.get("before_json"), "after_json": edge.get("after_json"),
            "delta_json": edge.get("delta_json"),
        }
        repairs_by_parent.setdefault(r["parent_candidate_id"], []).append(entry)
        parent = candidates.get(str(r["parent_candidate_id"])) or {}
        root = str(parent.get("root_candidate_id") or r["parent_candidate_id"] or "")
        repairs_by_root.setdefault(root, []).append(entry)
    return {
        "candidates": candidates, "checks_by_cid": checks_by_cid,
        "check_rows_by_cid": check_rows_by_cid,
        "metrics_by_key": metrics_by_key, "gate_by_cid": gate_by_cid,
        "repairs_by_parent": repairs_by_parent, "repairs_by_root": repairs_by_root,
    }


def _executed_attempts(repairs: Iterable[Mapping[str, Any]]) -> int:
    """Count evaluated repair strategies, not raw POST resource consumption.

    ``UNCERTAIN_SUBMISSION`` can consume remote/budget resources without
    producing an Alpha result that can evaluate the strategy.  Such a plan is
    deliberately kept READY with ``UNCERTAIN_SUBMISSION_HOLD`` and must not
    burn one of the bounded rescue-strategy attempts.  A strategy attempt is
    counted only after the canonical plan reaches EXECUTED.
    """
    return sum(1 for r in repairs if str(r.get("plan_status") or "").upper() == "EXECUTED")


def _latest_pretag_session_status_by_candidate(store: Any, run_id: str) -> Dict[str, Dict[str, Any]]:
    """Latest PRE_TAG session, including unresolved refresh attempts.

    Resolved-fact loaders intentionally ignore unresolved sessions, but a newer
    failed/PENDING refresh must invalidate an older READY queue snapshot rather
    than silently falling back to stale resolved evidence.
    """
    out: Dict[str, Dict[str, Any]] = {}
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT candidate_id,check_session_id,session_status,updated_at,created_at
               FROM ppl_check_sessions
               WHERE run_id=? AND phase='PRE_TAG' AND candidate_id IS NOT NULL
               ORDER BY updated_at,check_session_id""",
            (run_id,),
        ).fetchall()
    for r in rows:
        out[str(r[0])] = {
            "candidate_id": r[0], "check_session_id": r[1], "session_status": r[2],
            "updated_at": r[3], "created_at": r[4],
        }
    return out


def classify_run(store: Any, config: Any, alpha_db: Path, run_id: str, *,
                 emit_audit: bool = True) -> List[Dict[str, Any]]:
    """V3.0.4g platform-driven PPL classification over durable facts.

    Returns every COMPLETE candidate that is meaningfully classifiable. Repair
    queues filter by ``repair_priority``; diagnostics never become blockers.
    """
    ctx = build_rescue_context(store, config, alpha_db, run_id)
    run_profile = str((getattr(config, "plan", {}) or {}).get("run_profile") or "").upper()
    if run_profile == "CONTINUOUS_RESEARCH":
        qualification_snapshot = load_qualification_policy_snapshot(config.project_dir)
        policy = dict(qualification_snapshot.classification_policy)
        qualification_integration = dict(qualification_snapshot.integration)
    else:
        qualification_snapshot = None
        policy = load_ppl_classification_policy_for_config(config)
        qualification_integration = {}
    qualification_enabled = bool(qualification_integration.get("enabled", False))
    latest_sessions = _latest_pretag_session_status_by_candidate(store, run_id)
    out: List[Dict[str, Any]] = []
    for cid, cand in ctx["candidates"].items():
        metrics = ctx["metrics_by_key"].get(cand.get("sim_key")) or {}
        check_rows = ctx["check_rows_by_cid"].get(cid, [])
        cls = classify_ppl_candidate(cand, metrics, check_rows, policy)
        latest_session = latest_sessions.get(str(cid)) or {}
        latest_status = str(latest_session.get("session_status") or "").upper()
        if latest_status and latest_status != "RESOLVED" and cls.get("classification") in {
            "PPL_TECHNICALLY_READY", "PPL_READY_FOR_MANUAL_FINALIZATION",
            "PPL_THEME_UNRESOLVED", "PPL_STRATEGY_REJECT_HIGH_PPC",
            "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE",
        }:
            cls = dict(cls)
            cls["classification"] = "PPL_CHECK_UNRESOLVED"
            cls["ppl_status"] = "PPL_CHECK_UNRESOLVED"
            cls["evidence_label"] = "PPL_CHECK_UNRESOLVED"
            cls["manual_finalization_required"] = False
            cls["reasons"] = ["LATEST_PRETAG_CHECK_NOT_RESOLVED"]
            cls["latest_check_session_status"] = latest_status
            cls["latest_check_session_id"] = latest_session.get("check_session_id")
        if qualification_enabled:
            bundle = evaluate_ppl_qualification_compatibility(
                cand, metrics, check_rows, policy, qualification_integration, cls,
            )
            q = bundle.result
            cls = dict(cls)
            cls.update({
                "qualification_policy_version": q.policy_version,
                "qualification_policy_hash": bundle.policy_hash,
                "qualification_evaluator_version": bundle.evaluator_version,
                "qualification_qualified": q.qualified,
                "qualification_blockers": list(q.blockers),
                "qualification_unresolved": list(q.unresolved),
                "qualification_diagnostics": list(q.diagnostics),
                "qualification_repairable_failure_codes": list(q.repairable_failure_codes),
                "qualification_platform_facts": dict(q.platform_facts),
                "qualification_local_strategy_results": dict(q.local_strategy_results),
            })
        if cls["classification"] == "UNCLASSIFIED":
            continue
        repair_attempts = _executed_attempts(ctx["repairs_by_parent"].get(cid, []))
        row = {
            "candidate_id": cid, "alpha_id": cand.get("alpha_id"),
            **cls,
            "fitness": _as_float(metrics.get("fitness")),
            "returns": _as_float(metrics.get("returns")),
            "repair_attempts": repair_attempts,
        }
        out.append(row)
        if emit_audit and cls.get("repair_priority") in {"HIGH", "MEDIUM"} and cls.get("repair_blockers"):
            primary = cls["repair_blockers"][0]
            audit_event(
                action="PPL_REPAIR_CLASSIFIED", run_id=run_id, candidate_id=cid,
                alpha_id=cand.get("alpha_id"), near_pass_class=cls.get("evidence_label"),
                rescue_target=rescue_target_from_check(primary.get("check")),
                blocker_count=cls.get("blocker_count"), primary_blocker=primary.get("check"),
                raw_value=primary.get("raw_value"), live_limit=primary.get("raw_limit"),
                normalized_gap=primary.get("normalized_gap"),
            )
    status_rank = {
        "PPL_TECHNICALLY_READY": 0,
        "PPL_READY_FOR_MANUAL_FINALIZATION": 1,
        "PPL_FIXED_REPAIRABLE": 2,
        "PPL_THEME_REPAIRABLE": 2,
        "PPL_FIXED_AND_THEME_REPAIRABLE": 2,
        "PPL_THEME_UNRESOLVED": 3,
        "PPL_FIXED_UNRESOLVED": 4,
        "PPL_CHECK_UNRESOLVED": 5,
        "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE": 6,
        "PPL_STRATEGY_REJECT_HIGH_PPC": 6,
        "PPL_TERMINAL_FAIL": 7,
    }
    priority_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "NONE": 3}
    out.sort(key=lambda x: (
        status_rank.get(str(x.get("classification")), 9),
        priority_rank.get(str(x.get("repair_priority")), 9),
        x.get("max_normalized_gap") if x.get("max_normalized_gap") is not None else 999.0,
        -(x.get("sharpe") or 0), str(x.get("candidate_id")),
    ))
    return out


def list_near_pass(store: Any, config: Any, alpha_db: Path, run_id: str) -> Dict[str, Any]:
    rows = classify_run(store, config, alpha_db, run_id)
    strong = [r for r in rows if r.get("evidence_label") == "STRONG_NEAR_PASS"]
    near = [r for r in rows if r.get("evidence_label") == "NEAR_PASS"]
    return {
        "mode": "LIST_NEAR_PASS", "run_id": run_id,
        "strong_near_pass": strong, "near_pass": near,
        "counts": {"STRONG_NEAR_PASS": len(strong), "NEAR_PASS": len(near)},
        "network_requests": 0, "simulation_posts": 0, "check_requests": 0, "writes": 0,
    }


def list_manual_review(store: Any, config: Any, alpha_db: Path, run_id: str) -> Dict[str, Any]:
    cfg = near_pass_config(config.rules)
    ctx = build_rescue_context(store, config, alpha_db, run_id)
    rows: List[Dict[str, Any]] = []
    for cid, cand in ctx["candidates"].items():
        checks = ctx["checks_by_cid"].get(cid, {})
        metrics = ctx["metrics_by_key"].get(cand.get("sim_key")) or {}
        gate = ctx["gate_by_cid"].get(cid, "FAIL")
        cls = classify_near_pass(cand, metrics, checks, config.rules, local_gate_status=gate)
        attempts = _executed_attempts(ctx["repairs_by_parent"].get(cid, []))
        if cls["classification"] == "STRONG_NEAR_PASS":
            max_attempts = int(cfg["strong_near_pass_rescue_max_attempts"])
        elif cls["classification"] == "NEAR_PASS":
            max_attempts = int(cfg["near_pass_rescue_max_attempts"])
        else:
            max_attempts = int(cfg["normal_repair_max_attempts"])
        repairs = ctx["repairs_by_parent"].get(cid, [])
        executed_repairs = [
            r for r in repairs
            if int(r.get("consumed_posts") or 0) > 0 or str(r.get("plan_status")) == "EXECUTED"
        ]
        system_strategy_verdicts = [
            {"strategy": r.get("strategy"), "verdict": r.get("system_verdict"), "source": "DB_CONFIRMED"}
            for r in executed_repairs if r.get("system_verdict")
        ]
        stop_reason = evaluate_rescue_stop(system_strategy_verdicts, attempts, max_attempts)
        # A system-confirmed success is terminal success, never a manual-review
        # escalation even if the numerical attempt allowance has been exhausted.
        if stop_reason == "STOP_RESCUE_SUCCESS":
            continue
        mr = manual_review_classify(
            cls["classification"], attempts, max_attempts, cls["blockers"], metrics,
            explicit_stop_reason=stop_reason,
        )
        if mr["manual_priority"] in {"P1_MANUAL", "P2_MANUAL", "P3_ARCHIVE"}:
            rows.append({
                "candidate_id": cid, "alpha_id": cand.get("alpha_id"),
                "classification": cls["classification"], **mr,
                "sharpe": _as_float(metrics.get("sharpe")),
                "fitness": _as_float(metrics.get("fitness")),
                "turnover": _as_float(metrics.get("turnover")),
                "repair_attempt_count": attempts,
                "repairs_attempted": [r["strategy"] for r in executed_repairs],
                "auto_stop_reason": stop_reason,
            })
            blockers = cls["blockers"]
            primary = next((b for b in blockers if b.get("normalized_gap") is not None), None) or (blockers[0] if blockers else None)
            audit_event(
                action="MANUAL_REVIEW_ESCALATED", run_id=run_id,
                candidate_id=cid, alpha_id=cand.get("alpha_id"),
                manual_priority=mr["manual_priority"],
                target_failed_check=(primary or {}).get("check"),
                best_value=(primary or {}).get("raw_value"),
                live_limit=(primary or {}).get("raw_limit"),
                normalized_gap=(primary or {}).get("normalized_gap"),
                repair_attempt_count=attempts,
                auto_stop_reason=mr["manual_review_reason"],
            )
    order = {"P1_MANUAL": 0, "P2_MANUAL": 1, "P3_ARCHIVE": 2}
    rows.sort(key=lambda x: (order.get(x["manual_priority"], 9), x["blocker_count"],
                             (x["max_normalized_gap"] if x["max_normalized_gap"] is not None else 9),
                             -(x["sharpe"] or 0), x["candidate_id"]))
    return {
        "mode": "LIST_MANUAL_REVIEW", "run_id": run_id,
        "P1_MANUAL": [r for r in rows if r["manual_priority"] == "P1_MANUAL"],
        "P2_MANUAL": [r for r in rows if r["manual_priority"] == "P2_MANUAL"],
        "P3_ARCHIVE": [r for r in rows if r["manual_priority"] == "P3_ARCHIVE"],
        "counts": {
            "P1_MANUAL": sum(1 for r in rows if r["manual_priority"] == "P1_MANUAL"),
            "P2_MANUAL": sum(1 for r in rows if r["manual_priority"] == "P2_MANUAL"),
            "P3_ARCHIVE": sum(1 for r in rows if r["manual_priority"] == "P3_ARCHIVE"),
        },
        "network_requests": 0, "simulation_posts": 0, "check_requests": 0, "writes": 0,
    }


def _failure_from_blocker(check_name: Optional[str]) -> str:
    c = str(check_name or "").upper()
    if c in {"HIGH_TURNOVER_RETURNS_RATIO", "HT_HIGH_TURNOVER_RETURNS_RATIO"}:
        return "HT_RETURNS_RATIO_FAIL"
    if c in {"SHARPE", "LOW_SHARPE"}:
        return "SHARPE_NEAR_PASS"
    if c in {"FITNESS", "LOW_FITNESS"}:
        return "LOW_FITNESS"
    if c in {"SUB_UNIVERSE", "LOW_SUB_UNIVERSE_SHARPE"}:
        return "SUB_UNIVERSE_FAIL"
    if c == "POWER_POOL_CORRELATION":
        return "PP_CORRELATION_FAIL"
    if c == "SELF_CORRELATION":
        return "SELF_CORRELATION_FAIL"
    if c in {"THEME_MATCH", "MATCHES_THEMES", "MATCHES_CLASSIFICATION", "PURE_POWER_POOL_THEME"}:
        return "THEME_MATCH_UNKNOWN_CAUSE"
    return "UNKNOWN_CHECK"


def preview_rescue(store: Any, config: Any, alpha_db: Path, run_id: str, candidate_id: str,
                   external_evidence: Optional[List[Dict[str, Any]]] = None, *,
                   emit_audit: bool = True) -> Dict[str, Any]:
    """Preview a single candidate's rescue classification + recommendation."""
    ctx = build_rescue_context(store, config, alpha_db, run_id)
    cand = ctx["candidates"].get(candidate_id)
    if cand is None:
        from .config import ConfigError
        raise ConfigError(f"RESCUE_CANDIDATE_NOT_FOUND: {candidate_id}")
    cfg = near_pass_config(config.rules)
    metrics = ctx["metrics_by_key"].get(cand.get("sim_key")) or {}
    cls = classify_ppl_candidate(
        cand, metrics, ctx["check_rows_by_cid"].get(candidate_id, []),
        load_ppl_classification_policy_for_config(config),
    )
    repairs = ctx["repairs_by_parent"].get(candidate_id, [])

    primary = next((b for b in cls.get("repair_blockers", []) if b.get("normalized_gap") is not None), None)
    if primary is None and cls.get("repair_blockers"):
        primary = cls["repair_blockers"][0]
    current_failure = (primary or {}).get("failure") or _failure_from_blocker((primary or {}).get("check"))
    current_rescue_target = rescue_target_from_check((primary or {}).get("check"))
    ht_ratio_auto_repair_disabled = str(current_failure or "").upper() == "HT_RETURNS_RATIO_FAIL"

    ppc_state = None
    ppc_anchor = None
    if str(current_failure or "").upper() == PPC_TARGET_FAILURE:
        ppc_anchor = infer_ppc_branch_anchor(store, run_id, candidate_id)
        if ppc_anchor:
            ppc_state = ppc_branch_state(store, config, run_id, ppc_anchor)
            repairs = []
            for row in ppc_state.get("branch_rows", []):
                try:
                    spec = json.loads(row.get("candidate_spec_json") or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    spec = {}
                repairs.append({
                    "repair_plan_id": row.get("repair_plan_id"),
                    "target_failure": PPC_TARGET_FAILURE,
                    "strategy": row.get("repair_type"),
                    "repair_signature": row.get("repair_signature"),
                    "plan_status": row.get("plan_status"),
                    "consumed_posts": row.get("consumed_posts"),
                    "parameter_change": _parameter_change(spec),
                    "child_candidate_id": row.get("child_candidate_id"),
                    "system_verdict": row.get("side_effect_verdict"),
                    "before_json": row.get("before_json"),
                    "after_json": row.get("after_json"),
                    "delta_json": row.get("delta_json"),
                })
            max_attempts = int(ppc_state["max_attempts"])
            attempts = int(ppc_state["attempts_used"])
        else:
            max_attempts = int(ppc_branch_config(config.rules)["max_attempts"])
            attempts = 0
    elif cls.get("repair_priority") == "HIGH":
        max_attempts = int(cfg["strong_near_pass_rescue_max_attempts"])
        attempts = _executed_attempts(repairs)
    elif cls.get("repair_priority") == "MEDIUM":
        max_attempts = int(cfg["near_pass_rescue_max_attempts"])
        attempts = _executed_attempts(repairs)
    else:
        max_attempts = int(cfg["normal_repair_max_attempts"])
        attempts = _executed_attempts(repairs)

    if str(current_failure or "").upper() == PPC_TARGET_FAILURE:
        executed_repairs = [
            r for r in repairs
            if str(r.get("system_verdict") or "").upper() in PPC_EVALUATED_OUTCOMES
        ]
    else:
        executed_repairs = [
            r for r in repairs
            if int(r.get("consumed_posts") or 0) > 0 or str(r.get("plan_status")) == "EXECUTED"
        ]
    tried_strategies = {r["strategy"] for r in executed_repairs}
    tried_neut = {
        str(r["parameter_change"].get("neutralization")).upper()
        for r in executed_repairs if r["parameter_change"].get("neutralization")
    }

    recommendation = None
    if not ht_ratio_auto_repair_disabled and "NEUTRALIZATION_MICRO_TUNE" not in tried_strategies:
        neuts = neutralization_candidates(config)
        parent_settings = _candidate_settings(cand)
        parent_neut = str(parent_settings.get("neutralization") or cand.get("neutralization") or
                          config.plan["simulation_settings"].get("neutralization") or "").upper()
        for n in neuts:
            if n.upper() != parent_neut and n.upper() not in tried_neut:
                recommendation = {
                    "strategy": "NEUTRALIZATION_MICRO_TUNE",
                    "change": {"neutralization": n},
                    "priority_rank": RESCUE_PRIORITY["SETTINGS_MICRO_TUNE"],
                }
                break
    allow_more_pp_window_trials = str(current_failure or "").upper() == PPC_TARGET_FAILURE
    if (not ht_ratio_auto_repair_disabled and recommendation is None
            and ("SAME_FAMILY_MICRO_TUNE" not in tried_strategies or allow_more_pp_window_trials)):
        recommendation = {
            "strategy": "SAME_FAMILY_MICRO_TUNE",
            "change": {"window": "next"},
            "priority_rank": RESCUE_PRIORITY["WINDOW_MICRO_TUNE"],
        }

    matched_external = _matching_external_evidence(external_evidence or [], cand, current_failure)
    evidence = []
    for r in repairs:
        evidence.append({
            "strategy": r["strategy"], "parameter_change": r["parameter_change"],
            "plan_status": r["plan_status"], "system_verdict": r.get("system_verdict"),
            "child_candidate_id": r.get("child_candidate_id"),
        })
    for e in matched_external:
        evidence.append({
            "strategy": e.get("strategy"), "parameter_change": e.get("parameter_change"),
            "source": e.get("source", "EXTERNAL"), "verdict": e.get("verdict"),
        })

    external_success = next(
        (e for e in matched_external if str(e.get("verdict") or "").upper() == "TARGET_PASS"), None,
    )
    if recommendation is not None and external_success is not None:
        recommendation = dict(recommendation)
        recommendation["priority_hint"] = {
            "source": external_success.get("source"), "outcome": "TARGET_PASS",
            "strategy": external_success.get("strategy"),
            "change": external_success.get("parameter_change"),
        }

    system_strategy_verdicts: List[Any] = [
        {"strategy": r.get("strategy"), "verdict": r.get("system_verdict"), "source": "DB_CONFIRMED"}
        for r in executed_repairs if r.get("system_verdict")
    ]
    if str(current_failure or "").upper() == PPC_TARGET_FAILURE:
        if ppc_anchor is None:
            stop_reason = "PPC_BRANCH_ANCHOR_AMBIGUOUS"
        elif ppc_state and ppc_state.get("success"):
            stop_reason = "STOP_RESCUE_SUCCESS"
        elif ppc_state and ppc_state.get("exhausted"):
            stop_reason = "STOP_AUTOMATIC_RESCUE"
        elif ppc_state and ppc_state.get("evaluation_pending"):
            stop_reason = "HOLD_PPC_BRANCH_EVALUATION_PENDING"
        else:
            stop_reason = None
    else:
        stop_reason = evaluate_rescue_stop(system_strategy_verdicts, attempts, max_attempts)
    if ht_ratio_auto_repair_disabled:
        stop_reason = "NO_AUTO_REPAIR_HT_RATIO"

    if emit_audit and external_success is not None:
        audit_event(
            action="EXTERNAL_RESCUE_EVIDENCE", run_id=run_id, candidate_id=candidate_id,
            alpha_id=cand.get("alpha_id"), near_pass_class=cls["classification"],
            source=external_success.get("source"), strategy=external_success.get("strategy"),
            evidence_outcome=external_success.get("verdict"),
            external_target_value=external_success.get("child_value"), external_live_limit=external_success.get("live_limit"),
        )
    if emit_audit and stop_reason:
        audit_event(
            action="RESCUE_STOPPED", run_id=run_id, candidate_id=candidate_id,
            alpha_id=cand.get("alpha_id"), near_pass_class=cls["classification"],
            rescue_target=current_rescue_target, reason=stop_reason, attempts_used=attempts,
        )

    external_alpha_id = (external_success or {}).get("child_alpha_id") or (external_success or {}).get("alpha_id")
    recommended_action = "VERIFY_EXTERNAL_ALPHA_BEFORE_SIMULATION" if external_alpha_id else "EXECUTE_RECOMMENDED_RESCUE_IF_AUTHORIZED"
    blocking_stop_reasons = {
        "STOP_RESCUE_SUCCESS", "STOP_AUTOMATIC_RESCUE", "STOP_FOR_SIDE_EFFECT",
        "HOLD_PPC_BRANCH_EVALUATION_PENDING", "PPC_BRANCH_ANCHOR_AMBIGUOUS",
    }
    allowed = (
        cls.get("classification") in REPAIRABLE_STATUSES
        and cls.get("repair_priority") in {"HIGH", "MEDIUM"}
        and stop_reason not in blocking_stop_reasons
        and not external_alpha_id
        and not ht_ratio_auto_repair_disabled
    )
    if emit_audit:
        audit_event(
            action="RESCUE_PREVIEW", run_id=run_id, candidate_id=candidate_id,
            alpha_id=cand.get("alpha_id"), near_pass_class=cls["classification"],
            rescue_target=current_rescue_target, recommended_strategy=(recommendation or {}).get("strategy"),
            recommended_change=(recommendation or {}).get("change"), attempts_used=attempts,
            attempts_remaining=max(0, max_attempts - attempts), max_attempts=max_attempts,
            allowed_to_execute=allowed, stop_reason=stop_reason,
            ppc_branch_anchor_candidate_id=ppc_anchor,
            ppc_best_candidate_id=(ppc_state or {}).get("best_candidate_id"),
        )
    return {
        "mode": "RESCUE_PREVIEW", "run_id": run_id, "candidate_id": candidate_id,
        "alpha_id": cand.get("alpha_id"),
        # Preserve the public V3 preview contract: callers historically consume
        # the compatibility evidence label here while ppl_status carries the
        # platform-driven classification. PPC branch internals use current_failure
        # / primary_failure and do not depend on this presentation field.
        "classification": cls.get("evidence_label") or cls["classification"],
        "ppl_status": cls.get("ppl_status"),
        "repair_priority": cls.get("repair_priority"), "target_failure": current_failure,
        "rescue_target": current_rescue_target, "recommended_strategy": recommendation,
        "recommendation": recommendation, "recommended_action": recommended_action,
        "allowed_to_execute": allowed, "attempts_used": attempts,
        "attempts_remaining": max(0, max_attempts - attempts),
        "rescue_max_attempts": max_attempts, "max_attempts": max_attempts,
        "max_blocker_normalized_gap": cls.get("max_blocker_normalized_gap"),
        "blockers": cls.get("repair_blockers", []),
        "auto_stop_reason": stop_reason, "historical_evidence": evidence,
        "external_success_evidence": external_success is not None,
        "external_success_strategy": external_success.get("strategy") if external_success else None,
        "external_success_change": external_success.get("parameter_change") if external_success else None,
        "external_target_value": external_success.get("child_value") if external_success else None,
        "external_live_limit": external_success.get("live_limit") if external_success else None,
        "external_evidence_source": external_success.get("source") if external_success else None,
        "external_alpha_id": external_alpha_id,
        "ppc_branch": ({k: v for k, v in (ppc_state or {}).items() if k != "branch_rows"} if ppc_state else None),
        "network_requests": 0, "simulation_posts": 0, "check_requests": 0, "writes": 0,
    }
