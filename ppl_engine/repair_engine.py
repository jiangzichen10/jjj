"""Bounded, offline-only repair planning for V2.2 Phase 6."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .candidate_factory import (
    build_expression, canonicalize_expression, estimate_data_fields,
    evaluate_structure, pp_operator_count,
)
from .settings_contract import validate_full_simulation_settings



REPAIR_RULE_VERSION = 1
ALLOWED_OPERATOR_STATUSES = {"VERIFIED_API", "VERIFIED_PROJECT", "VALIDATED_SINGLE"}

# Strategy-level retirement registry. Historical plans are NEVER deleted; a retired
# strategy simply stops being recommended for NEW planning while old records remain
# as audit facts. V1 HT_RATIO_SIGNAL_HORIZON (ts_delta(field,2)) was empirically
# shown to degrade turnover/fitness, so it is no longer produced for HT failures.
TURNOVER_STAGED_POLICY = "TURNOVER_STAGED_POLICY_V1"
TURNOVER_DECAY_STEP_1 = "TURNOVER_DECAY_STEP_1"
TURNOVER_DECAY_STEP_2 = "TURNOVER_DECAY_STEP_2"
TURNOVER_HUMP = "TURNOVER_HUMP"
TURNOVER_STAGED_STRATEGIES = frozenset({
    TURNOVER_DECAY_STEP_1, TURNOVER_DECAY_STEP_2, TURNOVER_HUMP,
})
TURNOVER_DECAY_MAX = 20

RETIRED_STRATEGIES = {
    "HT_RATIO_SIGNAL_HORIZON": {
        "status": "STOP_THIS_REPAIR_PATH",
        "reason": "EMPIRICALLY_DEGRADES_TURNOVER_AND_FITNESS",
    },
    "HT_RATIO_TARGET_TVR": {
        "status": "STOP_THIS_REPAIR_PATH",
        "reason": "TARGET_TVR_AUTO_REPAIR_DISABLED",
    },
    "TURNOVER_HIGH_TS_MEAN_*": {
        "status": "STOP_THIS_REPAIR_PATH",
        "reason": "REPLACED_BY_TURNOVER_STAGED_POLICY_V1",
    },
}

# V2 HT Returns Ratio strategy: wrap the parent signal in ts_target_tvr_decay to
# explicitly steer target turnover instead of amplifying short-horizon change.
HT_V2_TARGET_TVR = "HT_RATIO_TARGET_TVR"
HT_V2_TARGET_TVR_OPERATOR = "ts_target_tvr_decay"

# V3 HT Returns Ratio strategy: keep the parent transform family and micro-tune
# the window (e.g. ts_mean 2 -> 3/4/5). This is used after the V2 target_tvr
# strategy was empirically shown to worsen HT ratio on the 0.625 canary.
SAME_FAMILY_MICRO_TUNE = "SAME_FAMILY_MICRO_TUNE"

# Settings micro-tune: keep the expression identical and change only a
# simulation setting (e.g. neutralization). Produces a NEW sim_key because
# sim_key = canonical {expression, settings}.
NEUTRALIZATION_MICRO_TUNE = "NEUTRALIZATION_MICRO_TUNE"

# Repair strategies that the Near-Pass Rescue Engine recommends (executed through
# the same Production Repair single-plan CLI; there is no separate rescue runner).
RESCUE_STRATEGIES = frozenset({SAME_FAMILY_MICRO_TUNE, NEUTRALIZATION_MICRO_TUNE})


def strategy_status() -> Dict[str, Any]:
    """Read-only report of retired/current HT repair strategy statuses."""
    return {
        "retired_strategies": dict(RETIRED_STRATEGIES),
        "active_ht_v2": None,
        "active_high_turnover": TURNOVER_STAGED_POLICY,
    }


_HT_RATIO_RAW_NAMES = frozenset({
    "HT_TURNOVER", "HT_HIGH_TURNOVER_RETURNS_RATIO",
    "HIGH_TURNOVER_RETURNS_RATIO", "HT_RETURNS_RATIO_FAIL",
})


def canonical_high_turnover_evidence_state(evidence: Mapping[str, Any]) -> str:
    """Return BLOCKED/CLEAR/INSUFFICIENT for the one supported turnover identity.

    Category and raw-name identity are deliberately part of the predicate.  In
    particular, legacy HT/theme aliases can never unlock this staged policy.
    """
    failure = str(evidence.get("failure") or evidence.get("primary_failure") or "").upper()
    check = str(evidence.get("check") or "").upper()
    group = str(evidence.get("group") or "").upper()
    source = str(evidence.get("source") or evidence.get("evidence_source") or "").upper()
    if (failure == "TURNOVER_ABOVE_BASE_MAX" and check == "TURNOVER"
            and group == "PPL_FIXED_GATE" and source == "SIMULATION_METRIC"):
        return "BLOCKED"

    raw = str(evidence.get("raw_name") or "").upper()
    normalized = str(evidence.get("normalized_name") or "").upper()
    category = str(evidence.get("category") or "").upper()
    outcome = str(evidence.get("eligibility_outcome") or evidence.get("normalized_result")
                  or evidence.get("raw_result") or "").upper()
    if raw in _HT_RATIO_RAW_NAMES or "TARGET_TVR" in raw or category == "PPL_THEME":
        return "INSUFFICIENT"
    if raw != "HIGH_TURNOVER" or normalized != "TURNOVER" or category != "PPL_BASE":
        return "INSUFFICIENT"
    if outcome in {"WARNING", "FAIL"}:
        return "BLOCKED"
    if outcome in {"PASS", "NOT_APPLICABLE"}:
        return "CLEAR"
    return "INSUFFICIENT"


def is_canonical_high_turnover_blocker(evidence: Mapping[str, Any]) -> bool:
    return canonical_high_turnover_evidence_state(evidence) == "BLOCKED"


def is_retired_auto_repair_plan(plan: Mapping[str, Any]) -> bool:
    """Execution-boundary retirement; historical rows remain untouched."""
    repair_type = str(plan.get("repair_type") or "").upper()
    target_failure = str(plan.get("target_failure") or "").upper()
    spec = plan.get("candidate_spec")
    if not isinstance(spec, Mapping):
        raw = plan.get("candidate_spec_json")
        try:
            spec = json.loads(str(raw)) if raw else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            spec = {}
    material = _canon(spec).upper()
    return (
        repair_type in {"HT_RATIO_TARGET_TVR", "HT_RATIO_SIGNAL_HORIZON"}
        or repair_type.startswith("TURNOVER_HIGH_TS_MEAN_")
        or target_failure == "HT_RETURNS_RATIO_FAIL"
        or "TARGET_TVR" in material
    )


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ht_target_tvr_grid(parent_turnover: Any, rules: Mapping[str, Any]) -> List[float]:
    """Parent-anchored, minimal target_tvr grid for V2 HT ratio repair.

    Anchors around the parent's turnover with a +-0.025 step, clamped to the
    High-Turnover Theme safe range [theme turnover preset_min, base preset_max]
    (i.e. 0.20–0.70). Never hardcodes a fixed set; it is a small micro-tune around
    the observed parent turnover.
    """
    base = rules["power_pool_base_presets"]["turnover"]
    theme_min = float(rules["current_theme"]["local_preconditions"]["high_turnover"]["turnover"]["preset_min"])
    lo = theme_min
    hi = float(base["preset_max"])
    t = _as_float(parent_turnover)
    if t is None:
        return []
    if not (lo <= t <= hi):
        t = hi  # already outside the safe range: anchor at the safe ceiling
    step = 0.025
    raw = [round(t - step, 3), round(t, 3), round(t + step, 3)]
    out: List[float] = []
    for v in raw:
        if lo <= v <= hi and v not in out:
            out.append(v)
    if not out:
        out = [round(t, 3)]
    return out[:4]


def _canon(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class RepairCandidateSpec:
    parent_candidate_id: str
    root_candidate_id: str
    repair_type: str
    base_expression_source: str = "PROVENANCE_REBUILD"
    direction_override: Optional[str] = None
    transform_family_override: Optional[str] = None
    window_override: Optional[int] = None
    expression_wrapper: Optional[str] = None
    settings_override: Mapping[str, Any] = field(default_factory=dict)
    target_failure: str = ""
    operator_requirements: Sequence[str] = field(default_factory=tuple)
    repair_signature: str = ""
    repair_depth: int = 1


def repair_signature(candidate: Mapping[str, Any], repair_type: str, target_failure: str,
                     parameters: Mapping[str, Any], settings_override: Mapping[str, Any]) -> str:
    material = {
        "root_signal_family": candidate.get("signal_family"),
        "parent_candidate_id": candidate.get("candidate_id"),
        "parent_expression_hash": candidate.get("expression_hash") or hashlib.sha256(str(candidate.get("expression", "")).encode()).hexdigest(),
        "target_failure": target_failure, "repair_type": repair_type,
        "parameters": parameters, "settings_override": settings_override,
        "repair_rule_version": REPAIR_RULE_VERSION,
    }
    return hashlib.sha256(_canon(material).encode()).hexdigest()


def detect_cycle(path: Sequence[str], target_failure: str, rules: Mapping[str, Any]) -> Optional[str]:
    max_repeat = int(rules["repair_cycle_control"]["max_same_failure_repeats"])
    failures = [part.split(":", 1)[0] for part in path if part != "RAW"] + [target_failure]
    if failures.count(target_failure) > max_repeat:
        return "STOP_REPEATED_FAILURE"
    if len(failures) >= 3 and failures[-3:] in (["TURNOVER_ABOVE_BASE_MAX", "TURNOVER_BELOW_THEME_MIN", "TURNOVER_ABOVE_BASE_MAX"], ["TURNOVER_BELOW_THEME_MIN", "TURNOVER_ABOVE_BASE_MAX", "TURNOVER_BELOW_THEME_MIN"]):
        return "REPAIR_OSCILLATION_DETECTED"
    families = ["SHARPE" if "SHARPE" in x else "TURNOVER" if "TURNOVER" in x else x for x in failures]
    if len(families) >= 4 and families[-4:] in (["SHARPE", "TURNOVER", "SHARPE", "TURNOVER"], ["TURNOVER", "SHARPE", "TURNOVER", "SHARPE"]):
        return "REPAIR_OSCILLATION_DETECTED"
    return None


def operator_gate(required: Iterable[str], registry: Mapping[str, str]) -> Dict[str, Any]:
    blocked, stopped = [], []
    for name in required:
        status = registry.get(name, "UNVERIFIED")
        if status in {"INVALID_SYNTAX", "UNAVAILABLE"}: stopped.append(name)
        elif status not in ALLOWED_OPERATOR_STATUSES: blocked.append(name)
    status = "STOP_OPERATOR_UNAVAILABLE" if stopped else "BLOCKED_OPERATOR_VALIDATION" if blocked else "READY"
    return {"status": status, "blocked": blocked, "stopped": stopped}


def _base_expr(candidate: Mapping[str, Any], *, transform: Optional[str] = None, window: Optional[int] = None,
               direction: Optional[str] = None) -> str:
    expression = build_expression(candidate["field_id"], candidate["field_type"], candidate.get("vector_reducer", "IDENTITY"), transform or candidate.get("operator", "raw"), window if window is not None else candidate.get("window"))
    final_direction = direction or candidate.get("direction", "NORMAL")
    return canonicalize_expression(f"-({expression})" if final_direction == "REVERSE" else expression)


def _spec(candidate: Mapping[str, Any], failure: str, repair_type: str, *, transform=None, window=None,
          direction=None, wrapper=None, wrapper_operator=None, settings=None, operators=()) -> Dict[str, Any]:
    params = {"transform": transform, "window": window, "direction": direction, "wrapper": wrapper}
    sig = repair_signature(candidate, repair_type, failure, params, settings or {})
    spec = RepairCandidateSpec(
        parent_candidate_id=str(candidate.get("candidate_id")), root_candidate_id=str(candidate.get("root_candidate_id") or candidate.get("candidate_id")),
        repair_type=repair_type, direction_override=direction, transform_family_override=wrapper_operator or transform,
        window_override=window, expression_wrapper=wrapper, settings_override=settings or {},
        target_failure=failure, operator_requirements=tuple(operators), repair_signature=sig,
        repair_depth=int(candidate.get("repair_depth") or 0) + 1,
    )
    result = asdict(spec)
    if wrapper is None:
        expression_preview = _base_expr(candidate, transform=transform, window=window, direction=direction)
    elif "{expr}" in wrapper:
        expression_preview = canonicalize_expression(wrapper.replace("{expr}", candidate["expression"]))
    else:
        expression_preview = f"{wrapper}({candidate['expression']})"
    result["expression_preview"] = expression_preview
    result.update({"candidate_stage": "REPAIR", "parent_sim_key": candidate.get("sim_key"), "inherits_power_pool_tag": False})
    return result


def same_family_micro_tune_spec(candidate: Mapping[str, Any], failure: str,
                                window: int) -> Dict[str, Any]:
    """Build a low-destruction same-family window micro-tune spec.

    The transform/operator and settings are preserved; only the time-series
    window changes. This is used by the bounded PP-correlation branch policy.
    """
    return _spec(candidate, failure, SAME_FAMILY_MICRO_TUNE,
                 transform=str(candidate.get("operator") or "raw"),
                 window=int(window), operators=(str(candidate.get("operator") or "raw"),))


def neutralization_micro_tune_spec(candidate: Mapping[str, Any], failure: str,
                                   neutralization: str) -> Dict[str, Any]:
    """Build a NEUTRALIZATION_MICRO_TUNE spec.

    The expression is kept byte-identical to the parent; only
    settings.neutralization changes. A new sim_key is derived downstream from
    the changed settings (sim_key = canonical {expression, settings}).
    """
    return _spec(candidate, failure, NEUTRALIZATION_MICRO_TUNE, wrapper="{expr}",
                 settings={"neutralization": str(neutralization).upper()}, operators=())


def _candidate_settings(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    raw = candidate.get("settings_json") or candidate.get("settings")
    if isinstance(raw, Mapping):
        return dict(raw)
    if raw:
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return {}


def turnover_stage_spec(candidate: Mapping[str, Any], stage: int, *,
                        origin_candidate_id: Optional[str] = None,
                        origin_signal_family: Optional[str] = None,
                        base_decay: Optional[int] = None,
                        previous_stage_candidate_id: Optional[str] = None) -> Dict[str, Any]:
    """Build exactly one stage of the bounded HIGH_TURNOVER policy."""
    if stage not in {1, 2, 3}:
        raise ValueError(f"Unsupported turnover repair stage: {stage}")
    settings = _candidate_settings(candidate)
    original_decay = int(settings.get("decay", candidate.get("decay") or 0)) if base_decay is None else int(base_decay)
    decay = min(original_decay + (2 if stage == 1 else 4), TURNOVER_DECAY_MAX)
    origin_id = str(origin_candidate_id or candidate.get("candidate_id") or "")
    origin_family = str(origin_signal_family or candidate.get("signal_family") or "")
    if not origin_id or not origin_family:
        raise ValueError("TURNOVER_STAGE_ORIGIN_IDENTITY_MISSING")
    if stage == 1:
        repair_type, wrapper, operators = TURNOVER_DECAY_STEP_1, "{expr}", ()
    elif stage == 2:
        repair_type, wrapper, operators = TURNOVER_DECAY_STEP_2, "{expr}", ()
    else:
        repair_type, wrapper, operators = TURNOVER_HUMP, "hump({expr}, hump=0.01)", ("hump",)
    spec = _spec(
        candidate, "TURNOVER_ABOVE_BASE_MAX", repair_type,
        wrapper=wrapper, wrapper_operator="hump" if stage == 3 else None,
        settings={"decay": decay}, operators=operators,
    )
    spec["turnover_staged_context"] = {
        "policy": TURNOVER_STAGED_POLICY,
        "origin_candidate_id": origin_id,
        "origin_signal_family": origin_family,
        "base_decay": original_decay,
        "stage": stage,
        "previous_stage_candidate_id": previous_stage_candidate_id,
    }
    return spec


def validate_turnover_hump_structure(spec: Mapping[str, Any], parent: Mapping[str, Any],
                                     rules: Mapping[str, Any], registry: Mapping[str, str]) -> Dict[str, Any]:
    """Apply the existing structural policy to the fixed Stage-3 template."""
    expression = str(spec.get("expression_preview") or "").strip()
    field_id = str(parent.get("field_id") or "").strip()
    if not expression or not field_id or "hump(" not in expression:
        return {"status": "TURNOVER_HUMP_STRUCTURE_BLOCKED", "reason": "IDENTITY_OR_TEMPLATE_MISSING"}
    gate = operator_gate(("hump",), registry)
    if gate["status"] != "READY":
        return {"status": "TURNOVER_HUMP_STRUCTURE_BLOCKED", "reason": gate["status"]}
    pp_policy = rules["operator_count_policy"]["pp_estimator"]
    count, calls = pp_operator_count(expression, pp_policy["exclude_functions"])
    fields = estimate_data_fields(expression, [field_id])
    structure = evaluate_structure(
        count, len(fields),
        operator_max=int(rules["power_pool_base_presets"]["operator_count"]["preset_max"]),
        field_max=int(rules["power_pool_base_presets"]["data_field_count"]["preset_max"]),
    )
    if structure != "ELIGIBLE" or fields != [field_id]:
        return {"status": "TURNOVER_HUMP_STRUCTURE_BLOCKED", "reason": structure,
                "operator_count": count, "operators": calls, "fields": fields}
    return {"status": "READY", "operator_count": count, "operators": calls, "fields": fields}


def parameter_change_summary(spec: Mapping[str, Any], parent: Mapping[str, Any]) -> Dict[str, Any]:
    """Structured parameter-change summary using the parent's durable settings."""
    change: Dict[str, Any] = {}
    parent_settings = _candidate_settings(parent)
    wo = spec.get("window_override")
    if wo is not None and wo != parent.get("window"):
        change["window"] = {"from": parent.get("window"), "to": wo}
    so = spec.get("settings_override") or {}
    for key in ("neutralization", "decay", "truncation"):
        before = parent_settings.get(key, parent.get(key))
        if key in so and so[key] != before:
            change[key] = {"from": before, "to": so[key]}
    tf = spec.get("transform_family_override")
    if tf and str(tf).lower() != str(parent.get("operator") or "").lower():
        change["transform"] = {"from": parent.get("operator"), "to": tf}
    wrapper = str(spec.get("expression_wrapper") or "")
    m = re.search(r"target_tvr\s*=\s*([0-9.]+)", wrapper)
    if m:
        change["target_tvr"] = {"to": float(m.group(1))}
    if not change and spec.get("expression_preview"):
        change["expression"] = spec["expression_preview"]
    return change


def plan_repairs(candidate: Mapping[str, Any], diagnosis: Mapping[str, Any], rules: Mapping[str, Any],
                 *, registry: Optional[Mapping[str, str]] = None, existing_signatures: Iterable[str] = (),
                 repair_reserve_remaining: int = 48, cache_by_expression: Optional[Mapping[str, str]] = None,
                 ht_strategy: Optional[str] = None) -> Dict[str, Any]:
    failure = str(diagnosis["primary_failure"]); registry = registry or {}; cache_by_expression = cache_by_expression or {}
    path = list(candidate.get("repair_path") or ["RAW"]); depth = int(candidate.get("repair_depth") or 0)
    stop_reason = None; proposed: List[Dict[str, Any]] = []; actions: List[str] = []
    if depth >= int(rules["repair_cycle_control"]["max_repair_depth"]): stop_reason = "STOP_REPAIR_DEPTH"
    if not stop_reason: stop_reason = detect_cycle(path, failure, rules)
    if failure in {"STRUCTURAL_CORRELATION_FAIL","STRUCTURAL_CORRELATION_RISK"}:
        stop_reason = "STOP_LOCAL_VARIANTS"; actions = ["STOP_LOCAL_VARIANTS", "SWITCH_FIELD", "SWITCH_FAMILY", "SWITCH_DATASET"]
    elif failure == "NEGATIVE_STRONG_SIGNAL" and not stop_reason:
        if "REVERSE_DIRECTION" in path or candidate.get("direction") == "REVERSE": stop_reason = "REVERSE_ALREADY_USED"
        else: proposed.append(_spec(candidate, failure, "REVERSE_DIRECTION", direction="REVERSE"))
    elif failure == "SHARPE_NEAR_PASS" and not stop_reason:
        for transform, window in (("rank", None), ("zscore", None), ("ts_mean", 3), ("ts_rank", 5)):
            proposed.append(_spec(candidate, failure, f"SHARPE_{transform.upper()}", transform=transform, window=window, operators=(transform,)))
    elif failure == "TURNOVER_ABOVE_BASE_MAX" and not stop_reason:
        if any(any(stage in str(part) for stage in TURNOVER_STAGED_STRATEGIES) for part in path):
            stop_reason = "TURNOVER_STAGE_ADVANCE_REQUIRES_DURABLE_CHECK"
        else:
            local_evidence = {
                "failure": failure, "check": "TURNOVER", "group": "PPL_FIXED_GATE",
                "source": "SIMULATION_METRIC",
            }
            if is_canonical_high_turnover_blocker(local_evidence):
                proposed.append(turnover_stage_spec(candidate, 1))
            else:
                stop_reason = "NO_AUTO_REPAIR_NONCANONICAL_TURNOVER"
    elif failure in {"TURNOVER_BELOW_BASE_MIN", "TURNOVER_BELOW_THEME_MIN"} and not stop_reason:
        if int(candidate.get("decay") or 0) == 0 and (candidate.get("window") or 1) <= 2:
            stop_reason = "NATURAL_HALF_LIFE_TOO_LONG"
        else:
            proposed.append(_spec(candidate, failure, "TURNOVER_LOW_TS_DELTA_1", transform="ts_delta", window=1, operators=("ts_delta",)))
    elif failure == "HT_RETURNS_RATIO_FAIL" and not stop_reason:
        stop_reason = "NO_AUTO_REPAIR_HT_RATIO"
    elif failure == "SUB_UNIVERSE_FAIL" and not stop_reason:
        proposed.extend([_spec(candidate, failure, "SUB_UNIVERSE_RANK", transform="rank", operators=("rank",)), _spec(candidate, failure, "SUB_UNIVERSE_ZSCORE", transform="zscore", operators=("zscore",))])
    elif failure == "WEIGHT_CONCENTRATION_FAIL" and not stop_reason:
        proposed.extend([_spec(candidate, failure, "CONCENTRATION_RANK", transform="rank", operators=("rank",)), _spec(candidate, failure, "CONCENTRATION_ZSCORE", transform="zscore", operators=("zscore",))])
    elif failure == "PP_CORRELATION_FAIL" and not stop_reason:
        actions = ["STRUCTURAL_REPAIR_REQUIRED", "RETURN_CHILD_TO_PRE_TAG"]
    elif failure in {"THEME_MATCH_UNKNOWN_CAUSE", "UNKNOWN_CHECK", "NO_FAILURE", "WEAK_SIGNAL_STRUCTURAL", "DETERMINISTIC_ERROR"}:
        stop_reason = "MANUAL_REVIEW" if failure in {"THEME_MATCH_UNKNOWN_CAUSE", "UNKNOWN_CHECK"} else "NO_REPAIR" if failure == "NO_FAILURE" else "STRUCTURAL_STOP"

    max_items = 0
    budget_key = {"SHARPE_NEAR_PASS":"sharpe", "TURNOVER_ABOVE_BASE_MAX":"turnover_high", "TURNOVER_BELOW_BASE_MIN":"turnover_low", "TURNOVER_BELOW_THEME_MIN":"turnover_low", "HT_RETURNS_RATIO_FAIL":"ht_ratio", "SUB_UNIVERSE_FAIL":"sub_universe", "WEIGHT_CONCENTRATION_FAIL":"concentration", "PP_CORRELATION_FAIL":"ppl_corr"}.get(failure)
    if budget_key: max_items = int(rules["repair_budget"][budget_key]["max_candidates_per_round"])
    else: max_items = 1
    proposed = proposed[:max_items]
    existing = set(existing_signatures); plans=[]; projected=0
    for item in proposed:
        item["repair_path"] = path + [f"{failure}:{item['repair_type']}"]
        if item["repair_signature"] in existing:
            item["plan_status"]="BLOCKED_CYCLE"
        else:
            gate=operator_gate(item["operator_requirements"],registry); item["plan_status"]=gate["status"]
            item["operator_gate"]=gate
            cache=cache_by_expression.get(item["expression_preview"],"CACHE_MISS")
            item["cache_classification"]=cache
            item["projected_new_post"]=int(cache=="CACHE_MISS" and item["plan_status"]=="READY")
            if projected+item["projected_new_post"]>repair_reserve_remaining:
                item["plan_status"]="BLOCKED_BUDGET"; item["projected_new_post"]=0
            projected+=item["projected_new_post"]
        plans.append(item)
    return {"primary_failure":failure,"plans":plans,"recommended_actions":actions,"stop_reason":stop_reason,
            "repair_projected_posts":projected,"repair_committed_posts":0,"repair_consumed_posts":0}


def evaluate_repair_side_effect(parent: Mapping[str, Any], child: Mapping[str, Any], primary_failure: str,
                                rules: Mapping[str, Any]) -> Dict[str, Any]:
    if not child or child.get("status") not in {None, "COMPLETE"}: return {"verdict":"PENDING_RESULT","reasons":["CHILD_NOT_COMPLETE"]}
    tolerance=rules["side_effect_tolerance"]; reasons=[]
    for metric,key in (("sharpe","sharpe_drop_fraction_max"),("fitness","fitness_drop_fraction_max"),("margin","margin_drop_fraction_max")):
        if parent.get(metric) is not None and child.get(metric) is not None and float(parent[metric])>0:
            if (float(parent[metric])-float(child[metric]))/float(parent[metric])>float(tolerance[key]) + 1e-12: reasons.append(f"{metric.upper()}_DROP_EXCEEDS_TOLERANCE")
    if parent.get("local_gate_status")=="PASS" and child.get("local_gate_status")=="FAIL": reasons.append("HARD_GATE_FLIPPED_TO_FAIL")
    for name,key in (("ht_ratio_status","preserve_ht_ratio_pass"),("sub_universe_status","preserve_sub_universe_pass")):
        if tolerance[key] and parent.get(name)=="PASS" and child.get(name)=="FAIL": reasons.append(f"{name.upper()}_FLIPPED_TO_FAIL")
    pphase,cphase=parent.get("check_phase"),child.get("check_phase")
    if pphase and cphase and pphase!=cphase and (parent.get("pp_corr") is not None or child.get("pp_corr") is not None): return {"verdict":"PENDING_RESULT","reasons":["INCOMPATIBLE_CHECK_PHASE"]}
    improved = child.get("primary_failure_resolved") is True
    if not improved: reasons.append("PRIMARY_FAILURE_NOT_IMPROVED")
    return {"verdict":"REJECT" if reasons else "ACCEPT","reasons":reasons}


def materialize_repair_candidate(parent: Mapping[str, Any], spec: Mapping[str, Any], config: Any,
                                 machine_lib: Any) -> Dict[str, Any]:
    """Use the established V2.1 identity/settings path; does not persist or perform I/O."""
    expression = canonicalize_expression(spec["expression_preview"])
    effective_operator = (spec.get("transform_family_override") or parent.get("operator", "raw")).lower()
    effective_window = spec.get("window_override")
    if effective_window is None:
        effective_window = parent.get("window")
        if effective_window is None and effective_operator == "ts_mean":
            # Legacy Repair children created by settings-only micro-tunes may
            # have NULL window metadata even though the expression still carries
            # the real ts_mean window. Never reinterpret NULL as window=0.
            from .ppc_controlled_branch import resolve_effective_window
            effective_window = resolve_effective_window(parent)
    candidate = {
        "expr": expression, "field": parent["field_id"], "data_fields": [parent["field_id"]],
        "dataset_id": parent["dataset_id"], "dataset_ids": [parent["dataset_id"]],
        "field_type": parent["field_type"],
        "vector_op": None if parent.get("vector_reducer", "IDENTITY") == "IDENTITY" else str(parent["vector_reducer"]).lower(),
        "operator": effective_operator,
        "window": effective_window,
        "decay": int(spec.get("settings_override", {}).get("decay", parent.get("decay", 0))),
        "stage": "REPAIR", "parent_candidate_id": parent.get("candidate_id"),
        "parent_sim_key": parent.get("sim_key"), "root_candidate_id": parent.get("root_candidate_id") or parent.get("candidate_id"),
        "repair_type": spec["repair_type"], "repair_signature": spec["repair_signature"],
        "repair_depth": spec["repair_depth"], "inherits_power_pool_tag": False,
    }
    annotated = machine_lib.annotate_candidate_strategy(candidate, config.target_mode)
    machine_lib.validate_candidate_context([annotated], dataset_id=parent["dataset_id"], target_mode=config.target_mode)
    # Preserve the actual parent settings first; a micro-tune must change only
    # the requested setting and must never silently reset another parent setting
    # back to the run-level default. Legacy rows without settings_json fall back
    # to the run plan.
    settings_plan = dict(config.plan["simulation_settings"])
    parent_settings = _candidate_settings(parent)
    settings_plan.update(parent_settings)
    settings_plan.update(spec.get("settings_override") or {})
    test_period = settings_plan.get("testPeriod", settings_plan.get("test_period", "P0Y"))
    if "decay" in settings_plan:
        annotated["decay"] = int(settings_plan["decay"])
    settings = validate_full_simulation_settings(
        machine_lib.build_settings(
            annotated, neutralization=settings_plan["neutralization"], region=settings_plan["region"],
            universe=settings_plan["universe"], delay=settings_plan["delay"],
            truncation=settings_plan["truncation"], test_period=test_period,
        ),
        context="REPAIR_CANDIDATE_MATERIALIZATION",
    )
    annotated["sim_key"] = machine_lib.simulation_key(expression, settings)
    annotated["settings"] = settings
    return annotated
