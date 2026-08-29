"""Offline, evidence-based diagnosis and LOCAL_PRE_GATE for V2.2 Phase 6."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional


DIAGNOSIS_RULE_VERSION = 2
PRECEDENCE = (
    "DETERMINISTIC_ERROR", "WEAK_SIGNAL_STRUCTURAL", "NEGATIVE_STRONG_SIGNAL",
    "STRUCTURAL_CORRELATION_FAIL", "STRUCTURAL_CORRELATION_RISK", "TURNOVER_ABOVE_BASE_MAX",
    "TURNOVER_BELOW_BASE_MIN", "TURNOVER_BELOW_THEME_MIN",
    "HT_RETURNS_RATIO_FAIL", "SUB_UNIVERSE_FAIL", "WEIGHT_CONCENTRATION_FAIL",
    "SHARPE_NEAR_PASS", "THEME_MATCH_WARNING", "THEME_MATCH_UNKNOWN_CAUSE", "UNKNOWN_CHECK",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def turnover_class(turnover: Optional[float], rules: Mapping[str, Any]) -> str:
    if turnover is None:
        return "UNKNOWN"
    base = rules["power_pool_base_presets"]["turnover"]
    theme = rules["current_theme"]["local_preconditions"]["high_turnover"]["turnover"]
    active_theme_required = {
        str(x).upper() for x in ((rules.get("current_theme", {}).get("live_theme_checks", {}) or {}).get("required", []))
    }
    value = float(turnover)
    if value > float(base["preset_max"]):
        return "TURNOVER_ABOVE_BASE_MAX"
    if value < float(base["preset_min"]):
        return "TURNOVER_BELOW_BASE_MIN"
    # The 20% floor is only meaningful for an active High-Turnover theme.
    # The simplified GLB Liquid theme keeps the legacy preset for audit/history
    # but does not apply it to local gating.
    if "HIGH_TURNOVER" in active_theme_required and value < float(theme["preset_min"]):
        return "TURNOVER_BELOW_THEME_MIN"
    return "TURNOVER_PASS"


def evaluate_local_pre_gate(candidate: Mapping[str, Any], metrics: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    """Evaluate only locally knowable facts; live checks are deliberately absent."""
    sharpe = metrics.get("sharpe")
    turnover = metrics.get("turnover")
    tc = turnover_class(turnover, rules)
    base = rules["power_pool_base_presets"]
    theme_match = bool(candidate.get("theme_settings_match", True))
    dataset_allowed = bool(candidate.get("dataset_allowed", True))
    structure = str(candidate.get("structure_status", "ELIGIBLE")) == "ELIGIBLE"
    checks = {
        "theme_settings_match": theme_match,
        "dataset_allowed": dataset_allowed,
        "simulation_sharpe": sharpe is not None and float(sharpe) >= float(base["sharpe"]["preset_min"]),
        "simulation_turnover_base": tc not in {"UNKNOWN", "TURNOVER_ABOVE_BASE_MAX", "TURNOVER_BELOW_BASE_MIN"},
        "theme_turnover_precondition": tc == "TURNOVER_PASS",
        "local_data_field_count": int(candidate.get("data_field_count_estimate", 0)) <= int(base["data_field_count"]["preset_max"]),
        "pp_total_operator_count_estimate": int(candidate.get("pp_total_operator_count_estimate", 0)) <= int(base["operator_count"]["preset_max"]),
        "candidate_structure_valid": structure,
    }
    theme_required = list((rules.get("current_theme", {}).get("live_theme_checks", {}) or {}).get("required", []))
    unknown_live = ["SUB_UNIVERSE", "POWER_POOL_CORRELATION"]
    for name in theme_required:
        if name not in unknown_live:
            unknown_live.append(name)
    return {
        "gate": "LOCAL_PRE_GATE", "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks, "turnover_class": tc,
        "unknown_live_facts": unknown_live,
    }


def _check_failures(results: Iterable[Mapping[str, Any]], phase: str, rules: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    structural = float(rules["heuristics"]["pp_corr"]["structural_fail"])
    active_theme_required = {
        str(x).upper() for x in ((rules.get("current_theme", {}).get("live_theme_checks", {}) or {}).get("required", []))
    }
    for item in results:
        category, name = str(item.get("category")), str(item.get("normalized_name"))
        outcome=str(item.get("eligibility_outcome") or item.get("normalized_result") or "UNKNOWN").upper()
        # Category is part of identity. Generic checks never masquerade as PPL checks.
        if category == "PPL_BASE" and name in {"POWER_POOL_CORRELATION", "POWER_POOL_SELF_CORRELATION"} and phase == "FINAL":
            value = item.get("normalized_value") if item.get("normalized_value") is not None else item.get("raw_value")
            limit = item.get("normalized_limit") if item.get("normalized_limit") is not None else item.get("raw_limit")
            if value is not None and limit is not None and float(value) > float(limit) and outcome in {"FAIL","WARNING"}:
                if outcome=="FAIL": code = "STRUCTURAL_CORRELATION_FAIL" if float(value) >= structural else "PP_CORRELATION_FAIL"
                else: code = "STRUCTURAL_CORRELATION_RISK" if float(value) >= structural else "PP_CORRELATION_WARNING"
                out.append({"failure": code, "evidence": item})
        elif outcome!="FAIL":
            if category=="PPL_THEME" and name=="THEME_MATCH" and outcome=="WARNING":
                out.append({"failure":"THEME_MATCH_WARNING","evidence":item})
            continue
        elif category == "PPL_BASE" and name == "SUB_UNIVERSE":
            out.append({"failure": "SUB_UNIVERSE_FAIL", "evidence": item})
        elif category == "PPL_BASE" and name == "WEIGHT_CONCENTRATION":
            out.append({"failure": "WEIGHT_CONCENTRATION_FAIL", "evidence": item})
        elif category == "PPL_THEME" and name == "HIGH_TURNOVER_RETURNS_RATIO":
            # Theme-specific checks are only blockers while the active theme
            # explicitly requires them. The simplified GLB Liquid theme no
            # longer requires HT returns ratio, so a lingering platform
            # WARNING remains evidence but does not drive repair.
            if name in active_theme_required:
                out.append({"failure": "HT_RETURNS_RATIO_FAIL", "evidence": item})
        elif category == "PPL_THEME" and name == "THEME_MATCH":
            out.append({"failure": "THEME_MATCH_UNKNOWN_CAUSE", "evidence": item})
        elif category == "UNKNOWN" or name == "UNKNOWN":
            out.append({"failure": "UNKNOWN_CHECK", "evidence": item})
    return out


def diagnose_evidence(envelope: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    metrics = dict(envelope.get("metrics") or envelope.get("facts") or {})
    candidate = dict(envelope.get("candidate") or {})
    phase = str(envelope.get("phase") or "SIMULATION").upper()
    local = evaluate_local_pre_gate(candidate, metrics, rules)
    failures: List[Dict[str, Any]] = []
    sharpe = metrics.get("sharpe")
    if envelope.get("error_nature") == "DETERMINISTIC":
        failures.append({"failure": "DETERMINISTIC_ERROR", "evidence": envelope.get("error")})
    if sharpe is not None:
        value = float(sharpe)
        if value < 0 and abs(value) >= float(rules["heuristics"]["reverse_abs_sharpe_min"]):
            failures.append({"failure": "NEGATIVE_STRONG_SIGNAL", "evidence": {"sharpe": value}})
        elif float(rules["heuristics"]["near_pass_sharpe_min"]) <= value < float(rules["power_pool_base_presets"]["sharpe"]["preset_min"]):
            failures.append({"failure": "SHARPE_NEAR_PASS", "evidence": {"sharpe": value}})
        elif abs(value) < float(rules["heuristics"]["weak_signal_stop_sharpe"]) and local["turnover_class"] != "TURNOVER_PASS":
            failures.append({"failure": "WEAK_SIGNAL_STRUCTURAL", "evidence": {"sharpe": value}})
    if local["turnover_class"] != "TURNOVER_PASS" and local["turnover_class"] != "UNKNOWN":
        failures.append({"failure": local["turnover_class"], "evidence": {"turnover": metrics.get("turnover")}})
    parsed = envelope.get("parsed") or {}
    failures.extend(_check_failures(parsed.get("results", envelope.get("check_results", [])), phase, rules))
    rank = {name: index for index, name in enumerate(PRECEDENCE)}
    failures.sort(key=lambda x: rank.get(x["failure"], len(rank)))
    primary = failures[0]["failure"] if failures else "NO_FAILURE"
    secondary = [x["failure"] for x in failures[1:]]
    structural = primary in {"DETERMINISTIC_ERROR", "WEAK_SIGNAL_STRUCTURAL", "STRUCTURAL_CORRELATION_FAIL", "STRUCTURAL_CORRELATION_RISK"}
    manual = primary in {"THEME_MATCH_WARNING", "THEME_MATCH_UNKNOWN_CAUSE", "UNKNOWN_CHECK"}
    repairability = "NO_ACTION" if primary == "NO_FAILURE" else "STRUCTURAL" if structural else "MANUAL_REVIEW" if manual else "LIMITED_REPAIR" if primary in {"SHARPE_NEAR_PASS", "PP_CORRELATION_FAIL"} else "REPAIRABLE"
    severity = "NONE" if primary == "NO_FAILURE" else "HIGH" if structural else "MEDIUM"
    root = "UNKNOWN" if primary in {"THEME_MATCH_WARNING", "THEME_MATCH_UNKNOWN_CAUSE", "UNKNOWN_CHECK"} else primary
    material = {"run": envelope.get("run_id"), "candidate": envelope.get("candidate_id"), "phase": phase, "failures": [x["failure"] for x in failures], "metrics": metrics}
    did = "diag_" + hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]
    actions=[]
    if primary=="STRUCTURAL_CORRELATION_RISK":actions=["STOP_LOCAL_VARIANTS","SWITCH_FIELD","SWITCH_FAMILY","SWITCH_DATASET"]
    elif primary=="THEME_MATCH_WARNING":actions=["MANUAL_REVIEW"]
    return {
        "diagnosis_id": did, "run_id": envelope.get("run_id"), "candidate_id": envelope.get("candidate_id"),
        "alpha_id": envelope.get("alpha_id"), "source_phase": phase,
        "evidence_source": envelope.get("evidence_source", "OFFLINE_FIXTURE"),
        "primary_failure": primary, "secondary_failures": secondary, "severity": severity,
        "repairability": repairability, "root_cause": root, "metrics_snapshot": metrics,
        "local_pre_gate": local, "check_session_id": envelope.get("check_session_id"),
        "check_result_ids": envelope.get("check_result_ids", []), "diagnosis_rule_version": DIAGNOSIS_RULE_VERSION,
        "recommended_research_actions":actions,
        "created_at": _now(),
    }


def compare_check_interpretations(manual: Mapping[str, Any], live: Mapping[str, Any]) -> Dict[str, Any]:
    """Compare labels without pretending identical numeric evidence conflicts."""
    same_value=manual.get("raw_value")==live.get("raw_value")
    same_limit=manual.get("raw_limit")==live.get("raw_limit")
    labels_differ=str(manual.get("raw_result"))!=str(live.get("raw_result"))
    kind="INTERPRETATION_CONFLICT" if same_value and same_limit and labels_differ else "RAW_EVIDENCE_CONFLICT" if not (same_value and same_limit) else "NONE"
    return {"conflict_type":kind,"manual_result":manual.get("raw_result"),"live_result":live.get("raw_result"),"raw_value_consistent":same_value,"raw_limit_consistent":same_limit,"authoritative_source":"LIVE_API"}
