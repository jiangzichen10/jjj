"""Versioned Power Pool check parser and gate aggregation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


CHECK_PARSER_VERSION = 4
CHECK_ALIAS_VERSION = 4
RESULTS = {"PASS", "FAIL", "WARNING", "PENDING", "UNKNOWN", "NOT_APPLICABLE"}
PHASES = {"PRE_TAG", "RECHECK", "FINAL"}


def _key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", str(value or "").strip().upper()).strip("_")


# Exact/normalized aliases only. Generic Alpha names deliberately remain separate.
ALIASES = {
    # Exact aliases observed in the sanitized Phase-9 live payload.
    "LOW_SHARPE": ("SHARPE", "REGULAR_ALPHA"),
    "LOW_TURNOVER": ("TURNOVER", "PPL_BASE"),
    "LOW_SUB_UNIVERSE_SHARPE": ("SUB_UNIVERSE", "PPL_BASE"),
    "HT_TURNOVER": ("HIGH_TURNOVER", "PPL_THEME"),
    "HT_HIGH_TURNOVER_RETURNS_RATIO": ("HIGH_TURNOVER_RETURNS_RATIO", "PPL_THEME"),
    "MATCHES_CLASSIFICATION": ("CLASSIFICATION_HIGH_TURNOVER", "PPL_THEME"),
    "MATCHES_THEMES": ("THEME_MATCH", "PPL_THEME"),
    "LOW_FITNESS": ("FITNESS", "REGULAR_ALPHA"),
    "CONCENTRATED_WEIGHT": ("WEIGHT_CONCENTRATION", "REGULAR_ALPHA"),
    "LOW_2Y_SHARPE": ("TWO_YEAR_SHARPE", "REGULAR_ALPHA"),
    "CLUSTER_TEST": ("CLUSTER", "INFORMATIONAL"),
    "OSMOSIS_ALLOCATION": ("OSMOSIS", "INFORMATIONAL"),
    "POWER_POOL_SHARPE": ("SHARPE", "PPL_BASE"),
    "PPL_SHARPE": ("SHARPE", "PPL_BASE"),
    "POWER_POOL_TURNOVER": ("TURNOVER", "PPL_BASE"),
    "PPL_TURNOVER": ("TURNOVER", "PPL_BASE"),
    "POWER_POOL_OPERATOR_COUNT": ("OPERATOR_COUNT", "PPL_BASE"),
    "OPERATOR_COUNT": ("OPERATOR_COUNT", "PPL_BASE"),
    "POWER_POOL_DATA_FIELD_COUNT": ("DATA_FIELD_COUNT", "PPL_BASE"),
    "DATA_FIELD_COUNT": ("DATA_FIELD_COUNT", "PPL_BASE"),
    "SUB_UNIVERSE": ("SUB_UNIVERSE", "PPL_BASE"),
    "SUBUNIVERSE": ("SUB_UNIVERSE", "PPL_BASE"),
    "POWER_POOL_CORRELATION": ("POWER_POOL_CORRELATION", "PPL_BASE"),
    "POWER_POOL_SELF_CORRELATION": ("POWER_POOL_SELF_CORRELATION", "PPL_BASE"),
    "HIGH_TURNOVER": ("HIGH_TURNOVER", "PPL_THEME"),
    "HIGH_TURNOVER_TURNOVER": ("HIGH_TURNOVER", "PPL_THEME"),
    "HIGH_TURNOVER_RETURNS_RATIO": ("HIGH_TURNOVER_RETURNS_RATIO", "PPL_THEME"),
    "CLASSIFICATION_HIGH_TURNOVER": ("CLASSIFICATION_HIGH_TURNOVER", "PPL_THEME"),
    "THEME_MATCH": ("THEME_MATCH", "PPL_THEME"),
    "CURRENT_THEME": ("THEME_MATCH", "PPL_THEME"),
    "FITNESS": ("FITNESS", "REGULAR_ALPHA"),
    "SHARPE": ("SHARPE", "REGULAR_ALPHA"),
    "TURNOVER": ("TURNOVER", "REGULAR_ALPHA"),
    "WEIGHT_CONCENTRATION": ("WEIGHT_CONCENTRATION", "REGULAR_ALPHA"),
    "REGION_SHARPE": ("REGION_SHARPE", "REGULAR_ALPHA"),
    "TWO_YEAR_SHARPE": ("TWO_YEAR_SHARPE", "REGULAR_ALPHA"),
    "CLUSTER": ("CLUSTER", "INFORMATIONAL"),
    "PROD_CORRELATION": ("PROD_CORRELATION", "REGULAR_ALPHA"),
    "SELF_CORRELATION": ("SELF_CORRELATION", "REGULAR_ALPHA"),
    "OSMOSIS": ("OSMOSIS", "INFORMATIONAL"),
    # V3.0.4: preserve platform semantics that were previously collapsed to UNKNOWN.
    "LOW_GLB_AMER_SHARPE": ("LOW_GLB_AMER_SHARPE", "NON_PPL_DIAGNOSTIC"),
    "LOW_GLB_EMEA_SHARPE": ("LOW_GLB_EMEA_SHARPE", "NON_PPL_DIAGNOSTIC"),
    "LOW_GLB_APAC_SHARPE": ("LOW_GLB_APAC_SHARPE", "NON_PPL_DIAGNOSTIC"),
    "DATA_DIVERSITY": ("DATA_DIVERSITY", "NON_PPL_DIAGNOSTIC"),
    "MATCHES_COMPETITION": ("MATCHES_COMPETITION", "NON_PPL_DIAGNOSTIC"),
    "MATCHES_PYRAMID": ("MATCHES_PYRAMID", "NON_PPL_DIAGNOSTIC"),
    "REGULAR_SUBMISSION": ("REGULAR_SUBMISSION", "NON_PPL_DIAGNOSTIC"),
    "POWER_POOL_DESCRIPTION_LENGTH": ("POWER_POOL_DESCRIPTION_LENGTH", "AUTOMATION_IGNORED"),
    "POWER_POOL_DESCRIPTION_FORMAT": ("POWER_POOL_DESCRIPTION_FORMAT", "AUTOMATION_IGNORED"),
    "PURE_POWER_POOL_THEME": ("PURE_POWER_POOL_THEME", "AUTOMATION_IGNORED"),
}


def normalize_check_name(raw_name: Any) -> Dict[str, str]:
    normalized = _key(raw_name)
    found = ALIASES.get(normalized)
    if found:
        return {"normalized_name": found[0], "category": found[1], "mapping_suggestion": None}
    # Unknown HT_* checks are theme-specific evidence discovered at runtime.
    # Preserve the raw normalized name instead of collapsing all of them to UNKNOWN;
    # the V3.0.4 classifier records them as UNMAPPED_THEME_SIGNAL until explicitly
    # enabled in the current theme policy.
    if normalized.startswith("HT_"):
        return {"normalized_name": normalized, "category": "PPL_THEME_UNMAPPED", "mapping_suggestion": None}
    suggestion = normalized if normalized else None
    return {"normalized_name": "UNKNOWN", "category": "UNKNOWN", "mapping_suggestion": suggestion}


def normalize_result(value: Any) -> str:
    key = _key(value)
    aliases = {"PASSED": "PASS", "FAILED": "FAIL", "NOT_MATCH": "FAIL", "NOT_MATCHED": "FAIL",
               "N_A": "NOT_APPLICABLE", "NA": "NOT_APPLICABLE"}
    result = aliases.get(key, key)
    return result if result in RESULTS else "UNKNOWN"


def _eligibility_outcome(name: Mapping[str, str], normalized_result: str, raw_value: Any,
                         raw_limit: Any, raw_name: Any) -> Dict[str, Any]:
    """Interpret eligibility without rewriting the platform's raw result."""
    canonical=name["normalized_name"];category=name["category"]
    threshold_exceeded=False
    if canonical=="POWER_POOL_CORRELATION" and isinstance(raw_value,(int,float)) and isinstance(raw_limit,(int,float)):
        threshold_exceeded=float(raw_value)>float(raw_limit)
    if category not in {"PPL_BASE","PPL_THEME"}:
        outcome=normalized_result
        reason="NON_PPL_CHECK_NOT_USED_BY_PPL_GATE"
    elif _key(raw_name)=="MATCHES_CLASSIFICATION" and canonical=="CLASSIFICATION_HIGH_TURNOVER" and normalized_result=="PASS":
        values=raw_value if isinstance(raw_value,list) else [raw_value]
        matched=any(_key(x)=="HIGH_TURNOVER" for x in values)
        outcome="PASS" if matched else "UNKNOWN"
        reason="CLASSIFICATION_HIGH_TURNOVER_CONFIRMED" if matched else "CLASSIFICATION_VALUE_NOT_HIGH_TURNOVER"
    elif canonical=="POWER_POOL_CORRELATION" and normalized_result=="WARNING" and threshold_exceeded:
        outcome="WARNING";reason="PP_CORRELATION_LIMIT_EXCEEDED_ADVANTAGE_STATUS_UNRESOLVED"
    elif normalized_result=="WARNING":
        outcome="WARNING";reason="PLATFORM_WARNING_REQUIRES_REVIEW"
    elif normalized_result in {"PASS","FAIL","PENDING","UNKNOWN","NOT_APPLICABLE"}:
        outcome=normalized_result;reason=f"PLATFORM_{normalized_result}"
    else:
        outcome="UNKNOWN";reason="UNSUPPORTED_PLATFORM_RESULT"
    return {"eligibility_outcome":outcome,"eligibility_reason":reason,"threshold_exceeded":threshold_exceeded}


def _preset_limit(name: str, rules: Mapping[str, Any]) -> Any:
    base = rules.get("power_pool_base_presets", {})
    theme = rules.get("current_theme", {})
    mapping = {
        "SHARPE": base.get("sharpe", {}).get("preset_min"),
        "TURNOVER": base.get("turnover"),
        "OPERATOR_COUNT": base.get("operator_count", {}).get("preset_max"),
        "DATA_FIELD_COUNT": base.get("data_field_count", {}).get("preset_max"),
        "HIGH_TURNOVER": theme.get("local_preconditions", {}).get("high_turnover", {}).get("turnover", {}).get("preset_min"),
        "HIGH_TURNOVER_RETURNS_RATIO": theme.get("live_theme_checks", {}).get("high_turnover_returns_ratio", {}).get("preset_min"),
    }
    return mapping.get(name)


def parse_individual_check(raw: Mapping[str, Any], phase: str, rules: Mapping[str, Any], evidence_source: str) -> Dict[str, Any]:
    name = normalize_check_name(raw.get("name"))
    raw_result = raw.get("result")
    normalized_result = normalize_result(raw_result)
    raw_value = raw.get("value")
    raw_limit = raw.get("limit")
    unit = raw.get("unit")
    unit_confidence = str(raw.get("unitConfidence") or ("HIGH" if unit else "UNKNOWN")).upper()
    known_unit = bool(unit) and unit_confidence in {"HIGH", "MEDIUM"}
    normalized_value = float(raw_value) if known_unit and isinstance(raw_value, (int, float)) else None
    normalized_limit = float(raw_limit) if known_unit and isinstance(raw_limit, (int, float)) else None
    preset = _preset_limit(name["normalized_name"], rules)
    live = raw_limit if raw_limit is not None else None
    effective = live if live is not None else preset
    eligibility=_eligibility_outcome(name,normalized_result,raw_value,raw_limit,raw.get("name"))
    diagnosis_outcome="NONE";diagnosis_reason="NO_CHECK_LEVEL_DIAGNOSIS"
    if name["normalized_name"]=="POWER_POOL_CORRELATION" and eligibility["threshold_exceeded"] and eligibility["eligibility_outcome"]=="WARNING":
        structural=float(rules["heuristics"]["pp_corr"]["structural_fail"])
        diagnosis_outcome="STRUCTURAL_CORRELATION_RISK" if float(raw_value)>=structural else "PP_CORRELATION_WARNING"
        diagnosis_reason="LOCAL_HEURISTIC_NOT_ELIGIBILITY_VERDICT"
    elif name["normalized_name"]=="THEME_MATCH" and eligibility["eligibility_outcome"]=="WARNING":
        diagnosis_outcome="MANUAL_REVIEW";diagnosis_reason="THEME_ROOT_CAUSE_UNKNOWN"
    return {
        "raw_name": raw.get("name"), **name, "category": name["category"],
        "raw_result": raw_result, "normalized_result": normalized_result,
        "raw_value": raw_value, "raw_limit": raw_limit,
        "normalized_value": normalized_value, "normalized_limit": normalized_limit,
        "unit": unit, "unit_confidence": unit_confidence,
        "preset_limit": preset, "live_limit": live, "effective_limit": effective,
        "limit_source": "LIVE_CHECK" if live is not None else "PRESET",
        "status": raw.get("status"), "message": raw.get("message"),
        **eligibility,"diagnosis_outcome":diagnosis_outcome,"diagnosis_reason":diagnosis_reason,
        "parser_version": CHECK_PARSER_VERSION, "alias_version": CHECK_ALIAS_VERSION,
        "evidence_source": evidence_source,
    }


def _checks_from_payload(payload: Mapping[str, Any]) -> Optional[List[Mapping[str, Any]]]:
    if isinstance(payload.get("checks"), list):
        return payload["checks"]
    nested = payload.get("is")
    if isinstance(nested, Mapping) and isinstance(nested.get("checks"), list):
        return nested["checks"]
    return None


def _by_gate(results: Sequence[Mapping[str, Any]], category: str) -> Dict[str, str]:
    out = {}
    rank = {"FAIL": 6, "PENDING": 5, "WARNING": 4, "UNKNOWN": 3, "DEFERRED": 2, "PASS": 1, "NOT_APPLICABLE": 0}
    for item in results:
        if item["category"] == category:
            name=item["normalized_name"];result=item.get("eligibility_outcome",item["normalized_result"])
            if name not in out or rank.get(result,2)>rank.get(out[name],2):out[name]=result
    return out


def evaluate_live_base_gate(results: Sequence[Mapping[str, Any]], phase: str, rules: Mapping[str, Any]) -> Dict[str, Any]:
    phase = phase.upper(); observed = _by_gate(results, "PPL_BASE")
    required = list(rules["power_pool_live_base_checks"]["required"])
    checks = {}; errors = []
    for name in required:
        if name in observed:
            checks[name] = observed[name]
        elif phase == "PRE_TAG" and name == "POWER_POOL_CORRELATION" and \
                rules["power_pool_live_base_checks"]["phase_policy"]["pre_tag"].get("power_pool_correlation") == "DEFER_IF_UNAVAILABLE":
            checks[name] = "DEFERRED"
        else:
            checks[name] = "MISSING"
            errors.append("MISSING_REQUIRED_FINAL_CHECK" if phase == "FINAL" else "MISSING_REQUIRED_CHECK")
    values = set(checks.values())
    if "FAIL" in values:
        status = "FAIL"
    elif "PENDING" in values:
        status = "PENDING"
    elif "WARNING" in values:
        status = "WARNING"
    elif "MISSING" in values:
        status = "PENDING" if phase != "FINAL" else "UNKNOWN"
    elif "UNKNOWN" in values:
        status = "UNKNOWN"
    elif phase == "PRE_TAG" and "DEFERRED" in values:
        status = "PROVISIONAL_PASS"
    else:
        status = "PASS"
    return {"gate": "LIVE_BASE_GATE", "phase": phase, "status": status, "checks": checks, "errors": sorted(set(errors))}


def evaluate_live_theme_gate(results: Sequence[Mapping[str, Any]], phase: str, rules: Mapping[str, Any]) -> Dict[str, Any]:
    phase = phase.upper(); observed = _by_gate(results, "PPL_THEME")
    required = list(rules["current_theme"]["live_theme_checks"]["required"])
    checks = {name: observed.get(name, "MISSING") for name in required}
    values = set(checks.values())
    if "FAIL" in values:
        status = "FAIL"
    elif "PENDING" in values:
        status = "PENDING"
    elif "WARNING" in values:
        status = "WARNING"
    elif "MISSING" in values:
        status = "PENDING" if phase != "FINAL" else "UNKNOWN"
    elif "UNKNOWN" in values:
        status = "UNKNOWN"
    else:
        status = "PASS"
    return {"gate": "LIVE_THEME_GATE", "phase": phase, "status": status, "checks": checks,
            "errors": ["MISSING_REQUIRED_CHECK"] if "MISSING" in values else []}


def parse_check_payload(payload: Mapping[str, Any], *, phase: str, rules: Mapping[str, Any],
                        evidence_source: str = "SYNTHETIC_TEST") -> Dict[str, Any]:
    phase = phase.upper()
    if phase not in PHASES:
        raise ValueError(f"Invalid check phase: {phase}")
    raw_checks = _checks_from_payload(payload)
    if raw_checks is None:
        raw_checks = []
        parse_status = "INVALID_CHECK_PAYLOAD"
    else:
        parse_status = "PARSED"
    results = [parse_individual_check(item, phase, rules, evidence_source) for item in raw_checks if isinstance(item, Mapping)]
    # The live 2026 payload contains both HIGH_TURNOVER (the regular upper
    # bound) and HT_TURNOVER (the theme lower bound). Older fixtures used
    # HIGH_TURNOVER alone for the theme check, so retain that v1 behavior when
    # the structural HT_TURNOVER marker is absent.
    live_ht_shape=any(_key(x.get("name"))=="HT_TURNOVER" for x in raw_checks if isinstance(x,Mapping))
    if live_ht_shape:
        for item in results:
            if _key(item.get("raw_name"))=="HIGH_TURNOVER":
                item.update({"normalized_name":"TURNOVER","category":"PPL_BASE","mapping_suggestion":None})
    base = evaluate_live_base_gate(results, phase, rules)
    theme = evaluate_live_theme_gate(results, phase, rules)
    explicit_pending = [x["normalized_name"] for x in results if x["normalized_result"] == "PENDING"]
    pending_names = sorted(set(explicit_pending + [k for g in (base, theme) for k,v in g["checks"].items() if v == "MISSING"]))
    unknown_names = sorted({str(x["raw_name"]) for x in results if x["normalized_name"] == "UNKNOWN"})
    # Missing expected gates make the gate verdict UNKNOWN, but do not mean the
    # server response itself is still PENDING. Only an empty payload or an
    # explicit PENDING result should trigger another semantic GET.
    if not raw_checks or explicit_pending:
        session_semantic_status = "PENDING"
    else:
        session_semantic_status = "RESOLVED"
    pre_pass = phase == "PRE_TAG" and base["status"] in {"PASS", "PROVISIONAL_PASS"} and theme["status"] == "PASS"
    final_pass = phase == "FINAL" and base["status"] == theme["status"] == "PASS"
    return {
        "phase": phase, "parse_status": parse_status, "results": results,
        "base_gate": base, "theme_gate": theme, "pending_check_names": pending_names,
        "unknown_check_names": unknown_names, "unknown_check_count": len(unknown_names),
        "session_semantic_status": session_semantic_status,
        "pre_tag_check_pass": pre_pass, "final_check_pass": final_pass,
        "check_parser_version": CHECK_PARSER_VERSION, "check_alias_version": CHECK_ALIAS_VERSION,
        "evidence_source": evidence_source,
    }


def parse_response_text(text: str, *, phase: str, rules: Mapping[str, Any], evidence_source: str) -> Dict[str, Any]:
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "phase": phase, "parse_status": "JSON_DECODE_ERROR", "results": [],
            "base_gate": {"status": "PENDING"}, "theme_gate": {"status": "PENDING"},
            "pending_check_names": [], "unknown_check_names": [], "unknown_check_count": 0,
            "session_semantic_status": "TRANSIENT_ERROR", "error_type": "JSON_DECODE_ERROR",
            "error_nature": "TRANSIENT", "message": str(exc),
            "check_parser_version": CHECK_PARSER_VERSION, "check_alias_version": CHECK_ALIAS_VERSION,
            "evidence_source": evidence_source,
        }
    if not isinstance(payload, Mapping):
        return parse_check_payload({}, phase=phase, rules=rules, evidence_source=evidence_source)
    result = parse_check_payload(payload, phase=phase, rules=rules, evidence_source=evidence_source)
    result["raw_payload"] = payload
    return result


def build_check_summary(parsed: Mapping[str, Any]) -> Dict[str, Any]:
    important = {x["normalized_name"]: {
        "status": x["normalized_result"], "value": x["raw_value"], "limit": x["raw_limit"]
    } for x in parsed.get("results", []) if x["category"] in {"PPL_BASE", "PPL_THEME"}}
    other_fails = [x["raw_name"] for x in parsed.get("results", [])
                   if x["category"] not in {"PPL_BASE", "PPL_THEME"} and x["normalized_result"] == "FAIL"]
    return {
        "status": parsed.get("session_semantic_status"),
        "base_gate": parsed.get("base_gate", {}).get("status"),
        "theme_gate": parsed.get("theme_gate", {}).get("status"),
        "important_checks": important,
        "other_check_fail_count": len(other_fails), "important_other_checks": other_fails[:5],
        "unknown_check_count": parsed.get("unknown_check_count", 0),
        "unknown_check_names": parsed.get("unknown_check_names", [])[:10],
    }
