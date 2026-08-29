"""Platform-driven Power Pool classification for V3.0.4b.

This module deliberately separates four concepts that the generic BRAIN
``/check`` response mixes together:

1. fixed Power Pool eligibility gates;
2. current-theme repair signals;
3. non-PPL diagnostics from Regular/other checks; and
4. ``MATCHES_THEMES`` as the final thematic outcome.

Platform PASS/WARNING/FAIL is preserved as authoritative platform fact. Numeric
value/limit pairs never rewrite that fact, but V3.0.4g may apply an explicit
local PPC quality strategy when deciding whether an Alpha is worth keeping.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import yaml

PPL_CLASSIFIER_VERSION = "V3_PPL_CLASS_004"

REPAIRABLE_STATUSES = {
    "PPL_FIXED_REPAIRABLE",
    "PPL_THEME_REPAIRABLE",
    "PPL_FIXED_AND_THEME_REPAIRABLE",
}


def _key(value: Any) -> str:
    import re
    return re.sub(r"[^A-Z0-9]+", "_", str(value or "").strip().upper()).strip("_")


def _float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return float(parsed) if isinstance(parsed, (int, float)) else None


def _json_value(row: Mapping[str, Any], key: str) -> Any:
    # Durable DB rows use *_json; freshly-parsed rows use raw_value/raw_limit.
    if key in row and row.get(key) is not None:
        return row.get(key)
    alt = key.replace("_json", "")
    return row.get(alt)


def platform_outcome(row: Optional[Mapping[str, Any]]) -> str:
    if not row:
        return "MISSING"
    return str(
        row.get("eligibility_outcome")
        or row.get("normalized_result")
        or row.get("raw_result")
        or row.get("result")
        or "UNKNOWN"
    ).upper()


def raw_check_name(row: Mapping[str, Any]) -> str:
    return _key(row.get("raw_name") or row.get("name") or row.get("normalized_name"))


def normalized_check_name(row: Mapping[str, Any]) -> str:
    return _key(row.get("normalized_name") or row.get("raw_name") or row.get("name"))


def load_ppl_classification_policy(project_dir: Path, round_policy_filename: str = "ppl_round_v3.yaml") -> Dict[str, Any]:
    path = Path(project_dir) / str(round_policy_filename)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    policy = dict(raw.get("ppl_classification") or {})
    if not policy:
        # Conservative fallback for callers/tests that do not load V3 round policy.
        policy = {
            "fixed_gates": {
                "sharpe_min": 1.0, "turnover_min": 0.01, "turnover_max": 0.70,
                "sub_universe_checks": ["LOW_SUB_UNIVERSE_SHARPE", "SUB_UNIVERSE"],
                "power_pool_correlation_checks": ["POWER_POOL_CORRELATION"],
            },
            "theme_specific": {
                "repair_signals": {},
                "capture_prefixes": ["HT_"],
                "observe_only": [
                    "HT_TURNOVER",
                    "HT_HIGH_TURNOVER_RETURNS_RATIO",
                    "MATCHES_CLASSIFICATION",
                ],
            },
            "final_theme_check": "MATCHES_THEMES",
            "non_ppl_diagnostics": [],
            "automation_ignored": ["POWER_POOL_DESCRIPTION_LENGTH", "POWER_POOL_DESCRIPTION_FORMAT", "PURE_POWER_POOL_THEME"],
            "manual_finalization": {
                "enabled": True,
                "description_checks": ["POWER_POOL_DESCRIPTION_LENGTH", "POWER_POOL_DESCRIPTION_FORMAT"],
                "pretag_theme_outcomes": ["WARNING", "PENDING"],
                "auto_refresh_every_batches": 10,
                "ppc_strategy": {"clean_max": 0.50, "mid_max": 0.65, "mid_min_sharpe": 2.00},
            },
            "repair_priority": {"high_gap_max": 0.05, "medium_gap_max": 0.10},
        }
    return policy


def load_ppl_classification_policy_for_config(config: Any) -> Dict[str, Any]:
    """Load the PPL classification policy appropriate for the active run profile.

    Legacy callers keep reading ``ppl_round_v3.yaml``.  V3.1 Continuous reads
    ``ppl_round_v31.yaml`` so future Qualification-only edits do not silently
    fall back to the frozen V3 policy.
    """
    plan = dict(getattr(config, "plan", {}) or {})
    run_profile = str(plan.get("run_profile") or "").upper()
    if run_profile == "CONTINUOUS_RESEARCH":
        # Freeze a single process-local snapshot. C3 may explicitly reload it at
        # a validated safe checkpoint; C2 must not hot-reload mid-batch.
        from .qualification_policy import load_qualification_policy_snapshot
        snapshot = load_qualification_policy_snapshot(Path(config.project_dir))
        return dict(snapshot.classification_policy)
    return load_ppl_classification_policy(Path(config.project_dir), "ppl_round_v3.yaml")


def _severity(outcome: str) -> int:
    return {"FAIL": 6, "WARNING": 5, "PENDING": 4, "UNKNOWN": 3, "PASS": 1, "NOT_APPLICABLE": 0}.get(outcome, 2)


def check_rows_by_raw_name(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Keep the highest-severity durable fact for every raw platform check name."""
    out: Dict[str, Dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        name = raw_check_name(row)
        if not name:
            continue
        if name not in out or _severity(platform_outcome(row)) > _severity(platform_outcome(out[name])):
            out[name] = row
    return out


def _first_check(by_raw: Mapping[str, Mapping[str, Any]], names: Sequence[str]) -> Optional[Dict[str, Any]]:
    for name in names:
        row = by_raw.get(_key(name))
        if row is not None:
            return dict(row)
    # Existing V3 databases sometimes only retain a useful normalized alias.
    wanted = {_key(x) for x in names}
    for row in by_raw.values():
        if normalized_check_name(row) in wanted:
            return dict(row)
    return None


def normalized_gap(value: Any, limit: Any, direction: str) -> Optional[float]:
    v, l = _float(value), _float(limit)
    if v is None or l is None or l == 0:
        return None
    direction = str(direction or "").upper()
    if direction == "MIN":
        return round((l - v) / abs(l), 6)
    if direction == "MAX":
        return round((v - l) / abs(l), 6)
    return None


def _blocker(*, check: str, group: str, row: Optional[Mapping[str, Any]], direction: str,
             fallback_limit: Any = None, failure: Optional[str] = None,
             source: str = "PLATFORM_CHECK") -> Dict[str, Any]:
    outcome = platform_outcome(row)
    value = _json_value(row or {}, "raw_value_json")
    live_limit = _json_value(row or {}, "raw_limit_json")
    limit = live_limit if _float(live_limit) is not None else fallback_limit
    gap = normalized_gap(value, limit, direction)
    return {
        "check": _key(check), "group": group, "platform_outcome": outcome,
        "raw_result": str((row or {}).get("raw_result") or (row or {}).get("result") or "").upper(),
        "raw_value": _float(value), "raw_limit": _float(limit),
        "live_limit_present": _float(live_limit) is not None,
        "direction": direction, "normalized_gap": gap,
        "failure": failure or _key(check), "source": source,
        "repairable": True,
    }


def _diagnostic(row: Mapping[str, Any], category: str = "NON_PPL_DIAGNOSTIC") -> Dict[str, Any]:
    return {
        "check": raw_check_name(row), "platform_outcome": platform_outcome(row),
        "raw_value": _float(_json_value(row, "raw_value_json")),
        "raw_limit": _float(_json_value(row, "raw_limit_json")),
        "category": category, "diagnostic_only": True,
    }


def _repair_priority(blockers: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> Dict[str, Any]:
    if not blockers:
        return {"repair_priority": "NONE", "max_repair_gap": None, "unquantified_repair_blockers": 0}
    gaps = [float(x["normalized_gap"]) for x in blockers if x.get("normalized_gap") is not None]
    unquantified = sum(x.get("normalized_gap") is None for x in blockers)
    if not gaps or unquantified:
        return {"repair_priority": "LOW", "max_repair_gap": max(gaps) if gaps else None,
                "unquantified_repair_blockers": unquantified}
    max_gap = max(gaps)
    cfg = dict(policy.get("repair_priority") or {})
    high = float(cfg.get("high_gap_max", 0.05))
    medium = float(cfg.get("medium_gap_max", 0.10))
    if max_gap <= high:
        priority = "HIGH"
    elif max_gap <= medium:
        priority = "MEDIUM"
    else:
        priority = "LOW"
    return {"repair_priority": priority, "max_repair_gap": round(max_gap, 6),
            "unquantified_repair_blockers": unquantified}


def classify_ppl_candidate(candidate: Mapping[str, Any], metrics: Mapping[str, Any],
                           check_rows: Iterable[Mapping[str, Any]],
                           policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Classify one Alpha without confusing Regular checks with PPL blockers."""
    by_raw = check_rows_by_raw_name(check_rows)
    fixed_cfg = dict(policy.get("fixed_gates") or {})
    theme_cfg = dict(policy.get("theme_specific") or {})
    repair_signals = {_key(k): dict(v or {}) for k, v in dict(theme_cfg.get("repair_signals") or {}).items()}
    ignored = {_key(x) for x in policy.get("automation_ignored", [])}
    non_ppl = {_key(x) for x in policy.get("non_ppl_diagnostics", [])}
    final_theme_name = _key(policy.get("final_theme_check") or "MATCHES_THEMES")

    sim_complete = str(candidate.get("simulation_status") or "").upper() == "COMPLETE"
    structure_ok = str(candidate.get("structure_status") or "ELIGIBLE").upper() != "INVALID"
    sharpe = _float(metrics.get("sharpe"))
    turnover = _float(metrics.get("turnover"))
    sharpe_min = float(fixed_cfg.get("sharpe_min", 1.0))
    turnover_min = float(fixed_cfg.get("turnover_min", 0.01))
    turnover_max = float(fixed_cfg.get("turnover_max", 0.70))

    fixed_blockers: List[Dict[str, Any]] = []
    theme_blockers: List[Dict[str, Any]] = []
    fixed_unresolved: List[str] = []
    diagnostics: List[Dict[str, Any]] = []
    unmapped_theme_signals: List[Dict[str, Any]] = []
    structural_diagnostics: List[Dict[str, Any]] = []

    terminal = False
    terminal_reasons: List[str] = []
    if sharpe is not None and sharpe < sharpe_min:
        terminal = True
        terminal_reasons.append("SHARPE_BELOW_PPL_MIN")
    elif sharpe is None and sim_complete:
        fixed_unresolved.append("SHARPE_MISSING")

    if turnover is None and sim_complete:
        fixed_unresolved.append("TURNOVER_MISSING")
    elif turnover is not None and turnover > turnover_max:
        fixed_blockers.append({
            "check": "TURNOVER", "group": "PPL_FIXED_GATE", "platform_outcome": "LOCAL_FAIL",
            "raw_value": turnover, "raw_limit": turnover_max, "direction": "MAX",
            "normalized_gap": normalized_gap(turnover, turnover_max, "MAX"),
            "failure": "TURNOVER_ABOVE_BASE_MAX", "source": "SIMULATION_METRIC", "repairable": True,
        })
    elif turnover is not None and turnover < turnover_min:
        fixed_blockers.append({
            "check": "TURNOVER", "group": "PPL_FIXED_GATE", "platform_outcome": "LOCAL_FAIL",
            "raw_value": turnover, "raw_limit": turnover_min, "direction": "MIN",
            "normalized_gap": normalized_gap(turnover, turnover_min, "MIN"),
            "failure": "TURNOVER_BELOW_BASE_MIN", "source": "SIMULATION_METRIC", "repairable": True,
        })

    sub = _first_check(by_raw, fixed_cfg.get("sub_universe_checks") or ["LOW_SUB_UNIVERSE_SHARPE", "SUB_UNIVERSE"])
    if sub is None:
        if by_raw:
            fixed_unresolved.append("SUB_UNIVERSE_MISSING")
    elif platform_outcome(sub) not in {"PASS", "NOT_APPLICABLE"}:
        fixed_blockers.append(_blocker(
            check=raw_check_name(sub), group="PPL_FIXED_GATE", row=sub, direction="MIN",
            failure="SUB_UNIVERSE_FAIL",
        ))

    pp = _first_check(by_raw, fixed_cfg.get("power_pool_correlation_checks") or ["POWER_POOL_CORRELATION"])
    if pp is None:
        if by_raw:
            fixed_unresolved.append("POWER_POOL_CORRELATION_MISSING")
    elif platform_outcome(pp) not in {"PASS", "NOT_APPLICABLE"}:
        fixed_blockers.append(_blocker(
            check=raw_check_name(pp), group="PPL_FIXED_GATE", row=pp, direction="MAX",
            failure="PP_CORRELATION_FAIL",
        ))

    # Operator/field limits are retained as an insurance diagnostic only for now;
    # the current generator keeps them in-range and V3.0.4b does not auto-repair them.
    op_count = _float(candidate.get("pp_total_operator_count_estimate"))
    field_count = _float(candidate.get("data_field_count_estimate"))
    op_max = _float(fixed_cfg.get("operator_count_max", 8))
    field_max = _float(fixed_cfg.get("data_field_count_max", 3))
    if op_count is not None and op_max is not None and op_count > op_max:
        structural_diagnostics.append({"check": "OPERATOR_COUNT", "value": op_count, "limit": op_max, "repairable": False})
    if field_count is not None and field_max is not None and field_count > field_max:
        structural_diagnostics.append({"check": "DATA_FIELD_COUNT", "value": field_count, "limit": field_max, "repairable": False})

    for name, spec in repair_signals.items():
        row = by_raw.get(name)
        if row is None:
            continue
        outcome = platform_outcome(row)
        if outcome in {"WARNING", "FAIL"}:
            theme_blockers.append(_blocker(
                check=name, group="THEME_SPECIFIC_SIGNAL", row=row,
                direction=str(spec.get("direction") or "MIN").upper(),
                fallback_limit=spec.get("fallback_limit"),
                failure=str(spec.get("failure") or (
                    "HT_RETURNS_RATIO_FAIL" if name == "HT_HIGH_TURNOVER_RETURNS_RATIO" else name + "_FAIL"
                )),
            ))

    capture_prefixes = tuple(_key(x) for x in theme_cfg.get("capture_prefixes", ["HT_"]))
    configured = set(repair_signals)
    observe_only = {_key(x) for x in theme_cfg.get("observe_only", [])}
    for name, row in by_raw.items():
        if name in configured or name in observe_only:
            continue
        if any(name.startswith(prefix) for prefix in capture_prefixes):
            unmapped_theme_signals.append(_diagnostic(row, "UNMAPPED_THEME_SIGNAL"))

    final_theme = by_raw.get(final_theme_name)
    # Existing DB rows preserve raw_name=MATCHES_THEMES even when normalized_name=THEME_MATCH.
    if final_theme is None:
        final_theme = _first_check(by_raw, [final_theme_name, "THEME_MATCH"])
    final_theme_outcome = platform_outcome(final_theme)

    manual_cfg = dict(policy.get("manual_finalization") or {})
    manual_enabled = bool(manual_cfg.get("enabled", True))
    description_check_names = [_key(x) for x in manual_cfg.get("description_checks", [
        "POWER_POOL_DESCRIPTION_LENGTH", "POWER_POOL_DESCRIPTION_FORMAT",
    ])]
    pretag_theme_outcomes = {str(x).upper() for x in manual_cfg.get("pretag_theme_outcomes", ["WARNING", "PENDING"])}
    description_pending_checks: List[Dict[str, Any]] = []
    for name in description_check_names:
        row = by_raw.get(name)
        if row is not None and platform_outcome(row) not in {"PASS", "NOT_APPLICABLE"}:
            description_pending_checks.append(_diagnostic(row, "MANUAL_DESCRIPTION_PENDING"))
    description_pending = bool(description_pending_checks)

    # Local portfolio-quality strategy layered on top of the platform fact.
    # The platform POWER_POOL_CORRELATION outcome is preserved verbatim; this
    # policy only decides whether we want to spend manual-finalization effort on
    # an Alpha.  It never rewrites platform PASS into FAIL.
    ppc_cfg = dict(manual_cfg.get("ppc_strategy") or {})
    ppc_clean_max = float(ppc_cfg.get("clean_max", 0.50))
    ppc_mid_max = float(ppc_cfg.get("mid_max", 0.65))
    ppc_mid_min_sharpe = float(ppc_cfg.get("mid_min_sharpe", 2.00))
    pp_value = _float(_json_value(pp or {}, "raw_value_json"))
    pp_platform_outcome = platform_outcome(pp)
    if pp_value is None:
        ppc_policy_band = "UNRESOLVED"
        ppc_strategy_result = "PPC_VALUE_UNRESOLVED"
    elif pp_value <= ppc_clean_max:
        ppc_policy_band = "CLEAN"
        ppc_strategy_result = "PASS_CLEAN_PPC"
    elif pp_value < ppc_mid_max:
        ppc_policy_band = "MID"
        ppc_strategy_result = (
            "PASS_MID_PPC_SHARPE_GT_2" if sharpe is not None and sharpe > ppc_mid_min_sharpe
            else "REJECT_MID_PPC_SHARPE_NOT_GT_2"
        )
    else:
        ppc_policy_band = "HIGH"
        ppc_strategy_result = "REJECT_PPC_GE_0_65"

    pre_strategy_manual_eligible = bool(
        manual_enabled and by_raw and not fixed_blockers and not fixed_unresolved
        and description_pending and final_theme_outcome in pretag_theme_outcomes
    )

    claimed = set(configured) | observe_only | ignored | non_ppl | {final_theme_name}
    claimed.update(_key(x) for x in fixed_cfg.get("sub_universe_checks", []))
    claimed.update(_key(x) for x in fixed_cfg.get("power_pool_correlation_checks", []))
    for name, row in by_raw.items():
        if name in ignored or any(name.startswith(p) for p in capture_prefixes):
            continue
        if name in non_ppl:
            if platform_outcome(row) not in {"PASS", "NOT_APPLICABLE"}:
                diagnostics.append(_diagnostic(row))
            continue
        if name in claimed:
            continue
        # Conservative default: an unmapped generic /check FAIL/WARNING is a
        # diagnostic, never an automatic PPL blocker.
        if platform_outcome(row) not in {"PASS", "NOT_APPLICABLE"}:
            diagnostics.append(_diagnostic(row, "UNMAPPED_NON_PPL_DIAGNOSTIC"))

    repair_blockers = fixed_blockers + theme_blockers
    priority = _repair_priority(repair_blockers, policy)

    if not sim_complete:
        status = "UNCLASSIFIED"
        reasons = ["SIMULATION_NOT_COMPLETE"]
    elif not structure_ok:
        status = "PPL_TERMINAL_FAIL"
        reasons = ["STRUCTURAL_INVALID"]
        priority = _repair_priority([], policy)
        repair_blockers = []
    elif terminal:
        status = "PPL_TERMINAL_FAIL"
        reasons = list(terminal_reasons)
        priority = _repair_priority([], policy)
        repair_blockers = []
    elif pp_value is not None and pp_value >= ppc_mid_max:
        # User strategy: correlation this high is not worth keeping even when
        # the platform grants a Sharpe-based exemption. Preserve the platform
        # PASS/WARNING/FAIL fact separately; this is a local strategy rejection.
        status = "PPL_STRATEGY_REJECT_HIGH_PPC"
        reasons = ["PPC_AT_OR_ABOVE_STRATEGY_MAX"]
        priority = _repair_priority([], policy)
        repair_blockers = []
    elif fixed_blockers and theme_blockers:
        status = "PPL_FIXED_AND_THEME_REPAIRABLE"
        reasons = ["FIXED_AND_THEME_REPAIRABLE"]
    elif fixed_blockers:
        status = "PPL_FIXED_REPAIRABLE"
        reasons = ["FIXED_GATE_REPAIRABLE"]
    elif final_theme_outcome == "PASS":
        # Final platform theme match is authoritative for eligibility, then the
        # local PPC strategy decides whether this Alpha is worth keeping.
        if pp_value is None:
            status = "PPL_CHECK_UNRESOLVED"
            reasons = ["PPC_VALUE_MISSING_FOR_STRATEGY"]
        elif ppc_policy_band == "MID" and not (sharpe is not None and sharpe > ppc_mid_min_sharpe):
            status = "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE"
            reasons = ["MID_PPC_REQUIRES_SHARPE_GT_2"]
        else:
            status = "PPL_TECHNICALLY_READY"
            reasons = ["PLATFORM_ACTIVE_THEME_MATCH"]
            theme_blockers = []
            repair_blockers = []
            priority = _repair_priority([], policy)
    elif theme_blockers:
        status = "PPL_THEME_REPAIRABLE"
        reasons = ["THEME_SIGNAL_REPAIRABLE"]
    elif pre_strategy_manual_eligible:
        # PRE-TAG /check commonly returns MATCHES_THEMES=WARNING while the
        # Power Pool Description is still missing/invalid.  Apply the local PPC
        # quality strategy before exposing it in the manual-finalization queue.
        if pp_value is None:
            status = "PPL_CHECK_UNRESOLVED"
            reasons = ["PPC_VALUE_MISSING_FOR_STRATEGY"]
        elif ppc_policy_band == "MID" and not (sharpe is not None and sharpe > ppc_mid_min_sharpe):
            status = "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE"
            reasons = ["MID_PPC_REQUIRES_SHARPE_GT_2"]
        else:
            status = "PPL_READY_FOR_MANUAL_FINALIZATION"
            reasons = ["DESCRIPTION_PENDING_BEFORE_FINAL_THEME_CHECK"]
            priority = _repair_priority([], policy)
    elif by_raw:
        status = "PPL_THEME_UNRESOLVED"
        reasons = ["ACTIVE_THEME_NOT_MATCHED_NO_CONFIGURED_REPAIR_SIGNAL"]
    else:
        status = "PPL_CHECK_UNRESOLVED"
        reasons = ["NO_RESOLVED_CHECK_FACTS"]

    # Fixed gate missing facts are audit warnings, not invented blockers.
    if fixed_unresolved:
        reasons.append("FIXED_GATE_FACTS_MISSING")

    p = priority["repair_priority"]
    if status == "PPL_TECHNICALLY_READY":
        evidence_label = "PPL_SUCCESS"
    elif status in REPAIRABLE_STATUSES and p == "HIGH":
        evidence_label = "STRONG_NEAR_PASS"
    elif status in REPAIRABLE_STATUSES and p == "MEDIUM":
        evidence_label = "NEAR_PASS"
    else:
        evidence_label = status

    return {
        "classification": status,
        "ppl_status": status,
        "classifier_version": PPL_CLASSIFIER_VERSION,
        "evidence_label": evidence_label,
        "repair_priority": p,
        "repair_drivers": [x.get("failure") for x in repair_blockers],
        "primary_failure": (repair_blockers[0].get("failure") if repair_blockers else None),
        "repair_blockers": repair_blockers,
        "blockers": repair_blockers,  # compatibility for rescue helpers
        "fixed_blockers": fixed_blockers,
        "theme_blockers": theme_blockers,
        "blocker_count": len(repair_blockers),
        "max_normalized_gap": priority["max_repair_gap"],
        "max_blocker_normalized_gap": priority["max_repair_gap"],
        "unquantified_repair_blockers": priority["unquantified_repair_blockers"],
        "final_theme_check": final_theme_name,
        "final_theme_outcome": final_theme_outcome,
        "theme_match": (
            True if final_theme_outcome == "PASS"
            else None if status == "PPL_READY_FOR_MANUAL_FINALIZATION"
            else False if final_theme_outcome in {"WARNING", "FAIL"}
            else None
        ),
        "manual_finalization_required": status == "PPL_READY_FOR_MANUAL_FINALIZATION",
        "manual_finalization_candidate_pre_strategy": pre_strategy_manual_eligible,
        "ppc_value": pp_value,
        "platform_ppc_outcome": pp_platform_outcome,
        "ppc_policy_band": ppc_policy_band,
        "ppc_strategy_result": ppc_strategy_result,
        "ppc_strategy_clean_max": ppc_clean_max,
        "ppc_strategy_mid_max": ppc_mid_max,
        "ppc_strategy_mid_min_sharpe": ppc_mid_min_sharpe,
        "description_pending": description_pending,
        "description_pending_checks": description_pending_checks,
        "unmapped_theme_signals": unmapped_theme_signals,
        "quality_diagnostics": diagnostics,
        "structural_diagnostics": structural_diagnostics,
        "fixed_unresolved": fixed_unresolved,
        "reasons": reasons,
        "sharpe": sharpe, "turnover": turnover,
    }
