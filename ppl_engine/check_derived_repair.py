"""Check-derived Repair Planning for Production research.

Runs strictly AFTER PRE_TAG_CHECK_COMPLETE. It reads already-persisted Check
Parser results (ppl_check_results + ppl_check_sessions) and, for candidates that
pass the local gate but carry a genuine HIGH_TURNOVER_RETURNS_RATIO WARNING/FAIL,
retains the diagnostic/telemetry evidence. HT-ratio automatic repair is retired,
so this layer now emits no target_tvr/signal-horizon proposal. It never
simulates, POSTs, or consumes Repair Reserve.

Phase 1 scope: HIGH_TURNOVER_RETURNS_RATIO (canonical: HT_HIGH_TURNOVER_RETURNS_RATIO).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .diagnosis import DIAGNOSIS_RULE_VERSION, evaluate_local_pre_gate
from .repair_engine import RESCUE_STRATEGIES, SAME_FAMILY_MICRO_TUNE, parameter_change_summary, plan_repairs
from .audit_log import audit_event

HT_RATIO_CANONICAL = "HIGH_TURNOVER_RETURNS_RATIO"
HT_RATIO_FAILURE = "HT_RETURNS_RATIO_FAIL"
CHECK_DERIVED_SOURCE_PHASE = "PRE_TAG_CHECK"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_resolved_check_facts(store: Any, run_id: str) -> List[Dict[str, Any]]:
    """Return one fact bundle per resolved PRE_TAG session with its check results."""
    with store.connect() as conn:
        sessions = conn.execute(
            """SELECT * FROM ppl_check_sessions
               WHERE run_id=? AND phase='PRE_TAG' AND session_status='RESOLVED'
               ORDER BY updated_at, check_session_id""",
            (run_id,),
        ).fetchall()
        facts = []
        for s in sessions:
            results = conn.execute(
                """SELECT * FROM ppl_check_results WHERE check_session_id=?
                   ORDER BY normalized_name, check_result_id""",
                (s["check_session_id"],),
            ).fetchall()
            facts.append({"session": dict(s), "results": [dict(r) for r in results]})
        return facts


def _check_map(results: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Map by normalized_name; keep the highest-severity result for a duplicate name."""
    rank = {"FAIL": 4, "WARNING": 3, "PENDING": 3, "UNKNOWN": 2, "PASS": 1, "NOT_APPLICABLE": 0, "DEFERRED": 0}
    out: Dict[str, Dict[str, Any]] = {}
    for r in results:
        name = str(r.get("normalized_name"))
        if name not in out or rank.get(str(r.get("eligibility_outcome") or r.get("normalized_result")), 0) > rank.get(str(out[name].get("eligibility_outcome") or out[name].get("normalized_result")), 0):
            out[name] = r
    return out


def _numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        parsed = json.loads(str(value))
        return float(parsed) if isinstance(parsed, (int, float)) else None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def _alpha_metrics(alpha_db: Path, sim_keys: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    keys = list(dict.fromkeys(str(x) for x in sim_keys))
    if not keys:
        return {}
    import sqlite3
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        out = {}
        for start in range(0, len(keys), 500):
            chunk = keys[start:start + 500]
            marks = ",".join("?" for _ in chunk)
            for row in connection.execute(f"SELECT * FROM alpha_results WHERE sim_key IN ({marks})", chunk):
                out[str(row["sim_key"])] = dict(row)
        return out
    finally:
        connection.close()


def ht_ratio_eligibility(
    candidate: Mapping[str, Any], metrics: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]], rules: Mapping[str, Any],
) -> Dict[str, Any]:
    """Evaluate whether a candidate is a genuine HT-ratio repair target.

    Reuses the existing local-pre-gate and eligibility semantics; it does not
    invent a second PPL rule set and never fakes a PASS for PENDING base gates.
    """
    base = rules["power_pool_base_presets"]
    theme = rules["current_theme"]
    reasons: List[str] = []
    context: Dict[str, Any] = {}

    active_theme_required = {
        str(x).upper() for x in ((theme.get("live_theme_checks") or {}).get("required") or [])
    }
    if HT_RATIO_CANONICAL not in active_theme_required:
        return {
            "eligible": False,
            "reasons": ["THEME_SIGNAL_NOT_ACTIVE"],
            "context": {"active_theme_required": sorted(active_theme_required)},
            "ht_ratio_result": checks.get(HT_RATIO_CANONICAL),
            "local_gate": None,
        }

    local = evaluate_local_pre_gate(candidate, metrics, rules)
    context["local_gate"] = local["status"]
    if local["status"] != "PASS":
        reasons.append("LOCAL_GATE_NOT_PASS")

    sharpe = _numeric(metrics.get("sharpe"))
    context["sharpe"] = sharpe
    if sharpe is None or sharpe < float(base["sharpe"]["preset_min"]):
        reasons.append("SHARPE_BELOW_PPL_MIN")

    turnover = _numeric(metrics.get("turnover"))
    context["turnover"] = turnover
    base_min = float(base["turnover"]["preset_min"]); base_max = float(base["turnover"]["preset_max"])
    theme_min = float(theme["local_preconditions"]["high_turnover"]["turnover"]["preset_min"])
    if turnover is None:
        reasons.append("TURNOVER_MISSING")
    else:
        if not (base_min <= turnover <= base_max):
            reasons.append("TURNOVER_OUTSIDE_BASE_RANGE")
        if turnover < theme_min:
            reasons.append("TURNOVER_BELOW_THEME_MIN")

    sub = checks.get("SUB_UNIVERSE")
    context["sub_universe"] = (sub or {}).get("eligibility_outcome")
    if sub and str(sub.get("eligibility_outcome")) == "FAIL":
        reasons.append("SUB_UNIVERSE_FAIL")

    pp = checks.get("POWER_POOL_CORRELATION")
    context["pp_corr"] = (pp or {}).get("eligibility_outcome")
    if pp and str(pp.get("eligibility_outcome")) == "FAIL":
        reasons.append("PP_CORRELATION_FAIL")

    ht = checks.get(HT_RATIO_CANONICAL)
    if ht is None:
        reasons.append("HT_RATIO_CHECK_MISSING")
    else:
        outcome = str(ht.get("eligibility_outcome") or ht.get("normalized_result") or "UNKNOWN").upper()
        raw_result = str(ht.get("raw_result") or "").upper()
        value = _numeric(ht.get("raw_value_json"))
        # Prefer the live raw_limit from the Check Result; fall back to the
        # configured theme preset only when the live limit is absent. Never
        # hardcode a fixed threshold in Production judgment.
        limit = _numeric(ht.get("raw_limit_json"))
        limit_source = "LIVE_RAW_LIMIT" if limit is not None else "PRESET_FALLBACK"
        if limit is None:
            preset = theme.get("live_theme_checks", {}).get("high_turnover_returns_ratio", {}).get("preset_min")
            limit = _numeric(preset)
        context["ht_ratio_outcome"] = outcome
        context["ht_ratio_value"] = value
        context["ht_ratio_limit"] = limit
        context["ht_ratio_limit_source"] = limit_source
        if raw_result == "PENDING":
            reasons.append("HT_RATIO_RAW_PENDING")
        elif outcome not in {"WARNING", "FAIL"}:
            reasons.append("HT_RATIO_NOT_WARNING_OR_FAIL")
        elif value is not None and limit is not None and value >= limit:
            reasons.append("HT_RATIO_THRESHOLD_ALREADY_MET")

    eligible = not reasons
    return {"eligible": eligible, "reasons": reasons, "context": context,
            "ht_ratio_result": ht, "local_gate": local}


def _build_analysis_candidate(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    analysis = dict(candidate)
    analysis["data_field_count_estimate"] = analysis.get("data_field_count_estimate") or 1
    analysis["pp_total_operator_count_estimate"] = analysis.get("pp_total_operator_count_estimate") or 0
    analysis.setdefault("theme_settings_match", True)
    analysis.setdefault("dataset_allowed", True)
    analysis.setdefault("structure_status", "ELIGIBLE")
    return analysis


def derive_check_repair_proposals(
    store: Any, config: Any, alpha_db: Path, run_id: str, *, persist: bool = True,
) -> Dict[str, Any]:
    """Generate Check-derived HT-ratio Repair Proposals (planning only, no POST)."""
    ctx = store.get_run(run_id)
    if not ctx:
        from .config import ConfigError
        raise ConfigError(f"Unknown run: {run_id}")

    facts = _load_resolved_check_facts(store, run_id)
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    registry = {}
    with store.connect() as conn:
        registry = {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}
        existing_signatures = {str(r[0]) for r in conn.execute(
            "SELECT repair_signature FROM ppl_repair_plans WHERE run_id=?", (run_id,)
        )}

    sim_keys = [candidates[s["session"]["candidate_id"]]["sim_key"] for s in facts
                if s["session"].get("candidate_id") in candidates]
    metrics_by_key = _alpha_metrics(alpha_db, sim_keys)

    proposals = []
    skipped = []
    for fact in facts:
        session = fact["session"]
        candidate_id = session.get("candidate_id")
        candidate = candidates.get(candidate_id)
        if candidate is None:
            skipped.append({"candidate_id": candidate_id, "reason": "CANDIDATE_MISSING"})
            continue
        checks = _check_map(fact["results"])
        metrics = metrics_by_key.get(candidate["sim_key"]) or {}
        analysis_candidate = _build_analysis_candidate(candidate)
        # Keep parent metrics in the diagnostic context. The retired HT-ratio
        # planner deliberately returns no automatic proposal.
        for key in ("sharpe", "fitness", "turnover", "returns", "margin"):
            if metrics.get(key) is not None:
                analysis_candidate[key] = metrics[key]
        eligibility = ht_ratio_eligibility(analysis_candidate, metrics, checks, config.rules)

        base_gate = session.get("base_gate_result")
        theme_gate = session.get("theme_gate_result")

        if not eligibility["eligible"]:
            skipped.append({
                "candidate_id": candidate_id, "alpha_id": candidate.get("alpha_id"),
                "reasons": eligibility["reasons"], "context": eligibility["context"],
                "base_gate_result": base_gate, "theme_gate_result": theme_gate,
            })
            continue

        diagnosis = {
            "primary_failure": HT_RATIO_FAILURE, "secondary_failures": [],
        }
        repair = plan_repairs(
            analysis_candidate, diagnosis, config.rules, registry=registry,
            existing_signatures=existing_signatures,
            repair_reserve_remaining=10**9,  # planning only; never constrains a proposal
        )
        generated = repair.get("plans", [])
        for item in generated:
            plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{item['repair_signature']}".encode()).hexdigest()[:24]
            diag_id = "diag_" + hashlib.sha256(
                _json({"run": run_id, "candidate": candidate_id, "phase": CHECK_DERIVED_SOURCE_PHASE, "failure": HT_RATIO_FAILURE})
                .encode()
            ).hexdigest()[:24]
            proposal = {
                "repair_plan_id": plan_id, "diagnosis_id": diag_id, "run_id": run_id,
                "parent_candidate_id": candidate_id, "alpha_id": candidate.get("alpha_id"),
                "target_failure": HT_RATIO_FAILURE, "repair_type": item.get("repair_type"),
                "repair_signature": item["repair_signature"], "repair_path": item.get("repair_path", []),
                "repair_depth": item.get("repair_depth", 1), "candidate_spec": item,
                "operator_requirements": item.get("operator_requirements", []),
                "operator_gate": item.get("operator_gate", {}),
                "plan_status": "PLANNED",
                "projected_new_posts": item.get("projected_new_post", 0),
                "base_gate_result": base_gate, "theme_gate_result": theme_gate,
                "ht_ratio_context": eligibility["context"],
            }
            if persist:
                with store.connect() as conn:
                    conn.execute(
                        """INSERT OR REPLACE INTO ppl_diagnoses(
                               diagnosis_id,run_id,candidate_id,alpha_id,source_phase,evidence_source,
                               primary_failure,secondary_failures_json,severity,repairability,root_cause,
                               metrics_snapshot_json,check_session_id,check_result_ids_json,
                               diagnosis_rule_version,created_at
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (diag_id, run_id, candidate_id, candidate.get("alpha_id"), CHECK_DERIVED_SOURCE_PHASE,
                         "LIVE_CHECK", HT_RATIO_FAILURE, _json([]), "MEDIUM", "REPAIRABLE", HT_RATIO_FAILURE,
                         _json({k: metrics.get(k) for k in ("sharpe", "fitness", "turnover", "returns", "margin")}),
                         session["check_session_id"],
                         _json([r["check_result_id"] for r in fact["results"] if str(r.get("normalized_name")) == HT_RATIO_CANONICAL]),
                         DIAGNOSIS_RULE_VERSION, _now()),
                    )
                    conn.execute(
                        """INSERT OR IGNORE INTO ppl_repair_plans(
                               repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
                               target_failure,repair_type,repair_signature,repair_path_json,repair_depth,
                               candidate_spec_json,operator_requirements_json,plan_status,
                               projected_new_posts,committed_posts,consumed_posts,blocked_reason,
                               created_at,updated_at
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (plan_id, diag_id, run_id, candidate_id,
                         candidate.get("root_candidate_id") or candidate_id,
                         HT_RATIO_FAILURE, item.get("repair_type"), item["repair_signature"],
                         _json(item.get("repair_path", [])), item.get("repair_depth", 1),
                         _json(item), _json(item.get("operator_requirements", [])),
                         "PLANNED", int(item.get("projected_new_post", 0)), 0, 0,
                         None, _now(), _now()),
                    )
                audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id,
                            repair_plan_id=plan_id, parent_candidate_id=candidate_id,
                            candidate_id=candidate_id, repair_strategy=item.get("repair_type"),
                            target_failure=HT_RATIO_FAILURE,
                            parameter_change=parameter_change_summary(item, candidate))
                if item.get("repair_type") in RESCUE_STRATEGIES:
                    audit_event(action="RESCUE_PLAN_CREATED", run_id=run_id,
                                repair_plan_id=plan_id, parent_candidate_id=candidate_id,
                                candidate_id=candidate_id, repair_strategy=item.get("repair_type"),
                                target_failure=HT_RATIO_FAILURE,
                                parameter_change=parameter_change_summary(item, candidate))
                existing_signatures.add(item["repair_signature"])
            proposals.append(proposal)

    return {
        "mode": "CHECK_DERIVED_REPAIR_PLANNING", "run_id": run_id,
        "resolved_sessions": len(facts), "proposals_generated": len(proposals),
        "skipped": skipped, "proposals": proposals,
        "simulation_posts": 0, "network_requests": 0, "check_requests": 0,
        "repair_budget_consumed": 0, "persisted": persist,
    }


def _latest_ht_ratio(store: Any, run_id: str, candidate_id: str) -> Optional[Dict[str, Any]]:
    """Read the latest RESOLVED PRE-TAG HT-ratio (value + limit) for a candidate."""
    with store.connect() as conn:
        row = conn.execute(
            """SELECT cr.raw_value_json, cr.raw_limit_json, cr.eligibility_outcome, cr.raw_result
               FROM ppl_check_results cr
               JOIN ppl_check_sessions cs ON cs.check_session_id = cr.check_session_id
               WHERE cs.run_id=? AND cs.candidate_id=? AND cs.phase='PRE_TAG'
                 AND cs.session_status='RESOLVED' AND cr.normalized_name=?
               ORDER BY cs.updated_at DESC, cr.check_result_id DESC LIMIT 1""",
            (run_id, candidate_id, HT_RATIO_CANONICAL),
        ).fetchone()
    if row is None:
        return None
    return {
        "value": _numeric(row["raw_value_json"]),
        "limit": _numeric(row["raw_limit_json"]),
        "outcome": row["eligibility_outcome"],
        "raw_result": row["raw_result"],
    }


def evaluate_ht_repair_outcome(
    store: Any, run_id: str, parent_candidate_id: str, child_candidate_id: str,
) -> Dict[str, Any]:
    """Research repair verdict comparing parent vs child HT-returns ratio.

    Reads persisted PRE-TAG check results only.  WARNING is never re-interpreted
    as FAIL here; the verdict is a research comparison, not a rewrite of the
    platform raw_result.  Verdicts: TARGET_PASS / IMPROVED / NO_IMPROVEMENT / WORSE.
    """
    parent = _latest_ht_ratio(store, run_id, parent_candidate_id)
    child = _latest_ht_ratio(store, run_id, child_candidate_id)
    result: Dict[str, Any] = {
        "run_id": run_id,
        "parent_candidate_id": parent_candidate_id,
        "child_candidate_id": child_candidate_id,
        "parent_ht_ratio": parent["value"] if parent else None,
        "child_ht_ratio": child["value"] if child else None,
        "live_limit": None,
        "delta_ht_ratio": None,
        "verdict": "UNKNOWN",
        "reason": "",
    }
    if parent is None:
        result["reason"] = "PARENT_HT_RATIO_MISSING"
        return result
    if child is None:
        result["reason"] = "CHILD_HT_RATIO_MISSING"
        return result
    pv, cv = parent["value"], child["value"]
    live_limit = child["limit"] if child["limit"] is not None else parent["limit"]
    result["live_limit"] = live_limit
    if pv is None or cv is None:
        result["reason"] = "HT_RATIO_VALUE_UNPARSEABLE"
        return result
    delta = round(cv - pv, 6)
    result["delta_ht_ratio"] = delta
    if live_limit is not None and cv >= live_limit:
        result["verdict"] = "TARGET_PASS"
    elif delta > 0:
        result["verdict"] = "IMPROVED"
    elif delta == 0:
        result["verdict"] = "NO_IMPROVEMENT"
    else:
        result["verdict"] = "WORSE"
    return result


def derive_same_family_micro_tune_plan(
    store: Any, config: Any, alpha_db: Path, run_id: str, parent_candidate_id: str, *,
    persist: bool = True,
) -> Dict[str, Any]:
    """Generate SAME_FAMILY_MICRO_TUNE proposals for a ts_mean parent (planning only).

    Keeps the parent transform family and micro-tunes the window (ts_mean 2 ->
    3/4/5). This only plans; it never simulates, never POSTs, and never consumes
    Repair Reserve. Each proposal is idempotent by UNIQUE(run_id, repair_signature).
    """
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    parent = candidates.get(parent_candidate_id)
    if parent is None:
        from .config import ConfigError
        raise ConfigError(f"SAME_FAMILY_PARENT_MISSING: {parent_candidate_id}")

    registry: Dict[str, str] = {}
    existing_signatures: set = set()
    with store.connect() as conn:
        registry = {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}
        existing_signatures = {str(r[0]) for r in conn.execute(
            "SELECT repair_signature FROM ppl_repair_plans WHERE run_id=?", (run_id,)
        )}

    metrics = _alpha_metrics(alpha_db, [parent["sim_key"]]).get(parent["sim_key"]) or {}
    analysis_candidate = _build_analysis_candidate(parent)
    for key in ("sharpe", "fitness", "turnover", "returns", "margin"):
        if metrics.get(key) is not None:
            analysis_candidate[key] = metrics[key]

    diagnosis = {"primary_failure": HT_RATIO_FAILURE, "secondary_failures": []}
    repair = plan_repairs(
        analysis_candidate, diagnosis, config.rules, registry=registry,
        existing_signatures=existing_signatures, repair_reserve_remaining=10**9,
        ht_strategy=SAME_FAMILY_MICRO_TUNE,
    )

    proposals = []
    for item in repair.get("plans", []):
        plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{item['repair_signature']}".encode()).hexdigest()[:24]
        diag_id = "diag_" + hashlib.sha256(
            _json({"run": run_id, "candidate": parent_candidate_id,
                   "phase": CHECK_DERIVED_SOURCE_PHASE, "failure": HT_RATIO_FAILURE,
                   "strategy": SAME_FAMILY_MICRO_TUNE})
            .encode()
        ).hexdigest()[:24]
        proposal = {
            "repair_plan_id": plan_id, "diagnosis_id": diag_id, "run_id": run_id,
            "parent_candidate_id": parent_candidate_id, "alpha_id": parent.get("alpha_id"),
            "target_failure": HT_RATIO_FAILURE, "repair_type": item.get("repair_type"),
            "repair_signature": item["repair_signature"], "repair_path": item.get("repair_path", []),
            "repair_depth": item.get("repair_depth", 1), "candidate_spec": item,
            "operator_requirements": item.get("operator_requirements", []),
            "operator_gate": item.get("operator_gate", {}),
            "plan_status": "PLANNED",
            "projected_new_posts": item.get("projected_new_post", 0),
        }
        if persist:
            with store.connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO ppl_diagnoses(
                           diagnosis_id,run_id,candidate_id,alpha_id,source_phase,evidence_source,
                           primary_failure,secondary_failures_json,severity,repairability,root_cause,
                           metrics_snapshot_json,check_session_id,check_result_ids_json,
                           diagnosis_rule_version,created_at
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (diag_id, run_id, parent_candidate_id, parent.get("alpha_id"), CHECK_DERIVED_SOURCE_PHASE,
                     "LIVE_CHECK", HT_RATIO_FAILURE, _json([]), "MEDIUM", "REPAIRABLE", HT_RATIO_FAILURE,
                     _json({k: metrics.get(k) for k in ("sharpe", "fitness", "turnover", "returns", "margin")}),
                     None, "[]", DIAGNOSIS_RULE_VERSION, _now()),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO ppl_repair_plans(
                           repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
                           target_failure,repair_type,repair_signature,repair_path_json,repair_depth,
                           candidate_spec_json,operator_requirements_json,plan_status,
                           projected_new_posts,committed_posts,consumed_posts,blocked_reason,
                           created_at,updated_at
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (plan_id, diag_id, run_id, parent_candidate_id,
                     parent.get("root_candidate_id") or parent_candidate_id,
                     HT_RATIO_FAILURE, item.get("repair_type"), item["repair_signature"],
                     _json(item.get("repair_path", [])), item.get("repair_depth", 1),
                     _json(item), _json(item.get("operator_requirements", [])),
                     "PLANNED", int(item.get("projected_new_post", 0)), 0, 0,
                     None, _now(), _now()),
                )
            audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id,
                        repair_plan_id=plan_id, parent_candidate_id=parent_candidate_id,
                        candidate_id=parent_candidate_id, repair_strategy=item.get("repair_type"),
                        target_failure=HT_RATIO_FAILURE,
                        parameter_change=parameter_change_summary(item, parent))
            if item.get("repair_type") in RESCUE_STRATEGIES:
                audit_event(action="RESCUE_PLAN_CREATED", run_id=run_id,
                            repair_plan_id=plan_id, parent_candidate_id=parent_candidate_id,
                            candidate_id=parent_candidate_id, repair_strategy=item.get("repair_type"),
                            target_failure=HT_RATIO_FAILURE,
                            parameter_change=parameter_change_summary(item, parent))
            existing_signatures.add(item["repair_signature"])
        proposals.append(proposal)

    return {
        "mode": "SAME_FAMILY_MICRO_TUNE_PLANNING", "run_id": run_id,
        "parent_candidate_id": parent_candidate_id, "parent_alpha_id": parent.get("alpha_id"),
        "strategy": SAME_FAMILY_MICRO_TUNE, "proposals_generated": len(proposals),
        "proposals": proposals, "stop_reason": repair.get("stop_reason"),
        "simulation_posts": 0, "network_requests": 0, "check_requests": 0,
        "repair_budget_consumed": 0, "persisted": persist,
    }
