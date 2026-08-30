"""Durable, sequential HIGH_TURNOVER repair planning.

This module only plans the next eligible stage.  It never simulates, submits,
or mutates budget facts.  Historical HT-ratio and turnover-window rows remain
untouched and are retired at selection/execution boundaries elsewhere.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from .audit_log import audit_event
from .diagnosis import DIAGNOSIS_RULE_VERSION
from .repair_engine import (
    TURNOVER_DECAY_STEP_1, TURNOVER_DECAY_STEP_2, TURNOVER_HUMP,
    TURNOVER_STAGED_POLICY, TURNOVER_STAGED_STRATEGIES,
    canonical_high_turnover_evidence_state, operator_gate,
    repair_signature, turnover_stage_spec, validate_turnover_hump_structure,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def latest_turnover_check_state(store: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    """Read only the latest resolved PRE_TAG session for a stage child."""
    with store.connect() as conn:
        latest = conn.execute(
            """SELECT * FROM ppl_check_sessions
               WHERE run_id=? AND candidate_id=? AND phase='PRE_TAG'
               ORDER BY updated_at DESC, check_session_id DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
        if latest is None:
            return {"state": "INSUFFICIENT", "reason": "PRETAG_SESSION_MISSING", "refresh_allowed": True}
        if str(latest["session_status"] or "").upper() != "RESOLVED":
            return {"state": "INSUFFICIENT", "reason": "LATEST_PRETAG_SESSION_UNRESOLVED",
                    "check_session_id": latest["check_session_id"], "refresh_allowed": False}
        session = latest
        rows = conn.execute(
            """SELECT * FROM ppl_check_results WHERE check_session_id=?
               ORDER BY check_result_id""", (session["check_session_id"],),
        ).fetchall()
    other_blockers = [dict(row) for row in rows if (
        str(row["category"] or "").upper() == "PPL_BASE"
        and str(row["normalized_name"] or "").upper() != "TURNOVER"
        and str(row["eligibility_outcome"] or row["normalized_result"] or "").upper() == "FAIL"
    )]
    if other_blockers:
        return {"state": "OTHER_BLOCKER", "reason": "NEW_PRIMARY_BLOCKER_PRESENT",
                "check_session_id": session["check_session_id"],
                "other_blockers": [x.get("normalized_name") for x in other_blockers]}
    states = [(canonical_high_turnover_evidence_state(dict(row)), dict(row)) for row in rows]
    matched = [(state, row) for state, row in states if state in {"BLOCKED", "CLEAR"}]
    if not matched:
        return {"state": "INSUFFICIENT", "reason": "CANONICAL_HIGH_TURNOVER_ROW_MISSING",
                "check_session_id": session["check_session_id"], "refresh_allowed": True}
    state, row = matched[-1]
    return {
        "state": state, "reason": "CANONICAL_HIGH_TURNOVER_EVIDENCE",
        "check_session_id": session["check_session_id"],
        "check_result_id": row.get("check_result_id"), "evidence": row,
    }


def _stage_context(plan: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        spec = json.loads(plan.get("candidate_spec_json") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    context = spec.get("turnover_staged_context")
    return dict(context) if isinstance(context, Mapping) else {}


def _bootstrap_stage1_plans(store: Any, run_id: str, round_id: str) -> list:
    """Create V1 Stage-1 plans from already-durable local turnover diagnoses.

    This lets an existing Round adopt the new strategy without rewriting or
    reactivating its historical turnover-window plans.  One deterministic
    origin is admitted per signal family.
    """
    candidates = {str(c["candidate_id"]): c for c in store.load_candidates(run_id)}
    existing_families = set()
    for plan in store.load_repair_plans(run_id):
        ctx = _stage_context(plan)
        if ctx.get("policy") == TURNOVER_STAGED_POLICY and ctx.get("origin_signal_family"):
            existing_families.add(str(ctx["origin_signal_family"]))
    with store.connect() as conn:
        diagnoses = [dict(r) for r in conn.execute(
            """SELECT * FROM ppl_diagnoses WHERE run_id=?
               AND primary_failure='TURNOVER_ABOVE_BASE_MAX'
               AND source_phase='SIMULATION' AND evidence_source='LIVE_SIMULATION'
               ORDER BY created_at DESC, diagnosis_id DESC""", (run_id,)
        )]
    by_family: Dict[str, list] = {}
    for diagnosis in diagnoses:
        parent = candidates.get(str(diagnosis.get("candidate_id") or ""))
        if not parent:
            continue
        family = str(parent.get("signal_family") or "")
        if not family or family in existing_families:
            continue
        path_raw = parent.get("repair_path_json") or parent.get("repair_path") or "[]"
        try:
            path = json.loads(path_raw) if isinstance(path_raw, str) else list(path_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            path = []
        if any(any(stage in str(part) for stage in TURNOVER_STAGED_STRATEGIES) for part in path):
            continue
        metrics = json.loads(diagnosis.get("metrics_snapshot_json") or "{}")
        sharpe = float(metrics.get("sharpe")) if metrics.get("sharpe") is not None else -999.0
        by_family.setdefault(family, []).append((sharpe, str(parent["candidate_id"]), diagnosis, parent, path))
    created = []
    for family in sorted(by_family):
        _, _, diagnosis, parent, path = sorted(by_family[family], key=lambda x: (-x[0], x[1]))[0]
        spec = turnover_stage_spec(parent, 1)
        spec["turnover_staged_context"]["round_id"] = round_id
        spec["repair_path"] = path + [f"TURNOVER_ABOVE_BASE_MAX:{TURNOVER_DECAY_STEP_1}"]
        plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{spec['repair_signature']}".encode()).hexdigest()[:24]
        now = _now()
        with store.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO ppl_repair_plans(
                       repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
                       target_failure,repair_type,repair_signature,repair_path_json,repair_depth,
                       candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,
                       committed_posts,consumed_posts,blocked_reason,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (plan_id, diagnosis["diagnosis_id"], run_id, parent["candidate_id"],
                 parent.get("root_candidate_id") or parent["candidate_id"], "TURNOVER_ABOVE_BASE_MAX",
                 TURNOVER_DECAY_STEP_1, spec["repair_signature"], _json(spec["repair_path"]),
                 int(parent.get("repair_depth") or 0) + 1, _json(spec), "[]", "READY", 1, 0, 0,
                 None, now, now),
            )
            inserted = conn.execute("SELECT changes()").fetchone()[0]
        if inserted:
            created.append(plan_id)
            audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id, round_id=round_id,
                        repair_plan_id=plan_id, parent_candidate_id=parent["candidate_id"],
                        repair_strategy=TURNOVER_DECAY_STEP_1,
                        target_failure="TURNOVER_ABOVE_BASE_MAX",
                        turnover_policy=TURNOVER_STAGED_POLICY, stage=1,
                        evidence_source="LIVE_SIMULATION")
    return created


def _candidate_complete(candidate: Mapping[str, Any], alpha_db: Path) -> bool:
    if str(candidate.get("simulation_status") or "").upper() == "COMPLETE":
        return True
    # Read-only cache fallback for a COMPLETE fact not yet mirrored locally.
    import sqlite3
    try:
        conn = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
        row = conn.execute("SELECT status FROM alpha_results WHERE sim_key=?", (candidate.get("sim_key"),)).fetchone()
        conn.close()
        return bool(row and str(row[0] or "").upper() == "COMPLETE")
    except sqlite3.Error:
        return False


def _persist_next_plan(store: Any, config: Any, run_id: str, round_id: str,
                       previous_plan: Mapping[str, Any], previous_child: Mapping[str, Any],
                       stage: int, context: Mapping[str, Any]) -> Optional[str]:
    spec = turnover_stage_spec(
        previous_child, stage,
        origin_candidate_id=str(context["origin_candidate_id"]),
        origin_signal_family=str(context["origin_signal_family"]),
        base_decay=int(context["base_decay"]),
        previous_stage_candidate_id=str(previous_child["candidate_id"]),
    )
    spec["turnover_staged_context"]["round_id"] = round_id
    spec["repair_path"] = json.loads(previous_plan.get("repair_path_json") or "[]") + [
        f"TURNOVER_ABOVE_BASE_MAX:{spec['repair_type']}"
    ]
    registry = {}
    with store.connect() as conn:
        registry = {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}
    gate = operator_gate(spec.get("operator_requirements") or (), registry)
    if gate["status"] != "READY":
        return None
    if stage == 3 and validate_turnover_hump_structure(spec, previous_child, config.rules, registry)["status"] != "READY":
        audit_event(action="TURNOVER_HUMP_STRUCTURE_BLOCKED", run_id=run_id,
                    candidate_id=previous_child["candidate_id"], round_id=round_id)
        return None
    signature = repair_signature(
        previous_child, spec["repair_type"], "TURNOVER_ABOVE_BASE_MAX",
        {"stage": stage, "policy": TURNOVER_STAGED_POLICY,
         "origin": context["origin_candidate_id"]}, spec.get("settings_override") or {},
    )
    spec["repair_signature"] = signature
    plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
    diagnosis_id = "diag_" + hashlib.sha256(
        f"{run_id}|{previous_child['candidate_id']}|{TURNOVER_STAGED_POLICY}|{stage}".encode()
    ).hexdigest()[:24]
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO ppl_diagnoses(
                   diagnosis_id,run_id,candidate_id,alpha_id,source_phase,evidence_source,
                   primary_failure,secondary_failures_json,severity,repairability,root_cause,
                   metrics_snapshot_json,check_session_id,check_result_ids_json,
                   diagnosis_rule_version,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (diagnosis_id, run_id, previous_child["candidate_id"], previous_child.get("alpha_id"),
             "PRE_TAG_CHECK", "DURABLE_STAGED_TURNOVER", "TURNOVER_ABOVE_BASE_MAX", "[]",
             "MEDIUM", "REPAIRABLE", "TURNOVER_ABOVE_BASE_MAX", "{}",
             context.get("check_session_id"), _json([context.get("check_result_id")] if context.get("check_result_id") else []),
             DIAGNOSIS_RULE_VERSION, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO ppl_repair_plans(
                   repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
                   target_failure,repair_type,repair_signature,repair_path_json,repair_depth,
                   candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,
                   committed_posts,consumed_posts,blocked_reason,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (plan_id, diagnosis_id, run_id, previous_child["candidate_id"],
             context["origin_candidate_id"], "TURNOVER_ABOVE_BASE_MAX", spec["repair_type"],
             signature, _json(spec["repair_path"]), int(previous_plan.get("repair_depth") or 0) + 1,
             _json(spec), _json(spec.get("operator_requirements") or []), "PLANNED", 1, 0, 0,
             None, now, now),
        )
    audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id, round_id=round_id,
                repair_plan_id=plan_id, parent_candidate_id=previous_child["candidate_id"],
                repair_strategy=spec["repair_type"], target_failure="TURNOVER_ABOVE_BASE_MAX",
                turnover_policy=TURNOVER_STAGED_POLICY, stage=stage,
                evidence_source=context.get("evidence_source"))
    return plan_id


def preview_turnover_staged_plans(
    store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
) -> Dict[str, Any]:
    """Read-only preview of the plans ``sync_turnover_staged_plans`` would create.

    No durable rows, audit events, HTTP requests, or Check refreshes are made.
    When the authoritative path would require a GET-only Check refresh before
    deciding whether the next stage exists, the preview returns
    ``evaluation_complete=False`` instead of guessing.
    """
    candidates = {str(c["candidate_id"]): c for c in store.load_candidates(run_id)}
    durable_plans = [dict(p) for p in store.load_repair_plans(run_id)]
    existing_signatures = {str(p.get("repair_signature") or "") for p in durable_plans}
    virtual: list = []
    incomplete: list = []
    exhausted: list = []

    # Stage-1 bootstrap parity.
    existing_families = set()
    for plan in durable_plans:
        ctx = _stage_context(plan)
        if ctx.get("policy") == TURNOVER_STAGED_POLICY and ctx.get("origin_signal_family"):
            existing_families.add(str(ctx["origin_signal_family"]))
    with store.connect() as conn:
        diagnoses = [dict(r) for r in conn.execute(
            """SELECT * FROM ppl_diagnoses WHERE run_id=?
               AND primary_failure='TURNOVER_ABOVE_BASE_MAX'
               AND source_phase='SIMULATION' AND evidence_source='LIVE_SIMULATION'
               ORDER BY created_at DESC, diagnosis_id DESC""", (run_id,)
        )]
        registry = {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}
        edges = {str(r["repair_signature"]): dict(r) for r in conn.execute(
            "SELECT * FROM ppl_repairs WHERE run_id=?", (run_id,)
        )}

    by_family: Dict[str, list] = {}
    for diagnosis in diagnoses:
        parent = candidates.get(str(diagnosis.get("candidate_id") or ""))
        if not parent:
            continue
        family = str(parent.get("signal_family") or "")
        if not family or family in existing_families:
            continue
        path_raw = parent.get("repair_path_json") or parent.get("repair_path") or "[]"
        try:
            path = json.loads(path_raw) if isinstance(path_raw, str) else list(path_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            path = []
        if any(any(stage in str(part) for stage in TURNOVER_STAGED_STRATEGIES) for part in path):
            continue
        metrics = json.loads(diagnosis.get("metrics_snapshot_json") or "{}")
        sharpe = float(metrics.get("sharpe")) if metrics.get("sharpe") is not None else -999.0
        by_family.setdefault(family, []).append((sharpe, str(parent["candidate_id"]), diagnosis, parent, path))
    for family in sorted(by_family):
        _, _, diagnosis, parent, path = sorted(by_family[family], key=lambda x: (-x[0], x[1]))[0]
        spec = turnover_stage_spec(parent, 1)
        spec["turnover_staged_context"]["round_id"] = round_id
        spec["repair_path"] = path + [f"TURNOVER_ABOVE_BASE_MAX:{TURNOVER_DECAY_STEP_1}"]
        signature = str(spec["repair_signature"])
        if signature in existing_signatures:
            continue
        plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
        virtual.append({
            "repair_plan_id": plan_id, "diagnosis_id": diagnosis["diagnosis_id"], "run_id": run_id,
            "parent_candidate_id": parent["candidate_id"],
            "root_candidate_id": parent.get("root_candidate_id") or parent["candidate_id"],
            "target_failure": "TURNOVER_ABOVE_BASE_MAX", "repair_type": TURNOVER_DECAY_STEP_1,
            "repair_signature": signature, "repair_path_json": _json(spec["repair_path"]),
            "repair_depth": int(parent.get("repair_depth") or 0) + 1,
            "candidate_spec_json": _json(spec), "operator_requirements_json": "[]",
            "plan_status": "READY", "projected_new_posts": 1,
            "committed_posts": 0, "consumed_posts": 0, "blocked_reason": None,
            "_virtual_source": "TURNOVER_STAGE1_PREVIEW",
        })
        existing_signatures.add(signature)

    staged_plans = [p for p in durable_plans if str(p.get("repair_type") or "") in TURNOVER_STAGED_STRATEGIES]
    by_origin: Dict[tuple, list] = {}
    for plan in staged_plans:
        ctx = _stage_context(plan)
        if ctx.get("policy") != TURNOVER_STAGED_POLICY:
            continue
        key = (str(ctx.get("origin_candidate_id") or ""), str(ctx.get("origin_signal_family") or ""))
        if all(key):
            by_origin.setdefault(key, []).append((int(ctx.get("stage") or 0), plan, ctx))

    for (origin_id, origin_family), chain in sorted(by_origin.items()):
        stage, plan, ctx = max(chain, key=lambda item: item[0])
        if stage not in {1, 2, 3} or str(plan.get("plan_status")) != "EXECUTED":
            continue
        edge = edges.get(str(plan.get("repair_signature") or ""))
        child = candidates.get(str((edge or {}).get("child_candidate_id") or ""))
        if not child or not _candidate_complete(child, alpha_db):
            continue
        evidence = latest_turnover_check_state(store, run_id, child["candidate_id"])
        if evidence["state"] == "INSUFFICIENT" and evidence.get("refresh_allowed"):
            incomplete.append({
                "origin_candidate_id": origin_id, "origin_signal_family": origin_family,
                "candidate_id": child["candidate_id"], "reason": "TURNOVER_CHECK_REFRESH_REQUIRED",
            })
            continue
        if evidence["state"] != "BLOCKED":
            continue
        if stage == 3:
            exhausted.append({
                "origin_candidate_id": origin_id, "origin_signal_family": origin_family,
                "policy": TURNOVER_STAGED_POLICY, "run_id": run_id, "round_id": round_id,
            })
            continue
        next_stage = stage + 1
        if any(s == next_stage for s, _, _ in chain):
            continue
        spec = turnover_stage_spec(
            child, next_stage, origin_candidate_id=str(ctx["origin_candidate_id"]),
            origin_signal_family=str(ctx["origin_signal_family"]), base_decay=int(ctx["base_decay"]),
            previous_stage_candidate_id=str(child["candidate_id"]),
        )
        spec["turnover_staged_context"]["round_id"] = round_id
        spec["repair_path"] = json.loads(plan.get("repair_path_json") or "[]") + [
            f"TURNOVER_ABOVE_BASE_MAX:{spec['repair_type']}"
        ]
        gate = operator_gate(spec.get("operator_requirements") or (), registry)
        if gate["status"] != "READY":
            continue
        if next_stage == 3 and validate_turnover_hump_structure(spec, child, config.rules, registry)["status"] != "READY":
            continue
        signature = repair_signature(
            child, spec["repair_type"], "TURNOVER_ABOVE_BASE_MAX",
            {"stage": next_stage, "policy": TURNOVER_STAGED_POLICY, "origin": ctx["origin_candidate_id"]},
            spec.get("settings_override") or {},
        )
        spec["repair_signature"] = signature
        if signature in existing_signatures:
            continue
        plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
        diagnosis_id = "diag_" + hashlib.sha256(
            f"{run_id}|{child['candidate_id']}|{TURNOVER_STAGED_POLICY}|{next_stage}".encode()
        ).hexdigest()[:24]
        virtual.append({
            "repair_plan_id": plan_id, "diagnosis_id": diagnosis_id, "run_id": run_id,
            "parent_candidate_id": child["candidate_id"], "root_candidate_id": ctx["origin_candidate_id"],
            "target_failure": "TURNOVER_ABOVE_BASE_MAX", "repair_type": spec["repair_type"],
            "repair_signature": signature, "repair_path_json": _json(spec["repair_path"]),
            "repair_depth": int(plan.get("repair_depth") or 0) + 1,
            "candidate_spec_json": _json(spec),
            "operator_requirements_json": _json(spec.get("operator_requirements") or []),
            "plan_status": "PLANNED", "projected_new_posts": 1,
            "committed_posts": 0, "consumed_posts": 0, "blocked_reason": None,
            "_virtual_source": f"TURNOVER_STAGE{next_stage}_PREVIEW",
        })
        existing_signatures.add(signature)

    return {
        "virtual_plan_rows": virtual, "evaluation_complete": not incomplete,
        "incomplete_reasons": incomplete, "exhausted": exhausted,
        "network_requests": 0, "check_requests": 0, "writes": 0,
    }


def sync_turnover_staged_plans(
    store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str, *,
    refresh_check: Optional[Callable[[str], Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Persist at most the next eligible stage for each origin family."""
    bootstrapped = _bootstrap_stage1_plans(store, run_id, round_id)
    plans = [p for p in store.load_repair_plans(run_id)
             if str(p.get("repair_type") or "") in TURNOVER_STAGED_STRATEGIES]
    candidates = {str(c["candidate_id"]): c for c in store.load_candidates(run_id)}
    edges = {}
    with store.connect() as conn:
        for row in conn.execute("SELECT * FROM ppl_repairs WHERE run_id=?", (run_id,)):
            edges[str(row["repair_signature"])] = dict(row)
    created, refreshed, exhausted = [], [], []
    by_origin: Dict[tuple, list] = {}
    for plan in plans:
        ctx = _stage_context(plan)
        if ctx.get("policy") != TURNOVER_STAGED_POLICY:
            continue
        key = (str(ctx.get("origin_candidate_id") or ""), str(ctx.get("origin_signal_family") or ""))
        if not all(key):
            continue
        by_origin.setdefault(key, []).append((int(ctx.get("stage") or 0), plan, ctx))
    for (origin_id, origin_family), chain in sorted(by_origin.items()):
        stage, plan, ctx = max(chain, key=lambda item: item[0])
        if stage not in {1, 2, 3} or str(plan.get("plan_status")) != "EXECUTED":
            continue
        edge = edges.get(str(plan.get("repair_signature") or ""))
        child = candidates.get(str((edge or {}).get("child_candidate_id") or ""))
        if not child or not _candidate_complete(child, alpha_db):
            continue
        evidence = latest_turnover_check_state(store, run_id, child["candidate_id"])
        evidence_source = "EXISTING_POST_SIMULATION_CHECK"
        if (evidence["state"] == "INSUFFICIENT" and evidence.get("refresh_allowed")
                and refresh_check is not None):
            report = dict(refresh_check(child["candidate_id"]) or {})
            refreshed.append({"candidate_id": child["candidate_id"], "report": report})
            evidence = latest_turnover_check_state(store, run_id, child["candidate_id"])
            evidence_source = "GET_ONLY_HIGH_TURNOVER_REFRESH"
        if evidence["state"] != "BLOCKED":
            continue
        evidence = {**evidence, "evidence_source": evidence_source}
        if stage == 3:
            exhausted.append({"origin_candidate_id": origin_id, "origin_signal_family": origin_family,
                              "policy": TURNOVER_STAGED_POLICY, "run_id": run_id, "round_id": round_id})
            continue
        next_stage = stage + 1
        if any(s == next_stage for s, _, _ in chain):
            continue
        new_id = _persist_next_plan(store, config, run_id, round_id, plan, child, next_stage,
                                    {**ctx, **evidence})
        if new_id:
            created.append(new_id)
    return {"bootstrapped_stage1_plan_ids": bootstrapped, "created_plan_ids": created,
            "refreshes": refreshed, "exhausted": exhausted}
