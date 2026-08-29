"""Phase 4 offline reconciliation. This module performs no BRAIN network calls."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from .atomic import atomic_write_json
from .config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    execution_hash_status_for_run,
    simulation_budget_allocation,
)
from .state_machine import CANDIDATE_TRANSITIONS, RUN_TRANSITIONS, validate_or_quarantine_state


IMMUTABLE_CANDIDATE_FIELDS = (
    "candidate_id", "expression", "expression_raw", "expression_canonical", "expression_hash",
    "sim_key", "settings_json", "settings_hash", "context_fingerprint", "dataset_id", "field_id",
    "field_type", "semantic_class", "signal_family", "direction", "transform_family", "window",
    "vector_reducer", "discovery_snapshot_id", "dry_run_snapshot_id", "root_candidate_id",
    "parent_candidate_id", "parent_sim_key",
)
MUTABLE_WORKFLOW_FIELDS = (
    "lifecycle_state", "simulation_status", "simulation_freshness", "execution_action",
    "live_reconcile_required", "provenance_warning", "updated_at",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _alpha_facts(alpha_db: Path, sim_keys: list) -> Dict[str, Dict[str, Any]]:
    if not sim_keys:
        return {}
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        facts = {}
        for start in range(0, len(sim_keys), 500):
            chunk = sim_keys[start:start + 500]
            marks = ",".join("?" for _ in chunk)
            for row in connection.execute(f"SELECT * FROM alpha_results WHERE sim_key IN ({marks})", chunk):
                facts[str(row["sim_key"])] = dict(row)
        return facts
    finally:
        connection.close()


def derive_simulation_state(fact: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not fact:
        return {
            "simulation_status": "NONE", "simulation_freshness": "UNKNOWN",
            "execution_action": "NEW_SIMULATION_REQUIRED", "live_reconcile_required": False,
            "cache_classification": "CACHE_MISS",
        }
    status = str(fact.get("status") or "UNKNOWN").upper()
    if status == "COMPLETE":
        return {"simulation_status": status, "simulation_freshness": "CONFIRMED_COMPLETE",
                "execution_action": "CACHE_RESTORE", "live_reconcile_required": False,
                "cache_classification": "CACHE_COMPLETE"}
    if status in {"RUNNING", "SUBMITTED"} and fact.get("simulation_url"):
        return {"simulation_status": status, "simulation_freshness": "STALE_NONTERMINAL",
                "execution_action": "RESUME_EXISTING", "live_reconcile_required": True,
                "cache_classification": f"RESUME_{status}"}
    if status == "INVALID":
        return {"simulation_status": status, "simulation_freshness": "LOCAL_ERROR",
                "execution_action": "STOP_INVALID", "live_reconcile_required": False,
                "cache_classification": "CACHE_INVALID"}
    if status == "UNCERTAIN_SUBMISSION":
        return {"simulation_status": status, "simulation_freshness": "UNKNOWN",
                "execution_action": "HOLD_UNCERTAIN", "live_reconcile_required": True,
                "cache_classification": "CACHE_UNCERTAIN"}
    if status == "STALE_RUNNING" and fact.get("simulation_url"):
        return {"simulation_status": status, "simulation_freshness": "STALE_NONTERMINAL",
                "execution_action": "RESUME_EXISTING", "live_reconcile_required": True,
                "cache_classification": "CACHE_STALE_RUNNING"}
    if status == "STALE_RUNNING":
        return {"simulation_status": "UNCERTAIN_SUBMISSION", "simulation_freshness": "UNKNOWN",
                "execution_action": "HOLD_UNCERTAIN", "live_reconcile_required": True,
                "cache_classification": "CACHE_UNCERTAIN"}
    if status == "REMOTE_NOT_FOUND":
        return {"simulation_status": status, "simulation_freshness": "REMOTE_TERMINAL",
                "execution_action": "HOLD_REMOTE_NOT_FOUND", "live_reconcile_required": False,
                "cache_classification": "CACHE_REMOTE_NOT_FOUND"}
    if status in {"ERROR", "AUTH_ERROR"}:
        return {"simulation_status": status, "simulation_freshness": "LOCAL_ERROR",
                "execution_action": "RETRY_PER_V21_POLICY", "live_reconcile_required": False,
                "cache_classification": f"CACHE_{status}"}
    return {"simulation_status": status if status else "UNKNOWN", "simulation_freshness": "UNKNOWN",
            "execution_action": "NEEDS_REVIEW", "live_reconcile_required": bool(fact.get("simulation_url")),
            "cache_classification": "CACHE_UNKNOWN"}


def _desired_lifecycle(old: str, simulation_status: str) -> str:
    if simulation_status == "COMPLETE" and old == "PLANNED":
        return "SIMULATION_COMPLETE"
    return old


def _transition_allowed(old: str, new: str, allowed: Mapping[str, set]) -> None:
    if old != new and new not in allowed.get(old, set()):
        raise ValueError(f"STATE_TRANSITION_REJECTED: {old} -> {new}")


def plan_offline_reconcile(store: Any, run_id: str, config: Any, alpha_db: Path) -> Dict[str, Any]:
    run = store.get_run(run_id)
    if not run:
        raise ConfigError(f"Unknown run: {run_id}")
    compat = execution_hash_status_for_run(config, run)
    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:
        raise ConfigError(
            f"EXECUTION_HASH_{compat['status']}: {compat['reason']}; the existing plan is immutable"
        )
    candidates = store.load_candidates(run_id)
    facts = _alpha_facts(alpha_db, [str(x["sim_key"]) for x in candidates])
    with store.connect() as conn:
        snapshots = {row[0] for row in conn.execute("SELECT snapshot_id FROM ppl_discovery_snapshots")}
    changes = []
    counts = Counter()
    selected_counts = Counter()
    lifecycle_before = Counter(str(x["lifecycle_state"]) for x in candidates)
    for candidate in candidates:
        desired = derive_simulation_state(facts.get(str(candidate["sim_key"])))
        new_lifecycle = _desired_lifecycle(str(candidate["lifecycle_state"]), desired["simulation_status"])
        _transition_allowed(str(candidate["lifecycle_state"]), new_lifecycle, CANDIDATE_TRANSITIONS)
        warning = None if candidate.get("discovery_snapshot_id") in snapshots else "PROVENANCE_SNAPSHOT_MISSING"
        desired.update({"lifecycle_state": new_lifecycle, "provenance_warning": warning})
        changed = any(
            (bool(candidate.get(key)) if key == "live_reconcile_required" else candidate.get(key)) != value
            for key, value in desired.items() if key != "cache_classification"
        )
        if changed:
            changes.append({"candidate_id": candidate["candidate_id"], "old": candidate, "desired": desired})
        counts[desired["cache_classification"]] += 1
        if candidate.get("selected_for_initial_search"):
            selected_counts[desired["cache_classification"]] += 1
    allocation = simulation_budget_allocation(config.plan)
    selected_total = sum(selected_counts.values())
    consumed = sum(int(x.get("new_post_budget_consumed") or 0) for x in candidates if x.get("selected_for_initial_search"))
    projected = selected_counts["CACHE_MISS"] + selected_counts["CACHE_ERROR"] + selected_counts["CACHE_AUTH_ERROR"]
    resume = selected_counts["RESUME_RUNNING"] + selected_counts["RESUME_SUBMITTED"]
    budget = {
        "initial_total": allocation["initial_search_budget"],
        "budget_committed": 0,
        "budget_consumed": consumed,
        "budget_projected": projected,
        "initial_post_budget_remaining_current": allocation["initial_search_budget"] - consumed,
        "initial_post_budget_remaining_after_plan": allocation["initial_search_budget"] - consumed - projected,
        "repair_reserve_current": allocation["repair_reserve_budget"],
    }
    actionable = projected + resume
    target_run_status = "READY_FOR_EXECUTION" if actionable else "COMPLETED"
    return {
        "mode": "OFFLINE_RECONCILE", "run_id": run_id, "candidates_scanned": len(candidates),
        "lifecycle_before": dict(lifecycle_before), "changes": changes,
        "would_apply_candidate_transitions": sum(x["old"]["lifecycle_state"] != x["desired"]["lifecycle_state"] for x in changes),
        "cache_counts": dict(sorted(counts.items())), "selected_initial_candidates": selected_total,
        "selected_cache_counts": dict(sorted(selected_counts.items())),
        "selected_cache_restored": selected_counts["CACHE_COMPLETE"],
        "selected_resume_required": resume, "selected_new_simulation_required": selected_counts["CACHE_MISS"],
        "selected_retry_required": selected_counts["CACHE_ERROR"] + selected_counts["CACHE_AUTH_ERROR"],
        "budget": budget, "target_run_status": target_run_status,
        "operational_revision_changed": run["operational_hash"] != config.operational_hash,
        "presentation_changed": run["presentation_hash"] != config.presentation_hash,
        "warnings": sorted({x["desired"]["provenance_warning"] for x in changes if x["desired"]["provenance_warning"]}),
        "network_requests": 0, "simulation_posts": 0, "check_requests": 0,
    }


def _audit_transition(conn: sqlite3.Connection, run_id: str, candidate_id: Optional[str], entity: str,
                      old: str, new: str, reason: str, source: str, metadata: Optional[Dict[str, Any]] = None) -> None:
    conn.execute(
        """INSERT INTO ppl_state_transitions(
               run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
           ) VALUES (?,?,?,?,?,?,?,?,?)""",
        (run_id, candidate_id, entity, old, new, reason, source,
         json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True), _utc_now()),
    )


def apply_offline_reconcile(store: Any, plan: Dict[str, Any], config: Any) -> Dict[str, Any]:
    run_id = plan["run_id"]
    candidate_transitions = 0
    run_transitions = 0
    with store.connect() as conn:
        for item in plan["changes"]:
            old, desired = item["old"], item["desired"]
            old_lifecycle = str(old["lifecycle_state"]); new_lifecycle = desired["lifecycle_state"]
            _transition_allowed(old_lifecycle, new_lifecycle, CANDIDATE_TRANSITIONS)
            conn.execute(
                """UPDATE ppl_candidates SET lifecycle_state=?,simulation_status=?,simulation_freshness=?,
                       execution_action=?,live_reconcile_required=?,cache_classification=?,provenance_warning=?,updated_at=?
                   WHERE candidate_id=?""",
                (new_lifecycle, desired["simulation_status"], desired["simulation_freshness"],
                 desired["execution_action"], int(desired["live_reconcile_required"]),
                 desired["cache_classification"], desired["provenance_warning"], _utc_now(), item["candidate_id"]),
            )
            if old_lifecycle != new_lifecycle:
                _audit_transition(conn, run_id, item["candidate_id"], "CANDIDATE", old_lifecycle, new_lifecycle,
                                  "Stable COMPLETE cache fact", "CACHE_RECONCILE", {"sim_key": old["sim_key"]})
                candidate_transitions += 1
        run_row = conn.execute("SELECT status,operational_hash,presentation_hash FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
        current = str(run_row["status"])
        path = []
        if current == "CREATED": path.extend(["PLANNED", "RECONCILED"])
        elif current == "PLANNED": path.append("RECONCILED")
        if (path[-1] if path else current) == "RECONCILED": path.append(plan["target_run_status"])
        for target in path:
            _transition_allowed(current, target, RUN_TRANSITIONS)
            conn.execute("UPDATE ppl_runs SET status=?,current_stage=?,updated_at=? WHERE run_id=?", (target, target, _utc_now(), run_id))
            _audit_transition(conn, run_id, None, "RUN", current, target, "Offline reconciliation", "SYSTEM_RECOVERY")
            current = target; run_transitions += 1
        revision_delta = int(run_row["operational_hash"] != config.operational_hash)
        conn.execute(
            """UPDATE ppl_runs SET operational_hash=?,presentation_hash=?,
                   operational_revision=operational_revision+?,updated_at=? WHERE run_id=?""",
            (config.operational_hash, config.presentation_hash, revision_delta, _utc_now(), run_id),
        )
    result = {k: v for k, v in plan.items() if k != "changes"}
    result.update({"candidate_transitions_applied": candidate_transitions,
                   "run_transitions_applied": run_transitions,
                   "no_op_candidates": plan["candidates_scanned"] - len(plan["changes"])})
    return result


def build_state_from_db(store: Any, run_id: str, config: Any, reconcile_result: Dict[str, Any]) -> Dict[str, Any]:
    run = store.get_run(run_id); candidates = store.load_candidates(run_id)
    states = Counter(str(x["lifecycle_state"]) for x in candidates)
    pending = [x["candidate_id"] for x in candidates if x.get("selected_for_initial_search") and
               x.get("execution_action") in {"NEW_SIMULATION_REQUIRED", "RESUME_EXISTING", "RETRY_PER_V21_POLICY"}]
    return {
        "schema_version": 2, "run_id": run_id, "runner_goal": run["runner_goal"],
        "target_mode": run["target_mode"], "atom_constraint_active": bool(run["atom_constraint_active"]),
        "status": run["status"], "current_stage": run["current_stage"],
        "execution_hash": run["execution_hash"], "operational_hash": run["operational_hash"],
        "presentation_hash": run["presentation_hash"], "workflow_source": "PPL_RUNNER_DB",
        "candidate_counts": dict(sorted(states.items())), "selected_initial_candidates": reconcile_result["selected_initial_candidates"],
        "pending_simulations": pending, "pending_checks": [], "budget": reconcile_result["budget"],
        "warnings": reconcile_result.get("warnings", []), "last_error": None, "updated_at": _utc_now(),
    }


def write_reconciled_outputs(state_path: Path, execution_plan_path: Path, store: Any,
                             run_id: str, config: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    state_file = validate_or_quarantine_state(state_path)
    state = build_state_from_db(store, run_id, config, result)
    atomic_write_json(state_path, state)
    execution_plan = {
        "run_id": run_id, "selected_candidates": result["selected_initial_candidates"],
        "cache_complete": result["selected_cache_restored"], "resume_existing": result["selected_resume_required"],
        "new_simulation_required": result["selected_new_simulation_required"],
        "retry_per_v21_policy": result["selected_retry_required"],
        "stop_invalid": result["selected_cache_counts"].get("CACHE_INVALID", 0),
        "hold_uncertain": result["selected_cache_counts"].get("CACHE_UNCERTAIN", 0),
        "budget": result["budget"], "generated_at": _utc_now(),
    }
    atomic_write_json(execution_plan_path, execution_plan)
    return {"state_file_rebuilt": True, "state_file_previous": state_file, "execution_plan": execution_plan}
