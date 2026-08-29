"""Production Repair execution entry for PRODUCTION_RESEARCH runs.

This is a thin, auditable orchestration layer only. It reuses the unchanged
V2.1 simulation engine (via the V2.2 simulation adapter), the existing Repair
Engine, Cache/Resume/Slot-Guard semantics, and the offline reconcile/analysis
path. It does NOT reimplement HTTP, simulation, budget, or reconcile logic.

Two closed loops are served here:
  1. Explicit Production Repair execution of deferred/local-diagnosis plans and
     check-derived proposals (single plan or a small explicit list, never "all").
  2. The shared execution path that both plan origins feed into, so there is
     exactly one Simulation execution route.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .candidate_factory import classify_cache_read_only
from .continuous_check import enqueue_pretag_checks
from .check_derived_repair import evaluate_ht_repair_outcome
from .config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    execution_hash_status_for_run,
    simulation_budget_allocation,
)
from .live_execution import (
    MACHINE_HASH_OPERATION_PRODUCTION_REPAIR,
    MACHINE_HASH_OPERATION_ROUND_REPAIR,
    _alpha_facts,
    _instrument_v21,
    _json,
    _now,
    _sync_candidate_fact,
    _v21_candidate,
    execute_continuous_remote_handoff,
    run_local_analysis,
    run_one_pretag_check,
    validate_machine_lib_hash,
)
from .round_store import get_round, set_batch_intent, start_batch
from .research_telemetry import record_event
from .repair_engine import (
    RESCUE_STRATEGIES,
    TURNOVER_HUMP,
    is_retired_auto_repair_plan,
    materialize_repair_candidate,
    operator_gate,
    parameter_change_summary,
    validate_turnover_hump_structure,
)
from .ppc_controlled_branch import (
    PPC_EVALUATED_OUTCOMES, PPC_TARGET_FAILURE, evaluate_ppc_repair_outcome,
    ppc_outcome_payload,
)
from .simulation_adapter import execute_with_v21, server_slot_deferred_sim_keys
from .settings_contract import validate_full_simulation_settings
from .state_machine import CANDIDATE_TRANSITIONS, REPAIR_PLAN_TRANSITIONS
from .audit_log import audit_event

PHASE = "PRODUCTION_REPAIR"

# Plan statuses that a Production run may explicitly promote into execution.
EXECUTABLE_STATUSES = {"READY", "DEFERRED_INITIAL_SEARCH", "DEFERRED_PHASE_END", "PLANNED"}
# Statuses that require a one-time promotion to READY before any POST.
PROMOTABLE_STATUSES = {"DEFERRED_INITIAL_SEARCH", "DEFERRED_PHASE_END", "PLANNED"}
# Actions that will actually produce a new Simulation POST (and consume reserve).
POST_ACTIONS = {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"}
ROUND_REPAIR_PREFLIGHT_VERSION = "ROUND_REPAIR_PREFLIGHT_V1"


def _durable_worker_progress_summary(
    runtime_stats: Mapping[str, Any], facts: Mapping[str, Mapping[str, Any]],
) -> Dict[str, int]:
    """Separate worker completion from durable Simulation terminality."""
    statuses = [str((fact or {}).get("status") or "UNKNOWN").upper() for fact in facts.values()]
    summary = {
        "workers_finished": int(runtime_stats.get("processed") or 0),
        "workers_submitted": int(runtime_stats.get("submitted_futures") or len(statuses)),
        "durable_complete": sum(status == "COMPLETE" for status in statuses),
        "durable_running": sum(status in {"SUBMITTED", "RUNNING", "STALE_RUNNING"} for status in statuses),
        "durable_uncertain": sum(status == "UNCERTAIN_SUBMISSION" for status in statuses),
    }
    print(
        "Durable Execution Summary\n"
        "-------------------------\n"
        f"Workers Finished: {summary['workers_finished']} / {summary['workers_submitted']}\n"
        f"Durable Complete: {summary['durable_complete']}\n"
        f"Durable Running: {summary['durable_running']}\n"
        f"Durable Uncertain: {summary['durable_uncertain']}"
    )
    return summary


def _release_server_slot_deferred_repair_children(
    store: Any,
    run_id: str,
    execution_units_by_key: Mapping[str, Mapping[str, Any]],
    deferred_keys: Iterable[str],
) -> Dict[str, Any]:
    """Return positively-known never-POSTed repair children to READY/PLANNED.

    This is the Repair-side counterpart of the V3 Search deferred-tail release.
    It only accepts child rows with no alpha id and simulation_status NONE, so a
    missing/ambiguous remote identity is never auto-retried.
    """
    keys = sorted({str(x) for x in deferred_keys if x})
    if not keys:
        return {"candidate_ids": [], "plan_ids": [], "sim_keys": []}
    candidates = {str(x.get("sim_key") or ""): dict(x) for x in store.load_candidates(run_id) if x.get("sim_key")}
    unsafe = []
    rows = []
    for key in keys:
        row = candidates.get(key)
        if not row:
            unsafe.append({"sim_key": key, "reason": "CANDIDATE_MISSING"})
            continue
        status = str(row.get("simulation_status") or "NONE").upper()
        lifecycle = str(row.get("lifecycle_state") or "").upper()
        if row.get("alpha_id") or status not in {"", "NONE"} or lifecycle not in {"SIMULATION_PENDING", "PLANNED"}:
            unsafe.append({
                "candidate_id": row.get("candidate_id"), "sim_key": key,
                "simulation_status": status, "lifecycle_state": lifecycle,
                "alpha_id": row.get("alpha_id"),
            })
            continue
        rows.append(row)
    if unsafe:
        raise ConfigError("PRODUCTION_REPAIR_DEFERRED_RELEASE_UNSAFE:" + _json(unsafe))

    candidate_ids = []
    plan_ids = []
    transitions = []
    now = _now()
    with store.connect() as conn:
        for row in rows:
            key = str(row["sim_key"])
            cid = str(row["candidate_id"])
            old = str(row.get("lifecycle_state") or "")
            unit = execution_units_by_key.get(key) or {}
            unit_plan_ids = [str(x) for x in unit.get("plan_ids", []) if x]
            conn.execute(
                """UPDATE ppl_candidates
                   SET lifecycle_state='PLANNED',simulation_status='NONE',alpha_id=NULL,
                       execution_action='NEW_SIMULATION_REQUIRED',cache_classification='CACHE_MISS',updated_at=?
                   WHERE run_id=? AND candidate_id=?""",
                (now, run_id, cid),
            )
            if old != "PLANNED":
                conn.execute(
                    """INSERT INTO ppl_state_transitions(
                           run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                       ) VALUES (?,?,'CANDIDATE',?,'PLANNED',?,?,?,?)""",
                    (run_id, cid, old, "V3_SERVER_SLOT_DEFERRED_RELEASED", PHASE,
                     _json({"sim_key": key, "repair_plan_ids": unit_plan_ids}), now),
                )
                transitions.append((cid, old))
            for plan_id in unit_plan_ids:
                conn.execute(
                    """UPDATE ppl_repair_plans
                       SET plan_status='READY',committed_posts=0,consumed_posts=0,
                           blocked_reason='SERVER_SLOT_DEFERRED',updated_at=?
                       WHERE run_id=? AND repair_plan_id=?""",
                    (now, run_id, plan_id),
                )
                plan_ids.append(plan_id)
            candidate_ids.append(cid)
    for cid, old in transitions:
        audit_event(
            action="STATE_TRANSITION", run_id=run_id, candidate_id=cid, entity_type="CANDIDATE",
            old_state=old, new_state="PLANNED",
            reason="V3_SERVER_SLOT_DEFERRED_RELEASED", source=PHASE,
        )
    audit_event(
        action="SERVER_SLOT_DEFERRED_TAIL_RELEASED", run_id=run_id,
        candidate_ids=sorted(candidate_ids), repair_plan_ids=sorted(set(plan_ids)), sim_keys=keys,
        released_undispatched=len(candidate_ids),
    )
    return {"candidate_ids": sorted(candidate_ids), "plan_ids": sorted(set(plan_ids)), "sim_keys": keys}


def _repair_check_progress(index: int, total: int, *, candidate_id: str, alpha_id: str = "", state: str = "") -> None:
    total = max(1, int(total)); index = max(0, min(int(index), total)); width = 24
    filled = int(round(width * index / total)); bar = "#" * filled + "-" * (width - filled)
    pct = 100.0 * index / total; identity = f"alpha={alpha_id}" if alpha_id else f"candidate={candidate_id}"
    suffix = f" | {state}" if state else ""
    print(f"PRE-TAG CHECK [{bar}] {index}/{total} ({pct:5.1f}%) | {identity}{suffix}", flush=True)


def _audit(store: Any, run_id: str, event: str, payload: Mapping[str, Any],
           candidate_id: Optional[str] = None, sim_key: Optional[str] = None) -> None:
    material = f"{run_id}|{event}|{candidate_id}|{sim_key}|{time.time_ns()}"
    audit_id = "prod_" + hashlib.sha256(material.encode()).hexdigest()[:24]
    with store.connect() as conn:
        conn.execute(
            "INSERT INTO ppl_live_execution_audits(audit_id,run_id,validation_phase,event_type,candidate_id,sim_key,payload_json,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (audit_id, run_id, PHASE, event, candidate_id, sim_key, _json(dict(payload)), _now()),
        )


def validate_production_repair_context(
    store: Any, config: Any, run_id: str, *, allow_continuous_profile: bool = False,
) -> Dict[str, Any]:
    """Confirm the run exists and is a Production Research run.

    The execution hash is validated through the shared compatibility validator:
    EXACT_MATCH and LEGACY_SCHEMA_MATCH are permitted; EXECUTION_DRIFT and
    UNRESOLVED keep hard-blocking.  This keeps one source of truth across
    reconcile / live execution / production repair.
    """
    run = store.get_run(run_id)
    if not run:
        raise ConfigError(f"Unknown run: {run_id}")
    profile = str(run.get("run_profile") or "")
    allowed_profiles = {"PRODUCTION_RESEARCH"}
    if allow_continuous_profile:
        allowed_profiles.add("CONTINUOUS_RESEARCH")
    if profile not in allowed_profiles:
        raise ConfigError("PRODUCTION_REPAIR_REQUIRES_PRODUCTION_RESEARCH_PROFILE")
    compat = execution_hash_status_for_run(config, run)
    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:
        raise ConfigError(f"EXECUTION_HASH_{compat['status']}: {compat['reason']}")
    allocation = simulation_budget_allocation(config.plan)
    budget_state = store.repair_budget_state(run_id)
    plan_consumed = int(budget_state["repair_consumed"])
    # V3 round accounting is authoritative for actual POST consumption because
    # an UNCERTAIN_SUBMISSION + explicitly authorized retry can consume two
    # remote/budget units while remaining one logical Repair Plan/strategy
    # attempt.  Plan-level consumed_posts intentionally tracks the logical plan
    # and can therefore undercount remote budget usage.  Use the larger durable
    # figure so standalone Production Repair cannot overspend the same reserve.
    round_consumed = 0
    try:
        with store.connect() as conn:
            row = conn.execute(
                "SELECT max(repair_consumed) FROM ppl_rounds WHERE run_id=?",
                (run_id,),
            ).fetchone()
        round_consumed = int((row[0] if row else 0) or 0)
    except Exception:
        # V2/standalone stores may not have round tables; the legacy durable
        # plan accounting remains the only available source in that case.
        round_consumed = 0
    effective_consumed = max(plan_consumed, round_consumed)
    return {
        "run": run,
        "repair_reserve_budget": int(allocation["repair_reserve_budget"]),
        "repair_committed": int(budget_state["repair_committed"]),
        "repair_consumed": effective_consumed,
        "repair_consumed_plan": plan_consumed,
        "repair_consumed_round": round_consumed,
        "repair_consumed_source": ("MAX_PLAN_AND_ROUND" if round_consumed else "PLAN_ONLY"),
        "execution_hash_status": compat["status"],
        "execution_hash_matches": compat["status"] == "EXACT_MATCH",
        "execution_semantics_compatible": compat["execution_semantics_compatible"],
        "matched_schema_version": compat["matched_schema_version"],
        "execution_hash_reason": compat["reason"],
    }


def initial_search_completed(store: Any, run_id: str) -> bool:
    """Production Repair is only valid once the initial search has no pending new posts."""
    with store.connect() as conn:
        pending = conn.execute(
            "SELECT COUNT(*) FROM ppl_candidates WHERE run_id=? AND selected_for_initial_search=1 "
            "AND execution_action='NEW_SIMULATION_REQUIRED'",
            (run_id,),
        ).fetchone()[0]
    return int(pending) == 0


def _operator_registry(store: Any) -> Dict[str, str]:
    with store.connect() as conn:
        return {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}


def _materialize_child(
    store: Any, run_id: str, plan: Mapping[str, Any], parent: Mapping[str, Any],
    spec: Mapping[str, Any], config: Any, machine_lib: Any,
) -> Dict[str, Any]:
    """Build the V2.1 repair candidate and the child candidate row (no persistence)."""
    v21 = materialize_repair_candidate(parent, spec, config, machine_lib)
    sim_key = str(v21["sim_key"])
    settings = validate_full_simulation_settings(
        v21["settings"], context="PRODUCTION_REPAIR_CHILD"
    )
    child_id = "repair_" + hashlib.sha256(f"{run_id}|{plan['repair_signature']}".encode()).hexdigest()[:24]
    row = dict(parent)
    row.update(
        candidate_id=child_id,
        source_candidate_id=parent.get("source_candidate_id"),
        expression=v21["expr"],
        expression_raw=v21["expr"],
        expression_canonical=v21["expr"],
        expression_hash=hashlib.sha256(v21["expr"].encode()).hexdigest(),
        sim_key=sim_key,
        settings_json=_json(settings),
        settings_hash=hashlib.sha256(_json(settings).encode()).hexdigest(),
        context_fingerprint=hashlib.sha256(
            f"{parent['context_fingerprint']}|{plan['repair_signature']}".encode()
        ).hexdigest(),
        direction=v21.get("direction") or spec.get("direction_override") or parent.get("direction"),
        transform_family=str(v21.get("operator") or parent.get("transform_family")).upper(),
        operator=v21.get("operator"),
        window=v21.get("window"),
        decay=v21.get("decay"),
        neutralization=settings.get("neutralization"),
        root_candidate_id=parent.get("root_candidate_id") or parent["candidate_id"],
        parent_candidate_id=parent["candidate_id"],
        parent_sim_key=parent["sim_key"],
        repair_path_json=_json(spec.get("repair_path", [])),
        repair_depth=spec.get("repair_depth", 1),
        lifecycle_state="PLANNED",
        simulation_status="NONE",
        simulation_freshness="UNKNOWN",
        cache_classification="CACHE_MISS",
        execution_action="NEW_SIMULATION_REQUIRED",
        selected_for_initial_search=0,
        selection_rank=None,
        new_post_budget_consumed=0,
        alpha_id=None,
        result_reference_json=None,
        created_at=_now(),
        updated_at=_now(),
    )
    return {"v21": v21, "sim_key": sim_key, "child_id": child_id, "row": row}


def _classify_plan(store: Any, run_id: str, plan: Mapping[str, Any], config: Any,
                   machine_lib: Any, alpha_db: Path, registry: Dict[str, str]) -> Dict[str, Any]:
    """Validate one plan and classify its cache/TOCTOU execution disposition."""
    parent_id = plan.get("parent_candidate_id")
    parent = next((x for x in store.load_candidates(run_id) if x["candidate_id"] == parent_id), None)
    if parent is None:
        raise ConfigError(f"REPAIR_PLAN_PARENT_MISSING: {plan.get('repair_plan_id')}")
    spec = json.loads(plan["candidate_spec_json"])
    if is_retired_auto_repair_plan({**dict(plan), "candidate_spec": spec}):
        raise ConfigError(f"REPAIR_STRATEGY_RETIRED: {plan.get('repair_type')}")
    max_depth = int(config.rules["repair_cycle_control"]["max_repair_depth"])
    if int(plan.get("repair_depth") or 0) > max_depth:
        raise ConfigError("REPAIR_DEPTH_EXCEEDED")
    gate = operator_gate(json.loads(plan.get("operator_requirements_json") or "[]"), registry)
    if gate["status"] != "READY":
        raise ConfigError(f"REPAIR_OPERATOR_GATE_{gate['status']}: {plan.get('repair_plan_id')}")
    if str(plan.get("repair_type") or "") == TURNOVER_HUMP:
        structure = validate_turnover_hump_structure(spec, parent, config.rules, registry)
        if structure["status"] != "READY":
            raise ConfigError(f"TURNOVER_HUMP_STRUCTURE_BLOCKED: {structure.get('reason')}")
    child = _materialize_child(store, run_id, plan, parent, spec, config, machine_lib)
    sim_key = child["sim_key"]
    # A Repair that resolves to the parent's exact simulation identity has no
    # empirical value. Fail closed before cache classification/persistence so a
    # no-op can never become EXECUTED or consume a bounded strategy attempt.
    if str(parent.get("sim_key") or "") and sim_key == str(parent.get("sim_key")):
        raise ConfigError(
            f"REPAIR_NO_EFFECTIVE_CHANGE_SAME_SIM_KEY:{plan.get('repair_plan_id')}:{sim_key}"
        )
    existing = next((x for x in store.load_candidates(run_id) if x.get("sim_key") == sim_key), None)
    cache = classify_cache_read_only(alpha_db, sim_key)
    cache_status = cache["cache_classification"]

    # Cache classification (alpha_results) drives the terminal dispositions first.
    # "A candidate row already exists" is NOT the same as "a simulation already ran":
    # a PLANNED/NONE candidate with a CACHE_MISS fact has never POSTed and must be
    # reusable, never permanently blocked by the presence of the row.
    if cache_status == "CACHE_COMPLETE":
        action, will_post, reuse = "CACHE_RESTORE", False, False
    elif cache_status in {"RESUME_RUNNING", "RESUME_SUBMITTED"}:
        action, will_post, reuse = "RESUME_EXISTING", False, False
    elif cache_status == "CACHE_UNCERTAIN":
        action, will_post, reuse = "HOLD_UNCERTAIN", False, False
    elif cache_status == "CACHE_MISS":
        if existing is not None:
            existing_sim = str(existing.get("simulation_status") or "NONE").upper()
            never_posted = (
                existing_sim in {"NONE", ""}
                and int(existing.get("new_post_budget_consumed") or 0) == 0
                and str(existing.get("lifecycle_state") or "") in {"PLANNED", "SIMULATION_PENDING", ""}
            )
            if never_posted:
                action, will_post, reuse = "REUSE_EXISTING_CANDIDATE", True, True
            else:
                # Candidate row claims a simulation state but alpha_results has no fact.
                action, will_post, reuse = "HOLD_UNCERTAIN", False, False
        else:
            action, will_post, reuse = "NEW_SIMULATION_REQUIRED", True, False
    else:
        # ERROR / AUTH_ERROR / INVALID -> per V2.1 retry/stop policy.
        action = cache["execution_action"]
        will_post = action in POST_ACTIONS
        reuse = False

    return {
        "plan": plan, "parent": parent, "spec": spec, "child": child, "sim_key": sim_key,
        "cache": cache, "cache_status": cache_status, "operator_gate": gate,
        "action": action, "will_post": will_post,
        # Any existing row with the same sim_key is the canonical child identity,
        # regardless of whether execution is a new POST, cache restore, or resume.
        "reuse_existing": bool(existing is not None), "existing_child": existing,
    }


def _group_repair_execution_units(classified: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse convergent Repair plans into one remote unit per sim_key."""
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for item in classified:
        grouped.setdefault(str(item["sim_key"]), []).append(item)
    units: List[Dict[str, Any]] = []
    for sim_key in sorted(grouped):
        members = sorted(grouped[sim_key], key=lambda x: str(x["plan"]["repair_plan_id"]))
        actions = {str(x["action"]) for x in members}
        will_post = {bool(x["will_post"]) for x in members}
        existing_ids = {
            str(x["existing_child"]["candidate_id"])
            for x in members if x.get("existing_child")
        }
        if len(actions) != 1 or len(will_post) != 1 or len(existing_ids) > 1:
            raise ConfigError(f"REPAIR_SHARED_SIM_KEY_DISPOSITION_CONFLICT:{sim_key}")
        canonical = members[0]
        existing = next((x.get("existing_child") for x in members if x.get("existing_child")), None)
        child_id = str((existing or {}).get("candidate_id") or canonical["child"]["child_id"])
        units.append({
            "sim_key": sim_key,
            "members": members,
            "canonical": canonical,
            "canonical_plan_id": str(canonical["plan"]["repair_plan_id"]),
            "plan_ids": [str(x["plan"]["repair_plan_id"]) for x in members],
            "parent_candidate_ids": [str(x["parent"]["candidate_id"]) for x in members],
            "repair_types": [str(x["plan"]["repair_type"]) for x in members],
            "action": str(canonical["action"]),
            "will_post": bool(canonical["will_post"]),
            "existing_child": existing,
            "child_id": child_id,
        })
    return units


def _prepare_repair_execution(
    store: Any, config: Any, alpha_db: Path, run_id: str,
    plan_ids: Iterable[str], machine_lib: Any, *, fail_on_uncertain: bool = True,
    allow_continuous_profile: bool = False, enforce_global_repair_budget: bool = True,
) -> Dict[str, Any]:
    """Pure durable-state/cache preflight; it does not mutate workflow state."""
    ctx = validate_production_repair_context(
        store, config, run_id, allow_continuous_profile=allow_continuous_profile,
    )
    ids = list(dict.fromkeys(str(x) for x in plan_ids))
    if not ids:
        raise ConfigError("PRODUCTION_REPAIR_REQUIRES_EXPLICIT_PLAN_ID: no plan selected")
    if not initial_search_completed(store, run_id):
        raise ConfigError("INITIAL_SEARCH_NOT_COMPLETE")
    registry = _operator_registry(store)
    plans_by_id = {str(p["repair_plan_id"]): p for p in store.load_repair_plans(run_id)}
    classified: List[Dict[str, Any]] = []
    for plan_id in ids:
        plan = plans_by_id.get(plan_id)
        if plan is None:
            raise ConfigError(f"REPAIR_PLAN_NOT_FOUND: {plan_id}")
        if plan.get("run_id") != run_id:
            raise ConfigError(f"REPAIR_PLAN_RUN_MISMATCH: {plan_id}")
        if plan.get("plan_status") not in EXECUTABLE_STATUSES:
            raise ConfigError(f"REPAIR_PLAN_STATUS_NOT_EXECUTABLE: {plan_id} ({plan.get('plan_status')})")
        if int(plan.get("consumed_posts") or 0) > 0:
            raise ConfigError(f"REPAIR_PLAN_ALREADY_CONSUMED: {plan_id}")
        if is_retired_auto_repair_plan(plan):
            raise ConfigError(f"REPAIR_STRATEGY_RETIRED: {plan.get('repair_type')}")
        classified.append(_classify_plan(store, run_id, plan, config, machine_lib, alpha_db, registry))
    uncertain = [x for x in classified if x["action"] == "HOLD_UNCERTAIN"]
    if uncertain and fail_on_uncertain:
        raise ConfigError(
            "PRODUCTION_REPAIR_UNCERTAIN_SUBMISSION_HOLD:"
            + str([x["plan"]["repair_plan_id"] for x in uncertain])
        )
    units = _group_repair_execution_units(classified)
    reserve = int(ctx["repair_reserve_budget"])
    consumed = int(ctx["repair_consumed"])
    projected = sum(1 for unit in units if unit["will_post"])
    remaining = max(0, reserve - consumed) if enforce_global_repair_budget else None
    if enforce_global_repair_budget and projected > int(remaining or 0):
        blocked_payload = {
            "repair_reserve": reserve,
            "repair_consumed": consumed,
            "repair_reserve_remaining": remaining,
            "projected_new_posts": projected,
            "unique_execution_unit_count": len(units),
            "sim_keys": [str(unit["sim_key"]) for unit in units if unit["will_post"]],
        }
        _audit(store, run_id, "BUDGET_BLOCKED", blocked_payload)
        audit_event(action="BUDGET_BLOCKED", run_id=run_id, **blocked_payload)
        raise ConfigError("PRODUCTION_REPAIR_BUDGET_EXCEEDED")
    fingerprint_material = [
        {
            "sim_key": u["sim_key"], "plan_ids": u["plan_ids"],
            "action": u["action"], "will_post": u["will_post"],
        }
        for u in units
    ]
    fingerprint = hashlib.sha256(_json(fingerprint_material).encode()).hexdigest()
    return {
        "context": ctx, "plan_ids": ids, "classified": classified, "units": units,
        "reserve": reserve, "consumed_before": consumed, "remaining": remaining,
        "global_repair_budget_enforced": bool(enforce_global_repair_budget),
        "projected_new_posts": projected, "fingerprint": fingerprint,
    }


def _preview_from_preflight(run_id: str, prepared: Mapping[str, Any]) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    for classified in prepared["classified"]:
        plan = classified["plan"]; parent = classified["parent"]; spec = classified["spec"]
        unit = next(u for u in prepared["units"] if u["sim_key"] == classified["sim_key"])
        items.append({
            "run_id": run_id, "repair_plan_id": plan["repair_plan_id"],
            "parent_candidate_id": parent["candidate_id"], "parent_alpha_id": parent.get("alpha_id"),
            "current_plan_status": plan["plan_status"], "target_failure": plan["target_failure"],
            "repair_type": plan["repair_type"], "repair_depth": plan["repair_depth"],
            "parent_expression": parent.get("expression"), "repair_expression": spec.get("expression_preview"),
            "parent_sim_key": parent.get("sim_key"), "repair_sim_key": classified["sim_key"],
            "dataset_id": parent.get("dataset_id"), "field_id": parent.get("field_id"),
            "signal_family": parent.get("signal_family"),
            "operator_requirements": json.loads(plan.get("operator_requirements_json") or "[]"),
            "operator_gate": classified["operator_gate"], "cache_status": classified["cache_status"],
            "required_action": classified["action"], "needs_promotion": plan["plan_status"] in PROMOTABLE_STATUSES,
            "will_post": classified["will_post"], "reuse_existing_candidate": classified["reuse_existing"],
            "existing_child_candidate_id": (classified["existing_child"] or {}).get("candidate_id"),
            "allowed_to_execute": True, "execution_unit_plan_ids": unit["plan_ids"],
            "canonical_execution_plan_id": unit["canonical_plan_id"],
        })
    ctx = prepared["context"]
    return {
        "mode": "PRODUCTION_REPAIR_PREVIEW", "run_id": run_id,
        "selected_plan_count": len(items), "unique_execution_unit_count": len(prepared["units"]),
        "items": items, "repair_reserve": prepared["reserve"],
        "repair_consumed": prepared["consumed_before"], "repair_reserve_remaining": prepared["remaining"],
        "global_repair_budget_enforced": bool(prepared.get("global_repair_budget_enforced", True)),
        "projected_new_posts": prepared["projected_new_posts"], "projected_budget_sufficient": True,
        "execution_hash_status": ctx["execution_hash_status"],
        "execution_hash_matches": ctx["execution_hash_matches"],
        "execution_semantics_compatible": ctx["execution_semantics_compatible"],
        "matched_schema_version": ctx["matched_schema_version"], "execution_hash_reason": ctx["execution_hash_reason"],
        "simulation_posts": 0, "network_requests": 0, "check_requests": 0, "writes": 0,
    }


def preview_production_repair(store: Any, config: Any, alpha_db: Path, run_id: str,
                              plan_ids: Iterable[str], machine_lib: Any) -> Dict[str, Any]:
    """Read-only preview. Never POSTs, never mutates plan status or budget."""
    prepared = _prepare_repair_execution(
        store, config, alpha_db, run_id, plan_ids, machine_lib, fail_on_uncertain=False,
    )
    out = _preview_from_preflight(run_id, prepared)
    _audit(store, run_id, "PRODUCTION_REPAIR_PREVIEW", out)
    return out


def preview_production_repair_read_only(
    store: Any, config: Any, alpha_db: Path, run_id: str,
    plan_ids: Iterable[str], machine_lib: Any,
) -> Dict[str, Any]:
    """Side-effect-free preflight evidence for observational callers.

    Unlike the operator-facing preview, this function writes no audit row and
    never enforces the statistics-only global repair budget.  It still applies
    durable context, plan, operator, cache, no-repost, and uncertainty checks.
    """
    continuous_profile = str(config.plan.get("run_profile") or "").upper() == "CONTINUOUS_RESEARCH"
    prepared = _prepare_repair_execution(
        store, config, alpha_db, run_id, plan_ids, machine_lib,
        fail_on_uncertain=False,
        allow_continuous_profile=continuous_profile,
        enforce_global_repair_budget=False,
    )
    return _preview_from_preflight(run_id, prepared)


def _persist_child(store: Any, run_id: str, plan: Mapping[str, Any], child: Mapping[str, Any]) -> None:
    row = child["row"]
    with store.connect() as conn:
        columns = [r[1] for r in conn.execute("PRAGMA table_info(ppl_candidates)")]
        use = [c for c in columns if c in row]
        conn.execute(
            f"INSERT INTO ppl_candidates({','.join(use)}) VALUES ({','.join('?' for _ in use)})",
            [row[c] for c in use],
        )
        pid = "prov_" + hashlib.sha256(f"{run_id}|{row['candidate_id']}".encode()).hexdigest()[:24]
        payload = {
            "candidate_stage": "REPAIR", "parent_candidate_id": row["parent_candidate_id"],
            "parent_sim_key": row["parent_sim_key"], "repair_plan_id": plan["repair_plan_id"],
            "repair_signature": plan["repair_signature"], "repair_phase": PHASE,
        }
        conn.execute(
            "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (pid, row["candidate_id"], run_id, row["sim_key"], row["context_fingerprint"],
             row["discovery_snapshot_id"], row["dry_run_snapshot_id"], _json(payload), _now(), _now()),
        )
    # The canonical repair edge is persisted separately so multiple plans can
    # safely converge on the same child sim_key without losing lineage.
    # Caller records the edge because it owns the full classified context.


def _record_repair_edge(store: Any, run_id: str, c: Mapping[str, Any], child_candidate_id: str) -> None:
    """Persist the many-to-one repair lineage in the existing ppl_repairs table."""
    plan = c["plan"]
    spec = c["spec"]
    repair_id = "repair_edge_" + hashlib.sha256(
        f"{run_id}|{plan['repair_signature']}".encode()
    ).hexdigest()[:24]
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_repairs(
                   repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,
                   repair_signature,repair_path_json,repair_depth,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,repair_signature) DO UPDATE SET
                   child_candidate_id=excluded.child_candidate_id,
                   repair_type=excluded.repair_type,
                   repair_path_json=excluded.repair_path_json,
                   repair_depth=excluded.repair_depth""",
            (repair_id, run_id, c["parent"]["candidate_id"], child_candidate_id,
             plan["repair_type"], plan["repair_signature"],
             _json(spec.get("repair_path", [])), int(spec.get("repair_depth") or plan.get("repair_depth") or 1),
             _now()),
        )


def _update_repair_edge_outcome(store: Any, run_id: str, repair_signature: str,
                                *, before: Mapping[str, Any], after: Mapping[str, Any],
                                delta: Mapping[str, Any], verdict: Optional[str]) -> None:
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_repairs SET before_json=?,after_json=?,delta_json=?,side_effect_verdict=?
               WHERE run_id=? AND repair_signature=?""",
            (_json(dict(before)), _json(dict(after)), _json(dict(delta)), verdict,
             run_id, repair_signature),
        )


def _link_reused_candidate(store: Any, run_id: str, c: Mapping[str, Any]) -> None:
    """Link a canonical existing candidate to a repair without destroying lineage.

    A sim_key can be reached by more than one repair plan. ``ppl_repairs`` is the
    durable many-to-one edge table. The candidate's legacy single-parent fields
    are populated only when blank, and provenance keeps an append-only
    ``repair_links`` list instead of being repointed on every reuse.
    """
    plan = c["plan"]; parent = c["parent"]; existing = c["existing_child"]; spec = c["spec"]
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_candidates SET
                   parent_candidate_id=COALESCE(parent_candidate_id,?),
                   parent_sim_key=COALESCE(parent_sim_key,?),
                   root_candidate_id=COALESCE(root_candidate_id,?),
                   repair_path_json=CASE WHEN repair_path_json IS NULL OR repair_path_json='' THEN ? ELSE repair_path_json END,
                   repair_depth=CASE WHEN COALESCE(repair_depth,0)=0 THEN ? ELSE repair_depth END,
                   updated_at=? WHERE candidate_id=?""",
            (parent["candidate_id"], parent.get("sim_key"),
             parent.get("root_candidate_id") or parent["candidate_id"],
             _json(spec.get("repair_path", [])), spec.get("repair_depth", 1), _now(), existing["candidate_id"]),
        )
        prov = conn.execute(
            "SELECT provenance_json FROM ppl_candidate_provenance WHERE candidate_id=?",
            (existing["candidate_id"],),
        ).fetchone()
        payload = json.loads(prov["provenance_json"]) if prov and prov["provenance_json"] else {}
        links = payload.get("repair_links") if isinstance(payload.get("repair_links"), list) else []
        link = {
            "parent_candidate_id": parent["candidate_id"], "parent_sim_key": parent.get("sim_key"),
            "repair_plan_id": plan["repair_plan_id"], "repair_signature": plan["repair_signature"],
            "repair_phase": PHASE, "reuse_existing_candidate": True,
        }
        if not any(x.get("repair_plan_id") == plan["repair_plan_id"] for x in links if isinstance(x, dict)):
            links.append(link)
        payload["repair_links"] = links
        payload.setdefault("candidate_stage", "REPAIR")
        # Backward-compatible primary repair fields: set once, never repoint.
        payload.setdefault("parent_candidate_id", parent["candidate_id"])
        payload.setdefault("parent_sim_key", parent.get("sim_key"))
        payload.setdefault("repair_plan_id", plan["repair_plan_id"])
        payload.setdefault("repair_signature", plan["repair_signature"])
        payload.setdefault("repair_phase", PHASE)
        payload.setdefault("reuse_existing_candidate", True)
        if prov:
            conn.execute(
                "UPDATE ppl_candidate_provenance SET provenance_json=?,updated_at=? WHERE candidate_id=?",
                (_json(payload), _now(), existing["candidate_id"]),
            )
        else:
            pid = "prov_" + hashlib.sha256(f"{run_id}|{existing['candidate_id']}".encode()).hexdigest()[:24]
            conn.execute(
                """INSERT INTO ppl_candidate_provenance(
                       provenance_id,candidate_id,run_id,sim_key,context_fingerprint,
                       discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (pid, existing["candidate_id"], run_id, existing.get("sim_key"),
                 existing.get("context_fingerprint") or hashlib.sha256(f"{run_id}|{existing['candidate_id']}|context".encode()).hexdigest(),
                 existing.get("discovery_snapshot_id") or "UNKNOWN_DISCOVERY",
                 existing.get("dry_run_snapshot_id") or "UNKNOWN_DRY_RUN",
                 _json(payload), _now(), _now()),
            )
    _record_repair_edge(store, run_id, c, existing["candidate_id"])


def preflight_round_repair_execution(
    store: Any, config: Any, machine: Any, alpha_db: Path, machine_path: Path,
    *, run_id: str, round_id: str, batch_no: int, plan_ids: Iterable[str],
    allow_simulation_post: bool, enforce_global_repair_budget: bool = True,
    global_hold_on_uncertain: bool = True,
) -> Dict[str, Any]:
    """Authorize an orchestrator-owned Round REPAIR before any durable POST intent."""
    if not allow_simulation_post:
        raise ConfigError("ROUND_REPAIR_SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
    rr = get_round(store, round_id=round_id)
    if not rr or str(rr.get("run_id")) != str(run_id):
        raise ConfigError("ROUND_REPAIR_RUN_ROUND_OWNERSHIP_MISMATCH")
    if str(rr.get("phase") or "").upper() != "REPAIR":
        raise ConfigError("ROUND_REPAIR_PHASE_REQUIRED")
    if int(batch_no) != int(rr.get("current_batch") or 0) + 1:
        raise ConfigError("ROUND_REPAIR_BATCH_SEQUENCE_MISMATCH")
    validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_ROUND_REPAIR,
        config=config, run_id=run_id, round_id=round_id,
    )
    prepared = _prepare_repair_execution(
        store, config, alpha_db, run_id, plan_ids, machine,
        fail_on_uncertain=bool(global_hold_on_uncertain),
        allow_continuous_profile=(str(config.plan.get("run_profile") or "").upper() == "CONTINUOUS_RESEARCH"),
        enforce_global_repair_budget=enforce_global_repair_budget,
    )
    prepared["global_hold_on_uncertain"] = bool(global_hold_on_uncertain)
    token_material = {
        "version": ROUND_REPAIR_PREFLIGHT_VERSION, "run_id": run_id, "round_id": round_id,
        "batch_no": int(batch_no), "plan_ids": prepared["plan_ids"],
        "execution_fingerprint": prepared["fingerprint"],
        "execution_hash_status": prepared["context"]["execution_hash_status"],
        "repair_consumed_before": prepared["consumed_before"],
        "global_repair_budget_enforced": bool(prepared.get("global_repair_budget_enforced", True)),
        "global_hold_on_uncertain": bool(prepared.get("global_hold_on_uncertain", True)),
    }
    return {
        **token_material,
        "token_digest": hashlib.sha256(_json(token_material).encode()).hexdigest(),
        "origin": "ROUND_ORCHESTRATOR_INTERNAL",
        "preview": _preview_from_preflight(run_id, prepared),
        "_prepared": prepared,
    }


def _validated_round_preflight(
    store: Any, *, run_id: str, round_id: str, batch_no: int,
    plan_ids: Iterable[str], preflight: Mapping[str, Any],
) -> Mapping[str, Any]:
    if str(preflight.get("origin")) != "ROUND_ORCHESTRATOR_INTERNAL":
        raise ConfigError("ROUND_REPAIR_INTERNAL_PREFLIGHT_REQUIRED")
    prepared = preflight.get("_prepared")
    if not isinstance(prepared, Mapping):
        raise ConfigError("ROUND_REPAIR_INTERNAL_PREFLIGHT_REQUIRED")
    ids = list(dict.fromkeys(str(x) for x in plan_ids))
    material = {
        "version": ROUND_REPAIR_PREFLIGHT_VERSION, "run_id": run_id, "round_id": round_id,
        "batch_no": int(batch_no), "plan_ids": ids,
        "execution_fingerprint": prepared.get("fingerprint"),
        "execution_hash_status": prepared.get("context", {}).get("execution_hash_status"),
        "repair_consumed_before": prepared.get("consumed_before"),
        "global_repair_budget_enforced": bool(prepared.get("global_repair_budget_enforced", True)),
        "global_hold_on_uncertain": bool(prepared.get("global_hold_on_uncertain", True)),
    }
    if hashlib.sha256(_json(material).encode()).hexdigest() != preflight.get("token_digest"):
        raise ConfigError("ROUND_REPAIR_PREFLIGHT_BINDING_MISMATCH")
    return prepared


def execute_round_repair(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
    machine_path: Path, run_id: str, round_id: str, batch_no: int,
    plan_ids: Iterable[str], allow_simulation_post: bool, *, preflight: Mapping[str, Any],
    nonblocking_remote: bool = False,
) -> Dict[str, Any]:
    """Internal existing-Round REPAIR entry; there is deliberately no CLI route."""
    if not allow_simulation_post:
        raise ConfigError("ROUND_REPAIR_SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
    prepared = _validated_round_preflight(
        store, run_id=run_id, round_id=round_id, batch_no=batch_no,
        plan_ids=plan_ids, preflight=preflight,
    )
    post_keys = [str(u["sim_key"]) for u in prepared["units"] if u["will_post"]]
    resume_keys = [str(u["sim_key"]) for u in prepared["units"] if u["action"] == "RESUME_EXISTING"]
    start_batch(
        store, round_id, int(batch_no), "REPAIR", plan_ids=prepared["plan_ids"],
        projected_new_posts=len(post_keys), planned_post_sim_keys=post_keys,
        planned_resume_sim_keys=resume_keys,
    )
    record_event(
        store, round_id, run_id, "BATCH_STARTED", batch_no=int(batch_no), phase="REPAIR",
        payload={
            "plan_ids": prepared["plan_ids"], "projected_new_posts": len(post_keys),
            "planned_post_sim_keys": sorted(post_keys), "planned_resume_sim_keys": sorted(resume_keys),
            "unique_execution_units": len(prepared["units"]),
        },
    )
    for unit in prepared["units"]:
        key = str(unit["sim_key"]); action = str(unit["action"])
        event_type = (
            "SIMULATION_POST_INTENT" if unit["will_post"] else
            "RESUME_EXISTING_INTENT" if action == "RESUME_EXISTING" else "CACHE_HIT"
        )
        record_event(
            store, round_id, run_id, event_type, batch_no=int(batch_no), phase="REPAIR",
            candidate_id=unit["child_id"], sim_key=key,
            payload={
                "plan_ids": unit["plan_ids"], "canonical_plan_id": unit["canonical_plan_id"],
                "parent_candidate_ids": unit["parent_candidate_ids"],
                "repair_types": unit["repair_types"], "required_action": action,
            },
            source_event_key=f"repair_execution_intent:{round_id}:{batch_no}:{key}:{event_type}",
        )
    return _execute_repair_common(
        store, config, machine, session, alpha_db, run_id, prepared,
        round_id=round_id, batch_no=int(batch_no), allow_simulation_delete=True,
        nonblocking_remote=nonblocking_remote,
    )


def execute_production_repair(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                              machine_path: Path, run_id: str, plan_ids: Iterable[str],
                              allow_simulation_post: bool) -> Dict[str, Any]:
    """Explicit CLI Production Repair; its machine guard is always forced-strict."""
    if not allow_simulation_post:
        report = preview_production_repair(store, config, alpha_db, run_id, plan_ids, machine)
        report.update(executed=False, reason="SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
        return report
    validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_PRODUCTION_REPAIR,
        config=config, run_id=run_id,
    )
    prepared = _prepare_repair_execution(store, config, alpha_db, run_id, plan_ids, machine)
    return _execute_repair_common(store, config, machine, session, alpha_db, run_id, prepared)


def _execute_repair_common(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
    run_id: str, prepared: Mapping[str, Any], *, round_id: Optional[str] = None,
    batch_no: Optional[int] = None, allow_simulation_delete: bool = False,
    nonblocking_remote: bool = False,
) -> Dict[str, Any]:
    classified = list(prepared["classified"])
    units = list(prepared["units"])
    reserve = int(prepared["reserve"])
    consumed_before = int(prepared["consumed_before"])
    budget_enforced = bool(prepared.get("global_repair_budget_enforced", True))
    remaining_raw = prepared.get("remaining")
    remaining = int(remaining_raw) if remaining_raw is not None else None
    global_hold_on_uncertain = bool(prepared.get("global_hold_on_uncertain", True))
    quarantined_uncertain_plan_ids: List[str] = []
    quarantined_uncertain_sim_keys: List[str] = []
    unit_by_key = {str(u["sim_key"]): u for u in units}

    def _child_id(c: Mapping[str, Any]) -> str:
        return str(unit_by_key[str(c["sim_key"])]["child_id"])

    # Decision + resume audit events (immediately after classification).
    for c in classified:
        action = c["action"]
        child_cid = _child_id(c)
        audit_event(action=action, run_id=run_id, candidate_id=child_cid,
                    parent_candidate_id=c["parent"]["candidate_id"],
                    repair_plan_id=c["plan"]["repair_plan_id"], sim_key=c["sim_key"],
                    cache_classification=c["cache_status"], will_post=c["will_post"],
                    reuse_existing=c["reuse_existing"])
        if action == "RESUME_EXISTING":
            rec = c["cache"].get("record") or {}
            audit_event(action="SIMULATION_RESUME", run_id=run_id, candidate_id=child_cid,
                        repair_plan_id=c["plan"]["repair_plan_id"], sim_key=c["sim_key"],
                        simulation_url=rec.get("simulation_url"), previous_status=rec.get("status"))

    # Repair execution starts for every explicit plan, including cache restore and
    # resume. A cache hit is a zero-POST execution/evaluation, not a skipped plan.
    for c in classified:
        audit_event(action="REPAIR_EXECUTION_START", run_id=run_id,
                    repair_plan_id=c["plan"]["repair_plan_id"],
                    parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=_child_id(c),
                    repair_strategy=c["plan"]["repair_type"], sim_key=c["sim_key"])
        if c["plan"]["repair_type"] in RESCUE_STRATEGIES:
            audit_event(action="RESCUE_EXECUTED", run_id=run_id,
                        repair_plan_id=c["plan"]["repair_plan_id"],
                        parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=_child_id(c),
                        repair_strategy=c["plan"]["repair_type"], sim_key=c["sim_key"])

    # Budget is one logical remote execution per unique sim_key, even when
    # multiple Repair plans converge on that same candidate identity.
    for unit in [u for u in units if u["will_post"]]:
        audit_event(
            action="BUDGET_CHECK", run_id=run_id, candidate_id=unit["child_id"],
            repair_plan_ids=unit["plan_ids"], canonical_plan_id=unit["canonical_plan_id"],
            sim_key=unit["sim_key"], budget_before=consumed_before,
            projected_new_posts=1, remaining=remaining,
            allowed=((not budget_enforced) or prepared["projected_new_posts"] <= int(remaining or 0)),
        )

    # Final TOCTOU disposition remains after the durable intent boundary in a
    # Round, but before any plan/candidate mutation or remote handoff. Any unit
    # that became non-POST is explicitly removed from the effective intent.
    preflight_post_keys = {str(u["sim_key"]) for u in units if u["will_post"]}
    final_action_by_key: Dict[str, str] = {}
    final_cache_by_key: Dict[str, Dict[str, Any]] = {}
    for unit in units:
        key = str(unit["sim_key"])
        cache = classify_cache_read_only(alpha_db, key)
        action = str(cache.get("execution_action") or "STOP_INVALID")
        if str(cache.get("cache_classification") or "") == "CACHE_MISS" and unit["will_post"]:
            action = str(unit["action"] if unit["action"] in POST_ACTIONS else "NEW_SIMULATION_REQUIRED")
        final_action_by_key[key] = action
        final_cache_by_key[key] = cache
    final_post_keys = {key for key, action in final_action_by_key.items() if action in POST_ACTIONS}
    if not final_post_keys.issubset(preflight_post_keys):
        raise ConfigError("ROUND_REPAIR_TOCTOU_POST_SCOPE_EXPANSION")
    final_resume_keys = {key for key, action in final_action_by_key.items() if action == "RESUME_EXISTING"}
    if round_id is not None and batch_no is not None:
        set_batch_intent(
            store, round_id, int(batch_no),
            planned_post_sim_keys=sorted(final_post_keys),
            planned_resume_sim_keys=sorted(final_resume_keys),
        )
        with store.connect() as conn:
            conn.execute(
                "UPDATE ppl_round_batches SET projected_new_posts=? WHERE round_id=? AND batch_no=?",
                (len(final_post_keys), round_id, int(batch_no)),
            )
        for key in sorted(preflight_post_keys - final_post_keys):
            unit = unit_by_key[key]
            record_event(
                store, round_id, run_id, "REPAIR_POST_INTENT_RESOLVED_NONPOST",
                batch_no=int(batch_no), phase="REPAIR", sim_key=key,
                payload={
                    "plan_ids": unit["plan_ids"], "preflight_action": unit["action"],
                    "final_action": final_action_by_key[key],
                    "reason": "TOCTOU_NON_POST_DISPOSITION",
                },
                source_event_key=f"repair_intent_resolved:{round_id}:{batch_no}:{key}",
            )
    held = sorted(key for key, action in final_action_by_key.items() if action == "HOLD_UNCERTAIN")
    if held and global_hold_on_uncertain:
        raise ConfigError(f"PRODUCTION_REPAIR_UNCERTAIN_SUBMISSION_HOLD:{held}")
    if held:
        held_set = set(held)
        quarantined_uncertain_sim_keys = list(held)
        quarantined_uncertain_plan_ids = sorted({
            str(plan_id)
            for key in held
            for plan_id in unit_by_key[key]["plan_ids"]
        })
        now = _now()
        with store.connect() as conn:
            for plan_id in quarantined_uncertain_plan_ids:
                conn.execute(
                    """UPDATE ppl_repair_plans
                       SET plan_status='READY',committed_posts=0,consumed_posts=0,
                           blocked_reason='UNCERTAIN_SUBMISSION_HOLD',updated_at=?
                       WHERE repair_plan_id=? AND COALESCE(consumed_posts,0)=0""",
                    (now, plan_id),
                )
        for key in held:
            unit = unit_by_key[key]
            audit_event(
                action="REPAIR_UNCERTAIN_QUARANTINED", run_id=run_id,
                candidate_id=unit.get("child_id"), sim_key=key,
                repair_plan_ids=unit.get("plan_ids") or [],
                reason="UNCERTAIN_SUBMISSION_HOLD", simulation_posts=0, global_hold=False,
            )
            if round_id is not None and batch_no is not None:
                record_event(
                    store, round_id, run_id, "REPAIR_UNCERTAIN_QUARANTINED",
                    batch_no=int(batch_no), phase="REPAIR", candidate_id=unit.get("child_id"), sim_key=key,
                    payload={
                        "plan_ids": unit.get("plan_ids") or [],
                        "required_action": "HOLD_UNCERTAIN",
                        "simulation_posts": 0,
                        "global_hold": False,
                    },
                    source_event_key=f"repair_uncertain_quarantine:{round_id}:{batch_no}:{key}",
                )
        # Continuous mode keeps the uncertain identity fail-closed for re-POST
        # while quarantining only this repair unit. Remove it from the active
        # execution set so local analysis/outcome code cannot mistake the held
        # fact for an executed repair.
        classified = [c for c in classified if str(c["sim_key"]) not in held_set]
        units = [u for u in units if str(u["sim_key"]) not in held_set]
        unit_by_key = {str(u["sim_key"]): u for u in units}

    started = time.time()

    # 1) Transactional, auditable promotion of explicitly selected plans.
    for c in classified:
        plan = c["plan"]
        if plan["plan_status"] in PROMOTABLE_STATUSES:
            store.transition_repair_plan(
                plan["repair_plan_id"], "READY", reason="Explicit production repair selection",
                source=PHASE, allowed=REPAIR_PLAN_TRANSITIONS, expected_from=PROMOTABLE_STATUSES,
            )

    # 2) Persist one canonical child per execution unit, then retain every
    # plan-to-child lineage edge without creating duplicate candidate rows.
    for unit in units:
        if unit["existing_child"]:
            for c in unit["members"]:
                c["existing_child"] = unit["existing_child"]
                c["reuse_existing"] = True
                _link_reused_candidate(store, run_id, c)
        else:
            canonical = unit["canonical"]
            _persist_child(store, run_id, canonical["plan"], canonical["child"])
            for c in unit["members"]:
                _record_repair_edge(store, run_id, c, unit["child_id"])

    candidate_ids = sorted({str(u["child_id"]) for u in units})
    rows = [x for x in store.load_candidates(run_id) if x["candidate_id"] in candidate_ids]
    classified_by_key = {str(u["sim_key"]): u["canonical"] for u in units}

    # 3) Build one wrapper per unique execution unit from the already-resolved
    # final TOCTOU disposition.
    wrappers: List[Dict[str, Any]] = []
    actual_post_keys = set(final_post_keys)
    resume_keys = set(final_resume_keys)
    for r in rows:
        execution_action = final_action_by_key[str(r["sim_key"])]
        if execution_action not in POST_ACTIONS | {"RESUME_EXISTING"}:
            continue
        current = str(r.get("lifecycle_state") or "")
        if current == "PLANNED":
            store.transition_candidate(r["candidate_id"], "SIMULATION_PENDING",
                                       reason="Scheduled from explicit production repair plan",
                                       source=PHASE, allowed=CANDIDATE_TRANSITIONS)
        wrappers.append({"execution_action": execution_action,
                         "v21_candidate": _v21_candidate(r, config.target_mode)})

    if budget_enforced and len(actual_post_keys) > int(remaining or 0):
        raise ConfigError("PRODUCTION_REPAIR_BUDGET_EXCEEDED_AFTER_TOCTOU")

    # POST attempt is emitted only for candidates that will actually be handed to
    # V2.1 with a POST-capable action after the final cache check.
    for key in actual_post_keys:
        unit = unit_by_key[key]; c = unit["canonical"]
        audit_event(action="SIMULATION_POST_ATTEMPT", run_id=run_id, candidate_id=unit["child_id"],
                    parent_candidate_ids=unit["parent_candidate_ids"], repair_plan_ids=unit["plan_ids"],
                    canonical_plan_id=unit["canonical_plan_id"], sim_key=key,
                    cache_classification=c["cache_status"], budget_before=consumed_before)

    by_key = {r["sim_key"]: r["candidate_id"] for r in rows if r["sim_key"] in actual_post_keys | resume_keys}
    stats: Dict[str, Any] = {}
    methods: List[Dict[str, Any]] = []
    frame: Any = []
    if wrappers:
        if nonblocking_remote:
            handoff = execute_continuous_remote_handoff(
                store, config, machine, session, alpha_db, run_id, wrappers, by_key,
                allow_simulation_post=True,
            )
            methods = list(handoff.get("http_audit") or [])
            frame = list(handoff.get("results") or [])
            stats.update({
                "processed": len(wrappers),
                "submitted_futures": len(wrappers),
                "nonblocking_remote_handoff": True,
            })
        else:
            with _instrument_v21(
                machine, store, by_key, run_id=run_id,
                allow_simulation_delete=bool(allow_simulation_delete),
            ) as methods:
                original = machine.simulate_candidates
                def delegated(*args, **kwargs):
                    kwargs["_runtime_stats"] = stats
                    return original(*args, **kwargs)
                machine.simulate_candidates = delegated
                try:
                    frame = execute_with_v21(wrappers, config, machine, session=session,
                                             cache_db=str(alpha_db), allow_simulation_post=True,
                                             remaining_initial_budget=max(1, int(remaining or 0)))
                finally:
                    machine.simulate_candidates = original

    deferred_keys = server_slot_deferred_sim_keys(frame, actual_post_keys)
    deferred_release = _release_server_slot_deferred_repair_children(
        store, run_id, unit_by_key, deferred_keys
    ) if deferred_keys else {"candidate_ids": [], "plan_ids": [], "sim_keys": []}
    if deferred_keys:
        actual_post_keys.difference_update(deferred_keys)

    # 4) Reconcile simulation/cache facts back into every repair child.
    facts = _alpha_facts(alpha_db, [x["sim_key"] for x in rows])
    durable_progress = _durable_worker_progress_summary(stats, facts)

    # POST outcomes are restricted to logical candidates that this execution
    # actually submitted. Resume/cache facts never masquerade as POST success.
    for key in actual_post_keys:
        unit = unit_by_key[key]
        child_cid = unit["child_id"]
        fact = facts.get(key) or {}
        status = str(fact.get("status") or "").upper()
        if status == "UNCERTAIN_SUBMISSION":
            audit_event(action="SIMULATION_UNCERTAIN", run_id=run_id, candidate_id=child_cid,
                        repair_plan_ids=unit["plan_ids"], sim_key=key)
        elif status in {"COMPLETE", "RUNNING", "SUBMITTED"}:
            audit_event(action="SIMULATION_POST_SUCCESS", run_id=run_id, candidate_id=child_cid,
                        repair_plan_ids=unit["plan_ids"], sim_key=key,
                        http_status=201, simulation_url=fact.get("simulation_url"), budget_before=consumed_before)
        else:
            audit_event(action="SIMULATION_POST_FAILED", run_id=run_id, candidate_id=child_cid,
                        repair_plan_ids=unit["plan_ids"], sim_key=key,
                        http_status=None, error_type="NO_SIMULATION_FACT")

    deferred_key_set = set(deferred_keys)
    for r in rows:
        if str(r["sim_key"]) in deferred_key_set:
            continue
        _sync_candidate_fact(store, r["candidate_id"],
                             facts.get(r["sim_key"]) or {"sim_key": r["sim_key"], "status": "UNKNOWN"},
                             source=f"{PHASE}_RECONCILE")

    # 5) Budget accounting is logical-candidate based. HTTP retries are retained
    # in ``post_attempted`` for audit but never count as multiple Repair Reserve
    # consumptions for the same sim_key.
    posts = [x for x in methods if x["method"] == "POST" and x["url"].rstrip("/").endswith("/simulations")]
    attempted = len(posts)
    logical_post_keys = set(actual_post_keys)
    uncertain = sum(
        str((facts.get(k) or {}).get("status") or "").upper() == "UNCERTAIN_SUBMISSION"
        for k in logical_post_keys
    )
    confirmed = sum(
        bool((facts.get(k) or {}).get("simulation_url"))
        and str((facts.get(k) or {}).get("status") or "").upper() in {"COMPLETE", "RUNNING", "SUBMITTED"}
        for k in logical_post_keys
    )
    consumed = confirmed + uncertain
    if budget_enforced and consumed > int(remaining or 0):
        raise RuntimeError("PRODUCTION_REPAIR_BUDGET_INVARIANT_BREACH")

    with store.connect() as conn:
        for unit in units:
            fact = facts.get(unit["sim_key"]) or {}
            status = str(fact.get("status") or "").upper()
            did_post = unit["sim_key"] in logical_post_keys
            used = int(did_post and status in {"COMPLETE", "RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"})
            if unit["sim_key"] in deferred_key_set:
                plan_status, blocked_reason = "READY", "SERVER_SLOT_DEFERRED"
            elif status == "COMPLETE":
                plan_status, blocked_reason = (
                    "EXECUTED",
                    None if did_post else f"{final_action_by_key[unit['sim_key']]}_NO_POST",
                )
            elif status in {"RUNNING", "SUBMITTED"}:
                # V3.1 non-blocking execution has already crossed the remote
                # dispatch boundary.  Keep the plan non-executable until the
                # Poll Queue later produces a durable terminal Simulation
                # fact; otherwise the scheduler could select another repair
                # from the same branch while this one is still running.
                plan_status, blocked_reason = (
                    "DISPATCHED",
                    "REMOTE_EXECUTION_IN_PROGRESS" if did_post else "RESUME_STILL_IN_PROGRESS",
                )
            elif status == "UNCERTAIN_SUBMISSION":
                plan_status, blocked_reason = "READY", "UNCERTAIN_SUBMISSION_HOLD"
            elif used:
                plan_status, blocked_reason = "EXECUTED", None
            else:
                plan_status, blocked_reason = "READY", "EXECUTION_NOT_COMPLETE"
            for plan_id in unit["plan_ids"]:
                owns_consumption = int(used and plan_id == unit["canonical_plan_id"])
                shared_reason = blocked_reason
                if used and not owns_consumption:
                    shared_reason = f"SHARED_SIM_KEY_WITH:{unit['canonical_plan_id']}"
                conn.execute(
                    "UPDATE ppl_repair_plans SET plan_status=?,committed_posts=?,consumed_posts=?,blocked_reason=?,updated_at=? WHERE repair_plan_id=?",
                    (plan_status, owns_consumption, owns_consumption, shared_reason, _now(), plan_id),
                )

    # BUDGET_CONSUMED only after committed workflow state, once per logical sim_key.
    budget_cursor = consumed_before
    for key in sorted(logical_post_keys):
        unit = unit_by_key[key]
        status = str((facts.get(key) or {}).get("status") or "").upper()
        used = int(status in {"COMPLETE", "RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"})
        if used:
            audit_event(action="BUDGET_CONSUMED", run_id=run_id,
                        candidate_id=unit["child_id"], repair_plan_ids=unit["plan_ids"],
                        canonical_plan_id=unit["canonical_plan_id"], sim_key=key,
                        budget_before=budget_cursor, budget_after=budget_cursor + 1, delta=1)
            budget_cursor += 1

    # 6) Local analysis/check only candidates with a durable COMPLETE fact. Cache
    # restores therefore receive the same analysis as a freshly-completed child.
    complete_candidate_ids = [
        r["candidate_id"] for r in rows
        if str((facts.get(r["sim_key"]) or {}).get("status") or "").upper() == "COMPLETE"
    ]
    local = {"analyzed": [], "repair_plans_generated": 0, "actual_repair_posts": 0}
    if complete_candidate_ids:
        local_repair_cap = (
            reserve if budget_enforced else
            int(config.plan.get("budgets", {}).get("local_repair_planning_cap_per_cycle", 40) or 40)
        )
        local = run_local_analysis(store, config, alpha_db, run_id, audit_source=PHASE,
                                   candidate_ids=complete_candidate_ids, repair_reserve_remaining=local_repair_cap)

    # 6b) PRE-TAG handling for Local Gate PASS repair children.  Legacy
    # Production Repair keeps the audited synchronous GET-only behavior.  V3.1
    # nonblocking execution hands checks to the durable Check Queue so a pending
    # /check response cannot occupy the Repair execution path.
    check_reports: List[Dict[str, Any]] = []
    queued_check_ids: List[str] = []
    local_pass_ids = [cid for cid in local.get("local_pass_candidates", []) if cid in complete_candidate_ids]
    if nonblocking_remote and local_pass_ids:
        queued = enqueue_pretag_checks(store, run_id, local_pass_ids, source="V31_CONTINUOUS_CHECK")
        queued_check_ids = [str(x) for x in (queued.get("queued") or []) if x]
    elif session is not None:
        candidate_map = {str(x.get("candidate_id")): x for x in store.load_candidates(run_id)}
        for idx, cid in enumerate(local_pass_ids, 1):
            alpha_id = str((candidate_map.get(cid) or {}).get("alpha_id") or "")
            _repair_check_progress(idx - 1, len(local_pass_ids), candidate_id=cid, alpha_id=alpha_id, state=f"START {idx}")
            try:
                report = run_one_pretag_check(
                    store, config, machine, session, run_id, [cid],
                    source=PHASE, evidence_source="LIVE_CHECK")
                check_reports.append(report)
                state = str(report.get("session_status") or ("EXECUTED" if report.get("executed") else "DONE"))
                _repair_check_progress(idx, len(local_pass_ids), candidate_id=cid, alpha_id=alpha_id, state=state)
            except Exception as exc:  # a check failure must never abort durable repair facts
                check_reports.append({"candidate_id": cid, "executed": False,
                                      "error": f"{type(exc).__name__}: {exc}"})
                _repair_check_progress(idx, len(local_pass_ids), candidate_id=cid, alpha_id=alpha_id, state=f"ERROR {type(exc).__name__}")

    # 6c) Research verdict + durable outcome on the repair edge.
    ht_outcomes: List[Dict[str, Any]] = []
    for c in classified:
        if c["plan"]["target_failure"] != "HT_RETURNS_RATIO_FAIL":
            continue
        child_id = _child_id(c)
        outcome = evaluate_ht_repair_outcome(store, run_id, c["parent"]["candidate_id"], child_id)
        ht_outcomes.append(outcome)
        _update_repair_edge_outcome(
            store, run_id, c["plan"]["repair_signature"],
            before={"ht_ratio": outcome.get("parent_ht_ratio")},
            after={"ht_ratio": outcome.get("child_ht_ratio"), "alpha_id": (facts.get(c["sim_key"]) or {}).get("alpha_id")},
            delta={"ht_ratio": outcome.get("delta_ht_ratio"), "live_limit": outcome.get("live_limit")},
            verdict=outcome.get("verdict"),
        )
        audit_event(action="REPAIR_OUTCOME", run_id=run_id,
                    parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=child_id,
                    alpha_id=(facts.get(c["sim_key"]) or {}).get("alpha_id"),
                    repair_plan_id=c["plan"]["repair_plan_id"], repair_strategy=c["plan"]["repair_type"],
                    target_failure=c["plan"]["target_failure"],
                    parent_value=outcome.get("parent_ht_ratio"), child_value=outcome.get("child_ht_ratio"),
                    live_limit=outcome.get("live_limit"), delta=outcome.get("delta_ht_ratio"),
                    repair_verdict=outcome.get("verdict"))

    # 6d) PP-correlation controlled-branch outcome.  A Simulation COMPLETE
    # does not consume a bounded PPC strategy attempt by itself: the attempt
    # becomes empirical only after the latest PRE-TAG facts support one of the
    # durable PPC outcomes below.
    ppc_outcomes: List[Dict[str, Any]] = []
    for c in classified:
        if str(c["plan"].get("target_failure") or "").upper() != PPC_TARGET_FAILURE:
            continue
        child_id = _child_id(c)
        outcome = evaluate_ppc_repair_outcome(
            store, config, alpha_db, run_id, c["parent"]["candidate_id"], child_id,
        )
        ppc_outcomes.append(outcome)
        verdict = str(outcome.get("verdict") or "").upper()
        if verdict not in PPC_EVALUATED_OUTCOMES:
            audit_event(
                action="PPC_REPAIR_OUTCOME_PENDING", run_id=run_id,
                parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=child_id,
                repair_plan_id=c["plan"]["repair_plan_id"], repair_strategy=c["plan"]["repair_type"],
                target_failure=PPC_TARGET_FAILURE, reason=outcome.get("reason"),
            )
            continue
        payload = ppc_outcome_payload(outcome)
        _update_repair_edge_outcome(
            store, run_id, c["plan"]["repair_signature"],
            before=payload["before"], after=payload["after"], delta=payload["delta"], verdict=verdict,
        )
        audit_event(
            action="REPAIR_OUTCOME", run_id=run_id,
            parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=child_id,
            alpha_id=(facts.get(c["sim_key"]) or {}).get("alpha_id"),
            repair_plan_id=c["plan"]["repair_plan_id"], repair_strategy=c["plan"]["repair_type"],
            target_failure=PPC_TARGET_FAILURE,
            parent_value=outcome.get("parent_ppc"), child_value=outcome.get("child_ppc"),
            delta=outcome.get("delta_ppc"), repair_verdict=verdict,
            side_effect_reasons=outcome.get("side_effect_reasons"),
        )

    # 7) Rebuild derived family / priority tables from the durable facts.
    derived_rebuilt = True
    derived_error = None
    try:
        from .summary_writer import build_analytics
        build_analytics(store, run_id, config, persist=True)
    except Exception as exc:  # derived rebuild is best-effort and never blocks facts
        derived_rebuilt = False
        derived_error = f"{type(exc).__name__}: {exc}"

    # Report the durable *current* candidate state, not the stale classification
    # snapshot captured before execution/local-check transitions. This previously
    # made successful reused repair children appear as lifecycle=PLANNED even
    # after the DB had advanced them.
    current_candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    results = []
    for c in classified:
        fact = facts.get(c["sim_key"]) or {}
        resolved_candidate_id = (c["existing_child"] or {}).get("candidate_id") if c["reuse_existing"] else c["child"]["child_id"]
        current_row = current_candidates.get(resolved_candidate_id) or {}
        results.append({
            "candidate_id": resolved_candidate_id, "parent_candidate_id": c["parent"].get("candidate_id"),
            "sim_key": c["sim_key"], "status": fact.get("status"), "alpha_id": fact.get("alpha_id"),
            "simulation_url": fact.get("simulation_url"), "sharpe": fact.get("sharpe"),
            "fitness": fact.get("fitness"), "turnover": fact.get("turnover"),
            "returns": fact.get("returns"), "margin": fact.get("margin"),
            "positions": ((fact.get("long_count") or 0) + (fact.get("short_count") or 0)) if fact.get("long_count") is not None and fact.get("short_count") is not None else None,
            "disposition": final_action_by_key[c["sim_key"]],
            "lifecycle": current_row.get("lifecycle_state"),
            "candidate_simulation_status": current_row.get("simulation_status"),
        })
    out = {
        "mode": "PRODUCTION_REPAIR_EXECUTION", "run_id": run_id, "phase": PHASE,
        "kind": "REPAIR", "elapsed_seconds": time.time() - started,
        "selected_plan_count": len(classified),
        "unique_execution_unit_count": len(units),
        "machine_hash_operation": (
            MACHINE_HASH_OPERATION_ROUND_REPAIR if round_id is not None
            else MACHINE_HASH_OPERATION_PRODUCTION_REPAIR
        ),
        "execution_units": [
            {"sim_key": u["sim_key"], "plan_ids": u["plan_ids"],
             "canonical_plan_id": u["canonical_plan_id"], "child_id": u["child_id"]}
            for u in units
        ],
        "post_attempted": attempted, "post_confirmed": confirmed, "post_uncertain": uncertain,
        "post_consumed": consumed, "repair_reserve": reserve, "repair_consumed_before": consumed_before,
        "global_repair_budget_enforced": budget_enforced,
        "runtime_stats": stats, "durable_progress": durable_progress, "http_audit": methods,
        "http_methods": sorted({x["method"] for x in methods}),
        "deferred_candidate_ids": deferred_release.get("candidate_ids", []),
        "deferred_plan_ids": deferred_release.get("plan_ids", []),
        "deferred_sim_keys": deferred_release.get("sim_keys", []),
        "quarantined_uncertain_plan_ids": quarantined_uncertain_plan_ids,
        "quarantined_uncertain_sim_keys": quarantined_uncertain_sim_keys,
        "results": results, "dataframe_rows": len(frame),
        "local_analysis": local, "check_reports": check_reports,
        "queued_check_candidate_ids": queued_check_ids,
        "ht_outcomes": ht_outcomes, "ppc_outcomes": ppc_outcomes,
        "derived_rebuilt": derived_rebuilt,
    }
    if derived_error:
        out["derived_rebuild_error"] = derived_error

    # REPAIR_EXECUTION_COMPLETE audit for every selected plan.
    local_by_cid = {x["candidate_id"]: x for x in local.get("analyzed", [])}
    checked_cids = {cr.get("candidate_id") for cr in check_reports if cr.get("executed")}
    for c in classified:
        child_cid = _child_id(c)
        fact = facts.get(c["sim_key"]) or {}
        gate = ((local_by_cid.get(child_cid) or {}).get("local_pre_gate") or {}).get("status")
        audit_event(action="REPAIR_EXECUTION_COMPLETE", run_id=run_id,
                    repair_plan_id=c["plan"]["repair_plan_id"],
                    parent_candidate_id=c["parent"]["candidate_id"], child_candidate_id=child_cid,
                    repair_strategy=c["plan"]["repair_type"],
                    final_simulation_status=fact.get("status"), alpha_id=fact.get("alpha_id"),
                    local_gate=gate, check_executed=(child_cid in checked_cids))

    _audit(store, run_id, "PRODUCTION_REPAIR_EXECUTION_COMPLETE", out)
    return out


def list_repair_plans(store: Any, run_id: str, *, source: str = "all") -> Dict[str, Any]:
    """Read-only listing of deferred (local) and check-derived repair plans."""
    plans = store.load_repair_plans(run_id)
    if source == "deferred":
        plans = [p for p in plans if p.get("plan_status") in {"DEFERRED_INITIAL_SEARCH", "DEFERRED_PHASE_END"}]
    elif source == "check-derived":
        plans = [p for p in plans if p.get("target_failure") == "HT_RETURNS_RATIO_FAIL"]
    rows = []
    for p in plans:
        spec = json.loads(p.get("candidate_spec_json") or "{}")
        rows.append({
            "repair_plan_id": p["repair_plan_id"], "run_id": p.get("run_id"),
            "parent_candidate_id": p.get("parent_candidate_id"), "target_failure": p["target_failure"],
            "repair_type": p["repair_type"], "plan_status": p["plan_status"],
            "repair_depth": p["repair_depth"], "expression_preview": spec.get("expression_preview"),
            "projected_new_posts": p["projected_new_posts"], "committed_posts": p["committed_posts"],
            "consumed_posts": p["consumed_posts"], "blocked_reason": p.get("blocked_reason"),
            "created_at": p["created_at"],
        })
    return {"run_id": run_id, "source_filter": source, "plan_count": len(rows), "plans": rows}


def reconcile_completed_repair_outcomes(
    store: Any, config: Any, alpha_db: Path, run_id: str, candidate_ids: Iterable[str],
) -> Dict[str, Any]:
    """Re-evaluate repair-edge outcomes after an async Poll Queue completion.

    Non-blocking V3.1 Repair may hand off a SUBMITTED remote job and return
    before PRE-TAG evidence exists.  Once the Poll Queue later completes and
    the normal analysis/check path runs, this function revisits only outcome
    types whose existing V3 logic depends on child check evidence.
    """
    ids = sorted({str(x) for x in candidate_ids if x})
    if not ids:
        return {"evaluated": 0, "updated": 0, "pending": 0}
    marks = ",".join("?" for _ in ids)
    with store.connect() as conn:
        rows = [dict(x) for x in conn.execute(
            f"""SELECT r.parent_candidate_id,r.child_candidate_id,r.repair_signature,r.repair_type,
                       p.target_failure,p.repair_plan_id
                FROM ppl_repairs r
                LEFT JOIN ppl_repair_plans p
                  ON p.run_id=r.run_id AND p.repair_signature=r.repair_signature
                WHERE r.run_id=? AND r.child_candidate_id IN ({marks})""",
            [run_id] + ids,
        )]
    facts = _alpha_facts(alpha_db, [str(x.get("sim_key") or "") for x in store.load_candidates(run_id)])
    candidate_map = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    evaluated = 0; updated = 0; pending = 0; plans_completed = 0
    for edge in rows:
        target = str(edge.get("target_failure") or "").upper()
        parent_id = str(edge.get("parent_candidate_id") or "")
        child_id = str(edge.get("child_candidate_id") or "")
        child = candidate_map.get(child_id) or {}
        fact = facts.get(str(child.get("sim_key") or "")) or {}
        if str(fact.get("status") or "").upper() != "COMPLETE":
            continue
        # A V3.1 asynchronous Repair plan is marked DISPATCHED while its
        # remote Simulation is running.  Once the Poll Queue confirms COMPLETE,
        # advance the selected plan to EXECUTED before evaluating any
        # strategy-specific edge outcome.  Also recover the narrow legacy
        # READY + RESUME_STILL_IN_PROGRESS shape produced by an interrupted
        # development checkpoint.
        plan_id = str(edge.get("repair_plan_id") or "")
        if plan_id:
            with store.connect() as conn:
                cur = conn.execute(
                    """UPDATE ppl_repair_plans
                       SET plan_status='EXECUTED',blocked_reason=NULL,updated_at=?
                       WHERE run_id=? AND repair_plan_id=?
                         AND (plan_status='DISPATCHED' OR
                              (plan_status='READY' AND blocked_reason IN
                                  ('REMOTE_EXECUTION_IN_PROGRESS','RESUME_STILL_IN_PROGRESS')))""",
                    (_now(), run_id, plan_id),
                )
                plans_completed += int(cur.rowcount or 0)
        evaluated += 1
        if target == "HT_RETURNS_RATIO_FAIL":
            outcome = evaluate_ht_repair_outcome(store, run_id, parent_id, child_id)
            verdict = str(outcome.get("verdict") or "UNKNOWN").upper()
            if verdict == "UNKNOWN":
                pending += 1; continue
            _update_repair_edge_outcome(
                store, run_id, str(edge.get("repair_signature") or ""),
                before={"ht_ratio": outcome.get("parent_ht_ratio")},
                after={"ht_ratio": outcome.get("child_ht_ratio"), "alpha_id": fact.get("alpha_id")},
                delta={"ht_ratio": outcome.get("delta_ht_ratio"), "live_limit": outcome.get("live_limit")},
                verdict=verdict,
            )
            updated += 1
        elif target == PPC_TARGET_FAILURE:
            outcome = evaluate_ppc_repair_outcome(store, config, alpha_db, run_id, parent_id, child_id)
            verdict = str(outcome.get("verdict") or "").upper()
            if verdict not in PPC_EVALUATED_OUTCOMES:
                pending += 1; continue
            payload = ppc_outcome_payload(outcome)
            _update_repair_edge_outcome(
                store, run_id, str(edge.get("repair_signature") or ""),
                before=payload["before"], after=payload["after"], delta=payload["delta"], verdict=verdict,
            )
            updated += 1
        else:
            # Other repair types already derive their current candidate state
            # from normal local analysis/classification; no special edge metric
            # is required here.
            continue
    return {
        "evaluated": evaluated,
        "updated": updated,
        "pending": pending,
        "plans_completed": plans_completed,
    }
