"""Durable lifecycle contracts and DB-derived state checkpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .atomic import atomic_write_json


CANDIDATE_TRANSITIONS = {
    "DISCOVERED": {"PLANNED", "STOPPED", "FAILED"},
    "PLANNED": {"SIMULATION_PENDING", "SIMULATION_COMPLETE", "STOPPED", "FAILED"},
    "SIMULATION_PENDING": {"SIMULATION_RUNNING", "SIMULATION_COMPLETE", "STOPPED", "FAILED"},
    "SIMULATION_RUNNING": {"SIMULATION_COMPLETE", "STOPPED", "FAILED"},
    "SIMULATION_COMPLETE": {"SIGNAL_ANALYZED", "STOPPED", "FAILED"},
    "SIGNAL_ANALYZED": {"PRE_CHECK_REPAIR", "LOCAL_PRE_GATE_PASS", "NEAR_PASS", "STRUCTURAL_FAIL", "STOPPED", "FAILED"},
    "PRE_CHECK_REPAIR": {"LOCAL_PRE_GATE_PASS", "NEAR_PASS", "STRUCTURAL_FAIL", "STOPPED", "FAILED"},
    "LOCAL_PRE_GATE_PASS": {"PRE_TAG_CHECK_PENDING", "STOPPED", "FAILED"},
    "PRE_TAG_CHECK_PENDING": {"PRE_TAG_CHECK_COMPLETE", "STOPPED", "FAILED"},
    "PRE_TAG_CHECK_COMPLETE": {"PRE_TAG_CHECK_PASS", "CHECK_REPAIR", "NEAR_PASS", "STOPPED", "FAILED"},
    "CHECK_REPAIR": {"PRE_TAG_CHECK_PENDING", "NEAR_PASS", "STRUCTURAL_FAIL", "STOPPED", "FAILED"},
    "PRE_TAG_CHECK_PASS": {"FAMILY_DEDUP", "STOPPED", "FAILED"},
    "FAMILY_DEDUP": {"PRE_TAG_FINALIST", "STOPPED", "FAILED"},
    "PRE_TAG_FINALIST": {"DESCRIPTION_DRAFT", "NEEDS_MANUAL_DESCRIPTION", "STOPPED", "FAILED"},
    "DESCRIPTION_DRAFT": {"DESCRIPTION_VALIDATED", "NEEDS_MANUAL_DESCRIPTION", "STOPPED", "FAILED"},
    "DESCRIPTION_VALIDATED": {"AWAITING_MANUAL_PROPERTIES", "STOPPED", "FAILED"},
    "AWAITING_MANUAL_PROPERTIES": {"PPL_TAGGED", "STOPPED", "FAILED"},
    "PPL_TAGGED": {"FINAL_CHECK_PENDING", "STOPPED", "FAILED"},
    "FINAL_CHECK_PENDING": {"FINAL_CHECK_COMPLETE", "STOPPED", "FAILED"},
    "FINAL_CHECK_COMPLETE": {"FINAL_CHECK_PASS", "CORRELATION_DIAGNOSIS", "STOPPED", "FAILED"},
    "FINAL_CHECK_PASS": {"READY_FOR_MANUAL_SUBMIT", "STOPPED", "FAILED"},
    "READY_FOR_MANUAL_SUBMIT": {"SUBMITTED", "STOPPED", "FAILED"},
}

RUN_TRANSITIONS = {
    "CREATED": {"PLANNED", "STOPPED", "FAILED"},
    "PLANNED": {"RECONCILED", "STOPPED", "FAILED"},
    "RECONCILED": {"READY_FOR_EXECUTION", "COMPLETED", "STOPPED", "FAILED"},
    "READY_FOR_EXECUTION": {"EXECUTING", "PAUSED", "STOPPED", "FAILED"},
    "EXECUTING": {"PAUSED", "AWAITING_MANUAL_ACTION", "COMPLETED", "STOPPED", "FAILED"},
    "PAUSED": {"READY_FOR_EXECUTION", "EXECUTING", "STOPPED", "FAILED"},
    "AWAITING_MANUAL_ACTION": {"READY_FOR_EXECUTION", "COMPLETED", "STOPPED", "FAILED"},
}

# Production Repair plan status flow. Both local-diagnosis plans (deferred during
# initial search) and check-derived proposals become executable only after they are
# explicitly selected and validated; the workflow never bulk-promotes them.
REPAIR_PLAN_TRANSITIONS = {
    "DEFERRED_INITIAL_SEARCH": {"READY", "STOPPED"},
    "DEFERRED_PHASE_END": {"READY", "STOPPED"},
    "PLANNED": {"READY", "STOPPED"},
    "BLOCKED_OPERATOR_VALIDATION": {"READY", "STOPPED"},
    "BLOCKED_BUDGET": {"READY", "STOPPED"},
    "BLOCKED_CYCLE": {"STOPPED"},
    "BLOCKED_DEPTH": {"STOPPED"},
    "BLOCKED_STRUCTURE": {"STOPPED"},
    "READY": {"DISPATCHED", "EXECUTED", "EVALUATED_ACCEPT", "EVALUATED_REJECT", "STOPPED"},
    "DISPATCHED": {"READY", "EXECUTED", "STOPPED"},
    "EXECUTED": {"EVALUATED_ACCEPT", "EVALUATED_REJECT"},
    "EVALUATED_ACCEPT": set(),
    "EVALUATED_REJECT": {"READY"},
    "STOPPED": set(),
}


def build_initial_state(run_id: str, config: Any) -> Dict[str, Any]:
    return {
        "schema_version": 2, "run_id": run_id, "runner_goal": "PPL",
        "target_mode": config.target_mode, "atom_constraint_active": config.atom_constraint_active,
        "status": "CREATED", "current_stage": "INIT", "execution_hash": config.execution_hash,
        "operational_hash": config.operational_hash, "presentation_hash": config.presentation_hash,
        "cursor": {"dataset_id": None, "field_id": None, "candidate_id": None},
        "candidate_counts": {}, "pending_simulations": [], "pending_checks": [],
        "budget": {}, "repair_budget_used": {}, "warnings": [], "last_error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_initial_state(path: Path, run_id: str, config: Any) -> Dict[str, Any]:
    state = build_initial_state(run_id, config)
    atomic_write_json(path, state)
    return state


def quarantine_corrupt_state(path: Path) -> Path:
    target = Path(path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    quarantine = target.with_name(f"{target.name}.corrupt.{stamp}")
    target.replace(quarantine)
    return quarantine


def validate_or_quarantine_state(path: Path) -> Dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {"status": "MISSING", "quarantined": None}
    try:
        json.loads(target.read_text(encoding="utf-8"))
        return {"status": "VALID", "quarantined": None}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        quarantined = quarantine_corrupt_state(target)
        return {"status": "STATE_FILE_CORRUPT", "quarantined": str(quarantined)}
