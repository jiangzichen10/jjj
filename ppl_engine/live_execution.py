"""Phase 10A canary execution over the unchanged V2.1 simulation engine.

This module owns workflow safety and audit only.  HTTP submission, polling,
resume, slot guarding and the alpha-results cache remain V2.1 responsibilities.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from .config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    execution_hash_status_for_run,
    simulation_budget_allocation,
)
from .diagnosis import diagnose_evidence
from .repair_engine import RESCUE_STRATEGIES, plan_repairs, parameter_change_summary
from .simulation_adapter import execute_with_v21
from .state_machine import CANDIDATE_TRANSITIONS, RUN_TRANSITIONS
from .live_validation import GetOnlySession, MeteredLiveCheckTransport
from .check_transport import CheckBudget, semantic_poll_check
from .audit_log import audit_event
from .store import _emit_sqlite_write_diagnostic


PHASE = "PHASE_10A_CANARY"
TOTAL_CAP = 10
INITIAL_CAP = 6
CANARY_CAP = 2
REPAIR_RESERVE = 4
# D2 Safety Gate re-baseline (2026-08-31): the D2E baseline machine library is
# now the canonical attestation target.  Historical expected hash before this
# re-baseline: 0F8944F696EAC8481771AE1DF87EBD2F467CF69922939B46E783944E9A794762.
EXPECTED_MACHINE_HASH = "58634F1EB01880EDC88B7D9904EDF3716335C35C17D57AAA0215985D82FA34E4"

MACHINE_HASH_POLICY_STRICT = "STRICT"
MACHINE_HASH_POLICY_WARN = "WARN"
MACHINE_HASH_POLICY_OFF = "OFF"
MACHINE_HASH_POLICY_DEFAULT = MACHINE_HASH_POLICY_WARN

MACHINE_HASH_OPERATION_CLASS_CONFIGURABLE = "CONFIGURABLE"
MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT = "FORCED_STRICT"
MACHINE_HASH_OPERATION_CLASS_NO_GUARD = "NO_GUARD"

MACHINE_HASH_OPERATION_STRICT = "STRICT"  # source-compatible forced guard
MACHINE_HASH_OPERATION_START = "START_NEW_ROUND"
MACHINE_HASH_OPERATION_RESUME = "RESUME_EXISTING_ROUND"
MACHINE_HASH_OPERATION_MANUAL_REFRESH = "MANUAL_FINALIZATION_REFRESH"
MACHINE_HASH_OPERATION_ROUND_REPAIR = "ROUND_REPAIR_EXECUTION"
MACHINE_HASH_OPERATION_PRODUCTION_REPAIR = "PRODUCTION_REPAIR"
MACHINE_HASH_OPERATION_PHASE10A = "PHASE10A"
MACHINE_HASH_OPERATION_PHASE10B = "PHASE10B"
MACHINE_HASH_OPERATION_PHASE10B_REPAIR = "PHASE10B_REPAIR"
MACHINE_HASH_OPERATION_LIVE_VALIDATION = "LIVE_VALIDATION"
MACHINE_HASH_OPERATION_DESTRUCTIVE_MIGRATION = "DESTRUCTIVE_MIGRATION"
MACHINE_HASH_OPERATION_ROUND_STATUS = "ROUND_STATUS"
MACHINE_HASH_OPERATION_REBUILD_REPORTS = "REBUILD_ROUND_REPORTS"
MACHINE_HASH_OPERATION_REMOTE_RESOLUTION = "REMOTE_SIMULATION_RESOLUTION"

MACHINE_HASH_OPERATION_REGISTRY = {
    MACHINE_HASH_OPERATION_START: MACHINE_HASH_OPERATION_CLASS_CONFIGURABLE,
    MACHINE_HASH_OPERATION_RESUME: MACHINE_HASH_OPERATION_CLASS_CONFIGURABLE,
    MACHINE_HASH_OPERATION_MANUAL_REFRESH: MACHINE_HASH_OPERATION_CLASS_CONFIGURABLE,
    MACHINE_HASH_OPERATION_ROUND_REPAIR: MACHINE_HASH_OPERATION_CLASS_CONFIGURABLE,
    MACHINE_HASH_OPERATION_PRODUCTION_REPAIR: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_PHASE10A: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_PHASE10B: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_PHASE10B_REPAIR: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_LIVE_VALIDATION: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_DESTRUCTIVE_MIGRATION: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_STRICT: MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    MACHINE_HASH_OPERATION_ROUND_STATUS: MACHINE_HASH_OPERATION_CLASS_NO_GUARD,
    MACHINE_HASH_OPERATION_REBUILD_REPORTS: MACHINE_HASH_OPERATION_CLASS_NO_GUARD,
    MACHINE_HASH_OPERATION_REMOTE_RESOLUTION: MACHINE_HASH_OPERATION_CLASS_NO_GUARD,
}

# Retired 2026-08-31 with the D2 machine-hash re-baseline: 58634F... is now the
# canonical EXPECTED_MACHINE_HASH, so the former STALE_RUNNING_RECOVERY_PATCH_V1
# compatibility exception is semantically redundant and must not coexist with the
# canonical expected value.  Unknown hashes and all unlisted live operations
# remain strict / configurable under MACHINE_HASH_OPERATION_REGISTRY.
_AUDITED_MACHINE_HASH_COMPATIBILITY = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def normalize_machine_hash_policy(value: Any) -> str:
    policy = str(value or "").strip().upper()
    if policy not in {MACHINE_HASH_POLICY_STRICT, MACHINE_HASH_POLICY_WARN, MACHINE_HASH_POLICY_OFF}:
        raise ConfigError("MACHINE_HASH_POLICY_INVALID")
    return policy


def classify_machine_hash_operation(operation: Any) -> str:
    normalized = str(operation or "").strip().upper()
    try:
        return MACHINE_HASH_OPERATION_REGISTRY[normalized]
    except KeyError as exc:
        raise ConfigError(f"MACHINE_HASH_OPERATION_UNREGISTERED:{normalized or '<EMPTY>'}") from exc


def resolve_machine_hash_policy(operation: Any, config: Any = None) -> Dict[str, Any]:
    normalized_operation = str(operation or "").strip().upper()
    operation_class = classify_machine_hash_operation(normalized_operation)
    override = getattr(config, "machine_hash_policy_override", None) if config is not None else None
    runtime = getattr(config, "plan", {}).get("runtime", {}) if config is not None else {}
    runtime_value = runtime.get("machine_hash_policy") if isinstance(runtime, Mapping) else None
    if override is not None:
        requested = normalize_machine_hash_policy(override)
        source = "CLI"
    elif runtime_value is not None:
        requested = normalize_machine_hash_policy(runtime_value)
        source = "RUNTIME"
    else:
        requested = MACHINE_HASH_POLICY_DEFAULT
        source = "DEFAULT"
    effective = (
        MACHINE_HASH_POLICY_STRICT
        if operation_class == MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT else requested
    )
    return {
        "operation": normalized_operation,
        "operation_class": operation_class,
        "requested_policy": requested,
        "effective_policy": effective,
        "policy_source": source,
        "forced_strict": operation_class == MACHINE_HASH_OPERATION_CLASS_FORCED_STRICT,
    }


def validate_machine_lib_hash(
    machine_path: Path, *, operation: str = MACHINE_HASH_OPERATION_STRICT,
    resume: bool = False, config: Any = None, run_id: Optional[str] = None,
    round_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate machine-lib identity for one explicitly named operation.

    ``resume`` is retained only as a source-compatible adapter for existing
    callers/tests; new callers must pass ``operation``.
    """
    if resume:
        if operation != MACHINE_HASH_OPERATION_STRICT:
            raise ConfigError("MACHINE_LIB_HASH_OPERATION_AMBIGUOUS")
        operation = MACHINE_HASH_OPERATION_RESUME
    operation = str(operation or MACHINE_HASH_OPERATION_STRICT).upper()
    policy = resolve_machine_hash_policy(operation, config)
    if policy["operation_class"] == MACHINE_HASH_OPERATION_CLASS_NO_GUARD:
        return {
            **policy, "check_result": "NO_GUARD", "compatible_patch": False,
            "expected_hash": EXPECTED_MACHINE_HASH, "actual_hash": None,
        }
    actual_hash = _hash_file(machine_path)
    if actual_hash == EXPECTED_MACHINE_HASH:
        return {
            **policy,
            "check_result": "EXPECTED_HASH_MATCH",
            "compatible_patch": False,
            "expected_hash": EXPECTED_MACHINE_HASH,
            "actual_hash": actual_hash,
            "operation": operation,
            "mode": operation,
        }
    compatibility = _AUDITED_MACHINE_HASH_COMPATIBILITY.get(actual_hash)
    if compatibility is not None and operation in compatibility["allowed_operations"]:
        result = {
            **policy,
            "check_result": "APPROVED_COMPATIBILITY",
            "compatible_patch": True,
            "expected_hash": EXPECTED_MACHINE_HASH,
            "actual_hash": actual_hash,
            "compatible_patch_id": compatibility["compatible_patch_id"],
            "reason": compatibility["reason"],
            "operation": operation,
            "mode": operation,
        }
        audit_event(
            action="MACHINE_LIB_HASH_COMPATIBLE_PATCH", run_id=run_id, round_id=round_id,
            expected_hash=EXPECTED_MACHINE_HASH, actual_hash=actual_hash,
            compatible_patch_id=compatibility["compatible_patch_id"], reason=compatibility["reason"],
            operation=operation, mode=operation, operation_class=policy["operation_class"],
            requested_policy=policy["requested_policy"], effective_policy=policy["effective_policy"],
            policy_source=policy["policy_source"], forced_strict=policy["forced_strict"],
        )
        return result

    result = {
        **policy,
        "compatible_patch": False,
        "expected_hash": EXPECTED_MACHINE_HASH,
        "actual_hash": actual_hash,
        "operation": operation,
        "mode": operation,
    }
    if policy["effective_policy"] == MACHINE_HASH_POLICY_STRICT:
        audit_event(action="MACHINE_LIB_HASH_MISMATCH_BLOCKED", run_id=run_id, round_id=round_id, **result)
        raise ConfigError("MACHINE_LIB_HASH_MISMATCH")
    if policy["effective_policy"] == MACHINE_HASH_POLICY_WARN:
        result["check_result"] = "MISMATCH_WARN"
        print(
            "\n[MACHINE HASH WARNING]\n"
            f"Expected: {EXPECTED_MACHINE_HASH}\nActual: {actual_hash}\n"
            f"Operation: {operation}\nPolicy: WARN\nExecution will continue.",
            file=sys.stderr,
        )
        audit_event(action="MACHINE_LIB_HASH_WARNING", run_id=run_id, round_id=round_id, **result)
        return result
    result["check_result"] = "MISMATCH_OFF"
    print(
        "\n[MACHINE HASH CHECK DISABLED]\n"
        f"Operation: {operation}\nExpected: {EXPECTED_MACHINE_HASH}\nActual: {actual_hash}",
        file=sys.stderr,
    )
    audit_event(action="MACHINE_LIB_HASH_CHECK_DISABLED", run_id=run_id, round_id=round_id, **result)
    return result


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_num(value: Any) -> Optional[float]:
    """Parse a JSON-serialized numeric (string or number) to float, else None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        parsed = json.loads(str(value))
        return float(parsed) if isinstance(parsed, (int, float)) else None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def _check_result_display_num(item: Optional[Mapping[str, Any]], *,
                              normalized_key: str, raw_json_key: str,
                              raw_key: str) -> Optional[float]:
    """Read a display number without changing durable Check result semantics."""
    if not item:
        return None
    value = item.get(normalized_key)
    if value is None:
        value = item.get(raw_json_key)
    if value is None:
        value = item.get(raw_key)
    return _json_num(value)


def _alpha_facts(alpha_db: Path, sim_keys: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    keys = list(dict.fromkeys(str(x) for x in sim_keys))
    if not keys:
        return {}
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        marks = ",".join("?" for _ in keys)
        return {str(row["sim_key"]): dict(row) for row in connection.execute(
            f"SELECT * FROM alpha_results WHERE sim_key IN ({marks})", keys
        )}
    finally:
        connection.close()


def alpha_schema_snapshot(alpha_db: Path) -> Dict[str, Any]:
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    try:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )]
        schema = {name: [tuple(row) for row in connection.execute(f"PRAGMA table_info({name})")] for name in tables}
        counts = {name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in tables}
        schema_hash = hashlib.sha256(_json(schema).encode()).hexdigest()
        return {"tables": tables, "schema_hash": schema_hash, "row_counts": counts}
    finally:
        connection.close()


def validate_phase10a_config(config: Any) -> Dict[str, int]:
    plan = config.plan
    if plan.get("run_profile") != "LIVE_VALIDATION":
        raise ConfigError("PHASE10A_REQUIRES_LIVE_VALIDATION_PROFILE")
    if plan.get("validation_phase") != PHASE:
        raise ConfigError("PHASE10A_VALIDATION_PHASE_MISMATCH")
    if not plan.get("source_run_id"):
        raise ConfigError("PHASE10A_SOURCE_RUN_REQUIRED")
    allocation = simulation_budget_allocation(plan)
    total = int(plan["budgets"]["max_new_simulation_posts"])
    if total > TOTAL_CAP:
        raise ConfigError("PHASE10_TOTAL_HARD_CAP_EXCEEDED")
    if allocation["initial_search_budget"] > INITIAL_CAP:
        raise ConfigError("PHASE10_INITIAL_HARD_CAP_EXCEEDED")
    if allocation["repair_reserve_budget"] < REPAIR_RESERVE:
        raise ConfigError("PHASE10_REPAIR_RESERVE_NOT_PRESERVED")
    if int(plan["runtime"]["concurrency"]) > CANARY_CAP:
        raise ConfigError("PHASE10A_CONCURRENCY_HARD_CAP_EXCEEDED")
    if int(plan["budgets"].get("max_repair_rounds", 0)) != 0:
        raise ConfigError("PHASE10A_REPAIR_SIMULATION_FORBIDDEN")
    return {"total": total, "initial": allocation["initial_search_budget"],
            "canary": CANARY_CAP, "repair": allocation["repair_reserve_budget"]}


def _next_run_id(store: Any) -> str:
    with store.connect() as conn:
        values = [str(row[0]) for row in conn.execute("SELECT run_id FROM ppl_runs")]
    numbers = [int(v.split("_", 1)[1]) for v in values if v.startswith("run_") and v[4:].isdigit()]
    return f"run_{max(numbers, default=0) + 1:04d}"


def _candidate_sort_key(row: Mapping[str, Any]) -> Tuple[Any, ...]:
    return (int(row.get("selection_rank") or 10**9), -float(row.get("initial_selection_score") or 0), str(row["candidate_id"]))


def select_canaries(source: Iterable[Mapping[str, Any]], facts: Mapping[str, Mapping[str, Any]], count: int = 2) -> List[Dict[str, Any]]:
    eligible = []
    for raw in source:
        row = dict(raw)
        fact = facts.get(str(row.get("sim_key")))
        if not row.get("selected_for_initial_search"):
            continue
        if row.get("execution_action") != "NEW_SIMULATION_REQUIRED" or row.get("cache_classification") != "CACHE_MISS":
            continue
        if fact:
            continue
        eligible.append(row)
    eligible.sort(key=_candidate_sort_key)
    if len(eligible) < count:
        raise ConfigError("PHASE10A_INSUFFICIENT_CACHE_MISS_CANDIDATES")
    chosen = [eligible[0]]
    while len(chosen) < count:
        def diversity(row: Mapping[str, Any]) -> Tuple[int, int, int, int, Tuple[Any, ...]]:
            return (
                int(all(row.get("dataset_id") != x.get("dataset_id") for x in chosen)),
                int(all(row.get("field_id") != x.get("field_id") for x in chosen)),
                int(all(row.get("signal_family") != x.get("signal_family") for x in chosen)),
                int(all(row.get("semantic_class") != x.get("semantic_class") for x in chosen)),
                tuple(-x if isinstance(x, (int, float)) else x for x in _candidate_sort_key(row)),
            )
        remaining = [x for x in eligible if x["candidate_id"] not in {y["candidate_id"] for y in chosen}]
        # Family diversity is mandatory; dataset/field/semantic diversity then wins.
        remaining = [x for x in remaining if x.get("signal_family") not in {y.get("signal_family") for y in chosen}]
        if not remaining:
            raise ConfigError("PHASE10A_DIVERSE_CANARY_NOT_FOUND")
        remaining.sort(key=lambda x: (-diversity(x)[0], -diversity(x)[1], -diversity(x)[2], -diversity(x)[3], _candidate_sort_key(x)))
        chosen.append(remaining[0])
    return chosen


def _audit(store: Any, run_id: str, event: str, payload: Mapping[str, Any], candidate_id: Optional[str] = None, sim_key: Optional[str] = None) -> None:
    material = f"{run_id}|{event}|{candidate_id}|{sim_key}|{time.time_ns()}"
    audit_id = "live_" + hashlib.sha256(material.encode()).hexdigest()[:24]
    with store.connect() as conn:
        conn.execute(
            "INSERT INTO ppl_live_execution_audits(audit_id,run_id,validation_phase,event_type,candidate_id,sim_key,payload_json,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (audit_id, run_id, PHASE, event, candidate_id, sim_key, _json(dict(payload)), _now()),
        )


def create_validation_run(store: Any, config: Any, alpha_db: Path, *, run_id: Optional[str] = None) -> Dict[str, Any]:
    caps = validate_phase10a_config(config)
    store.initialize()
    source_run_id = str(config.plan["source_run_id"])
    source_run = store.get_run(source_run_id)
    if not source_run or source_run.get("status") != "READY_FOR_EXECUTION":
        raise ConfigError("PHASE10A_SOURCE_RUN_NOT_READY")
    source_rows = store.load_candidates(source_run_id)
    facts = _alpha_facts(alpha_db, [x["sim_key"] for x in source_rows])
    selected = select_canaries(source_rows, facts, CANARY_CAP)
    validation_run_id = run_id or _next_run_id(store)
    if store.get_run(validation_run_id):
        raise ConfigError(f"Validation run already exists: {validation_run_id}")
    store.create_run(validation_run_id, config)
    with store.connect() as conn:
        conn.execute("UPDATE ppl_runs SET source_run_id=?,validation_phase=? WHERE run_id=?", (source_run_id, PHASE, validation_run_id))
        columns = [row[1] for row in conn.execute("PRAGMA table_info(ppl_candidates)")]
        source_ids = []
        for rank, row in enumerate(selected, 1):
            clone = dict(row)
            source_id = str(row["candidate_id"])
            candidate_id = "canary_" + hashlib.sha256(f"{validation_run_id}|{source_id}".encode()).hexdigest()[:24]
            clone.update({
                "candidate_id": candidate_id, "run_id": validation_run_id,
                "source_candidate_id": source_id, "lifecycle_state": "PLANNED",
                "simulation_status": "NONE", "simulation_freshness": "UNKNOWN",
                "cache_classification": "CACHE_MISS", "execution_action": "NEW_SIMULATION_REQUIRED",
                "selected_for_initial_search": 1, "selection_rank": rank,
                "new_post_budget_consumed": 0, "alpha_id": None,
                "result_reference_json": None, "created_at": _now(), "updated_at": _now(),
            })
            insert_columns = [c for c in columns if c in clone]
            conn.execute(
                f"INSERT INTO ppl_candidates({','.join(insert_columns)}) VALUES ({','.join('?' for _ in insert_columns)})",
                [clone[c] for c in insert_columns],
            )
            provenance = conn.execute("SELECT * FROM ppl_candidate_provenance WHERE candidate_id=?", (source_id,)).fetchone()
            if provenance is None:
                raise ConfigError("PHASE10A_SOURCE_PROVENANCE_MISSING")
            prov_payload = json.loads(provenance["provenance_json"])
            prov_payload.update({"source_run_id": source_run_id, "source_candidate_id": source_id,
                                 "validation_phase": PHASE})
            prov_id = "prov_" + hashlib.sha256(f"{validation_run_id}|{source_id}".encode()).hexdigest()[:24]
            conn.execute(
                "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (prov_id, candidate_id, validation_run_id, row["sim_key"], row["context_fingerprint"],
                 row["discovery_snapshot_id"], row["dry_run_snapshot_id"], _json(prov_payload), _now(), _now()),
            )
            source_ids.append({"candidate_id": candidate_id, "source_candidate_id": source_id})
    for state in ("PLANNED", "RECONCILED", "READY_FOR_EXECUTION"):
        store.transition_run(validation_run_id, state, reason="Phase 10A validation preparation", source="PHASE10A", allowed=RUN_TRANSITIONS)
    report = {"validation_run_id": validation_run_id, "source_run_id": source_run_id,
              "phase": PHASE, "caps": caps, "candidates": source_ids}
    _audit(store, validation_run_id, "VALIDATION_RUN_CREATED", report)
    return report


def _preview_row(row: Mapping[str, Any], fact: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    status = str((fact or {}).get("status") or "NONE").upper()
    if not fact:
        cache_status, action = "CACHE_MISS", "NEW_SIMULATION_REQUIRED"
    elif status == "COMPLETE":
        cache_status, action = "CACHE_COMPLETE", "CACHE_RESTORE"
    elif status in {"RUNNING", "SUBMITTED"} and fact.get("simulation_url"):
        cache_status, action = f"RESUME_{status}", "RESUME_EXISTING"
    elif status == "UNCERTAIN_SUBMISSION":
        cache_status, action = "CACHE_UNCERTAIN", "HOLD_UNCERTAIN"
    else:
        cache_status, action = f"CACHE_{status}", "RETRY_PER_V21_POLICY"
    return {"candidate_id": row["candidate_id"], "source_candidate_id": row.get("source_candidate_id"),
            "dataset": row.get("dataset_id"), "field": row.get("field_id"),
            "semantic_class": row.get("semantic_class"), "signal_family": row.get("signal_family"),
            "transform": row.get("operator"), "window": row.get("window"), "expression": row.get("expression"),
            "sim_key": row.get("sim_key"), "cache_status": cache_status, "execution_action": action}


def preview_phase10a(store: Any, config: Any, alpha_db: Path, run_id: str) -> Dict[str, Any]:
    caps = validate_phase10a_config(config)
    run = store.get_run(run_id)
    if not run or run.get("validation_phase") != PHASE or run.get("source_run_id") != config.plan.get("source_run_id"):
        raise ConfigError("PHASE10A_RUN_IDENTITY_MISMATCH")
    compat = execution_hash_status_for_run(config, run)
    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:
        raise ConfigError(f"EXECUTION_HASH_{compat['status']}: {compat['reason']}")
    rows = store.load_candidates(run_id)
    if len(rows) != CANARY_CAP or len({x["sim_key"] for x in rows}) != CANARY_CAP:
        raise ConfigError("PHASE10A_REQUIRES_EXACTLY_TWO_UNIQUE_CANARIES")
    with store.connect() as conn:
        snapshot_ids = {r[0] for r in conn.execute("SELECT snapshot_id FROM ppl_discovery_snapshots")}
        provenance_count = conn.execute("SELECT COUNT(*) FROM ppl_candidate_provenance WHERE run_id=?", (run_id,)).fetchone()[0]
    if provenance_count != CANARY_CAP or any(x.get("discovery_snapshot_id") not in snapshot_ids for x in rows):
        raise ConfigError("PHASE10A_PROVENANCE_OR_SNAPSHOT_MISMATCH")
    facts = _alpha_facts(alpha_db, [x["sim_key"] for x in rows])
    candidates = [_preview_row(x, facts.get(x["sim_key"])) for x in sorted(rows, key=lambda x: x.get("selection_rank") or 0)]
    new_posts = sum(x["execution_action"] in {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"} for x in candidates)
    report = {"validation_run_id": run_id, "source_run_id": run["source_run_id"], "phase": PHASE,
              "selected_canary_candidates": len(candidates), "candidates": candidates,
              "estimated_new_posts": new_posts, "phase10_total_budget": caps["total"],
              "initial_budget": caps["initial"], "phase10a_cap": caps["canary"],
              "repair_reserve": caps["repair"], "repair_posts": 0}
    _audit(store, run_id, "FINAL_PREVIEW", report)
    return report


def _v21_candidate(row: Mapping[str, Any], target_mode: str) -> Dict[str, Any]:
    """Build a V2.1 candidate while carrying V2.2 execution identity metadata.

    ``settings_json`` is durable per-candidate truth. The unchanged V2.1 engine
    accepts settings at call scope, so ``simulation_adapter.execute_with_v21``
    consumes the private metadata below to group calls and assert that the
    actual settings reproduce the stored sim_key.
    """
    vector = str(row.get("vector_reducer") or "IDENTITY")
    settings = {}
    raw_settings = row.get("settings_json")
    if isinstance(raw_settings, Mapping):
        settings = dict(raw_settings)
    elif raw_settings:
        try:
            settings = json.loads(str(raw_settings))
        except (TypeError, ValueError, json.JSONDecodeError):
            settings = {}
    decay = settings.get("decay", row.get("decay", 0))
    return {
        "expr": row["expression"], "field": row.get("field_id"), "data_fields": [row.get("field_id")],
        "dataset_id": row.get("dataset_id"), "dataset_ids": [row.get("dataset_id")],
        "field_type": row.get("field_type"), "vector_op": None if vector == "IDENTITY" else vector.lower(),
        "operator": row.get("operator"), "window": row.get("window"), "decay": int(decay or 0),
        "stage": "PPL_INITIAL", "target_mode": target_mode,
        "_v22_settings": settings or None, "_expected_sim_key": row.get("sim_key"),
    }


def _sync_candidate_fact(store: Any, candidate_id: str, result: Mapping[str, Any], *, source: str) -> None:
    status = str(result.get("status") or "UNKNOWN").upper()
    with store.connect(stage="SYNC_CANDIDATE_FACT") as conn:
        row = conn.execute("SELECT run_id,lifecycle_state FROM ppl_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if row is None:
            return
        old = str(row["lifecycle_state"]); target = old
        if status in {"SUBMITTED", "RUNNING"} and old == "SIMULATION_PENDING":
            target = "SIMULATION_RUNNING"
        elif status == "COMPLETE" and old in {"PLANNED", "SIMULATION_PENDING", "SIMULATION_RUNNING"}:
            target = "SIMULATION_COMPLETE"
        # Budget consumption is a confirmed POST fact, not a lifecycle label.
        consumed = int(bool(result.get("simulation_url") and result.get("submitted_at")))
        reference = {k: result.get(k) for k in ("sim_key", "alpha_id", "simulation_url", "status", "updated_at")}
        conn.execute(
            "UPDATE ppl_candidates SET lifecycle_state=?,simulation_status=?,alpha_id=?,execution_action=?,cache_classification=?,new_post_budget_consumed=max(new_post_budget_consumed,?),result_reference_json=?,updated_at=? WHERE candidate_id=?",
            (target, status, result.get("alpha_id"), "CACHE_RESTORE" if status == "COMPLETE" else "RESUME_EXISTING" if status in {"SUBMITTED", "RUNNING", "STALE_RUNNING"} else "HOLD_UNCERTAIN" if status == "UNCERTAIN_SUBMISSION" else "HOLD_REMOTE_NOT_FOUND" if status == "REMOTE_NOT_FOUND" else "NEEDS_REVIEW",
             "CACHE_COMPLETE" if status == "COMPLETE" else f"CACHE_{status}", consumed, _json(reference), _now(), candidate_id),
        )
        if target != old:
            conn.execute(
                "INSERT INTO ppl_state_transitions(run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at) VALUES (?,?,'CANDIDATE',?,?,?,?,?,?)",
                (row["run_id"], candidate_id, old, target, f"V2.1 cache status {status}", source, _json(reference), _now()),
            )
            audit_event(
                action="STATE_TRANSITION", run_id=row["run_id"], candidate_id=candidate_id,
                entity_type="CANDIDATE", old_state=old, new_state=target,
                reason=f"V2.1 cache status {status}", source=source,
            )
            if target == "SIMULATION_COMPLETE":
                audit_event(
                    action="SIMULATION_COMPLETE", run_id=row["run_id"], candidate_id=candidate_id,
                    sim_key=result.get("sim_key"), simulation_url=result.get("simulation_url"),
                    alpha_id=result.get("alpha_id"), sharpe=result.get("sharpe"),
                    fitness=result.get("fitness"), turnover=result.get("turnover"),
                    returns=result.get("returns"), margin=result.get("margin"),
                    positions=((result.get("long_count") or 0) + (result.get("short_count") or 0))
                    if result.get("long_count") is not None and result.get("short_count") is not None else None,
                )


def run_local_analysis(store: Any, config: Any, alpha_db: Path, run_id: str, *, audit_source: str = "PHASE10A", candidate_ids: Optional[Iterable[str]] = None, repair_reserve_remaining: Optional[int] = None) -> Dict[str, Any]:
    """Analyze COMPLETE canaries and persist bounded, non-executing repair plans."""
    reserve = REPAIR_RESERVE if repair_reserve_remaining is None else int(repair_reserve_remaining)
    selected_ids = set(candidate_ids or [])
    rows = [x for x in store.load_candidates(run_id) if not selected_ids or x["candidate_id"] in selected_ids]
    facts = _alpha_facts(alpha_db, [x["sim_key"] for x in rows])
    with store.connect() as conn:
        registry = {str(r[0]): str(r[1]) for r in conn.execute(
            "SELECT operator_name,status FROM ppl_operator_capabilities"
        )}
    analyzed = []; repair_plans = []; local_pass_candidates = []
    for row in rows:
        fact = facts.get(str(row["sim_key"])) or {}
        if str(fact.get("status") or "").upper() != "COMPLETE":
            continue
        # A durable COMPLETE fact may be discovered from the shared alpha cache
        # before this run has advanced the candidate workflow lifecycle.  Local
        # analysis must never jump directly from PLANNED/SIMULATION_* into a
        # post-analysis state (e.g. PRE_CHECK_REPAIR).  Reconcile the durable
        # Simulation fact first so the audited state path remains:
        # PLANNED -> SIMULATION_COMPLETE -> SIGNAL_ANALYZED -> ...
        lifecycle_state = str(row.get("lifecycle_state") or "")
        if lifecycle_state in {"PLANNED", "SIMULATION_PENDING", "SIMULATION_RUNNING"}:
            _sync_candidate_fact(
                store, row["candidate_id"], {**fact, "sim_key": row.get("sim_key")},
                source=f"{audit_source}_LOCAL_ANALYSIS_RECONCILE",
            )
            with store.connect() as conn:
                refreshed = conn.execute(
                    "SELECT lifecycle_state FROM ppl_candidates WHERE candidate_id=?",
                    (row["candidate_id"],),
                ).fetchone()
            lifecycle_state = str(refreshed[0]) if refreshed is not None else lifecycle_state
        if lifecycle_state == "SIMULATION_COMPLETE":
            store.transition_candidate(row["candidate_id"], "SIGNAL_ANALYZED", reason=f"{audit_source} local signal analysis", source=audit_source, allowed=CANDIDATE_TRANSITIONS)
            lifecycle_state = "SIGNAL_ANALYZED"

        # Local analysis is a one-way/idempotent workflow stage.  Continuous
        # startup/poll cycles may surface the same durable COMPLETE fact more
        # than once.  Never regress a candidate that has already advanced past
        # SIGNAL_ANALYZED (the production failure was PRE_TAG_CHECK_PENDING ->
        # LOCAL_PRE_GATE_PASS).  LOCAL_PRE_GATE_PASS is the only already-
        # analyzed state that still needs to be returned to the caller so an
        # interrupted cycle can enqueue its PRE_TAG check exactly once.
        if lifecycle_state == "LOCAL_PRE_GATE_PASS":
            local_pass_candidates.append(str(row["candidate_id"]))
            continue
        already_analyzed_states = {
            "PRE_CHECK_REPAIR", "NEAR_PASS", "STRUCTURAL_FAIL",
            "PRE_TAG_CHECK_PENDING", "PRE_TAG_CHECK_COMPLETE", "PRE_TAG_CHECK_PASS",
            "CHECK_REPAIR", "FAMILY_DEDUP", "PRE_TAG_FINALIST",
            "DESCRIPTION_DRAFT", "DESCRIPTION_VALIDATED",
            "AWAITING_MANUAL_PROPERTIES", "PPL_TAGGED",
            "FINAL_CHECK_PENDING", "FINAL_CHECK_COMPLETE", "FINAL_CHECK_PASS",
            "READY_FOR_MANUAL_SUBMIT", "SUBMITTED", "STOPPED", "FAILED",
        }
        if lifecycle_state in already_analyzed_states:
            continue
        if lifecycle_state != "SIGNAL_ANALYZED":
            raise ValueError(
                f"LOCAL_ANALYSIS_UNEXPECTED_STATE: {row['candidate_id']}:{lifecycle_state}"
            )

        metrics = {k: fact.get(k) for k in ("sharpe", "fitness", "turnover", "returns", "margin", "long_count", "short_count")}
        analysis_candidate = dict(row)
        analysis_candidate["data_field_count_estimate"] = analysis_candidate.get("data_field_count_estimate") or 1
        analysis_candidate["pp_total_operator_count_estimate"] = analysis_candidate.get("pp_total_operator_count_estimate") or 0
        diagnosis = diagnose_evidence({"run_id": run_id, "candidate_id": row["candidate_id"],
                                      "alpha_id": fact.get("alpha_id"),
                                      "metrics": metrics, "phase": "SIMULATION",
                                      "evidence_source": "LIVE_SIMULATION", "candidate": analysis_candidate}, config.rules)
        with store.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ppl_diagnoses(diagnosis_id,run_id,candidate_id,alpha_id,source_phase,evidence_source,primary_failure,secondary_failures_json,severity,repairability,root_cause,metrics_snapshot_json,check_session_id,check_result_ids_json,diagnosis_rule_version,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (diagnosis["diagnosis_id"], run_id, row["candidate_id"], fact.get("alpha_id"), "SIMULATION",
                 "LIVE_SIMULATION", diagnosis["primary_failure"], _json(diagnosis["secondary_failures"]),
                 diagnosis["severity"], diagnosis["repairability"], diagnosis["root_cause"], _json(metrics),
                 None, "[]", diagnosis["diagnosis_rule_version"], _now()),
            )
        local = diagnosis["local_pre_gate"]
        if local["status"] == "PASS":
            store.transition_candidate(row["candidate_id"], "LOCAL_PRE_GATE_PASS", reason="Local pre-gate passed", source=audit_source, allowed=CANDIDATE_TRANSITIONS)
            local_pass_candidates.append(str(row["candidate_id"]))
            audit_event(action="LOCAL_GATE_PASS", run_id=run_id, candidate_id=row["candidate_id"],
                        alpha_id=fact.get("alpha_id"), gate_status="PASS",
                        turnover_class=local.get("turnover_class"))
        else:
            repair = plan_repairs(row, diagnosis, config.rules, registry=registry,
                                  repair_reserve_remaining=reserve)
            plans = repair.get("plans", [])
            target = "PRE_CHECK_REPAIR" if plans else "STRUCTURAL_FAIL" if diagnosis["repairability"] == "STRUCTURAL" else "NEAR_PASS"
            store.transition_candidate(row["candidate_id"], target, reason=diagnosis["primary_failure"], source=audit_source, allowed=CANDIDATE_TRANSITIONS)
            failed_checks = [name for name, ok in (local.get("checks") or {}).items() if not ok]
            audit_event(action="LOCAL_GATE_FAIL", run_id=run_id, candidate_id=row["candidate_id"],
                        alpha_id=fact.get("alpha_id"), gate_status="FAIL",
                        turnover_class=local.get("turnover_class"), failed_checks=failed_checks)
            for item in plans:
                plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{item['repair_signature']}".encode()).hexdigest()[:24]
                with store.connect() as conn:
                    conn.execute(
                        "INSERT OR IGNORE INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,blocked_reason,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (plan_id, diagnosis["diagnosis_id"], run_id, row["candidate_id"], row.get("root_candidate_id") or row["candidate_id"],
                         diagnosis["primary_failure"], item["repair_type"], item["repair_signature"], _json(item.get("repair_path", [])),
                         item["repair_depth"], _json(item), _json(item.get("operator_requirements", [])), item.get("plan_status", "PLANNED"),
                         int(item.get("projected_new_post", 0)), 0, 0, item.get("stop_reason"), _now(), _now()),
                    )
                audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id,
                            repair_plan_id=plan_id, parent_candidate_id=row["candidate_id"],
                            candidate_id=row.get("candidate_id"), repair_strategy=item["repair_type"],
                            target_failure=diagnosis["primary_failure"],
                            parameter_change=parameter_change_summary(item, row))
                if item["repair_type"] in RESCUE_STRATEGIES:
                    audit_event(action="RESCUE_PLAN_CREATED", run_id=run_id,
                                repair_plan_id=plan_id, parent_candidate_id=row["candidate_id"],
                                candidate_id=row.get("candidate_id"), repair_strategy=item["repair_type"],
                                target_failure=diagnosis["primary_failure"],
                                parameter_change=parameter_change_summary(item, row))
                repair_plans.append({"candidate_id": row["candidate_id"], "repair_type": item["repair_type"],
                                     "status": item.get("plan_status"), "projected_new_posts": item.get("projected_new_post", 0)})
        analyzed.append({"candidate_id": row["candidate_id"], "alpha_id": fact.get("alpha_id"),
                         "local_pre_gate": local, "diagnosis": diagnosis["primary_failure"]})
    return {"analyzed": analyzed, "repair_plans": repair_plans,
            "repair_plans_generated": len(repair_plans), "actual_repair_posts": 0,
            "local_pass_candidates": list(dict.fromkeys(local_pass_candidates))}


def run_one_pretag_check(store: Any, config: Any, machine: Any, session: Any, run_id: str,
                         candidate_ids: List[str], *, source: str = "PHASE10A",
                         evidence_source: str = "LIVE_VALIDATION") -> Dict[str, Any]:
    if not candidate_ids:
        return {"executed": False, "reason": "NO_LOCAL_GATE_PASS_CANARY", "count": 0}
    candidate_id = candidate_ids[0]
    row = next(x for x in store.load_candidates(run_id) if x["candidate_id"] == candidate_id)
    if not row.get("alpha_id"):
        return {"executed": False, "reason": "LOCAL_PASS_ALPHA_ID_MISSING", "count": 0}
    store.transition_candidate(candidate_id, "PRE_TAG_CHECK_PENDING", reason="GET-only pre-tag check", source=source, allowed=CANDIDATE_TRANSITIONS)
    audit_event(action="PRETAG_CHECK_START", run_id=run_id, candidate_id=candidate_id,
                alpha_id=row["alpha_id"], source=source)
    plan_budgets = config.plan.get("budgets", {})
    runtime = config.plan.get("runtime", {})
    per_candidate_polls = max(1, int(plan_budgets.get("max_poll_requests_per_candidate", 8)))
    safe = GetOnlySession(session, max_requests=per_candidate_polls)
    transport = MeteredLiveCheckTransport(safe, machine, runtime=runtime)
    budget = CheckBudget(1, per_candidate_polls, per_candidate_polls, 1)
    check = semantic_poll_check(
        transport, alpha_id=row["alpha_id"], phase="PRE_TAG", rules=config.rules,
        budget=budget, candidate_id=candidate_id, run_id=run_id,
        evidence_source=evidence_source, wait=time.sleep, store=store,
        throttle_max_events=max(1, int(runtime.get("check_429_max_events_per_session", 4))),
    )
    store.transition_candidate(candidate_id, "PRE_TAG_CHECK_COMPLETE", reason=check["session_status"], source=source, allowed=CANDIDATE_TRANSITIONS)
    final = check.get("final") or {}
    base = (final.get("base_gate") or {}).get("status")
    theme = (final.get("theme_gate") or {}).get("status")
    passed = check.get("session_status") == "RESOLVED" and base == "PASS" and theme == "PASS"
    if passed:
        store.transition_candidate(candidate_id, "PRE_TAG_CHECK_PASS", reason="Resolved live pre-tag gates passed", source=source, allowed=CANDIDATE_TRANSITIONS)

    # Audit summary: only key checks, never the full /check JSON (which stays in DB).
    results = {str(x.get("normalized_name")): x for x in final.get("results", [])}
    ht = results.get("HIGH_TURNOVER_RETURNS_RATIO") or results.get("HT_HIGH_TURNOVER_RETURNS_RATIO")
    pp = results.get("POWER_POOL_CORRELATION")
    sub = results.get("SUB_UNIVERSE") or results.get("LOW_SUB_UNIVERSE_SHARPE")
    summary = {
        "run_id": run_id, "candidate_id": candidate_id, "alpha_id": row["alpha_id"],
        "session_status": check.get("session_status"), "poll_count": check.get("poll_count"),
        "http_request_count": check.get("http_request_count"), "base_gate": base, "theme_gate": theme,
        "ht_ratio_value": _json_num(ht.get("raw_value_json")) if ht else None,
        "ht_ratio_limit": _json_num(ht.get("raw_limit_json")) if ht else None,
        "ht_ratio_outcome": ht.get("eligibility_outcome") if ht else None,
        "pp_corr_value": _json_num(pp.get("raw_value_json")) if pp else None,
        "pp_corr_limit": _json_num(pp.get("raw_limit_json")) if pp else None,
        "pp_corr_outcome": pp.get("eligibility_outcome") if pp else None,
        "sub_universe_value": _json_num(sub.get("raw_value_json")) if sub else None,
        "sub_universe_limit": _json_num(sub.get("raw_limit_json")) if sub else None,
        "sub_universe_outcome": sub.get("eligibility_outcome") if sub else None,
    }
    status = str(check.get("session_status") or "").upper()
    # WARNING is a resolved outcome, NOT a failure. Only genuinely unresolved /
    # errored / pending sessions are logged as PRETAG_CHECK_FAILED.
    if status == "RESOLVED":
        audit_event(action="PRETAG_CHECK_COMPLETE", **summary)
    else:
        audit_event(action="PRETAG_CHECK_FAILED", **summary,
                    error_type=check.get("error_type"), error_nature=check.get("error_nature"))

    return {"executed": True, "count": 1, "candidate_id": candidate_id, "alpha_id": row["alpha_id"],
            "session_status": check.get("session_status"), "poll_count": check.get("poll_count"),
            "http_request_count": check.get("http_request_count"), "base_gate": base, "theme_gate": theme,
            "raw_names": [x.get("raw_name") for x in final.get("results", [])],
            "eligibility_outcomes": {x.get("raw_name"): x.get("eligibility_outcome") for x in final.get("results", [])},
            "passed": passed, "http_methods": sorted(set(safe.methods)), "http_statuses": dict(safe.statuses)}


def refresh_one_pretag_check(store: Any, config: Any, machine: Any, session: Any, run_id: str,
                             candidate_id: str, *, source: str = "MANUAL_FINALIZATION_REFRESH",
                             evidence_source: str = "LIVE_CHECK_REFRESH",
                             poll_observer: Optional[Callable[[Mapping[str, Any]], None]] = None,
                             min_retry_after_seconds: float = 0.5) -> Dict[str, Any]:
    """Refresh one existing Alpha's PRE_TAG /check without changing lifecycle.

    This is deliberately GET-only. It appends a new durable check session so
    the classifier naturally sees the latest platform facts, but it does not
    transition workflow state, simulate, PATCH properties, or submit anything.
    """
    row = next((x for x in store.load_candidates(run_id) if x["candidate_id"] == candidate_id), None)
    if not row:
        return {"executed": False, "reason": "CANDIDATE_NOT_FOUND", "candidate_id": candidate_id, "count": 0}
    if not row.get("alpha_id"):
        return {"executed": False, "reason": "ALPHA_ID_MISSING", "candidate_id": candidate_id, "count": 0}

    audit_event(action="MANUAL_FINALIZATION_CHECK_REFRESH_START", run_id=run_id,
                candidate_id=candidate_id, alpha_id=row["alpha_id"], source=source)
    plan_budgets = config.plan.get("budgets", {})
    runtime = config.plan.get("runtime", {})
    per_candidate_polls = max(1, int(plan_budgets.get("max_poll_requests_per_candidate", 8)))
    safe = GetOnlySession(session, max_requests=per_candidate_polls)
    transport = MeteredLiveCheckTransport(safe, machine, runtime=runtime)
    budget = CheckBudget(1, per_candidate_polls, per_candidate_polls, 1)
    check = semantic_poll_check(
        transport, alpha_id=row["alpha_id"], phase="PRE_TAG", rules=config.rules,
        budget=budget, candidate_id=candidate_id, run_id=run_id,
        evidence_source=evidence_source, wait=time.sleep, store=store,
        throttle_max_events=max(1, int(runtime.get("check_429_max_events_per_session", 4))),
        poll_observer=poll_observer,
        min_retry_after_seconds=min_retry_after_seconds,
    )
    final = check.get("final") or {}
    results = {str(x.get("normalized_name")): x for x in final.get("results", [])}
    pp = results.get("POWER_POOL_CORRELATION")
    sub = results.get("SUB_UNIVERSE") or results.get("LOW_SUB_UNIVERSE_SHARPE")
    theme = (final.get("theme_gate") or {}).get("status")
    base = (final.get("base_gate") or {}).get("status")
    summary = {
        "run_id": run_id, "candidate_id": candidate_id, "alpha_id": row["alpha_id"],
        "session_status": check.get("session_status"), "poll_count": check.get("poll_count"),
        "http_request_count": check.get("http_request_count"), "base_gate": base, "theme_gate": theme,
        "pp_corr_value": _check_result_display_num(
            pp, normalized_key="normalized_value", raw_json_key="raw_value_json", raw_key="raw_value",
        ),
        "pp_corr_limit": _check_result_display_num(
            pp, normalized_key="normalized_limit", raw_json_key="raw_limit_json", raw_key="raw_limit",
        ),
        "pp_corr_outcome": pp.get("eligibility_outcome") if pp else None,
        "sub_universe_value": _json_num(sub.get("raw_value_json")) if sub else None,
        "sub_universe_limit": _json_num(sub.get("raw_limit_json")) if sub else None,
        "sub_universe_outcome": sub.get("eligibility_outcome") if sub else None,
    }
    status = str(check.get("session_status") or "").upper()
    audit_event(
        action="MANUAL_FINALIZATION_CHECK_REFRESH_COMPLETE" if status == "RESOLVED"
               else "MANUAL_FINALIZATION_CHECK_REFRESH_FAILED",
        **summary, source=source, error_type=check.get("error_type"), error_nature=check.get("error_nature"),
    )
    return {
        "executed": True, "count": 1, "candidate_id": candidate_id, "alpha_id": row["alpha_id"],
        "session_status": check.get("session_status"), "poll_count": check.get("poll_count"),
        "http_request_count": check.get("http_request_count"), "base_gate": base, "theme_gate": theme,
        "error_type": check.get("error_type"), "error_nature": check.get("error_nature"),
        "transient_retry_seen": bool(check.get("transient_retry_seen")),
        "last_transient_error": check.get("last_transient_error"),
        "pp_corr_value": summary["pp_corr_value"], "pp_corr_limit": summary["pp_corr_limit"],
        "pp_corr_outcome": summary["pp_corr_outcome"],
        "sub_universe_outcome": summary["sub_universe_outcome"],
        "raw_names": [x.get("raw_name") for x in final.get("results", [])],
        "eligibility_outcomes": {x.get("raw_name"): x.get("eligibility_outcome") for x in final.get("results", [])},
        "http_methods": sorted(set(safe.methods)), "http_statuses": dict(safe.statuses),
    }


def _endpoint_type(method: str, url: str) -> str:
    u = str(url).rstrip("/")
    m = str(method).upper()
    if m == "POST" and u.endswith("/simulations"):
        return "SIMULATION_POST"
    if "/authentication" in u:
        return "AUTHENTICATION"
    if u.endswith("/check"):
        return "ALPHA_CHECK"
    if "/simulations/" in u:
        return "SIMULATION_POLL"
    if "/alphas/" in u:
        return "ALPHA_GET"
    return "HTTP"


def _is_brain_authentication_url(machine: Any, url: str) -> bool:
    """Allow only the machine-lib's own BRAIN authentication endpoint tree.

    ``machine_lib_V2_1.ensure_session`` creates a fresh requests.Session and
    POSTs to ``BRAIN_API_URL/authentication`` after a 401/403.  The V3 network
    firewall is installed at requests.Session.request level, so without this
    narrow exception a legitimate session refresh is mistaken for an
    unexpected write.  Host + scheme + path are all constrained to the
    machine-lib's configured BRAIN API root; arbitrary POST endpoints remain
    forbidden.
    """
    base = str(getattr(machine, "BRAIN_API_URL", "") or "").strip()
    if not base:
        return False
    try:
        target = urlsplit(str(url))
        root = urlsplit(base)
    except Exception:
        return False
    if (target.scheme.lower(), target.netloc.lower()) != (root.scheme.lower(), root.netloc.lower()):
        return False
    root_path = root.path.rstrip("/")
    auth_root = (root_path + "/authentication").rstrip("/")
    target_path = target.path.rstrip("/")
    return target_path == auth_root or target_path.startswith(auth_root + "/")


def _http_audit(run_id: Optional[str], method: str, url: str, status: Optional[int],
                elapsed_ms: float, *, exception: Optional[BaseException] = None,
                retry_count: int = 0, retry_after: Any = None) -> None:
    """Emit transport-level HTTP audit events (never credentials / headers)."""
    endpoint = _endpoint_type(method, url)
    if status == 429:
        audit_event(action="HTTP_429", run_id=run_id, endpoint_type=endpoint,
                    http_method=str(method).upper(), http_status=429,
                    retry_count=retry_count, elapsed_ms=elapsed_ms,
                    retry_after_seconds=retry_after)
    elif exception is not None:
        is_timeout = any(k in str(exception).lower() for k in ("timeout", "timed out", "read timed"))
        audit_event(action="HTTP_TIMEOUT" if is_timeout else "HTTP_ERROR", run_id=run_id,
                    endpoint_type=endpoint, http_method=str(method).upper(), http_status=status,
                    retry_count=retry_count, elapsed_ms=elapsed_ms,
                    error_type=type(exception).__name__)
    elif status is not None and status >= 500:
        audit_event(action="HTTP_ERROR", run_id=run_id, endpoint_type=endpoint,
                    http_method=str(method).upper(), http_status=status,
                    retry_count=retry_count, elapsed_ms=elapsed_ms)


@contextmanager
def _instrument_v21(machine: Any, store: Any, candidate_by_key: Mapping[str, str],
                    stop_event: Optional[threading.Event] = None, run_id: Optional[str] = None,
                    allow_simulation_delete: bool = False):
    lock = threading.Lock(); methods: List[Dict[str, Any]] = []
    attempts: Dict[Any, int] = {}
    prev_outcome: Dict[Any, str] = {}
    original_request = machine.requests.sessions.Session.request
    original_cache_put = machine.cache_put

    def request(session, method, url, *args, **kwargs):
        upper = str(method).upper()
        if upper == "DELETE" and allow_simulation_delete:
            from .remote_simulation import validate_simulation_url
            validate_simulation_url(str(url))
        elif upper in {"PATCH", "PUT", "DELETE"}:
            raise RuntimeError(f"PHASE10A_FORBIDDEN_HTTP_METHOD:{upper}")
        is_simulation_post = upper == "POST" and str(url).rstrip("/").endswith("/simulations")
        is_authentication_post = upper == "POST" and _is_brain_authentication_url(machine, str(url))
        if upper == "POST" and not (is_simulation_post or is_authentication_post):
            raise RuntimeError(f"PHASE10A_UNEXPECTED_POST:{url}")
        key = (upper, str(url))
        with lock:
            attempts[key] = attempts.get(key, 0) + 1
            retry_count = attempts[key] - 1
        # A repeat attempt after a retryable outcome is an HTTP retry.
        if prev_outcome.get(key) in {"429", "5XX", "EXCEPTION"}:
            audit_event(action="HTTP_RETRY", run_id=run_id, endpoint_type=_endpoint_type(upper, str(url)),
                        http_method=upper, http_status=None, retry_count=retry_count)
        started = time.time()
        try:
            response = original_request(session, method, url, *args, **kwargs)
            status = getattr(response, "status_code", None)
            elapsed = (time.time() - started) * 1000.0
            item = {"method": upper, "url": str(url), "status": status, "elapsed": time.time() - started}
            if status == 429:
                prev_outcome[key] = "429"
                retry_after = None
                try:
                    retry_after = response.headers.get("Retry-After")
                except Exception:
                    retry_after = None
                _http_audit(run_id, upper, str(url), status, elapsed, retry_count=retry_count, retry_after=retry_after)
            elif status is not None and status >= 500:
                prev_outcome[key] = "5XX"
                _http_audit(run_id, upper, str(url), status, elapsed, retry_count=retry_count)
            else:
                prev_outcome[key] = "OK"
            return response
        except BaseException as exc:
            elapsed = (time.time() - started) * 1000.0
            item = {"method": upper, "url": str(url), "status": None, "exception": repr(exc), "elapsed": time.time() - started}
            prev_outcome[key] = "EXCEPTION"
            _http_audit(run_id, upper, str(url), None, elapsed, exception=exc, retry_count=retry_count)
            raise
        finally:
            with lock:
                methods.append(item)

    def cache_put(db_path, sim_key, candidate, settings, result):
        try:
            value = original_cache_put(db_path, sim_key, candidate, settings, result)
        except sqlite3.Error as exc:
            _emit_sqlite_write_diagnostic(
                db_path=Path(db_path), stage="ALPHA_CACHE_WRITE", exc=exc, conn=None,
            )
            raise
        cid = candidate_by_key.get(str(sim_key))
        if cid:
            fact = machine.cache_get(db_path, sim_key) or dict(result)
            fact["sim_key"] = sim_key
            _sync_candidate_fact(store, cid, fact, source="V21_CACHE_EVENT")
            if stop_event is not None and str(fact.get("status") or "").upper() in {"UNCERTAIN_SUBMISSION", "AUTH_ERROR"}:
                stop_event.set()
        return value

    machine.requests.sessions.Session.request = request
    machine.cache_put = cache_put
    try:
        yield methods
    finally:
        machine.requests.sessions.Session.request = original_request
        machine.cache_put = original_cache_put


def execute_phase10a(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                     machine_path: Path, run_id: str, *, allow_simulation_post: bool) -> Dict[str, Any]:
    caps = validate_phase10a_config(config)
    if not allow_simulation_post:
        report = preview_phase10a(store, config, alpha_db, run_id)
        report.update({"executed": False, "reason": "SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG"})
        return report
    validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_PHASE10A,
        config=config, run_id=run_id,
    )
    preview = preview_phase10a(store, config, alpha_db, run_id)
    if preview["estimated_new_posts"] != CANARY_CAP:
        raise ConfigError("PHASE10A_FINAL_TOCTOU_REQUIRES_TWO_CACHE_MISSES")
    rows = sorted(store.load_candidates(run_id), key=lambda x: x.get("selection_rank") or 0)
    # Lowest-level hard clamp immediately before the unchanged V2.1 adapter.
    if len(rows) > CANARY_CAP or caps["total"] > TOTAL_CAP or caps["initial"] > INITIAL_CAP:
        raise ConfigError("PHASE10A_LOW_LEVEL_HARD_CLAMP")
    source_before = store.get_run(str(config.plan["source_run_id"]))
    store.transition_run(run_id, "EXECUTING", reason="Explicit Phase 10A authorization", source="PHASE10A", allowed=RUN_TRANSITIONS)
    for row in rows:
        store.transition_candidate(row["candidate_id"], "SIMULATION_PENDING", reason="Scheduled by Phase 10A", source="PHASE10A", allowed=CANDIDATE_TRANSITIONS)
    runtime_stats: Dict[str, Any] = {}
    candidates = [{"execution_action": "NEW_SIMULATION_REQUIRED", "v21_candidate": _v21_candidate(row, config.target_mode)} for row in rows]
    by_key = {str(row["sim_key"]): str(row["candidate_id"]) for row in rows}
    started = time.time()
    with _instrument_v21(machine, store, by_key, run_id=run_id) as methods:
        # Runtime stats are passed through the established engine without changing it.
        original = machine.simulate_candidates
        def delegated(*args, **kwargs):
            kwargs["_runtime_stats"] = runtime_stats
            return original(*args, **kwargs)
        machine.simulate_candidates = delegated
        try:
            frame = execute_with_v21(candidates, config, machine, session=session, cache_db=str(alpha_db),
                                     allow_simulation_post=True, remaining_initial_budget=CANARY_CAP)
        finally:
            machine.simulate_candidates = original
    facts = _alpha_facts(alpha_db, by_key)
    for key, cid in by_key.items():
        fact = facts.get(key) or {"sim_key": key, "status": "UNKNOWN"}
        _sync_candidate_fact(store, cid, fact, source="PHASE10A_RESULT_RECONCILE")
    post_events = [x for x in methods if x["method"] == "POST" and x["url"].rstrip("/").endswith("/simulations")]
    attempted = len(post_events)
    uncertain = sum(str((facts.get(key) or {}).get("status") or "").upper() == "UNCERTAIN_SUBMISSION" for key in by_key)
    confirmed = sum(bool((facts.get(key) or {}).get("simulation_url")) and str((facts.get(key) or {}).get("status") or "").upper() != "UNCERTAIN_SUBMISSION" for key in by_key)
    consumed = confirmed + uncertain
    if attempted > CANARY_CAP or consumed > CANARY_CAP:
        raise RuntimeError("PHASE10A_POST_BUDGET_INVARIANT_BREACH")
    with store.connect() as conn:
        conn.execute("UPDATE ppl_runs SET post_attempted=?,post_confirmed=?,post_uncertain=?,post_consumed=?,updated_at=? WHERE run_id=?",
                     (attempted, confirmed, uncertain, consumed, _now(), run_id))
    local_analysis = run_local_analysis(store, config, alpha_db, run_id)
    pretag = run_one_pretag_check(store, config, machine, session, run_id, local_analysis["local_pass_candidates"])
    final_rows = store.load_candidates(run_id)
    run_target = "PAUSED" if any(x.get("simulation_status") in {"RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION", "UNKNOWN"} for x in final_rows) else "COMPLETED"
    store.transition_run(run_id, run_target, reason="Phase 10A execution ended", source="PHASE10A", allowed=RUN_TRANSITIONS)
    source_after = store.get_run(str(config.plan["source_run_id"]))
    if {k: source_before[k] for k in ("status", "current_stage", "updated_at")} != {k: source_after[k] for k in ("status", "current_stage", "updated_at")}:
        raise RuntimeError("SOURCE_RUN_WAS_MODIFIED")
    records = []
    for row in final_rows:
        fact = facts.get(row["sim_key"]) or {}
        records.append({"candidate_id": row["candidate_id"], "source_candidate_id": row.get("source_candidate_id"),
                        "sim_key": row["sim_key"], "status": fact.get("status"), "alpha_id": fact.get("alpha_id"),
                        "simulation_url": fact.get("simulation_url"), "sharpe": fact.get("sharpe"),
                        "fitness": fact.get("fitness"), "turnover": fact.get("turnover"), "returns": fact.get("returns"),
                        "margin": fact.get("margin"), "positions": (fact.get("long_count") or 0) + (fact.get("short_count") or 0) if fact.get("long_count") is not None and fact.get("short_count") is not None else None,
                        "retry_count": fact.get("retry_count"), "last_http_status": fact.get("last_http_status"),
                        "lifecycle_state": row.get("lifecycle_state")})
    report = {"validation_run_id": run_id, "phase": PHASE, "elapsed_seconds": time.time() - started,
              "post_attempted": attempted, "post_confirmed": confirmed, "post_uncertain": uncertain,
              "post_consumed": consumed, "repair_posts": 0, "runtime_stats": runtime_stats,
              "http_audit": methods, "http_methods": sorted({x["method"] for x in methods}),
              "results": records, "dataframe_rows": len(frame), "run_status": run_target,
              "local_analysis": local_analysis,
              "pre_tag_check": pretag}
    _audit(store, run_id, "EXECUTION_COMPLETE", report)
    return report


def execute_continuous_remote_handoff(
    store: Any,
    config: Any,
    machine: Any,
    session: Any,
    alpha_db: Path,
    run_id: str,
    wrappers: Sequence[Mapping[str, Any]],
    candidate_by_key: Mapping[str, str],
    *,
    allow_simulation_post: bool,
) -> Dict[str, Any]:
    """V3.1 handoff: POST once (when required), persist URL, then return.

    Unlike ``execute_with_v21`` this function deliberately does not call
    ``wait_simulation``.  Existing RUNNING/SUBMITTED work is registered with
    the durable Continuous Poll Queue; new POSTs return immediately after the
    durable SUBMITTED fact is written.  It therefore cannot consume a worker
    for the lifetime of a remote Simulation.

    The function is execution infrastructure, not strategy code.  sim_key
    validation, durable-first cache writes and the audited HTTP method firewall
    are retained from the V3.0.x path.
    """
    if not wrappers:
        return {
            "post_attempted": 0, "post_confirmed": 0, "post_uncertain": 0,
            "post_consumed": 0, "resume_count": 0, "submitted_candidate_ids": [],
            "nonterminal_candidate_ids": [], "http_audit": [], "results": [],
        }
    if session is None:
        raise ConfigError("CONTINUOUS_REMOTE_HANDOFF_REQUIRES_LIVE_SESSION")

    from .continuous_remote import register_submitted_remote, sync_remote_work_from_durable_facts
    from .simulation_adapter import _effective_settings, _validate_expected_sim_key

    defaults = config.plan["simulation_settings"]
    max_retries = int(config.plan.get("runtime", {}).get("submit_max_retries", 3) or 3)
    methods: List[Dict[str, Any]] = []
    submitted_ids: List[str] = []
    nonterminal_ids: List[str] = []
    resume_count = 0
    keys: List[str] = []

    with _instrument_v21(machine, store, candidate_by_key, run_id=run_id) as methods:
        for wrapped in wrappers:
            action = str(wrapped.get("execution_action") or "")
            v21 = dict(wrapped["v21_candidate"])
            effective = _effective_settings(v21, defaults, machine)
            _validate_expected_sim_key(machine, v21, effective)
            clean = {
                k: v for k, v in v21.items()
                if not str(k).startswith("_v22_") and k != "_expected_sim_key"
            }
            # Full durable settings are the single source of truth for direct
            # Continuous POST.  Do not rebuild or project a second payload here:
            # candidate identity, cache identity and the HTTP request must all
            # refer to the same canonical settings object.
            submission_settings = dict(effective)
            key = str(v21.get("_expected_sim_key") or "")
            if not key:
                key = str(machine.simulation_key(str(clean["expr"]), submission_settings))
            keys.append(key)
            cid = str(candidate_by_key.get(key) or "")
            current = machine.cache_get(str(alpha_db), key) or {}
            current_status = str(current.get("status") or "").upper()

            if action == "RESUME_EXISTING":
                url = str(current.get("simulation_url") or "")
                if not url:
                    # A RUNNING identity without a URL cannot be safely retried.
                    machine.cache_put(
                        str(alpha_db), key, clean, submission_settings,
                        {
                            "status": "UNCERTAIN_SUBMISSION",
                            "error": "RESUME_EXISTING_WITHOUT_SIMULATION_URL; no re-POST",
                            "retry_count": current.get("retry_count"),
                        },
                    )
                    fact = machine.cache_get(str(alpha_db), key) or {}
                    fact["sim_key"] = key
                    if cid:
                        _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF")
                        nonterminal_ids.append(cid)
                    continue
                resume_count += 1
                if cid:
                    fact = dict(current); fact["sim_key"] = key
                    _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF")
                    nonterminal_ids.append(cid)
                register_submitted_remote(
                    store, run_id, cid, key, url, current.get("submitted_at"),
                    initial_retry_after=float(current.get("last_retry_after") or 0.5),
                )
                continue

            if action not in {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"}:
                continue
            if not allow_simulation_post:
                raise ConfigError("SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
            if not bool(config.plan.get("execution", {}).get("allow_new_simulations")):
                raise ConfigError("SIMULATION_POST_DISABLED")
            # Final durable cache check immediately before POST.  A concurrent
            # durable URL wins and is converted to resume, never duplicated.
            current = machine.cache_get(str(alpha_db), key) or {}
            current_status = str(current.get("status") or "").upper()
            if current_status in {"SUBMITTED", "RUNNING"} and current.get("simulation_url"):
                resume_count += 1
                if cid:
                    fact = dict(current); fact["sim_key"] = key
                    _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF_TOCTOU_RESUME")
                    nonterminal_ids.append(cid)
                register_submitted_remote(
                    store, run_id, cid, key, str(current["simulation_url"]), current.get("submitted_at"),
                    initial_retry_after=float(current.get("last_retry_after") or 0.5),
                )
                continue
            if current_status == "UNCERTAIN_SUBMISSION":
                if cid:
                    fact = dict(current); fact["sim_key"] = key
                    _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF_UNCERTAIN")
                    nonterminal_ids.append(cid)
                continue
            if current_status == "COMPLETE":
                if cid:
                    fact = dict(current); fact["sim_key"] = key
                    _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF_CACHE_COMPLETE")
                continue

            submission_meta: Dict[str, Any] = {}
            # Last fail-closed identity gate immediately before HTTP POST.
            # A payload that does not reproduce the durable candidate sim_key
            # must never reach WorldQuant, even if it is otherwise syntactically valid.
            actual_post_key = str(machine.simulation_key(str(clean["expr"]), submission_settings))
            if key and actual_post_key != key:
                raise ConfigError(
                    f"POST_SETTINGS_IDENTITY_MISMATCH: expected={key} actual={actual_post_key}"
                )
            try:
                url = machine.submit_simulation(
                    session, clean, submission_settings,
                    cache_db=str(alpha_db), sim_key=key,
                    max_retries=max_retries,
                    progress_label=f"[CONTINUOUS {cid or key[:8]}]",
                    submission_meta=submission_meta,
                )
            except Exception:
                # submit_simulation already wrote INVALID/AUTH/UNCERTAIN/ERROR
                # cache facts as appropriate.  Preserve those facts and let the
                # queue/runtime scope the failure instead of inventing a retry.
                fact = machine.cache_get(str(alpha_db), key) or {"sim_key": key, "status": "UNKNOWN"}
                fact["sim_key"] = key
                if cid:
                    _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF_POST_ERROR")
                    if str(fact.get("status") or "").upper() in {"UNCERTAIN_SUBMISSION", "AUTH_ERROR"}:
                        nonterminal_ids.append(cid)
                continue

            fact = machine.cache_get(str(alpha_db), key) or {}
            fact["sim_key"] = key
            if cid:
                _sync_candidate_fact(store, cid, fact, source="V31_REMOTE_HANDOFF_SUBMITTED")
                submitted_ids.append(cid)
                nonterminal_ids.append(cid)
            register_submitted_remote(
                store, run_id, cid, key, url, fact.get("submitted_at"),
                initial_retry_after=float(submission_meta.get("retry_after") or 0.5),
            )

    # Project any UNCERTAIN/AUTH facts created by the submit path too.
    sync_remote_work_from_durable_facts(store, alpha_db, run_id, force_due_existing=False)
    facts = _alpha_facts(alpha_db, keys)
    post_events = [
        x for x in methods
        if x.get("method") == "POST" and str(x.get("url") or "").rstrip("/").endswith("/simulations")
    ]
    logical_post_keys = {
        str(w["v21_candidate"].get("_expected_sim_key") or "")
        for w in wrappers if str(w.get("execution_action")) in {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"}
    }
    logical_post_keys.discard("")
    uncertain = sum(
        str((facts.get(k) or {}).get("status") or "").upper() == "UNCERTAIN_SUBMISSION"
        for k in logical_post_keys
    )
    confirmed = sum(
        bool((facts.get(k) or {}).get("simulation_url"))
        and str((facts.get(k) or {}).get("status") or "").upper() in {"SUBMITTED", "RUNNING", "COMPLETE"}
        for k in logical_post_keys
    )
    results = []
    for key in keys:
        fact = dict(facts.get(key) or {})
        results.append({"sim_key": key, **fact})
    return {
        "post_attempted": len(post_events),
        "post_confirmed": confirmed,
        "post_uncertain": uncertain,
        "post_consumed": confirmed + uncertain,
        "resume_count": resume_count,
        "submitted_candidate_ids": submitted_ids,
        "nonterminal_candidate_ids": sorted(set(nonterminal_ids)),
        "http_audit": methods,
        "results": results,
    }
