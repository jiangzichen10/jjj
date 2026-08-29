"""Independent SQLite store for V2.2 workflow facts."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .audit_log import audit_event, audit_state_transition
from .config import COMPATIBLE_EXECUTION_HASH_STATUSES, validate_execution_hash_compatibility


SCHEMA_VERSION = 13


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _diagnostic_file_state(path: Path) -> Dict[str, Any]:
    try:
        stat = path.stat()
        return {
            "path": str(path), "exists": True, "size": int(stat.st_size),
            "mtime": float(stat.st_mtime),
            "file_attributes": getattr(stat, "st_file_attributes", None),
        }
    except FileNotFoundError:
        return {"path": str(path), "exists": False}
    except BaseException as exc:
        return {"path": str(path), "diagnostic_error": repr(exc)}


def _safe_pragma(conn: Optional[sqlite3.Connection], pragma: str) -> Any:
    if conn is None:
        return None
    try:
        rows = conn.execute(f"PRAGMA {pragma}").fetchall()
        return [list(row) for row in rows]
    except BaseException as exc:
        return {"diagnostic_error": repr(exc)}


def _emit_sqlite_write_diagnostic(
    *, db_path: Path, stage: str, exc: BaseException,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Best-effort diagnostics that must never replace the original DB error."""
    try:
        resolved = Path(db_path).resolve()
        payload: Dict[str, Any] = {
            "timestamp": _utc_now(),
            "stage": str(stage or "RUNNER_STORE_WRITE"),
            "thread_id": threading.get_ident(),
            "thread_name": threading.current_thread().name,
            "process_id": os.getpid(),
            "db_path": str(db_path),
            "resolved_db_path": str(resolved),
            "exception_type": type(exc).__name__,
            "exception_repr": repr(exc),
            "exception_string": str(exc),
            "sqlite_errorcode": getattr(exc, "sqlite_errorcode", None),
            "sqlite_errorname": getattr(exc, "sqlite_errorname", None),
            "connection_in_transaction": (
                bool(getattr(conn, "in_transaction", False)) if conn is not None else None
            ),
            "pragmas": {
                "database_list": _safe_pragma(conn, "database_list"),
                "query_only": _safe_pragma(conn, "query_only"),
                "journal_mode": _safe_pragma(conn, "journal_mode"),
                "locking_mode": _safe_pragma(conn, "locking_mode"),
                "synchronous": _safe_pragma(conn, "synchronous"),
            },
            "files": {
                "db": _diagnostic_file_state(resolved),
                "wal": _diagnostic_file_state(Path(str(resolved) + "-wal")),
                "shm": _diagnostic_file_state(Path(str(resolved) + "-shm")),
            },
        }
        audit_event(action="SQLITE_WRITE_ERROR_DIAGNOSTIC", **payload)
        print(
            "[SQLITE WRITE DIAGNOSTIC] "
            + json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
            file=sys.stderr,
        )
    except BaseException:
        # Diagnostics are strictly secondary. The caller re-raises ``exc``.
        pass


class RunnerStore:
    def __init__(self, path: Path):
        self.path = Path(path).resolve()

    @contextmanager
    def connect(self, *, stage: str = "RUNNER_STORE_WRITE"):
        conn = None
        try:
            conn = sqlite3.connect(str(self.path), timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        except BaseException as exc:
            if isinstance(exc, sqlite3.Error):
                _emit_sqlite_write_diagnostic(
                    db_path=self.path, stage=stage, exc=exc, conn=conn,
                )
            if conn is not None:
                try:
                    conn.rollback()
                except BaseException:
                    pass
            raise
        finally:
            if conn is not None:
                conn.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect(stage="RUNNER_STORE_WRITE") as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ppl_schema_meta (
                    schema_version INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ppl_runs (
                    run_id TEXT PRIMARY KEY,
                    runner_goal TEXT NOT NULL,
                    target_mode TEXT NOT NULL,
                    atom_constraint_active INTEGER NOT NULL,
                    run_profile TEXT NOT NULL,
                    current_stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_hash TEXT NOT NULL,
                    operational_hash TEXT NOT NULL,
                    presentation_hash TEXT NOT NULL,
                    rules_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    budget_json TEXT NOT NULL,
                    operational_revision INTEGER NOT NULL DEFAULT 1,
                    error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ppl_catalog (
                    catalog_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT NOT NULL,
                    field_id TEXT NOT NULL,
                    field_type TEXT,
                    semantic_class TEXT,
                    classification_source TEXT,
                    classification_rule_id TEXT,
                    classification_confidence TEXT,
                    raw_metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(dataset_id, field_id)
                );
                CREATE TABLE IF NOT EXISTS ppl_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                    expression TEXT NOT NULL,
                    sim_key TEXT,
                    settings_json TEXT,
                    settings_hash TEXT,
                    context_fingerprint TEXT,
                    dataset_id TEXT,
                    field_id TEXT,
                    field_type TEXT,
                    semantic_class TEXT,
                    direction TEXT,
                    signal_family TEXT,
                    transform_family TEXT,
                    operator TEXT,
                    window INTEGER,
                    decay INTEGER,
                    neutralization TEXT,
                    legacy_unique_operator_count INTEGER,
                    pp_total_operator_count_estimate INTEGER,
                    pp_operator_estimator_version INTEGER,
                    root_candidate_id TEXT,
                    parent_candidate_id TEXT,
                    parent_sim_key TEXT,
                    repair_path_json TEXT,
                    repair_depth INTEGER NOT NULL DEFAULT 0,
                    lifecycle_state TEXT NOT NULL,
                    simulation_status TEXT,
                    alpha_id TEXT,
                    research_priority_score REAL,
                    priority_components_json TEXT,
                    primary_failure TEXT,
                    severity TEXT,
                    stop_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, sim_key)
                );
                CREATE TABLE IF NOT EXISTS ppl_check_polls (
                    poll_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_session_id TEXT NOT NULL,
                    candidate_id TEXT REFERENCES ppl_candidates(candidate_id),
                    alpha_id TEXT,
                    phase TEXT NOT NULL CHECK (phase IN ('PRE_TAG','RECHECK','FINAL')),
                    semantic_poll_index INTEGER NOT NULL,
                    http_request_delta INTEGER NOT NULL DEFAULT 0,
                    raw_payload_json TEXT,
                    parsed_payload_json TEXT,
                    live_base_gate_result TEXT,
                    live_theme_gate_result TEXT,
                    individual_checks_json TEXT,
                    limits_json TEXT,
                    pending INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    error_nature TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ppl_repairs (
                    repair_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                    parent_candidate_id TEXT NOT NULL,
                    child_candidate_id TEXT,
                    repair_type TEXT NOT NULL,
                    repair_signature TEXT NOT NULL,
                    repair_path_json TEXT NOT NULL,
                    repair_depth INTEGER NOT NULL,
                    before_json TEXT,
                    after_json TEXT,
                    delta_json TEXT,
                    side_effect_verdict TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_id, repair_signature)
                );
                CREATE TABLE IF NOT EXISTS ppl_operator_capabilities (
                    operator_name TEXT NOT NULL,
                    signature_hash TEXT NOT NULL,
                    operator_metadata_hash TEXT,
                    capability_class TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source TEXT,
                    evidence_json TEXT,
                    validated_at TEXT,
                    last_seen_at TEXT,
                    validation_error TEXT,
                    PRIMARY KEY(operator_name, signature_hash)
                );
                CREATE TABLE IF NOT EXISTS ppl_descriptions (
                    description_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id TEXT NOT NULL REFERENCES ppl_candidates(candidate_id),
                    version INTEGER NOT NULL,
                    idea TEXT,
                    data_rationale TEXT,
                    operator_rationale TEXT,
                    full_text TEXT,
                    validation_status TEXT,
                    validation_warnings_json TEXT,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(candidate_id, version)
                );
                CREATE TABLE IF NOT EXISTS ppl_reference_pool (
                    reference_id TEXT PRIMARY KEY,
                    alpha_id TEXT,
                    candidate_id TEXT,
                    signal_family TEXT NOT NULL,
                    lifecycle_state TEXT,
                    source TEXT NOT NULL,
                    evidence_json TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ppl_manual_evidence (
                    evidence_id TEXT PRIMARY KEY,
                    alpha_id TEXT,
                    evidence_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL CHECK(source='MANUAL'),
                    conflict_state TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ppl_candidates_run_state
                    ON ppl_candidates(run_id, lifecycle_state);
                CREATE INDEX IF NOT EXISTS idx_ppl_check_polls_candidate_phase
                    ON ppl_check_polls(candidate_id, phase, semantic_poll_index);
                CREATE TABLE IF NOT EXISTS ppl_discovery_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    region TEXT NOT NULL,
                    universe TEXT NOT NULL,
                    delay INTEGER NOT NULL,
                    instrument_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    dataset_count INTEGER NOT NULL,
                    field_count INTEGER NOT NULL,
                    metadata_hash TEXT NOT NULL,
                    exclusion_status_json TEXT NOT NULL,
                    automatic_preselection INTEGER NOT NULL DEFAULT 1,
                    discovery_pool_size INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ppl_dataset_catalog (
                    snapshot_id TEXT NOT NULL REFERENCES ppl_discovery_snapshots(snapshot_id),
                    dataset_id TEXT NOT NULL,
                    selected INTEGER NOT NULL,
                    in_discovery_pool INTEGER NOT NULL DEFAULT 0,
                    excluded INTEGER NOT NULL,
                    dataset_semantic_hint TEXT,
                    dataset_hint_source TEXT,
                    dataset_hint_confidence TEXT,
                    dataset_hint_rule_id TEXT,
                    dataset_hint_matched_text TEXT,
                    dataset_preselection_score REAL,
                    preselection_components_json TEXT,
                    field_evidence_json TEXT,
                    raw_metadata_json TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id, dataset_id)
                );
                CREATE TABLE IF NOT EXISTS ppl_field_catalog (
                    snapshot_id TEXT NOT NULL REFERENCES ppl_discovery_snapshots(snapshot_id),
                    dataset_id TEXT NOT NULL,
                    field_id TEXT NOT NULL,
                    description TEXT,
                    field_type TEXT,
                    coverage REAL,
                    date_coverage REAL,
                    user_count INTEGER,
                    alpha_count INTEGER,
                    pyramid_multiplier REAL,
                    category_json TEXT,
                    subcategory_json TEXT,
                    themes_json TEXT,
                    semantic_class TEXT NOT NULL,
                    classification_source TEXT NOT NULL,
                    classification_rule_id TEXT,
                    classification_confidence TEXT NOT NULL,
                    classification_warning TEXT,
                    matched_text TEXT,
                    coverage_pass INTEGER NOT NULL,
                    selected INTEGER NOT NULL,
                    raw_metadata_json TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id, dataset_id, field_id)
                );
                CREATE TABLE IF NOT EXISTS ppl_dry_run_snapshots (
                    dry_run_id TEXT PRIMARY KEY,
                    discovery_snapshot_id TEXT NOT NULL REFERENCES ppl_discovery_snapshots(snapshot_id),
                    execution_hash TEXT NOT NULL,
                    source TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ppl_discovery_settings
                    ON ppl_discovery_snapshots(region, universe, delay, instrument_type, created_at);
                CREATE INDEX IF NOT EXISTS idx_ppl_field_snapshot_selected
                    ON ppl_field_catalog(snapshot_id, selected, dataset_id);
                """
            )
            self._ensure_column(conn, "ppl_catalog", "classification_warning", "TEXT")
            self._ensure_column(conn, "ppl_catalog", "matched_text", "TEXT")
            self._ensure_column(conn, "ppl_operator_capabilities", "complete_expression_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "ppl_operator_capabilities", "example_sim_keys_json", "TEXT")
            self._ensure_column(conn, "ppl_operator_capabilities", "signature", "TEXT")
            self._ensure_column(conn, "ppl_operator_capabilities", "evidence_note", "TEXT")
            self._ensure_column(conn, "ppl_discovery_snapshots", "automatic_preselection", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(conn, "ppl_discovery_snapshots", "discovery_pool_size", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "ppl_dataset_catalog", "in_discovery_pool", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_semantic_hint", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_hint_source", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_hint_confidence", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_hint_rule_id", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_hint_matched_text", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "dataset_preselection_score", "REAL")
            self._ensure_column(conn, "ppl_dataset_catalog", "preselection_components_json", "TEXT")
            self._ensure_column(conn, "ppl_dataset_catalog", "field_evidence_json", "TEXT")
            for column, sql_type in {
                "expression_raw": "TEXT",
                "expression_canonical": "TEXT",
                "expression_hash": "TEXT",
                "dry_run_snapshot_id": "TEXT",
                "discovery_snapshot_id": "TEXT",
                "vector_reducer": "TEXT",
                "data_field_count_estimate": "INTEGER",
                "data_fields_used_json": "TEXT",
                "cache_classification": "TEXT",
                "execution_action": "TEXT",
                "initial_selection_score": "REAL",
                "selection_reason": "TEXT",
                "selection_rank": "INTEGER",
                "selected_for_initial_search": "INTEGER NOT NULL DEFAULT 0",
                "structure_status": "TEXT",
                "available_result_json": "TEXT",
                "simulation_freshness": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
                "live_reconcile_required": "INTEGER NOT NULL DEFAULT 0",
                "provenance_warning": "TEXT",
                "new_post_budget_consumed": "INTEGER NOT NULL DEFAULT 0",
            }.items():
                self._ensure_column(conn, "ppl_candidates", column, sql_type)
            for column, sql_type in {
                "source_run_id": "TEXT",
                "validation_phase": "TEXT",
                "post_attempted": "INTEGER NOT NULL DEFAULT 0",
                "post_confirmed": "INTEGER NOT NULL DEFAULT 0",
                "post_uncertain": "INTEGER NOT NULL DEFAULT 0",
                "post_consumed": "INTEGER NOT NULL DEFAULT 0",
            }.items():
                self._ensure_column(conn, "ppl_runs", column, sql_type)
            for column, sql_type in {
                "source_candidate_id": "TEXT",
                "result_reference_json": "TEXT",
            }.items():
                self._ensure_column(conn, "ppl_candidates", column, sql_type)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_live_execution_audits (
                       audit_id TEXT PRIMARY KEY,
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       validation_phase TEXT NOT NULL,
                       event_type TEXT NOT NULL,
                       candidate_id TEXT,
                       sim_key TEXT,
                       payload_json TEXT NOT NULL,
                       created_at TEXT NOT NULL
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ppl_live_audit_run ON ppl_live_execution_audits(run_id,created_at)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_candidate_provenance (
                       provenance_id TEXT PRIMARY KEY,
                       candidate_id TEXT NOT NULL REFERENCES ppl_candidates(candidate_id),
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       sim_key TEXT NOT NULL,
                       context_fingerprint TEXT NOT NULL,
                       discovery_snapshot_id TEXT NOT NULL,
                       dry_run_snapshot_id TEXT NOT NULL,
                       provenance_json TEXT NOT NULL,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       UNIQUE(run_id, sim_key, context_fingerprint)
                   )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_state_transitions (
                       transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       candidate_id TEXT REFERENCES ppl_candidates(candidate_id),
                       entity_type TEXT NOT NULL CHECK(entity_type IN ('CANDIDATE','RUN')),
                       from_state TEXT NOT NULL,
                       to_state TEXT NOT NULL,
                       reason TEXT NOT NULL,
                       source TEXT NOT NULL,
                       metadata_json TEXT,
                       created_at TEXT NOT NULL
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ppl_transitions_run ON ppl_state_transitions(run_id, entity_type, candidate_id)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_check_sessions (
                       check_session_id TEXT PRIMARY KEY,
                       run_id TEXT,
                       candidate_id TEXT,
                       alpha_id TEXT NOT NULL,
                       phase TEXT NOT NULL CHECK(phase IN ('PRE_TAG','RECHECK','FINAL')),
                       session_status TEXT NOT NULL,
                       started_at TEXT NOT NULL,
                       resolved_at TEXT,
                       poll_count INTEGER NOT NULL DEFAULT 0,
                       http_request_count INTEGER NOT NULL DEFAULT 0,
                       pending_poll_requests INTEGER NOT NULL DEFAULT 0,
                       base_gate_result TEXT,
                       theme_gate_result TEXT,
                       error_type TEXT,
                       error_nature TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                   )"""
            )
            for column, sql_type in {
                "http_status": "INTEGER", "raw_response_text": "TEXT", "parse_status": "TEXT",
                "pending_check_names_json": "TEXT", "error_type": "TEXT", "parser_version": "INTEGER",
                "alias_version": "INTEGER", "evidence_source": "TEXT",
            }.items():
                self._ensure_column(conn, "ppl_check_polls", column, sql_type)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_check_results (
                       check_result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       check_session_id TEXT NOT NULL REFERENCES ppl_check_sessions(check_session_id),
                       poll_id INTEGER NOT NULL REFERENCES ppl_check_polls(poll_id),
                       candidate_id TEXT,
                       alpha_id TEXT NOT NULL,
                       phase TEXT NOT NULL,
                       raw_name TEXT,
                       normalized_name TEXT NOT NULL,
                       category TEXT NOT NULL,
                       raw_result TEXT,
                       normalized_result TEXT NOT NULL,
                       raw_value_json TEXT,
                       raw_limit_json TEXT,
                       normalized_value REAL,
                       normalized_limit REAL,
                       unit TEXT,
                       unit_confidence TEXT NOT NULL,
                       preset_limit_json TEXT,
                       live_limit_json TEXT,
                       effective_limit_json TEXT,
                       limit_source TEXT,
                       status TEXT,
                       message TEXT,
                       parser_version INTEGER NOT NULL,
                       alias_version INTEGER NOT NULL,
                       evidence_source TEXT NOT NULL,
                       mapping_suggestion TEXT,
                       created_at TEXT NOT NULL
                   )"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ppl_check_results_name ON ppl_check_results(check_session_id,normalized_name,phase)")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_remote_work (
                       remote_work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       candidate_id TEXT REFERENCES ppl_candidates(candidate_id),
                       sim_key TEXT NOT NULL,
                       simulation_url TEXT,
                       remote_status TEXT,
                       queue_state TEXT NOT NULL,
                       next_poll_at TEXT,
                       poll_attempts INTEGER NOT NULL DEFAULT 0,
                       missing_confirmations INTEGER NOT NULL DEFAULT 0,
                       reserved_slot INTEGER NOT NULL DEFAULT 0,
                       last_http_status INTEGER,
                       last_error TEXT,
                       retry_after_seconds REAL,
                       submitted_at TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       UNIQUE(run_id, sim_key)
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ppl_remote_work_due ON ppl_remote_work(run_id,queue_state,next_poll_at)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_check_work (
                       check_work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       candidate_id TEXT NOT NULL REFERENCES ppl_candidates(candidate_id),
                       alpha_id TEXT NOT NULL,
                       phase TEXT NOT NULL,
                       queue_state TEXT NOT NULL,
                       next_check_at TEXT,
                       attempt_count INTEGER NOT NULL DEFAULT 0,
                       last_http_status INTEGER,
                       last_error TEXT,
                       retry_after_seconds REAL,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       UNIQUE(run_id, candidate_id, alpha_id, phase)
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ppl_check_work_due ON ppl_check_work(run_id,queue_state,next_check_at)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_discovery_work (
                       discovery_work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       round_id TEXT NOT NULL,
                       refresh_no INTEGER NOT NULL,
                       batch_no INTEGER NOT NULL DEFAULT 0,
                       trigger TEXT NOT NULL,
                       queue_state TEXT NOT NULL,
                       stage TEXT NOT NULL DEFAULT 'DATASETS',
                       next_attempt_at TEXT,
                       attempt_count INTEGER NOT NULL DEFAULT 0,
                       probe_count INTEGER NOT NULL DEFAULT 0,
                       admit_count INTEGER NOT NULL DEFAULT 0,
                       excluded_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                       datasets_json TEXT NOT NULL DEFAULT '[]',
                       probe_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                       fields_by_dataset_json TEXT NOT NULL DEFAULT '{}',
                       current_dataset_index INTEGER NOT NULL DEFAULT 0,
                       current_offset INTEGER NOT NULL DEFAULT 0,
                       network_get_count INTEGER NOT NULL DEFAULT 0,
                       last_http_status INTEGER,
                       last_error TEXT,
                       retry_after_seconds REAL,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       UNIQUE(run_id, round_id, refresh_no)
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ppl_discovery_work_due ON ppl_discovery_work(run_id,queue_state,next_attempt_at)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_endpoint_waits (
                       run_id TEXT NOT NULL REFERENCES ppl_runs(run_id),
                       endpoint_type TEXT NOT NULL,
                       wait_state TEXT NOT NULL,
                       next_retry_at TEXT,
                       retry_after_seconds REAL,
                       consecutive_failures INTEGER NOT NULL DEFAULT 0,
                       last_http_status INTEGER,
                       last_error TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       PRIMARY KEY(run_id, endpoint_type)
                   )"""
            )
            for column, sql_type in {
                "eligibility_outcome": "TEXT",
                "eligibility_reason": "TEXT",
                "threshold_exceeded": "INTEGER NOT NULL DEFAULT 0",
                "diagnosis_outcome": "TEXT",
                "diagnosis_reason": "TEXT",
            }.items():
                self._ensure_column(conn, "ppl_check_results", column, sql_type)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_diagnoses (
                       diagnosis_id TEXT PRIMARY KEY,
                       run_id TEXT,
                       candidate_id TEXT,
                       alpha_id TEXT,
                       source_phase TEXT NOT NULL,
                       evidence_source TEXT NOT NULL,
                       primary_failure TEXT NOT NULL,
                       secondary_failures_json TEXT NOT NULL,
                       severity TEXT NOT NULL,
                       repairability TEXT NOT NULL,
                       root_cause TEXT,
                       metrics_snapshot_json TEXT NOT NULL,
                       check_session_id TEXT,
                       check_result_ids_json TEXT NOT NULL,
                       diagnosis_rule_version INTEGER NOT NULL,
                       created_at TEXT NOT NULL
                   )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ppl_repair_plans (
                       repair_plan_id TEXT PRIMARY KEY,
                       diagnosis_id TEXT,
                       run_id TEXT,
                       parent_candidate_id TEXT,
                       root_candidate_id TEXT,
                       target_failure TEXT NOT NULL,
                       repair_type TEXT NOT NULL,
                       repair_signature TEXT NOT NULL,
                       repair_path_json TEXT NOT NULL,
                       repair_depth INTEGER NOT NULL,
                       candidate_spec_json TEXT NOT NULL,
                       operator_requirements_json TEXT NOT NULL,
                       plan_status TEXT NOT NULL,
                       projected_new_posts INTEGER NOT NULL DEFAULT 0,
                       committed_posts INTEGER NOT NULL DEFAULT 0,
                       consumed_posts INTEGER NOT NULL DEFAULT 0,
                       blocked_reason TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL,
                       UNIQUE(run_id, repair_signature)
                   )"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ppl_diagnoses_run ON ppl_diagnoses(run_id,candidate_id,created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ppl_repair_plans_run ON ppl_repair_plans(run_id,plan_status)")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_families(
                run_id TEXT NOT NULL, family_id TEXT NOT NULL, family_fingerprint TEXT NOT NULL,
                family_json TEXT NOT NULL, calculated_at TEXT NOT NULL,
                family_similarity_version INTEGER NOT NULL, PRIMARY KEY(run_id,family_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_family_members(
                run_id TEXT NOT NULL, family_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
                family_role TEXT NOT NULL, evidence_level TEXT NOT NULL, calculated_at TEXT NOT NULL,
                PRIMARY KEY(run_id,candidate_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_priority_scores(
                run_id TEXT NOT NULL, candidate_id TEXT NOT NULL, priority_score REAL NOT NULL,
                priority_confidence TEXT NOT NULL, components_json TEXT NOT NULL, calculated_at TEXT NOT NULL,
                priority_score_version INTEGER NOT NULL, PRIMARY KEY(run_id,candidate_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_family_similarity(
                run_id TEXT NOT NULL, candidate_id TEXT NOT NULL, reference_id TEXT NOT NULL,
                similarity_risk TEXT NOT NULL, components_json TEXT NOT NULL, calculated_at TEXT NOT NULL,
                family_similarity_version INTEGER NOT NULL, PRIMARY KEY(run_id,candidate_id,reference_id))""")
            for column, sql_type in {
                "updated_at":"TEXT", "field_metadata_source":"TEXT", "expression_snapshot":"TEXT",
                "operator_snapshot_json":"TEXT", "description_template_version":"INTEGER NOT NULL DEFAULT 1",
            }.items(): self._ensure_column(conn,"ppl_descriptions",column,sql_type)
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_property_snapshots(
                snapshot_id TEXT PRIMARY KEY,candidate_id TEXT,alpha_id TEXT,phase TEXT NOT NULL,
                description_present INTEGER NOT NULL,description_text TEXT,description_length INTEGER NOT NULL,
                tags_raw_json TEXT,normalized_tags_json TEXT,power_pool_selected_status TEXT NOT NULL,
                submission_status TEXT NOT NULL,raw_payload_json TEXT NOT NULL,property_parser_version INTEGER NOT NULL,
                tag_alias_version INTEGER NOT NULL,evidence_source TEXT NOT NULL,previous_snapshot_id TEXT,captured_at TEXT NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_manual_actions(
                manual_action_id TEXT PRIMARY KEY,run_id TEXT,candidate_id TEXT,alpha_id TEXT,
                action_type TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,confirmed_at TEXT,
                evidence_source TEXT,metadata_json TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_live_validation_sessions(
                validation_session_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,completed_at TEXT NOT NULL,
                status TEXT NOT NULL,summary_json TEXT NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS ppl_live_validation_evidence(
                evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,validation_session_id TEXT NOT NULL,
                target_type TEXT NOT NULL,target_id TEXT,endpoint TEXT NOT NULL,http_method TEXT NOT NULL,
                http_status INTEGER,raw_payload_sanitized_json TEXT NOT NULL,schema_fingerprint TEXT NOT NULL,
                parser_version INTEGER,observations_json TEXT NOT NULL,captured_at TEXT NOT NULL)""")
            now = _utc_now()
            conn.execute(
                """INSERT INTO ppl_schema_meta(schema_version, created_at, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(schema_version) DO UPDATE SET updated_at=excluded.updated_at""",
                (SCHEMA_VERSION, now, now),
            )

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, sql_type: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")

    def create_run(self, run_id: str, config: Any) -> None:
        now = _utc_now()
        snapshot = config.run_snapshot()
        with self.connect(stage="RUNNER_STORE_WRITE") as conn:
            conn.execute(
                """INSERT INTO ppl_runs(
                       run_id, runner_goal, target_mode, atom_constraint_active,
                       run_profile, current_stage, status, execution_hash,
                       operational_hash, presentation_hash, rules_json, plan_json,
                       budget_json, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, 'INIT', 'CREATED', ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    snapshot["runner_goal"],
                    snapshot["target_mode"],
                    int(snapshot["atom_constraint_active"]),
                    snapshot["run_profile"],
                    snapshot["execution_hash"],
                    snapshot["operational_hash"],
                    snapshot["presentation_hash"],
                    json.dumps(config.rules, ensure_ascii=False, sort_keys=True),
                    json.dumps(config.plan, ensure_ascii=False, sort_keys=True),
                    json.dumps(config.plan.get("budgets", {}), ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )

    def status(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        if not self.path.exists():
            return {"database": str(self.path), "exists": False, "runs": []}
        with self.connect() as conn:
            if run_id:
                rows = conn.execute("SELECT * FROM ppl_runs WHERE run_id=?", (run_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM ppl_runs ORDER BY created_at DESC").fetchall()
            runs = []
            for row in rows:
                item = dict(row)
                for field in ("rules_json", "plan_json", "budget_json", "error_json"):
                    item.pop(field, None)
                item["atom_constraint_active"] = bool(item["atom_constraint_active"])
                counts = conn.execute(
                    "SELECT lifecycle_state, COUNT(*) n FROM ppl_candidates WHERE run_id=? GROUP BY lifecycle_state",
                    (item["run_id"],),
                ).fetchall()
                item["candidate_states"] = {r["lifecycle_state"]: r["n"] for r in counts}
                runs.append(item)
            return {"database": str(self.path), "exists": True, "runs": runs}

    def latest_run(self) -> Optional[Dict[str, Any]]:
        status = self.status()
        return status["runs"][0] if status["runs"] else None

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    def load_candidates(self, run_id: str) -> list:
        with self.connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM ppl_candidates WHERE run_id=? ORDER BY candidate_id", (run_id,)
            )]

    def transition_candidate(
        self, candidate_id: str, to_state: str, *, reason: str, source: str,
        allowed: Any, metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Atomically update lifecycle and append its audit record."""
        with self.connect(stage="ROUND_STATE_TRANSITION") as conn:
            row = conn.execute("SELECT run_id,lifecycle_state FROM ppl_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
            if row is None:
                raise ValueError(f"Unknown candidate: {candidate_id}")
            old = str(row["lifecycle_state"])
            if old == to_state:
                return False
            if to_state not in allowed.get(old, set()):
                raise ValueError(f"STATE_TRANSITION_REJECTED: {old} -> {to_state}")
            conn.execute("UPDATE ppl_candidates SET lifecycle_state=?,updated_at=? WHERE candidate_id=?", (to_state, _utc_now(), candidate_id))
            conn.execute(
                """INSERT INTO ppl_state_transitions(
                       run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                   ) VALUES (?,?,'CANDIDATE',?,?,?,?,?,?)""",
                (row["run_id"], candidate_id, old, to_state, reason, source,
                 json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True), _utc_now()),
            )
            audit_state_transition("CANDIDATE", candidate_id, run_id=row["run_id"],
                                   old_state=old, new_state=to_state, reason=reason, source=source)
            return True

    def transition_run(
        self, run_id: str, to_state: str, *, reason: str, source: str,
        allowed: Any, metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        with self.connect(stage="ROUND_STATE_TRANSITION") as conn:
            row = conn.execute("SELECT status FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise ValueError(f"Unknown run: {run_id}")
            old = str(row["status"])
            if old == to_state:
                return False
            if to_state not in allowed.get(old, set()):
                raise ValueError(f"STATE_TRANSITION_REJECTED: {old} -> {to_state}")
            conn.execute("UPDATE ppl_runs SET status=?,current_stage=?,updated_at=? WHERE run_id=?", (to_state, to_state, _utc_now(), run_id))
            conn.execute(
                """INSERT INTO ppl_state_transitions(
                       run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                   ) VALUES (?,NULL,'RUN',?,?,?,?,?,?)""",
                (run_id, old, to_state, reason, source,
                 json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True), _utc_now()),
            )
            audit_state_transition("RUN", run_id, run_id=run_id,
                                   old_state=old, new_state=to_state, reason=reason, source=source)
            return True

    def transition_counts(self, run_id: str) -> Dict[str, int]:
        with self.connect() as conn:
            return dict(conn.execute(
                "SELECT entity_type,COUNT(*) FROM ppl_state_transitions WHERE run_id=? GROUP BY entity_type", (run_id,)
            ).fetchall())

    def load_repair_plans(self, run_id: Optional[str] = None) -> list:
        """Return repair plan rows (optionally scoped to a run) as plain dicts."""
        if not self.path.exists():
            return []
        with self.connect() as conn:
            if run_id:
                return [dict(row) for row in conn.execute(
                    "SELECT * FROM ppl_repair_plans WHERE run_id=? ORDER BY created_at, repair_plan_id", (run_id,)
                )]
            return [dict(row) for row in conn.execute(
                "SELECT * FROM ppl_repair_plans ORDER BY created_at, repair_plan_id"
            )]

    def repair_budget_state(self, run_id: str) -> Dict[str, int]:
        """Repair Reserve consumption derived from committed repair POSTs only."""
        with self.connect() as conn:
            committed = int(conn.execute(
                "SELECT coalesce(sum(committed_posts),0) FROM ppl_repair_plans WHERE run_id=?", (run_id,)
            ).fetchone()[0])
            consumed = int(conn.execute(
                "SELECT coalesce(sum(consumed_posts),0) FROM ppl_repair_plans WHERE run_id=?", (run_id,)
            ).fetchone()[0])
            return {"repair_committed": committed, "repair_consumed": consumed}

    def transition_repair_plan(
        self, repair_plan_id: str, to_status: str, *, reason: str, source: str,
        allowed: Any, expected_from: Optional[frozenset] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Atomically advance a repair plan status and append an audit record.

        Idempotent: returns False (and writes nothing) when already in the target
        status. Rejects transitions outside the supplied `allowed` map. This is the
        only supported path for DEFERRED_INITIAL_SEARCH -> READY promotion; raw SQL
        updates are intentionally not part of the workflow.
        """
        with self.connect() as conn:
            row = conn.execute(
                "SELECT run_id,parent_candidate_id,repair_signature,plan_status FROM ppl_repair_plans WHERE repair_plan_id=?",
                (repair_plan_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Unknown repair plan: {repair_plan_id}")
            old = str(row["plan_status"])
            if old == to_status:
                return False
            if expected_from is not None and old not in expected_from:
                raise ValueError(f"REPAIR_PLAN_STATE_TRANSITION_REJECTED: {old} -> {to_status}")
            if to_status not in allowed.get(old, set()):
                raise ValueError(f"REPAIR_PLAN_STATE_TRANSITION_REJECTED: {old} -> {to_status}")
            conn.execute(
                "UPDATE ppl_repair_plans SET plan_status=?, updated_at=? WHERE repair_plan_id=?",
                (to_status, _utc_now(), repair_plan_id),
            )
            audit_id = "rpt_" + hashlib.sha256(
                f"{repair_plan_id}|{old}|{to_status}|{_utc_now()}".encode()
            ).hexdigest()[:24]
            conn.execute(
                """INSERT INTO ppl_live_execution_audits(
                       audit_id,run_id,validation_phase,event_type,candidate_id,sim_key,payload_json,created_at
                   ) VALUES (?,?,?,?,?,?,?,?)""",
                (audit_id, row["run_id"], source, "REPAIR_PLAN_STATUS",
                 row["parent_candidate_id"], None,
                 json.dumps({"repair_plan_id": repair_plan_id, "from": old, "to": to_status,
                             "reason": reason, "source": source, **(metadata or {})},
                            ensure_ascii=False, sort_keys=True),
                 _utc_now()),
            )
            audit_state_transition("REPAIR_PLAN", repair_plan_id, run_id=row["run_id"],
                                   old_state=old, new_state=to_status, reason=reason, source=source)
            return True

    def save_check_session(self, session: Dict[str, Any], *, evidence_source: str) -> None:
        """Persist immutable poll evidence and versioned structured results."""
        now = _utc_now()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_check_sessions(
                       check_session_id,run_id,candidate_id,alpha_id,phase,session_status,started_at,resolved_at,
                       poll_count,http_request_count,pending_poll_requests,base_gate_result,theme_gate_result,
                       error_type,error_nature,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (session["check_session_id"],session.get("run_id"),session.get("candidate_id"),session["alpha_id"],
                 session["phase"],session["session_status"],session["started_at"],session.get("resolved_at"),
                 session["poll_count"],session["http_request_count"],session["pending_poll_requests"],
                 session.get("base_gate_result"),session.get("theme_gate_result"),session.get("error_type"),
                 session.get("error_nature"),now,now),
            )
            for poll in session["polls"]:
                parsed = poll["parsed"]
                cursor = conn.execute(
                    """INSERT INTO ppl_check_polls(
                           check_session_id,candidate_id,alpha_id,phase,semantic_poll_index,http_request_delta,
                           raw_payload_json,parsed_payload_json,live_base_gate_result,live_theme_gate_result,
                           individual_checks_json,limits_json,pending,error_code,error_nature,created_at,
                           http_status,raw_response_text,parse_status,pending_check_names_json,error_type,
                           parser_version,alias_version,evidence_source
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (session["check_session_id"],session.get("candidate_id"),session["alpha_id"],session["phase"],
                     poll["semantic_poll_index"],poll["http_request_count_delta"],
                     json.dumps(parsed.get("raw_payload"),ensure_ascii=False,sort_keys=True),
                     json.dumps(parsed,ensure_ascii=False,sort_keys=True),parsed.get("base_gate",{}).get("status"),
                     parsed.get("theme_gate",{}).get("status"),json.dumps(parsed.get("results",[]),ensure_ascii=False,sort_keys=True),
                     json.dumps({},ensure_ascii=False),int(parsed.get("session_semantic_status") != "RESOLVED"),
                     parsed.get("error_type"),parsed.get("error_nature"),poll["created_at"],poll["http_status"],
                     poll["raw_response_text"],parsed.get("parse_status"),json.dumps(parsed.get("pending_check_names",[]),ensure_ascii=False),
                     parsed.get("error_type"),parsed.get("check_parser_version",1),parsed.get("check_alias_version",1),evidence_source),
                )
                poll_id = cursor.lastrowid
                for item in parsed.get("results", []):
                    conn.execute(
                        """INSERT INTO ppl_check_results(
                               check_session_id,poll_id,candidate_id,alpha_id,phase,raw_name,normalized_name,category,
                               raw_result,normalized_result,raw_value_json,raw_limit_json,normalized_value,normalized_limit,
                               unit,unit_confidence,preset_limit_json,live_limit_json,effective_limit_json,limit_source,
                               status,message,parser_version,alias_version,evidence_source,mapping_suggestion,created_at,
                               eligibility_outcome,eligibility_reason,threshold_exceeded,diagnosis_outcome,diagnosis_reason
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (session["check_session_id"],poll_id,session.get("candidate_id"),session["alpha_id"],session["phase"],
                         item.get("raw_name"),item["normalized_name"],item["category"],item.get("raw_result"),
                         item["normalized_result"],json.dumps(item.get("raw_value"),ensure_ascii=False),
                         json.dumps(item.get("raw_limit"),ensure_ascii=False),item.get("normalized_value"),
                         item.get("normalized_limit"),item.get("unit"),item["unit_confidence"],
                         json.dumps(item.get("preset_limit"),ensure_ascii=False),json.dumps(item.get("live_limit"),ensure_ascii=False),
                         json.dumps(item.get("effective_limit"),ensure_ascii=False),item.get("limit_source"),item.get("status"),
                         item.get("message"),item["parser_version"],item["alias_version"],evidence_source,
                         item.get("mapping_suggestion"),now,item.get("eligibility_outcome"),
                         item.get("eligibility_reason"),int(bool(item.get("threshold_exceeded"))),
                         item.get("diagnosis_outcome"),item.get("diagnosis_reason")),
                    )

    def get_or_create_run(self, config: Any, run_id: Optional[str] = None) -> str:
        """Reuse a hash-compatible run or create the next stable run_NNNN id."""
        self.initialize()
        with self.connect() as conn:
            if run_id:
                row = conn.execute("SELECT execution_hash FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
                if row:
                    compat = validate_execution_hash_compatibility(config, row["execution_hash"])
                    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:
                        raise ValueError(f"RUN_EXECUTION_HASH_{compat['status']}")
                    return run_id
            else:
                row = conn.execute(
                    "SELECT run_id FROM ppl_runs WHERE execution_hash=? ORDER BY created_at DESC LIMIT 1",
                    (config.execution_hash,),
                ).fetchone()
                if row:
                    return str(row["run_id"])
                numbers = []
                for item in conn.execute("SELECT run_id FROM ppl_runs"):
                    value = str(item[0])
                    if value.startswith("run_") and value[4:].isdigit():
                        numbers.append(int(value[4:]))
                run_id = f"run_{max(numbers, default=0) + 1:04d}"
        self.create_run(run_id, config)
        return run_id

    def save_discovery_snapshot(
        self,
        snapshot: Dict[str, Any],
        datasets: Any,
        fields: Any,
    ) -> None:
        """Insert an immutable discovery snapshot and update the latest catalog view."""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_discovery_snapshots(
                       snapshot_id, region, universe, delay, instrument_type, source,
                       dataset_count, field_count, metadata_hash, exclusion_status_json,
                       automatic_preselection, discovery_pool_size, created_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot["snapshot_id"], snapshot["region"], snapshot["universe"],
                    snapshot["delay"], snapshot["instrument_type"], snapshot["source"],
                    snapshot["dataset_count"], snapshot["field_count"], snapshot["metadata_hash"],
                    json.dumps(snapshot["exclusion_status"], ensure_ascii=False, sort_keys=True),
                    int(snapshot.get("automatic_preselection", True)),
                    int(snapshot.get("discovery_pool_size", 0)),
                    snapshot["created_at"],
                ),
            )
            for dataset in datasets:
                conn.execute(
                    """INSERT INTO ppl_dataset_catalog(
                           snapshot_id, dataset_id, selected, in_discovery_pool, excluded,
                           dataset_semantic_hint, dataset_hint_source, dataset_hint_confidence,
                           dataset_hint_rule_id, dataset_hint_matched_text,
                           dataset_preselection_score, preselection_components_json,
                           field_evidence_json, raw_metadata_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot["snapshot_id"], dataset["dataset_id"], int(dataset["selected"]),
                        int(dataset.get("in_discovery_pool", dataset["selected"])), int(dataset["excluded"]),
                        dataset.get("dataset_semantic_hint"), dataset.get("dataset_hint_source"),
                        dataset.get("dataset_hint_confidence"), dataset.get("dataset_hint_rule_id"),
                        dataset.get("dataset_hint_matched_text"), dataset.get("dataset_preselection_score"),
                        json.dumps(dataset.get("preselection_components"), ensure_ascii=False, sort_keys=True),
                        json.dumps(dataset.get("field_evidence"), ensure_ascii=False, sort_keys=True),
                        json.dumps(dataset["raw_metadata"], ensure_ascii=False, sort_keys=True),
                    ),
                )
            for field in fields:
                conn.execute(
                    """INSERT INTO ppl_field_catalog(
                           snapshot_id, dataset_id, field_id, description, field_type,
                           coverage, date_coverage, user_count, alpha_count, pyramid_multiplier,
                           category_json, subcategory_json, themes_json, semantic_class,
                           classification_source, classification_rule_id, classification_confidence,
                           classification_warning, matched_text, coverage_pass, selected, raw_metadata_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot["snapshot_id"], field["dataset_id"], field["field_id"],
                        field.get("description"), field.get("field_type"), field.get("coverage"),
                        field.get("dateCoverage"), field.get("userCount"), field.get("alphaCount"),
                        field.get("pyramidMultiplier"),
                        json.dumps(field.get("category"), ensure_ascii=False, sort_keys=True),
                        json.dumps(field.get("subcategory"), ensure_ascii=False, sort_keys=True),
                        json.dumps(field.get("themes"), ensure_ascii=False, sort_keys=True),
                        field["semantic_class"], field["classification_source"],
                        field.get("classification_rule_id"), field["classification_confidence"],
                        field.get("classification_warning"), field.get("matched_text"),
                        int(field["coverage_pass"]), int(field["selected"]),
                        json.dumps(field["raw_metadata"], ensure_ascii=False, sort_keys=True),
                    ),
                )
                now = snapshot["created_at"]
                conn.execute(
                    """INSERT INTO ppl_catalog(
                           dataset_id, field_id, field_type, semantic_class,
                           classification_source, classification_rule_id,
                           classification_confidence, classification_warning, matched_text,
                           raw_metadata_json, created_at, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(dataset_id, field_id) DO UPDATE SET
                           field_type=excluded.field_type,
                           semantic_class=excluded.semantic_class,
                           classification_source=excluded.classification_source,
                           classification_rule_id=excluded.classification_rule_id,
                           classification_confidence=excluded.classification_confidence,
                           classification_warning=excluded.classification_warning,
                           matched_text=excluded.matched_text,
                           raw_metadata_json=excluded.raw_metadata_json,
                           updated_at=excluded.updated_at""",
                    (
                        field["dataset_id"], field["field_id"], field.get("field_type"),
                        field["semantic_class"], field["classification_source"],
                        field.get("classification_rule_id"), field["classification_confidence"],
                        field.get("classification_warning"), field.get("matched_text"),
                        json.dumps(field["raw_metadata"], ensure_ascii=False, sort_keys=True), now, now,
                    ),
                )

    def load_latest_discovery(self, settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        with self.connect() as conn:
            row = conn.execute(
                """SELECT * FROM ppl_discovery_snapshots
                   WHERE region=? AND universe=? AND delay=? AND instrument_type=?
                   ORDER BY created_at DESC LIMIT 1""",
                (
                    settings["region"], settings["universe"], settings["delay"],
                    settings["instrument_type"],
                ),
            ).fetchone()
            if row is None:
                return None
            snapshot = dict(row)
            snapshot["exclusion_status"] = json.loads(snapshot.pop("exclusion_status_json"))
            snapshot["automatic_preselection"] = bool(snapshot["automatic_preselection"])
            datasets = []
            for item in conn.execute(
                "SELECT * FROM ppl_dataset_catalog WHERE snapshot_id=? ORDER BY dataset_id",
                (snapshot["snapshot_id"],),
            ):
                x = dict(item)
                x["selected"] = bool(x["selected"])
                x["in_discovery_pool"] = bool(x["in_discovery_pool"])
                x["excluded"] = bool(x["excluded"])
                x["preselection_components"] = json.loads(x.pop("preselection_components_json") or "null")
                x["field_evidence"] = json.loads(x.pop("field_evidence_json") or "null")
                x["raw_metadata"] = json.loads(x.pop("raw_metadata_json"))
                datasets.append(x)
            fields = []
            for item in conn.execute(
                "SELECT * FROM ppl_field_catalog WHERE snapshot_id=? ORDER BY dataset_id, field_id",
                (snapshot["snapshot_id"],),
            ):
                x = dict(item)
                for key in ("category", "subcategory", "themes"):
                    x[key] = json.loads(x.pop(f"{key}_json"))
                x["raw_metadata"] = json.loads(x.pop("raw_metadata_json"))
                x["coverage_pass"] = bool(x["coverage_pass"])
                x["selected"] = bool(x["selected"])
                fields.append(x)
            return {"snapshot": snapshot, "datasets": datasets, "fields": fields}

    def save_dry_run(self, dry_run_id: str, snapshot_id: str, execution_hash: str, source: str, report: Dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_dry_run_snapshots(
                       dry_run_id, discovery_snapshot_id, execution_hash, source, report_json, created_at
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    dry_run_id, snapshot_id, execution_hash, source,
                    json.dumps(report, ensure_ascii=False, sort_keys=True), _utc_now(),
                ),
            )

    def load_latest_dry_run(self, discovery_snapshot_id: str, execution_hash: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT * FROM ppl_dry_run_snapshots
                   WHERE discovery_snapshot_id=? AND execution_hash=?
                   ORDER BY created_at DESC LIMIT 1""",
                (discovery_snapshot_id, execution_hash),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["report"] = json.loads(result.pop("report_json"))
            return result

    def upsert_candidates(self, candidates: Any) -> None:
        """Persist planning facts without overwriting simulation facts from prior previews."""
        now = _utc_now()
        with self.connect() as conn:
            for item in candidates:
                existing = conn.execute(
                    "SELECT candidate_id FROM ppl_candidates WHERE run_id=? AND sim_key=?",
                    (item["run_id"], item["sim_key"]),
                ).fetchone()
                candidate_id = str(existing["candidate_id"]) if existing else item["candidate_id"]
                if existing:
                    conn.execute(
                        """UPDATE ppl_candidates SET
                               expression=?, expression_raw=?, expression_canonical=?, expression_hash=?,
                               settings_json=?, settings_hash=?, dataset_id=?, field_id=?, field_type=?,
                               semantic_class=?, direction=?, signal_family=?, transform_family=?, operator=?,
                               window=?, decay=?, neutralization=?, legacy_unique_operator_count=?,
                               pp_total_operator_count_estimate=?, pp_operator_estimator_version=?,
                               discovery_snapshot_id=?, dry_run_snapshot_id=?, vector_reducer=?,
                               data_field_count_estimate=?, data_fields_used_json=?, cache_classification=?,
                               execution_action=?, initial_selection_score=?, selection_reason=?, selection_rank=?,
                               selected_for_initial_search=?, structure_status=?, available_result_json=?, updated_at=?
                           WHERE candidate_id=?""",
                        self._candidate_update_values(item, now) + (candidate_id,),
                    )
                else:
                    conn.execute(
                        """INSERT INTO ppl_candidates(
                               candidate_id, run_id, expression, sim_key, settings_json, settings_hash,
                               context_fingerprint, dataset_id, field_id, field_type, semantic_class,
                               direction, signal_family, transform_family, operator, window, decay,
                               neutralization, legacy_unique_operator_count, pp_total_operator_count_estimate,
                               pp_operator_estimator_version, lifecycle_state, simulation_status, alpha_id,
                               created_at, updated_at, expression_raw, expression_canonical, expression_hash,
                               discovery_snapshot_id, dry_run_snapshot_id, vector_reducer,
                               data_field_count_estimate, data_fields_used_json, cache_classification,
                               execution_action, initial_selection_score, selection_reason, selection_rank,
                               selected_for_initial_search, structure_status, available_result_json
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PLANNED',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            item["candidate_id"], item["run_id"], item["expression_canonical"], item["sim_key"],
                            item["settings_json"], item["settings_hash"], item["context_fingerprint"],
                            item["dataset_id"], item["field_id"], item["field_type"], item["semantic_class"],
                            item["direction"], item["signal_family"], item["transform_family"], item["operator"],
                            item.get("window"), item["decay"], item["neutralization"],
                            item["legacy_unique_operator_count"], item["pp_total_operator_count_estimate"],
                            item["pp_operator_estimator_version"], item["simulation_status"], item.get("alpha_id"),
                            now, now, item["expression_raw"], item["expression_canonical"], item["expression_hash"],
                            item["discovery_snapshot_id"], item["dry_run_snapshot_id"], item["vector_reducer"],
                            item["data_field_count_estimate"], json.dumps(item["data_fields_used"], ensure_ascii=False),
                            item["cache_classification"], item["execution_action"], item["initial_selection_score"],
                            item["selection_reason"], item.get("selection_rank"),
                            int(item["selected_for_initial_search"]), item["structure_status"],
                            json.dumps(item.get("available_result"), ensure_ascii=False, sort_keys=True),
                        ),
                    )
                provenance_id = item["provenance_id"]
                conn.execute(
                    """INSERT INTO ppl_candidate_provenance(
                           provenance_id, candidate_id, run_id, sim_key, context_fingerprint,
                           discovery_snapshot_id, dry_run_snapshot_id, provenance_json, created_at, updated_at
                       ) VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(run_id, sim_key, context_fingerprint) DO UPDATE SET
                           provenance_json=excluded.provenance_json, updated_at=excluded.updated_at""",
                    (
                        provenance_id, candidate_id, item["run_id"], item["sim_key"], item["context_fingerprint"],
                        item["discovery_snapshot_id"], item["dry_run_snapshot_id"],
                        json.dumps(item["provenance"], ensure_ascii=False, sort_keys=True), now, now,
                    ),
                )

    @staticmethod
    def _candidate_update_values(item: Dict[str, Any], now: str):
        return (
            item["expression_canonical"], item["expression_raw"], item["expression_canonical"], item["expression_hash"],
            item["settings_json"], item["settings_hash"], item["dataset_id"], item["field_id"], item["field_type"],
            item["semantic_class"], item["direction"], item["signal_family"], item["transform_family"],
            item["operator"], item.get("window"), item["decay"], item["neutralization"],
            item["legacy_unique_operator_count"], item["pp_total_operator_count_estimate"],
            item["pp_operator_estimator_version"], item["discovery_snapshot_id"], item["dry_run_snapshot_id"],
            item["vector_reducer"], item["data_field_count_estimate"],
            json.dumps(item["data_fields_used"], ensure_ascii=False), item["cache_classification"],
            item["execution_action"], item["initial_selection_score"], item["selection_reason"],
            item.get("selection_rank"), int(item["selected_for_initial_search"]), item["structure_status"],
            json.dumps(item.get("available_result"), ensure_ascii=False, sort_keys=True), now,
        )

    def upsert_operator_evidence(self, records: Any) -> None:
        with self.connect() as conn:
            for item in records:
                conn.execute(
                    """INSERT INTO ppl_operator_capabilities(
                           operator_name, signature_hash, operator_metadata_hash,
                           capability_class, status, source, evidence_json,
                           validated_at, last_seen_at, validation_error,
                           complete_expression_count, example_sim_keys_json,
                           signature, evidence_note
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(operator_name, signature_hash) DO UPDATE SET
                           operator_metadata_hash=excluded.operator_metadata_hash,
                           capability_class=excluded.capability_class,
                           status=excluded.status,
                           source=excluded.source,
                           evidence_json=excluded.evidence_json,
                           validated_at=excluded.validated_at,
                           last_seen_at=excluded.last_seen_at,
                           validation_error=excluded.validation_error,
                           complete_expression_count=excluded.complete_expression_count,
                           example_sim_keys_json=excluded.example_sim_keys_json,
                           signature=excluded.signature,
                           evidence_note=excluded.evidence_note""",
                    (
                        item["operator_name"], item["signature_hash"], item.get("operator_metadata_hash"),
                        item["operator_class"], item["status"], item["source"],
                        json.dumps(item.get("evidence", {}), ensure_ascii=False, sort_keys=True),
                        item.get("validated_at"), item.get("last_seen_at"), item.get("validation_error"),
                        item["complete_expression_count"],
                        json.dumps(item.get("example_sim_keys", []), ensure_ascii=False),
                        item.get("signature"), item.get("evidence_note"),
                    ),
                )

    def operator_registry_summary(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"total": 0, "by_status": {}, "by_class": {}}
        with self.connect() as conn:
            by_status = dict(conn.execute(
                "SELECT status, count(1) FROM ppl_operator_capabilities GROUP BY status"
            ).fetchall())
            by_class = dict(conn.execute(
                "SELECT capability_class, count(1) FROM ppl_operator_capabilities GROUP BY capability_class"
            ).fetchall())
            return {"total": sum(by_status.values()), "by_status": by_status, "by_class": by_class}

    def check_summary(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        if not self.path.exists():
            return {"sessions": 0, "unknown_check_count": 0, "unknown_check_names": []}
        with self.connect() as conn:
            clause = "WHERE run_id=?" if run_id else ""
            params = (run_id,) if run_id else ()
            sessions = conn.execute(f"SELECT COUNT(*) FROM ppl_check_sessions {clause}", params).fetchone()[0]
            join_clause = "WHERE s.run_id=? AND r.normalized_name='UNKNOWN'" if run_id else "WHERE r.normalized_name='UNKNOWN'"
            unknown = [row[0] for row in conn.execute(
                f"""SELECT DISTINCT r.raw_name FROM ppl_check_results r
                    JOIN ppl_check_sessions s ON s.check_session_id=r.check_session_id
                    {join_clause} ORDER BY r.raw_name LIMIT 10""", params
            )]
            return {"sessions": int(sessions), "unknown_check_count": len(unknown), "unknown_check_names": unknown}

    def load_reference_pool(self) -> list:
        if not self.path.exists(): return []
        with self.connect() as conn: return [dict(x) for x in conn.execute("SELECT * FROM ppl_reference_pool WHERE active=1")]

    def load_manual_evidence(self) -> list:
        if not self.path.exists(): return []
        with self.connect() as conn: return [dict(x) for x in conn.execute("SELECT * FROM ppl_manual_evidence ORDER BY created_at")]

    def save_derived(self, run_id: str, families: list, members: list, scores: list) -> None:
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("DELETE FROM ppl_families WHERE run_id=?",(run_id,)); conn.execute("DELETE FROM ppl_family_members WHERE run_id=?",(run_id,)); conn.execute("DELETE FROM ppl_priority_scores WHERE run_id=?",(run_id,))
            for f in families: conn.execute("INSERT INTO ppl_families VALUES(?,?,?,?,?,?)",(run_id,f["family_id"],f["family_fingerprint"],json.dumps(f,ensure_ascii=False,sort_keys=True),now,1))
            for m in members: conn.execute("INSERT INTO ppl_family_members VALUES(?,?,?,?,?,?)",(run_id,m["family_id"],m["candidate_id"],m["family_role"],m["evidence_level"],now))
            for s in scores: conn.execute("INSERT INTO ppl_priority_scores VALUES(?,?,?,?,?,?,?)",(run_id,s["candidate_id"],s["research_priority_score"],s["priority_confidence"],json.dumps(s,ensure_ascii=False,sort_keys=True),now,s["priority_score_version"]))

    def save_description_version(self, draft: Dict[str, Any]) -> int:
        """Append a version; never overwrites AUTO/MANUAL/API history."""
        with self.connect() as conn:
            version=int(draft.get("version") or (conn.execute("SELECT coalesce(max(version),0)+1 FROM ppl_descriptions WHERE candidate_id=?",(draft["candidate_id"],)).fetchone()[0]))
            conn.execute("""INSERT INTO ppl_descriptions(candidate_id,version,idea,data_rationale,operator_rationale,full_text,validation_status,validation_warnings_json,source,created_at,updated_at,field_metadata_source,expression_snapshot,operator_snapshot_json,description_template_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(draft["candidate_id"],version,draft.get("idea"),draft.get("data_rationale"),draft.get("operator_rationale"),draft.get("full_text"),draft.get("validation_status"),json.dumps(draft.get("validation_warnings",[]),ensure_ascii=False),draft["source"],draft["created_at"],draft.get("updated_at") or draft["created_at"],draft.get("field_metadata_source"),draft.get("expression_snapshot"),json.dumps(draft.get("operator_snapshot",[])),draft.get("description_template_version",1)))
            return version

    def save_property_snapshot(self, snapshot: Dict[str, Any], *, candidate_id=None, alpha_id=None, phase="REFRESH", previous_snapshot_id=None) -> None:
        with self.connect() as conn:
            conn.execute("""INSERT OR IGNORE INTO ppl_property_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(snapshot["snapshot_id"],candidate_id,alpha_id,phase,int(snapshot["description_present"]),snapshot.get("description_text"),snapshot["description_length"],json.dumps(snapshot.get("tags_raw"),ensure_ascii=False),json.dumps(snapshot.get("normalized_tags",[]),ensure_ascii=False),snapshot["power_pool_selected_status"],snapshot["submission_status"],json.dumps(snapshot["raw_payload"],ensure_ascii=False,sort_keys=True),snapshot["property_parser_version"],snapshot["tag_alias_version"],snapshot["evidence_source"],previous_snapshot_id,snapshot["captured_at"]))

    def save_manual_actions(self, run_id: str, actions: list) -> None:
        now=_utc_now()
        with self.connect() as conn:
            for x in actions: conn.execute("INSERT OR IGNORE INTO ppl_manual_actions(manual_action_id,run_id,candidate_id,alpha_id,action_type,status,created_at) VALUES(?,?,?,?,?,?,?)",(x["manual_action_id"],run_id,x.get("candidate_id"),x.get("alpha_id"),x["action_type"],x.get("status","PENDING"),now))

    def save_live_validation(self, session_id: str, started_at: str, evidence: list, summary: Dict[str, Any]) -> None:
        now=_utc_now()
        with self.connect() as conn:
            conn.execute("INSERT INTO ppl_live_validation_sessions VALUES(?,?,?,?,?)",(session_id,started_at,now,"COMPLETE",json.dumps(summary,ensure_ascii=False,sort_keys=True)))
            for x in evidence:conn.execute("""INSERT INTO ppl_live_validation_evidence(validation_session_id,target_type,target_id,endpoint,http_method,http_status,raw_payload_sanitized_json,schema_fingerprint,parser_version,observations_json,captured_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",(session_id,x["target_type"],x.get("target_id"),x["endpoint"],x["http_method"],x.get("http_status"),json.dumps(x["raw_payload_sanitized"],ensure_ascii=False,sort_keys=True),x["schema_fingerprint"],x.get("parser_version"),json.dumps(x.get("observations",{}),ensure_ascii=False,sort_keys=True),now))
