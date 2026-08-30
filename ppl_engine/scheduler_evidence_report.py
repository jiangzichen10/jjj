"""Read-only V3.1 Scheduler Evidence calibration report.

This module exists between D2 evidence collection and any future D3 canary.
It reads durable D2 Scheduler evidence, summarizes how much real observation
has accumulated, and exposes statistical precision/coverage.  It never writes
SQLite, never edits activation thresholds, and never changes Search/Repair
execution.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sqlite3
import statistics
from typing import Any, Iterable, Mapping, Optional, Sequence

from .scheduler_evidence import evidence_policy_from_mapping, evidence_policy_hash
from .scheduler_shadow import (
    is_matured_productivity_row, policy_from_mapping, shadow_policy_hash,
)
from .strategy_contracts import SchedulerActionType
from .research_run_mode import research_run_status


REPORT_SCHEMA = "V31_SCHED_EVIDENCE_CALIBRATION_REPORT_001"
REPORT_MODE = "READ_ONLY_EVIDENCE_REVIEW"


def _upper(value: Any) -> str:
    return str(value or "").upper()


def _ratio(numerator: int | float, denominator: int | float) -> Optional[float]:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def _wilson_interval(successes: int, total: int, z: float = 1.959963984540054) -> Optional[dict[str, float]]:
    """Return a two-sided Wilson score interval for a Bernoulli proportion."""
    n = int(total)
    x = int(successes)
    if n <= 0:
        return None
    p = x / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = z * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n))) / denom
    return {
        "rate": p,
        "lower": max(0.0, center - half),
        "upper": min(1.0, center + half),
        "half_width": half,
        "confidence": 0.95,
        "method": "WILSON_SCORE",
    }


def _summary(values: Sequence[float]) -> dict[str, Optional[float] | int]:
    clean = [float(v) for v in values]
    if not clean:
        return {
            "count": 0,
            "min": None,
            "p25": None,
            "median": None,
            "mean": None,
            "p75": None,
            "max": None,
        }
    ordered = sorted(clean)

    def percentile(q: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        pos = (len(ordered) - 1) * q
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return ordered[lo]
        frac = pos - lo
        return ordered[lo] * (1.0 - frac) + ordered[hi] * frac

    return {
        "count": len(ordered),
        "min": ordered[0],
        "p25": percentile(0.25),
        "median": statistics.median(ordered),
        "mean": statistics.fmean(ordered),
        "p75": percentile(0.75),
        "max": ordered[-1],
    }


def _readonly_connection(db_path: Path) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(str(path))
    uri = path.as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _load_round(conn: sqlite3.Connection, run_id: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM ppl_rounds WHERE run_id=? ORDER BY started_at DESC LIMIT 1",
        (str(run_id),),
    ).fetchone()
    return dict(row) if row is not None else None


def _stored_policy(round_row: Mapping[str, Any]) -> dict[str, Any]:
    raw = str(round_row.get("config_json") or "{}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("SCHEDULER_EVIDENCE_STORED_POLICY_INVALID_JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("SCHEDULER_EVIDENCE_STORED_POLICY_NOT_MAPPING")
    return value


def _rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, tuple(params))]


def _counts(rows: Iterable[Mapping[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))


def _disagreement_matrix(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    matrix: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        matrix[str(row.get("actual_action") or "UNKNOWN")][str(row.get("shadow_action") or "UNKNOWN")] += 1
    return {actual: dict(sorted(counter.items())) for actual, counter in sorted(matrix.items())}


def _action_outcome(rows: Sequence[Mapping[str, Any]], action: str) -> dict[str, Any]:
    subset = [row for row in rows if _upper(row.get("actual_action")) == action]
    total_new = sum(int(row.get("total_new_posts") or 0) for row in subset)
    matured = sum(int(row.get("matured_new_posts") or 0) for row in subset)
    censored = sum(int(row.get("censored_new_posts") or 0) for row in subset)
    complete = sum(int(row.get("complete_count") or 0) for row in subset)
    ready = sum(int(row.get("ready_count") or 0) for row in subset)
    near_pass = sum(int(row.get("near_pass_count") or 0) for row in subset)
    distinct_family_batch_sum = sum(int(row.get("distinct_family_count") or 0) for row in subset)
    resolved = sum(int(row.get("repair_resolved_count") or 0) for row in subset)
    target_pass = sum(int(row.get("repair_target_pass_count") or 0) for row in subset)
    improved = sum(int(row.get("repair_improved_count") or 0) for row in subset)
    accept = sum(int(row.get("repair_accept_count") or 0) for row in subset)
    effective = sum(int(row.get("effective_simulation_count") or 0) for row in subset)
    payload: dict[str, Any] = {
        "decision_count": len(subset),
        "outcome_state_counts": _counts(subset, "outcome_state"),
        "total_new_posts": total_new,
        "matured_new_posts": matured,
        "censored_new_posts": censored,
        "maturity_ratio": _ratio(matured, total_new),
        "complete_count": complete,
        "effective_simulation_ratio": _ratio(effective, matured),
        "distinct_family_batch_sum": distinct_family_batch_sum,
        "complete_rate_ci95": _wilson_interval(complete, matured),
    }
    if action == "SEARCH":
        payload.update({
            "ready_count": ready,
            "near_pass_count": near_pass,
            "ready_rate": _ratio(ready, matured),
            "near_pass_rate": _ratio(near_pass, matured),
            "ready_rate_ci95": _wilson_interval(ready, matured),
            "near_pass_rate_ci95": _wilson_interval(near_pass, matured),
        })
    elif action == "REPAIR":
        successful = target_pass + improved + accept
        payload.update({
            "repair_resolved_count": resolved,
            "repair_target_pass_count": target_pass,
            "repair_improved_count": improved,
            "repair_accept_count": accept,
            "repair_success_count": successful,
            "repair_resolved_rate": _ratio(resolved, matured),
            "repair_success_rate": _ratio(successful, matured),
            "repair_success_rate_ci95": _wilson_interval(successful, matured),
        })
    return payload


def _identity_scope(
    evaluations: Sequence[Mapping[str, Any]],
    *,
    scheduler_version: str,
    scheduler_hash: str,
    evidence_version: str,
    evidence_hash: str,
) -> dict[str, Any]:
    matching = [
        row for row in evaluations
        if str(row.get("scheduler_policy_hash") or "") == scheduler_hash
        and str(row.get("evidence_policy_hash") or "") == evidence_hash
    ]
    scheduler_version_conflict = any(
        str(row.get("scheduler_policy_version") or "") == scheduler_version
        and str(row.get("scheduler_policy_hash") or "") != scheduler_hash
        for row in evaluations
    )
    evidence_version_conflict = any(
        str(row.get("evidence_policy_version") or "") == evidence_version
        and str(row.get("evidence_policy_hash") or "") != evidence_hash
        for row in evaluations
    )
    return {
        "all_observations": len(evaluations),
        "matching_current_identity": len(matching),
        "excluded_other_identity": len(evaluations) - len(matching),
        "scheduler_version_hash_conflict": scheduler_version_conflict,
        "evidence_version_hash_conflict": evidence_version_conflict,
        "policy_identity_pass": bool(matching) and not scheduler_version_conflict and not evidence_version_conflict,
        "matching_rows": matching,
    }



def _productivity_window_series(evaluations: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluations:
        raw = row.get(field)
        try:
            values = json.loads(str(raw or "[]")) if isinstance(raw, str) else list(raw or [])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for item in values:
            if not isinstance(item, Mapping):
                continue
            window = int(item.get("window") or 0)
            if window <= 0:
                continue
            by_window[window].append({
                "batch_no": int(row.get("batch_no") or 0),
                "score": float(item.get("score") or 0.0),
                "matured_attempts": int(item.get("attempts") or 0),
                "observed_attempts": int(item.get("observed_attempts") or 0),
                "censored_attempts": int(item.get("censored_attempts") or 0),
            })
    out: dict[str, Any] = {}
    for window, series in sorted(by_window.items()):
        series = sorted(series, key=lambda x: x["batch_no"])
        scores = [x["score"] for x in series]
        deltas = [abs(scores[i] - scores[i - 1]) for i in range(1, len(scores))]
        out[str(window)] = {
            "observation_count": len(series),
            "score_summary": _summary(scores),
            "absolute_step_change_summary": _summary(deltas),
            "latest_score": scores[-1] if scores else None,
            "latest_matured_attempts": series[-1]["matured_attempts"] if series else 0,
            "latest_observed_attempts": series[-1]["observed_attempts"] if series else 0,
            "latest_censored_attempts": series[-1]["censored_attempts"] if series else 0,
            "stability_threshold": None,
            "stability_pass": None,
        }
    return out


def _duration_seconds(start: Any, end: Any) -> Optional[float]:
    from datetime import datetime
    if not start or not end:
        return None
    try:
        return max(0.0, (datetime.fromisoformat(str(end)) - datetime.fromisoformat(str(start))).total_seconds())
    except (TypeError, ValueError):
        return None


def _maturation_latency(rows: Sequence[Mapping[str, Any]], action: SchedulerActionType) -> dict[str, Any]:
    relevant = [
        row for row in rows
        if _upper(row.get("phase")) == action.value
        and _upper(row.get("origin")) == "NEW_POST"
    ]
    matured = [row for row in relevant if is_matured_productivity_row(row, action)]
    latencies = []
    for row in matured:
        # updated_at is the durable telemetry observation time, not necessarily
        # the exact remote/check event time. Keep this explicitly labelled proxy.
        value = _duration_seconds(row.get("post_started_at") or row.get("created_at"), row.get("updated_at"))
        if value is not None:
            latencies.append(value)
    return {
        "observed_new_posts": len(relevant),
        "matured_new_posts": len(matured),
        "right_censored_new_posts": len(relevant) - len(matured),
        "durable_observation_latency_proxy_seconds": _summary(latencies),
        "latency_semantics": "POST_OR_LEDGER_CREATE_TO_LATEST_DURABLE_LEDGER_UPDATE_PROXY",
        "minimum_observation_age_seconds": None,
    }

def build_scheduler_evidence_report(db_path: Path, *, run_id: str) -> dict[str, Any]:
    """Build a read-only calibration report from durable D2 evidence.

    No SQLite schema helper is called here on purpose: a reporting command must
    never create or migrate evidence tables just by being observed.
    """
    if not str(run_id or "").strip():
        raise ValueError("SCHEDULER_EVIDENCE_REPORT_RUN_ID_REQUIRED")

    with _readonly_connection(Path(db_path)) as conn:
        tables = _table_names(conn)
        if "ppl_rounds" not in tables:
            raise ValueError("SCHEDULER_EVIDENCE_ROUND_SCHEMA_MISSING")
        round_row = _load_round(conn, str(run_id))
        if round_row is None:
            raise ValueError("SCHEDULER_EVIDENCE_RUN_NOT_FOUND")
        round_id = str(round_row.get("round_id") or "")
        policy = _stored_policy(round_row)
        shadow_raw = dict(policy.get("scheduler_shadow") or {})
        evidence_raw = dict(policy.get("scheduler_evidence") or {})
        if not shadow_raw or not evidence_raw:
            return {
                "report_schema": REPORT_SCHEMA,
                "mode": REPORT_MODE,
                "run_id": str(run_id),
                "round_id": round_id,
                "status": "D2_EVIDENCE_NOT_CONFIGURED_FOR_RUN",
                "authoritative": False,
                "activation_side_effect": False,
                "database_writes": 0,
                "threshold_mutations": 0,
            }

        scheduler_policy = policy_from_mapping(shadow_raw)
        evidence_policy = evidence_policy_from_mapping(evidence_raw)
        run_identity = research_run_status(policy, run_id=str(run_id))
        sched_hash = shadow_policy_hash(scheduler_policy)
        evid_hash = evidence_policy_hash(evidence_raw)

        required = {
            "ppl_round_scheduler_evaluations",
            "ppl_round_scheduler_outcomes",
        }
        missing_tables = sorted(required - tables)
        if missing_tables:
            return {
                "report_schema": REPORT_SCHEMA,
                "mode": REPORT_MODE,
                "run_id": str(run_id),
                "round_id": round_id,
                "round_status": str(round_row.get("status") or ""),
                "round_phase": str(round_row.get("phase") or ""),
                "status": "WAITING_FOR_D2_EVIDENCE_TABLES",
                "missing_tables": missing_tables,
                "research_run": run_identity,
                "maturation_protocol": {
                    "semantics": run_identity.get("maturation_semantics"),
                    "long_running_semantics": run_identity.get("long_running_semantics"),
                    "state_based_before_time_based": True,
                    "minimum_observation_age_seconds": None,
                    "post_hoc_age_threshold_forbidden": True,
                },
                "policy_identity": {
                    "scheduler_policy_version": scheduler_policy.policy_version,
                    "scheduler_policy_hash": sched_hash,
                    "evidence_policy_version": evidence_policy.policy_version,
                    "evidence_policy_hash": evid_hash,
                },
                "authoritative": False,
                "activation_side_effect": False,
                "database_writes": 0,
                "threshold_mutations": 0,
            }

        evaluations_all = _rows(
            conn,
            "SELECT * FROM ppl_round_scheduler_evaluations WHERE round_id=? ORDER BY batch_no,decision_timestamp",
            (round_id,),
        )
        identity = _identity_scope(
            evaluations_all,
            scheduler_version=scheduler_policy.policy_version,
            scheduler_hash=sched_hash,
            evidence_version=evidence_policy.policy_version,
            evidence_hash=evid_hash,
        )
        evaluations = list(identity.pop("matching_rows"))
        decision_keys = {str(row.get("decision_key") or "") for row in evaluations}
        outcomes_all = _rows(
            conn,
            "SELECT * FROM ppl_round_scheduler_outcomes WHERE round_id=? ORDER BY batch_no,decision_key",
            (round_id,),
        )
        outcomes = [row for row in outcomes_all if str(row.get("decision_key") or "") in decision_keys]
        ledger_rows = []
        if "ppl_round_simulation_ledger" in tables:
            ledger_rows = _rows(
                conn,
                "SELECT * FROM ppl_round_simulation_ledger WHERE round_id=? ORDER BY logical_sequence_no",
                (round_id,),
            )

        agreement_count = sum(int(row.get("agreement") or 0) == 1 for row in evaluations)
        replay_pass_count = sum(int(row.get("replay_pass") or 0) == 1 for row in evaluations)
        no_slot_rows = [row for row in evaluations if int(row.get("remote_slots_free") or 0) <= 0]
        no_slot_wait = sum(_upper(row.get("shadow_action")) == "WAIT" for row in no_slot_rows)
        both_backlogs_free_slot = [
            row for row in evaluations
            if int(row.get("search_backlog") or 0) > 0
            and int(row.get("repair_backlog") or 0) > 0
            and int(row.get("remote_slots_free") or 0) > 0
        ]
        both_executable_free_slot = [
            row for row in evaluations
            if int(row.get("search_evaluation_complete") or 0) == 1
            and int(row.get("repair_evaluation_complete") or 0) == 1
            and int(row.get("search_execution_eligible_count") or 0) > 0
            and int(row.get("repair_execution_eligible_count") or 0) > 0
            and int(row.get("remote_slots_free") or 0) > 0
        ]
        starvation_guard = [
            row for row in evaluations
            if str(row.get("decision_reason") or "") == "SHADOW_HARD_STARVATION_GUARD"
        ]
        score_margins = [float(row.get("search_score") or 0.0) - float(row.get("repair_score") or 0.0) for row in evaluations]
        absolute_margins = [abs(value) for value in score_margins]

        total_new = sum(int(row.get("total_new_posts") or 0) for row in outcomes)
        matured_new = sum(int(row.get("matured_new_posts") or 0) for row in outcomes)
        censored_new = sum(int(row.get("censored_new_posts") or 0) for row in outcomes)

        thresholds = {
            "minimum_observations": evidence_policy.minimum_observations,
            "minimum_search_samples": evidence_policy.minimum_search_samples,
            "minimum_repair_samples": evidence_policy.minimum_repair_samples,
        }
        thresholds_unset = any(value is None for value in thresholds.values())

        if not evaluations:
            status = "WAITING_FOR_REAL_SHADOW_EVIDENCE"
        elif thresholds_unset:
            status = "EVIDENCE_ACCUMULATING_THRESHOLDS_UNSET"
        else:
            status = "EVIDENCE_AVAILABLE_FOR_GATE_REVIEW"

        return {
            "report_schema": REPORT_SCHEMA,
            "mode": REPORT_MODE,
            "run_id": str(run_id),
            "round_id": round_id,
            "round_status": str(round_row.get("status") or ""),
            "round_phase": str(round_row.get("phase") or ""),
            "status": status,
            "research_run": run_identity,
            "maturation_protocol": {
                "semantics": run_identity.get("maturation_semantics"),
                "long_running_semantics": run_identity.get("long_running_semantics"),
                "state_based_before_time_based": True,
                "minimum_observation_age_seconds": None,
                "post_hoc_age_threshold_forbidden": True,
            },
            "policy_identity": {
                "scheduler_policy_version": scheduler_policy.policy_version,
                "scheduler_policy_hash": sched_hash,
                "evidence_policy_version": evidence_policy.policy_version,
                "evidence_policy_hash": evid_hash,
                **identity,
            },
            "observations": {
                "decision_count": len(evaluations),
                "actual_action_counts": _counts(evaluations, "actual_action"),
                "shadow_action_counts": _counts(evaluations, "shadow_action"),
                "agreement_count": agreement_count,
                "disagreement_count": len(evaluations) - agreement_count,
                "agreement_rate": _ratio(agreement_count, len(evaluations)),
                "agreement_rate_ci95": _wilson_interval(agreement_count, len(evaluations)),
                "disagreement_matrix": _disagreement_matrix(evaluations),
                "replay_pass_count": replay_pass_count,
                "replay_fail_count": len(evaluations) - replay_pass_count,
                "replay_pass_rate": _ratio(replay_pass_count, len(evaluations)),
            },
            "maturity": {
                "outcome_row_count": len(outcomes),
                "outcome_state_counts": _counts(outcomes, "outcome_state"),
                "total_new_posts": total_new,
                "matured_new_posts": matured_new,
                "censored_new_posts": censored_new,
                "maturity_ratio": _ratio(matured_new, total_new),
                "maturity_ratio_ci95": _wilson_interval(matured_new, total_new),
            },
            "maturation_latency": {
                "SEARCH": _maturation_latency(ledger_rows, SchedulerActionType.SEARCH),
                "REPAIR": _maturation_latency(ledger_rows, SchedulerActionType.REPAIR),
            },
            "actual_outcomes": {
                "SEARCH": _action_outcome(outcomes, "SEARCH"),
                "REPAIR": _action_outcome(outcomes, "REPAIR"),
            },
            "productivity_window_stability": {
                "SEARCH": _productivity_window_series(evaluations, "search_productivity_json"),
                "REPAIR": _productivity_window_series(evaluations, "repair_productivity_json"),
                "automatic_stability_threshold": False,
                "stability_thresholds": None,
            },
            "decision_margin": {
                "search_minus_repair": _summary(score_margins),
                "absolute_margin": _summary(absolute_margins),
            },
            "availability": {
                "semantics": {
                    "raw_backlog_is_not_executable": True,
                    "execution_eligible_excludes_remote_slots": True,
                    "immediately_dispatchable_includes_remote_slots": True,
                },
                "SEARCH": {
                    "raw_backlog": _summary([float(row.get("search_backlog") or 0) for row in evaluations]),
                    "selector_eligible": _summary([float(row.get("search_selector_eligible_count") or 0) for row in evaluations]),
                    "preview_safe": _summary([float(row.get("search_preview_safe_count") or 0) for row in evaluations]),
                    "execution_eligible": _summary([float(row.get("search_execution_eligible_count") or 0) for row in evaluations]),
                    "immediately_dispatchable": _summary([float(row.get("search_immediately_dispatchable_count") or 0) for row in evaluations]),
                },
                "REPAIR": {
                    "raw_backlog": _summary([float(row.get("repair_backlog") or 0) for row in evaluations]),
                    "selector_eligible": _summary([float(row.get("repair_selector_eligible_count") or 0) for row in evaluations]),
                    "preview_safe": _summary([float(row.get("repair_preview_safe_count") or 0) for row in evaluations]),
                    "execution_eligible": _summary([float(row.get("repair_execution_eligible_count") or 0) for row in evaluations]),
                    "immediately_dispatchable": _summary([float(row.get("repair_immediately_dispatchable_count") or 0) for row in evaluations]),
                },
            },
            "fairness_and_slot_coverage": {
                "both_raw_backlogs_with_free_slot_observations": len(both_backlogs_free_slot),
                "both_execution_eligible_with_free_slot_observations": len(both_executable_free_slot),
                "hard_starvation_guard_observations": len(starvation_guard),
                "zero_free_slot_observations": len(no_slot_rows),
                "zero_free_slot_shadow_wait_count": no_slot_wait,
                "observed_zero_slot_safety_pass": (
                    None if not no_slot_rows else no_slot_wait == len(no_slot_rows)
                ),
                "runtime_obligations_are_not_expected_value_inputs": True,
            },
            "evidence_sufficiency_monitor": {
                "automatic_stop": False,
                "automatic_pause": False,
                "automatic_ready_for_calibration": False,
                "search_matured_evidence_present": _action_outcome(outcomes, "SEARCH")["matured_new_posts"] > 0,
                "repair_matured_evidence_present": _action_outcome(outcomes, "REPAIR")["matured_new_posts"] > 0,
                "disagreement_evidence_present": (len(evaluations) - agreement_count) > 0,
                "both_raw_backlog_coverage_present": len(both_backlogs_free_slot) > 0,
                "both_execution_eligible_fairness_coverage_present": len(both_executable_free_slot) > 0,
                "zero_slot_coverage_present": len(no_slot_rows) > 0,
                "quantitative_sufficiency_thresholds": None,
                "state": (
                    "WAITING_FOR_REAL_SHADOW_EVIDENCE" if not evaluations
                    else "EVIDENCE_PRESENT_CALIBRATION_REVIEW_REQUIRED"
                ),
                "manual_pause_only": True,
            },
            "activation_threshold_review": {
                "configured_thresholds": thresholds,
                "thresholds_unset": thresholds_unset,
                "automatic_threshold_recommendation": False,
                "recommended_thresholds": None,
                "reason": "REAL_D2_EVIDENCE_PRECISION_AND_EXPLICIT_RISK_TOLERANCE_REQUIRED",
                "evidence_to_review": [
                    "agreement_rate_ci95",
                    "Search/Repair matured sample balance",
                    "Search READY/Near-Pass yield precision",
                    "Repair success yield precision",
                    "censoring/maturity ratio",
                    "both-backlog fairness coverage",
                    "zero-slot safety coverage",
                    "policy identity and replay consistency",
                ],
            },
            "counterfactual_semantics": "UNEXECUTED_SHADOW_ALTERNATIVE_REMAINS_PROXY_ONLY",
            "authoritative": False,
            "activation_side_effect": False,
            "database_writes": 0,
            "threshold_mutations": 0,
        }
