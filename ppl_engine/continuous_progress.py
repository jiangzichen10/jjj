"""Best-effort Continuous console progress from existing durable/runtime facts."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import sys
from typing import Any, Mapping, Optional, TextIO

from .continuous_policy import parse_continuous_policy
from .scheduler_shadow import ResearchAvailabilityFacts


ACTIVE_REMOTE_STATES = {
    "POLL_DUE", "WAIT_REMOTE", "WAIT_NETWORK", "WAIT_RATE_LIMIT", "WAIT_AUTH",
    "MISSING_CONFIRMATION_PENDING", "QUARANTINED_UNCERTAIN",
}
TERMINAL_CHECK_STATES = {"RESOLVED", "COMPLETE", "FAILED", "TERMINAL"}


def _rows(conn: Any, table: str, where: str = "", params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
    ).fetchone()
    if not exists:
        return []
    sql = f"SELECT * FROM {table}" + (f" WHERE {where}" if where else "")
    return [dict(row) for row in conn.execute(sql, params)]


def build_continuous_progress_snapshot(
    store: Any, run_id: str, round_id: str, policy: Mapping[str, Any], *,
    search_availability: Optional[ResearchAvailabilityFacts] = None,
    repair_availability: Optional[ResearchAvailabilityFacts] = None,
    checking_count: int = 0,
    remote_slot_limit: Optional[int] = None,
) -> dict[str, Any]:
    """Read existing facts only; no schema creation, network request, or state write."""
    with store.connect() as conn:
        runs = _rows(conn, "ppl_runs", "run_id=?", (run_id,))
        rounds = _rows(conn, "ppl_rounds", "round_id=?", (round_id,))
        candidates = _rows(conn, "ppl_candidates", "run_id=?", (run_id,))
        remote = _rows(conn, "ppl_remote_work", "run_id=?", (run_id,))
        checks = _rows(conn, "ppl_check_work", "run_id=?", (run_id,))
        waits = _rows(conn, "ppl_endpoint_waits", "run_id=?", (run_id,))
        outcomes = _rows(conn, "ppl_round_scheduler_outcomes", "round_id=?", (round_id,))
        plans = _rows(conn, "ppl_repair_plans", "run_id=?", (run_id,))
    run = runs[0] if runs else {}
    round_row = rounds[0] if rounds else {}
    remote_status = Counter(str(row.get("remote_status") or row.get("queue_state") or "UNKNOWN").upper() for row in remote)
    remote_queue = Counter(str(row.get("queue_state") or "UNKNOWN").upper() for row in remote)
    check_queue = Counter(str(row.get("queue_state") or "UNKNOWN").upper() for row in checks)
    endpoint_waits = Counter(str(row.get("wait_state") or "UNKNOWN").upper() for row in waits)
    active_remote = sum(count for state, count in remote_queue.items() if state in ACTIVE_REMOTE_STATES)
    planned_search = sum(
        not row.get("parent_candidate_id")
        and str(row.get("lifecycle_state") or "").upper() == "PLANNED"
        and str(row.get("simulation_status") or "NONE").upper() in {"", "NONE"}
        for row in candidates
    )
    repair_raw = sum(
        str(row.get("plan_status") or "").upper() in {"PLANNED", "READY", "PREVIEWED", "APPROVED", "AUTHORIZED"}
        and not str(row.get("blocked_reason") or "").strip()
        for row in plans
    )
    attempted = int(run.get("post_attempted") or 0)
    confirmed = int(run.get("post_confirmed") or 0)
    uncertain = int(run.get("post_uncertain") or 0)
    failed_or_rejected = max(0, attempted - confirmed - uncertain)
    matured = Counter()
    for row in outcomes:
        matured[str(row.get("actual_action") or "UNKNOWN").upper()] += int(row.get("matured_new_posts") or 0)
    continuous = parse_continuous_policy(policy)
    search = search_availability.as_dict() if search_availability is not None else {
        "raw_backlog_count": int(planned_search), "selector_eligible_count": None,
        "preview_safe_count": None, "execution_eligible_count": None,
        "immediately_dispatchable_count": None, "evaluation_complete": False,
        "reason": "NOT_EVALUATED_AT_THIS_CHECKPOINT",
    }
    repair = repair_availability.as_dict() if repair_availability is not None else {
        "raw_backlog_count": int(repair_raw), "selector_eligible_count": None,
        "preview_safe_count": None, "execution_eligible_count": None,
        "immediately_dispatchable_count": None, "evaluation_complete": False,
        "reason": "NOT_EVALUATED_AT_THIS_CHECKPOINT",
    }
    return {
        "post": {"confirmed": confirmed, "uncertain": uncertain, "failed_or_rejected": failed_or_rejected,
                 "active": active_remote, "slot_limit": int(remote_slot_limit) if remote_slot_limit is not None else None},
        "alpha": {"submitted": remote_status["SUBMITTED"], "running": remote_status["RUNNING"],
                  "complete": remote_status["COMPLETE"], "remote_not_found": remote_status["REMOTE_NOT_FOUND"],
                  "pending": active_remote},
        "check": {"queued_pending": sum(count for state, count in check_queue.items() if state not in TERMINAL_CHECK_STATES),
                  "checking": max(0, int(checking_count)),
                  "resolved": sum(count for state, count in check_queue.items() if state in TERMINAL_CHECK_STATES),
                  "wait_check": check_queue["WAIT_CHECK"],
                  "wait_rate_limit_transient": check_queue["WAIT_RATE_LIMIT"] + check_queue["WAIT_NETWORK"] + check_queue["WAIT_AUTH"],
                  "endpoint_waits": dict(endpoint_waits)},
        "research": {"current_batch": int(round_row.get("current_batch") or 0),
                     "actual_phase": str(round_row.get("phase") or "UNKNOWN"),
                     "search_consumed": int(round_row.get("search_consumed") or 0),
                     "repair_consumed": int(round_row.get("repair_consumed") or 0),
                     "search_planned_backlog": int(planned_search),
                     "allow_search_pool_expansion": bool(continuous.allow_search_pool_expansion)},
        "search_availability": search,
        "repair_availability": repair,
        "d2_matured": {"search": matured["SEARCH"], "repair": matured["REPAIR"]},
    }


def _display(value: Any) -> str:
    return "n/a" if value is None else str(value)


def format_continuous_progress(snapshot: Mapping[str, Any]) -> str:
    post = snapshot["post"]; alpha = snapshot["alpha"]; check = snapshot["check"]
    research = snapshot["research"]; search = snapshot["search_availability"]
    repair = snapshot["repair_availability"]; matured = snapshot["d2_matured"]
    return (
        "CONTINUOUS | "
        f"POST confirmed={post['confirmed']} uncertain={post['uncertain']} failed/rejected={post['failed_or_rejected']} "
        f"active/slot={post['active']}/{_display(post['slot_limit'])} | "
        f"Alpha submitted={alpha['submitted']} running={alpha['running']} complete={alpha['complete']} "
        f"remote_not_found={alpha['remote_not_found']} pending={alpha['pending']} | "
        f"Check queued/pending={check['queued_pending']} checking={check['checking']} resolved={check['resolved']} "
        f"wait_check={check['wait_check']} wait_transient={check['wait_rate_limit_transient']} | "
        f"Research batch={research['current_batch']} phase={research['actual_phase']} "
        f"search_consumed={research['search_consumed']} repair_consumed={research['repair_consumed']} "
        f"Search PLANNED backlog={research['search_planned_backlog']} expansion={research['allow_search_pool_expansion']} | "
        f"Search selector eligible={_display(search['selector_eligible_count'])} "
        f"Search execution eligible={_display(search['execution_eligible_count'])} | "
        f"Repair raw backlog={repair['raw_backlog_count']} selector eligible={_display(repair['selector_eligible_count'])} "
        f"preview safe={_display(repair['preview_safe_count'])} execution eligible={_display(repair['execution_eligible_count'])} "
        f"immediately dispatchable={_display(repair['immediately_dispatchable_count'])} | "
        f"D2 matured search={matured['search']} repair={matured['repair']}"
    )


class ContinuousProgressRenderer:
    def __init__(self, stream: Optional[TextIO] = None) -> None:
        self.stream = stream or sys.stdout
        self._last_fingerprint: Optional[str] = None

    def emit(self, snapshot: Mapping[str, Any]) -> bool:
        """Print only changed state; UI errors never escape into execution."""
        try:
            raw = json.dumps(dict(snapshot), ensure_ascii=False, sort_keys=True, default=str)
            fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            if fingerprint == self._last_fingerprint:
                return False
            print(format_continuous_progress(snapshot), file=self.stream, flush=True)
            self._last_fingerprint = fingerprint
            return True
        except Exception:
            return False

    def emit_completed_results(self, rows: list[Mapping[str, Any]]) -> int:
        emitted = 0
        for row in rows:
            try:
                print(
                    "ALPHA COMPLETE | "
                    f"candidate_id={row.get('candidate_id')} alpha_id={row.get('alpha_id')} "
                    f"Sharpe={_display(row.get('sharpe'))} Fitness={_display(row.get('fitness'))} "
                    f"Turnover={_display(row.get('turnover'))}",
                    file=self.stream, flush=True,
                )
                emitted += 1
            except Exception:
                return emitted
        return emitted
