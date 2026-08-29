"""V3.1 durable non-blocking PRE_TAG Check queue.

Each due item performs at most one GET per scheduler cycle.  Pending, 429,
network/5xx and auth outcomes are requeued with durable due-times; a single
candidate never sleeps inside a long semantic-poll loop.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .audit_log import audit_event
from .check_parser import parse_response_text
from .state_machine import CANDIDATE_TRANSITIONS


ACTIVE_CHECK_STATES = {"CHECK_DUE", "WAIT_CHECK", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH"}
TERMINAL_CHECK_STATES = {"RESOLVED", "FAILED"}


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


def _after(seconds: float) -> str:
    return (_now_dt() + timedelta(seconds=max(0.0, float(seconds)))).isoformat()


def _retry_after(response: Any, default_seconds: float) -> float:
    try:
        raw = response.headers.get("Retry-After")
    except Exception:
        raw = None
    if raw not in (None, ""):
        try:
            return max(0.5, float(raw))
        except (TypeError, ValueError):
            pass
    return max(0.5, float(default_seconds))


def enqueue_pretag_checks(store: Any, run_id: str, candidate_ids: Iterable[str], *,
                          source: str = "V31_CONTINUOUS_CHECK") -> Dict[str, Any]:
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    now = _now(); queued: List[str] = []; skipped: List[Dict[str, str]] = []
    for cid in [str(x) for x in candidate_ids if x]:
        row = candidates.get(cid)
        if not row:
            skipped.append({"candidate_id": cid, "reason": "CANDIDATE_NOT_FOUND"}); continue
        alpha_id = str(row.get("alpha_id") or "")
        if not alpha_id:
            skipped.append({"candidate_id": cid, "reason": "ALPHA_ID_MISSING"}); continue
        state = str(row.get("lifecycle_state") or "")
        if state == "LOCAL_PRE_GATE_PASS":
            store.transition_candidate(cid, "PRE_TAG_CHECK_PENDING", reason="Queued GET-only V3.1 PRE_TAG check",
                                       source=source, allowed=CANDIDATE_TRANSITIONS)
        elif state not in {"PRE_TAG_CHECK_PENDING", "PRE_TAG_CHECK_COMPLETE", "PRE_TAG_CHECK_PASS"}:
            skipped.append({"candidate_id": cid, "reason": f"STATE_NOT_CHECKABLE:{state}"}); continue
        if state in {"PRE_TAG_CHECK_COMPLETE", "PRE_TAG_CHECK_PASS"}:
            skipped.append({"candidate_id": cid, "reason": f"ALREADY_TERMINAL:{state}"}); continue
        with store.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_check_work(
                       run_id,candidate_id,alpha_id,phase,queue_state,next_check_at,
                       attempt_count,created_at,updated_at
                   ) VALUES (?,?,?,'PRE_TAG','CHECK_DUE',?,0,?,?)
                   ON CONFLICT(run_id,candidate_id,alpha_id,phase) DO UPDATE SET
                       queue_state=CASE WHEN ppl_check_work.queue_state IN ('RESOLVED','FAILED')
                                        THEN ppl_check_work.queue_state ELSE 'CHECK_DUE' END,
                       next_check_at=CASE WHEN ppl_check_work.queue_state IN ('RESOLVED','FAILED')
                                          THEN ppl_check_work.next_check_at ELSE excluded.next_check_at END,
                       updated_at=excluded.updated_at""",
                (run_id, cid, alpha_id, now, now, now),
            )
        queued.append(cid)
    return {"queued": queued, "queued_count": len(queued), "skipped": skipped}




def enqueue_manual_refresh_checks(store: Any, run_id: str, candidate_ids: Iterable[str], *,
                                  exclude_candidate_ids: Iterable[str] = (),
                                  source: str = "V31_MANUAL_REFRESH_CHECK") -> Dict[str, Any]:
    """Queue fresh GET-only rechecks for manual-finalization candidates.

    Unlike PRE_TAG work, a prior terminal RECHECK is deliberately re-opened:
    periodic manual-finalization refresh is about freshness, not first-time
    workflow advancement.  The candidate lifecycle is never changed here.
    """
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    excluded = {str(x) for x in exclude_candidate_ids if x}
    now = _now(); queued: List[str] = []; skipped: List[Dict[str, str]] = []; seen_alpha = set()
    for cid in [str(x) for x in candidate_ids if x]:
        if cid in excluded:
            skipped.append({"candidate_id": cid, "reason": "ALREADY_FRESH_THIS_BATCH"}); continue
        row = candidates.get(cid)
        if not row:
            skipped.append({"candidate_id": cid, "reason": "CANDIDATE_NOT_FOUND"}); continue
        alpha_id = str(row.get("alpha_id") or "")
        if not alpha_id:
            skipped.append({"candidate_id": cid, "reason": "ALPHA_ID_MISSING"}); continue
        if alpha_id in seen_alpha:
            skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "DUPLICATE_ALPHA_ID"}); continue
        seen_alpha.add(alpha_id)
        with store.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_check_work(
                       run_id,candidate_id,alpha_id,phase,queue_state,next_check_at,
                       attempt_count,created_at,updated_at
                   ) VALUES (?,?,?,'RECHECK','CHECK_DUE',?,0,?,?)
                   ON CONFLICT(run_id,candidate_id,alpha_id,phase) DO UPDATE SET
                       queue_state='CHECK_DUE',next_check_at=excluded.next_check_at,
                       attempt_count=0,last_http_status=NULL,last_error=NULL,retry_after_seconds=NULL,
                       updated_at=excluded.updated_at""",
                (run_id, cid, alpha_id, now, now, now),
            )
        queued.append(cid)
    return {"queued": queued, "queued_count": len(queued), "skipped": skipped, "source": source}

def due_check_work(store: Any, run_id: str, *, limit: int = 4) -> List[Dict[str, Any]]:
    now = _now()
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT * FROM ppl_check_work
               WHERE run_id=? AND queue_state IN ('CHECK_DUE','WAIT_CHECK','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH')
                 AND (next_check_at IS NULL OR next_check_at<=?)
               ORDER BY coalesce(next_check_at,created_at),check_work_id LIMIT ?""",
            (run_id, now, max(1, int(limit))),
        ).fetchall()
    return [dict(x) for x in rows]


def _parsed_error(status: int) -> Dict[str, Any]:
    if status == 429:
        err = ("HTTP_429", "TRANSIENT")
    elif status in {401,403}:
        err = ("AUTH_ERROR", "TRANSIENT")
    elif status >= 500:
        err = ("HTTP_5XX", "TRANSIENT")
    else:
        err = ("HTTP_4XX", "DETERMINISTIC")
    return {
        "session_semantic_status": "TRANSIENT_ERROR" if err[1] == "TRANSIENT" else "ERROR",
        "parse_status": err[0], "results": [], "pending_check_names": [],
        "base_gate": {"status": "PENDING"}, "theme_gate": {"status": "PENDING"},
        "error_type": err[0], "error_nature": err[1],
    }


def _save_one_poll(store: Any, *, run_id: str, candidate_id: str, alpha_id: str,
                   phase: str, status_code: int, text: str, parsed: Mapping[str, Any],
                   evidence_source: str) -> str:
    session_id = "chk_" + uuid.uuid4().hex
    semantic = str(parsed.get("session_semantic_status") or "")
    session_status = "RESOLVED" if semantic == "RESOLVED" else "TRANSIENT_ERROR" if semantic == "TRANSIENT_ERROR" else "PENDING"
    now = _now()
    session = {
        "check_session_id": session_id, "run_id": run_id, "candidate_id": candidate_id,
        "alpha_id": alpha_id, "phase": phase, "session_status": session_status,
        "started_at": now, "resolved_at": now if session_status == "RESOLVED" else None,
        "poll_count": 1, "http_request_count": 1, "pending_poll_requests": 0,
        "base_gate_result": (parsed.get("base_gate") or {}).get("status"),
        "theme_gate_result": (parsed.get("theme_gate") or {}).get("status"),
        "error_type": parsed.get("error_type"), "error_nature": parsed.get("error_nature"),
        "polls": [{"semantic_poll_index": 1, "http_request_count_delta": 1,
                   "http_status": int(status_code), "raw_response_text": text,
                   "parsed": dict(parsed), "created_at": now}],
    }
    store.save_check_session(session, evidence_source=evidence_source)
    return session_id


def poll_due_checks(store: Any, config: Any, machine: Any, session: Any, run_id: str, *,
                    limit: int = 4, poll_interval_seconds: float = 10.0,
                    network_backoff_seconds: float = 30.0,
                    max_network_backoff_seconds: float = 300.0,
                    evidence_source: str = "V31_CONTINUOUS_CHECK") -> Dict[str, Any]:
    rows = due_check_work(store, run_id, limit=limit)
    if not rows or session is None:
        return {"polled": 0, "resolved_candidate_ids": [], "resolved_recheck_candidate_ids": [],
                "waits": 0, "auth_waits": 0, "failed": []}
    resolved: List[str] = []; resolved_rechecks: List[str] = []; failed: List[str] = []; waits = 0; auth_waits = 0
    now = _now()
    for row in rows:
        cid = str(row["candidate_id"]); alpha_id = str(row["alpha_id"]); phase = str(row["phase"])
        url = f"{machine.BRAIN_API_URL}/alphas/{alpha_id}/check"
        attempt = int(row.get("attempt_count") or 0) + 1
        try:
            response = session.get(url, timeout=60)
            status = int(getattr(response, "status_code", 0) or 0)
            text = str(getattr(response, "text", "") or "")
        except Exception as exc:
            backoff = min(float(max_network_backoff_seconds), float(network_backoff_seconds) * (2 ** min(5, max(0, attempt-1))))
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='WAIT_NETWORK',next_check_at=?,attempt_count=?,
                           last_error=?,retry_after_seconds=?,updated_at=? WHERE check_work_id=?""",
                    (_after(backoff), attempt, f"{type(exc).__name__}: {exc}", backoff, now, row["check_work_id"]),
                )
            waits += 1; continue
        if status == 429:
            wait = _retry_after(response, poll_interval_seconds)
            parsed = _parsed_error(status)
            _save_one_poll(store, run_id=run_id, candidate_id=cid, alpha_id=alpha_id, phase=phase,
                           status_code=status, text=text, parsed=parsed, evidence_source=evidence_source)
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='WAIT_RATE_LIMIT',next_check_at=?,attempt_count=?,
                           last_http_status=?,last_error='HTTP_429',retry_after_seconds=?,updated_at=? WHERE check_work_id=?""",
                    (_after(wait), attempt, status, wait, now, row["check_work_id"]),
                )
            waits += 1; continue
        if status in {401,403}:
            parsed = _parsed_error(status)
            _save_one_poll(store, run_id=run_id, candidate_id=cid, alpha_id=alpha_id, phase=phase,
                           status_code=status, text=text, parsed=parsed, evidence_source=evidence_source)
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='WAIT_AUTH',next_check_at=?,attempt_count=?,
                           last_http_status=?,last_error='AUTH_ERROR',retry_after_seconds=?,updated_at=? WHERE check_work_id=?""",
                    (_after(network_backoff_seconds), attempt, status, float(network_backoff_seconds), now, row["check_work_id"]),
                )
            waits += 1; auth_waits += 1; continue
        if status >= 500:
            wait = min(float(max_network_backoff_seconds), float(network_backoff_seconds) * (2 ** min(5, max(0, attempt-1))))
            parsed = _parsed_error(status)
            _save_one_poll(store, run_id=run_id, candidate_id=cid, alpha_id=alpha_id, phase=phase,
                           status_code=status, text=text, parsed=parsed, evidence_source=evidence_source)
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='WAIT_NETWORK',next_check_at=?,attempt_count=?,
                           last_http_status=?,last_error=?,retry_after_seconds=?,updated_at=? WHERE check_work_id=?""",
                    (_after(wait), attempt, status, f"HTTP_{status}", wait, now, row["check_work_id"]),
                )
            waits += 1; continue
        if not (200 <= status < 300):
            parsed = _parsed_error(status)
            _save_one_poll(store, run_id=run_id, candidate_id=cid, alpha_id=alpha_id, phase=phase,
                           status_code=status, text=text, parsed=parsed, evidence_source=evidence_source)
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='FAILED',next_check_at=NULL,attempt_count=?,
                           last_http_status=?,last_error=?,updated_at=? WHERE check_work_id=?""",
                    (attempt, status, f"HTTP_{status}", now, row["check_work_id"]),
                )
            failed.append(cid); continue
        parsed = parse_response_text(text, phase=phase, rules=config.rules, evidence_source=evidence_source)
        _save_one_poll(store, run_id=run_id, candidate_id=cid, alpha_id=alpha_id, phase=phase,
                       status_code=status, text=text, parsed=parsed, evidence_source=evidence_source)
        semantic = str(parsed.get("session_semantic_status") or "")
        if semantic == "RESOLVED":
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='RESOLVED',next_check_at=NULL,attempt_count=?,
                           last_http_status=?,last_error=NULL,retry_after_seconds=NULL,updated_at=? WHERE check_work_id=?""",
                    (attempt, status, now, row["check_work_id"]),
                )
            if phase == "PRE_TAG":
                cand = next((x for x in store.load_candidates(run_id) if str(x.get("candidate_id")) == cid), None)
                if cand and str(cand.get("lifecycle_state")) == "PRE_TAG_CHECK_PENDING":
                    store.transition_candidate(cid, "PRE_TAG_CHECK_COMPLETE", reason="V3.1 PRE_TAG check resolved",
                                               source="V31_CONTINUOUS_CHECK", allowed=CANDIDATE_TRANSITIONS)
                    base = (parsed.get("base_gate") or {}).get("status")
                    theme = (parsed.get("theme_gate") or {}).get("status")
                    if base == "PASS" and theme == "PASS":
                        store.transition_candidate(cid, "PRE_TAG_CHECK_PASS", reason="Resolved live PRE_TAG gates passed",
                                                   source="V31_CONTINUOUS_CHECK", allowed=CANDIDATE_TRANSITIONS)
                audit_event(action="PRETAG_CHECK_COMPLETE", run_id=run_id, candidate_id=cid, alpha_id=alpha_id,
                            source="V31_CONTINUOUS_CHECK", session_status="RESOLVED")
            else:
                resolved_rechecks.append(cid)
                audit_event(action="MANUAL_FINALIZATION_RECHECK_COMPLETE", run_id=run_id, candidate_id=cid,
                            alpha_id=alpha_id, source="V31_CONTINUOUS_CHECK", session_status="RESOLVED")
            resolved.append(cid)
        else:
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_check_work SET queue_state='WAIT_CHECK',next_check_at=?,attempt_count=?,
                           last_http_status=?,last_error=NULL,retry_after_seconds=?,updated_at=? WHERE check_work_id=?""",
                    (_after(poll_interval_seconds), attempt, status, float(poll_interval_seconds), now, row["check_work_id"]),
                )
            waits += 1
    return {"polled": len(rows), "resolved_candidate_ids": resolved,
            "resolved_recheck_candidate_ids": resolved_rechecks, "waits": waits,
            "auth_waits": auth_waits, "failed": failed}
