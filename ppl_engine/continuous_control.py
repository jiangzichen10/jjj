"""V3.1 unattended control-plane helpers.

This module owns recoverable WAIT coordination only.  It performs no strategy
selection and never creates Simulation POSTs.

B2 invariants:
- exactly one auth refresh attempt is performed per coordinator call;
- WAIT_AUTH remote/check work is released only after auth refresh succeeds;
- auth failure keeps work durable and schedules bounded retry;
- sleeping decisions are derived from durable due-times rather than a fixed
  unconditional loop delay whenever possible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


def _after(seconds: float) -> str:
    return (_now_dt() + timedelta(seconds=max(0.0, float(seconds)))).isoformat()


def _parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class DueSnapshot:
    due_now: bool
    next_due_at: Optional[str]
    wait_seconds: float
    remote_due: int
    check_due: int
    discovery_due: int
    endpoint_due: int
    auth_waiting: int


def _due_counts(store: Any, run_id: str) -> Dict[str, Any]:
    now = _now()
    with store.connect() as conn:
        remote_due = int(conn.execute(
            """SELECT count(*) FROM ppl_remote_work
               WHERE run_id=? AND queue_state IN (
                 'POLL_DUE','WAIT_REMOTE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH',
                 'MISSING_CONFIRMATION_PENDING'
               ) AND (next_poll_at IS NULL OR next_poll_at<=?)""",
            (run_id, now),
        ).fetchone()[0])
        check_due = int(conn.execute(
            """SELECT count(*) FROM ppl_check_work
               WHERE run_id=? AND queue_state IN (
                 'CHECK_DUE','WAIT_CHECK','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH'
               ) AND (next_check_at IS NULL OR next_check_at<=?)""",
            (run_id, now),
        ).fetchone()[0])
        discovery_due = int(conn.execute(
            """SELECT count(*) FROM ppl_discovery_work
               WHERE run_id=? AND queue_state IN ('DISCOVERY_DUE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH')
                 AND (next_attempt_at IS NULL OR next_attempt_at<=?)""",
            (run_id, now),
        ).fetchone()[0])
        endpoint_due = int(conn.execute(
            """SELECT count(*) FROM ppl_endpoint_waits
               WHERE run_id=? AND wait_state!='READY'
                 AND next_retry_at IS NOT NULL AND next_retry_at<=?""",
            (run_id, now),
        ).fetchone()[0])
        auth_waiting = int(conn.execute(
            """SELECT (
                 (SELECT count(*) FROM ppl_remote_work WHERE run_id=? AND queue_state='WAIT_AUTH') +
                 (SELECT count(*) FROM ppl_check_work WHERE run_id=? AND queue_state='WAIT_AUTH') +
                 (SELECT count(*) FROM ppl_discovery_work WHERE run_id=? AND queue_state='WAIT_AUTH')
               )""",
            (run_id, run_id, run_id),
        ).fetchone()[0])
        times = []
        for table, column, states in (
            ("ppl_remote_work", "next_poll_at", ("POLL_DUE","WAIT_REMOTE","WAIT_RATE_LIMIT","WAIT_NETWORK","WAIT_AUTH","MISSING_CONFIRMATION_PENDING")),
            ("ppl_check_work", "next_check_at", ("CHECK_DUE","WAIT_CHECK","WAIT_RATE_LIMIT","WAIT_NETWORK","WAIT_AUTH")),
            ("ppl_discovery_work", "next_attempt_at", ("DISCOVERY_DUE","WAIT_RATE_LIMIT","WAIT_NETWORK","WAIT_AUTH")),
        ):
            marks = ",".join("?" for _ in states)
            row = conn.execute(
                f"SELECT min({column}) FROM {table} WHERE run_id=? AND queue_state IN ({marks}) AND {column} IS NOT NULL",
                (run_id, *states),
            ).fetchone()
            if row and row[0]:
                times.append(str(row[0]))
        endpoint_row = conn.execute(
            """SELECT min(next_retry_at) FROM ppl_endpoint_waits
               WHERE run_id=? AND wait_state!='READY' AND next_retry_at IS NOT NULL""",
            (run_id,),
        ).fetchone()
        if endpoint_row and endpoint_row[0]:
            times.append(str(endpoint_row[0]))
    return {"remote_due": remote_due, "check_due": check_due, "discovery_due": discovery_due,
            "endpoint_due": endpoint_due, "auth_waiting": auth_waiting, "times": times}


def due_snapshot(store: Any, run_id: str, *, default_wait_seconds: float = 30.0,
                 max_wait_seconds: float = 300.0) -> DueSnapshot:
    counts = _due_counts(store, run_id)
    due_now = bool(counts["remote_due"] or counts["check_due"] or counts["discovery_due"] or counts["endpoint_due"])
    now = _now_dt()
    parsed = [x for x in (_parse_time(v) for v in counts["times"]) if x is not None]
    next_due = min(parsed) if parsed else None
    if due_now:
        wait = 0.0
    elif next_due is None:
        wait = max(0.5, float(default_wait_seconds))
    else:
        wait = max(0.0, (next_due - now).total_seconds())
        wait = min(max(0.5, wait), max(0.5, float(max_wait_seconds)))
    return DueSnapshot(
        due_now=due_now,
        next_due_at=next_due.isoformat() if next_due else None,
        wait_seconds=wait,
        remote_due=int(counts["remote_due"]),
        check_due=int(counts["check_due"]),
        discovery_due=int(counts["discovery_due"]),
        endpoint_due=int(counts["endpoint_due"]),
        auth_waiting=int(counts["auth_waiting"]),
    )


def recover_waiting_auth(store: Any, machine: Any, session: Any, run_id: str, *,
                         retry_seconds: float = 60.0) -> Dict[str, Any]:
    """Coordinate one re-authentication attempt for all durable WAIT_AUTH work.

    ``machine.ensure_session`` already serializes refreshes process-wide.  This
    coordinator adds durable queue semantics: one successful refresh releases
    all WAIT_AUTH work, while failure leaves every item fail-closed and due for
    a later retry.  No Simulation POST is performed here.
    """
    if session is None:
        return {"attempted": False, "reason": "NO_SESSION", "released": 0}
    with store.connect() as conn:
        remote = int(conn.execute(
            "SELECT count(*) FROM ppl_remote_work WHERE run_id=? AND queue_state='WAIT_AUTH'",
            (run_id,),
        ).fetchone()[0])
        checks = int(conn.execute(
            "SELECT count(*) FROM ppl_check_work WHERE run_id=? AND queue_state='WAIT_AUTH'",
            (run_id,),
        ).fetchone()[0])
        discovery = int(conn.execute(
            "SELECT count(*) FROM ppl_discovery_work WHERE run_id=? AND queue_state='WAIT_AUTH'",
            (run_id,),
        ).fetchone()[0])
    total = remote + checks + discovery
    if total <= 0:
        return {"attempted": False, "reason": "NO_WAIT_AUTH", "released": 0}
    now = _now()
    try:
        machine.ensure_session(session)
    except Exception as exc:
        due = _after(retry_seconds)
        message = f"{type(exc).__name__}: {exc}"
        with store.connect() as conn:
            conn.execute(
                """UPDATE ppl_remote_work SET next_poll_at=?,retry_after_seconds=?,last_error=?,updated_at=?
                   WHERE run_id=? AND queue_state='WAIT_AUTH'""",
                (due, float(retry_seconds), message, now, run_id),
            )
            conn.execute(
                """UPDATE ppl_check_work SET next_check_at=?,retry_after_seconds=?,last_error=?,updated_at=?
                   WHERE run_id=? AND queue_state='WAIT_AUTH'""",
                (due, float(retry_seconds), message, now, run_id),
            )
            conn.execute(
                """UPDATE ppl_discovery_work SET next_attempt_at=?,retry_after_seconds=?,last_error=?,updated_at=?
                   WHERE run_id=? AND queue_state='WAIT_AUTH'""",
                (due, float(retry_seconds), message, now, run_id),
            )
            conn.execute(
                """INSERT INTO ppl_endpoint_waits(
                       run_id,endpoint_type,wait_state,next_retry_at,retry_after_seconds,
                       consecutive_failures,last_error,created_at,updated_at
                   ) VALUES (?, 'AUTH', 'WAIT_AUTH', ?, ?, 1, ?, ?, ?)
                   ON CONFLICT(run_id,endpoint_type) DO UPDATE SET
                       wait_state='WAIT_AUTH',next_retry_at=excluded.next_retry_at,
                       retry_after_seconds=excluded.retry_after_seconds,
                       consecutive_failures=ppl_endpoint_waits.consecutive_failures+1,
                       last_error=excluded.last_error,updated_at=excluded.updated_at""",
                (run_id, due, float(retry_seconds), message, now, now),
            )
        return {"attempted": True, "success": False, "released": 0, "error": message,
                "next_retry_at": due}

    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_remote_work SET queue_state='POLL_DUE',next_poll_at=?,retry_after_seconds=NULL,
                   last_error=NULL,updated_at=? WHERE run_id=? AND queue_state='WAIT_AUTH'""",
            (now, now, run_id),
        )
        conn.execute(
            """UPDATE ppl_check_work SET queue_state='CHECK_DUE',next_check_at=?,retry_after_seconds=NULL,
                   last_error=NULL,updated_at=? WHERE run_id=? AND queue_state='WAIT_AUTH'""",
            (now, now, run_id),
        )
        conn.execute(
            """UPDATE ppl_discovery_work SET queue_state='DISCOVERY_DUE',next_attempt_at=?,retry_after_seconds=NULL,
                   last_error=NULL,updated_at=? WHERE run_id=? AND queue_state='WAIT_AUTH'""",
            (now, now, run_id),
        )
        conn.execute(
            """INSERT INTO ppl_endpoint_waits(
                   run_id,endpoint_type,wait_state,next_retry_at,retry_after_seconds,
                   consecutive_failures,last_error,created_at,updated_at
               ) VALUES (?, 'AUTH', 'READY', NULL, NULL, 0, NULL, ?, ?)
               ON CONFLICT(run_id,endpoint_type) DO UPDATE SET
                   wait_state='READY',next_retry_at=NULL,retry_after_seconds=NULL,
                   consecutive_failures=0,last_error=NULL,updated_at=excluded.updated_at""",
            (run_id, now, now),
        )
    return {"attempted": True, "success": True, "released": total,
            "remote_released": remote, "check_released": checks, "discovery_released": discovery}
