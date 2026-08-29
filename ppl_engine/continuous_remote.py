"""V3.1 non-blocking remote Simulation queue primitives.

This module is intentionally execution-infrastructure code, not strategy code.
It turns durable remote Simulation identities into a due-poll queue so a
server-side RUNNING job no longer occupies a Python worker for minutes/hours.

Safety invariants:
- a saved simulation_url is never replaced by a new POST;
- UNCERTAIN_SUBMISSION reserves capacity and is never auto-reposted;
- 404/410 requires two observations before REMOTE_NOT_FOUND is persisted;
- endpoint/network/auth failures keep the remote identity and schedule retry;
- core RunnerStore/alpha-cache write failures are not swallowed.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .audit_log import audit_event
from .simulation_adapter import production_remote_resolution_status


ACTIVE_REMOTE_STATES = {
    "POLL_DUE", "WAIT_REMOTE", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH",
    "MISSING_CONFIRMATION_PENDING", "QUARANTINED_UNCERTAIN",
}
TERMINAL_REMOTE_STATES = {"COMPLETE", "REMOTE_NOT_FOUND", "FAILED", "INVALID"}
MISSING_HTTP_STATUSES = {404, 410}


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


def _parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


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


def _read_alpha_facts(alpha_db: Path, sim_keys: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    keys = [str(x) for x in sim_keys if x]
    if not keys or not Path(alpha_db).exists():
        return {}
    uri = f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        marks = ",".join("?" for _ in keys)
        rows = conn.execute(
            f"SELECT * FROM alpha_results WHERE sim_key IN ({marks})", keys
        ).fetchall()
        return {str(r["sim_key"]): dict(r) for r in rows}
    finally:
        conn.close()


def _candidate_reference(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("result_reference_json")
    if not raw:
        return {}
    try:
        payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
        return dict(payload) if isinstance(payload, Mapping) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _queue_state_for(status: str, simulation_url: Optional[str]) -> tuple[str, int]:
    value = str(status or "").upper()
    if value in {"RUNNING", "SUBMITTED", "STALE_RUNNING"} and simulation_url:
        return "POLL_DUE", 1
    if value == "UNCERTAIN_SUBMISSION":
        # Server outcome may exist even without a URL; reserve one slot.
        return "QUARANTINED_UNCERTAIN", 1
    if value == "AUTH_ERROR":
        return "WAIT_AUTH", 0
    if value == "COMPLETE":
        return "COMPLETE", 0
    if value == "REMOTE_NOT_FOUND":
        return "REMOTE_NOT_FOUND", 0
    if value in {"ERROR", "FAILED"}:
        return "FAILED", 0
    if value == "INVALID":
        return "INVALID", 0
    return "", 0


@dataclass(frozen=True)
class RemoteSlotSnapshot:
    slot_limit: int
    reserved_slots: int
    running_or_submitted: int
    uncertain: int
    wait_auth: int

    @property
    def free_slots(self) -> int:
        return max(0, int(self.slot_limit) - int(self.reserved_slots))


def sync_remote_work_from_durable_facts(
    store: Any,
    alpha_db: Path,
    run_id: str,
    *,
    force_due_existing: bool = False,
) -> Dict[str, int]:
    """Project candidate/cache facts into the durable remote-work queue.

    Alpha cache is consulted because it is durable-first for POST identity.  A
    RunnerStore sync failure after POST must therefore still be discoverable on
    the next process startup.
    """
    candidates = [dict(x) for x in store.load_candidates(run_id)]
    by_key = {str(x.get("sim_key") or ""): x for x in candidates if x.get("sim_key")}
    facts = _read_alpha_facts(alpha_db, by_key.keys())
    now = _now()
    synced = 0
    active = 0
    uncertain = 0
    with store.connect(stage="CONTINUOUS_REMOTE_QUEUE_SYNC") as conn:
        for key, candidate in by_key.items():
            fact = dict(facts.get(key) or {})
            ref = _candidate_reference(candidate)
            status = str(
                fact.get("status") or candidate.get("simulation_status") or ref.get("status") or ""
            ).upper()
            simulation_url = str(
                fact.get("simulation_url") or ref.get("simulation_url") or ""
            ).strip() or None
            submitted_at = fact.get("submitted_at") or ref.get("submitted_at")
            queue_state, reserved = _queue_state_for(status, simulation_url)
            if not queue_state:
                continue
            existing = conn.execute(
                "SELECT next_poll_at,queue_state,missing_confirmations FROM ppl_remote_work WHERE run_id=? AND sim_key=?",
                (run_id, key),
            ).fetchone()
            if queue_state in TERMINAL_REMOTE_STATES:
                next_poll_at = None
            elif existing and str(existing[1] or "") in {
                "WAIT_REMOTE", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH",
                "MISSING_CONFIRMATION_PENDING", "QUARANTINED_UNCERTAIN",
            }:
                # Persisted backoff/missing-confirmation state survives process
                # restart. Startup reconciliation may count the slot immediately
                # but must not violate a durable Retry-After window.
                queue_state = str(existing[1])
                next_poll_at = existing[0] or now
            elif force_due_existing and queue_state == "POLL_DUE":
                next_poll_at = now
            else:
                next_poll_at = (existing[0] if existing and existing[0] else now)
            conn.execute(
                """INSERT INTO ppl_remote_work(
                       run_id,candidate_id,sim_key,simulation_url,remote_status,queue_state,
                       next_poll_at,poll_attempts,missing_confirmations,reserved_slot,
                       last_http_status,last_error,retry_after_seconds,submitted_at,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_id,sim_key) DO UPDATE SET
                       candidate_id=excluded.candidate_id,
                       simulation_url=COALESCE(excluded.simulation_url,ppl_remote_work.simulation_url),
                       remote_status=excluded.remote_status,
                       queue_state=excluded.queue_state,
                       next_poll_at=COALESCE(excluded.next_poll_at,ppl_remote_work.next_poll_at),
                       reserved_slot=excluded.reserved_slot,
                       last_http_status=COALESCE(excluded.last_http_status,ppl_remote_work.last_http_status),
                       last_error=COALESCE(excluded.last_error,ppl_remote_work.last_error),
                       retry_after_seconds=COALESCE(excluded.retry_after_seconds,ppl_remote_work.retry_after_seconds),
                       submitted_at=COALESCE(excluded.submitted_at,ppl_remote_work.submitted_at),
                       updated_at=excluded.updated_at""",
                (
                    run_id, candidate.get("candidate_id"), key, simulation_url, status, queue_state,
                    next_poll_at, 0, 0, reserved,
                    fact.get("last_http_status"), fact.get("error"), fact.get("last_retry_after"),
                    submitted_at, now, now,
                ),
            )
            synced += 1
            if reserved:
                active += 1
            if queue_state == "QUARANTINED_UNCERTAIN":
                uncertain += 1
    return {"synced": synced, "reserved": active, "uncertain": uncertain}


def remote_slot_snapshot(store: Any, run_id: str, slot_limit: int) -> RemoteSlotSnapshot:
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT queue_state,reserved_slot,COUNT(*) n
               FROM ppl_remote_work WHERE run_id=?
               GROUP BY queue_state,reserved_slot""",
            (run_id,),
        ).fetchall()
    reserved = 0
    running = 0
    uncertain = 0
    wait_auth = 0
    for row in rows:
        state = str(row[0] or "")
        count = int(row[2] or 0)
        if int(row[1] or 0):
            reserved += count
        if state in {"POLL_DUE", "WAIT_REMOTE", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "MISSING_CONFIRMATION_PENDING"}:
            running += count
        elif state == "QUARANTINED_UNCERTAIN":
            uncertain += count
        elif state == "WAIT_AUTH":
            wait_auth += count
    return RemoteSlotSnapshot(int(slot_limit), reserved, running, uncertain, wait_auth)


def due_remote_work(store: Any, run_id: str, *, limit: int = 8, now: Optional[str] = None) -> List[Dict[str, Any]]:
    due = now or _now()
    with store.connect() as conn:
        return [dict(x) for x in conn.execute(
            """SELECT * FROM ppl_remote_work
               WHERE run_id=? AND queue_state IN (
                   'POLL_DUE','WAIT_REMOTE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH','MISSING_CONFIRMATION_PENDING'
               ) AND simulation_url IS NOT NULL
                 AND (next_poll_at IS NULL OR next_poll_at<=?)
               ORDER BY COALESCE(next_poll_at,created_at),created_at
               LIMIT ?""",
            (run_id, due, max(1, int(limit))),
        )]


def _update_queue(
    store: Any, run_id: str, sim_key: str, *, queue_state: str,
    remote_status: Optional[str] = None, next_poll_at: Optional[str] = None,
    reserved_slot: Optional[int] = None, last_http_status: Optional[int] = None,
    last_error: Optional[str] = None, retry_after_seconds: Optional[float] = None,
    missing_confirmations: Optional[int] = None, increment_attempt: bool = True,
) -> None:
    fields = ["queue_state=?", "updated_at=?"]
    values: List[Any] = [queue_state, _now()]
    if remote_status is not None:
        fields.append("remote_status=?"); values.append(remote_status)
    if next_poll_at is not None or queue_state in TERMINAL_REMOTE_STATES:
        fields.append("next_poll_at=?"); values.append(next_poll_at)
    if reserved_slot is not None:
        fields.append("reserved_slot=?"); values.append(int(reserved_slot))
    if last_http_status is not None:
        fields.append("last_http_status=?"); values.append(int(last_http_status))
    if last_error is not None:
        fields.append("last_error=?"); values.append(str(last_error))
    if retry_after_seconds is not None:
        fields.append("retry_after_seconds=?"); values.append(float(retry_after_seconds))
    if missing_confirmations is not None:
        fields.append("missing_confirmations=?"); values.append(int(missing_confirmations))
    if increment_attempt:
        fields.append("poll_attempts=poll_attempts+1")
    values.extend([run_id, sim_key])
    with store.connect(stage="CONTINUOUS_REMOTE_QUEUE_WRITE") as conn:
        conn.execute(
            f"UPDATE ppl_remote_work SET {','.join(fields)} WHERE run_id=? AND sim_key=?",
            values,
        )


def _build_cache_candidate(row: Mapping[str, Any], config: Any, machine: Any) -> tuple[Dict[str, Any], Dict[str, Any]]:
    from .live_execution import _v21_candidate
    from .simulation_adapter import _effective_settings, _validate_expected_sim_key

    v21 = _v21_candidate(row, config.target_mode)
    settings = _effective_settings(v21, config.plan["simulation_settings"], machine)
    _validate_expected_sim_key(machine, v21, settings)
    clean = {k: v for k, v in v21.items() if not str(k).startswith("_v22_") and k != "_expected_sim_key"}
    return clean, settings


def _quarantine_missing_candidate(store: Any, run_id: str, candidate_id: str, sim_key: str, simulation_url: str, http_status: int) -> None:
    now = _now()
    reference = {
        "sim_key": sim_key,
        "simulation_url": simulation_url,
        "status": "REMOTE_NOT_FOUND",
        "http_status": int(http_status),
        "resume_resolution": "CONFIRMED_404_410_NO_REPOST",
        "updated_at": now,
    }
    with store.connect(stage="ROUND_STATE_TRANSITION") as conn:
        current = conn.execute(
            "SELECT lifecycle_state FROM ppl_candidates WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if not current:
            return
        old = str(current[0])
        conn.execute(
            """UPDATE ppl_candidates
               SET lifecycle_state='SIMULATION_REMOTE_MISSING',simulation_status='REMOTE_NOT_FOUND',
                   execution_action='HOLD_REMOTE_NOT_FOUND',cache_classification='CACHE_REMOTE_NOT_FOUND',
                   stop_reason='REMOTE_SIMULATION_URL_NOT_FOUND',result_reference_json=?,updated_at=?
               WHERE candidate_id=?""",
            (json.dumps(reference, ensure_ascii=False, sort_keys=True), now, candidate_id),
        )
        if old != "SIMULATION_REMOTE_MISSING":
            conn.execute(
                """INSERT INTO ppl_state_transitions(
                       run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                   ) VALUES (?,?,'CANDIDATE',?,?,?,?,?,?)""",
                (run_id, candidate_id, old, "SIMULATION_REMOTE_MISSING",
                 "Saved Simulation URL returned 404/410 twice; quarantined without re-POST",
                 "V31_CONTINUOUS_REMOTE", json.dumps(reference, ensure_ascii=False, sort_keys=True), now),
            )


def poll_due_remote_work(
    store: Any,
    config: Any,
    machine: Any,
    session: Any,
    alpha_db: Path,
    run_id: str,
    *,
    limit: int = 8,
    poll_interval_seconds: float = 5.0,
    network_backoff_seconds: float = 30.0,
    max_network_backoff_seconds: float = 300.0,
) -> Dict[str, Any]:
    """Poll due remote jobs once each; never sleep and never POST a Simulation."""
    rows = due_remote_work(store, run_id, limit=limit)
    if not rows or session is None:
        return {"polled": 0, "completed_candidate_ids": [], "remote_missing_candidate_ids": [], "waits": 0}
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    completed: List[str] = []
    completed_results: List[Dict[str, Any]] = []
    remote_missing: List[str] = []
    waits = 0
    for work in rows:
        cid = str(work.get("candidate_id") or "")
        key = str(work.get("sim_key") or "")
        url = str(work.get("simulation_url") or "")
        candidate_row = candidates.get(cid)
        if not candidate_row or not key or not url:
            _update_queue(store, run_id, key, queue_state="FAILED", reserved_slot=0,
                          last_error="REMOTE_QUEUE_IDENTITY_INCOMPLETE")
            continue
        clean, settings = _build_cache_candidate(candidate_row, config, machine)
        cache_candidate = {"candidate_id": cid, "sim_key": key, "_cache_candidate": clean}
        try:
            response = session.get(url, timeout=30)
        except Exception as exc:
            attempts = int(work.get("poll_attempts") or 0) + 1
            backoff = min(max_network_backoff_seconds, network_backoff_seconds * (2 ** min(attempts - 1, 4)))
            _update_queue(store, run_id, key, queue_state="WAIT_NETWORK", remote_status="RUNNING",
                          next_poll_at=_after(backoff), reserved_slot=1,
                          last_error=f"{type(exc).__name__}: {exc}", retry_after_seconds=backoff)
            audit_event(action="CONTINUOUS_REMOTE_WAIT", run_id=run_id, candidate_id=cid, sim_key=key,
                        wait_reason="NETWORK", retry_after_seconds=backoff, error_type=type(exc).__name__)
            waits += 1
            continue

        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code in {401, 403}:
            _update_queue(store, run_id, key, queue_state="WAIT_AUTH", remote_status="RUNNING",
                          next_poll_at=_after(30), reserved_slot=1, last_http_status=status_code,
                          last_error="REMOTE_POLL_AUTH_REQUIRED", retry_after_seconds=30)
            waits += 1
            continue
        if status_code == 429:
            delay = _retry_after(response, 60.0)
            _update_queue(store, run_id, key, queue_state="WAIT_RATE_LIMIT", remote_status="RUNNING",
                          next_poll_at=_after(delay), reserved_slot=1, last_http_status=status_code,
                          last_error="REMOTE_POLL_RATE_LIMIT", retry_after_seconds=delay)
            waits += 1
            continue
        if status_code >= 500:
            attempts = int(work.get("poll_attempts") or 0) + 1
            delay = min(max_network_backoff_seconds, network_backoff_seconds * (2 ** min(attempts - 1, 4)))
            _update_queue(store, run_id, key, queue_state="WAIT_NETWORK", remote_status="RUNNING",
                          next_poll_at=_after(delay), reserved_slot=1, last_http_status=status_code,
                          last_error=f"REMOTE_POLL_HTTP_{status_code}", retry_after_seconds=delay)
            waits += 1
            continue
        if status_code in MISSING_HTTP_STATUSES:
            confirmations = int(work.get("missing_confirmations") or 0) + 1
            if confirmations < 2:
                _update_queue(store, run_id, key, queue_state="MISSING_CONFIRMATION_PENDING",
                              remote_status="RUNNING", next_poll_at=_after(1.0), reserved_slot=1,
                              last_http_status=status_code, missing_confirmations=confirmations,
                              last_error="REMOTE_MISSING_CONFIRMATION_PENDING", retry_after_seconds=1.0)
                waits += 1
                continue
            result = {
                "status": "REMOTE_NOT_FOUND", "simulation_url": url,
                "submitted_at": work.get("submitted_at"), "last_http_status": status_code,
                "error": "CONFIRMED_404_410_NO_REPOST",
            }
            machine.cache_put(str(alpha_db), key, clean, settings, result)
            _quarantine_missing_candidate(store, run_id, cid, key, url, status_code)
            _update_queue(store, run_id, key, queue_state="REMOTE_NOT_FOUND", remote_status="REMOTE_NOT_FOUND",
                          next_poll_at=None, reserved_slot=0, last_http_status=status_code,
                          missing_confirmations=confirmations, last_error="CONFIRMED_404_410_NO_REPOST")
            remote_missing.append(cid)
            continue
        if status_code != 200:
            _update_queue(store, run_id, key, queue_state="WAIT_NETWORK", remote_status="RUNNING",
                          next_poll_at=_after(network_backoff_seconds), reserved_slot=1,
                          last_http_status=status_code, last_error=f"REMOTE_POLL_HTTP_{status_code}",
                          retry_after_seconds=network_backoff_seconds)
            waits += 1
            continue

        try:
            payload = response.json()
        except Exception as exc:
            _update_queue(store, run_id, key, queue_state="WAIT_REMOTE", remote_status="RUNNING",
                          next_poll_at=_after(poll_interval_seconds), reserved_slot=1,
                          last_http_status=status_code, last_error=f"INVALID_JSON:{type(exc).__name__}",
                          retry_after_seconds=poll_interval_seconds, missing_confirmations=0)
            waits += 1
            continue
        if not isinstance(payload, Mapping):
            _update_queue(store, run_id, key, queue_state="WAIT_REMOTE", remote_status="RUNNING",
                          next_poll_at=_after(poll_interval_seconds), reserved_slot=1,
                          last_http_status=status_code, last_error="INVALID_REMOTE_PAYLOAD",
                          retry_after_seconds=poll_interval_seconds, missing_confirmations=0)
            waits += 1
            continue

        classification = production_remote_resolution_status(payload)
        if classification == "RUNNING":
            delay = _retry_after(response, poll_interval_seconds)
            result = {
                "alpha_id": payload.get("alpha"), "status": "RUNNING", "simulation_url": url,
                "submitted_at": work.get("submitted_at"), "last_http_status": status_code,
                "last_retry_after": delay, "error": None,
            }
            machine.cache_put(str(alpha_db), key, clean, settings, result)
            from .live_execution import _sync_candidate_fact
            fact = machine.cache_get(str(alpha_db), key) or result; fact["sim_key"] = key
            _sync_candidate_fact(store, cid, fact, source="V31_POLL_QUEUE")
            _update_queue(store, run_id, key, queue_state="WAIT_REMOTE", remote_status="RUNNING",
                          next_poll_at=_after(delay), reserved_slot=1, last_http_status=status_code,
                          last_error="", retry_after_seconds=delay, missing_confirmations=0)
            waits += 1
            continue
        if classification == "RESULT_READY":
            alpha_id = str(payload.get("alpha") or "")
            if not alpha_id:
                _update_queue(store, run_id, key, queue_state="WAIT_REMOTE", remote_status="RUNNING",
                              next_poll_at=_after(poll_interval_seconds), reserved_slot=1,
                              last_http_status=status_code, last_error="RESULT_READY_WITHOUT_ALPHA",
                              retry_after_seconds=poll_interval_seconds, missing_confirmations=0)
                waits += 1
                continue
            try:
                result = machine.fetch_alpha_result(session, alpha_id)
            except Exception as exc:
                _update_queue(store, run_id, key, queue_state="WAIT_NETWORK", remote_status="RUNNING",
                              next_poll_at=_after(network_backoff_seconds), reserved_slot=1,
                              last_http_status=status_code,
                              last_error=f"METRICS_FETCH:{type(exc).__name__}: {exc}",
                              retry_after_seconds=network_backoff_seconds, missing_confirmations=0)
                waits += 1
                continue
            result.update({"status": "COMPLETE", "simulation_url": url, "submitted_at": work.get("submitted_at"), "error": None})
            machine.cache_put(str(alpha_db), key, clean, settings, result)
            from .live_execution import _sync_candidate_fact
            fact = machine.cache_get(str(alpha_db), key) or result; fact["sim_key"] = key
            _sync_candidate_fact(store, cid, fact, source="V31_POLL_QUEUE")
            _update_queue(store, run_id, key, queue_state="COMPLETE", remote_status="COMPLETE",
                          next_poll_at=None, reserved_slot=0, last_http_status=status_code,
                          last_error="", missing_confirmations=0)
            completed.append(cid)
            completed_results.append({
                "candidate_id": cid, "alpha_id": alpha_id,
                "sharpe": result.get("sharpe"), "fitness": result.get("fitness"),
                "turnover": result.get("turnover"),
            })
            audit_event(action="CONTINUOUS_REMOTE_COMPLETE", run_id=run_id, candidate_id=cid,
                        sim_key=key, alpha_id=alpha_id, simulation_url=url)
            continue
        if classification == "TERMINAL_FAILURE":
            result = {
                "alpha_id": payload.get("alpha"), "status": "ERROR", "simulation_url": url,
                "submitted_at": work.get("submitted_at"), "last_http_status": status_code,
                "error": json.dumps(dict(payload), ensure_ascii=False)[:2000],
            }
            machine.cache_put(str(alpha_db), key, clean, settings, result)
            from .live_execution import _sync_candidate_fact
            fact = machine.cache_get(str(alpha_db), key) or result; fact["sim_key"] = key
            _sync_candidate_fact(store, cid, fact, source="V31_POLL_QUEUE")
            _update_queue(store, run_id, key, queue_state="FAILED", remote_status="ERROR",
                          next_poll_at=None, reserved_slot=0, last_http_status=status_code,
                          last_error=result["error"], missing_confirmations=0)
            continue

        _update_queue(store, run_id, key, queue_state="WAIT_REMOTE", remote_status="RUNNING",
                      next_poll_at=_after(poll_interval_seconds), reserved_slot=1,
                      last_http_status=status_code, last_error="UNKNOWN_REMOTE_STATUS",
                      retry_after_seconds=poll_interval_seconds, missing_confirmations=0)
        waits += 1

    return {
        "polled": len(rows), "completed_candidate_ids": completed,
        "completed_results": completed_results,
        "remote_missing_candidate_ids": remote_missing, "waits": waits,
    }


def register_submitted_remote(
    store: Any, run_id: str, candidate_id: str, sim_key: str,
    simulation_url: str, submitted_at: Optional[str], *, initial_retry_after: float = 0.0,
) -> None:
    now = _now()
    next_poll = _after(max(0.5, initial_retry_after or 0.5))
    with store.connect(stage="CONTINUOUS_REMOTE_QUEUE_WRITE") as conn:
        conn.execute(
            """INSERT INTO ppl_remote_work(
                   run_id,candidate_id,sim_key,simulation_url,remote_status,queue_state,next_poll_at,
                   poll_attempts,missing_confirmations,reserved_slot,retry_after_seconds,submitted_at,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,sim_key) DO UPDATE SET
                   candidate_id=excluded.candidate_id,simulation_url=excluded.simulation_url,
                   remote_status='SUBMITTED',queue_state='WAIT_REMOTE',next_poll_at=excluded.next_poll_at,
                   reserved_slot=1,retry_after_seconds=excluded.retry_after_seconds,
                   submitted_at=COALESCE(excluded.submitted_at,ppl_remote_work.submitted_at),updated_at=excluded.updated_at""",
            (run_id,candidate_id,sim_key,simulation_url,"SUBMITTED","WAIT_REMOTE",next_poll,
             0,0,1,float(initial_retry_after or 0.5),submitted_at,now,now),
        )
