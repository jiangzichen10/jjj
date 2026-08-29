"""V3.1 durable non-blocking rolling Dataset discovery.

The legacy rolling discovery helper can issue many GETs and contains nested retry
waits inside machine_lib.  Continuous mode instead persists one discovery work
item and performs at most one control-plane GET per scheduler cycle.  Dataset
metadata, DataField pages, retry state, and the final apply boundary are durable.

This module never creates Simulation POSTs and never mutates candidate/search
strategy state.  It only gathers read-only BRAIN metadata and materializes the
same DiscoveryResult that the legacy rolling-discovery algorithm would produce.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from .discovery import DiscoveryResult, discover_rolling_online, rolling_probe_dataset_ids


ACTIVE_DISCOVERY_STATES = {
    "DISCOVERY_DUE", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH", "READY_APPLY", "APPLYING"
}
WAIT_DISCOVERY_STATES = {"WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH"}
TERMINAL_DISCOVERY_STATES = {"APPLIED", "FAILED", "SUPPRESSED_EXPANSION_DISABLED"}


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


def _after(seconds: float) -> str:
    return (_now_dt() + timedelta(seconds=max(0.0, float(seconds)))).isoformat()


def _retry_after(response: Any, fallback: float) -> float:
    try:
        raw = response.headers.get("Retry-After")
    except Exception:
        raw = None
    if raw not in (None, ""):
        try:
            return max(0.5, float(raw))
        except (TypeError, ValueError):
            pass
    return max(0.5, float(fallback))


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _set_endpoint_wait(store: Any, run_id: str, *, state: str, next_retry_at: Optional[str],
                       retry_after_seconds: Optional[float], status: Optional[int],
                       error: Optional[str], reset_failures: bool = False) -> None:
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_endpoint_waits(
                   run_id,endpoint_type,wait_state,next_retry_at,retry_after_seconds,
                   consecutive_failures,last_http_status,last_error,created_at,updated_at
               ) VALUES (?, 'DISCOVERY', ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id,endpoint_type) DO UPDATE SET
                   wait_state=excluded.wait_state,
                   next_retry_at=excluded.next_retry_at,
                   retry_after_seconds=excluded.retry_after_seconds,
                   consecutive_failures=CASE WHEN ? THEN 0 ELSE ppl_endpoint_waits.consecutive_failures+1 END,
                   last_http_status=excluded.last_http_status,
                   last_error=excluded.last_error,
                   updated_at=excluded.updated_at""",
            (
                run_id, state, next_retry_at, retry_after_seconds,
                0 if reset_failures else 1, status, error, now, now, 1 if reset_failures else 0,
            ),
        )


def enqueue_discovery_refresh(store: Any, run_id: str, round_id: str, *, refresh_no: int,
                              batch_no: int, trigger: str, excluded_dataset_ids: Sequence[str],
                              probe_count: int, admit_count: int) -> Dict[str, Any]:
    """Create or reuse one durable rolling-discovery work item.

    Re-enqueue is idempotent for ``(run_id, round_id, refresh_no)``.  An already
    active work item is never duplicated, so repeated scheduler passes cannot
    create parallel metadata refreshes for the same logical refresh number.
    """
    now = _now()
    probe_count = max(0, int(probe_count))
    admit_count = max(0, min(int(admit_count), probe_count))
    with store.connect() as conn:
        before_changes = int(conn.total_changes)
        conn.execute(
            """INSERT INTO ppl_discovery_work(
                   run_id,round_id,refresh_no,batch_no,trigger,queue_state,stage,next_attempt_at,
                   probe_count,admit_count,excluded_dataset_ids_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,'DISCOVERY_DUE','DATASETS',?,?,?,?,?,?)
               ON CONFLICT(run_id,round_id,refresh_no) DO NOTHING""",
            (
                run_id, round_id, int(refresh_no), int(batch_no), str(trigger), now,
                probe_count, admit_count, _dumps(sorted({str(x) for x in excluded_dataset_ids if x is not None})),
                now, now,
            ),
        )
        created_new = int(conn.total_changes) > before_changes
        row = conn.execute(
            "SELECT * FROM ppl_discovery_work WHERE run_id=? AND round_id=? AND refresh_no=?",
            (run_id, round_id, int(refresh_no)),
        ).fetchone()
    out = dict(row) if row else {}
    out["created_new"] = bool(created_new)
    return out


def due_discovery_work(store: Any, run_id: str, *, limit: int = 1) -> List[Dict[str, Any]]:
    now = _now()
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT * FROM ppl_discovery_work
               WHERE run_id=? AND queue_state IN ('DISCOVERY_DUE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH')
                 AND (next_attempt_at IS NULL OR next_attempt_at<=?)
               ORDER BY coalesce(next_attempt_at,created_at),discovery_work_id
               LIMIT ?""",
            (run_id, now, max(1, int(limit))),
        ).fetchall()
    return [dict(x) for x in rows]


def ready_discovery_work(store: Any, run_id: str, *, limit: int = 1) -> List[Dict[str, Any]]:
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT * FROM ppl_discovery_work
               WHERE run_id=? AND queue_state IN ('READY_APPLY','APPLYING')
               ORDER BY discovery_work_id LIMIT ?""",
            (run_id, max(1, int(limit))),
        ).fetchall()
    return [dict(x) for x in rows]


def _request_url(machine: Any, config: Any, row: Mapping[str, Any]) -> str:
    settings = config.plan["simulation_settings"]
    base = str(machine.BRAIN_API_URL).rstrip("/")
    if str(row.get("stage") or "DATASETS") == "DATASETS":
        return (
            f"{base}/data-sets?instrumentType={settings['instrument_type']}"
            f"&region={settings['region']}&delay={settings['delay']}&universe={settings['universe']}"
        )
    probe_ids = [str(x) for x in _loads(row.get("probe_dataset_ids_json"), [])]
    idx = int(row.get("current_dataset_index") or 0)
    if idx >= len(probe_ids):
        raise RuntimeError("DISCOVERY_FIELD_INDEX_OUT_OF_RANGE")
    dataset_id = probe_ids[idx]
    offset = max(0, int(row.get("current_offset") or 0))
    return (
        f"{base}/data-fields?instrumentType={settings['instrument_type']}"
        f"&region={settings['region']}&delay={settings['delay']}&universe={settings['universe']}"
        f"&dataset.id={dataset_id}&limit=50&offset={offset}"
    )


def _mark_wait(store: Any, row: Mapping[str, Any], *, state: str, wait_seconds: float,
               status: Optional[int], error: str, attempt: int) -> None:
    due = _after(wait_seconds)
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_discovery_work SET queue_state=?,next_attempt_at=?,attempt_count=?,
                   last_http_status=?,last_error=?,retry_after_seconds=?,updated_at=?
               WHERE discovery_work_id=?""",
            (state, due, int(attempt), status, error, float(wait_seconds), now, row["discovery_work_id"]),
        )
    _set_endpoint_wait(
        store, str(row["run_id"]), state=state, next_retry_at=due,
        retry_after_seconds=float(wait_seconds), status=status, error=error,
    )


def _mark_failed_refresh(store: Any, row: Mapping[str, Any], *, status: Optional[int],
                         error: str, attempt: int, cooldown_seconds: float) -> str:
    """Close one deterministic refresh attempt but schedule a later refresh window.

    ``FAILED`` is terminal for this logical refresh number, so we never replay
    the same work item.  The endpoint-level cooldown gives the Continuous
    scheduler a durable wake-up time after which the orchestrator may allocate
    the next refresh number.  This avoids both tight failure loops and permanent
    pinning on a single failed refresh.
    """
    due = _after(cooldown_seconds)
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_discovery_work SET queue_state='FAILED',next_attempt_at=?,attempt_count=?,
                   last_http_status=?,last_error=?,retry_after_seconds=?,updated_at=?
               WHERE discovery_work_id=?""",
            (
                due, int(attempt), status, str(error), float(cooldown_seconds), now,
                row["discovery_work_id"],
            ),
        )
    _set_endpoint_wait(
        store, str(row["run_id"]), state="WAIT_DISCOVERY_REFRESH", next_retry_at=due,
        retry_after_seconds=float(cooldown_seconds), status=status, error=str(error),
    )
    return due


def poll_due_discovery_work(store: Any, config: Any, machine: Any, session: Any, run_id: str, *,
                            limit: int = 1, poll_interval_seconds: float = 10.0,
                            network_backoff_seconds: float = 30.0,
                            max_network_backoff_seconds: float = 300.0,
                            deterministic_failure_cooldown_seconds: float = 300.0) -> Dict[str, Any]:
    """Perform at most one GET per due discovery work item."""
    rows = due_discovery_work(store, run_id, limit=limit)
    if not rows or session is None:
        return {"polled": 0, "ready_apply_ids": [], "waits": 0, "auth_waits": 0,
                "failed": [], "skipped_dataset_ids": []}
    ready: List[int] = []
    failed: List[int] = []
    skipped_dataset_ids: List[str] = []
    waits = 0
    auth_waits = 0
    for row in rows:
        attempt = int(row.get("attempt_count") or 0) + 1
        stage = str(row.get("stage") or "DATASETS")
        try:
            url = _request_url(machine, config, row)
            response = session.get(url, timeout=60)
            status = int(getattr(response, "status_code", 0) or 0)
        except Exception as exc:
            backoff = min(
                float(max_network_backoff_seconds),
                float(network_backoff_seconds) * (2 ** min(5, max(0, attempt - 1))),
            )
            _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=backoff, status=None,
                       error=f"{type(exc).__name__}: {exc}", attempt=attempt)
            waits += 1
            continue

        if status == 429:
            wait = _retry_after(response, poll_interval_seconds)
            _mark_wait(store, row, state="WAIT_RATE_LIMIT", wait_seconds=wait, status=status,
                       error="HTTP_429", attempt=attempt)
            waits += 1
            continue
        if status in {401, 403}:
            _mark_wait(store, row, state="WAIT_AUTH", wait_seconds=network_backoff_seconds,
                       status=status, error="AUTH_ERROR", attempt=attempt)
            waits += 1
            auth_waits += 1
            continue
        if status >= 500:
            wait = min(
                float(max_network_backoff_seconds),
                float(network_backoff_seconds) * (2 ** min(5, max(0, attempt - 1))),
            )
            _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=wait, status=status,
                       error=f"HTTP_{status}", attempt=attempt)
            waits += 1
            continue
        if status in {404, 410} and stage == "FIELDS":
            # A Dataset can disappear between the /data-sets snapshot and the
            # subsequent /data-fields page fetch.  Scope that fact to the
            # Dataset: record an empty field set, advance to the next probe, and
            # keep the overall discovery refresh usable.
            probe_ids = [str(x) for x in _loads(row.get("probe_dataset_ids_json"), [])]
            idx = int(row.get("current_dataset_index") or 0)
            if idx < len(probe_ids):
                dataset_id = probe_ids[idx]
                fields_by_dataset = _loads(row.get("fields_by_dataset_json"), {})
                if not isinstance(fields_by_dataset, dict):
                    fields_by_dataset = {}
                fields_by_dataset.setdefault(dataset_id, [])
                new_idx = idx + 1
                now = _now()
                network_get_count = int(row.get("network_get_count") or 0) + 1
                if new_idx >= len(probe_ids):
                    state, next_stage, next_due = "READY_APPLY", "FINALIZE", None
                    ready.append(int(row["discovery_work_id"]))
                else:
                    state, next_stage, next_due = "DISCOVERY_DUE", "FIELDS", now
                with store.connect() as conn:
                    conn.execute(
                        """UPDATE ppl_discovery_work SET queue_state=?,stage=?,next_attempt_at=?,attempt_count=?,
                               fields_by_dataset_json=?,current_dataset_index=?,current_offset=0,network_get_count=?,
                               last_http_status=?,last_error=?,retry_after_seconds=NULL,updated_at=?
                           WHERE discovery_work_id=?""",
                        (
                            state, next_stage, next_due, attempt, _dumps(fields_by_dataset), new_idx,
                            network_get_count, status,
                            f"DATASET_FIELDS_REMOTE_MISSING:{dataset_id}:HTTP_{status}", now,
                            row["discovery_work_id"],
                        ),
                    )
                _set_endpoint_wait(store, run_id, state="READY", next_retry_at=None,
                                   retry_after_seconds=None, status=status, error=None, reset_failures=True)
                skipped_dataset_ids.append(dataset_id)
                continue

        if not (200 <= status < 300):
            _mark_failed_refresh(
                store, row, status=status, error=f"HTTP_{status}", attempt=attempt,
                cooldown_seconds=deterministic_failure_cooldown_seconds,
            )
            failed.append(int(row["discovery_work_id"]))
            continue

        try:
            payload = response.json()
        except Exception as exc:
            backoff = min(float(max_network_backoff_seconds), float(network_backoff_seconds))
            _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=backoff, status=status,
                       error=f"JSON_DECODE:{type(exc).__name__}: {exc}", attempt=attempt)
            waits += 1
            continue
        if not isinstance(payload, Mapping):
            backoff = min(float(max_network_backoff_seconds), float(network_backoff_seconds))
            _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=backoff, status=status,
                       error="INVALID_DISCOVERY_JSON", attempt=attempt)
            waits += 1
            continue

        now = _now()
        network_get_count = int(row.get("network_get_count") or 0) + 1
        if stage == "DATASETS":
            raw_datasets = payload.get("results")
            if not isinstance(raw_datasets, list):
                backoff = min(float(max_network_backoff_seconds), float(network_backoff_seconds))
                _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=backoff, status=status,
                           error="DISCOVERY_DATASETS_RESULTS_MISSING", attempt=attempt)
                waits += 1
                continue
            excluded = [str(x) for x in _loads(row.get("excluded_dataset_ids_json"), [])]
            probe_ids = rolling_probe_dataset_ids(
                raw_datasets, config, excluded_dataset_ids=excluded, probe_count=int(row.get("probe_count") or 0)
            )
            next_state = "DISCOVERY_DUE" if probe_ids else "READY_APPLY"
            next_stage = "FIELDS" if probe_ids else "FINALIZE"
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_discovery_work SET queue_state=?,stage=?,next_attempt_at=?,attempt_count=?,
                           datasets_json=?,probe_dataset_ids_json=?,fields_by_dataset_json='{}',
                           current_dataset_index=0,current_offset=0,network_get_count=?,last_http_status=?,
                           last_error=NULL,retry_after_seconds=NULL,updated_at=? WHERE discovery_work_id=?""",
                    (
                        next_state, next_stage, now if probe_ids else None, attempt,
                        _dumps(raw_datasets), _dumps(probe_ids), network_get_count, status, now,
                        row["discovery_work_id"],
                    ),
                )
            _set_endpoint_wait(store, run_id, state="READY", next_retry_at=None,
                               retry_after_seconds=None, status=status, error=None, reset_failures=True)
            if not probe_ids:
                ready.append(int(row["discovery_work_id"]))
            continue

        if stage != "FIELDS":
            # Invalid durable stage is deterministic for this refresh number.
            # Close it and let a *later* refresh number retry after cooldown.
            _mark_failed_refresh(
                store, row, status=status, error="DISCOVERY_STAGE_INVALID", attempt=attempt,
                cooldown_seconds=deterministic_failure_cooldown_seconds,
            )
            failed.append(int(row["discovery_work_id"]))
            continue

        results = payload.get("results")
        if not isinstance(results, list):
            backoff = min(float(max_network_backoff_seconds), float(network_backoff_seconds))
            _mark_wait(store, row, state="WAIT_NETWORK", wait_seconds=backoff, status=status,
                       error="DISCOVERY_FIELDS_RESULTS_MISSING", attempt=attempt)
            waits += 1
            continue
        probe_ids = [str(x) for x in _loads(row.get("probe_dataset_ids_json"), [])]
        idx = int(row.get("current_dataset_index") or 0)
        offset = max(0, int(row.get("current_offset") or 0))
        if idx >= len(probe_ids):
            with store.connect() as conn:
                conn.execute(
                    """UPDATE ppl_discovery_work SET queue_state='READY_APPLY',stage='FINALIZE',next_attempt_at=NULL,
                           attempt_count=?,network_get_count=?,last_http_status=?,last_error=NULL,
                           retry_after_seconds=NULL,updated_at=? WHERE discovery_work_id=?""",
                    (attempt, network_get_count, status, now, row["discovery_work_id"]),
                )
            ready.append(int(row["discovery_work_id"]))
            continue
        dataset_id = probe_ids[idx]
        fields_by_dataset = _loads(row.get("fields_by_dataset_json"), {})
        if not isinstance(fields_by_dataset, dict):
            fields_by_dataset = {}
        existing = list(fields_by_dataset.get(dataset_id) or [])
        existing.extend(results)
        fields_by_dataset[dataset_id] = existing
        raw_count = payload.get("count")
        try:
            count = int(raw_count) if raw_count is not None else None
        except (TypeError, ValueError):
            count = None
        more_pages = len(results) >= 50 and (count is None or offset + 50 < count)
        if more_pages:
            new_idx = idx
            new_offset = offset + 50
            state = "DISCOVERY_DUE"
            stage2 = "FIELDS"
            next_due = now
        else:
            new_idx = idx + 1
            new_offset = 0
            if new_idx >= len(probe_ids):
                state = "READY_APPLY"
                stage2 = "FINALIZE"
                next_due = None
                ready.append(int(row["discovery_work_id"]))
            else:
                state = "DISCOVERY_DUE"
                stage2 = "FIELDS"
                next_due = now
        with store.connect() as conn:
            conn.execute(
                """UPDATE ppl_discovery_work SET queue_state=?,stage=?,next_attempt_at=?,attempt_count=?,
                       fields_by_dataset_json=?,current_dataset_index=?,current_offset=?,network_get_count=?,
                       last_http_status=?,last_error=NULL,retry_after_seconds=NULL,updated_at=?
                   WHERE discovery_work_id=?""",
                (
                    state, stage2, next_due, attempt, _dumps(fields_by_dataset), new_idx, new_offset,
                    network_get_count, status, now, row["discovery_work_id"],
                ),
            )
        _set_endpoint_wait(store, run_id, state="READY", next_retry_at=None,
                           retry_after_seconds=None, status=status, error=None, reset_failures=True)

    return {
        "polled": len(rows), "ready_apply_ids": ready, "waits": waits,
        "auth_waits": auth_waits, "failed": failed,
        "skipped_dataset_ids": sorted(set(skipped_dataset_ids)),
    }


class _CachedDiscoveryMachine:
    def __init__(self, datasets: Sequence[Mapping[str, Any]], fields_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]]):
        self._datasets = [dict(x) for x in datasets]
        self._fields = {str(k): [dict(x) for x in v] for k, v in fields_by_dataset.items()}

    def get_datasets(self, _session: Any, **_kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame(self._datasets)

    def get_datafields(self, _session: Any, *, dataset_id: str = "", **_kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame(self._fields.get(str(dataset_id), []))


def materialize_discovery_result(row: Mapping[str, Any], config: Any) -> DiscoveryResult:
    """Build the legacy-equivalent DiscoveryResult entirely from durable JSON."""
    datasets = _loads(row.get("datasets_json"), [])
    fields = _loads(row.get("fields_by_dataset_json"), {})
    excluded = [str(x) for x in _loads(row.get("excluded_dataset_ids_json"), [])]
    machine = _CachedDiscoveryMachine(datasets, fields)
    return discover_rolling_online(
        None, config, machine, excluded_dataset_ids=excluded,
        probe_count=int(row.get("probe_count") or 0), admit_count=int(row.get("admit_count") or 0),
    )


def mark_discovery_applying(store: Any, discovery_work_id: int) -> None:
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_discovery_work SET queue_state='APPLYING',updated_at=? WHERE discovery_work_id=?",
            (_now(), int(discovery_work_id)),
        )


def mark_discovery_applied(store: Any, discovery_work_id: int) -> None:
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_discovery_work SET queue_state='APPLIED',stage='DONE',next_attempt_at=NULL,
                   last_error=NULL,retry_after_seconds=NULL,updated_at=? WHERE discovery_work_id=?""",
            (_now(), int(discovery_work_id)),
        )


def mark_discovery_expansion_suppressed(store: Any, discovery_work_id: int) -> None:
    """Close READY_APPLY work without representing it as an applied refresh."""
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_discovery_work
               SET queue_state='SUPPRESSED_EXPANSION_DISABLED',stage='DONE',next_attempt_at=NULL,
                   last_error='EXPANSION_DISABLED',retry_after_seconds=NULL,updated_at=?
               WHERE discovery_work_id=?""",
            (_now(), int(discovery_work_id)),
        )
