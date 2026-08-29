"""Safety wrapper that delegates all future simulation work to V2.1."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .audit_log import audit_event
from .config import ConfigError
from .settings_contract import (
    FULL_SIMULATION_SETTING_KEYS,
    full_settings_identity,
    validate_full_simulation_settings,
)
from .remote_simulation import (
    durable_running_age_seconds, remote_resolution_audit_payload,
    resolve_remote_simulation,
)


POST_ACTIONS = {"NEW_SIMULATION_REQUIRED", "RETRY_PER_V21_POLICY"}


def production_remote_resolution_status(payload: Mapping[str, Any]) -> str:
    """Exact current wait/worker status policy; no aliases or new terminals."""
    status = str(payload.get("status") or "").upper()
    alpha_id = payload.get("alpha")
    if status in {"COMPLETE", "WARNING"}:
        return "RESULT_READY" if alpha_id else "UNKNOWN"
    if alpha_id and status not in {"PENDING", "RUNNING", "QUEUED"}:
        return "RESULT_READY"
    if status in {"FAIL", "FAILED", "ERROR"}:
        return "TERMINAL_FAILURE"
    if status in {"PENDING", "RUNNING", "QUEUED"}:
        return "RUNNING"
    if not status and not alpha_id and "progress" in payload:
        progress = payload["progress"]
        # JSON numbers become int/float. bool is deliberately excluded even
        # though it subclasses int; NaN/inf and out-of-range values are not
        # credible BRAIN progress evidence and remain fail-closed.
        if (
            isinstance(progress, (int, float))
            and not isinstance(progress, bool)
            and math.isfinite(float(progress))
            and 0.0 <= float(progress) <= 1.0
        ):
            return "RUNNING"
    return "UNKNOWN"


# Remote simulation status normalization.
# BRAIN status names may vary between endpoints/versions.  Keep the adapter
# boundary stable so downstream V3 logic only handles canonical states.
_REMOTE_STATUS_ALIASES = {
    "COMPLETE": "COMPLETE",
    "COMPLETED": "COMPLETE",
    "DONE": "COMPLETE",
    "SUCCESS": "COMPLETE",
    "FINISHED": "COMPLETE",
    "RUNNING": "RUNNING",
    "SUBMITTED": "RUNNING",
    "PENDING": "RUNNING",
    "QUEUED": "RUNNING",
    "WAITING": "RUNNING",
    "PROCESSING": "RUNNING",
    "IN_PROGRESS": "RUNNING",
    "FAILED": "FAILED",
    "FAIL": "FAILED",
    "ERROR": "FAILED",
    "CANCELLED": "FAILED",
    "CANCELED": "FAILED",
    "TIMEOUT": "FAILED",
}


def normalize_remote_simulation_status(status: Any) -> str:
    """Normalize remote simulation statuses at the adapter boundary.

    This function is intentionally side-effect free.  It does not change
    durable ledger states; it only prevents equivalent remote responses from
    being interpreted differently by callers.
    """
    value = str(status or "").strip().upper()
    return _REMOTE_STATUS_ALIASES.get(value, "UNKNOWN")

SERVER_SLOT_DEFERRED_PREFIX = "Deferred: an existing server-side simulation is still RUNNING"

_HTTP_STATUS_IN_ERROR = re.compile(r"\bstatus=(\d{3})\b")
_TRANSIENT_POLL_ERROR_MARKERS = (
    "category=NETWORK",
    "ConnectionError",
    "Timeout",
    "RemoteDisconnected",
)


def frame_records(frame: Any) -> List[Dict[str, Any]]:
    """Normalize the V2.1 result container into plain record dicts."""
    if frame is None:
        return []
    if isinstance(frame, list):
        return [dict(x) for x in frame if isinstance(x, Mapping)]
    to_dict = getattr(frame, "to_dict", None)
    if callable(to_dict):
        try:
            rows = to_dict(orient="records")
        except TypeError:
            rows = to_dict("records")
        if isinstance(rows, list):
            return [dict(x) for x in rows if isinstance(x, Mapping)]
    return []


def server_slot_deferred_sim_keys(frame: Any, eligible_post_keys: Iterable[str]) -> List[str]:
    """Return only rows that V2.1 positively marks as never POST-dispatched.

    Missing durable facts stay ambiguous and must remain fail-closed.  V2.1's
    SERVER SLOT GUARD is stronger evidence: on a normal return it synthesizes a
    ``status=NEW`` result with a stable ``Deferred: ...`` error for items it
    deliberately did not dispatch.
    """
    eligible = {str(x) for x in eligible_post_keys if x}
    out: List[str] = []
    for row in frame_records(frame):
        key = str(row.get("sim_key") or "")
        if key not in eligible:
            continue
        status = str(row.get("status") or "").upper()
        error = str(row.get("error") or "")
        if status == "NEW" and error.startswith(SERVER_SLOT_DEFERRED_PREFIX):
            out.append(key)
    return sorted(set(out))


def _running_elapsed_seconds(record: Mapping[str, Any]) -> Optional[int]:
    """Return durable execution age without using poll-refreshed updated_at."""
    return durable_running_age_seconds(record.get("submitted_at"))


def _transient_poll_http_status(record: Mapping[str, Any]) -> Optional[int]:
    error = str(record.get("error") or "")
    match = _HTTP_STATUS_IN_ERROR.search(error)
    error_status = int(match.group(1)) if match else None
    try:
        stored_status = int(record.get("last_http_status"))
    except (TypeError, ValueError):
        stored_status = None
    if error_status == 429 or (error_status is not None and 500 <= error_status <= 599):
        return error_status
    if stored_status == 429 or (stored_status is not None and 500 <= stored_status <= 599):
        return stored_status
    if any(marker in error for marker in _TRANSIENT_POLL_ERROR_MARKERS):
        return error_status or stored_status
    return None


def _recover_stale_running_record(
    machine_lib: Any,
    *,
    cache_db: str,
    sim_key: str,
    candidate: Mapping[str, Any],
    settings: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    """Mark one old resume-only transient failure stale without re-POSTing."""
    required = ("cache_get", "_is_stale_running_record", "mark_stale_running")
    if not all(callable(getattr(machine_lib, name, None)) for name in required):
        return None
    current = machine_lib.cache_get(cache_db, sim_key) or {}
    status = str(current.get("status") or "").upper()
    simulation_url = str(current.get("simulation_url") or "")
    if status not in {"RUNNING", "SUBMITTED"} or not simulation_url:
        return None
    last_http_status = _transient_poll_http_status(current)
    if last_http_status is None:
        return None

    # Poll failures update cache.updated_at, so use the immutable submission
    # timestamp as the running-age anchor when asking V2.1's existing stale
    # predicate to apply its STALE_RUNNING_AFTER_SECONDS threshold.
    stale_probe = dict(current)
    stale_probe["updated_at"] = current.get("submitted_at") or current.get("updated_at")
    if not machine_lib._is_stale_running_record(stale_probe):
        return None
    elapsed_seconds = _running_elapsed_seconds(current)
    if elapsed_seconds is None:
        return None

    reason = "REMOTE_HTTP_ERROR_TIMEOUT"
    machine_lib.mark_stale_running(
        cache_db,
        sim_key,
        dict(candidate),
        dict(settings),
        reason=reason,
    )
    recovered = machine_lib.cache_get(cache_db, sim_key) or {}
    audit_event(
        action="STALE_RUNNING_RECOVERY",
        reason=reason,
        sim_key=sim_key,
        simulation_url=simulation_url,
        elapsed_seconds=elapsed_seconds,
        last_http_status=last_http_status,
    )
    return {
        **recovered,
        "candidate": dict(candidate),
        "settings": dict(settings),
        "sim_key": sim_key,
        "cached": True,
    }


class RemoteSimulationResolved(RuntimeError):
    """Internal control flow: the remote URL was durably closed without result."""


def _persist_resolution_fact(
    machine_lib: Any, cache_db: str, sim_key: str, candidate: Mapping[str, Any],
    settings: Mapping[str, Any], current: Mapping[str, Any], resolution: Mapping[str, Any],
) -> None:
    machine_lib.cache_put(
        cache_db, sim_key, dict(candidate), dict(settings),
        {
            "alpha_id": current.get("alpha_id"),
            "status": "REMOTE_NOT_FOUND",
            "simulation_url": current.get("simulation_url"),
            "submitted_at": current.get("submitted_at"),
            "retry_count": current.get("retry_count"),
            "last_http_status": (
                (resolution.get("verification_statuses") or [None])[-1]
                or resolution.get("delete_http_status")
                or current.get("last_http_status")
            ),
            "error": json.dumps(dict(resolution), ensure_ascii=False, sort_keys=True),
            "warning": current.get("warning"),
        },
    )


def _resolve_existing_remote(
    machine_lib: Any, session: Any, *, cache_db: str, sim_key: str,
    candidate: Mapping[str, Any], settings: Mapping[str, Any], trigger_source: str,
    cancellation_reason: str, verify_attempts: int,
) -> Mapping[str, Any]:
    current = machine_lib.cache_get(cache_db, sim_key) or {}
    simulation_url = str(current.get("simulation_url") or "")
    if not simulation_url:
        raise RuntimeError("REMOTE_RESOLUTION_SIMULATION_URL_MISSING")
    resolution = resolve_remote_simulation(
        session, simulation_url, trigger_source=trigger_source,
        submitted_at=current.get("submitted_at"), cancellation_reason=cancellation_reason,
        verify_attempts=verify_attempts,
        status_policy=production_remote_resolution_status,
    ).to_dict()
    audit_event(**remote_resolution_audit_payload(resolution, sim_key=sim_key))
    if resolution["resolution_result"] in {"DELEGATE_RESULT", "DELEGATE_TERMINAL_FAILURE"}:
        return dict(resolution.get("payload") or {})
    if resolution["resolution_result"] == "REMOTE_NOT_FOUND":
        _persist_resolution_fact(
            machine_lib, cache_db, sim_key, candidate, settings, current, resolution,
        )
        raise RemoteSimulationResolved(
            f"REMOTE_NOT_FOUND:{resolution.get('resolution_reason')}"
        )
    raise RuntimeError(str(resolution.get("error_reason") or "REMOTE_RESOLUTION_UNRESOLVED"))


@contextmanager
def _durable_timeout_polling(
    machine_lib: Any, *, operational_timeout_seconds: int,
    stale_backstop_seconds: int, auto_cancel: bool, verify_attempts: int,
):
    """Wrap V2.1 polling while retaining its result/status implementation."""
    original = getattr(machine_lib, "wait_simulation", None)
    if not callable(original):
        # Test/validation facades may implement simulate_candidates directly.
        # They cannot perform the production polling path, so there is nothing
        # to wrap and their established adapter contract remains unchanged.
        yield
        return

    def wait(session: Any, progress_url: str, **kwargs: Any) -> Mapping[str, Any]:
        cache_db = kwargs.get("cache_db")
        sim_key = kwargs.get("sim_key")
        current = machine_lib.cache_get(cache_db, sim_key) if cache_db and sim_key else None
        age = durable_running_age_seconds((current or {}).get("submitted_at"))
        configured = max(1, int(operational_timeout_seconds))
        remaining = configured if age is None else max(0, configured - age)
        trigger_source = "STALE_RECOVERY" if age is not None and age >= int(stale_backstop_seconds) else "AUTO_TIMEOUT"
        cancellation_reason = "STALE_RECOVERY_CANCELLED" if trigger_source == "STALE_RECOVERY" else "AUTO_CANCEL_TIMEOUT"
        if not auto_cancel:
            kwargs["max_wait_seconds"] = max(1, remaining or configured)
            return original(session, progress_url, **kwargs)
        if remaining <= 0 and current:
            return _resolve_existing_remote(
                machine_lib, session, cache_db=str(cache_db), sim_key=str(sim_key),
                candidate=kwargs.get("candidate") or {}, settings=kwargs.get("settings") or {},
                trigger_source=trigger_source, cancellation_reason=cancellation_reason,
                verify_attempts=verify_attempts,
            )
        kwargs["max_wait_seconds"] = max(1, remaining)
        try:
            return original(session, progress_url, **kwargs)
        except TimeoutError:
            return _resolve_existing_remote(
                machine_lib, session, cache_db=str(cache_db), sim_key=str(sim_key),
                candidate=kwargs.get("candidate") or {}, settings=kwargs.get("settings") or {},
                trigger_source=trigger_source, cancellation_reason=cancellation_reason,
                verify_attempts=verify_attempts,
            )

    machine_lib.wait_simulation = wait
    try:
        yield
    finally:
        machine_lib.wait_simulation = original



def validate_execution_permission(
    config: Any,
    candidates: Iterable[Mapping[str, Any]],
    *,
    dry_run: bool,
    allow_simulation_post: bool,
    remaining_initial_budget: int,
) -> None:
    actions = {str(item.get("execution_action")) for item in candidates}
    if dry_run or not config.plan["execution"].get("allow_new_simulations"):
        raise ConfigError("SIMULATION_POST_DISABLED")
    if actions & POST_ACTIONS and not allow_simulation_post:
        raise ConfigError("SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
    if "NEW_SIMULATION_REQUIRED" in actions and remaining_initial_budget <= 0:
        raise ConfigError("INITIAL_SIMULATION_BUDGET_EXHAUSTED")
    if config.plan["run_profile"] not in {"PRODUCTION_RESEARCH", "LIVE_VALIDATION"}:
        raise ConfigError("RUN_PROFILE_DOES_NOT_ALLOW_SIMULATION")


def to_v21_candidates(candidates: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item["v21_candidate"]) for item in candidates]


def _clean_v21_candidate(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        k: v for k, v in dict(candidate).items()
        if not str(k).startswith("_v22_") and k != "_expected_sim_key"
    }


def _effective_settings(
    v21_candidate: Mapping[str, Any],
    default_settings: Mapping[str, Any],
    machine_lib: Any,
) -> Dict[str, Any]:
    """Return one candidate's complete canonical Simulation settings.

    Durable ``settings_json`` is the authoritative identity for V3 candidates.
    It is carried into the V2.1 wrapper as ``_v22_settings`` and, when complete,
    is returned unchanged (aside from making a plain ``dict`` copy).  Legacy
    rows without complete durable settings are materialized through the unchanged
    V2.1 ``build_settings`` helper so there is still only one definition of the
    platform payload defaults.

    This function intentionally returns the *full* BRAIN settings payload.  Code
    that only needs the compatibility/grouping dimensions must project them via
    ``_execution_scope`` instead of redefining a second settings representation.
    """
    if not hasattr(machine_lib, "build_settings"):
        raise ConfigError("V21_SETTINGS_BUILD_CAPABILITY_MISSING")

    raw = v21_candidate.get("_v22_settings")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            raw = None

    if isinstance(raw, Mapping) and all(k in raw for k in FULL_SIMULATION_SETTING_KEYS):
        return validate_full_simulation_settings(raw, context="V21_DURABLE_SETTINGS")

    source = dict(default_settings)
    if isinstance(raw, Mapping):
        source.update(dict(raw))
    clean = _clean_v21_candidate(v21_candidate)
    built = machine_lib.build_settings(
        clean,
        neutralization=source["neutralization"],
        region=source["region"],
        universe=source["universe"],
        delay=int(source["delay"]),
        truncation=float(source["truncation"]),
        test_period=source.get("testPeriod", source.get("test_period", "P0Y")),
    )
    return validate_full_simulation_settings(built, context="V21_MATERIALIZED_SETTINGS")


def _execution_scope(settings: Mapping[str, Any]) -> Tuple[Any, ...]:
    """Project full canonical settings to V2.1's call-scope dimensions."""
    return (
        settings["neutralization"], settings["region"], settings["universe"],
        int(settings["delay"]), float(settings["truncation"]), settings["testPeriod"],
    )


def _validate_expected_sim_key(machine_lib: Any, candidate: Mapping[str, Any], settings: Mapping[str, Any]) -> None:
    """Fail closed if full effective settings do not reproduce durable identity."""
    expected = candidate.get("_expected_sim_key")
    if not expected:
        return
    if not hasattr(machine_lib, "simulation_key"):
        raise ConfigError("V21_SETTINGS_VALIDATION_CAPABILITY_MISSING")
    clean = _clean_v21_candidate(candidate)
    actual = machine_lib.simulation_key(str(clean["expr"]), dict(settings))
    if str(actual) != str(expected):
        raise ConfigError(
            f"V21_SETTINGS_SIM_KEY_MISMATCH: expected={expected} actual={actual}"
        )


def _validate_v21_materialization(
    machine_lib: Any, candidate: Mapping[str, Any], settings: Mapping[str, Any]
) -> None:
    """Fail closed if V2.1 would rebuild different settings at execution time.

    ``execute_with_v21`` cannot hand a full settings object directly to the
    unchanged V2.1 scheduler; it projects the legacy call-scope arguments and
    V2.1 then calls ``build_settings`` for each candidate.  Validate that this
    materialized payload is exactly the durable full-settings identity before
    delegating.  This closes the same class of drift that caused the D2E direct
    POST compact-settings incident, but on the legacy adapter path.
    """
    if not hasattr(machine_lib, "build_settings"):
        raise ConfigError("V21_SETTINGS_BUILD_CAPABILITY_MISSING")
    clean = _clean_v21_candidate(candidate)
    rebuilt = machine_lib.build_settings(
        clean,
        neutralization=settings["neutralization"],
        region=settings["region"],
        universe=settings["universe"],
        delay=int(settings["delay"]),
        truncation=float(settings["truncation"]),
        test_period=settings["testPeriod"],
    )
    if dict(rebuilt) != dict(settings):
        raise ConfigError(
            "V21_POST_SETTINGS_MATERIALIZATION_MISMATCH: "
            f"durable={json.dumps(dict(settings), ensure_ascii=False, sort_keys=True, separators=(',', ':'))} "
            f"rebuilt={json.dumps(dict(rebuilt), ensure_ascii=False, sort_keys=True, separators=(',', ':'))}"
        )


def _combine_results(outputs: List[Any]) -> Any:
    if not outputs:
        return []
    if len(outputs) == 1:
        return outputs[0]
    # Production V2.1 returns pandas.DataFrame. Keep the adapter lightweight and
    # only import pandas when multiple settings groups actually occur.
    if all(hasattr(x, "columns") and hasattr(x, "index") for x in outputs):
        import pandas as pd
        return pd.concat(outputs, ignore_index=True)
    if all(isinstance(x, list) for x in outputs):
        merged: List[Any] = []
        for x in outputs:
            merged.extend(x)
        return merged
    return outputs


def execute_with_v21(
    candidates: List[Mapping[str, Any]], config: Any, machine_lib: Any, *, session: Any,
    cache_db: str, allow_simulation_post: bool, remaining_initial_budget: int,
    stop_event: Optional[Any] = None,
) -> Any:
    """Delegate execution to unchanged V2.1 while preserving per-candidate settings.

    V2.1 accepts one call-scope settings tuple per ``simulate_candidates`` call.
    V2.2 repair candidates may intentionally vary any durable Simulation setting.
    We therefore keep each group homogeneous in the *complete* durable settings,
    project only the six legacy call-scope arguments at delegation time, and
    validate that those settings
    reproduce the previewed sim_key, then invoke V2.1 once per group. This keeps
    V2.1 untouched and prevents a previewed MARKET sim_key from being POSTed with
    the run-level SUBINDUSTRY settings.
    """
    validate_execution_permission(
        config, candidates, dry_run=False, allow_simulation_post=allow_simulation_post,
        remaining_initial_budget=remaining_initial_budget,
    )
    defaults = config.plan["simulation_settings"]
    runtime = config.plan["runtime"]
    operational_timeout = int(runtime["simulation_poll_timeout_seconds"])
    auto_cancel = bool(runtime.get("simulation_auto_cancel_on_timeout", False))
    verify_attempts = max(2, int(runtime.get("simulation_auto_cancel_verify_attempts", 2)))
    stale_backstop = max(
        operational_timeout,
        int(runtime.get("simulation_stale_backstop_seconds", 21600)),
    )
    groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    settings_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    stale_rows: List[Dict[str, Any]] = []

    for wrapped in candidates:
        v21 = dict(wrapped["v21_candidate"])
        effective = _effective_settings(v21, defaults, machine_lib)
        _validate_expected_sim_key(machine_lib, v21, effective)
        _validate_v21_materialization(machine_lib, v21, effective)
        key = full_settings_identity(effective)
        clean = {k: v for k, v in v21.items() if not str(k).startswith("_v22_") and k != "_expected_sim_key"}
        expected_sim_key = str(v21.get("_expected_sim_key") or "")
        if str(wrapped.get("execution_action") or "") == "RESUME_EXISTING" and expected_sim_key:
            current = machine_lib.cache_get(cache_db, expected_sim_key) or {}
            current_status = str(current.get("status") or "").upper()
            if current_status == "REMOTE_NOT_FOUND":
                stale_rows.append({**current, "candidate": clean, "settings": effective,
                                   "sim_key": expected_sim_key, "cached": True})
                continue
            if current_status == "STALE_RUNNING" and current.get("simulation_url"):
                # V2.1 treats unknown statuses as POST candidates. Restore the
                # resumable local marker before delegation; the durable-age
                # wrapper below immediately forces remote resolution.
                machine_lib.cache_put(
                    cache_db, expected_sim_key, clean, effective,
                    {
                        "alpha_id": current.get("alpha_id"), "status": "RUNNING",
                        "simulation_url": current.get("simulation_url"),
                        "submitted_at": current.get("submitted_at"),
                        "retry_count": current.get("retry_count"),
                        "last_http_status": current.get("last_http_status"),
                        "error": current.get("error"), "warning": current.get("warning"),
                    },
                )
        groups[key].append(clean)
        settings_by_key[key] = effective

    outputs: List[Any] = []
    for key, group in groups.items():
        settings = settings_by_key[key]
        with _durable_timeout_polling(
            machine_lib, operational_timeout_seconds=operational_timeout,
            stale_backstop_seconds=stale_backstop,
            auto_cancel=auto_cancel, verify_attempts=verify_attempts,
        ):
            output = machine_lib.simulate_candidates(
                group, neutralization=settings["neutralization"],
                region=settings["region"], universe=settings["universe"], session=session,
                cache_db=cache_db, concurrency=runtime["concurrency"],
                delay=settings["delay"], truncation=settings["truncation"],
                test_period=settings["testPeriod"],
                poll_timeout_seconds=operational_timeout,
                glb_max_concurrency=runtime["glb_max_concurrency"],
                other_max_concurrency=runtime["other_max_concurrency"],
                stop_event=stop_event,
            )
        outputs.append(output)
        for row in frame_records(output):
            _recover_stale_running_record(
                machine_lib,
                cache_db=cache_db,
                sim_key=str(row.get("sim_key") or ""),
                candidate=row.get("candidate") or {},
                settings=row.get("settings") or settings,
            )
    combined = _combine_results(outputs)
    if not stale_rows:
        return combined
    if not outputs:
        return stale_rows
    if isinstance(combined, list):
        return [*combined, *stale_rows]
    if hasattr(combined, "columns") and hasattr(combined, "index"):
        import pandas as pd
        return pd.concat([combined, pd.DataFrame(stale_rows)], ignore_index=True)
    return [*frame_records(combined), *stale_rows]
