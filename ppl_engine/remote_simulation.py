"""Fail-closed resolution of an already-submitted remote Simulation.

This module owns the GET-before-DELETE protocol only.  It deliberately does
not normalize or redefine BRAIN terminal states: callers provide the exact
production status policy and retain responsibility for normal COMPLETE result
processing and local workflow transitions.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional
from urllib.parse import urlparse


MISSING_HTTP_STATUSES = {404, 410}
LOCAL_HTTP_POLICY_ERROR_PREFIXES = (
    "PHASE10A_FORBIDDEN_HTTP_METHOD:",
    "PHASE10A_UNEXPECTED_POST:",
)


@dataclass
class RemoteResolutionResult:
    trigger_source: str
    resolution_result: str
    resolution_reason: Optional[str]
    simulation_url: str
    simulation_id: str
    submitted_at: Optional[str]
    running_age_seconds: Optional[int]
    pre_resolution_remote_status: Optional[str] = None
    delete_attempted: bool = False
    delete_http_status: Optional[int] = None
    verification_statuses: List[int] = field(default_factory=list)
    payload: Optional[Dict[str, Any]] = None
    error_reason: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def remote_resolution_audit_payload(
    resolution: Mapping[str, Any], **identity: Any,
) -> Dict[str, Any]:
    """Build one collision-free audit mapping at the call boundary.

    Resolver identity fields remain in ``resolution``.  Context known only to
    the caller is overlaid once, then the complete mapping is passed to
    ``audit_event`` without mixing explicit keywords and ``**resolution``.
    """
    payload = dict(resolution)
    for key, value in identity.items():
        if value is not None:
            payload[str(key)] = value
    payload["action"] = "REMOTE_SIMULATION_RESOLUTION"
    return payload


def durable_running_age_seconds(submitted_at: Any, *, now: Optional[datetime] = None) -> Optional[int]:
    """Calculate total remote age from the immutable submit timestamp only."""
    if not submitted_at:
        return None
    try:
        parsed = datetime.fromisoformat(str(submitted_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0, int((current - parsed).total_seconds()))
    except (TypeError, ValueError, OverflowError):
        return None


def validate_simulation_url(simulation_url: str, simulation_id: Optional[str] = None) -> str:
    """Restrict destructive requests to one HTTPS BRAIN simulation resource."""
    value = str(simulation_url or "").strip()
    parsed = urlparse(value)
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.scheme != "https" or parsed.netloc.lower() != "api.worldquantbrain.com":
        raise ValueError("REMOTE_SIMULATION_URL_NOT_ALLOWED")
    if len(parts) != 2 or parts[0] != "simulations" or not parts[1]:
        raise ValueError("REMOTE_SIMULATION_URL_NOT_SINGLE_RESOURCE")
    if parsed.query or parsed.fragment:
        raise ValueError("REMOTE_SIMULATION_URL_NOT_SINGLE_RESOURCE")
    if simulation_id is not None and parts[1] != str(simulation_id):
        raise ValueError("REMOTE_SIMULATION_ID_URL_MISMATCH")
    return value


def _response_payload(response: Any) -> Optional[Dict[str, Any]]:
    try:
        payload = response.json()
    except (TypeError, ValueError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _is_local_http_policy_error(exc: BaseException) -> bool:
    message = str(exc)
    return any(message.startswith(prefix) for prefix in LOCAL_HTTP_POLICY_ERROR_PREFIXES)


def _confirmed_missing(session: Any, url: str, *, attempts: int, timeout: int,
                       sleep_seconds: float, initial_status: Optional[int] = None) -> List[int]:
    statuses: List[int] = []
    if initial_status is not None:
        statuses.append(int(initial_status))
    while len(statuses) < max(2, int(attempts)):
        if statuses and sleep_seconds > 0:
            time.sleep(sleep_seconds)
        response = session.get(url, timeout=timeout)
        status = int(getattr(response, "status_code", 0) or 0)
        statuses.append(status)
        if status not in MISSING_HTTP_STATUSES:
            break
    return statuses


def resolve_remote_simulation(
    session: Any,
    simulation_url: str,
    *,
    trigger_source: str,
    submitted_at: Optional[str],
    cancellation_reason: str,
    verify_attempts: int = 2,
    request_timeout_seconds: int = 30,
    verification_sleep_seconds: float = 1.0,
    status_policy: Callable[[Mapping[str, Any]], str],
) -> RemoteResolutionResult:
    """Resolve one remote resource, preserving uncertainty on every failure."""
    url = validate_simulation_url(simulation_url)
    age = durable_running_age_seconds(submitted_at)
    result = RemoteResolutionResult(
        trigger_source=str(trigger_source), resolution_result="UNRESOLVED",
        resolution_reason=None, simulation_url=url,
        simulation_id=url.rstrip("/").rsplit("/", 1)[-1], submitted_at=submitted_at,
        running_age_seconds=age,
    )
    try:
        first = session.get(url, timeout=request_timeout_seconds)
    except Exception as exc:
        result.error_reason = "REMOTE_RESOLUTION_NETWORK_ERROR"
        result.payload = {"error_type": type(exc).__name__, "error": str(exc)}
        return result

    first_status = int(getattr(first, "status_code", 0) or 0)
    if first_status in MISSING_HTTP_STATUSES:
        try:
            statuses = _confirmed_missing(
                session, url, attempts=verify_attempts, timeout=request_timeout_seconds,
                sleep_seconds=verification_sleep_seconds, initial_status=first_status,
            )
        except Exception as exc:
            result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
            result.payload = {"error_type": type(exc).__name__, "error": str(exc)}
            return result
        result.verification_statuses = statuses
        if len(statuses) >= 2 and all(code in MISSING_HTTP_STATUSES for code in statuses):
            result.resolution_result = "REMOTE_NOT_FOUND"
            result.resolution_reason = "REMOTE_ALREADY_ABSENT"
        else:
            result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
        return result

    if first_status in {401, 403}:
        result.error_reason = "REMOTE_CANCEL_AUTH_ERROR"
        return result
    if first_status != 200:
        result.error_reason = "REMOTE_CANCEL_HTTP_ERROR"
        result.payload = {"http_status": first_status}
        return result

    payload = _response_payload(first)
    if payload is None:
        result.error_reason = "REMOTE_RESOLUTION_INVALID_RESPONSE"
        return result
    result.payload = payload
    result.pre_resolution_remote_status = str(payload.get("status") or "").upper() or None
    classification = status_policy(payload)
    if classification == "RUNNING" and result.pre_resolution_remote_status is None:
        # Existing wait_simulation displays a missing status as PENDING while
        # persisting it as RUNNING. Preserve that production-facing meaning in
        # resolution audit output.
        result.pre_resolution_remote_status = "PENDING"
    if classification == "RESULT_READY":
        result.resolution_result = "DELEGATE_RESULT"
        return result
    if classification == "TERMINAL_FAILURE":
        if verification_sleep_seconds > 0:
            time.sleep(verification_sleep_seconds)
        try:
            confirmed = session.get(url, timeout=request_timeout_seconds)
            confirmed_status = int(getattr(confirmed, "status_code", 0) or 0)
            confirmed_payload = _response_payload(confirmed) if confirmed_status == 200 else None
        except Exception as exc:
            result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
            result.payload = {**payload, "verification_error_type": type(exc).__name__,
                              "verification_error": str(exc)}
            return result
        result.verification_statuses = [first_status, confirmed_status]
        if confirmed_payload is not None and status_policy(confirmed_payload) == "TERMINAL_FAILURE":
            result.payload = confirmed_payload
            result.resolution_result = "DELEGATE_TERMINAL_FAILURE"
        else:
            result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
        return result
    if classification != "RUNNING":
        result.error_reason = "REMOTE_RESOLUTION_UNKNOWN_STATUS"
        return result

    result.delete_attempted = True
    try:
        deleted = session.delete(
            url, timeout=request_timeout_seconds,
            headers={"Accept": "application/json;version=2.0"},
        )
    except Exception as exc:
        result.error_reason = (
            "REMOTE_CANCEL_LOCAL_POLICY_ERROR"
            if _is_local_http_policy_error(exc)
            else "REMOTE_CANCEL_NETWORK_ERROR"
        )
        result.payload = {**payload, "delete_error_type": type(exc).__name__, "delete_error": str(exc)}
        return result
    result.delete_http_status = int(getattr(deleted, "status_code", 0) or 0)
    if result.delete_http_status in {401, 403}:
        result.error_reason = "REMOTE_CANCEL_AUTH_ERROR"
        return result
    if result.delete_http_status not in ({200} | MISSING_HTTP_STATUSES):
        result.error_reason = "REMOTE_CANCEL_HTTP_ERROR"
        return result

    try:
        statuses = _confirmed_missing(
            session, url, attempts=verify_attempts, timeout=request_timeout_seconds,
            sleep_seconds=verification_sleep_seconds,
        )
    except Exception as exc:
        result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
        result.payload = {**payload, "verification_error_type": type(exc).__name__, "verification_error": str(exc)}
        return result
    result.verification_statuses = statuses
    if len(statuses) >= 2 and all(code in MISSING_HTTP_STATUSES for code in statuses):
        result.resolution_result = "REMOTE_NOT_FOUND"
        result.resolution_reason = (
            "REMOTE_ALREADY_ABSENT"
            if result.delete_http_status in MISSING_HTTP_STATUSES
            else str(cancellation_reason)
        )
    else:
        result.error_reason = "REMOTE_CANCEL_VERIFICATION_FAILED"
    return result
