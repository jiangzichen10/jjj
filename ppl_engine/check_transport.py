"""Transport boundary and offline-testable semantic polling for /check."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol

from .check_parser import parse_response_text


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CheckResponse:
    http_status: int
    text: str
    http_request_count: int = 1
    # Time spent waiting behind the shared /check throttle gate.  This is
    # operational back-pressure, not semantic polling time, so callers may
    # exclude it from the normal per-check timeout budget.
    throttle_wait_seconds: float = 0.0


class CheckTransport(Protocol):
    def fetch_check(self, alpha_id: str) -> CheckResponse: ...


class V21CheckTransport:
    """Future live adapter. Construction alone performs no network request."""
    def __init__(self, session: Any, machine_lib: Any):
        self.session = session
        self.machine_lib = machine_lib

    def fetch_check(self, alpha_id: str) -> CheckResponse:
        response = self.machine_lib._request_with_retry(
            self.session, "GET", f"{self.machine_lib.BRAIN_API_URL}/alphas/{alpha_id}/check"
        )
        return CheckResponse(int(response.status_code), response.text, 1)


class MeteredSession:
    """Counts real low-level request() calls without changing V2.1 retry behavior."""
    def __init__(self, session: Any):
        self.session = session
        self.request_count = 0

    def request(self, *args: Any, **kwargs: Any) -> Any:
        self.request_count += 1
        return self.session.request(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.session, name)


@dataclass
class CheckBudget:
    max_check_candidates: int
    max_check_http_requests: int
    max_poll_requests_per_candidate: int
    max_check_sessions_per_candidate: int
    check_candidates: int = 0
    check_sessions: int = 0
    check_http_requests: int = 0
    pending_poll_requests: int = 0
    seen_candidates: set = field(default_factory=set)
    sessions_by_candidate: Dict[str, int] = field(default_factory=dict)


def _http_error(status: int) -> Dict[str, str]:
    if status == 429:
        return {"error_type": "HTTP_429", "error_nature": "TRANSIENT"}
    if status in {401, 403}:
        return {"error_type": "AUTH_ERROR", "error_nature": "TRANSIENT"}
    if 400 <= status < 500:
        return {"error_type": "HTTP_4XX", "error_nature": "DETERMINISTIC"}
    if status >= 500:
        return {"error_type": "HTTP_5XX", "error_nature": "TRANSIENT"}
    return {"error_type": "UNKNOWN_ERROR", "error_nature": "UNKNOWN"}


def semantic_poll_check(
    transport: CheckTransport, *, alpha_id: str, phase: str, rules: Dict[str, Any],
    budget: CheckBudget, candidate_id: Optional[str] = None, run_id: Optional[str] = None,
    evidence_source: str = "SYNTHETIC_TEST", wait: Callable[[float], None] = lambda _: None,
    clock: Callable[[], float] = time.monotonic, store: Any = None,
    throttle_max_events: Optional[int] = None,
) -> Dict[str, Any]:
    candidate_key = candidate_id or alpha_id
    if candidate_key not in budget.seen_candidates and budget.check_candidates >= budget.max_check_candidates:
        return {"session_status": "BUDGET_EXHAUSTED", "error_type": "BUDGET_EXHAUSTED", "polls": []}
    if budget.sessions_by_candidate.get(candidate_key, 0) >= budget.max_check_sessions_per_candidate:
        return {"session_status": "BUDGET_EXHAUSTED", "error_type": "BUDGET_EXHAUSTED", "polls": []}
    if candidate_key not in budget.seen_candidates:
        budget.seen_candidates.add(candidate_key); budget.check_candidates += 1
    budget.sessions_by_candidate[candidate_key] = budget.sessions_by_candidate.get(candidate_key, 0) + 1
    budget.check_sessions += 1
    session_id = "chk_" + uuid.uuid4().hex
    start = clock(); timeout = float(rules["check"]["timeout_seconds"]); interval = float(rules["check"]["poll_seconds"])
    throttle_wait_total = 0.0
    throttle_events = 0
    polls: List[Dict[str, Any]] = []; final = None; status = "CREATED"; error_type = None; error_nature = None
    for index in range(1, budget.max_poll_requests_per_candidate + 1):
        if budget.check_http_requests >= budget.max_check_http_requests:
            status = "BUDGET_EXHAUSTED"; error_type = "BUDGET_EXHAUSTED"; error_nature = "UNKNOWN"; break
        # A platform-wide 429 cooldown can legitimately last minutes.  Do not
        # count time spent behind the shared throttle gate against the normal
        # semantic /check timeout; otherwise a healthy cooldown would be
        # misclassified as POLL_TIMEOUT before the next request is attempted.
        if clock() - start - throttle_wait_total >= timeout:
            status = "TIMEOUT"; error_type = "POLL_TIMEOUT"; error_nature = "TRANSIENT"; break
        response = transport.fetch_check(alpha_id)
        throttle_wait_total += max(0.0, float(getattr(response, "throttle_wait_seconds", 0.0) or 0.0))
        delta = max(1, int(response.http_request_count)); budget.check_http_requests += delta
        if not (200 <= int(response.http_status) < 300):
            error = _http_error(int(response.http_status))
            parsed = {"session_semantic_status": "TRANSIENT_ERROR", "parse_status": error["error_type"],
                      "results": [], "pending_check_names": [], "base_gate": {"status": "PENDING"},
                      "theme_gate": {"status": "PENDING"}, **error}
        else:
            parsed = parse_response_text(response.text, phase=phase, rules=rules, evidence_source=evidence_source)
        poll = {"semantic_poll_index": index, "http_request_count_delta": delta,
                "http_status": response.http_status, "raw_response_text": response.text,
                "parsed": parsed, "created_at": _utc_now()}
        polls.append(poll); final = parsed
        if int(response.http_status) == 429:
            throttle_events += 1
            if throttle_max_events is not None and throttle_events >= max(1, int(throttle_max_events)):
                status = "BUDGET_EXHAUSTED"
                error_type = "HTTP_429_THROTTLE_DEFERRED"
                error_nature = "TRANSIENT"
                break
        if budget.check_http_requests > budget.max_check_http_requests:
            status = "BUDGET_EXHAUSTED"; error_type = "BUDGET_EXHAUSTED"; error_nature = "UNKNOWN"; break
        semantic = parsed.get("session_semantic_status")
        if semantic == "RESOLVED":
            status = "RESOLVED"; break
        status = "TRANSIENT_ERROR" if semantic == "TRANSIENT_ERROR" else "PENDING"
        error_type = parsed.get("error_type"); error_nature = parsed.get("error_nature")
        if index >= budget.max_poll_requests_per_candidate:
            status = "BUDGET_EXHAUSTED"; error_type = "BUDGET_EXHAUSTED"; error_nature = "UNKNOWN"; break
        budget.pending_poll_requests += 1
        wait(interval)
    else:
        status = "BUDGET_EXHAUSTED"; error_type = "BUDGET_EXHAUSTED"
    session = {
        "check_session_id": session_id, "run_id": run_id, "candidate_id": candidate_id,
        "alpha_id": alpha_id, "phase": phase, "session_status": status,
        "started_at": _utc_now(), "resolved_at": _utc_now() if status == "RESOLVED" else None,
        "poll_count": len(polls), "http_request_count": sum(x["http_request_count_delta"] for x in polls),
        "pending_poll_requests": max(0, len(polls) - 1), "base_gate_result": (final or {}).get("base_gate", {}).get("status"),
        "theme_gate_result": (final or {}).get("theme_gate", {}).get("status"),
        "error_type": error_type, "error_nature": error_nature,
        "throttle_wait_seconds": throttle_wait_total,
        "throttle_events": throttle_events,
        "polls": polls, "final": final,
    }
    if store is not None:
        store.save_check_session(session, evidence_source=evidence_source)
    return session
