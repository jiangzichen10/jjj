"""V3.1 research-run identity and scheduler-authority locks.

D2E uses a real-platform run for evidence collection while permanently keeping
PHASE_COMPATIBILITY authoritative.  The lock is stored in the round policy
snapshot (ppl_rounds.config_json) and is validated both at creation and resume.
This module does not perform HTTP, scheduling, or workflow transitions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


STANDARD_CONTINUOUS_MODE = "STANDARD_CONTINUOUS"
COMPATIBILITY_EVIDENCE_MODE = "COMPATIBILITY_EVIDENCE"
ADAPTIVE_CANARY_MODE = "ADAPTIVE_CANARY"
PHASE_COMPATIBILITY_AUTHORITY = "PHASE_COMPATIBILITY"
ADAPTIVE_AUTHORITY = "ADAPTIVE"
SHADOW_ONLY = "SHADOW_ONLY"
ADAPTIVE_DISABLED = "DISABLED"
ADAPTIVE_ARMED = "ARMED"
ADAPTIVE_ACTIVE = "ACTIVE"
STATE_RESOLVED_MATURATION = "STATE_RESOLVED"


def _is_hex_digest(value: Optional[str], length: int) -> bool:
    text = str(value or "")
    return len(text) == length and all(ch in "0123456789abcdefABCDEF" for ch in text)


@dataclass(frozen=True)
class ResearchRunPolicy:
    mode: str = STANDARD_CONTINUOUS_MODE
    expected_run_id: Optional[str] = None
    scheduler_authority: str = PHASE_COMPATIBILITY_AUTHORITY
    scheduler_shadow: str = SHADOW_ONLY
    adaptive_control: str = ADAPTIVE_DISABLED
    authority_transition_allowed: bool = False
    automatic_evidence_stop: bool = False
    maturation_semantics: str = STATE_RESOLVED_MATURATION
    long_running_semantics: str = "RIGHT_CENSORED_UNTIL_RESOLVED"
    d2_source_run_id: Optional[str] = None
    d2_gate_report_key: Optional[str] = None
    baseline_commit: Optional[str] = None
    expected_machine_hash: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_research_run_policy(raw_policy: Mapping[str, Any]) -> ResearchRunPolicy:
    raw = dict((raw_policy or {}).get("research_run") or {})
    if not raw:
        return ResearchRunPolicy()
    policy = ResearchRunPolicy(
        mode=str(raw.get("mode") or STANDARD_CONTINUOUS_MODE).upper(),
        expected_run_id=(str(raw.get("expected_run_id")).strip() if raw.get("expected_run_id") else None),
        scheduler_authority=str(raw.get("scheduler_authority") or PHASE_COMPATIBILITY_AUTHORITY).upper(),
        scheduler_shadow=str(raw.get("scheduler_shadow") or SHADOW_ONLY).upper(),
        adaptive_control=str(raw.get("adaptive_control") or ADAPTIVE_DISABLED).upper(),
        authority_transition_allowed=bool(raw.get("authority_transition_allowed", False)),
        automatic_evidence_stop=bool(raw.get("automatic_evidence_stop", False)),
        maturation_semantics=str(raw.get("maturation_semantics") or STATE_RESOLVED_MATURATION).upper(),
        long_running_semantics=str(raw.get("long_running_semantics") or "RIGHT_CENSORED_UNTIL_RESOLVED").upper(),
        d2_source_run_id=(str(raw.get("d2_source_run_id")).strip() if raw.get("d2_source_run_id") else None),
        d2_gate_report_key=(str(raw.get("d2_gate_report_key")).strip() if raw.get("d2_gate_report_key") else None),
        baseline_commit=(str(raw.get("baseline_commit")).strip().lower() if raw.get("baseline_commit") else None),
        expected_machine_hash=(str(raw.get("expected_machine_hash")).strip().upper() if raw.get("expected_machine_hash") else None),
    )
    if policy.mode not in {STANDARD_CONTINUOUS_MODE, COMPATIBILITY_EVIDENCE_MODE, ADAPTIVE_CANARY_MODE}:
        raise ValueError("RESEARCH_RUN_MODE_UNSUPPORTED")
    if policy.mode == COMPATIBILITY_EVIDENCE_MODE:
        if not policy.expected_run_id:
            raise ValueError("D2E_EXPECTED_RUN_ID_REQUIRED")
        if policy.scheduler_authority != PHASE_COMPATIBILITY_AUTHORITY:
            raise ValueError("D2E_AUTHORITY_MUST_BE_PHASE_COMPATIBILITY")
        if policy.scheduler_shadow != SHADOW_ONLY:
            raise ValueError("D2E_SHADOW_MODE_MUST_BE_SHADOW_ONLY")
        if policy.adaptive_control != ADAPTIVE_DISABLED:
            raise ValueError("D2E_ADAPTIVE_CONTROL_MUST_BE_DISABLED")
        if policy.authority_transition_allowed:
            raise ValueError("D2E_AUTHORITY_TRANSITION_MUST_BE_DISABLED")
        if policy.automatic_evidence_stop:
            raise ValueError("D2E_AUTOMATIC_EVIDENCE_STOP_FORBIDDEN")
        if policy.maturation_semantics != STATE_RESOLVED_MATURATION:
            raise ValueError("D2E_MATURATION_SEMANTICS_UNSUPPORTED")
    elif policy.mode == ADAPTIVE_CANARY_MODE:
        if not policy.expected_run_id:
            raise ValueError("D3_EXPECTED_RUN_ID_REQUIRED")
        if policy.scheduler_authority != ADAPTIVE_AUTHORITY:
            raise ValueError("D3_AUTHORITY_MUST_BE_ADAPTIVE")
        if policy.scheduler_shadow != PHASE_COMPATIBILITY_AUTHORITY:
            raise ValueError("D3_SHADOW_MUST_BE_PHASE_COMPATIBILITY")
        if policy.adaptive_control not in {ADAPTIVE_ARMED, ADAPTIVE_ACTIVE}:
            raise ValueError("D3_ADAPTIVE_CONTROL_MUST_BE_ARMED_OR_ACTIVE")
        if not policy.authority_transition_allowed:
            raise ValueError("D3_AUTHORITY_TRANSITION_MUST_BE_ALLOWED")
        if policy.maturation_semantics != STATE_RESOLVED_MATURATION:
            raise ValueError("D3_MATURATION_SEMANTICS_UNSUPPORTED")
        if not policy.d2_source_run_id or not policy.d2_gate_report_key:
            raise ValueError("D3_D2_GATE_IDENTITY_REQUIRED")
        if not _is_hex_digest(policy.baseline_commit, 40):
            raise ValueError("D3_BASELINE_COMMIT_REQUIRED")
        if not _is_hex_digest(policy.expected_machine_hash, 64):
            raise ValueError("D3_MACHINE_HASH_REQUIRED")
        if policy.d2_source_run_id != "run_0006":
            raise ValueError("D3_D2_SOURCE_RUN_MUST_BE_RUN_0006")
    return policy


def validate_new_research_run(
    policy: Mapping[str, Any], *, requested_run_id: Optional[str], resolved_run_id: Optional[str] = None,
) -> ResearchRunPolicy:
    parsed = parse_research_run_policy(policy)
    # Project-level experimental identity reservation: run_0006 is the first
    # V3.1 real-platform control/evidence run and may not be created under the
    # generic Continuous policy by operator mistake.
    effective_run_id = str(resolved_run_id or requested_run_id or "")
    if effective_run_id == "run_0006" and parsed.mode != COMPATIBILITY_EVIDENCE_MODE:
        raise ValueError("RUN_0006_RESERVED_FOR_D2E_COMPATIBILITY_EVIDENCE")
    if effective_run_id == "run_0007" and parsed.mode == STANDARD_CONTINUOUS_MODE:
        raise ValueError("RUN_0007_RESERVED_FOR_D3_ADAPTIVE_CANARY")
    if parsed.mode == COMPATIBILITY_EVIDENCE_MODE:
        if not requested_run_id:
            raise ValueError("D2E_EXPLICIT_RUN_ID_REQUIRED")
        if str(requested_run_id) != str(parsed.expected_run_id) or effective_run_id != str(parsed.expected_run_id):
            raise ValueError(f"D2E_RUN_ID_LOCK_MISMATCH:{effective_run_id}:{parsed.expected_run_id}")
        strategy = dict(policy.get("strategy_integration") or {})
        shadow = dict(policy.get("scheduler_shadow") or {})
        if str(strategy.get("mode") or "").upper() != PHASE_COMPATIBILITY_AUTHORITY:
            raise ValueError("D2E_STRATEGY_INTEGRATION_NOT_COMPATIBILITY")
        if not bool(shadow.get("enabled", False)) or str(shadow.get("mode") or "").upper() != SHADOW_ONLY:
            raise ValueError("D2E_SCHEDULER_SHADOW_NOT_SHADOW_ONLY")
    elif parsed.mode == ADAPTIVE_CANARY_MODE:
        if not requested_run_id:
            raise ValueError("D3_EXPLICIT_RUN_ID_REQUIRED")
        if str(requested_run_id) != str(parsed.expected_run_id) or effective_run_id != str(parsed.expected_run_id):
            raise ValueError(f"D3_RUN_ID_LOCK_MISMATCH:{effective_run_id}:{parsed.expected_run_id}")
        if parsed.expected_run_id != "run_0007":
            raise ValueError("D3_CANARY_RUN_ID_MUST_BE_RUN_0007")
        if parsed.adaptive_control != ADAPTIVE_ARMED:
            raise ValueError("D3_NEW_RUN_MUST_START_ARMED")
    return parsed


def validate_durable_research_run_lock(
    stored_policy: Mapping[str, Any], candidate_policy: Mapping[str, Any], *, run_id: str,
) -> ResearchRunPolicy:
    """Reject any attempt to change a D2E run's authority identity on resume."""
    stored = parse_research_run_policy(stored_policy)
    if str(run_id) == "run_0006" and stored.mode != COMPATIBILITY_EVIDENCE_MODE:
        raise ValueError("RUN_0006_DURABLE_MODE_CONFLICT")
    if stored.mode not in {COMPATIBILITY_EVIDENCE_MODE, ADAPTIVE_CANARY_MODE}:
        return stored
    if str(run_id) != str(stored.expected_run_id):
        raise ValueError(
            "D2E_DURABLE_RUN_ID_CONFLICT"
            if stored.mode == COMPATIBILITY_EVIDENCE_MODE
            else "RESEARCH_RUN_DURABLE_RUN_ID_CONFLICT"
        )
    stored_raw = dict((stored_policy or {}).get("research_run") or {})
    candidate_raw = dict((candidate_policy or {}).get("research_run") or {})
    if candidate_raw != stored_raw:
        raise ValueError(
            "D2E_RESEARCH_RUN_LOCK_DRIFT"
            if stored.mode == COMPATIBILITY_EVIDENCE_MODE
            else "RESEARCH_RUN_LOCK_DRIFT"
        )
    candidate = parse_research_run_policy(candidate_policy)
    if candidate.as_dict() != stored.as_dict():
        raise ValueError(
            "D2E_RESEARCH_RUN_LOCK_DRIFT"
            if stored.mode == COMPATIBILITY_EVIDENCE_MODE
            else "RESEARCH_RUN_LOCK_DRIFT"
        )
    # Revalidate the durable policy's linked execution sections as well.
    validate_new_research_run(stored_policy, requested_run_id=run_id, resolved_run_id=run_id)
    return stored


def research_run_status(policy: Mapping[str, Any], *, run_id: Optional[str] = None) -> dict[str, Any]:
    parsed = parse_research_run_policy(policy)
    return {
        **parsed.as_dict(),
        "run_id": run_id,
        "authority_locked": parsed.mode in {COMPATIBILITY_EVIDENCE_MODE, ADAPTIVE_CANARY_MODE},
        "adaptive_activation_allowed": (
            parsed.mode != COMPATIBILITY_EVIDENCE_MODE and parsed.authority_transition_allowed
        ),
        "d2e_evidence_only": parsed.mode == COMPATIBILITY_EVIDENCE_MODE,
        "d3_adaptive_canary": parsed.mode == ADAPTIVE_CANARY_MODE,
    }
