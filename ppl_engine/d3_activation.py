"""Read-only D3 Adaptive Canary activation preflight.

D3-A intentionally stops before authority activation.  This module verifies a
durable D2 gate and immutable runtime identity, but it never updates a round,
selects work, performs HTTP, or changes scheduler authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Optional

from .research_run_mode import (
    ADAPTIVE_ARMED,
    ADAPTIVE_CANARY_MODE,
    validate_new_research_run,
)
from .scheduler_evidence import evidence_policy_hash
from .scheduler_shadow import policy_from_mapping, shadow_policy_hash


REQUIRED_D2_GATE_CHECKS = (
    "deterministic_replay",
    "starvation",
    "slot_safety",
    "no_repost",
    "recovery_safety",
    "policy_identity",
)


@dataclass(frozen=True)
class D3ActivationPreflight:
    eligible: bool
    status: str
    checks: Mapping[str, bool] = field(default_factory=dict)
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _sqlite_integrity_read_only(path: Path) -> bool:
    resolved = Path(path).resolve()
    if not resolved.exists():
        return False
    uri = f"file:{resolved.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        return str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower() == "ok"
    finally:
        conn.close()


def evaluate_d3_activation_preflight(
    store: Any,
    policy: Mapping[str, Any],
    *,
    requested_run_id: str,
    current_baseline_commit: str,
    current_machine_hash: str,
    alpha_db: Optional[Path] = None,
) -> D3ActivationPreflight:
    """Evaluate D3 prerequisites from durable facts without activation effects."""
    research = validate_new_research_run(
        policy, requested_run_id=requested_run_id, resolved_run_id=requested_run_id,
    )
    if research.mode != ADAPTIVE_CANARY_MODE or research.adaptive_control != ADAPTIVE_ARMED:
        raise ValueError("D3_PREFLIGHT_REQUIRES_ARMED_ADAPTIVE_CANARY")

    checks: dict[str, bool] = {
        "run_identity": str(requested_run_id) == str(research.expected_run_id),
        "baseline_commit": str(current_baseline_commit or "").lower() == str(research.baseline_commit or "").lower(),
        "machine_hash": str(current_machine_hash or "").upper() == str(research.expected_machine_hash or "").upper(),
        "runner_db_integrity": _sqlite_integrity_read_only(Path(store.path)),
        "alpha_db_integrity": True if alpha_db is None else _sqlite_integrity_read_only(Path(alpha_db)),
    }
    expected_scheduler_hash = shadow_policy_hash(policy_from_mapping(dict(policy.get("scheduler_shadow") or {})))
    expected_evidence_hash = evidence_policy_hash(dict(policy.get("scheduler_evidence") or {}))

    gate_row = None
    d2_round = None
    d3_round = None
    active_d2_batches = 0
    with store.connect() as conn:
        d2_round_raw = conn.execute(
            "SELECT * FROM ppl_rounds WHERE run_id=?", (research.d2_source_run_id,),
        ).fetchone()
        d2_round = dict(d2_round_raw) if d2_round_raw else None
        d3_round_raw = conn.execute(
            "SELECT * FROM ppl_rounds WHERE run_id=?", (requested_run_id,),
        ).fetchone()
        d3_round = dict(d3_round_raw) if d3_round_raw else None
        if d2_round:
            active_d2_batches = int(conn.execute(
                """SELECT COUNT(*) FROM ppl_round_batches
                   WHERE round_id=? AND status NOT IN ('COMPLETED','RECOVERED','RECOVERED_PRE_DISPATCH')""",
                (d2_round["round_id"],),
            ).fetchone()[0])
        gate_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_scheduler_gate_reports'"
        ).fetchone()
        if gate_table:
            raw = conn.execute(
                """SELECT * FROM ppl_round_scheduler_gate_reports
                   WHERE report_key=? AND run_id=?""",
                (research.d2_gate_report_key, research.d2_source_run_id),
            ).fetchone()
            gate_row = dict(raw) if raw else None

    checks["d2_round_frozen"] = bool(
        d2_round and str(d2_round.get("status") or "").upper() == "PAUSED" and active_d2_batches == 0
    )
    d2_identity = {}
    if d2_round:
        try:
            d2_identity = json.loads(str(d2_round.get("config_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            d2_identity = {}
    d2_research = dict(d2_identity.get("research_run") or {})
    checks["d2_identity"] = bool(
        str(d2_research.get("mode") or "").upper() == "COMPATIBILITY_EVIDENCE"
        and str(d2_research.get("scheduler_authority") or "").upper() == "PHASE_COMPATIBILITY"
        and str(d2_research.get("adaptive_control") or "").upper() == "DISABLED"
    )
    checks["d3_run_not_conflicting"] = d3_round is None or str(d3_round.get("config_hash") or "") == _policy_hash(policy)
    checks["d2_gate_present"] = gate_row is not None
    checks["d2_gate_eligible"] = bool(
        gate_row
        and int(gate_row.get("eligible") or 0) == 1
        and str(gate_row.get("status") or "") == "ELIGIBLE_FOR_FUTURE_CANARY_REVIEW"
    )
    checks["d2_gate_scheduler_policy"] = bool(
        gate_row and str(gate_row.get("scheduler_policy_hash") or "") == expected_scheduler_hash
    )
    checks["d2_gate_evidence_policy"] = bool(
        gate_row and str(gate_row.get("evidence_policy_hash") or "") == expected_evidence_hash
    )

    gate_payload: dict[str, Any] = {}
    if gate_row:
        try:
            gate_payload = json.loads(str(gate_row.get("report_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            gate_payload = {}
    gate_checks = dict(gate_payload.get("checks") or {})
    for name in REQUIRED_D2_GATE_CHECKS:
        checks[f"d2_gate_{name}"] = bool(gate_checks.get(name))

    eligible = all(checks.values())
    if eligible:
        status = "D3_CANARY_ARMED_PREFLIGHT_PASS"
    else:
        failed = [name for name, passed in checks.items() if not passed]
        status = "D3_CANARY_ARMED_PREFLIGHT_FAIL:" + ",".join(failed)
    return D3ActivationPreflight(
        eligible=eligible,
        status=status,
        checks=checks,
        evidence={
            "run_id": requested_run_id,
            "d2_source_run_id": research.d2_source_run_id,
            "d2_round_id": d2_round.get("round_id") if d2_round else None,
            "d2_gate_report_key": research.d2_gate_report_key,
            "scheduler_policy_hash": expected_scheduler_hash,
            "evidence_policy_hash": expected_evidence_hash,
            "baseline_commit": str(current_baseline_commit or "").lower(),
            "machine_hash": str(current_machine_hash or "").upper(),
            "authority_transition_performed": False,
            "database_writes": 0,
        },
    )


def _policy_hash(policy: Mapping[str, Any]) -> str:
    import hashlib
    body = json.dumps(dict(policy), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def assert_d3_activation_preflight(result: D3ActivationPreflight) -> None:
    """Fail closed while keeping activation as a separate future operation."""
    if not result.eligible:
        raise RuntimeError(result.status)
