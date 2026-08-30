"""V3.1-D2 Scheduler Evidence Gate.

This module is deliberately evidence-only.  It records immutable scheduler
facts, evaluates only outcomes that actually occurred, provides deterministic
replay and activation-eligibility evidence, and never changes Search/Repair
execution.  Unexecuted alternatives are explicitly labelled counterfactual
proxies rather than observed outcomes.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Mapping, Optional, Sequence

from .scheduler_shadow import (
    ProductivityMetrics,
    QueueFacts,
    ResearchAvailabilityFacts,
    ShadowSchedulerDecision,
    ShadowSchedulerPolicy,
    ShadowSchedulerSnapshot,
    choose_shadow_action,
    is_matured_productivity_row,
    shadow_policy_hash,
)
from .strategy_contracts import SchedulerActionType


EVIDENCE_ONLY_MODE = "EVIDENCE_ONLY"
DEFAULT_EVIDENCE_POLICY_VERSION = "V31_SCHED_EVIDENCE_003"
COUNTERFACTUAL_PROXY_KIND = "COUNTERFACTUAL_PROXY"

_FAIL_CLOSED_FALLBACK_REASONS = {
    "DUPLICATE_POST_RISK",
    "DURABLE_IDENTITY_CONFLICT",
    "DB_CORRUPTION",
    "CORE_INVARIANT_FAILURE",
    "SIM_KEY_IDENTITY_CONFLICT",
}
_PHASE_COMPATIBILITY_FALLBACK_REASONS = {
    "SCHEDULER_EXCEPTION",
    "INVALID_DECISION",
    "POLICY_MISMATCH",
    "SLOT_CONFLICT",
    "MISSING_IDENTITY",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _upper(value: Any) -> str:
    return str(value or "").upper()


@dataclass(frozen=True)
class EvidencePolicy:
    policy_version: str = DEFAULT_EVIDENCE_POLICY_VERSION
    replay_repetitions: int = 5
    minimum_observations: Optional[int] = None
    minimum_search_samples: Optional[int] = None
    minimum_repair_samples: Optional[int] = None


@dataclass(frozen=True)
class ReplayReport:
    repetitions: int
    passed: bool
    decision_hash: str
    hashes: tuple[str, ...]
    scheduler_policy_version: str
    scheduler_policy_hash: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SafetyGateReport:
    eligible: bool
    status: str
    scheduler_policy_version: str
    scheduler_policy_hash: str
    evidence_policy_version: str
    evidence_policy_hash: str
    observation_count: int
    search_samples: int
    repair_samples: int
    checks: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "status": self.status,
            "scheduler_policy_version": self.scheduler_policy_version,
            "scheduler_policy_hash": self.scheduler_policy_hash,
            "evidence_policy_version": self.evidence_policy_version,
            "evidence_policy_hash": self.evidence_policy_hash,
            "observation_count": self.observation_count,
            "search_samples": self.search_samples,
            "repair_samples": self.repair_samples,
            "checks": dict(self.checks),
            "authoritative": False,
            "activation_side_effect": False,
        }


def evidence_policy_from_mapping(raw: Mapping[str, Any]) -> EvidencePolicy:
    data = dict(raw or {})
    mode = str(data.get("mode") or EVIDENCE_ONLY_MODE)
    if mode != EVIDENCE_ONLY_MODE:
        raise ValueError("SCHEDULER_EVIDENCE_MODE_UNSUPPORTED")
    version = str(data.get("evidence_policy_version") or DEFAULT_EVIDENCE_POLICY_VERSION).strip()
    if not version:
        raise ValueError("SCHEDULER_EVIDENCE_POLICY_VERSION_REQUIRED")
    replay = int(data.get("replay_repetitions") or 5)
    if replay < 2:
        raise ValueError("SCHEDULER_EVIDENCE_REPLAY_REPETITIONS_INVALID")
    thresholds = dict(data.get("activation_thresholds") or {})

    def optional_nonnegative(name: str) -> Optional[int]:
        value = thresholds.get(name)
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 0:
            raise ValueError(f"SCHEDULER_EVIDENCE_{name.upper()}_INVALID")
        return parsed

    return EvidencePolicy(
        policy_version=version,
        replay_repetitions=replay,
        minimum_observations=optional_nonnegative("minimum_observations"),
        minimum_search_samples=optional_nonnegative("minimum_search_samples"),
        minimum_repair_samples=optional_nonnegative("minimum_repair_samples"),
    )


def evidence_policy_hash(raw: Mapping[str, Any]) -> str:
    policy = evidence_policy_from_mapping(raw)
    return _hash(asdict(policy))


def deterministic_replay(
    snapshot: ShadowSchedulerSnapshot,
    policy: ShadowSchedulerPolicy,
    *,
    repetitions: int = 5,
) -> ReplayReport:
    count = max(2, int(repetitions))
    hashes: list[str] = []
    for _ in range(count):
        decision = choose_shadow_action(snapshot, policy)
        hashes.append(_hash(decision.as_dict()))
    return ReplayReport(
        repetitions=count,
        passed=len(set(hashes)) == 1,
        decision_hash=hashes[0],
        hashes=tuple(hashes),
        scheduler_policy_version=policy.policy_version,
        scheduler_policy_hash=shadow_policy_hash(policy),
    )


def snapshot_from_frozen_facts(facts: Mapping[str, Any]) -> ShadowSchedulerSnapshot:
    """Rebuild the pure Scheduler snapshot from a durable Evaluation Ledger fact set."""
    def metrics(items: Sequence[Mapping[str, Any]]) -> tuple[ProductivityMetrics, ...]:
        out = []
        for item in items:
            data = dict(item)
            data["action"] = SchedulerActionType(str(data.get("action") or "WAIT").upper())
            out.append(ProductivityMetrics(**data))
        return tuple(out)

    consecutive = facts.get("fairness_state", {}).get("consecutive_action") if isinstance(facts.get("fairness_state"), Mapping) else None

    def availability(name: str, raw_backlog: int) -> ResearchAvailabilityFacts:
        payload = facts.get(f"{name}_availability")
        if isinstance(payload, Mapping):
            return ResearchAvailabilityFacts(**dict(payload))
        # V31_SCHED_EVIDENCE_001 rows retain their original raw-backlog replay
        # semantics.  The durable row itself is never rewritten or relabelled.
        slots = int(facts.get("remote_slots_free") or 0)
        return ResearchAvailabilityFacts(
            raw_backlog_count=raw_backlog, selector_eligible_count=raw_backlog,
            preview_safe_count=raw_backlog, execution_eligible_count=raw_backlog,
            evaluation_complete=True, reason="LEGACY_RAW_BACKLOG_SEMANTICS",
            remote_slots_free=slots,
            immediately_dispatchable_count=min(raw_backlog, slots),
        )

    search_raw = int(facts.get("search_backlog") or 0)
    repair_raw = int(facts.get("repair_backlog") or 0)
    return ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType(str(facts.get("actual_action") or "WAIT").upper()),
        search_queue=QueueFacts(
            backlog=search_raw,
            oldest_age_seconds=float(facts.get("search_queue_age_seconds") or 0.0),
        ),
        repair_queue=QueueFacts(
            backlog=repair_raw,
            oldest_age_seconds=float(facts.get("repair_queue_age_seconds") or 0.0),
        ),
        search_availability=availability("search", search_raw),
        repair_availability=availability("repair", repair_raw),
        search_productivity=metrics(facts.get("search_productivity") or []),
        repair_productivity=metrics(facts.get("repair_productivity") or []),
        remote_slot_limit=int(facts.get("remote_slot_limit") or 0),
        remote_slots_reserved=int(facts.get("remote_slots_reserved") or 0),
        consecutive_action=SchedulerActionType(str(consecutive).upper()) if consecutive else None,
        consecutive_count=int(facts.get("fairness_state", {}).get("consecutive_count") or 0) if isinstance(facts.get("fairness_state"), Mapping) else 0,
    )


def replay_evaluation_record(
    evaluation: Mapping[str, Any],
    policy: ShadowSchedulerPolicy,
    *,
    repetitions: int = 5,
) -> ReplayReport:
    """Replay an immutable durable Evaluation Ledger row under the same policy identity."""
    expected_hash = str(evaluation.get("scheduler_policy_hash") or "")
    actual_hash = shadow_policy_hash(policy)
    if not expected_hash or expected_hash != actual_hash:
        raise ValueError("SCHEDULER_REPLAY_POLICY_IDENTITY_MISMATCH")
    raw = evaluation.get("facts_json")
    facts = json.loads(str(raw)) if isinstance(raw, str) else dict(raw or {})
    snapshot = snapshot_from_frozen_facts(facts)
    return deterministic_replay(snapshot, policy, repetitions=repetitions)


def starvation_stress_report(policy: ShadowSchedulerPolicy) -> dict[str, Any]:
    """Pure D2 stress harness for fairness/slot safety; never controls execution."""
    def run(dominant: SchedulerActionType, steps: int = 12) -> list[str]:
        sequence: list[str] = []
        consecutive_action: Optional[SchedulerActionType] = None
        consecutive_count = 0
        for _ in range(steps):
            search_score = 100.0 if dominant is SchedulerActionType.SEARCH else 0.0
            repair_score = 100.0 if dominant is SchedulerActionType.REPAIR else 0.0
            snap = ShadowSchedulerSnapshot(
                actual_action=dominant,
                search_queue=QueueFacts(backlog=100 if dominant is SchedulerActionType.SEARCH else 1),
                repair_queue=QueueFacts(backlog=100 if dominant is SchedulerActionType.REPAIR else 1),
                search_availability=ResearchAvailabilityFacts(
                    raw_backlog_count=100 if dominant is SchedulerActionType.SEARCH else 1,
                    selector_eligible_count=1,preview_safe_count=1,execution_eligible_count=1,
                    evaluation_complete=True,reason="STRESS",remote_slots_free=1,
                    immediately_dispatchable_count=1,
                ),
                repair_availability=ResearchAvailabilityFacts(
                    raw_backlog_count=100 if dominant is SchedulerActionType.REPAIR else 1,
                    selector_eligible_count=1,preview_safe_count=1,execution_eligible_count=1,
                    evaluation_complete=True,reason="STRESS",remote_slots_free=1,
                    immediately_dispatchable_count=1,
                ),
                search_productivity=(ProductivityMetrics(action=SchedulerActionType.SEARCH, window=100, attempts=100, score=search_score),),
                repair_productivity=(ProductivityMetrics(action=SchedulerActionType.REPAIR, window=100, attempts=100, score=repair_score),),
                remote_slot_limit=1,remote_slots_reserved=0,
                consecutive_action=consecutive_action,consecutive_count=consecutive_count,
            )
            chosen = choose_shadow_action(snap, policy).shadow_action
            sequence.append(chosen.value)
            if chosen is consecutive_action:
                consecutive_count += 1
            else:
                consecutive_action = chosen
                consecutive_count = 1
        return sequence

    search_dominant = run(SchedulerActionType.SEARCH)
    repair_dominant = run(SchedulerActionType.REPAIR)
    full_slot = choose_shadow_action(
        ShadowSchedulerSnapshot(
            actual_action=SchedulerActionType.SEARCH,
            search_queue=QueueFacts(backlog=1),repair_queue=QueueFacts(backlog=1),
            search_availability=ResearchAvailabilityFacts(
                raw_backlog_count=1,selector_eligible_count=1,preview_safe_count=1,
                execution_eligible_count=1,evaluation_complete=True,reason="STRESS",
                remote_slots_free=0,immediately_dispatchable_count=0,
            ),
            repair_availability=ResearchAvailabilityFacts(
                raw_backlog_count=1,selector_eligible_count=1,preview_safe_count=1,
                execution_eligible_count=1,evaluation_complete=True,reason="STRESS",
                remote_slots_free=0,immediately_dispatchable_count=0,
            ),
            remote_slot_limit=1,remote_slots_reserved=1,
        ),
        policy,
    )
    return {
        "search_dominant_sequence": search_dominant,
        "repair_dominant_sequence": repair_dominant,
        "search_not_starved": "SEARCH" in repair_dominant,
        "repair_not_starved": "REPAIR" in search_dominant,
        "slot_one_full_wait": full_slot.shadow_action is SchedulerActionType.WAIT,
        "remote_running_capacity_pressure_modeled_only_as_slot_reservation": True,
        "rate_limit_wait_is_runtime_obligation_not_value_input": True,
        "check_backlog_is_runtime_obligation_not_value_input": True,
        "discovery_wait_is_runtime_obligation_not_value_input": True,
        "passed": (
            "SEARCH" in repair_dominant
            and "REPAIR" in search_dominant
            and full_slot.shadow_action is SchedulerActionType.WAIT
        ),
        "authoritative": False,
    }


def fallback_disposition(reason: str) -> str:
    """Classify a future adaptive-scheduler failure without performing fallback."""
    code = _upper(reason)
    if code in _FAIL_CLOSED_FALLBACK_REASONS:
        return "FAIL_CLOSED_GLOBAL_HALT"
    if code in _PHASE_COMPATIBILITY_FALLBACK_REASONS:
        return "PHASE_COMPATIBILITY"
    return "PHASE_COMPATIBILITY"


def ensure_scheduler_evidence_schema(store: Any) -> None:
    """Create additive D2 evidence tables without changing ROUND_SCHEMA_VERSION."""
    with store.connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ppl_round_scheduler_evaluations(
                decision_key TEXT PRIMARY KEY,
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                batch_no INTEGER NOT NULL,
                decision_timestamp TEXT NOT NULL,
                actual_action TEXT NOT NULL,
                shadow_action TEXT NOT NULL,
                agreement INTEGER NOT NULL,
                authoritative INTEGER NOT NULL DEFAULT 0,
                scheduler_policy_version TEXT NOT NULL,
                scheduler_policy_hash TEXT NOT NULL,
                replay_pass INTEGER NOT NULL DEFAULT 0,
                replay_decision_hash TEXT,
                replay_repetitions INTEGER NOT NULL DEFAULT 0,
                evidence_policy_version TEXT NOT NULL,
                evidence_policy_hash TEXT NOT NULL,
                search_backlog INTEGER NOT NULL,
                repair_backlog INTEGER NOT NULL,
                search_selector_eligible_count INTEGER,
                search_preview_safe_count INTEGER,
                search_execution_eligible_count INTEGER,
                search_evaluation_complete INTEGER,
                search_availability_reason TEXT,
                search_immediately_dispatchable_count INTEGER,
                repair_selector_eligible_count INTEGER,
                repair_preview_safe_count INTEGER,
                repair_execution_eligible_count INTEGER,
                repair_evaluation_complete INTEGER,
                repair_availability_reason TEXT,
                repair_immediately_dispatchable_count INTEGER,
                search_queue_age_seconds REAL NOT NULL,
                repair_queue_age_seconds REAL NOT NULL,
                search_productivity_json TEXT NOT NULL,
                repair_productivity_json TEXT NOT NULL,
                remote_slot_limit INTEGER NOT NULL,
                remote_slots_reserved INTEGER NOT NULL,
                remote_slots_free INTEGER NOT NULL,
                fairness_state_json TEXT NOT NULL,
                consecutive_action TEXT,
                consecutive_count INTEGER NOT NULL,
                selected_count INTEGER NOT NULL,
                selection_fingerprint TEXT NOT NULL,
                search_score REAL NOT NULL,
                repair_score REAL NOT NULL,
                shadow_score REAL NOT NULL,
                decision_reason TEXT NOT NULL,
                facts_json TEXT NOT NULL,
                decision_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(round_id,batch_no,actual_action,scheduler_policy_hash,selection_fingerprint)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_scheduler_outcomes(
                decision_key TEXT PRIMARY KEY REFERENCES ppl_round_scheduler_evaluations(decision_key),
                round_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                batch_no INTEGER NOT NULL,
                actual_action TEXT NOT NULL,
                outcome_state TEXT NOT NULL,
                total_new_posts INTEGER NOT NULL,
                matured_new_posts INTEGER NOT NULL,
                censored_new_posts INTEGER NOT NULL,
                complete_count INTEGER NOT NULL,
                ready_count INTEGER NOT NULL,
                near_pass_count INTEGER NOT NULL,
                distinct_family_count INTEGER NOT NULL,
                family_winner_count INTEGER NOT NULL,
                repair_resolved_count INTEGER NOT NULL,
                repair_target_pass_count INTEGER NOT NULL,
                repair_improved_count INTEGER NOT NULL,
                repair_accept_count INTEGER NOT NULL,
                effective_simulation_count INTEGER NOT NULL,
                effective_simulation_ratio REAL,
                counterfactual_kind TEXT,
                counterfactual_proxy_json TEXT,
                outcome_json TEXT NOT NULL,
                matured_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ppl_round_scheduler_gate_reports(
                report_key TEXT PRIMARY KEY,
                round_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                scheduler_policy_version TEXT NOT NULL,
                scheduler_policy_hash TEXT NOT NULL,
                evidence_policy_version TEXT NOT NULL,
                evidence_policy_hash TEXT NOT NULL,
                eligible INTEGER NOT NULL,
                status TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                search_samples INTEGER NOT NULL,
                repair_samples INTEGER NOT NULL,
                report_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sched_eval_round_batch
              ON ppl_round_scheduler_evaluations(round_id,batch_no,decision_timestamp);
            CREATE INDEX IF NOT EXISTS idx_sched_outcome_round_state
              ON ppl_round_scheduler_outcomes(round_id,outcome_state,batch_no);
            CREATE INDEX IF NOT EXISTS idx_sched_gate_round_time
              ON ppl_round_scheduler_gate_reports(round_id,created_at);
            """
        )
        columns = {str(r[1]) for r in conn.execute("PRAGMA table_info(ppl_round_scheduler_evaluations)")}
        if "replay_pass" not in columns:
            conn.execute("ALTER TABLE ppl_round_scheduler_evaluations ADD COLUMN replay_pass INTEGER NOT NULL DEFAULT 0")
        if "replay_decision_hash" not in columns:
            conn.execute("ALTER TABLE ppl_round_scheduler_evaluations ADD COLUMN replay_decision_hash TEXT")
        if "replay_repetitions" not in columns:
            conn.execute("ALTER TABLE ppl_round_scheduler_evaluations ADD COLUMN replay_repetitions INTEGER NOT NULL DEFAULT 0")
        availability_columns = {
            "search_selector_eligible_count": "INTEGER",
            "search_preview_safe_count": "INTEGER",
            "search_execution_eligible_count": "INTEGER",
            "search_evaluation_complete": "INTEGER",
            "search_availability_reason": "TEXT",
            "search_immediately_dispatchable_count": "INTEGER",
            "repair_selector_eligible_count": "INTEGER",
            "repair_preview_safe_count": "INTEGER",
            "repair_execution_eligible_count": "INTEGER",
            "repair_evaluation_complete": "INTEGER",
            "repair_availability_reason": "TEXT",
            "repair_immediately_dispatchable_count": "INTEGER",
        }
        columns = {str(r[1]) for r in conn.execute("PRAGMA table_info(ppl_round_scheduler_evaluations)")}
        for name, declaration in availability_columns.items():
            if name not in columns:
                conn.execute(f"ALTER TABLE ppl_round_scheduler_evaluations ADD COLUMN {name} {declaration}")


def _decision_key(
    *, round_id: str, run_id: str, batch_no: int, actual_action: str,
    scheduler_policy_hash_value: str, selection_fingerprint: str,
) -> str:
    return hashlib.sha256(
        f"{round_id}|{run_id}|{int(batch_no)}|{actual_action}|{scheduler_policy_hash_value}|{selection_fingerprint}".encode("utf-8")
    ).hexdigest()


def record_scheduler_evaluation(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    batch_no: int,
    snapshot: ShadowSchedulerSnapshot,
    decision: ShadowSchedulerDecision,
    scheduler_policy: ShadowSchedulerPolicy,
    evidence_raw: Mapping[str, Any],
    selected_count: int,
    selection_fingerprint: str,
    decision_timestamp: Optional[str] = None,
    replay_report: Optional[ReplayReport] = None,
) -> dict[str, Any]:
    ensure_scheduler_evidence_schema(store)
    evidence = evidence_policy_from_mapping(evidence_raw)
    sched_hash = shadow_policy_hash(scheduler_policy)
    evid_hash = evidence_policy_hash(evidence_raw)
    timestamp = decision_timestamp or _now()
    replay = replay_report or deterministic_replay(snapshot, scheduler_policy, repetitions=evidence.replay_repetitions)
    key = _decision_key(
        round_id=round_id, run_id=run_id, batch_no=batch_no,
        actual_action=decision.actual_action.value,
        scheduler_policy_hash_value=sched_hash,
        selection_fingerprint=selection_fingerprint,
    )
    search_prod = [x.as_dict() for x in snapshot.search_productivity]
    repair_prod = [x.as_dict() for x in snapshot.repair_productivity]
    search_availability = snapshot.search_availability.as_dict()
    repair_availability = snapshot.repair_availability.as_dict()
    fairness = {
        "max_consecutive_same_action": int(scheduler_policy.max_consecutive_same_action),
        "hard_starvation_guard": True,
        "consecutive_action": snapshot.consecutive_action.value if snapshot.consecutive_action else None,
        "consecutive_count": int(snapshot.consecutive_count),
    }
    facts = {
        "actual_action": snapshot.actual_action.value,
        "search_backlog": int(snapshot.search_queue.backlog),
        "repair_backlog": int(snapshot.repair_queue.backlog),
        "search_availability": search_availability,
        "repair_availability": repair_availability,
        "search_queue_age_seconds": float(snapshot.search_queue.oldest_age_seconds),
        "repair_queue_age_seconds": float(snapshot.repair_queue.oldest_age_seconds),
        "search_productivity": search_prod,
        "repair_productivity": repair_prod,
        "remote_slot_limit": int(snapshot.remote_slot_limit),
        "remote_slots_reserved": int(snapshot.remote_slots_reserved),
        "remote_slots_free": int(snapshot.free_remote_slots),
        "fairness_state": fairness,
        "selected_count": int(selected_count),
        "selection_fingerprint": selection_fingerprint,
    }
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_scheduler_evaluations(
                   decision_key,round_id,run_id,batch_no,decision_timestamp,actual_action,shadow_action,
                   agreement,authoritative,scheduler_policy_version,scheduler_policy_hash,
                   replay_pass,replay_decision_hash,replay_repetitions,
                   evidence_policy_version,evidence_policy_hash,search_backlog,repair_backlog,
                   search_selector_eligible_count,search_preview_safe_count,search_execution_eligible_count,
                   search_evaluation_complete,search_availability_reason,search_immediately_dispatchable_count,
                   repair_selector_eligible_count,repair_preview_safe_count,repair_execution_eligible_count,
                   repair_evaluation_complete,repair_availability_reason,repair_immediately_dispatchable_count,
                   search_queue_age_seconds,repair_queue_age_seconds,search_productivity_json,
                   repair_productivity_json,remote_slot_limit,remote_slots_reserved,remote_slots_free,
                   fairness_state_json,consecutive_action,consecutive_count,selected_count,
                   selection_fingerprint,search_score,repair_score,shadow_score,decision_reason,
                   facts_json,decision_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(decision_key) DO UPDATE SET updated_at=excluded.updated_at""",
            (
                key,round_id,run_id,int(batch_no),timestamp,decision.actual_action.value,decision.shadow_action.value,
                1 if decision.agreement else 0,0,scheduler_policy.policy_version,sched_hash,
                1 if replay.passed else 0,replay.decision_hash,int(replay.repetitions),
                evidence.policy_version,evid_hash,int(snapshot.search_queue.backlog),int(snapshot.repair_queue.backlog),
                int(search_availability["selector_eligible_count"]),int(search_availability["preview_safe_count"]),
                int(search_availability["execution_eligible_count"]),1 if search_availability["evaluation_complete"] else 0,
                str(search_availability["reason"]),int(search_availability["immediately_dispatchable_count"]),
                int(repair_availability["selector_eligible_count"]),int(repair_availability["preview_safe_count"]),
                int(repair_availability["execution_eligible_count"]),1 if repair_availability["evaluation_complete"] else 0,
                str(repair_availability["reason"]),int(repair_availability["immediately_dispatchable_count"]),
                float(snapshot.search_queue.oldest_age_seconds),float(snapshot.repair_queue.oldest_age_seconds),
                _json(search_prod),_json(repair_prod),int(snapshot.remote_slot_limit),int(snapshot.remote_slots_reserved),
                int(snapshot.free_remote_slots),_json(fairness),
                snapshot.consecutive_action.value if snapshot.consecutive_action else None,int(snapshot.consecutive_count),
                int(selected_count),selection_fingerprint,float(decision.search_score),float(decision.repair_score),
                float(decision.shadow_score),str(decision.reason),_json(facts),_json(decision.as_dict()),now,now,
            ),
        )
    with store.connect() as conn:
        durable = conn.execute(
            """SELECT decision_timestamp,scheduler_policy_version,scheduler_policy_hash,
                      evidence_policy_version,evidence_policy_hash,replay_pass,replay_decision_hash,replay_repetitions
               FROM ppl_round_scheduler_evaluations WHERE decision_key=?""",
            (key,),
        ).fetchone()
    return {
        "decision_key": key,
        "decision_timestamp": str(durable[0]),
        "scheduler_policy_version": str(durable[1]),
        "scheduler_policy_hash": str(durable[2]),
        "evidence_policy_version": str(durable[3]),
        "evidence_policy_hash": str(durable[4]),
        "replay_pass": bool(durable[5]),
        "replay_decision_hash": str(durable[6] or ""),
        "replay_repetitions": int(durable[7] or 0),
        "authoritative": False,
    }


def _is_ready(classification: str) -> bool:
    return classification in {
        "PPL_SUCCESS", "PPL_TECHNICALLY_READY", "PPL_READY_FOR_MANUAL_FINALIZATION",
        "READY_FOR_MANUAL_FINALIZATION",
    }


def _is_near_pass(classification: str) -> bool:
    return classification in {"STRONG_NEAR_PASS", "NEAR_PASS"} or "REPAIRABLE" in classification


def _outcome_for_decision(store: Any, evaluation: Mapping[str, Any]) -> dict[str, Any]:
    action = SchedulerActionType(str(evaluation["actual_action"]).upper())
    round_id = str(evaluation["round_id"])
    batch_no = int(evaluation["batch_no"])
    with store.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT * FROM ppl_round_simulation_ledger
               WHERE round_id=? AND batch_no=? AND upper(coalesce(phase,''))=?
                 AND upper(coalesce(origin,''))='NEW_POST'
               ORDER BY logical_sequence_no,sim_key""",
            (round_id,batch_no,action.value),
        )]
    total = len(rows)
    matured = [r for r in rows if is_matured_productivity_row(r, action)]
    censored = total - len(matured)
    complete = sum(_upper(r.get("simulation_status")) == "COMPLETE" for r in matured)
    classifications = [_upper(r.get("classification")) for r in matured]
    ready = sum(_is_ready(c) for c in classifications)
    near = sum(_is_near_pass(c) for c in classifications)
    families = {str(r.get("family_id") or "") for r in matured if str(r.get("family_id") or "")}
    family_winners = sum(_upper(r.get("family_result")) == "WINNER" for r in matured)
    verdicts = [_upper(r.get("repair_verdict")) for r in matured]
    resolved = sum(bool(v) for v in verdicts)
    target_pass = sum(v == "TARGET_PASS" for v in verdicts)
    improved = sum(v == "IMPROVED" for v in verdicts)
    accept = sum(v == "ACCEPT" for v in verdicts)
    effective = complete
    ratio = (float(effective) / float(len(matured))) if matured else None
    state = (
        "NO_NEW_POST" if total == 0
        else "MATURED" if censored == 0
        else "PARTIAL" if matured
        else "PENDING"
    )
    return {
        "outcome_state": state,
        "total_new_posts": total,
        "matured_new_posts": len(matured),
        "censored_new_posts": censored,
        "complete_count": complete,
        "ready_count": ready,
        "near_pass_count": near,
        "distinct_family_count": len(families),
        "family_winner_count": family_winners,
        "repair_resolved_count": resolved,
        "repair_target_pass_count": target_pass,
        "repair_improved_count": improved,
        "repair_accept_count": accept,
        "effective_simulation_count": effective,
        "effective_simulation_ratio": ratio,
        "effective_simulation_definition": "MATURED_NEW_POST_WITH_SIMULATION_STATUS_COMPLETE",
        "maturity_definition": (
            "terminal failed/missing simulation OR COMPLETE with resolved Search classification / Repair verdict"
        ),
    }


def refresh_scheduler_outcomes(store: Any, round_id: str, run_id: str) -> list[dict[str, Any]]:
    ensure_scheduler_evidence_schema(store)
    with store.connect() as conn:
        evaluations = [dict(r) for r in conn.execute(
            """SELECT * FROM ppl_round_scheduler_evaluations
               WHERE round_id=? AND run_id=? ORDER BY batch_no,decision_timestamp""",
            (round_id,run_id),
        )]
    updated: list[dict[str, Any]] = []
    for evaluation in evaluations:
        outcome = _outcome_for_decision(store, evaluation)
        actual = str(evaluation["actual_action"])
        shadow = str(evaluation["shadow_action"])
        proxy: Optional[dict[str, Any]] = None
        if shadow in {"SEARCH", "REPAIR"} and shadow != actual:
            selected_proxy_score = (
                float(evaluation["search_score"]) if shadow == "SEARCH" else float(evaluation["repair_score"])
            )
            proxy = {
                "kind": COUNTERFACTUAL_PROXY_KIND,
                "action": shadow,
                "observed_outcome": False,
                "proxy_score": selected_proxy_score,
                "warning": "UNEXECUTED_ALTERNATIVE_NOT_A_REAL_COUNTERFACTUAL_OUTCOME",
            }
        now = _now()
        matured_at = now if outcome["outcome_state"] == "MATURED" else None
        with store.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_round_scheduler_outcomes(
                       decision_key,round_id,run_id,batch_no,actual_action,outcome_state,total_new_posts,
                       matured_new_posts,censored_new_posts,complete_count,ready_count,near_pass_count,
                       distinct_family_count,family_winner_count,repair_resolved_count,
                       repair_target_pass_count,repair_improved_count,repair_accept_count,
                       effective_simulation_count,effective_simulation_ratio,counterfactual_kind,
                       counterfactual_proxy_json,outcome_json,matured_at,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(decision_key) DO UPDATE SET
                       outcome_state=excluded.outcome_state,total_new_posts=excluded.total_new_posts,
                       matured_new_posts=excluded.matured_new_posts,censored_new_posts=excluded.censored_new_posts,
                       complete_count=excluded.complete_count,ready_count=excluded.ready_count,
                       near_pass_count=excluded.near_pass_count,distinct_family_count=excluded.distinct_family_count,
                       family_winner_count=excluded.family_winner_count,repair_resolved_count=excluded.repair_resolved_count,
                       repair_target_pass_count=excluded.repair_target_pass_count,
                       repair_improved_count=excluded.repair_improved_count,repair_accept_count=excluded.repair_accept_count,
                       effective_simulation_count=excluded.effective_simulation_count,
                       effective_simulation_ratio=excluded.effective_simulation_ratio,
                       counterfactual_kind=excluded.counterfactual_kind,
                       counterfactual_proxy_json=excluded.counterfactual_proxy_json,
                       outcome_json=excluded.outcome_json,
                       matured_at=coalesce(ppl_round_scheduler_outcomes.matured_at,excluded.matured_at),
                       updated_at=excluded.updated_at""",
                (
                    evaluation["decision_key"],round_id,run_id,int(evaluation["batch_no"]),actual,
                    outcome["outcome_state"],outcome["total_new_posts"],outcome["matured_new_posts"],
                    outcome["censored_new_posts"],outcome["complete_count"],outcome["ready_count"],
                    outcome["near_pass_count"],outcome["distinct_family_count"],outcome["family_winner_count"],
                    outcome["repair_resolved_count"],outcome["repair_target_pass_count"],
                    outcome["repair_improved_count"],outcome["repair_accept_count"],
                    outcome["effective_simulation_count"],outcome["effective_simulation_ratio"],
                    proxy.get("kind") if proxy else None,_json(proxy) if proxy else None,
                    _json(outcome),matured_at,now,now,
                ),
            )
        updated.append({"decision_key": evaluation["decision_key"], **outcome, "counterfactual_proxy": proxy})
    return updated


def load_scheduler_evaluations(store: Any, round_id: str) -> list[dict[str, Any]]:
    ensure_scheduler_evidence_schema(store)
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_scheduler_evaluations WHERE round_id=? ORDER BY batch_no,decision_timestamp",
            (round_id,),
        )]


def load_scheduler_outcomes(store: Any, round_id: str) -> list[dict[str, Any]]:
    ensure_scheduler_evidence_schema(store)
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_scheduler_outcomes WHERE round_id=? ORDER BY batch_no,decision_key",
            (round_id,),
        )]


def build_safety_gate_report(
    *,
    scheduler_policy: ShadowSchedulerPolicy,
    evidence_raw: Mapping[str, Any],
    observation_count: int,
    search_samples: int,
    repair_samples: int,
    replay_pass: bool,
    starvation_pass: bool,
    slot_safety_pass: bool,
    no_repost_pass: bool,
    recovery_safety_pass: bool,
    policy_identity_pass: bool,
) -> SafetyGateReport:
    evidence = evidence_policy_from_mapping(evidence_raw)
    thresholds = {
        "minimum_observations": evidence.minimum_observations,
        "minimum_search_samples": evidence.minimum_search_samples,
        "minimum_repair_samples": evidence.minimum_repair_samples,
    }
    thresholds_set = all(v is not None for v in thresholds.values())
    sample_checks = {
        "minimum_observations": None if evidence.minimum_observations is None else observation_count >= evidence.minimum_observations,
        "minimum_search_samples": None if evidence.minimum_search_samples is None else search_samples >= evidence.minimum_search_samples,
        "minimum_repair_samples": None if evidence.minimum_repair_samples is None else repair_samples >= evidence.minimum_repair_samples,
    }
    checks = {
        "thresholds_configured": thresholds_set,
        "threshold_values": thresholds,
        "sample_checks": sample_checks,
        "deterministic_replay": bool(replay_pass),
        "starvation": bool(starvation_pass),
        "slot_safety": bool(slot_safety_pass),
        "no_repost": bool(no_repost_pass),
        "recovery_safety": bool(recovery_safety_pass),
        "policy_identity": bool(policy_identity_pass),
        "authoritative": False,
    }
    safety_pass = all((replay_pass, starvation_pass, slot_safety_pass, no_repost_pass, recovery_safety_pass, policy_identity_pass))
    samples_pass = thresholds_set and all(bool(x) for x in sample_checks.values())
    eligible = bool(safety_pass and samples_pass)
    if not thresholds_set:
        status = "INELIGIBLE_THRESHOLDS_UNSET"
    elif not safety_pass:
        status = "INELIGIBLE_SAFETY_EVIDENCE"
    elif not samples_pass:
        status = "INELIGIBLE_SAMPLE_EVIDENCE"
    else:
        status = "ELIGIBLE_FOR_FUTURE_CANARY_REVIEW"
    return SafetyGateReport(
        eligible=eligible,status=status,
        scheduler_policy_version=scheduler_policy.policy_version,
        scheduler_policy_hash=shadow_policy_hash(scheduler_policy),
        evidence_policy_version=evidence.policy_version,
        evidence_policy_hash=evidence_policy_hash(evidence_raw),
        observation_count=int(observation_count),search_samples=int(search_samples),repair_samples=int(repair_samples),
        checks=checks,
    )


def evaluate_durable_safety_gate(
    store: Any,
    *,
    round_id: str,
    scheduler_policy: ShadowSchedulerPolicy,
    evidence_raw: Mapping[str, Any],
    no_repost_pass: bool,
    recovery_safety_pass: bool,
) -> SafetyGateReport:
    """Build eligibility from durable Shadow evidence plus offline safety attestations.

    No-repost and recovery safety are execution-layer properties, so D2 does
    not pretend to infer them from scheduler yield data.  They must be supplied
    by the validated regression matrix.
    """
    all_evaluations = load_scheduler_evaluations(store, round_id)
    all_outcomes = load_scheduler_outcomes(store, round_id)
    current_sched_hash = shadow_policy_hash(scheduler_policy)
    current_evidence_hash = evidence_policy_hash(evidence_raw)
    evidence_policy = evidence_policy_from_mapping(evidence_raw)
    evaluations = [
        r for r in all_evaluations
        if str(r.get("scheduler_policy_hash") or "") == current_sched_hash
        and str(r.get("evidence_policy_hash") or "") == current_evidence_hash
    ]
    # Older policy versions remain valid historical evidence.  Reusing the same
    # version string with a different hash is an identity conflict and must not
    # be silently pooled with current observations.
    identity_conflict = any(
        (
            str(r.get("scheduler_policy_version") or "") == scheduler_policy.policy_version
            and str(r.get("scheduler_policy_hash") or "") != current_sched_hash
        )
        or (
            str(r.get("evidence_policy_version") or "") == evidence_policy.policy_version
            and str(r.get("evidence_policy_hash") or "") != current_evidence_hash
        )
        for r in all_evaluations
    )
    policy_identity_pass = bool(evaluations) and not identity_conflict
    replay_pass = bool(evaluations) and all(int(r.get("replay_pass") or 0) == 1 for r in evaluations)
    current_keys = {str(r.get("decision_key") or "") for r in evaluations}
    outcomes = [r for r in all_outcomes if str(r.get("decision_key") or "") in current_keys]
    stress = starvation_stress_report(scheduler_policy)
    starvation_pass = bool(stress.get("passed"))
    slot_safety_pass = bool(stress.get("slot_one_full_wait"))
    search_samples = sum(
        int(r.get("matured_new_posts") or 0) for r in outcomes if _upper(r.get("actual_action")) == "SEARCH"
    )
    repair_samples = sum(
        int(r.get("matured_new_posts") or 0) for r in outcomes if _upper(r.get("actual_action")) == "REPAIR"
    )
    return build_safety_gate_report(
        scheduler_policy=scheduler_policy,evidence_raw=evidence_raw,
        observation_count=len(evaluations),search_samples=search_samples,repair_samples=repair_samples,
        replay_pass=replay_pass,starvation_pass=starvation_pass,slot_safety_pass=slot_safety_pass,
        no_repost_pass=bool(no_repost_pass),recovery_safety_pass=bool(recovery_safety_pass),
        policy_identity_pass=policy_identity_pass,
    )


def persist_safety_gate_report(store: Any, round_id: str, run_id: str, report: SafetyGateReport) -> str:
    ensure_scheduler_evidence_schema(store)
    payload = report.as_dict()
    key = _hash({"round_id": round_id, "run_id": run_id, **payload})
    with store.connect() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO ppl_round_scheduler_gate_reports(
                   report_key,round_id,run_id,scheduler_policy_version,scheduler_policy_hash,
                   evidence_policy_version,evidence_policy_hash,eligible,status,observation_count,
                   search_samples,repair_samples,report_json,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key,round_id,run_id,report.scheduler_policy_version,report.scheduler_policy_hash,
             report.evidence_policy_version,report.evidence_policy_hash,1 if report.eligible else 0,
             report.status,report.observation_count,report.search_samples,report.repair_samples,_json(payload),_now()),
        )
    return key
