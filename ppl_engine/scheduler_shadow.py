"""V3.1-D1 adaptive scheduler shadow model.

This module is deliberately pure: it accepts immutable facts and returns a
shadow recommendation.  It never reads SQLite, performs HTTP, mutates run
state, or controls execution.  D1 is observational only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any, Mapping, Optional, Sequence, Tuple

from .strategy_contracts import SchedulerActionType


SHADOW_ONLY_MODE = "SHADOW_ONLY"
DEFAULT_SHADOW_POLICY_VERSION = "V31_SCHED_SHADOW_004"


@dataclass(frozen=True)
class ProductivityMetrics:
    action: SchedulerActionType
    window: int
    attempts: int = 0
    observed_attempts: int = 0
    censored_attempts: int = 0
    completed: int = 0
    ready: int = 0
    pretag_pass: int = 0
    local_pass: int = 0
    near_pass_proxy: int = 0
    resolved_verdicts: int = 0
    target_pass: int = 0
    improved: int = 0
    accept: int = 0
    worse: int = 0
    no_improvement: int = 0
    distinct_families: int = 0
    score: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action.value,
            "window": self.window,
            "attempts": self.attempts,
            "observed_attempts": self.observed_attempts,
            "censored_attempts": self.censored_attempts,
            "completed": self.completed,
            "ready": self.ready,
            "pretag_pass": self.pretag_pass,
            "local_pass": self.local_pass,
            "near_pass_proxy": self.near_pass_proxy,
            "resolved_verdicts": self.resolved_verdicts,
            "target_pass": self.target_pass,
            "improved": self.improved,
            "accept": self.accept,
            "worse": self.worse,
            "no_improvement": self.no_improvement,
            "distinct_families": self.distinct_families,
            "score": self.score,
        }


@dataclass(frozen=True)
class QueueFacts:
    backlog: int = 0
    oldest_age_seconds: float = 0.0


@dataclass(frozen=True)
class ResearchAvailabilityFacts:
    raw_backlog_count: int = 0
    selector_eligible_count: int = 0
    preview_safe_count: int = 0
    execution_eligible_count: int = 0
    evaluation_complete: bool = False
    reason: str = "AVAILABILITY_NOT_EVALUATED"
    remote_slots_free: int = 0
    immediately_dispatchable_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ShadowSchedulerPolicy:
    policy_version: str = DEFAULT_SHADOW_POLICY_VERSION
    productivity_windows: Tuple[int, ...] = (100, 500)
    short_window_weight: float = 0.65
    long_window_weight: float = 0.35
    backlog_weight: float = 8.0
    backlog_scale: float = 20.0
    aging_score_per_minute: float = 0.10
    max_aging_bonus: float = 12.0
    cold_start_attempts: int = 20
    cold_start_bonus: float = 6.0
    max_consecutive_same_action: int = 4
    fairness_penalty: float = 15.0


@dataclass(frozen=True)
class ShadowSchedulerSnapshot:
    actual_action: SchedulerActionType
    search_queue: QueueFacts
    repair_queue: QueueFacts
    search_availability: ResearchAvailabilityFacts = field(default_factory=ResearchAvailabilityFacts)
    repair_availability: ResearchAvailabilityFacts = field(default_factory=ResearchAvailabilityFacts)
    search_productivity: Tuple[ProductivityMetrics, ...] = ()
    repair_productivity: Tuple[ProductivityMetrics, ...] = ()
    remote_slot_limit: int = 0
    remote_slots_reserved: int = 0
    consecutive_action: Optional[SchedulerActionType] = None
    consecutive_count: int = 0

    @property
    def free_remote_slots(self) -> int:
        return max(0, int(self.remote_slot_limit) - int(self.remote_slots_reserved))


@dataclass(frozen=True)
class ShadowSchedulerDecision:
    actual_action: SchedulerActionType
    shadow_action: SchedulerActionType
    agreement: bool
    shadow_score: float
    search_score: float
    repair_score: float
    policy_version: str
    authoritative: bool = False
    execution_action_unchanged: bool = True
    reason: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "actual_action": self.actual_action.value,
            "shadow_action": self.shadow_action.value,
            "agreement": self.agreement,
            "shadow_score": self.shadow_score,
            "search_score": self.search_score,
            "repair_score": self.repair_score,
            "policy_version": self.policy_version,
            "authoritative": self.authoritative,
            "execution_action_unchanged": self.execution_action_unchanged,
            "reason": self.reason,
            "metadata": dict(self.metadata),
        }


def _upper(value: Any) -> str:
    return str(value or "").upper()


def _is_ready(classification: str) -> bool:
    return classification in {
        "READY_FOR_MANUAL_FINALIZATION",
        "PPL_TECHNICALLY_READY",
        "PPL_READY_FOR_MANUAL_FINALIZATION",
        "PPL_SUCCESS",
    }


def _is_near_pass_proxy(classification: str) -> bool:
    # The immutable simulation ledger does not preserve every historical
    # evidence_label.  D1 therefore exposes this only as a proxy, never as the
    # exact historical Near-Pass metric.
    return (
        "REPAIRABLE" in classification
        or "NEAR_PASS" in classification
        or _is_ready(classification)
    )


TERMINAL_FAILED_SIMULATION_STATUSES = {"REMOTE_NOT_FOUND", "FAILED", "INVALID", "ERROR"}


def is_matured_productivity_row(row: Mapping[str, Any], action: SchedulerActionType) -> bool:
    """Return whether a NEW_POST has enough durable outcome evidence to score.

    RUNNING/SUBMITTED/STALE_RUNNING/UNCERTAIN rows are right-censored and are
    never treated as low productivity.  COMPLETE Search rows wait for a
    resolved classification; COMPLETE Repair rows wait for a durable repair
    verdict.  Terminal failed/missing simulations are mature zero-yield facts.
    """
    status = _upper(row.get("simulation_status"))
    if status in TERMINAL_FAILED_SIMULATION_STATUSES:
        return True
    if status != "COMPLETE":
        return False
    if action is SchedulerActionType.SEARCH:
        return bool(str(row.get("classification") or "").strip())
    if action is SchedulerActionType.REPAIR:
        return bool(str(row.get("repair_verdict") or "").strip())
    return False


def _new_post_rows(
    ledger: Sequence[Mapping[str, Any]],
    action: SchedulerActionType,
) -> list[Mapping[str, Any]]:
    phase = action.value
    rows = [
        row for row in ledger
        if _upper(row.get("phase")) == phase
        and _upper(row.get("origin")) == "NEW_POST"
    ]
    rows.sort(key=lambda r: (int(r.get("logical_sequence_no") or 0), str(r.get("sim_key") or "")))
    return rows


def _take_latest_matured_new_posts(
    ledger: Sequence[Mapping[str, Any]],
    action: SchedulerActionType,
    window: int,
) -> list[Mapping[str, Any]]:
    rows = [row for row in _new_post_rows(ledger, action) if is_matured_productivity_row(row, action)]
    return rows[-max(1, int(window)):]


def shadow_policy_hash(policy: ShadowSchedulerPolicy) -> str:
    """Hash effective scheduler semantics, independent of YAML ordering."""
    payload = asdict(policy)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def productivity_for_window(
    ledger: Sequence[Mapping[str, Any]],
    action: SchedulerActionType,
    window: int,
) -> ProductivityMetrics:
    all_rows = _new_post_rows(ledger, action)
    rows = _take_latest_matured_new_posts(ledger, action, window)
    attempts = len(rows)
    observed_attempts = len(all_rows)
    censored_attempts = sum(not is_matured_productivity_row(r, action) for r in all_rows)
    completed = sum(_upper(r.get("simulation_status")) == "COMPLETE" for r in rows)
    families = {str(r.get("family_id") or "") for r in rows if str(r.get("family_id") or "")}

    if action is SchedulerActionType.SEARCH:
        ready = sum(_is_ready(_upper(r.get("classification"))) for r in rows)
        pretag_pass = sum(_upper(r.get("pretag_status")) == "PASS" for r in rows)
        local_pass = sum(_upper(r.get("local_gate")) == "PASS" for r in rows)
        near_pass_proxy = sum(_is_near_pass_proxy(_upper(r.get("classification"))) for r in rows)
        denom = max(1, attempts)
        score = (
            18.0 * completed / denom
            + 34.0 * ready / denom
            + 18.0 * pretag_pass / denom
            + 10.0 * local_pass / denom
            + 12.0 * near_pass_proxy / denom
            + 8.0 * len(families) / denom
        )
        return ProductivityMetrics(
            action=action, window=int(window), attempts=attempts, observed_attempts=observed_attempts,
            censored_attempts=censored_attempts, completed=completed,
            ready=ready, pretag_pass=pretag_pass, local_pass=local_pass,
            near_pass_proxy=near_pass_proxy, distinct_families=len(families), score=score,
        )

    verdicts = [_upper(r.get("repair_verdict")) for r in rows]
    resolved = sum(bool(v) for v in verdicts)
    target_pass = sum(v == "TARGET_PASS" for v in verdicts)
    improved = sum(v == "IMPROVED" for v in verdicts)
    accept = sum(v == "ACCEPT" for v in verdicts)
    worse = sum(v == "WORSE" for v in verdicts)
    no_improvement = sum(v == "NO_IMPROVEMENT" for v in verdicts)
    denom = max(1, attempts)
    score = (
        14.0 * completed / denom
        + 36.0 * target_pass / denom
        + 22.0 * improved / denom
        + 18.0 * accept / denom
        - 10.0 * worse / denom
        - 6.0 * no_improvement / denom
        + 6.0 * resolved / denom
        + 6.0 * len(families) / denom
    )
    return ProductivityMetrics(
        action=action, window=int(window), attempts=attempts, observed_attempts=observed_attempts,
        censored_attempts=censored_attempts, completed=completed,
        resolved_verdicts=resolved, target_pass=target_pass, improved=improved,
        accept=accept, worse=worse, no_improvement=no_improvement,
        distinct_families=len(families), score=score,
    )


def productivity_windows(
    ledger: Sequence[Mapping[str, Any]],
    action: SchedulerActionType,
    windows: Sequence[int],
) -> Tuple[ProductivityMetrics, ...]:
    return tuple(productivity_for_window(ledger, action, int(w)) for w in windows)


def _productivity_score(metrics: Sequence[ProductivityMetrics], policy: ShadowSchedulerPolicy) -> tuple[float, int]:
    if not metrics:
        return 0.0, 0
    by_window = sorted(metrics, key=lambda m: int(m.window))
    short = by_window[0]
    long = by_window[-1]
    if short.window == long.window:
        score = float(short.score)
    else:
        score = (
            float(policy.short_window_weight) * float(short.score)
            + float(policy.long_window_weight) * float(long.score)
        )
    attempts = max((int(m.attempts) for m in metrics), default=0)
    return score, attempts


def _queue_score(
    action: SchedulerActionType,
    queue: QueueFacts,
    availability: ResearchAvailabilityFacts,
    productivity: Sequence[ProductivityMetrics],
    snapshot: ShadowSchedulerSnapshot,
    policy: ShadowSchedulerPolicy,
) -> float:
    if not availability.evaluation_complete or int(availability.execution_eligible_count) <= 0:
        return -1_000_000_000.0
    score, attempts = _productivity_score(productivity, policy)
    backlog_ratio = min(
        1.0,
        max(0.0, float(availability.execution_eligible_count)) / max(1.0, float(policy.backlog_scale)),
    )
    score += float(policy.backlog_weight) * backlog_ratio
    age_bonus = max(0.0, float(queue.oldest_age_seconds)) / 60.0 * float(policy.aging_score_per_minute)
    score += min(float(policy.max_aging_bonus), age_bonus)
    if attempts < int(policy.cold_start_attempts):
        score += float(policy.cold_start_bonus)
    if (
        snapshot.consecutive_action is action
        and int(snapshot.consecutive_count) >= int(policy.max_consecutive_same_action)
    ):
        score -= float(policy.fairness_penalty)
    return score


def choose_shadow_action(
    snapshot: ShadowSchedulerSnapshot,
    policy: ShadowSchedulerPolicy = ShadowSchedulerPolicy(),
) -> ShadowSchedulerDecision:
    """Return an observational recommendation without controlling execution."""
    search_score = _queue_score(
        SchedulerActionType.SEARCH, snapshot.search_queue, snapshot.search_availability,
        snapshot.search_productivity, snapshot, policy,
    )
    repair_score = _queue_score(
        SchedulerActionType.REPAIR, snapshot.repair_queue, snapshot.repair_availability,
        snapshot.repair_productivity, snapshot, policy,
    )

    search_available = bool(
        snapshot.search_availability.evaluation_complete
        and int(snapshot.search_availability.execution_eligible_count) > 0
    )
    repair_available = bool(
        snapshot.repair_availability.evaluation_complete
        and int(snapshot.repair_availability.execution_eligible_count) > 0
    )

    if snapshot.free_remote_slots <= 0 and (search_available or repair_available):
        shadow = SchedulerActionType.WAIT
        selected_score = 0.0
        reason = "SHADOW_WAIT_SERVER_SLOT"
    elif not search_available and not repair_available:
        shadow = SchedulerActionType.WAIT
        selected_score = 0.0
        if snapshot.repair_queue.backlog > 0 and snapshot.search_queue.backlog <= 0:
            reason = "NO_EXECUTABLE_REPAIR_EVIDENCE"
        elif snapshot.search_queue.backlog > 0 and snapshot.repair_queue.backlog <= 0:
            reason = "NO_EXECUTABLE_SEARCH_EVIDENCE"
        else:
            reason = "SHADOW_NO_EXECUTABLE_RESEARCH_WORK"
    elif (
        search_available
        and repair_available
        and snapshot.free_remote_slots > 0
        and snapshot.consecutive_action in {SchedulerActionType.SEARCH, SchedulerActionType.REPAIR}
        and int(snapshot.consecutive_count) >= int(policy.max_consecutive_same_action)
    ):
        shadow = (
            SchedulerActionType.REPAIR
            if snapshot.consecutive_action is SchedulerActionType.SEARCH
            else SchedulerActionType.SEARCH
        )
        selected_score = repair_score if shadow is SchedulerActionType.REPAIR else search_score
        reason = "SHADOW_HARD_STARVATION_GUARD"
    elif repair_available and repair_score > search_score:
        shadow = SchedulerActionType.REPAIR
        selected_score = repair_score
        reason = "SHADOW_REPAIR_HIGHER_VALUE"
    elif search_available:
        shadow = SchedulerActionType.SEARCH
        selected_score = search_score
        reason = (
            "NO_EXECUTABLE_REPAIR_EVIDENCE"
            if snapshot.repair_queue.backlog > 0 and not repair_available
            else "SHADOW_SEARCH_HIGHER_OR_EQUAL_VALUE"
        )
    else:
        shadow = SchedulerActionType.REPAIR
        selected_score = repair_score
        reason = (
            "NO_EXECUTABLE_SEARCH_EVIDENCE"
            if snapshot.search_queue.backlog > 0 and not search_available
            else "SHADOW_ONLY_REPAIR_EXECUTABLE"
        )

    return ShadowSchedulerDecision(
        actual_action=snapshot.actual_action,
        shadow_action=shadow,
        agreement=(shadow is snapshot.actual_action),
        shadow_score=float(selected_score),
        search_score=float(search_score),
        repair_score=float(repair_score),
        policy_version=str(policy.policy_version),
        authoritative=False,
        execution_action_unchanged=True,
        reason=reason,
        metadata={
            "search_backlog": int(snapshot.search_queue.backlog),
            "search_oldest_age_seconds": float(snapshot.search_queue.oldest_age_seconds),
            "repair_backlog": int(snapshot.repair_queue.backlog),
            "search_availability": snapshot.search_availability.as_dict(),
            "repair_availability": snapshot.repair_availability.as_dict(),
            "repair_oldest_age_seconds": float(snapshot.repair_queue.oldest_age_seconds),
            "remote_slot_limit": int(snapshot.remote_slot_limit),
            "remote_slots_reserved": int(snapshot.remote_slots_reserved),
            "remote_slots_free": int(snapshot.free_remote_slots),
            "consecutive_action": snapshot.consecutive_action.value if snapshot.consecutive_action else None,
            "consecutive_count": int(snapshot.consecutive_count),
            "search_productivity": [m.as_dict() for m in snapshot.search_productivity],
            "repair_productivity": [m.as_dict() for m in snapshot.repair_productivity],
            "near_pass_metric_kind": "PROXY_FROM_LEDGER_CLASSIFICATION",
            "productivity_maturity": "MATURED_NEW_POST_COHORT",
            "right_censored_rows_excluded_from_score": True,
            "hard_starvation_guard": True,
        },
    )


def policy_from_mapping(raw: Mapping[str, Any]) -> ShadowSchedulerPolicy:
    """Parse the D1 shadow block without inventing execution semantics."""
    data = dict(raw or {})
    windows = tuple(int(x) for x in (data.get("productivity_windows") or (100, 500)))
    if not windows or any(x <= 0 for x in windows):
        raise ValueError("SCHEDULER_SHADOW_WINDOWS_INVALID")
    if str(data.get("mode") or SHADOW_ONLY_MODE) != SHADOW_ONLY_MODE:
        raise ValueError("SCHEDULER_SHADOW_MODE_UNSUPPORTED")
    short_weight = float(data.get("short_window_weight", 0.65))
    long_weight = float(data.get("long_window_weight", 0.35))
    if short_weight < 0 or long_weight < 0 or abs(short_weight + long_weight - 1.0) > 1e-9:
        raise ValueError("SCHEDULER_SHADOW_WINDOW_WEIGHTS_INVALID")
    policy_version = str(data.get("policy_version") or DEFAULT_SHADOW_POLICY_VERSION).strip()
    backlog_weight = float(data.get("backlog_weight", 8.0))
    backlog_scale = float(data.get("backlog_scale", 20.0))
    aging_score = float(data.get("aging_score_per_minute", 0.10))
    max_aging = float(data.get("max_aging_bonus", 12.0))
    cold_attempts = int(data.get("cold_start_attempts", 20))
    cold_bonus = float(data.get("cold_start_bonus", 6.0))
    max_consecutive = int(data.get("max_consecutive_same_action", 4))
    fairness_penalty = float(data.get("fairness_penalty", 15.0))
    if not policy_version:
        raise ValueError("SCHEDULER_SHADOW_POLICY_VERSION_REQUIRED")
    if backlog_weight < 0 or backlog_scale <= 0:
        raise ValueError("SCHEDULER_SHADOW_BACKLOG_CONFIG_INVALID")
    if aging_score < 0 or max_aging < 0:
        raise ValueError("SCHEDULER_SHADOW_AGING_CONFIG_INVALID")
    if cold_attempts < 0 or cold_bonus < 0:
        raise ValueError("SCHEDULER_SHADOW_COLD_START_CONFIG_INVALID")
    if max_consecutive < 1 or fairness_penalty < 0:
        raise ValueError("SCHEDULER_SHADOW_FAIRNESS_CONFIG_INVALID")
    return ShadowSchedulerPolicy(
        policy_version=policy_version, productivity_windows=windows,
        short_window_weight=short_weight, long_window_weight=long_weight,
        backlog_weight=backlog_weight, backlog_scale=backlog_scale,
        aging_score_per_minute=aging_score, max_aging_bonus=max_aging,
        cold_start_attempts=cold_attempts, cold_start_bonus=cold_bonus,
        max_consecutive_same_action=max_consecutive, fairness_penalty=fairness_penalty,
    )
