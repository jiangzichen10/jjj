"""Pure V3.1 scheduler primitives.

The scheduler never performs HTTP or database writes.  It ranks declarative
work items using research value plus aging/fairness and respects remote-slot
capacity for work that may create a new Simulation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence

from .strategy_contracts import SchedulerActionType, SchedulerDecision


@dataclass(frozen=True)
class SchedulerPolicy:
    aging_score_per_minute: float = 0.25
    max_consecutive_same_action: int = 4
    fairness_penalty: float = 1000.0


@dataclass(frozen=True)
class WorkItem:
    action: SchedulerActionType
    value_score: float = 0.0
    queue_age_seconds: float = 0.0
    reason: str = ""
    requires_new_remote_slot: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchedulerSnapshot:
    work: Sequence[WorkItem] = ()
    remote_slot_limit: int = 0
    remote_slots_reserved: int = 0
    consecutive_action: Optional[SchedulerActionType] = None
    consecutive_count: int = 0
    wait_reason: str = "NO_EXECUTABLE_WORK"
    halt_reason: Optional[str] = None
    stop_requested: bool = False

    @property
    def free_remote_slots(self) -> int:
        return max(0, int(self.remote_slot_limit) - int(self.remote_slots_reserved))


def _effective_score(item: WorkItem, snapshot: SchedulerSnapshot, policy: SchedulerPolicy) -> float:
    age_bonus = max(0.0, float(item.queue_age_seconds)) / 60.0 * float(policy.aging_score_per_minute)
    score = float(item.value_score) + age_bonus
    if (
        snapshot.consecutive_action is item.action
        and int(snapshot.consecutive_count) >= int(policy.max_consecutive_same_action)
    ):
        score -= float(policy.fairness_penalty)
    return score


def choose_next_action(snapshot: SchedulerSnapshot, policy: SchedulerPolicy = SchedulerPolicy()) -> SchedulerDecision:
    """Choose one declarative next action.

    HALT and explicit user STOP are control-plane decisions.  All normal work is
    then ranked by value + queue age.  Work requiring a new remote slot is not
    executable while all slots are conservatively reserved.
    """

    if snapshot.halt_reason:
        return SchedulerDecision(
            action=SchedulerActionType.HALT,
            score=float("inf"),
            reason=str(snapshot.halt_reason),
        )
    if snapshot.stop_requested:
        return SchedulerDecision(
            action=SchedulerActionType.STOP,
            score=float("inf"),
            reason="USER_STOP_REQUESTED",
        )

    executable = []
    for item in snapshot.work:
        if item.requires_new_remote_slot and snapshot.free_remote_slots <= 0:
            continue
        executable.append(item)

    if not executable:
        reason = "WAIT_SERVER_SLOT" if any(x.requires_new_remote_slot for x in snapshot.work) and snapshot.free_remote_slots <= 0 else snapshot.wait_reason
        return SchedulerDecision(action=SchedulerActionType.WAIT, reason=reason)

    scored = [(_effective_score(item, snapshot, policy), idx, item) for idx, item in enumerate(executable)]
    score, _, selected = max(scored, key=lambda row: (row[0], -row[1]))
    return SchedulerDecision(
        action=selected.action,
        score=score,
        reason=selected.reason,
        queue_age_seconds=selected.queue_age_seconds,
        metadata=dict(selected.metadata),
    )
