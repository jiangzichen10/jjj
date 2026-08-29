"""Stable V3.1 strategy contracts.

These contracts intentionally contain no database or HTTP primitives.  Strategy
code receives read-only facts and returns declarative decisions.  Execution,
state transitions, durability and remote side effects stay in the engine layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple


class RuleRole(str, Enum):
    PLATFORM_HARD_RULE = "PLATFORM_HARD_RULE"
    LOCAL_QUALIFICATION_RULE = "LOCAL_QUALIFICATION_RULE"
    LOCAL_STRATEGY_RULE = "LOCAL_STRATEGY_RULE"
    DIAGNOSTIC_WARNING = "DIAGNOSTIC_WARNING"


class MissingFactAction(str, Enum):
    UNRESOLVED = "UNRESOLVED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    FAIL = "FAIL"


class SchedulerActionType(str, Enum):
    RECOVER_DURABLE = "RECOVER_DURABLE"
    POLL_REMOTE = "POLL_REMOTE"
    CHECK_DUE = "CHECK_DUE"
    REPAIR = "REPAIR"
    SEARCH = "SEARCH"
    DISCOVERY_REFRESH = "DISCOVERY_REFRESH"
    WAIT = "WAIT"
    HALT = "HALT"
    STOP = "STOP"


@dataclass(frozen=True)
class PolicyVersions:
    qualification: str
    search: str
    repair: str
    scheduler: str


@dataclass(frozen=True)
class ResearchContext:
    """Read-only strategy input.

    Facts are deliberately mappings/tuples so callers can construct immutable
    snapshots without exposing RunnerStore, sqlite3.Connection or HTTP session
    objects to strategy code.
    """

    run_id: str
    candidate_facts: Tuple[Mapping[str, Any], ...] = ()
    check_facts: Tuple[Mapping[str, Any], ...] = ()
    repair_history: Tuple[Mapping[str, Any], ...] = ()
    dataset_stats: Tuple[Mapping[str, Any], ...] = ()
    productivity: Mapping[str, Any] = field(default_factory=dict)
    policy_versions: Optional[PolicyVersions] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QualificationResult:
    classification: str
    qualified: bool
    blockers: Tuple[str, ...] = ()
    unresolved: Tuple[str, ...] = ()
    diagnostics: Tuple[str, ...] = ()
    repairable_failure_codes: Tuple[str, ...] = ()
    platform_facts: Mapping[str, Any] = field(default_factory=dict)
    local_strategy_results: Mapping[str, Any] = field(default_factory=dict)
    policy_version: str = ""


@dataclass(frozen=True)
class SearchDecision:
    candidate_id: str
    score: float
    strategy: str
    reason: str
    policy_version: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RepairDecision:
    candidate_id: str
    action: str
    score: float
    strategy: str
    reason: str
    policy_version: str
    expression: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchedulerDecision:
    action: SchedulerActionType
    score: float = 0.0
    reason: str = ""
    queue_age_seconds: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)


class QualificationPolicy(Protocol):
    version: str

    def evaluate(self, context: ResearchContext) -> QualificationResult:
        ...


class SearchStrategy(Protocol):
    version: str

    def propose(self, context: ResearchContext) -> Sequence[SearchDecision]:
        ...


class RepairStrategy(Protocol):
    version: str

    def supports(self, context: ResearchContext) -> bool:
        ...

    def score(self, context: ResearchContext) -> float:
        ...

    def propose(self, context: ResearchContext) -> Sequence[RepairDecision]:
        ...
