"""V3.1 continuous-lifecycle policy parsing.

The parser is intentionally side-effect free and defaults to legacy semantics.
This lets V3.1 be introduced without changing frozen V3.0.x run behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional


class LifecycleMode(str, Enum):
    LEGACY_ROUND = "LEGACY_ROUND"
    CONTINUOUS = "CONTINUOUS"


class GlobalBudgetMode(str, Enum):
    ENFORCE = "ENFORCE"
    STATISTICS_ONLY = "STATISTICS_ONLY"


@dataclass(frozen=True)
class ContinuousPolicy:
    lifecycle_mode: LifecycleMode = LifecycleMode.LEGACY_ROUND
    global_budget_mode: GlobalBudgetMode = GlobalBudgetMode.ENFORCE
    startup_reconcile_before_new_post: bool = True
    poll_remote_without_blocking_worker: bool = False
    recoverable_failures_wait: bool = False
    scheduler_fairness_enabled: bool = False
    safe_checkpoint_policy_reload: bool = False
    allow_search_pool_expansion: bool = True
    qualified_check_min_retry_after_seconds: float = 3.0
    default_max_batches: Optional[int] = None
    idle_wait_seconds: float = 30.0
    remote_poll_interval_seconds: float = 5.0
    remote_poll_max_per_cycle: int = 4
    network_backoff_initial_seconds: float = 30.0
    network_backoff_max_seconds: float = 300.0
    check_queue_enabled: bool = False
    check_poll_interval_seconds: float = 10.0
    check_poll_max_per_cycle: int = 4
    discovery_queue_enabled: bool = False
    discovery_poll_interval_seconds: float = 10.0
    discovery_poll_max_per_cycle: int = 1
    discovery_failure_cooldown_seconds: float = 300.0
    manual_finalization_check_queue_enabled: bool = False
    auth_retry_seconds: float = 60.0
    due_sleep_max_seconds: float = 300.0

    @property
    def enabled(self) -> bool:
        return self.lifecycle_mode is LifecycleMode.CONTINUOUS

    @property
    def global_budget_enforced(self) -> bool:
        return self.global_budget_mode is GlobalBudgetMode.ENFORCE


def parse_continuous_policy(round_policy: Mapping[str, Any]) -> ContinuousPolicy:
    """Parse a V3.1 continuous policy while preserving legacy defaults.

    If the section is absent, callers get exact legacy lifecycle intent:
    budget enforced, no continuous scheduler assumptions, and no behavior
    change to existing V3.0.x rounds.
    """

    raw = dict(round_policy.get("continuous") or {})
    if not raw:
        return ContinuousPolicy()

    mode = LifecycleMode(str(raw.get("lifecycle_mode", "CONTINUOUS")).upper())
    budget_default = "STATISTICS_ONLY" if mode is LifecycleMode.CONTINUOUS else "ENFORCE"
    budget = GlobalBudgetMode(str(raw.get("global_budget_mode", budget_default)).upper())

    max_batches_raw = raw.get("default_max_batches")
    default_max_batches: Optional[int]
    if max_batches_raw is None:
        default_max_batches = None
    else:
        default_max_batches = int(max_batches_raw)
        if default_max_batches <= 0:
            raise ValueError("CONTINUOUS_DEFAULT_MAX_BATCHES_MUST_BE_POSITIVE_OR_NULL")

    idle_wait_seconds = float(raw.get("idle_wait_seconds", 30.0))
    if idle_wait_seconds <= 0:
        raise ValueError("CONTINUOUS_IDLE_WAIT_SECONDS_MUST_BE_POSITIVE")
    remote_poll_interval_seconds = float(raw.get("remote_poll_interval_seconds", 5.0))
    remote_poll_max_per_cycle = int(raw.get("remote_poll_max_per_cycle", 4))
    network_backoff_initial_seconds = float(raw.get("network_backoff_initial_seconds", 30.0))
    network_backoff_max_seconds = float(raw.get("network_backoff_max_seconds", 300.0))
    if remote_poll_interval_seconds <= 0 or remote_poll_max_per_cycle <= 0:
        raise ValueError("CONTINUOUS_REMOTE_POLL_SETTINGS_MUST_BE_POSITIVE")
    if network_backoff_initial_seconds <= 0 or network_backoff_max_seconds < network_backoff_initial_seconds:
        raise ValueError("CONTINUOUS_NETWORK_BACKOFF_SETTINGS_INVALID")
    check_poll_interval_seconds = float(raw.get("check_poll_interval_seconds", 10.0))
    check_poll_max_per_cycle = int(raw.get("check_poll_max_per_cycle", 4))
    discovery_poll_interval_seconds = float(raw.get("discovery_poll_interval_seconds", 10.0))
    discovery_poll_max_per_cycle = int(raw.get("discovery_poll_max_per_cycle", 1))
    discovery_failure_cooldown_seconds = float(raw.get("discovery_failure_cooldown_seconds", 300.0))
    auth_retry_seconds = float(raw.get("auth_retry_seconds", 60.0))
    due_sleep_max_seconds = float(raw.get("due_sleep_max_seconds", 300.0))
    qualified_refresh = dict(raw.get("qualified_check_refresh") or {})
    qualified_check_min_retry_after_seconds = float(
        qualified_refresh.get("min_retry_after_seconds", 3.0)
    )
    if check_poll_interval_seconds <= 0 or check_poll_max_per_cycle <= 0:
        raise ValueError("CONTINUOUS_CHECK_QUEUE_SETTINGS_MUST_BE_POSITIVE")
    if discovery_poll_interval_seconds <= 0 or discovery_poll_max_per_cycle <= 0 or discovery_failure_cooldown_seconds <= 0:
        raise ValueError("CONTINUOUS_DISCOVERY_QUEUE_SETTINGS_MUST_BE_POSITIVE")
    if auth_retry_seconds <= 0 or due_sleep_max_seconds <= 0:
        raise ValueError("CONTINUOUS_CONTROL_WAIT_SETTINGS_MUST_BE_POSITIVE")
    if qualified_check_min_retry_after_seconds <= 0:
        raise ValueError("QUALIFIED_CHECK_MIN_RETRY_AFTER_SECONDS_MUST_BE_POSITIVE")

    return ContinuousPolicy(
        lifecycle_mode=mode,
        global_budget_mode=budget,
        startup_reconcile_before_new_post=bool(raw.get("startup_reconcile_before_new_post", True)),
        poll_remote_without_blocking_worker=bool(raw.get("poll_remote_without_blocking_worker", mode is LifecycleMode.CONTINUOUS)),
        recoverable_failures_wait=bool(raw.get("recoverable_failures_wait", mode is LifecycleMode.CONTINUOUS)),
        scheduler_fairness_enabled=bool(raw.get("scheduler_fairness_enabled", mode is LifecycleMode.CONTINUOUS)),
        safe_checkpoint_policy_reload=bool(raw.get("safe_checkpoint_policy_reload", mode is LifecycleMode.CONTINUOUS)),
        allow_search_pool_expansion=bool(raw.get("allow_search_pool_expansion", True)),
        qualified_check_min_retry_after_seconds=qualified_check_min_retry_after_seconds,
        default_max_batches=default_max_batches,
        idle_wait_seconds=idle_wait_seconds,
        remote_poll_interval_seconds=remote_poll_interval_seconds,
        remote_poll_max_per_cycle=remote_poll_max_per_cycle,
        network_backoff_initial_seconds=network_backoff_initial_seconds,
        network_backoff_max_seconds=network_backoff_max_seconds,
        check_queue_enabled=bool(raw.get("check_queue_enabled", mode is LifecycleMode.CONTINUOUS)),
        check_poll_interval_seconds=check_poll_interval_seconds,
        check_poll_max_per_cycle=check_poll_max_per_cycle,
        discovery_queue_enabled=bool(raw.get("discovery_queue_enabled", mode is LifecycleMode.CONTINUOUS)),
        discovery_poll_interval_seconds=discovery_poll_interval_seconds,
        discovery_poll_max_per_cycle=discovery_poll_max_per_cycle,
        discovery_failure_cooldown_seconds=discovery_failure_cooldown_seconds,
        manual_finalization_check_queue_enabled=bool(raw.get("manual_finalization_check_queue_enabled", mode is LifecycleMode.CONTINUOUS)),
        auth_retry_seconds=auth_retry_seconds,
        due_sleep_max_seconds=due_sleep_max_seconds,
    )


def continuous_policy_dict(policy: ContinuousPolicy) -> dict:
    """Return a stable JSON/YAML-friendly normalized policy mapping."""
    return {
        "lifecycle_mode": policy.lifecycle_mode.value,
        "global_budget_mode": policy.global_budget_mode.value,
        "startup_reconcile_before_new_post": policy.startup_reconcile_before_new_post,
        "poll_remote_without_blocking_worker": policy.poll_remote_without_blocking_worker,
        "recoverable_failures_wait": policy.recoverable_failures_wait,
        "scheduler_fairness_enabled": policy.scheduler_fairness_enabled,
        "safe_checkpoint_policy_reload": policy.safe_checkpoint_policy_reload,
        "allow_search_pool_expansion": policy.allow_search_pool_expansion,
        "qualified_check_refresh": {
            "min_retry_after_seconds": policy.qualified_check_min_retry_after_seconds,
        },
        "default_max_batches": policy.default_max_batches,
        "idle_wait_seconds": policy.idle_wait_seconds,
        "remote_poll_interval_seconds": policy.remote_poll_interval_seconds,
        "remote_poll_max_per_cycle": policy.remote_poll_max_per_cycle,
        "network_backoff_initial_seconds": policy.network_backoff_initial_seconds,
        "network_backoff_max_seconds": policy.network_backoff_max_seconds,
        "check_queue_enabled": policy.check_queue_enabled,
        "check_poll_interval_seconds": policy.check_poll_interval_seconds,
        "check_poll_max_per_cycle": policy.check_poll_max_per_cycle,
        "discovery_queue_enabled": policy.discovery_queue_enabled,
        "discovery_poll_interval_seconds": policy.discovery_poll_interval_seconds,
        "discovery_poll_max_per_cycle": policy.discovery_poll_max_per_cycle,
        "discovery_failure_cooldown_seconds": policy.discovery_failure_cooldown_seconds,
        "manual_finalization_check_queue_enabled": policy.manual_finalization_check_queue_enabled,
        "auth_retry_seconds": policy.auth_retry_seconds,
        "due_sleep_max_seconds": policy.due_sleep_max_seconds,
    }
