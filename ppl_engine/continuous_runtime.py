"""V3.1 continuous lifecycle helpers.

This module contains only lifecycle decisions.  It does not perform HTTP, DB
writes, state transitions or sleeping.  Legacy V3.0.x keeps budget-enforced
semantics; V3.1 Continuous mode treats global budgets as statistics only while
retaining bounded per-batch/local safety limits.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .continuous_policy import ContinuousPolicy, parse_continuous_policy
from .policy_specs import effective_repair_allocation, effective_search_allocation


@dataclass(frozen=True)
class PhaseCapacity:
    phase: str
    enforced: bool
    capacity: int
    consumed: int
    configured_budget: int
    remaining: Optional[int]


@dataclass(frozen=True)
class BudgetView:
    enforced: bool
    total_budget: int
    search_budget: int
    repair_budget: int
    search_consumed: int
    repair_consumed: int
    remaining_total: Optional[int]
    remaining_search: Optional[int]
    remaining_repair: Optional[int]


def lifecycle_policy(round_policy: Mapping[str, Any]) -> ContinuousPolicy:
    return parse_continuous_policy(round_policy)


def phase_capacity(round_policy: Mapping[str, Any], round_row: Mapping[str, Any], phase: str) -> PhaseCapacity:
    """Return the maximum paid work selectable for one scheduling cycle.

    In legacy mode this is the remaining global phase budget.  In Continuous
    mode the historical round budget is statistics-only, so selection is
    bounded only by the local batch size.  This is deliberately *not* an
    unlimited integer: local operation bounds stay bounded even when global
    research budget is unlimited.
    """

    p = lifecycle_policy(round_policy)
    normalized = str(phase or "SEARCH").upper()
    if normalized not in {"SEARCH", "REPAIR"}:
        raise ValueError(f"UNSUPPORTED_PHASE:{normalized}")

    budget_key = "search_budget" if normalized == "SEARCH" else "repair_budget"
    consumed_key = "search_consumed" if normalized == "SEARCH" else "repair_consumed"
    configured = int(round_row.get(budget_key) or 0)
    consumed = int(round_row.get(consumed_key) or 0)

    if p.global_budget_enforced:
        remaining = max(0, configured - consumed)
        return PhaseCapacity(normalized, True, remaining, consumed, configured, remaining)

    if normalized == "SEARCH":
        batch_size = int(effective_search_allocation(round_policy)["batch_size"])
    else:
        batch_size = int(effective_repair_allocation(round_policy)["batch_size"])
    return PhaseCapacity(normalized, False, max(1, batch_size), consumed, configured, None)


def budget_view(round_policy: Mapping[str, Any], round_row: Mapping[str, Any]) -> BudgetView:
    p = lifecycle_policy(round_policy)
    total = int(round_row.get("total_budget") or 0)
    search = int(round_row.get("search_budget") or 0)
    repair = int(round_row.get("repair_budget") or 0)
    search_used = int(round_row.get("search_consumed") or 0)
    repair_used = int(round_row.get("repair_consumed") or 0)
    if p.global_budget_enforced:
        return BudgetView(
            True, total, search, repair, search_used, repair_used,
            max(0, total - search_used - repair_used),
            max(0, search - search_used),
            max(0, repair - repair_used),
        )
    return BudgetView(False, total, search, repair, search_used, repair_used, None, None, None)


def budget_exhaustion_is_terminal(round_policy: Mapping[str, Any]) -> bool:
    return lifecycle_policy(round_policy).global_budget_enforced


def no_safe_work_is_terminal(round_policy: Mapping[str, Any]) -> bool:
    return not lifecycle_policy(round_policy).enabled


def resolve_invocation_batch_limit(round_policy: Mapping[str, Any], explicit_max_batches: Optional[int]) -> Optional[int]:
    """Resolve an operator/canary invocation guard.

    Continuous production defaults to no invocation batch limit.  An explicit
    CLI limit remains valid for canaries and controlled tests.
    """

    if explicit_max_batches is not None:
        value = int(explicit_max_batches)
        if value <= 0:
            raise ValueError("MAX_BATCHES_MUST_BE_POSITIVE")
        return value
    p = lifecycle_policy(round_policy)
    return p.default_max_batches if p.enabled else None
