"""V3.1 compatibility adapters for the existing PPL Search/Repair selectors.

C1 deliberately does not rewrite the proven V3 ranking/repair algorithms.  The
legacy selectors remain engine-owned while their already-selected immutable
facts are projected into the stable declarative strategy contracts.  The real
orchestrator then executes only what comes back through these decisions.

This bridge is transitional but important: adapters never receive RunnerStore,
sqlite connections, HTTP sessions or machine_lib.  Later C/D checkpoints can
move the ranking logic behind the same contracts without changing execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Tuple

from .scheduler import SchedulerSnapshot, WorkItem, choose_next_action
from .strategy_contracts import (
    PolicyVersions,
    RepairDecision,
    ResearchContext,
    SchedulerActionType,
    SchedulerDecision,
    SearchDecision,
)

SEARCH_COMPAT_STRATEGY = "LEGACY_SEARCH_SELECTOR_COMPAT"
REPAIR_COMPAT_STRATEGY = "LEGACY_REPAIR_SELECTOR_COMPAT"
SCHEDULER_COMPAT_MODE = "PHASE_COMPATIBILITY"


def policy_versions_from_policy(policy: Mapping[str, Any]) -> PolicyVersions:
    versions = dict(policy.get("policy_versions") or {})
    return PolicyVersions(
        qualification=str(versions.get("qualification") or versions.get("ppl_classification") or ""),
        search=str(versions.get("search") or versions.get("ranking") or ""),
        repair=str(versions.get("repair") or ""),
        scheduler=str(versions.get("scheduler") or ""),
    )


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


@dataclass(frozen=True)
class LegacySearchStrategyAdapter:
    """Project legacy-selected Search rows into declarative SearchDecision values."""

    version: str

    def propose(self, context: ResearchContext) -> Sequence[SearchDecision]:
        decisions = []
        for index, row in enumerate(context.candidate_facts, 1):
            cid = str(row.get("candidate_id") or "")
            if not cid:
                continue
            mode = str(row.get("round_selection_mode") or "UNKNOWN")
            if mode == "EXPLORE":
                score = _float(row.get("round_explore_score"), row.get("round_adaptive_score"))
            elif mode in {"EXPLOIT", "BACKFILL"}:
                score = _float(row.get("round_exploit_score"), row.get("round_adaptive_score"))
            else:
                score = _float(row.get("round_adaptive_score"), row.get("initial_selection_score"))
            action = str(row.get("round_cache_action") or "NEW_SIMULATION_REQUIRED")
            decisions.append(
                SearchDecision(
                    candidate_id=cid,
                    score=score,
                    strategy=SEARCH_COMPAT_STRATEGY,
                    reason=f"{mode}:{action}",
                    policy_version=self.version,
                    metadata={
                        "selection_rank": index,
                        "selection_mode": mode,
                        "cache_action": action,
                        "sim_key": str(row.get("sim_key") or ""),
                        "requires_new_remote_slot": action not in {"CACHE_RESTORE", "RESUME_EXISTING"},
                    },
                )
            )
        return tuple(decisions)


@dataclass(frozen=True)
class LegacyRepairStrategyAdapter:
    """Project legacy-selected repair-plan facts into declarative RepairDecision values."""

    version: str

    def propose(self, context: ResearchContext) -> Sequence[RepairDecision]:
        decisions = []
        for index, plan in enumerate(context.repair_history, 1):
            plan_id = str(plan.get("repair_plan_id") or "")
            if not plan_id:
                continue
            parent_id = str(plan.get("parent_candidate_id") or plan.get("candidate_id") or "")
            repair_type = str(plan.get("repair_type") or "UNKNOWN_REPAIR")
            score = _float(plan.get("compat_selection_score"), -index)
            decisions.append(
                RepairDecision(
                    candidate_id=parent_id,
                    action="EXECUTE_REPAIR_PLAN",
                    score=score,
                    strategy=REPAIR_COMPAT_STRATEGY,
                    reason=f"SELECTED_PLAN:{repair_type}",
                    policy_version=self.version,
                    expression=plan.get("expression") or plan.get("proposed_expression"),
                    metadata={
                        "selection_rank": index,
                        "repair_plan_id": plan_id,
                        "repair_type": repair_type,
                        # Exact POST necessity is revalidated by the execution preflight.
                        # Compatibility scheduling reserves capacity conservatively.
                        "requires_new_remote_slot": bool(plan.get("compat_requires_new_remote_slot", True)),
                    },
                )
            )
        return tuple(decisions)


def search_decisions_from_selected_rows(
    run_id: str,
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> Tuple[SearchDecision, ...]:
    versions = policy_versions_from_policy(policy)
    context = ResearchContext(
        run_id=run_id,
        candidate_facts=tuple(dict(row) for row in rows),
        policy_versions=versions,
        metadata={"compatibility_adapter": SEARCH_COMPAT_STRATEGY},
    )
    return tuple(LegacySearchStrategyAdapter(versions.search).propose(context))


def repair_decisions_from_selected_plans(
    run_id: str,
    plans: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> Tuple[RepairDecision, ...]:
    versions = policy_versions_from_policy(policy)
    context = ResearchContext(
        run_id=run_id,
        repair_history=tuple(dict(plan) for plan in plans),
        policy_versions=versions,
        metadata={"compatibility_adapter": REPAIR_COMPAT_STRATEGY},
    )
    return tuple(LegacyRepairStrategyAdapter(versions.repair).propose(context))


def choose_compatibility_strategy_action(
    *,
    search_decisions: Sequence[SearchDecision] = (),
    repair_decisions: Sequence[RepairDecision] = (),
    remote_slot_limit: int = 0,
    remote_slots_reserved: int = 0,
    wait_reason: str = "NO_DECLARATIVE_STRATEGY_WORK",
    enforce_remote_slots: bool = False,
) -> SchedulerDecision:
    """Route declarative Search/Repair proposals through the real scheduler.

    C1 intentionally does not add productivity/fairness arbitration.  The
    orchestrator supplies compatibility proposals in the same phase order as V3,
    while this function becomes the common execution gate.  V3.1-D can feed both
    queues concurrently without changing Search/Repair/Execution contracts.
    """

    work = []
    if search_decisions:
        work.append(
            WorkItem(
                action=SchedulerActionType.SEARCH,
                value_score=max(float(x.score) for x in search_decisions),
                reason="SEARCH_STRATEGY_DECISIONS_READY",
                requires_new_remote_slot=(enforce_remote_slots and any(bool(x.metadata.get("requires_new_remote_slot")) for x in search_decisions)),
                metadata={
                    "decision_count": len(search_decisions),
                    "policy_version": search_decisions[0].policy_version,
                    "strategy": search_decisions[0].strategy,
                },
            )
        )
    if repair_decisions:
        work.append(
            WorkItem(
                action=SchedulerActionType.REPAIR,
                value_score=max(float(x.score) for x in repair_decisions),
                reason="REPAIR_STRATEGY_DECISIONS_READY",
                requires_new_remote_slot=(enforce_remote_slots and any(bool(x.metadata.get("requires_new_remote_slot")) for x in repair_decisions)),
                metadata={
                    "decision_count": len(repair_decisions),
                    "policy_version": repair_decisions[0].policy_version,
                    "strategy": repair_decisions[0].strategy,
                },
            )
        )
    return choose_next_action(
        SchedulerSnapshot(
            work=tuple(work),
            remote_slot_limit=max(0, int(remote_slot_limit)),
            remote_slots_reserved=max(0, int(remote_slots_reserved)),
            wait_reason=wait_reason,
        )
    )
