from pathlib import Path
import copy

from ppl_engine.scheduler_shadow import (
    QueueFacts,
    ResearchAvailabilityFacts,
    ShadowSchedulerSnapshot,
    choose_shadow_action,
    policy_from_mapping,
)
from ppl_engine.strategy_contracts import SchedulerActionType
from ppl_engine.config import load_effective_config
from ppl_engine.round_orchestrator import load_round_policy, _round_policy_upgrade_compatible

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(
        ROOT / "ppl_rules.yaml",
        ROOT / "ppl_plan_v31.yaml",
        project_dir=ROOT,
    )


def _policy():
    return load_round_policy(ROOT / "ppl_round_v31_d2e.yaml", _config())


def _available(count: int, slots: int = 1) -> ResearchAvailabilityFacts:
    return ResearchAvailabilityFacts(
        raw_backlog_count=count,
        selector_eligible_count=count,
        preview_safe_count=count,
        execution_eligible_count=count,
        evaluation_complete=True,
        reason="TEST",
        remote_slots_free=slots,
        immediately_dispatchable_count=min(count, slots),
    )


def test_open1_identity_bumped_for_semantic_break():
    policy = _policy()
    assert policy["scheduler_shadow"]["policy_version"] == "V31_SCHED_SHADOW_004"
    assert policy["scheduler_evidence"]["evidence_policy_version"] == "V31_SCHED_EVIDENCE_003"


def test_open1_incomplete_repair_proxy_cannot_trigger_hard_starvation():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    incomplete_repair = ResearchAvailabilityFacts(
        raw_backlog_count=433,
        selector_eligible_count=92,
        preview_safe_count=92,
        execution_eligible_count=0,
        evaluation_complete=False,
        reason="REPAIR_SELECTOR_PARITY_PENDING:INDIVIDUAL_PLAN_PREVIEW_PROXY_ONLY:PROXY_FAMILIES=92",
        remote_slots_free=1,
        immediately_dispatchable_count=0,
    )
    snapshot = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=10),
        repair_queue=QueueFacts(backlog=433),
        search_availability=_available(10, 1),
        repair_availability=incomplete_repair,
        remote_slot_limit=1,
        remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.SEARCH,
        consecutive_count=4,
    )
    decision = choose_shadow_action(snapshot, policy)
    assert decision.shadow_action is SchedulerActionType.SEARCH
    assert decision.reason != "SHADOW_HARD_STARVATION_GUARD"


def test_open1_source_fails_closed_instead_of_promoting_proxy_to_executable():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    start = source.index("def _repair_availability_read_only")
    end = source.index("def _scheduler_shadow_observation", start)
    block = source[start:end]
    assert "REPAIR_SELECTOR_PARITY_PENDING" in block
    assert "executable=0" in block
    assert "complete=False" in block
    assert "proxy_count" in block


def test_open1_fairness_report_requires_complete_availability_evaluations():
    source = (ROOT / "ppl_engine" / "scheduler_evidence_report.py").read_text(encoding="utf-8")
    assert 'int(row.get("search_evaluation_complete") or 0) == 1' in source
    assert 'int(row.get("repair_evaluation_complete") or 0) == 1' in source


def test_open1_durable_policy_upgrade_accepts_exact_003_002_to_004_003():
    current = _policy()
    stored = copy.deepcopy(current)
    stored["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_003"
    stored["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_002"
    assert _round_policy_upgrade_compatible(stored, current) is True

    bad = copy.deepcopy(stored)
    bad["batch_size"] += 1
    assert _round_policy_upgrade_compatible(bad, current) is False


def test_open1_mixed_scheduler_evidence_identity_pairs_are_rejected():
    current = _policy()

    stored = copy.deepcopy(current)
    stored["continuous"].pop("allow_search_pool_expansion")
    stored["rolling_discovery"]["enabled"] = True
    stored["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_002"
    stored["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_001"

    mixed_004_002 = copy.deepcopy(current)
    mixed_004_002["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_002"
    assert _round_policy_upgrade_compatible(stored, mixed_004_002) is False

    mixed_003_003 = copy.deepcopy(current)
    mixed_003_003["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_003"
    assert _round_policy_upgrade_compatible(stored, mixed_003_003) is False
