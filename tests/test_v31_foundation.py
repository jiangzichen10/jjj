from ppl_engine.continuous_policy import (
    ContinuousPolicy,
    GlobalBudgetMode,
    LifecycleMode,
    parse_continuous_policy,
)
from ppl_engine.strategy_contracts import (
    MissingFactAction,
    PolicyVersions,
    QualificationResult,
    ResearchContext,
    RuleRole,
    SchedulerActionType,
    SearchDecision,
)


def test_v31_continuous_policy_absent_is_exact_legacy_intent():
    p = parse_continuous_policy({"objective": "x"})
    assert p == ContinuousPolicy()
    assert p.lifecycle_mode is LifecycleMode.LEGACY_ROUND
    assert p.global_budget_mode is GlobalBudgetMode.ENFORCE
    assert p.enabled is False
    assert p.global_budget_enforced is True


def test_v31_continuous_policy_defaults_global_budget_to_statistics_only():
    p = parse_continuous_policy({"continuous": {"lifecycle_mode": "CONTINUOUS"}})
    assert p.enabled is True
    assert p.global_budget_enforced is False
    assert p.global_budget_mode is GlobalBudgetMode.STATISTICS_ONLY
    assert p.poll_remote_without_blocking_worker is True
    assert p.recoverable_failures_wait is True
    assert p.scheduler_fairness_enabled is True
    assert p.safe_checkpoint_policy_reload is True
    assert p.default_max_batches is None


def test_v31_continuous_policy_can_keep_explicit_canary_batch_guard():
    p = parse_continuous_policy({
        "continuous": {
            "lifecycle_mode": "CONTINUOUS",
            "global_budget_mode": "STATISTICS_ONLY",
            "default_max_batches": 1,
        }
    })
    assert p.default_max_batches == 1


def test_v31_policy_roles_separate_platform_local_and_diagnostics():
    assert RuleRole.PLATFORM_HARD_RULE.value == "PLATFORM_HARD_RULE"
    assert RuleRole.LOCAL_QUALIFICATION_RULE.value == "LOCAL_QUALIFICATION_RULE"
    assert RuleRole.LOCAL_STRATEGY_RULE.value == "LOCAL_STRATEGY_RULE"
    assert RuleRole.DIAGNOSTIC_WARNING.value == "DIAGNOSTIC_WARNING"
    assert MissingFactAction.UNRESOLVED.value == "UNRESOLVED"


def test_v31_strategy_contracts_are_declarative_value_objects():
    versions = PolicyVersions("Q1", "S1", "R1", "SCH1")
    ctx = ResearchContext(run_id="run_0006", policy_versions=versions)
    decision = SearchDecision(
        candidate_id="cand_1", score=1.5, strategy="compat_search",
        reason="test", policy_version=versions.search,
    )
    result = QualificationResult(
        classification="READY", qualified=True, policy_version=versions.qualification,
    )
    assert ctx.run_id == "run_0006"
    assert decision.policy_version == "S1"
    assert result.qualified is True
    assert SchedulerActionType.POLL_REMOTE.value == "POLL_REMOTE"


def test_v31_round_policy_is_opt_in_and_legacy_policy_has_no_continuous_section():
    from pathlib import Path
    from ppl_engine.config import load_effective_config
    from ppl_engine.round_orchestrator import load_round_policy

    root = Path(__file__).resolve().parents[1]
    legacy_config = load_effective_config(root / "ppl_rules.yaml", root / "ppl_plan_v3.yaml", project_dir=root)
    legacy = load_round_policy(root / "ppl_round_v3.yaml", legacy_config)
    assert "continuous" not in legacy

    v31_config = load_effective_config(root / "ppl_rules.yaml", root / "ppl_plan_v31.yaml", project_dir=root)
    v31 = load_round_policy(root / "ppl_round_v31.yaml", v31_config)
    assert v31["continuous"]["lifecycle_mode"] == "CONTINUOUS"
    assert v31["continuous"]["global_budget_mode"] == "STATISTICS_ONLY"
    # Legacy counters still materialize during the compatibility foundation.
    # V3.1 lifecycle code will stop using them as normal termination criteria.
    assert v31["total_budget"] == 2000
    assert v31["search_budget"] == 1600
    assert v31["repair_budget"] == 400


def test_v31_scheduler_uses_value_and_aging_without_fixed_search_repair_ratio():
    from ppl_engine.scheduler import SchedulerPolicy, SchedulerSnapshot, WorkItem, choose_next_action
    from ppl_engine.strategy_contracts import SchedulerActionType

    snapshot = SchedulerSnapshot(work=(
        WorkItem(SchedulerActionType.REPAIR, value_score=10.0, queue_age_seconds=0, reason="fresh high-value repair"),
        WorkItem(SchedulerActionType.SEARCH, value_score=5.0, queue_age_seconds=1800, reason="aged search"),
    ))
    decision = choose_next_action(snapshot, SchedulerPolicy(aging_score_per_minute=0.25))
    assert decision.action is SchedulerActionType.SEARCH


def test_v31_scheduler_does_not_allocate_new_post_when_all_remote_slots_reserved():
    from ppl_engine.scheduler import SchedulerSnapshot, WorkItem, choose_next_action
    from ppl_engine.strategy_contracts import SchedulerActionType

    snapshot = SchedulerSnapshot(
        work=(
            WorkItem(SchedulerActionType.SEARCH, value_score=100, requires_new_remote_slot=True),
            WorkItem(SchedulerActionType.POLL_REMOTE, value_score=1, requires_new_remote_slot=False),
        ),
        remote_slot_limit=4,
        remote_slots_reserved=4,
    )
    assert choose_next_action(snapshot).action is SchedulerActionType.POLL_REMOTE


def test_v31_scheduler_waits_for_slots_if_only_new_post_work_exists():
    from ppl_engine.scheduler import SchedulerSnapshot, WorkItem, choose_next_action
    from ppl_engine.strategy_contracts import SchedulerActionType

    snapshot = SchedulerSnapshot(
        work=(WorkItem(SchedulerActionType.REPAIR, value_score=10, requires_new_remote_slot=True),),
        remote_slot_limit=4,
        remote_slots_reserved=4,
    )
    decision = choose_next_action(snapshot)
    assert decision.action is SchedulerActionType.WAIT
    assert decision.reason == "WAIT_SERVER_SLOT"


def test_v31_scheduler_halt_and_user_stop_override_research_work():
    from ppl_engine.scheduler import SchedulerSnapshot, WorkItem, choose_next_action
    from ppl_engine.strategy_contracts import SchedulerActionType

    work = (WorkItem(SchedulerActionType.SEARCH, value_score=999),)
    assert choose_next_action(SchedulerSnapshot(work=work, halt_reason="CORE_DB_WRITE_FAILURE")).action is SchedulerActionType.HALT
    assert choose_next_action(SchedulerSnapshot(work=work, stop_requested=True)).action is SchedulerActionType.STOP
