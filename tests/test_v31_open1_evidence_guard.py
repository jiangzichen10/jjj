from pathlib import Path
import copy

import ppl_engine.round_orchestrator as ro

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
    assert policy["scheduler_shadow"]["policy_version"] == "V31_SCHED_SHADOW_005"
    assert policy["scheduler_evidence"]["evidence_policy_version"] == "V31_SCHED_EVIDENCE_004"


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


def test_open1_repair_availability_uses_shared_selector_parity_core():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    start = source.index("def _repair_availability_read_only")
    end = source.index("def _scheduler_shadow_observation", start)
    block = source[start:end]
    assert "_evaluate_repair_eligibility_core(" in block
    assert "eligible_plan_ids" in block
    assert "REPAIR_SELECTOR_PARITY_PENDING" not in block


def test_open1_fairness_report_requires_complete_availability_evaluations():
    source = (ROOT / "ppl_engine" / "scheduler_evidence_report.py").read_text(encoding="utf-8")
    assert 'int(row.get("search_evaluation_complete") or 0) == 1' in source
    assert 'int(row.get("repair_evaluation_complete") or 0) == 1' in source


def test_open1_durable_policy_upgrade_accepts_exact_003_002_to_005_004():
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

    mixed_005_003 = copy.deepcopy(current)
    mixed_005_003["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_003"
    assert _round_policy_upgrade_compatible(stored, mixed_005_003) is False

    mixed_004_004 = copy.deepcopy(current)
    mixed_004_004["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_004"
    assert _round_policy_upgrade_compatible(stored, mixed_004_004) is False


def test_open1_durable_policy_upgrade_accepts_exact_004_003_to_005_004():
    current = _policy()
    stored = copy.deepcopy(current)
    stored["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_004"
    stored["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_003"
    assert _round_policy_upgrade_compatible(stored, current) is True


def test_open1_actual_and_shadow_share_same_repair_eligibility_core_without_execution_primitives():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    actual_start = source.index("def _select_repair_batch")
    actual_end = source.index("def _sha256_file", actual_start)
    actual_block = source[actual_start:actual_end]
    shadow_start = source.index("def _repair_availability_read_only")
    shadow_end = source.index("def _scheduler_shadow_observation", shadow_start)
    shadow_block = source[shadow_start:shadow_end]
    core_start = source.index("def _evaluate_repair_eligibility_core")
    core_end = source.index("def _materialize_selector_virtual_plans", core_start)
    core_block = source[core_start:core_end]

    assert "_evaluate_repair_eligibility_core(" in actual_block
    assert "_evaluate_repair_eligibility_core(" in shadow_block
    for forbidden in (
        "_ensure_neutralization_plan(",
        "_ensure_same_family_micro_tune_plan(",
        "sync_turnover_staged_plans(",
        "derive_check_repair_proposals(",
        "refresh_one_pretag_check(",
        "execute_round_repair(",
    ):
        assert forbidden not in core_block


class _DirectionParityStore:
    def __init__(self):
        self._candidates = [
            {"candidate_id": "c0", "sim_key": "s0", "signal_family": "fam0", "dataset_id": "d", "field_id": "f0", "transform_family": "RAW"},
            {"candidate_id": "c1", "sim_key": "s1", "signal_family": "famA", "dataset_id": "d", "field_id": "f1", "transform_family": "RAW"},
            {"candidate_id": "c2", "sim_key": "s2", "signal_family": "famA", "dataset_id": "d", "field_id": "f2", "transform_family": "RAW"},
        ]
        self._plans = [
            {"repair_plan_id": "p0", "run_id": "run", "parent_candidate_id": "c0", "repair_type": "REVERSE_DIRECTION", "plan_status": "PLANNED", "consumed_posts": 0, "repair_signature": "sig0"},
            {"repair_plan_id": "p1", "run_id": "run", "parent_candidate_id": "c1", "repair_type": "REVERSE_DIRECTION", "plan_status": "PLANNED", "consumed_posts": 0, "repair_signature": "sig1"},
            {"repair_plan_id": "p2", "run_id": "run", "parent_candidate_id": "c2", "repair_type": "REVERSE_DIRECTION", "plan_status": "PLANNED", "consumed_posts": 0, "repair_signature": "sig2"},
        ]

    def load_repair_plans(self, run_id):
        return [dict(x) for x in self._plans]

    def load_candidates(self, run_id):
        return [dict(x) for x in self._candidates]


def test_open1_direction_actual_selection_is_not_blocked_by_eligibility_family_dedup(monkeypatch, tmp_path):
    store = _DirectionParityStore()
    monkeypatch.setattr(ro, "classify_run", lambda *a, **k: [])
    monkeypatch.setattr(ro, "_repair_attempts_by_family", lambda *a, **k: {})
    monkeypatch.setattr(ro, "load_winners", lambda *a, **k: [])
    monkeypatch.setattr(ro, "load_external_evidence", lambda *a, **k: [])
    monkeypatch.setattr(ro, "_ppc_controlled_ranked_pool", lambda *a, **k: [])
    monkeypatch.setattr(ro, "rank_repair_candidates", lambda *a, **k: [])
    monkeypatch.setattr(ro, "_alpha_facts", lambda *a, **k: {k: {} for k in ("s0", "s1", "s2")})
    monkeypatch.setattr(ro, "_direction_repair_value", lambda *a, **k: {"score": 1, "sharpe": 2.0, "turnover": 0.2, "band": "POSITIVE"})
    monkeypatch.setattr(ro, "rank_direction_repair_candidates", lambda rows: list(rows))

    projected = {"p0": 1, "p1": 1, "p2": 0}
    def _preview(_store, _config, _alpha_db, _run_id, plans, _machine, *, emit_audit=False, enforce_global_repair_budget=False):
        pid = str(plans[0]["repair_plan_id"])
        return {"items": [{"repair_plan_id": pid, "required_action": "CACHE_RESTORE" if projected[pid] == 0 else "NEW_SIMULATION_REQUIRED"}],
                "projected_new_posts": projected[pid]}
    monkeypatch.setattr(ro, "preview_production_repair_plan_rows_read_only", _preview)

    policy = {"batch_size": 2, "normal_near_pass_repair_cap_per_family": 1,
              "strong_near_pass_repair_cap_per_family": 2, "adaptive_ranking": {},
              "ppl_classification": {"fixed_gates": {"turnover_max": 0.70}}}
    out = ro._evaluate_repair_eligibility_core(
        store, type("Cfg", (), {"rules": {}})(), tmp_path / "alpha.db", object(), "run", "round",
        policy, 1, tmp_path / "evidence.json", emit_audit=True,
    )
    assert out["selected"] == ["p0", "p2"]
    assert set(out["eligible_plan_ids"]) == {"p0", "p1"}


def test_open1_shared_core_preserves_actual_fail_closed_and_budget_preview_semantics():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    core_start = source.index("def _evaluate_repair_eligibility_core")
    core_end = source.index("def _materialize_selector_virtual_plans", core_start)
    core = source[core_start:core_end]
    assert "enforce_global_repair_budget=bool(audit)" in core
    assert "except (ConfigError, ValueError, KeyError, TypeError)" not in core
