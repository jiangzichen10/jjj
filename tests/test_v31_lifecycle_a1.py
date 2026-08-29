import copy
from pathlib import Path

import pytest

from ppl_engine.config import build_execution_material, load_effective_config
from ppl_engine.continuous_runtime import (
    budget_exhaustion_is_terminal,
    budget_view,
    no_safe_work_is_terminal,
    phase_capacity,
    resolve_invocation_batch_limit,
)
from ppl_engine.round_orchestrator import _reconcile_round_accounting, load_round_policy
from ppl_engine.round_store import create_round, finish_batch, start_batch
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]


def _config(plan_name: str):
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / plan_name, project_dir=ROOT)


def test_v31_hypothetical_two_year_threshold_is_not_a_baseline_rule():
    text = (ROOT / "V3_1_ARCHITECTURE_CHANGE_MAP.md").read_text(encoding="utf-8")
    assert "TWO_YEAR_SHARPE_MIN" not in text
    assert "value: 1.58" not in text


def test_v31_continuous_phase_capacity_ignores_legacy_global_budget_exhaustion():
    cfg = _config("ppl_plan_v31.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    row = {
        "batch_size": 40,
        "search_budget": 1600,
        "repair_budget": 400,
        "search_consumed": 9999,
        "repair_consumed": 9999,
    }
    search = phase_capacity(policy, row, "SEARCH")
    repair = phase_capacity(policy, row, "REPAIR")
    assert search.enforced is False and search.capacity == 40 and search.remaining is None
    assert repair.enforced is False and repair.capacity == 40 and repair.remaining is None
    assert budget_exhaustion_is_terminal(policy) is False
    assert no_safe_work_is_terminal(policy) is False


def test_v31_legacy_phase_capacity_remains_budget_enforced():
    cfg = _config("ppl_plan_v3.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg)
    row = {
        "batch_size": 40,
        "search_budget": 10,
        "repair_budget": 5,
        "search_consumed": 10,
        "repair_consumed": 2,
        "total_budget": 15,
    }
    assert phase_capacity(policy, row, "SEARCH").capacity == 0
    assert phase_capacity(policy, row, "REPAIR").capacity == 3
    view = budget_view(policy, row)
    assert view.enforced is True
    assert view.remaining_total == 3
    assert budget_exhaustion_is_terminal(policy) is True
    assert no_safe_work_is_terminal(policy) is True


def test_v31_budget_view_marks_legacy_numbers_statistics_only():
    cfg = _config("ppl_plan_v31.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    row = {
        "total_budget": 2000,
        "search_budget": 1600,
        "repair_budget": 400,
        "search_consumed": 2300,
        "repair_consumed": 700,
    }
    view = budget_view(policy, row)
    assert view.enforced is False
    assert view.search_consumed == 2300
    assert view.repair_consumed == 700
    assert view.remaining_total is None
    assert view.remaining_search is None
    assert view.remaining_repair is None


def test_v31_explicit_max_batches_is_canary_guard_not_default_lifecycle_limit():
    cfg = _config("ppl_plan_v31.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    assert resolve_invocation_batch_limit(policy, None) is None
    assert resolve_invocation_batch_limit(policy, 1) == 1
    with pytest.raises(ValueError, match="MAX_BATCHES_MUST_BE_POSITIVE"):
        resolve_invocation_batch_limit(policy, 0)


def test_v31_global_budget_edits_do_not_change_continuous_execution_material():
    cfg = _config("ppl_plan_v31.yaml")
    plan2 = copy.deepcopy(cfg.plan)
    plan2["budgets"]["max_new_simulation_posts"] = 987654
    plan2["budgets"]["simulation_budget_allocation"]["initial_search_fraction"] = 0.1
    plan2["budgets"]["simulation_budget_allocation"]["repair_reserve_fraction"] = 0.1
    plan2["budgets"]["max_repair_rounds"] = 99999
    a = build_execution_material(cfg.plan, cfg.rules)
    b = build_execution_material(plan2, cfg.rules)
    assert a == b
    assert a["new_simulation_and_repair_budgets"] == {}


def test_v31_simulation_setting_edit_still_changes_execution_material():
    cfg = _config("ppl_plan_v31.yaml")
    plan2 = copy.deepcopy(cfg.plan)
    plan2["simulation_settings"]["decay"] = 7
    assert build_execution_material(cfg.plan, cfg.rules) != build_execution_material(plan2, cfg.rules)


def test_v31_accounting_can_exceed_legacy_budget_when_limits_are_statistics_only(tmp_path):
    cfg = _config("ppl_plan_v31.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=1, search_budget=1, repair_budget=0,
    )
    start_batch(store, "round_run_0006", 1, "SEARCH")
    finish_batch(store, "round_run_0006", 1, {}, logical_posts_consumed=1)
    start_batch(store, "round_run_0006", 2, "SEARCH")
    finish_batch(store, "round_run_0006", 2, {}, logical_posts_consumed=1)

    report = _reconcile_round_accounting(
        store, tmp_path / "alpha_results.db", "run_0006", "round_run_0006",
        enforce_budget_limits=False,
    )
    assert report["search_consumed"] == 2
    assert report["total_consumed"] == 2

    with pytest.raises(Exception, match="ROUND_RECONCILED_BUDGET_EXCEEDED"):
        _reconcile_round_accounting(
            store, tmp_path / "alpha_results.db", "run_0006", "round_run_0006",
            enforce_budget_limits=True,
        )


def test_v31_rolling_discovery_has_no_lifetime_refresh_count_cap():
    cfg = _config("ppl_plan_v31.yaml")
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    assert policy["rolling_discovery"]["max_refreshes"] is None
    assert policy["stop_when_no_safe_candidate"] is False
    assert policy["continuous"]["idle_wait_seconds"] == 30.0


def test_v31_cli_exposes_dedicated_continuous_action_and_defaults():
    from ppl_runner import _parser

    args = _parser().parse_args(["--continuous", "--run-id", "run_0006"])
    assert args.continuous is True
    assert args.continuous_plan.name == "ppl_plan_v31.yaml"
    assert args.continuous_policy.name == "ppl_round_v31.yaml"


def test_v31_continuous_entry_calls_orchestrator_once_not_auto_resume_loop(monkeypatch, tmp_path):
    from ppl_engine.continuous_engine import execute_continuous
    import ppl_engine.round_orchestrator as ro

    cfg = _config("ppl_plan_v31.yaml")
    calls = []

    def fake_execute_round(*args, **kwargs):
        calls.append((args, kwargs))
        return {"round_result": "TEST"}

    monkeypatch.setattr(ro, "execute_round", fake_execute_round)
    out = execute_continuous(
        object(), cfg, object(), None,
        tmp_path / "alpha.db", tmp_path / "machine.py", tmp_path / "evidence.json",
        ROOT / "ppl_round_v31.yaml", run_id="run_0006", max_batches=1,
    )
    assert out["round_result"] == "TEST"
    assert len(calls) == 1
    assert calls[0][1]["run_id"] == "run_0006"
    assert calls[0][1]["max_batches"] == 1


def test_v31_continuous_entry_rejects_legacy_policy(tmp_path):
    from ppl_engine.continuous_engine import execute_continuous
    from ppl_engine.config import ConfigError

    cfg = _config("ppl_plan_v3.yaml")
    with pytest.raises(ConfigError, match="CONTINUOUS_ACTION_REQUIRES_CONTINUOUS_POLICY"):
        execute_continuous(
            object(), cfg, object(), None,
            tmp_path / "alpha.db", tmp_path / "machine.py", tmp_path / "evidence.json",
            ROOT / "ppl_round_v3.yaml",
        )
