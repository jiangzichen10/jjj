import pytest

import ppl_engine.round_orchestrator as ro


class _FakeStore:
    def __init__(self):
        self._plans = []
        self._candidates = [{
            "candidate_id": "cand_pp_corr",
            "run_id": "run_test",
            "dataset_id": "tech_chart_model",
            "field_id": "predicted_return_quantile5_12",
            "vector_reducer": "IDENTITY",
            "direction": "NORMAL",
            "transform_family": "TS_MEAN",
            "signal_family": "tech_chart_model/predicted_return_quantile5_12/IDENTITY/NORMAL/TS_MEAN",
            "expression": "ts_mean(predicted_return_quantile5_12, 4)",
            "operator": "ts_mean",
            "window": 4,
            "initial_selection_score": 100.0,
        }]

    def load_repair_plans(self, run_id):
        assert run_id == "run_test"
        return [dict(x) for x in self._plans]

    def load_candidates(self, run_id):
        assert run_id == "run_test"
        return [dict(x) for x in self._candidates]


def test_round_selector_materializes_recommended_strategy_neutralization(monkeypatch, tmp_path):
    """Regression for Batch-44 recommendation contract mismatch.

    preview_rescue() exposes the executable proposal through
    ``recommended_strategy``.  _select_repair_batch() must consume that value,
    materialize the neutralization plan, preview it, and select it instead of
    silently producing SKIP_NO_SAFE_REPAIR_PLAN.
    """
    store = _FakeStore()

    monkeypatch.setattr(ro, "derive_check_repair_proposals", lambda *a, **k: None)
    monkeypatch.setattr(
        ro,
        "sync_turnover_staged_plans",
        lambda *a, **k: {"exhausted": []},
    )
    monkeypatch.setattr(ro, "_repair_attempts_by_family", lambda *a, **k: {})
    monkeypatch.setattr(ro, "load_winners", lambda *a, **k: [])
    monkeypatch.setattr(ro, "load_external_evidence", lambda *a, **k: [])
    # This regression isolates recommendation materialization; controlled-branch
    # state itself is covered by test_ppc_controlled_branch_v1.py.
    monkeypatch.setattr(ro, "_ppc_controlled_ranked_pool", lambda _s, _c, _r, pool: list(pool))

    monkeypatch.setattr(
        ro,
        "classify_run",
        lambda *a, **k: [{
            "candidate_id": "cand_pp_corr",
            "classification": "PPL_FIXED_REPAIRABLE",
            "repair_priority": "MEDIUM",
            "repair_drivers": ["PP_CORRELATION_FAIL"],
            "primary_failure": "PP_CORRELATION_FAIL",
            "max_normalized_gap": 0.0884,
            "sharpe": 1.95,
            "turnover": 0.4073,
        }],
    )

    # This is the real preview_rescue contract that exposed the production bug:
    # there is no "recommendation" key here.
    monkeypatch.setattr(
        ro,
        "preview_rescue",
        lambda *a, **k: {
            "allowed_to_execute": True,
            "target_failure": "PP_CORRELATION_FAIL",
            "rescue_target": "CORRELATION_NEAR_PASS",
            "recommended_strategy": {
                "strategy": "NEUTRALIZATION_MICRO_TUNE",
                "change": {"neutralization": "MARKET"},
                "priority_rank": 0,
            },
            "recommended_change": {"neutralization": "MARKET"},
            "auto_stop_reason": None,
        },
    )

    materialized = []

    def _fake_ensure(store_arg, config, run_id, candidate_id, target_failure, neutralization):
        assert store_arg is store
        assert run_id == "run_test"
        assert candidate_id == "cand_pp_corr"
        assert target_failure == "PP_CORRELATION_FAIL"
        assert neutralization == "MARKET"

        plan = {
            "repair_plan_id": "rplan_neutralization_regression",
            "run_id": run_id,
            "parent_candidate_id": candidate_id,
            "repair_type": "NEUTRALIZATION_MICRO_TUNE",
            "target_failure": target_failure,
            "plan_status": "PLANNED",
            "consumed_posts": 0,
        }
        store._plans.append(plan)
        materialized.append(plan)
        return plan["repair_plan_id"]

    monkeypatch.setattr(ro, "_ensure_neutralization_plan", _fake_ensure)

    preview_calls = []

    def _fake_preview(store_arg, config, alpha_db, run_id, plan_ids, machine):
        preview_calls.append(list(plan_ids))
        assert plan_ids == ["rplan_neutralization_regression"]
        return {
            "items": [{
                "repair_plan_id": "rplan_neutralization_regression",
                "required_action": "NEW_SIMULATION_REQUIRED",
            }],
            "projected_new_posts": 1,
        }

    monkeypatch.setattr(ro, "preview_production_repair", _fake_preview)

    policy = {
        "batch_size": 40,
        "normal_near_pass_repair_cap_per_family": 1,
        "strong_near_pass_repair_cap_per_family": 2,
        "adaptive_ranking": {},
        "ppl_classification": {"fixed_gates": {"turnover_max": 0.70}},
    }

    selected = ro._select_repair_batch(
        store,
        config=type("Cfg", (), {"rules": {"near_pass": {"ppc_controlled_branch": {"max_attempts": 3}}}})(),
        alpha_db=tmp_path / "alpha.db",
        machine=object(),
        run_id="run_test",
        round_id="round_run_test",
        policy=policy,
        remaining=384,
        external_evidence_path=tmp_path / "external.json",
        batch_no=None,
        session=None,
    )

    assert materialized, "NEUTRALIZATION_MICRO_TUNE plan was not materialized"
    assert preview_calls == [["rplan_neutralization_regression"]]
    assert selected == ["rplan_neutralization_regression"]
