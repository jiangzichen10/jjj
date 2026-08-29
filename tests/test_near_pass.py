"""Offline tests for the Near-Pass Rescue Engine."""

import hashlib
import json
from pathlib import Path

import pytest

from ppl_engine.near_pass import (
    classify_near_pass,
    evaluate_rescue_stop,
    gap_summary,
    manual_review_classify,
    near_pass_config,
    neutralization_candidates,
    normalized_gap,
    preview_rescue,
    threshold_direction,
)
from ppl_engine.repair_engine import neutralization_micro_tune_spec

ROOT = Path(__file__).resolve().parents[1]


def _rules():
    import yaml
    return yaml.safe_load((ROOT / "ppl_rules.yaml").read_text(encoding="utf-8"))


def _candidate(**kw):
    base = {
        "candidate_id": "cand_x", "alpha_id": "A1", "sim_key": "sk_x",
        "simulation_status": "COMPLETE", "structure_status": "ELIGIBLE",
        "field_id": "predicted_first_quantile_one_day_return_2", "field_type": "MATRIX",
        "vector_reducer": "IDENTITY", "operator": "ts_mean", "window": 2,
        "direction": "NORMAL", "expression": "ts_mean(predicted_first_quantile_one_day_return_2, 2)",
        "dataset_id": "techindi_model", "repair_depth": 0, "repair_path": ["RAW"],
        "signal_family": "fam", "turnover": 0.6249,
    }
    base.update(kw)
    return base


def _metrics(sharpe=2.42, fitness=1.18, turnover=0.6249):
    return {"sharpe": sharpe, "fitness": fitness, "turnover": turnover,
            "returns": 0.15, "margin": 0.0004, "long_count": 10, "short_count": 10}


def _checks(**kw):
    """Build a check map keyed by normalized_name."""
    return kw


# ---- threshold direction + normalized gap ----------------------------------

def test_threshold_direction():
    assert threshold_direction("HIGH_TURNOVER_RETURNS_RATIO") == "MIN"
    assert threshold_direction("SHARPE") == "MIN"
    assert threshold_direction("POWER_POOL_CORRELATION") == "MAX"
    assert threshold_direction("PROD_CORRELATION") == "MAX"
    assert threshold_direction("SOME_UNKNOWN_CHECK") == "UNRESOLVED_THRESHOLD_DIRECTION"


def test_normalized_gap_min_and_max():
    assert normalized_gap(0.7374, 0.75, "MIN") == 0.0168
    assert normalized_gap(0.6, 0.5, "MAX") == 0.2
    # PASS (value meets threshold) -> negative gap
    assert normalized_gap(0.76, 0.75, "MIN") < 0


def test_unknown_direction_unresolved():
    g = gap_summary(0.5, 0.7, "UNRESOLVED_THRESHOLD_DIRECTION")
    assert g["normalized_gap"] is None
    assert g["threshold_direction"] == "UNRESOLVED_THRESHOLD_DIRECTION"


# ---- classification --------------------------------------------------------

def _ht_warning(value=0.7374, limit=0.75):
    return {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
            "raw_value_json": json.dumps(value), "raw_limit_json": json.dumps(limit)}


def test_single_small_blocker_strong_near_pass():
    checks = {"HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75)}
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["blocker_count"] == 1


def test_two_small_blockers_near_pass():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.71, 0.75),
        "SUB_UNIVERSE": {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
                         "raw_value_json": "1.45", "raw_limit_json": "1.5"},
    }
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "NEAR_PASS"
    assert out["blocker_count"] == 2


def test_many_large_fails_normal():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.5, 0.75),
        "SHARPE": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                   "raw_value_json": "0.8", "raw_limit_json": "1.58"},
        "SUB_UNIVERSE": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                         "raw_value_json": "0.5", "raw_limit_json": "1.5"},
    }
    out = classify_near_pass(_candidate(), _metrics(sharpe=0.8), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "NORMAL"


def test_warning_not_treated_as_fail():
    # A WARNING with a small gap is STRONG_NEAR_PASS (not demoted to NORMAL as a FAIL would be).
    checks = {"HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75)}
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["blockers"][0]["raw_result"] == "WARNING"


def test_pretag_theme_match_warning_is_deferred_not_blocker():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75),
        "THEME_MATCH": {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
                        "raw_value_json": None, "raw_limit_json": None},
    }
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["blocker_count"] == 1
    assert out["deferred_ppl_checks"][0]["check"] == "THEME_MATCH"
    assert out["deferred_ppl_checks"][0]["deferred_reason"] == "PENDING_MANUAL_POWER_POOL_TAG"


def test_posttag_theme_match_warning_is_hard_blocker():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75),
        "THEME_MATCH": {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
                        "raw_value_json": None, "raw_limit_json": None},
    }
    out = classify_near_pass(
        _candidate(), _metrics(), checks, _rules(), local_gate_status="PASS", check_phase="POST_TAG"
    )
    assert out["classification"] == "NORMAL"
    assert out["blocker_count"] == 2
    assert "UNQUANTIFIED_PPL_HARD_BLOCKER" in out["reasons"]


def test_ak7jnmk2_pretag_facts_remain_ppl_near_pass():
    checks = {
        "HIGH_TURNOVER": {"eligibility_outcome": "PASS", "raw_result": "PASS",
                          "raw_value_json": "0.5566", "raw_limit_json": "0.2"},
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.681, 0.75),
        "POWER_POOL_CORRELATION": {"eligibility_outcome": "PASS", "raw_result": "PASS",
                                   "raw_value_json": "0.4868", "raw_limit_json": "0.5"},
        "PROD_CORRELATION": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                             "raw_value_json": "0.7077", "raw_limit_json": "0.7"},
        "SHARPE": {"eligibility_outcome": "PASS", "raw_result": "PASS",
                   "raw_value_json": "3.02", "raw_limit_json": "1.58"},
        "SUB_UNIVERSE": {"eligibility_outcome": "PASS", "raw_result": "PASS",
                         "raw_value_json": "2.34", "raw_limit_json": "1.85"},
        "THEME_MATCH": {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
                        "raw_value_json": "null", "raw_limit_json": "null"},
    }
    out = classify_near_pass(
        _candidate(turnover=0.5566), _metrics(sharpe=3.02, fitness=1.76, turnover=0.5566),
        checks, _rules(), local_gate_status="PASS"
    )
    assert out["classification"] == "NEAR_PASS"
    assert out["blocker_count"] == 1
    assert out["blockers"][0]["check"] == "HIGH_TURNOVER_RETURNS_RATIO"
    assert out["max_blocker_normalized_gap"] == pytest.approx(0.092)
    assert any(x["check"] == "THEME_MATCH" for x in out["deferred_ppl_checks"])
    assert any(x["check"] == "PROD_CORRELATION" for x in out["quality_diagnostics"])


def test_small_correlation_overrun_tolerated():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75),
        "PROD_CORRELATION": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                             "raw_value_json": "0.7112", "raw_limit_json": "0.7"},
    }
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"  # 0.7112 vs 0.7 is a tiny overrun
    assert out["blocker_count"] == 1


def test_prod_correlation_is_diagnostic_only_even_when_severely_failed():
    checks = {
        "HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75),
        "PROD_CORRELATION": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                             "raw_value_json": "0.95", "raw_limit_json": "0.7"},
    }
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["blocker_count"] == 1
    assert any(x["check"] == "PROD_CORRELATION" and x["diagnostic_only"] for x in out["quality_diagnostics"])


def test_power_pool_correlation_small_fail_is_real_ppl_near_blocker():
    checks = {
        "POWER_POOL_CORRELATION": {"eligibility_outcome": "FAIL", "raw_result": "FAIL",
                                   "raw_value_json": "0.51", "raw_limit_json": "0.50"},
    }
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="PASS")
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["blocker_count"] == 1
    assert out["blockers"][0]["check"] == "POWER_POOL_CORRELATION"


def test_local_gate_fail_not_near_pass():
    checks = {"HIGH_TURNOVER_RETURNS_RATIO": _ht_warning(0.7374, 0.75)}
    out = classify_near_pass(_candidate(), _metrics(), checks, _rules(), local_gate_status="FAIL")
    assert out["classification"] == "NORMAL"


# ---- neutralization micro-tune ---------------------------------------------

def test_neutralization_spec_keeps_expression_changes_settings():
    spec = neutralization_micro_tune_spec(_candidate(), "HT_RETURNS_RATIO_FAIL", "MARKET")
    assert spec["repair_type"] == "NEUTRALIZATION_MICRO_TUNE"
    assert spec["expression_preview"] == "ts_mean(predicted_first_quantile_one_day_return_2, 2)"
    assert spec["settings_override"] == {"neutralization": "MARKET"}


def test_neutralization_excludes_parent():
    import machine_lib_V2_1 as m

    parent = _candidate()
    parent_spec = neutralization_micro_tune_spec(parent, "HT_RETURNS_RATIO_FAIL", "SUBINDUSTRY")
    market_spec = neutralization_micro_tune_spec(parent, "HT_RETURNS_RATIO_FAIL", "MARKET")
    # same expression, different neutralization -> different sim_key
    settings_sub = {"instrumentType": "EQUITY", "region": "GLB", "universe": "TOPDIV3000",
                    "delay": 1, "decay": 0, "neutralization": "SUBINDUSTRY", "truncation": 0.08,
                    "pasteurization": "ON", "testPeriod": "P0Y", "unitHandling": "VERIFY",
                    "nanHandling": "ON", "language": "FASTEXPR", "visualization": False}
    settings_mkt = dict(settings_sub, neutralization="MARKET")
    key_sub = m.simulation_key(parent_spec["expression_preview"], settings_sub)
    key_mkt = m.simulation_key(market_spec["expression_preview"], settings_mkt)
    assert key_sub != key_mkt
    assert parent_spec["expression_preview"] == market_spec["expression_preview"]


def test_neutralization_candidates_read_from_config():
    import types
    config = types.SimpleNamespace(
        rules=_rules(),
        plan={"simulation_settings": {"region": "GLB", "neutralization": "SUBINDUSTRY"}},
    )
    cands = neutralization_candidates(config)
    assert "MARKET" in cands
    assert "SUBINDUSTRY" in cands


# ---- stop rules + manual review -------------------------------------------

def test_stop_rules():
    assert evaluate_rescue_stop([("S", "TARGET_PASS")], 1, 5) == "STOP_RESCUE_SUCCESS"
    assert evaluate_rescue_stop([("S", "WORSE")], 1, 5) == "STOP_THIS_PARAMETER"
    assert evaluate_rescue_stop([("S", "WORSE"), ("S", "WORSE")], 2, 5) == "STOP_THIS_REPAIR_STRATEGY"
    assert evaluate_rescue_stop([("S", "WORSE")], 5, 5) == "STOP_AUTOMATIC_RESCUE"
    assert evaluate_rescue_stop([("S", "IMPROVED")], 1, 5) is None


def test_manual_review_p1():
    blockers = [{"normalized_gap": 0.0168}]
    out = manual_review_classify("STRONG_NEAR_PASS", 5, 5, blockers, _metrics())
    assert out["manual_priority"] == "P1_MANUAL"


def test_manual_review_p2():
    blockers = [{"normalized_gap": 0.06}]
    out = manual_review_classify("NEAR_PASS", 3, 3, blockers, _metrics())
    assert out["manual_priority"] == "P2_MANUAL"


def test_manual_review_p3():
    out = manual_review_classify("NORMAL", 1, 1, [], _metrics(sharpe=0.8))
    assert out["manual_priority"] == "P3_ARCHIVE"


# ---- config defaults -------------------------------------------------------

def test_config_defaults():
    cfg = near_pass_config(_rules())
    assert cfg["near_pass_max_blockers"] == 2
    assert cfg["strong_near_pass_max_blockers"] == 1
    assert cfg["strong_near_pass_normalized_gap_max"] == 0.05
    assert cfg["strong_near_pass_rescue_max_attempts"] == 5


def test_uncertain_consumed_post_does_not_burn_rescue_strategy_attempt():
    from ppl_engine.near_pass import _executed_attempts
    repairs = [
        {"plan_status": "READY", "consumed_posts": 1, "blocked_reason": "UNCERTAIN_SUBMISSION_HOLD"},
        {"plan_status": "EXECUTED", "consumed_posts": 1},
    ]
    assert _executed_attempts(repairs) == 1
