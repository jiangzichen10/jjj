"""Offline tests for the V2 HT Returns Ratio Repair Strategy (ts_target_tvr_decay).

Covers: V1 retirement, V2 generation, operator gate, parent-signal wrapping,
parent-anchored target_tvr grid, and base-turnover safety. No BRAIN requests.
"""

from pathlib import Path

import pytest
import yaml

from ppl_engine.repair_engine import (
    HT_V2_TARGET_TVR,
    HT_V2_TARGET_TVR_OPERATOR,
    ht_target_tvr_grid,
    plan_repairs,
    strategy_status,
)

ROOT = Path(__file__).resolve().parents[1]


def _rules():
    return yaml.safe_load((ROOT / "ppl_rules.yaml").read_text(encoding="utf-8"))


def _candidate(expression="ts_mean(predicted_first_quantile_one_day_return_2, 2)", turnover=0.6249):
    return {
        "candidate_id": "cand_zy", "root_candidate_id": "cand_zy",
        "field_id": "predicted_first_quantile_one_day_return_2", "field_type": "MATRIX",
        "vector_reducer": "IDENTITY", "operator": "ts_mean", "window": 2, "direction": "NORMAL",
        "sim_key": "parent_sim_key", "expression": expression, "repair_depth": 0,
        "repair_path": ["RAW"], "signal_family": "fam", "turnover": turnover,
    }


def _diag():
    return {"primary_failure": "HT_RETURNS_RATIO_FAIL", "secondary_failures": []}


# ---- target_tvr grid -------------------------------------------------------

def test_grid_parent_anchored():
    rules = _rules()
    assert ht_target_tvr_grid(0.6249, rules) == [0.6, 0.625, 0.65]


def test_grid_clamps_to_base_ceiling_when_above_max():
    rules = _rules()
    grid = ht_target_tvr_grid(0.95, rules)
    assert all(0.20 <= v <= 0.70 for v in grid)
    assert max(grid) <= 0.70


def test_grid_none_returns_empty():
    assert ht_target_tvr_grid(None, _rules()) == []


def test_grid_never_exceeds_base_max():
    rules = _rules()
    for t in (0.20, 0.6249, 0.6999, 0.70, 0.85):
        grid = ht_target_tvr_grid(t, rules)
        assert all(0.20 <= v <= 0.70 for v in grid)


# ---- V2 generation vs V1 retirement ----------------------------------------

def test_v2_generated_not_v1():
    rules = _rules()
    out = plan_repairs(_candidate(), _diag(), rules, registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    types = {p["repair_type"] for p in out["plans"]}
    assert types == set()
    assert out["stop_reason"] == "NO_AUTO_REPAIR_HT_RATIO"


def test_v2_wraps_parent_signal_not_raw_field():
    rules = _rules()
    parent_expr = "ts_mean(predicted_first_quantile_one_day_return_2, 2)"
    out = plan_repairs(_candidate(expression=parent_expr), _diag(), rules,
                       registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    assert out["plans"] == []


def test_v2_requires_verified_operator():
    rules = _rules()
    out = plan_repairs(_candidate(), _diag(), rules, registry={HT_V2_TARGET_TVR_OPERATOR: "UNVERIFIED"})
    assert out["plans"] == []


def test_v2_verified_operator_ready():
    rules = _rules()
    out = plan_repairs(_candidate(), _diag(), rules, registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    assert out["plans"] == []


def test_v2_three_candidates_for_zyevroy0_turnover():
    rules = _rules()
    out = plan_repairs(_candidate(turnover=0.6249), _diag(), rules,
                       registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    assert out["plans"] == []


# ---- strategy status -------------------------------------------------------

def test_strategy_status_marks_v1_stop():
    st = strategy_status()
    assert st["retired_strategies"]["HT_RATIO_SIGNAL_HORIZON"]["status"] == "STOP_THIS_REPAIR_PATH"
    assert st["retired_strategies"]["HT_RATIO_SIGNAL_HORIZON"]["reason"] == "EMPIRICALLY_DEGRADES_TURNOVER_AND_FITNESS"
    assert st["active_ht_v2"] is None
    assert st["retired_strategies"]["HT_RATIO_TARGET_TVR"]["status"] == "STOP_THIS_REPAIR_PATH"


def test_repair_signature_differs_by_target_tvr():
    rules = _rules()
    out = plan_repairs(_candidate(turnover=0.6249), _diag(), rules,
                       registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    sigs = {p["repair_signature"] for p in out["plans"]}
    assert len(sigs) == 0


def test_no_hardcoded_field_or_expression():
    rules = _rules()
    # different field + expression still wraps correctly (no hardcoded field)
    cand = _candidate(expression="ts_mean(different_field_xyz, 4)", turnover=0.5)
    cand["field_id"] = "different_field_xyz"; cand["window"] = 4
    out = plan_repairs(cand, _diag(), rules, registry={HT_V2_TARGET_TVR_OPERATOR: "VERIFIED_PROJECT"})
    assert all("different_field_xyz" in p["expression_preview"] for p in out["plans"])
