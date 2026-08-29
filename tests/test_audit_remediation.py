"""Regression tests for the 2026-08-17 production audit remediation."""

import json
from pathlib import Path
from types import SimpleNamespace

import machine_lib_V2_1 as ml
import pytest

from ppl_engine.audit_log import read_audit_log
from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.live_execution import _v21_candidate
from ppl_engine.near_pass import (
    _matching_external_evidence,
    evaluate_rescue_stop,
    manual_review_classify,
)
from ppl_engine.repair_engine import (
    materialize_repair_candidate,
    neutralization_micro_tune_spec,
    parameter_change_summary,
)
from ppl_engine.simulation_adapter import execute_with_v21

ROOT = Path(__file__).resolve().parents[1]


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def _full_settings(config, *, neutralization=None, decay=0, truncation=None, expr="ts_mean(predicted_first_quantile_one_day_return_2, 2)"):
    sim = config.plan["simulation_settings"]
    return ml.build_settings(
        {"expr": expr, "decay": decay},
        neutralization=neutralization or sim["neutralization"],
        region=sim["region"], universe=sim["universe"], delay=int(sim["delay"]),
        truncation=float(sim["truncation"] if truncation is None else truncation),
        test_period=sim.get("test_period", sim.get("testPeriod", "P0Y")),
    )


def parent(settings=None):
    settings = settings or {
        "instrumentType": "EQUITY", "region": "GLB", "universe": "TOPDIV3000",
        "delay": 1, "decay": 0, "neutralization": "SUBINDUSTRY", "truncation": 0.08,
        "pasteurization": "ON", "testPeriod": "P0Y", "unitHandling": "VERIFY",
        "nanHandling": "ON", "language": "FASTEXPR", "visualization": False,
    }
    expr = "ts_mean(predicted_first_quantile_one_day_return_2, 2)"
    return {
        "candidate_id": "cand_parent", "root_candidate_id": "cand_parent",
        "field_id": "predicted_first_quantile_one_day_return_2", "field_type": "MATRIX",
        "dataset_id": "techindi_model", "vector_reducer": "IDENTITY",
        "operator": "ts_mean", "transform_family": "TS_MEAN", "window": 2,
        "direction": "NORMAL", "decay": settings["decay"], "sim_key": "parent_key",
        "expression": expr, "settings_json": json.dumps(settings),
        "signal_family": "techindi_model/predicted_first_quantile_one_day_return_2/IDENTITY/NORMAL/TS_MEAN",
        "repair_depth": 0, "repair_path": ["RAW"],
    }


def test_materialize_neutralization_preserves_parent_decay_and_other_settings():
    settings = parent()["settings_json"]
    d = json.loads(settings); d["decay"] = 2; d["truncation"] = 0.05
    p = parent(d)
    spec = neutralization_micro_tune_spec(p, "HT_RETURNS_RATIO_FAIL", "MARKET")
    child = materialize_repair_candidate(p, spec, cfg(), ml)
    assert child["settings"]["neutralization"] == "MARKET"
    assert child["settings"]["decay"] == 2
    assert child["settings"]["truncation"] == 0.05


def test_parameter_change_reads_settings_json_not_sparse_candidate_columns():
    p = parent()
    p.pop("neutralization", None)
    spec = neutralization_micro_tune_spec(p, "HT_RETURNS_RATIO_FAIL", "MARKET")
    out = parameter_change_summary(spec, p)
    assert out["neutralization"] == {"from": "SUBINDUSTRY", "to": "MARKET"}


class CaptureMachine:
    build_settings = staticmethod(ml.build_settings)
    simulation_key = staticmethod(ml.simulation_key)
    def __init__(self):
        self.calls = []
    def simulate_candidates(self, candidates, **kwargs):
        self.calls.append((candidates, kwargs))
        return []


def _wrapped_for(settings):
    expr = "ts_mean(predicted_first_quantile_one_day_return_2, 2)"
    v21 = {
        "expr": expr, "field": "predicted_first_quantile_one_day_return_2",
        "data_fields": ["predicted_first_quantile_one_day_return_2"],
        "dataset_id": "techindi_model", "dataset_ids": ["techindi_model"],
        "field_type": "MATRIX", "vector_op": None, "operator": "ts_mean", "window": 2,
        "decay": settings["decay"], "stage": "REPAIR", "target_mode": "PPL",
        "_v22_settings": dict(settings),
    }
    built = ml.build_settings(v21, neutralization=settings["neutralization"], region=settings["region"],
                              universe=settings["universe"], delay=settings["delay"],
                              truncation=settings["truncation"], test_period=settings["testPeriod"])
    v21["_expected_sim_key"] = ml.simulation_key(expr, built)
    return {"execution_action": "NEW_SIMULATION_REQUIRED", "v21_candidate": v21}


def test_adapter_executes_market_override_instead_of_run_default(tmp_path):
    c = cfg(); m = CaptureMachine()
    settings = _full_settings(c, neutralization="MARKET", decay=0)
    execute_with_v21([_wrapped_for(settings)], c, m, session=None, cache_db=str(tmp_path / "a.db"),
                     allow_simulation_post=True, remaining_initial_budget=1)
    assert len(m.calls) == 1
    assert m.calls[0][1]["neutralization"] == "MARKET"
    assert m.calls[0][0][0]["decay"] == 0
    assert "_v22_settings" not in m.calls[0][0][0]


def test_adapter_rejects_compact_settings_materializer(tmp_path):
    c = cfg()

    class CompactBuildMachine(CaptureMachine):
        @staticmethod
        def build_settings(candidate, **kwargs):
            return {
                "neutralization": kwargs["neutralization"], "region": kwargs["region"],
                "universe": kwargs["universe"], "delay": kwargs["delay"],
                "truncation": kwargs["truncation"], "testPeriod": kwargs["test_period"],
            }

    m = CompactBuildMachine()
    settings = _full_settings(c, decay=0)
    wrapped = _wrapped_for(settings)
    wrapped["v21_candidate"]["_v22_settings"] = {
        "neutralization": settings["neutralization"], "region": settings["region"],
        "universe": settings["universe"], "delay": settings["delay"],
        "truncation": settings["truncation"], "testPeriod": settings["testPeriod"],
    }
    with pytest.raises(ConfigError, match="SIMULATION_SETTINGS_INCOMPLETE"):
        execute_with_v21([wrapped], c, m, session=None, cache_db=str(tmp_path / "a.db"),
                         allow_simulation_post=True, remaining_initial_budget=1)
    assert m.calls == []


def test_adapter_hard_blocks_settings_simkey_mismatch(tmp_path):
    c = cfg(); m = CaptureMachine()
    settings = _full_settings(c, neutralization="MARKET", decay=0)
    wrapped = _wrapped_for(settings)
    wrapped["v21_candidate"]["_expected_sim_key"] = "wrong"
    with pytest.raises(ConfigError, match="V21_SETTINGS_SIM_KEY_MISMATCH"):
        execute_with_v21([wrapped], c, m, session=None, cache_db=str(tmp_path / "a.db"),
                         allow_simulation_post=True, remaining_initial_budget=1)
    assert m.calls == []


def test_adapter_hard_blocks_v21_materialization_drift(tmp_path):
    c = cfg(); m = CaptureMachine()
    settings = _full_settings(c, neutralization="SUBINDUSTRY", decay=0)
    wrapped = _wrapped_for(settings)
    # Durable identity says visualization=True, but unchanged V2.1 build_settings
    # would materialize visualization=False from the same call scope. The adapter
    # must fail before simulate_candidates can POST anything.
    drifted = dict(settings); drifted["visualization"] = True
    wrapped["v21_candidate"]["_v22_settings"] = drifted
    wrapped["v21_candidate"]["_expected_sim_key"] = ml.simulation_key(
        wrapped["v21_candidate"]["expr"], drifted
    )
    with pytest.raises(ConfigError, match="V21_POST_SETTINGS_MATERIALIZATION_MISMATCH"):
        execute_with_v21([wrapped], c, m, session=None, cache_db=str(tmp_path / "a.db"),
                         allow_simulation_post=True, remaining_initial_budget=1)
    assert m.calls == []


def test_adapter_groups_candidates_by_effective_settings(tmp_path):
    c = cfg(); m = CaptureMachine()
    a = _full_settings(c, neutralization="SUBINDUSTRY", decay=0)
    b = _full_settings(c, neutralization="MARKET", decay=0)
    wb = _wrapped_for(b)
    wb["v21_candidate"]["expr"] = "ts_mean(predicted_first_quantile_one_day_return_2, 3)"
    built = ml.build_settings(wb["v21_candidate"], neutralization="MARKET", region=b["region"], universe=b["universe"], delay=b["delay"], truncation=b["truncation"], test_period=b["testPeriod"])
    wb["v21_candidate"]["_expected_sim_key"] = ml.simulation_key(wb["v21_candidate"]["expr"], built)
    execute_with_v21([_wrapped_for(a), wb], c, m, session=None, cache_db=str(tmp_path / "a.db"),
                     allow_simulation_post=True, remaining_initial_budget=2)
    assert [x[1]["neutralization"] for x in m.calls] == ["SUBINDUSTRY", "MARKET"]


def test_adapter_does_not_merge_distinct_full_settings_with_same_call_scope(tmp_path):
    c = cfg(); m = CaptureMachine()
    a = _full_settings(c, neutralization="SUBINDUSTRY", decay=0)
    b = _full_settings(c, neutralization="SUBINDUSTRY", decay=4)
    execute_with_v21([_wrapped_for(a), _wrapped_for(b)], c, m, session=None,
                     cache_db=str(tmp_path / "a.db"), allow_simulation_post=True,
                     remaining_initial_budget=2)
    assert len(m.calls) == 2
    assert [call[0][0]["decay"] for call in m.calls] == [0, 4]


def test_v21_candidate_carries_durable_settings_and_expected_key():
    p = parent()
    p["sim_key"] = "expected-key"
    out = _v21_candidate(p, "PPL")
    assert out["_expected_sim_key"] == "expected-key"
    assert out["_v22_settings"]["neutralization"] == "SUBINDUSTRY"


def test_external_evidence_is_scoped_to_expression_family_and_failure():
    p = parent()
    good = {"parent_expression": p["expression"], "parent_signal_family": p["signal_family"],
            "target_failure": "HT_RETURNS_RATIO_FAIL", "verdict": "TARGET_PASS"}
    other = dict(good, parent_expression="rank(other_field)")
    out = _matching_external_evidence([good, other], p, "HT_RETURNS_RATIO_FAIL")
    assert out == [good]


def test_stop_two_worse_must_be_same_strategy():
    assert evaluate_rescue_stop([("A", "WORSE"), ("B", "WORSE")], 2, 5) == "STOP_THIS_PARAMETER"
    assert evaluate_rescue_stop([("A", "WORSE"), ("A", "WORSE")], 2, 5) == "STOP_THIS_REPAIR_STRATEGY"


def test_manual_review_not_escalated_before_rescue_exhausted():
    out = manual_review_classify("STRONG_NEAR_PASS", 1, 5, [{"normalized_gap": 0.01}], {"sharpe": 2.0})
    assert out["manual_priority"] is None
    assert out["manual_review_reason"] == "AUTO_RESCUE_STILL_AVAILABLE"


def test_read_audit_log_returns_latest_matches(tmp_path):
    p = tmp_path / "a.log"
    p.write_text("\n".join(json.dumps({"timestamp": str(i), "level": "INFO", "action": "X", "run_id": "r"}) for i in range(6)) + "\n", encoding="utf-8")
    rows = list(read_audit_log(p, run_id="r", limit=2))
    assert [r["timestamp"] for r in rows] == ["5", "4"]
