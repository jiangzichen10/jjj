import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import machine_lib_V2_1 as ml
from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.diagnosis import diagnose_evidence, evaluate_local_pre_gate, turnover_class
from ppl_engine.repair_engine import (
    detect_cycle, evaluate_repair_side_effect, materialize_repair_candidate,
    operator_gate, plan_repairs, repair_signature,
)
from ppl_engine.store import RunnerStore, SCHEMA_VERSION

ROOT=Path(__file__).resolve().parents[1]; FIX=ROOT/"tests"/"fixtures"
def cfg(active_ht=False):
    c=load_effective_config(ROOT/"ppl_rules.yaml",ROOT/"ppl_plan.yaml",project_dir=ROOT)
    if active_ht:
        req=c.rules["current_theme"]["live_theme_checks"]["required"]
        for name in ("HIGH_TURNOVER", "HIGH_TURNOVER_RETURNS_RATIO"):
            if name not in req: req.append(name)
    return c
def candidate(**kw):
    x=dict(candidate_id="c",dataset_id="ds",field_id="x",field_type="MATRIX",vector_reducer="IDENTITY",operator="raw",window=None,direction="NORMAL",expression="x",expression_hash="h",signal_family="ds/x/NORMAL/RAW",repair_depth=0,repair_path=["RAW"],decay=0,structure_status="ELIGIBLE",pp_total_operator_count_estimate=0,data_field_count_estimate=1)
    x.update(kw)
    settings = ml.build_settings(
        {"expr": x["expression"], "decay": int(x.get("decay") or 0)},
        neutralization="SUBINDUSTRY", region="GLB", universe="TOPDIV3000",
        delay=1, truncation=0.08, test_period="P0Y",
    )
    x.setdefault("settings_json", json.dumps(settings, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    x.setdefault("sim_key", ml.simulation_key(x["expression"], settings))
    return x
def test_repair_materialization_rejects_compact_settings_builder(monkeypatch):
    parent = candidate()
    spec = {
        "expression_preview": "rank(x)", "repair_type": "TEST", "repair_signature": "sig",
        "repair_depth": 1, "settings_override": {}, "operator_requirements": [],
        "transform_family_override": "rank", "window_override": None, "direction_override": None,
    }
    def compact_builder(candidate, **kwargs):
        return {
            "neutralization": kwargs["neutralization"], "region": kwargs["region"],
            "universe": kwargs["universe"], "delay": kwargs["delay"],
            "truncation": kwargs["truncation"], "testPeriod": kwargs["test_period"],
        }
    monkeypatch.setattr(ml, "build_settings", compact_builder)
    with pytest.raises(ConfigError, match="SIMULATION_SETTINGS_INCOMPLETE"):
        materialize_repair_candidate(parent, spec, cfg(), ml)


def diag(f): return {"primary_failure":f}

@pytest.mark.parametrize("value,expected",[(.6858,"TURNOVER_PASS"),(1.4035,"TURNOVER_ABOVE_BASE_MAX"),(.1053,"TURNOVER_PASS"),(.20,"TURNOVER_PASS"),(.70,"TURNOVER_PASS"),(.009,"TURNOVER_BELOW_BASE_MIN")])
def test_turnover_classes(value,expected): assert turnover_class(value,cfg().rules)==expected

@pytest.mark.parametrize("sharpe,turnover,status",[(1,.2,"PASS"),(1,.7,"PASS"),(.99,.4,"FAIL"),(1,.71,"FAIL"),(1,.1,"PASS")])
def test_local_gate(sharpe,turnover,status):
    out=evaluate_local_pre_gate(candidate(),{"sharpe":sharpe,"turnover":turnover},cfg().rules)
    assert out["status"]==status and "POWER_POOL_CORRELATION" in out["unknown_live_facts"]

def test_negative_precedes_turnover():
    d=diagnose_evidence({"candidate":candidate(),"metrics":{"sharpe":-1.38,"turnover":1.4}},cfg().rules)
    assert d["primary_failure"]=="NEGATIVE_STRONG_SIGNAL" and "TURNOVER_ABOVE_BASE_MAX" in d["secondary_failures"]

def test_near_pass_and_weak_structural():
    assert diagnose_evidence({"candidate":candidate(),"metrics":{"sharpe":.85,"turnover":.4}},cfg().rules)["primary_failure"]=="SHARPE_NEAR_PASS"
    assert diagnose_evidence({"candidate":candidate(),"metrics":{"sharpe":.2,"turnover":.9}},cfg().rules)["primary_failure"]=="WEAK_SIGNAL_STRUCTURAL"

def test_generic_sharpe_does_not_trigger():
    env={"phase":"FINAL","candidate":candidate(),"metrics":{"sharpe":1.17,"turnover":.5},"check_results":[{"category":"REGULAR_ALPHA","normalized_name":"SHARPE","normalized_result":"FAIL"}]}
    assert diagnose_evidence(env,cfg().rules)["primary_failure"]=="NO_FAILURE"

@pytest.mark.parametrize("category,name,phase,expected",[("REGULAR_ALPHA","PROD_CORRELATION","FINAL","NO_FAILURE"),("PPL_BASE","SUB_UNIVERSE","PRE_TAG","SUB_UNIVERSE_FAIL"),("PPL_THEME","HIGH_TURNOVER_RETURNS_RATIO","PRE_TAG","NO_FAILURE"),("UNKNOWN","UNKNOWN","FINAL","UNKNOWN_CHECK")])
def test_check_triple_identity(category,name,phase,expected):
    env={"phase":phase,"candidate":candidate(),"metrics":{"sharpe":1.2,"turnover":.4},"check_results":[{"category":category,"normalized_name":name,"normalized_result":"FAIL"}]}
    assert diagnose_evidence(env,cfg().rules)["primary_failure"]==expected

def test_ht_ratio_failure_only_when_theme_explicitly_activates_it():
    env={"phase":"PRE_TAG","candidate":candidate(),"metrics":{"sharpe":1.2,"turnover":.4},"check_results":[{"category":"PPL_THEME","normalized_name":"HIGH_TURNOVER_RETURNS_RATIO","normalized_result":"FAIL"}]}
    assert diagnose_evidence(env,cfg(active_ht=True).rules)["primary_failure"]=="HT_RETURNS_RATIO_FAIL"


def test_pp_corr_phase_and_severity():
    base={"candidate":candidate(),"metrics":{"sharpe":1.2,"turnover":.4},"check_results":[{"category":"PPL_BASE","normalized_name":"POWER_POOL_CORRELATION","normalized_result":"FAIL","raw_value":.88,"raw_limit":.5}]}
    assert diagnose_evidence({**base,"phase":"PRE_TAG"},cfg().rules)["primary_failure"]=="NO_FAILURE"
    assert diagnose_evidence({**base,"phase":"FINAL"},cfg().rules)["primary_failure"]=="STRUCTURAL_CORRELATION_FAIL"

def test_theme_unknown_manual_review():
    x={"phase":"FINAL","candidate":candidate(),"metrics":{"sharpe":1.2,"turnover":.4},"check_results":[{"category":"PPL_THEME","normalized_name":"THEME_MATCH","normalized_result":"FAIL"}]}
    d=diagnose_evidence(x,cfg().rules); assert d["root_cause"]=="UNKNOWN" and d["repairability"]=="MANUAL_REVIEW"

def test_reverse_once_and_not_double_negative():
    p=plan_repairs(candidate(),diag("NEGATIVE_STRONG_SIGNAL"),cfg().rules,registry={})
    assert len(p["plans"])==1 and p["plans"][0]["expression_preview"]=="-(x)"
    p2=plan_repairs(candidate(direction="REVERSE",repair_path=["RAW","REVERSE_DIRECTION"]),diag("NEGATIVE_STRONG_SIGNAL"),cfg().rules)
    assert not p2["plans"] and p2["stop_reason"]=="REVERSE_ALREADY_USED"

def test_high_turnover_stage1_preserves_expression_and_adds_decay_two():
    p=plan_repairs(candidate(operator="ts_mean",window=4,expression="ts_mean(x, 4)"),diag("TURNOVER_ABOVE_BASE_MAX"),cfg().rules,registry={"ts_mean":"VERIFIED_PROJECT"})
    assert p["plans"][0]["expression_preview"]=="ts_mean(x, 4)"
    assert p["plans"][0]["settings_override"]=={"decay":2}
    assert p["plans"][0]["repair_type"]=="TURNOVER_DECAY_STEP_1"

def test_high_turnover_stage1_does_not_rebuild_parent_expression():
    p=plan_repairs(candidate(field_type="VECTOR",vector_reducer="VEC_SUM"),diag("TURNOVER_ABOVE_BASE_MAX"),cfg().rules,registry={"ts_mean":"VERIFIED_PROJECT"})
    assert p["plans"][0]["expression_preview"]=="x"

def test_materialize_uses_v21_identity():
    parent=candidate(); spec=plan_repairs(parent,diag("SHARPE_NEAR_PASS"),cfg().rules,registry={"rank":"VERIFIED_PROJECT"})["plans"][0]
    child=materialize_repair_candidate(parent,spec,cfg(),ml)
    assert child["stage"]=="REPAIR" and child["sim_key"]==ml.simulation_key(child["expr"],child["settings"]) and child["inherits_power_pool_tag"] is False

def test_signature_stable_and_sensitive():
    a=repair_signature(candidate(),"R","F",{"w":2},{})
    assert a==repair_signature(candidate(),"R","F",{"w":2},{}) and a!=repair_signature(candidate(),"R","F",{"w":3},{})

def test_duplicate_signature_and_depth():
    p=plan_repairs(candidate(),diag("SHARPE_NEAR_PASS"),cfg().rules,registry={"rank":"VERIFIED_PROJECT"})
    sig=p["plans"][0]["repair_signature"]
    p2=plan_repairs(candidate(),diag("SHARPE_NEAR_PASS"),cfg().rules,registry={"rank":"VERIFIED_PROJECT"},existing_signatures={sig})
    assert p2["plans"][0]["plan_status"]=="BLOCKED_CYCLE"
    assert plan_repairs(candidate(repair_depth=4),diag("SHARPE_NEAR_PASS"),cfg().rules)["stop_reason"]=="STOP_REPAIR_DEPTH"

def test_repeat_and_oscillation():
    r=cfg().rules
    assert detect_cycle(["RAW","TURNOVER_ABOVE_BASE_MAX:A","TURNOVER_ABOVE_BASE_MAX:B"],"TURNOVER_ABOVE_BASE_MAX",r)=="STOP_REPEATED_FAILURE"
    assert detect_cycle(["RAW","TURNOVER_ABOVE_BASE_MAX:A","TURNOVER_BELOW_THEME_MIN:B"],"TURNOVER_ABOVE_BASE_MAX",r)=="REPAIR_OSCILLATION_DETECTED"

@pytest.mark.parametrize("status,expected",[("VERIFIED_PROJECT","READY"),("VERIFIED_API","READY"),("VALIDATED_SINGLE","READY"),("UNVERIFIED","BLOCKED_OPERATOR_VALIDATION"),("NEEDS_REVALIDATION","BLOCKED_OPERATOR_VALIDATION"),("UNAVAILABLE","STOP_OPERATOR_UNAVAILABLE")])
def test_operator_gate(status,expected): assert operator_gate(["op"],{"op":status})["status"]==expected

def test_budget_and_cache_resume():
    reg={"rank":"VERIFIED_PROJECT","zscore":"VERIFIED_PROJECT","ts_mean":"VERIFIED_PROJECT","ts_rank":"VERIFIED_PROJECT"}
    blocked=plan_repairs(candidate(),diag("SHARPE_NEAR_PASS"),cfg().rules,registry=reg,repair_reserve_remaining=0)
    assert all(x["plan_status"]=="BLOCKED_BUDGET" for x in blocked["plans"])
    cached=plan_repairs(candidate(),diag("SHARPE_NEAR_PASS"),cfg().rules,registry=reg,cache_by_expression={"rank(x)":"CACHE_COMPLETE","zscore(x)":"RESUME_EXISTING"})
    assert cached["plans"][0]["projected_new_post"]==0 and cached["plans"][1]["projected_new_post"]==0 and cached["repair_consumed_posts"]==0

@pytest.mark.parametrize("metric,tol",[("sharpe",.10),("fitness",.20),("margin",.15)])
def test_side_effect_tolerances(metric,tol):
    parent={metric:1,"local_gate_status":"PASS"}; child={metric:1-tol,"local_gate_status":"PASS","primary_failure_resolved":True}
    assert evaluate_repair_side_effect(parent,child,"X",cfg().rules)["verdict"]=="ACCEPT"
    child[metric]=1-tol-.01; assert evaluate_repair_side_effect(parent,child,"X",cfg().rules)["verdict"]=="REJECT"

def test_side_effect_hard_checks_and_phase():
    assert evaluate_repair_side_effect({"local_gate_status":"PASS"},{"local_gate_status":"FAIL","primary_failure_resolved":True},"X",cfg().rules)["verdict"]=="REJECT"
    assert evaluate_repair_side_effect({"ht_ratio_status":"PASS"},{"ht_ratio_status":"FAIL","primary_failure_resolved":True},"X",cfg().rules)["verdict"]=="REJECT"
    assert evaluate_repair_side_effect({"sub_universe_status":"PASS"},{"sub_universe_status":"FAIL","primary_failure_resolved":True},"X",cfg().rules)["verdict"]=="REJECT"
    assert evaluate_repair_side_effect({"check_phase":"FINAL","pp_corr":.8},{"check_phase":"PRE_TAG","primary_failure_resolved":True},"X",cfg().rules)["verdict"]=="PENDING_RESULT"

def test_wjax_fixture_structural_no_neighbors():
    env=json.loads((FIX/"check_high_corr_WjAxWeoG.json").read_text())
    from ppl_engine.check_parser import parse_check_payload
    env["parsed"]=parse_check_payload(env,phase="FINAL",rules=cfg().rules,evidence_source=env["evidence_source"])
    d=diagnose_evidence(env,cfg().rules); p=plan_repairs(candidate(),d,cfg().rules)
    assert d["primary_failure"]=="STRUCTURAL_CORRELATION_FAIL" and p["stop_reason"]=="STOP_LOCAL_VARIANTS"
    assert not any("TS_MEAN" in json.dumps(x) for x in p["plans"])

@pytest.mark.parametrize("fixture,failure",[("diagnosis_near_pass.json","SHARPE_NEAR_PASS"),("diagnosis_turnover_high.json","TURNOVER_ABOVE_BASE_MAX"),("diagnosis_turnover_theme_low.json","NO_FAILURE"),("diagnosis_vwap_path.json","NEGATIVE_STRONG_SIGNAL")])
def test_preview_fixtures(fixture,failure):
    env=json.loads((FIX/fixture).read_text()); assert diagnose_evidence(env,cfg().rules)["primary_failure"]==failure

def test_schema_and_preview_no_persistence(tmp_path):
    db=tmp_path/"r.db"; RunnerStore(db).initialize()
    with sqlite3.connect(db) as c:
        assert c.execute("select max(schema_version) from ppl_schema_meta").fetchone()[0]==SCHEMA_VERSION
        assert c.execute("select count(*) from ppl_diagnoses").fetchone()[0]==0
        assert c.execute("select count(*) from ppl_repair_plans").fetchone()[0]==0
    result=subprocess.run([sys.executable,str(ROOT/"ppl_runner.py"),"--diagnose-preview","--repair-preview","--fixture",str(FIX/"diagnosis_near_pass.json"),"--db",str(db)],capture_output=True,text=True,cwd=ROOT)
    assert result.returncode==0 and '"network_requests": 0' in result.stdout
    with sqlite3.connect(db) as c: assert c.execute("select count(*) from ppl_diagnoses").fetchone()[0]==0
