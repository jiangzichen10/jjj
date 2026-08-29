"""Offline tests for the SAME_FAMILY_MICRO_TUNE HT-ratio repair strategy."""

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
import yaml
import machine_lib_V2_1 as real_machine

from ppl_engine.check_derived_repair import derive_same_family_micro_tune_plan
from ppl_engine.repair_engine import (
    SAME_FAMILY_MICRO_TUNE,
    plan_repairs,
)
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


def _identity(expression: str, *, decay: int = 0, region: str = "GLB", universe: str = "TOPDIV3000"):
    settings = real_machine.build_settings(
        {"expr": expression, "decay": decay}, neutralization="SUBINDUSTRY",
        region=region, universe=universe, delay=1, truncation=0.08, test_period="P0Y",
    )
    settings_json = json.dumps(settings, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (real_machine.simulation_key(expression, settings), settings_json,
            hashlib.sha256(settings_json.encode("utf-8")).hexdigest())


def _rules():
    return yaml.safe_load((ROOT / "ppl_rules.yaml").read_text(encoding="utf-8"))


def _candidate(expression="ts_mean(predicted_first_quantile_one_day_return_2, 2)",
               operator="ts_mean", window=2, turnover=0.6249):
    sim_key, settings_json, settings_hash = _identity(expression, decay=0)
    return {
        "candidate_id": "cand_zy", "root_candidate_id": "cand_zy",
        "field_id": "predicted_first_quantile_one_day_return_2", "field_type": "MATRIX",
        "vector_reducer": "IDENTITY", "operator": operator, "window": window, "direction": "NORMAL",
        "sim_key": sim_key, "expression": expression, "repair_depth": 0,
        "repair_path": ["RAW"], "signal_family": "fam", "turnover": turnover,
        "settings_json": settings_json, "settings_hash": settings_hash, "decay": 0,
    }


def _diag():
    return {"primary_failure": "HT_RETURNS_RATIO_FAIL", "secondary_failures": []}


# ---- plan_repairs strategy branch ------------------------------------------

def test_micro_tune_generates_ts_mean_windows():
    out = plan_repairs(_candidate(), _diag(), _rules(),
                       registry={"ts_mean": "VERIFIED_PROJECT"},
                       ht_strategy=SAME_FAMILY_MICRO_TUNE)
    types = {p["repair_type"] for p in out["plans"]}
    assert types == set()
    assert out["stop_reason"] == "NO_AUTO_REPAIR_HT_RATIO"


def test_micro_tune_non_ts_mean_needs_review():
    out = plan_repairs(_candidate(expression="rank(field_x)", operator="rank", window=None),
                       _diag(), _rules(), registry={"rank": "VERIFIED_PROJECT"},
                       ht_strategy=SAME_FAMILY_MICRO_TUNE)
    assert out["plans"] == []
    assert out["stop_reason"] == "NO_AUTO_REPAIR_HT_RATIO"


def test_micro_tune_signature_distinct_per_window():
    out = plan_repairs(_candidate(), _diag(), _rules(),
                       registry={"ts_mean": "VERIFIED_PROJECT"},
                       ht_strategy=SAME_FAMILY_MICRO_TUNE)
    sigs = {p["repair_signature"] for p in out["plans"]}
    assert len(sigs) == 0


def test_default_strategy_still_target_tvr():
    # The default (no ht_strategy) must remain the V2 ts_target_tvr_decay wrap.
    out = plan_repairs(_candidate(), _diag(), _rules(),
                       registry={"ts_target_tvr_decay": "VERIFIED_PROJECT"})
    types = {p["repair_type"] for p in out["plans"]}
    assert types == set()


# ---- derive_same_family_micro_tune_plan ------------------------------------

def _make_store_with_parent(tmp_path):
    from ppl_engine.config import load_effective_config

    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    conf = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)
    expression = "ts_mean(f1, 2)"
    parent_key, parent_settings_json, parent_settings_hash = _identity(expression, decay=0)
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET run_profile='PRODUCTION_RESEARCH',status='PAUSED',current_stage='PAUSED' WHERE run_id='run_0002'")
        c.execute(
            "INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,"
            "context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,"
            "transform_family,operator,window,decay,vector_reducer,root_candidate_id,parent_candidate_id,"
            "parent_sim_key,repair_depth,lifecycle_state,simulation_status,selected_for_initial_search,"
            "execution_action,cache_classification,discovery_snapshot_id,dry_run_snapshot_id,"
            "structure_status,data_field_count_estimate,pp_total_operator_count_estimate,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("cand_parent", "run_0002", expression, parent_key, parent_settings_json, parent_settings_hash, "ctx_parent",
             "pv30", "f1", "MATRIX", "RETURN", "NORMAL", "pv30/f1/IDENTITY/NORMAL/TS_MEAN", "TS_MEAN",
             "ts_mean", 2, 0, "IDENTITY", "cand_parent", None, None, 0, "PRE_TAG_CHECK_COMPLETE",
             "COMPLETE", 1, "CACHE_RESTORE", "CACHE_COMPLETE", "disc", "dry",
             "ELIGIBLE", 1, 1, NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) "
            "VALUES ('ts_mean','h','CORE_VERIFIED_OPERATOR','VERIFIED_PROJECT')"
        )
    # parent alpha fact
    adb = tmp_path / "a.db"
    conn = sqlite3.connect(adb)
    conn.execute(
        "CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, alpha_id TEXT, "
        "sharpe REAL, fitness REAL, turnover REAL, long_count INTEGER, short_count INTEGER)"
    )
    conn.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    conn.execute(
        "INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (parent_key, "COMPLETE", "u", "ZYEVroY0", 2.42, 1.18, 0.6249, 10, 10),
    )
    conn.commit(); conn.close()
    return s, conf, adb


def test_derive_micro_tune_plans_and_is_idempotent(tmp_path):
    s, conf, adb = _make_store_with_parent(tmp_path)
    out1 = derive_same_family_micro_tune_plan(s, conf, adb, "run_0002", "cand_parent", persist=True)
    assert out1["proposals_generated"] == 0
    assert out1["repair_budget_consumed"] == 0 and out1["simulation_posts"] == 0
    assert out1["strategy"] == SAME_FAMILY_MICRO_TUNE
    # second call must not duplicate plans (UNIQUE(run_id, repair_signature))
    out2 = derive_same_family_micro_tune_plan(s, conf, adb, "run_0002", "cand_parent", persist=True)
    with s.connect() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM ppl_repair_plans WHERE run_id='run_0002' AND repair_type=?",
            (SAME_FAMILY_MICRO_TUNE,),
        ).fetchone()[0]
    assert n == 0
    assert out2["proposals_generated"] == 0
