"""Tests for Check-derived Repair Planning (Problem 2).

All offline: temporary databases and persisted check-result fixtures. No BRAIN
requests and no simulation POST are ever issued.
"""

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
import machine_lib_V2_1 as real_machine

from ppl_engine.check_derived_repair import (
    HT_RATIO_CANONICAL,
    HT_RATIO_FAILURE,
    derive_check_repair_proposals,
    ht_ratio_eligibility,
)
from ppl_engine.config import load_effective_config
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def make_alpha_db(path, rows=()):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, sharpe REAL, fitness REAL, turnover REAL)")
    c.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    for r in rows:
        c.execute("INSERT INTO alpha_results (sim_key,status,sharpe,fitness,turnover) VALUES (?,?,?,?,?)", r)
    c.commit(); c.close()


def _candidate(store, cid, _legacy_sim_key, alpha_id):
    expression = f"ts_mean({cid},2)"
    settings = real_machine.build_settings(
        {"expr": expression, "decay": 0}, neutralization="SUBINDUSTRY",
        region="GLB", universe="TOPDIV3000", delay=1, truncation=0.08, test_period="P0Y",
    )
    settings_json = json.dumps(settings, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    sim_key = real_machine.simulation_key(expression, settings)
    settings_hash = hashlib.sha256(settings_json.encode("utf-8")).hexdigest()
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_candidates(
                 candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
                 dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,
                 operator,window,decay,vector_reducer,data_field_count_estimate,pp_total_operator_count_estimate,
                 structure_status,alpha_id,repair_depth,lifecycle_state,simulation_status,
                 selected_for_initial_search,execution_action,cache_classification,
                 discovery_snapshot_id,dry_run_snapshot_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cid, "run_0002", expression, sim_key, settings_json, settings_hash, f"ctx_{cid}",
             "techindi_model", "predicted_first_quantile_one_day_return_2", "MATRIX", "RETURN", "NORMAL",
             f"techindi_model/f/IDENTITY/NORMAL/TS_MEAN", "TS_MEAN", "ts_mean", 2, 0, "IDENTITY",
             1, 0, "ELIGIBLE", alpha_id, 0, "PRE_TAG_CHECK_COMPLETE", "COMPLETE", 1,
             "CACHE_RESTORE", "CACHE_COMPLETE", "disc", "dry", NOW, NOW),
        )
    return sim_key


def _check_session(store, candidate_id, alpha_id, session_id="chk1", session_status="RESOLVED", base_gate="PENDING", theme_gate="WARNING"):
    with store.connect() as c:
        c.execute(
            "INSERT INTO ppl_check_sessions(check_session_id,run_id,candidate_id,alpha_id,phase,session_status,started_at,resolved_at,poll_count,http_request_count,pending_poll_requests,base_gate_result,theme_gate_result,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (session_id, "run_0002", candidate_id, alpha_id, "PRE_TAG", session_status, NOW, NOW, 1, 1, 0, base_gate, theme_gate, NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_check_polls(poll_id,check_session_id,phase,semantic_poll_index,http_request_delta,pending,created_at) VALUES (1,?, 'PRE_TAG', 1, 1, 0, ?)",
            (session_id, NOW),
        )


def _check_result(store, session_id, candidate_id, alpha_id, *, raw_name, normalized_name, category, raw_result, normalized_result, value=None, limit=None, eligibility_outcome=None, eligibility_reason=None):
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_check_results(
                 check_session_id,poll_id,candidate_id,alpha_id,phase,raw_name,normalized_name,category,
                 raw_result,normalized_result,raw_value_json,raw_limit_json,unit_confidence,parser_version,
                 alias_version,evidence_source,threshold_exceeded,eligibility_outcome,eligibility_reason,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (session_id, 1, candidate_id, alpha_id, "PRE_TAG", raw_name, normalized_name, category,
             raw_result, normalized_result, json.dumps(value) if value is not None else None,
             json.dumps(limit) if limit is not None else None, "UNKNOWN", 3, 3, "LIVE_VALIDATION",
             0, eligibility_outcome, eligibility_reason, NOW),
        )


def _operator(store, name="ts_delta", status="VERIFIED_PROJECT"):
    with store.connect() as c:
        c.execute("INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES (?,?,?,?)",
                  (name, "h", "CORE_VERIFIED_OPERATOR", status))


def _full_store(tmp_path, *, active_ht=True):
    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    conf = cfg()
    if active_ht:
        # Keep the generic/legacy HT repair machinery covered even though the
        # current simplified GLB Liquid theme no longer activates it.
        conf.rules["current_theme"]["local_preconditions"]["high_turnover"]["turnover"]["preset_min"] = 0.20
        req = conf.rules["current_theme"]["live_theme_checks"]["required"]
        if "HIGH_TURNOVER_RETURNS_RATIO" not in req:
            req.append("HIGH_TURNOVER_RETURNS_RATIO")
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
    _operator(s)  # ts_delta (used by V1 legacy tests if any)
    _operator(s, name="ts_target_tvr_decay", status="VERIFIED_PROJECT")  # V2 HT strategy operator
    return s, conf


def _ht_warning_check(store, session_id, candidate_id, alpha_id, value=0.7374, limit=0.75):
    _check_result(store, session_id, candidate_id, alpha_id, raw_name="SUB_UNIVERSE", normalized_name="SUB_UNIVERSE", category="PPL_BASE", raw_result="PASS", normalized_result="PASS", eligibility_outcome="PASS")
    _check_result(store, session_id, candidate_id, alpha_id, raw_name="POWER_POOL_CORRELATION", normalized_name="POWER_POOL_CORRELATION", category="PPL_BASE", raw_result="PASS", normalized_result="PASS", eligibility_outcome="PASS")
    _check_result(store, session_id, candidate_id, alpha_id, raw_name="HT_HIGH_TURNOVER_RETURNS_RATIO", normalized_name="HIGH_TURNOVER_RETURNS_RATIO", category="PPL_THEME", raw_result="WARNING", normalized_result="WARNING", value=value, limit=limit, eligibility_outcome="WARNING", eligibility_reason="PLATFORM_WARNING_REQUIRES_REVIEW")


# ---- ht_ratio_eligibility unit tests --------------------------------------

def _candidate_dict():
    return {"candidate_id": "c", "field_id": "f", "field_type": "MATRIX", "vector_reducer": "IDENTITY",
            "operator": "ts_mean", "window": 2, "direction": "NORMAL", "data_field_count_estimate": 1,
            "pp_total_operator_count_estimate": 0, "structure_status": "ELIGIBLE",
            "theme_settings_match": True, "dataset_allowed": True}


def _checks():
    return {
        "SUB_UNIVERSE": {"eligibility_outcome": "PASS"},
        "POWER_POOL_CORRELATION": {"eligibility_outcome": "PASS"},
        "HIGH_TURNOVER_RETURNS_RATIO": {"eligibility_outcome": "WARNING", "raw_result": "WARNING",
                                        "raw_value_json": 0.7374, "raw_limit_json": 0.75},
    }


def test_simplified_theme_disables_ht_ratio_repair(tmp_path):
    s, conf = _full_store(tmp_path, active_ht=False)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.42, "turnover": 0.6249}, _checks(), conf.rules)
    assert e["eligible"] is False
    assert e["reasons"] == ["THEME_SIGNAL_NOT_ACTIVE"]


def test_ht_ratio_eligible(tmp_path):
    s, conf = _full_store(tmp_path)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.42, "turnover": 0.6249}, _checks(), conf.rules)
    assert e["eligible"] is True and not e["reasons"]


def test_ht_ratio_sharpe_below_min(tmp_path):
    s, conf = _full_store(tmp_path)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 0.5, "turnover": 0.5}, _checks(), conf.rules)
    assert e["eligible"] is False and "SHARPE_BELOW_PPL_MIN" in e["reasons"]


def test_ht_ratio_turnover_below_theme_min(tmp_path):
    s, conf = _full_store(tmp_path)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.15}, _checks(), conf.rules)
    assert e["eligible"] is False and "TURNOVER_BELOW_THEME_MIN" in e["reasons"]


def test_ht_ratio_subuniverse_fail(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["SUB_UNIVERSE"] = {"eligibility_outcome": "FAIL"}
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "SUB_UNIVERSE_FAIL" in e["reasons"]


def test_ht_ratio_pp_corr_fail(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["POWER_POOL_CORRELATION"] = {"eligibility_outcome": "FAIL"}
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "PP_CORRELATION_FAIL" in e["reasons"]


def test_ht_ratio_pass_not_eligible(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = {"eligibility_outcome": "PASS", "raw_result": "PASS", "raw_value_json": 0.9, "raw_limit_json": 0.75}
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "HT_RATIO_NOT_WARNING_OR_FAIL" in e["reasons"]


def test_ht_ratio_raw_pending_not_pass(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = {"eligibility_outcome": "PENDING", "raw_result": "PENDING", "raw_value_json": None, "raw_limit_json": 0.75}
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "HT_RATIO_RAW_PENDING" in e["reasons"]


# ---- HT ratio threshold source (live raw_limit > preset fallback) ----------

def _ht_with_limit(value, limit):
    return {"eligibility_outcome": "WARNING", "raw_result": "WARNING", "raw_value_json": value, "raw_limit_json": limit}


def test_ht_ratio_live_limit_0_70(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.65, 0.70)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is True and e["context"]["ht_ratio_limit_source"] == "LIVE_RAW_LIMIT"


def test_ht_ratio_live_limit_0_70_value_above(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.72, 0.70)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "HT_RATIO_THRESHOLD_ALREADY_MET" in e["reasons"]


def test_ht_ratio_live_limit_0_75_near_miss(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.7374, 0.75)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is True and e["context"]["ht_ratio_limit"] == 0.75


def test_ht_ratio_live_limit_0_80(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.7374, 0.80)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is True and e["context"]["ht_ratio_limit"] == 0.80


def test_ht_ratio_fallback_when_live_limit_missing(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.7374, None)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    # fallback to preset_min (0.75 from rules), not a hardcoded literal
    assert e["context"]["ht_ratio_limit_source"] == "PRESET_FALLBACK"
    assert e["context"]["ht_ratio_limit"] == 0.75
    assert e["eligible"] is True  # 0.7374 < 0.75


def test_ht_ratio_fallback_value_above_preset(tmp_path):
    s, conf = _full_store(tmp_path)
    checks = _checks(); checks["HIGH_TURNOVER_RETURNS_RATIO"] = _ht_with_limit(0.78, None)
    e = ht_ratio_eligibility(_candidate_dict(), {"sharpe": 2.0, "turnover": 0.5}, checks, conf.rules)
    assert e["eligible"] is False and "HT_RATIO_THRESHOLD_ALREADY_MET" in e["reasons"]


# ---- derive_check_repair_proposals integration -----------------------------

def test_derive_ht_proposal_no_simulation(tmp_path):
    s, conf = _full_store(tmp_path)
    sim_key = _candidate(s, "cand_zy", "sim_zy", "ZYEVroY0")
    _check_session(s, "cand_zy", "ZYEVroY0")
    _ht_warning_check(s, "chk1", "cand_zy", "ZYEVroY0", value=0.7374, limit=0.75)
    make_alpha_db(tmp_path / "a.db", [(sim_key, "COMPLETE", 2.42, 1.18, 0.6249)])

    out = derive_check_repair_proposals(s, conf, tmp_path / "a.db", "run_0002", persist=True)
    assert out["simulation_posts"] == 0 and out["repair_budget_consumed"] == 0
    assert out["proposals_generated"] == 0
    with s.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM ppl_repair_plans WHERE run_id='run_0002'").fetchone()[0] == 0
    out2 = derive_check_repair_proposals(s, conf, tmp_path / "a.db", "run_0002", persist=True)
    assert out2["proposals_generated"] == 0
    with s.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM ppl_repair_plans WHERE run_id='run_0002'").fetchone()[0] == 0


def test_derive_skips_ht_pass_candidate(tmp_path):
    s, conf = _full_store(tmp_path)
    sim_key = _candidate(s, "cand_pass", "sim_pass", "APASS")
    _check_session(s, "cand_pass", "APASS")
    _check_result(s, "chk1", "cand_pass", "APASS", raw_name="SUB_UNIVERSE", normalized_name="SUB_UNIVERSE", category="PPL_BASE", raw_result="PASS", normalized_result="PASS", eligibility_outcome="PASS")
    _check_result(s, "chk1", "cand_pass", "APASS", raw_name="POWER_POOL_CORRELATION", normalized_name="POWER_POOL_CORRELATION", category="PPL_BASE", raw_result="PASS", normalized_result="PASS", eligibility_outcome="PASS")
    _check_result(s, "chk1", "cand_pass", "APASS", raw_name="HT_HIGH_TURNOVER_RETURNS_RATIO", normalized_name="HIGH_TURNOVER_RETURNS_RATIO", category="PPL_THEME", raw_result="PASS", normalized_result="PASS", value=0.9, limit=0.75, eligibility_outcome="PASS")
    make_alpha_db(tmp_path / "a.db", [(sim_key, "COMPLETE", 2.0, 1.0, 0.5)])

    out = derive_check_repair_proposals(s, conf, tmp_path / "a.db", "run_0002", persist=True)
    assert out["proposals_generated"] == 0
    assert out["skipped"] and "HT_RATIO_NOT_WARNING_OR_FAIL" in out["skipped"][0]["reasons"]


def test_derive_skips_unresolved_session(tmp_path):
    s, conf = _full_store(tmp_path)
    sim_key = _candidate(s, "cand_ur", "sim_ur", "AUR")
    _check_session(s, "cand_ur", "AUR", session_status="PENDING")
    _ht_warning_check(s, "chk1", "cand_ur", "AUR")
    make_alpha_db(tmp_path / "a.db", [(sim_key, "COMPLETE", 2.42, 1.18, 0.6249)])
    out = derive_check_repair_proposals(s, conf, tmp_path / "a.db", "run_0002", persist=True)
    # unresolved sessions are excluded entirely by the read query
    assert out["resolved_sessions"] == 0 and out["proposals_generated"] == 0


def test_derive_skips_local_gate_fail(tmp_path):
    s, conf = _full_store(tmp_path)
    sim_key = _candidate(s, "cand_fail", "sim_fail", "AFAIL")
    with s.connect() as c:
        c.execute("UPDATE ppl_candidates SET structure_status='LOCAL_STRUCTURE_REJECTED' WHERE candidate_id='cand_fail'")
    _check_session(s, "cand_fail", "AFAIL")
    _ht_warning_check(s, "chk1", "cand_fail", "AFAIL")
    make_alpha_db(tmp_path / "a.db", [(sim_key, "COMPLETE", 2.42, 1.18, 0.6249)])
    out = derive_check_repair_proposals(s, conf, tmp_path / "a.db", "run_0002", persist=True)
    assert out["proposals_generated"] == 0
    assert any("LOCAL_GATE_NOT_PASS" in x.get("reasons", []) for x in out["skipped"])
