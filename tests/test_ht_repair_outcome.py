"""Tests for the Repair -> PRE-TAG check auto-loop and HT-ratio repair outcome.

All offline: temporary databases, mocked simulation adapter and mocked check
transport.  No real BRAIN requests are ever issued.
"""

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
import machine_lib_V2_1 as real_machine

from ppl_engine.check_derived_repair import evaluate_ht_repair_outcome
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


def _full_settings(*, expr="ts_mean(f1, 2)", decay=0):
    from ppl_engine.config import load_effective_config
    conf = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)
    defaults = conf.plan["simulation_settings"]
    return real_machine.build_settings(
        {"expr": expr, "decay": decay},
        neutralization=defaults["neutralization"], region=defaults["region"],
        universe=defaults["universe"], delay=int(defaults["delay"]),
        truncation=float(defaults["truncation"]),
        test_period=defaults.get("test_period", defaults.get("testPeriod", "P0Y")),
    )


@pytest.fixture(autouse=True)
def _expected_machine_hash_for_non_hash_repair_tests(monkeypatch):
    """These tests target repair behavior; strict hash scope has its own regression suite."""
    from ppl_engine.live_execution import EXPECTED_MACHINE_HASH
    monkeypatch.setattr("ppl_engine.live_execution._hash_file", lambda _path: EXPECTED_MACHINE_HASH)


# ---- HT-ratio outcome verdict (pure, DB-backed) ----------------------------

def _make_store(tmp_path):
    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    return s


def _insert_check(store, session_id, run_id, candidate_id, alpha_id, ht_value, ht_limit):
    # Insert a RESOLVED PRE-TAG session + one HT-ratio result directly (no
    # foreign-key enforcement so we can omit poll rows in the fixture).
    conn = sqlite3.connect(str(store.path))
    conn.execute(
        "INSERT INTO ppl_check_sessions(check_session_id,run_id,candidate_id,alpha_id,phase,session_status,"
        "started_at,resolved_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (session_id, run_id, candidate_id, alpha_id, "PRE_TAG", "RESOLVED", NOW, NOW, NOW, NOW),
    )
    conn.execute(
        "INSERT INTO ppl_check_results(check_session_id,poll_id,candidate_id,alpha_id,phase,normalized_name,"
        "category,normalized_result,unit_confidence,parser_version,alias_version,evidence_source,created_at,"
        "raw_value_json,raw_limit_json,eligibility_outcome,raw_result) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (session_id, 1, candidate_id, alpha_id, "PRE_TAG", "HIGH_TURNOVER_RETURNS_RATIO",
         "THEME", "WARNING", "HIGH", 1, 1, "LIVE_CHECK", NOW,
         json.dumps(ht_value), json.dumps(ht_limit), "WARNING", "WARNING"),
    )
    conn.commit()
    conn.close()


def test_verdict_target_pass(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.7374, 0.75)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.76, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "child")
    assert out["verdict"] == "TARGET_PASS"
    assert out["parent_ht_ratio"] == 0.7374 and out["child_ht_ratio"] == 0.76
    assert out["delta_ht_ratio"] == 0.0226


def test_verdict_improved(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.7374, 0.75)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.74, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "child")
    assert out["verdict"] == "IMPROVED"
    assert out["delta_ht_ratio"] == 0.0026


def test_verdict_no_improvement(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.7374, 0.75)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.7374, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "child")
    assert out["verdict"] == "NO_IMPROVEMENT"
    assert out["delta_ht_ratio"] == 0.0


def test_verdict_worse(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.7374, 0.75)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.6514, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "child")
    assert out["verdict"] == "WORSE"
    assert out["delta_ht_ratio"] == -0.086


def test_verdict_child_missing(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.7374, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "missing_child")
    assert out["verdict"] == "UNKNOWN"
    assert out["reason"] == "CHILD_HT_RATIO_MISSING"


def test_verdict_parent_missing(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.6514, 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "missing_parent", "child")
    assert out["verdict"] == "UNKNOWN"
    assert out["reason"] == "PARENT_HT_RATIO_MISSING"


def test_verdict_uses_live_limit_not_fixed_constant(tmp_path):
    s = _make_store(tmp_path)
    _insert_check(s, "chk_p", "run_0002", "parent", "A1", 0.70, 0.80)
    _insert_check(s, "chk_c", "run_0002", "child", "A2", 0.78, 0.80)
    # 0.78 < 0.80 -> not TARGET_PASS even though 0.78 > 0.75 (no hardcoded 0.75)
    out = evaluate_ht_repair_outcome(s, "run_0002", "parent", "child")
    assert out["verdict"] == "IMPROVED"
    assert out["live_limit"] == 0.80


# ---- auto GET-only PRE-TAG check loop --------------------------------------

def _mock_materialize(monkeypatch, child_sim_key="child_key"):
    monkeypatch.setattr(
        "ppl_engine.production_repair.materialize_repair_candidate",
        lambda *a, **k: {"sim_key": child_sim_key, "settings": _full_settings(), "expr": "ts_mean(f1, 2)",
                         "operator": "ts_mean", "window": 2, "decay": 0},
    )


def _fake_instrument():
    @contextmanager
    def _cm(machine, store, by_key, stop_event=None, run_id=None, **kwargs):
        yield []
    return _cm


class _FakeMachine:
    def simulate_candidates(self, *a, **k):
        return []


def _make_full_store(tmp_path):
    from ppl_engine.config import load_effective_config

    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    conf = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        c.execute(
            "INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,"
            "context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,"
            "transform_family,operator,window,decay,vector_reducer,root_candidate_id,parent_candidate_id,"
            "parent_sim_key,repair_depth,lifecycle_state,simulation_status,selected_for_initial_search,"
            "execution_action,cache_classification,discovery_snapshot_id,dry_run_snapshot_id,"
            "structure_status,data_field_count_estimate,pp_total_operator_count_estimate,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("cand_parent", "run_0002", "rank(parent_key)", "parent_key", "{}", "sh", "ctx_parent",
             "pv30", "f1", "MATRIX", "RETURN", "NORMAL", "pv30/f1/IDENTITY/NORMAL/RANK", "RANK", "rank",
             None, 0, "IDENTITY", "cand_parent", None, None, 0, "SIMULATION_COMPLETE", "COMPLETE", 1,
             "CACHE_RESTORE", "CACHE_COMPLETE", "disc", "dry", "ELIGIBLE", 1, 1, NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,"
            "discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) "
            "VALUES ('prov_parent','cand_parent','run_0002','parent_key','ctx_parent','disc','dry','{}',?,?)", (NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES ('rank','h','CORE_VERIFIED_OPERATOR','VERIFIED_PROJECT')"
        )
        spec = {
            "repair_type": "SHARPE_RANK", "repair_signature": "sig1", "repair_depth": 1,
            "expression_preview": "rank(f1)",
            "repair_path": ["RAW", "SHARPE_NEAR_PASS:SHARPE_RANK"],
            "direction_override": None, "transform_family_override": "rank", "window_override": None,
            "operator_requirements": ["rank"],
        }
        c.execute(
            "INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,"
            "target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,"
            "operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,"
            "blocked_reason,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("rplan_test", "diag1", "run_0002", "cand_parent", "cand_parent", "SHARPE_NEAR_PASS",
             "SHARPE_RANK", "sig1", json.dumps(["RAW"]), 1, json.dumps(spec),
             json.dumps(["rank"]), "DEFERRED_INITIAL_SEARCH", 1, 0, 0, None, NOW, NOW),
        )
    return s, conf


def _make_alpha_db(path, rows=()):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, alpha_id TEXT, "
        "sharpe REAL, fitness REAL, turnover REAL, long_count INTEGER, short_count INTEGER)"
    )
    conn.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    for r in rows:
        conn.execute(
            "INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) "
            "VALUES (?,?,?,?,?,?,?,?,?)", r,
        )
    conn.commit()
    conn.close()


def test_execute_local_gate_pass_triggers_get_only_check(tmp_path, monkeypatch):
    from ppl_engine.production_repair import execute_production_repair

    s, c = _make_full_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_execute(candidates, config, machine, *, session, cache_db, allow_simulation_post, remaining_initial_budget, stop_event=None):
        con = sqlite3.connect(cache_db)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close()
        return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    calls = []
    def fake_check(store, config, machine, session, run_id, candidate_ids, *, source, evidence_source):
        calls.append({"candidate_ids": candidate_ids, "source": source, "evidence_source": evidence_source})
        return {"executed": True, "count": 1, "http_methods": ["GET"]}
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", fake_check)

    out = execute_production_repair(s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    assert len(calls) >= 1
    assert calls[0]["source"] == "PRODUCTION_REPAIR"
    assert calls[0]["evidence_source"] == "LIVE_CHECK"
    assert len(out["check_reports"]) >= 1
    # never auto-recurses: no second repair plan was scheduled
    assert out["post_consumed"] == 1


def test_execute_no_session_does_not_check(tmp_path, monkeypatch):
    from ppl_engine.production_repair import execute_production_repair

    s, c = _make_full_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21",
                        lambda *a, **k: [])
    calls = []
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check",
                        lambda *a, **k: calls.append(1) or {"executed": True})
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                              "run_0002", ["rplan_test"], True)
    assert calls == []  # no session -> no check, backward compatible
