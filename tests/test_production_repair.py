"""Tests for the Production Repair execution entry (Problem 1).

All tests are offline: temporary databases, mocked V2.1 materialization and a
mocked simulation adapter. No real BRAIN requests are ever issued.
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
import machine_lib_V2_1 as real_machine

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.production_repair import (
    EXECUTABLE_STATUSES,
    PHASE,
    execute_production_repair,
    execute_round_repair,
    initial_search_completed,
    list_repair_plans,
    preview_production_repair,
    preflight_round_repair_execution,
    reconcile_completed_repair_outcomes,
    validate_production_repair_context,
)
from ppl_engine.round_store import create_round, load_batches, update_round
from ppl_engine.store import RunnerStore


@pytest.fixture(autouse=True)
def _expected_machine_hash_for_non_hash_repair_tests(monkeypatch):
    """Repair semantics run under expected hash; strict scope is tested separately."""
    from ppl_engine.live_execution import EXPECTED_MACHINE_HASH
    monkeypatch.setattr("ppl_engine.live_execution._hash_file", lambda _path: EXPECTED_MACHINE_HASH)

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def cfg_v31():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _full_settings(conf=None, *, expr="ts_mean(f1, 2)", decay=0):
    conf = conf or cfg()
    defaults = conf.plan["simulation_settings"]
    return real_machine.build_settings(
        {"expr": expr, "decay": decay},
        neutralization=defaults["neutralization"], region=defaults["region"],
        universe=defaults["universe"], delay=int(defaults["delay"]),
        truncation=float(defaults["truncation"]),
        test_period=defaults.get("test_period", defaults.get("testPeriod", "P0Y")),
    )


def make_alpha_db(path, rows=()):
    c = sqlite3.connect(path)
    c.execute(
        "CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, "
        "alpha_id TEXT, sharpe REAL, fitness REAL, turnover REAL, long_count INTEGER, short_count INTEGER)"
    )
    c.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    for r in rows:
        c.execute(
            "INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) "
            "VALUES (?,?,?,?,?,?,?,?,?)", r,
        )
    c.commit(); c.close()


def _parent_insert(c, cid, sim_key, *, selected=1, action="CACHE_RESTORE", lifecycle="SIMULATION_COMPLETE"):
    c.execute(
        """INSERT INTO ppl_candidates(
             candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
             dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,
             operator,window,decay,vector_reducer,root_candidate_id,parent_candidate_id,parent_sim_key,
             repair_depth,lifecycle_state,simulation_status,selected_for_initial_search,execution_action,
             cache_classification,discovery_snapshot_id,dry_run_snapshot_id,created_at,updated_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (cid, "run_0002", f"rank({sim_key})", sim_key, "{}", "sh", f"ctx_{cid}", "pv30", "f1",
         "MATRIX", "RETURN", "NORMAL", f"pv30/f1/IDENTITY/NORMAL/RANK", "RANK", "rank", None, 0,
         "IDENTITY", cid, None, None, 0, lifecycle, "COMPLETE", selected, action, "CACHE_COMPLETE",
         "disc", "dry", NOW, NOW),
    )


def make_store(tmp_path, *, plan_status="DEFERRED_INITIAL_SEARCH", repair_signature="sig1",
               parent_sim_key="parent_key", operator_name="rank", operator_status="VERIFIED_PROJECT",
               conf=None):
    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    conf = conf or cfg()
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        _parent_insert(c, "cand_parent", parent_sim_key)
        c.execute(
            "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) "
            "VALUES ('prov_parent','cand_parent','run_0002','parent_key','ctx_cand_parent','disc','dry','{}',?,?)", (NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES (?,?,?,?)",
            (operator_name, "h", "CORE_VERIFIED_OPERATOR", operator_status),
        )
        spec = {
            "repair_type": "SHARPE_RANK",
            "repair_signature": repair_signature,
            "repair_depth": 1,
            "expression_preview": "ts_mean(f1, 2)",
            "repair_path": ["RAW", "SHARPE_NEAR_PASS:SHARPE_RANK"],
            "direction_override": None,
            "transform_family_override": "rank",
            "window_override": None,
            "operator_requirements": [operator_name],
        }
        c.execute(
            """INSERT INTO ppl_repair_plans(
                 repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                 repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("rplan_test", "diag1", "run_0002", "cand_parent", "cand_parent", "SHARPE_NEAR_PASS",
             "SHARPE_RANK", repair_signature, json.dumps(["RAW"]), 1, json.dumps(spec),
             json.dumps([operator_name]), plan_status, 1, 0, 0, None, NOW, NOW),
        )
    return s, conf


def _mock_materialize(monkeypatch, child_sim_key="child_key"):
    monkeypatch.setattr(
        "ppl_engine.production_repair.materialize_repair_candidate",
        lambda *a, **k: {"sim_key": child_sim_key, "settings": _full_settings(), "expr": "ts_mean(f1, 2)",
                         "operator": "ts_mean", "window": 2, "decay": 0},
    )


def _add_second_convergent_plan(store):
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_repair_plans(
                 repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                 repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 blocked_reason,created_at,updated_at
               ) SELECT 'rplan_second',diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                        repair_type,'sig2',repair_path_json,repair_depth,candidate_spec_json,
                        operator_requirements_json,plan_status,projected_new_posts,0,0,NULL,created_at,updated_at
                 FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'"""
        )


def _add_round(store):
    create_round(
        store, round_id="round_run_0002", run_id="run_0002",
        policy={"objective": "test", "batch_size": 24},
        total_budget=60, search_budget=12, repair_budget=48,
    )
    update_round(store, "round_run_0002", status="RUNNING", phase="REPAIR", current_batch=0)


# ---- context / validation -------------------------------------------------

def test_context_requires_production_research(tmp_path):
    s, c = make_store(tmp_path)
    with s.connect() as con:
        con.execute("UPDATE ppl_runs SET run_profile='LIVE_VALIDATION' WHERE run_id='run_0002'")
    with pytest.raises(ConfigError, match="PRODUCTION_REPAIR_REQUIRES_PRODUCTION_RESEARCH"):
        validate_production_repair_context(s, c, "run_0002")


def test_context_unknown_run(tmp_path):
    s, c = make_store(tmp_path)
    with pytest.raises(ConfigError, match="Unknown run"):
        validate_production_repair_context(s, c, "run_9999")


def test_initial_search_completed(tmp_path):
    s, c = make_store(tmp_path)
    assert initial_search_completed(s, "run_0002") is True
    with s.connect() as con:
        con.execute("UPDATE ppl_candidates SET execution_action='NEW_SIMULATION_REQUIRED' WHERE candidate_id='cand_parent'")
    assert initial_search_completed(s, "run_0002") is False


# ---- preview --------------------------------------------------------------

def test_preview_requires_explicit_plan(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    with pytest.raises(ConfigError, match="EXPLICIT_PLAN_ID"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", [], object())


def test_preview_wrong_run_rejected(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    with pytest.raises(ConfigError, match="REPAIR_PLAN_NOT_FOUND"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_nonexistent"], object())


def test_preview_plan_from_other_run_rejected(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    # A plan scoped to a validation run must never be executable for run_0002.
    with s.connect() as con:
        con.execute("UPDATE ppl_repair_plans SET run_id='run_0004' WHERE repair_plan_id='rplan_test'")
    with pytest.raises(ConfigError, match="REPAIR_PLAN_NOT_FOUND"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())


def test_preview_no_post_and_shows_fields(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_missing")
    make_alpha_db(tmp_path / "a.db")
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    assert out["simulation_posts"] == 0 and out["network_requests"] == 0
    assert out["repair_reserve"] == 48 and out["repair_consumed"] == 0
    it = out["items"][0]
    for k in ("repair_plan_id", "parent_candidate_id", "parent_alpha_id", "current_plan_status",
              "target_failure", "repair_type", "repair_depth", "parent_expression", "repair_expression",
              "parent_sim_key", "repair_sim_key", "dataset_id", "field_id", "signal_family",
              "operator_gate", "cache_status", "required_action", "needs_promotion", "will_post"):
        assert k in it
    assert it["current_plan_status"] == "DEFERRED_INITIAL_SEARCH"
    assert it["required_action"] == "NEW_SIMULATION_REQUIRED" and it["will_post"] is True


def test_preview_cache_complete_no_post(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db", [("child_key", "COMPLETE", "u", "A1", 2.0, 1.0, 0.5, 10, 10)])
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    assert out["projected_new_posts"] == 0
    assert out["items"][0]["required_action"] == "CACHE_RESTORE" and out["items"][0]["will_post"] is False


def test_preview_running_resume_no_post(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db", [("child_key", "RUNNING", "u", None, None, None, None, None, None)])
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    assert out["items"][0]["required_action"] == "RESUME_EXISTING" and out["items"][0]["will_post"] is False


def test_preview_uncertain_hold(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db", [("child_key", "UNCERTAIN_SUBMISSION", None, None, None, None, None, None, None)])
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    assert out["items"][0]["required_action"] == "HOLD_UNCERTAIN" and out["items"][0]["will_post"] is False


def test_preview_already_consumed_rejected(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    with s.connect() as con:
        con.execute("UPDATE ppl_repair_plans SET consumed_posts=1 WHERE repair_plan_id='rplan_test'")
    with pytest.raises(ConfigError, match="ALREADY_CONSUMED"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())


def test_preview_operator_gate_blocked(tmp_path, monkeypatch):
    s, c = make_store(tmp_path, operator_name="ts_mean", operator_status="UNVERIFIED")
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    with pytest.raises(ConfigError, match="REPAIR_OPERATOR_GATE"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())


def test_preview_multi_plan_explicit_list(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    with s.connect() as con:
        con.execute(
            "INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,created_at,updated_at) "
                "VALUES ('rplan_2','diag2','run_0002','cand_parent','cand_parent','SHARPE_NEAR_PASS','X','sig2','[]',1,'{}','[\"rank\"]','DEFERRED_INITIAL_SEARCH',1,0,0,?,?)", (NOW, NOW),
        )
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test", "rplan_2"], object())
    assert out["selected_plan_count"] == 2


def test_historical_retired_strategy_is_rejected_at_execution_boundary(tmp_path):
    s, c = make_store(tmp_path)
    make_alpha_db(tmp_path / "a.db")
    with s.connect() as con:
        con.execute(
            "UPDATE ppl_repair_plans SET target_failure='HT_RETURNS_RATIO_FAIL',"
            "repair_type='HT_RATIO_TARGET_TVR' WHERE repair_plan_id='rplan_test'"
        )
    with pytest.raises(ConfigError, match="REPAIR_STRATEGY_RETIRED"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())


# ---- execute authorization + promotion ------------------------------------

def test_execute_requires_allow_flag(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch)
    make_alpha_db(tmp_path / "a.db")
    out = execute_production_repair(s, c, object(), None, tmp_path / "a.db", ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], False)
    assert out["executed"] is False
    # nothing mutated
    with s.connect() as con:
        assert con.execute("SELECT plan_status FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()[0] == "DEFERRED_INITIAL_SEARCH"


def test_execute_uncertain_hold_stops(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db", [("child_key", "UNCERTAIN_SUBMISSION", None, None, None, None, None, None, None)])
    with pytest.raises(ConfigError, match="UNCERTAIN"):
        execute_production_repair(s, c, object(), None, tmp_path / "a.db", ROOT / "machine_lib_V2_1.py",
                                  "run_0002", ["rplan_test"], True)


def test_execute_budget_exceeded_rejected(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db")
    with s.connect() as con:
        con.execute("UPDATE ppl_repair_plans SET consumed_posts=48 WHERE repair_plan_id='rplan_test'")
    with pytest.raises(ConfigError, match="ALREADY_CONSUMED"):
        execute_production_repair(s, c, object(), None, tmp_path / "a.db", ROOT / "machine_lib_V2_1.py",
                                  "run_0002", ["rplan_test"], True)


def _fake_instrument():
    @contextmanager
    def _cm(machine, store, by_key, stop_event=None, run_id=None, **kwargs):
        yield []
    return _cm


class _FakeMachine:
    def simulate_candidates(self, *a, **k):
        return []


def test_execute_confirmed_post_consumes_budget(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)

    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_execute(candidates, config, machine, *, session, cache_db, allow_simulation_post, remaining_initial_budget, stop_event=None):
        con = sqlite3.connect(cache_db)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close()
        return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)

    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    assert out["post_confirmed"] == 1 and out["post_consumed"] == 1
    with s.connect() as con:
        plan = con.execute("SELECT plan_status,consumed_posts,committed_posts FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()
        assert plan["plan_status"] == "EXECUTED" and plan["consumed_posts"] == 1
        child = con.execute("SELECT candidate_id FROM ppl_candidates WHERE run_id='run_0002' AND sim_key='child_key'").fetchone()
        assert child is not None
        # auditable transition recorded
        audit = con.execute("SELECT COUNT(*) FROM ppl_live_execution_audits WHERE run_id='run_0002' AND event_type='REPAIR_PLAN_STATUS'").fetchone()[0]
        assert audit >= 1


def test_convergent_plans_use_one_unique_remote_execution_and_budget_unit(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _add_second_convergent_plan(s)
    _mock_materialize(monkeypatch, child_sim_key="shared_child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    wrapper_counts = []

    def fake_execute(candidates, config, machine, *, session, cache_db, **kwargs):
        wrapper_counts.append(len(candidates))
        con = sqlite3.connect(cache_db)
        con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) VALUES ('shared_child_key','COMPLETE','u','A2')")
        con.commit(); con.close()
        return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    out = execute_production_repair(
        s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002",
        ["rplan_test", "rplan_second"], True,
    )
    assert out["unique_execution_unit_count"] == 1
    assert out["post_consumed"] == 1
    assert wrapper_counts == [1]
    assert out["execution_units"][0]["plan_ids"] == ["rplan_second", "rplan_test"]
    with s.connect() as con:
        plans = con.execute(
            "SELECT repair_plan_id,consumed_posts,blocked_reason FROM ppl_repair_plans "
            "WHERE repair_plan_id IN ('rplan_test','rplan_second') ORDER BY repair_plan_id"
        ).fetchall()
        assert sum(int(x["consumed_posts"]) for x in plans) == 1
        assert con.execute("SELECT count(*) FROM ppl_candidates WHERE sim_key='shared_child_key'").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM ppl_repairs WHERE run_id='run_0002'").fetchone()[0] == 2


def test_round_repair_toctou_cache_restore_resolves_durable_post_intent(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
    )
    con = sqlite3.connect(adb)
    con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) VALUES ('child_key','COMPLETE','u','A2')")
    con.commit(); con.close()
    posted = []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", lambda *a, **k: posted.append(1))
    out = execute_round_repair(
        s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", "round_run_0002", 1, ["rplan_test"], True, preflight=preflight,
    )
    assert out["post_consumed"] == 0 and posted == []
    batch = load_batches(s, "round_run_0002")[0]
    assert json.loads(batch["planned_post_sim_keys_json"]) == []
    with s.connect() as con:
        event = con.execute(
            "SELECT payload_json FROM ppl_round_events WHERE event_type='REPAIR_POST_INTENT_RESOLVED_NONPOST'"
        ).fetchone()
    assert event and json.loads(event[0])["final_action"] == "CACHE_RESTORE"


def test_v304o_round_repair_executor_explicitly_enables_simulation_delete(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
    )
    observed = []

    @contextmanager
    def instrument(machine, store, by_key, **kwargs):
        observed.append(kwargs.get("allow_simulation_delete"))
        yield []

    def fake_execute(candidates, config, machine, *, cache_db, **kwargs):
        con = sqlite3.connect(cache_db)
        con.execute(
            "INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) "
            "VALUES ('child_key','COMPLETE','u','A2')"
        )
        con.commit(); con.close()
        return []

    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", instrument)
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    execute_round_repair(
        s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", "round_run_0002", 1, ["rplan_test"], True,
        preflight=preflight,
    )
    assert observed == [True]


def test_round_repair_hash_guard_failure_creates_no_batch_intent(tmp_path, monkeypatch):
    from ppl_engine.config import config_with_machine_hash_policy_override
    s, c = make_store(tmp_path); _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    strict = config_with_machine_hash_policy_override(c, "STRICT")
    monkeypatch.setattr("ppl_engine.live_execution._hash_file", lambda _p: "A" * 64)
    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        preflight_round_repair_execution(
            s, strict, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
            run_id="run_0002", round_id="round_run_0002", batch_no=1,
            plan_ids=["rplan_test"], allow_simulation_post=True,
        )
    assert load_batches(s, "round_run_0002") == []


def test_execute_cache_restore_does_not_consume_budget(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb, [("child_key", "COMPLETE", "u", "A1", 2.0, 1.0, 0.5, 10, 10)])
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    posted = []
    def fake_execute(*a, **k):
        posted.append(1); return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    assert out["post_consumed"] == 0 and not posted
    with s.connect() as con:
        plan = con.execute("SELECT plan_status,consumed_posts,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()
        assert plan["plan_status"] == "EXECUTED"
        assert plan["consumed_posts"] == 0
        assert plan["blocked_reason"] == "CACHE_RESTORE_NO_POST"
        edge = con.execute("SELECT child_candidate_id,repair_signature FROM ppl_repairs WHERE run_id='run_0002' AND repair_signature='sig1'").fetchone()
        assert edge is not None and edge["repair_signature"] == "sig1"


# ---- REUSE_EXISTING_CANDIDATE semantics ------------------------------------

def _add_planned_candidate(store, cid, sim_key, *, lifecycle="PLANNED", sim_status="NONE", execution_action="NEW_SIMULATION_REQUIRED"):
    """Insert a never-simulated (PLANNED/NONE) candidate row sharing a target sim_key."""
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_candidates(
                 candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
                 dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,
                 operator,window,decay,vector_reducer,repair_depth,lifecycle_state,simulation_status,
                 selected_for_initial_search,execution_action,cache_classification,new_post_budget_consumed,
                 discovery_snapshot_id,dry_run_snapshot_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cid, "run_0002", f"ts_mean({sim_key},2)", sim_key, "{}", "sh", f"ctx_{cid}", "pv30", "f1",
             "MATRIX", "RETURN", "NORMAL", "pv30/f1/IDENTITY/NORMAL/TS_MEAN", "TS_MEAN", "ts_mean", 2, 0,
             "IDENTITY", 0, lifecycle, sim_status, 0, execution_action, "CACHE_MISS", 0, "disc", "dry", NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (f"prov_{cid}", cid, "run_0002", sim_key, f"ctx_{cid}", "disc", "dry", json.dumps({"candidate_stage": "PPL_INITIAL"}), NOW, NOW),
        )


def test_preview_reuse_existing_candidate(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db")  # CACHE_MISS for child_key
    _add_planned_candidate(s, "cand_existing", "child_key")
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    it = out["items"][0]
    assert it["required_action"] == "REUSE_EXISTING_CANDIDATE"
    assert it["reuse_existing_candidate"] is True and it["will_post"] is True
    assert it["existing_child_candidate_id"] == "cand_existing"
    assert out["projected_new_posts"] == 1
    assert out["simulation_posts"] == 0  # preview never POSTs


def test_preview_reuse_never_permanently_blocked(tmp_path, monkeypatch):
    # "Candidate row exists" must not block a never-POSTed sim_key forever.
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    make_alpha_db(tmp_path / "a.db")
    _add_planned_candidate(s, "cand_existing", "child_key")
    out = preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    assert out["items"][0]["required_action"] != "ALREADY_EXISTS"


def test_execute_reuse_does_not_create_duplicate_candidate(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    def fake_execute(*a, **k):
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    assert out["post_confirmed"] == 1 and out["post_consumed"] == 1
    with s.connect() as con:
        # exactly one candidate row with that sim_key (no duplicate created)
        n = con.execute("SELECT COUNT(*) FROM ppl_candidates WHERE run_id='run_0002' AND sim_key='child_key'").fetchone()[0]
        assert n == 1
        # plan EXECUTED with 1 consumed post
        plan = con.execute("SELECT plan_status,consumed_posts FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()
        assert plan["plan_status"] == "EXECUTED" and plan["consumed_posts"] == 1


def test_execute_reuse_links_existing_candidate(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    def fake_execute(*a, **k):
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                              "run_0002", ["rplan_test"], True)
    with s.connect() as con:
        row = con.execute("SELECT parent_candidate_id,parent_sim_key,repair_depth,lifecycle_state FROM ppl_candidates WHERE candidate_id='cand_existing'").fetchone()
        assert row["parent_candidate_id"] == "cand_parent"          # linked to repair parent
        assert row["parent_sim_key"] == "parent_key"
        assert row["repair_depth"] == 1
        prov = con.execute("SELECT provenance_json FROM ppl_candidate_provenance WHERE candidate_id='cand_existing'").fetchone()
        payload = json.loads(prov["provenance_json"])
        assert payload["repair_plan_id"] == "rplan_test" and payload["reuse_existing_candidate"] is True


def test_execute_reuse_sim_key_consistency(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    def fake_execute(*a, **k):
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                              "run_0002", ["rplan_test"], True)
    with s.connect() as con:
        row = con.execute("SELECT sim_key FROM ppl_candidates WHERE candidate_id='cand_existing'").fetchone()
        assert row["sim_key"] == "child_key"  # sim_key never mutated


def test_execute_reuse_second_call_no_second_post(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    posted = []
    def fake_execute(*a, **k):
        posted.append(1)
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                              "run_0002", ["rplan_test"], True)
    assert len(posted) == 1
    # second call: plan already EXECUTED → rejected, never POSTs a second time
    with pytest.raises(ConfigError, match="NOT_EXECUTABLE|ALREADY_CONSUMED"):
        execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                  "run_0002", ["rplan_test"], True)
    assert len(posted) == 1  # exactly one POST ever


# ---- state machine ---------------------------------------------------------

def test_transition_repair_plan_idempotent_and_audited(tmp_path):
    s, c = make_store(tmp_path)
    from ppl_engine.state_machine import REPAIR_PLAN_TRANSITIONS
    assert s.transition_repair_plan("rplan_test", "READY", reason="t", source=PHASE,
                                    allowed=REPAIR_PLAN_TRANSITIONS, expected_from=frozenset({"DEFERRED_INITIAL_SEARCH"})) is True
    # idempotent
    assert s.transition_repair_plan("rplan_test", "READY", reason="t", source=PHASE,
                                    allowed=REPAIR_PLAN_TRANSITIONS) is False
    with s.connect() as con:
        assert con.execute("SELECT plan_status FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()[0] == "READY"
        assert con.execute("SELECT COUNT(*) FROM ppl_live_execution_audits WHERE event_type='REPAIR_PLAN_STATUS'").fetchone()[0] == 1


def test_transition_repair_plan_rejects_illegal(tmp_path):
    s, c = make_store(tmp_path)
    from ppl_engine.state_machine import REPAIR_PLAN_TRANSITIONS
    with pytest.raises(ValueError, match="REJECTED"):
        s.transition_repair_plan("rplan_test", "EVALUATED_ACCEPT", reason="t", source=PHASE,
                                 allowed=REPAIR_PLAN_TRANSITIONS)


# ---- listing ---------------------------------------------------------------

def test_list_deferred_and_check_derived(tmp_path):
    s, c = make_store(tmp_path)
    with s.connect() as con:
        con.execute(
            "INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,created_at,updated_at) "
            "VALUES ('rplan_ht','diag_ht','run_0002','cand_parent','cand_parent','HT_RETURNS_RATIO_FAIL','HT_RATIO_SIGNAL_HORIZON','sig_ht','[]',1,'{}','[\"ts_delta\"]','PLANNED',1,0,0,?,?)", (NOW, NOW),
        )
    deferred = list_repair_plans(s, "run_0002", source="deferred")
    assert deferred["plan_count"] == 1 and deferred["plans"][0]["repair_plan_id"] == "rplan_test"
    checkd = list_repair_plans(s, "run_0002", source="check-derived")
    assert checkd["plan_count"] == 1 and checkd["plans"][0]["repair_plan_id"] == "rplan_ht"
    allp = list_repair_plans(s, "run_0002", source="all")
    assert allp["plan_count"] == 2


def test_execute_resume_completes_without_budget_or_second_post(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb, [("child_key", "RUNNING", "https://api.worldquantbrain.com/simulations/resume1", None, None, None, None, None, None)])
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    delegated = []

    def fake_resume(candidates, config, machine, *, session, cache_db, allow_simulation_post, remaining_initial_budget, stop_event=None):
        delegated.extend(x.get("execution_action") for x in candidates)
        con = sqlite3.connect(cache_db)
        con.execute("UPDATE alpha_results SET status='COMPLETE',alpha_id='A_RESUMED',sharpe=2.1,fitness=1.1,turnover=0.5,long_count=10,short_count=10 WHERE sim_key='child_key'")
        con.commit(); con.close()
        return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_resume)
    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    assert delegated == ["RESUME_EXISTING"]
    assert out["post_attempted"] == 0
    assert out["post_consumed"] == 0
    assert out["results"][0]["alpha_id"] == "A_RESUMED"
    with s.connect() as con:
        plan = con.execute("SELECT plan_status,consumed_posts,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'").fetchone()
        assert plan["plan_status"] == "EXECUTED"
        assert plan["consumed_posts"] == 0
        assert plan["blocked_reason"] == "RESUME_EXISTING_NO_POST"
        edge = con.execute("SELECT child_candidate_id FROM ppl_repairs WHERE run_id='run_0002' AND repair_signature='sig1'").fetchone()
        assert edge is not None


def test_repair_edge_preserves_existing_candidate_primary_lineage(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    with s.connect() as con:
        con.execute("UPDATE ppl_candidates SET parent_candidate_id='original_parent',parent_sim_key='original_key',root_candidate_id='original_root',repair_depth=2 WHERE candidate_id='cand_existing'")
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_execute(*a, **k):
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                              "run_0002", ["rplan_test"], True)
    with s.connect() as con:
        row = con.execute("SELECT parent_candidate_id,parent_sim_key,root_candidate_id,repair_depth FROM ppl_candidates WHERE candidate_id='cand_existing'").fetchone()
        assert tuple(row) == ("original_parent", "original_key", "original_root", 2)
        edge = con.execute("SELECT parent_candidate_id,child_candidate_id FROM ppl_repairs WHERE run_id='run_0002' AND repair_signature='sig1'").fetchone()
        assert tuple(edge) == ("cand_parent", "cand_existing")


def test_execution_report_uses_current_lifecycle_not_preexecution_snapshot(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    _add_planned_candidate(s, "cand_existing", "child_key")
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_execute(*a, **k):
        con = sqlite3.connect(adb)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','COMPLETE','u','A2',2.0,1.0,0.5,10,10)")
        con.commit(); con.close(); return []

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_execute)
    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
                                    "run_0002", ["rplan_test"], True)
    result = out["results"][0]
    with s.connect() as con:
        durable = con.execute("SELECT lifecycle_state,simulation_status FROM ppl_candidates WHERE candidate_id='cand_existing'").fetchone()
    assert result["lifecycle"] == durable["lifecycle_state"]
    assert result["candidate_simulation_status"] == durable["simulation_status"]
    assert result["lifecycle"] != "PLANNED"


def test_v304l_production_repair_server_slot_deferred_child_is_released_without_budget(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_deferred(candidates, config, machine, *, session, cache_db, allow_simulation_post,
                      remaining_initial_budget, stop_event=None):
        return [{
            "sim_key": "child_key",
            "status": "NEW",
            "error": "Deferred: an existing server-side simulation is still RUNNING and occupies a concurrency slot.",
        }]

    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_deferred)
    out = execute_production_repair(
        s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", ["rplan_test"], True,
    )
    assert out["post_consumed"] == 0
    assert out["deferred_sim_keys"] == ["child_key"]
    assert len(out["deferred_candidate_ids"]) == 1
    with s.connect() as con:
        plan = con.execute(
            "SELECT plan_status,consumed_posts,committed_posts,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'"
        ).fetchone()
        assert tuple(plan) == ("READY", 0, 0, "SERVER_SLOT_DEFERRED")
        child = con.execute(
            "SELECT lifecycle_state,simulation_status,alpha_id,execution_action FROM ppl_candidates WHERE run_id='run_0002' AND sim_key='child_key'"
        ).fetchone()
        assert tuple(child) == ("PLANNED", "NONE", None, "NEW_SIMULATION_REQUIRED")


def test_preview_rejects_same_sim_key_noop_without_counting_attempt(tmp_path, monkeypatch):
    """v3.0.4m: an identity repair must fail closed before cache/post handling."""
    s, c = make_store(tmp_path, parent_sim_key="parent_key")
    _mock_materialize(monkeypatch, child_sim_key="parent_key")
    with pytest.raises(ConfigError, match="REPAIR_NO_EFFECTIVE_CHANGE_SAME_SIM_KEY"):
        preview_production_repair(s, c, tmp_path / "a.db", "run_0002", ["rplan_test"], object())
    with s.connect() as con:
        row = con.execute(
            "SELECT plan_status,consumed_posts FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'"
        ).fetchone()
    assert row["plan_status"] == "DEFERRED_INITIAL_SEARCH"
    assert int(row["consumed_posts"] or 0) == 0


def test_context_uses_round_repair_consumption_when_higher_than_plan_sum(tmp_path):
    """Remote retry budget is authoritative even when one logical plan consumed two POST units."""
    s, c = make_store(tmp_path)
    _add_round(s)
    with s.connect() as con:
        con.execute("UPDATE ppl_repair_plans SET consumed_posts=2 WHERE repair_plan_id='rplan_test'")
        con.execute("UPDATE ppl_rounds SET repair_consumed=7 WHERE round_id='round_run_0002'")
    ctx = validate_production_repair_context(s, c, "run_0002")
    assert ctx["repair_consumed_plan"] == 2
    assert ctx["repair_consumed_round"] == 7
    assert ctx["repair_consumed"] == 7
    assert ctx["repair_consumed_source"] == "MAX_PLAN_AND_ROUND"


def test_v31_round_repair_nonblocking_handoff_does_not_enter_v21_poll_loop(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
    )
    calls = {"handoff": 0, "blocking": 0}

    def fake_handoff(store, config, machine, session, alpha_db, run_id, wrappers, by_key, **kwargs):
        calls["handoff"] += 1
        con = sqlite3.connect(alpha_db)
        con.execute(
            "INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) VALUES (?,?,?,?)",
            ("child_key", "SUBMITTED", "https://api.worldquantbrain.com/simulations/v31", None),
        )
        con.commit(); con.close()
        return {"http_audit": [], "results": [{"sim_key": "child_key", "status": "SUBMITTED"}]}

    def blocking(*args, **kwargs):
        calls["blocking"] += 1
        raise AssertionError("V3.1 nonblocking repair must not enter execute_with_v21 polling")

    monkeypatch.setattr("ppl_engine.production_repair.execute_continuous_remote_handoff", fake_handoff)
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", blocking)
    out = execute_round_repair(
        s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", "round_run_0002", 1, ["rplan_test"], True,
        preflight=preflight, nonblocking_remote=True,
    )
    assert calls == {"handoff": 1, "blocking": 0}
    assert out["post_confirmed"] == 1
    assert out["post_consumed"] == 1
    assert out["durable_progress"]["durable_running"] == 1


def test_v31_nonblocking_repair_dispatch_is_nonterminal_and_ignores_legacy_global_reserve(tmp_path, monkeypatch):
    c = cfg_v31()
    s, c = make_store(tmp_path, conf=c)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    with s.connect() as con:
        con.execute("UPDATE ppl_rounds SET repair_consumed=400 WHERE round_id='round_run_0002'")

    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
        enforce_global_repair_budget=False,
    )

    def fake_handoff(store, config, machine, session, alpha_db, run_id, wrappers, by_key, **kwargs):
        con = sqlite3.connect(alpha_db)
        con.execute(
            "INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) VALUES (?,?,?,?)",
            ("child_key", "SUBMITTED", "https://api.worldquantbrain.com/simulations/v31", None),
        )
        con.commit(); con.close()
        return {"http_audit": [], "results": [{"sim_key": "child_key", "status": "SUBMITTED"}]}

    monkeypatch.setattr("ppl_engine.production_repair.execute_continuous_remote_handoff", fake_handoff)
    out = execute_round_repair(
        s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", "round_run_0002", 1, ["rplan_test"], True,
        preflight=preflight, nonblocking_remote=True,
    )
    assert out["global_repair_budget_enforced"] is False
    assert out["post_consumed"] == 1
    with s.connect() as con:
        plan = con.execute(
            "SELECT plan_status,consumed_posts,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'"
        ).fetchone()
    assert tuple(plan) == ("DISPATCHED", 1, "REMOTE_EXECUTION_IN_PROGRESS")


def test_v31_poll_completion_advances_dispatched_repair_plan_to_executed(tmp_path, monkeypatch):
    c = cfg_v31()
    s, c = make_store(tmp_path, conf=c)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
        enforce_global_repair_budget=False,
    )

    def fake_handoff(store, config, machine, session, alpha_db, run_id, wrappers, by_key, **kwargs):
        con = sqlite3.connect(alpha_db)
        con.execute(
            "INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id) VALUES (?,?,?,?)",
            ("child_key", "SUBMITTED", "https://api.worldquantbrain.com/simulations/v31", None),
        )
        con.commit(); con.close()
        return {"http_audit": [], "results": [{"sim_key": "child_key", "status": "SUBMITTED"}]}

    monkeypatch.setattr("ppl_engine.production_repair.execute_continuous_remote_handoff", fake_handoff)
    execute_round_repair(
        s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py",
        "run_0002", "round_run_0002", 1, ["rplan_test"], True,
        preflight=preflight, nonblocking_remote=True,
    )
    with s.connect() as con:
        child = con.execute(
            "SELECT candidate_id FROM ppl_candidates WHERE run_id='run_0002' AND sim_key='child_key'"
        ).fetchone()[0]
    con = sqlite3.connect(adb)
    con.execute(
        "UPDATE alpha_results SET status='COMPLETE',alpha_id='A_CHILD',sharpe=1.7,fitness=0.8,turnover=0.4 WHERE sim_key='child_key'"
    )
    con.commit(); con.close()

    rec = reconcile_completed_repair_outcomes(s, c, adb, "run_0002", [child])
    assert rec["plans_completed"] == 1
    with s.connect() as con:
        plan = con.execute(
            "SELECT plan_status,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rplan_test'"
        ).fetchone()
    assert tuple(plan) == ("EXECUTED", None)


def test_v31_round_repair_global_400_reserve_is_statistics_only(tmp_path, monkeypatch):
    c = cfg_v31()
    s, c = make_store(tmp_path, conf=c)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    # Simulate a long-running Continuous run that has already crossed the old
    # V3 400-post Repair reserve.  The count remains durable/statistical, but
    # must not block a new bounded Repair scheduling cycle.
    with s.connect() as con:
        con.execute("UPDATE ppl_rounds SET repair_consumed=400 WHERE round_id='round_run_0002'")
    preflight = preflight_round_repair_execution(
        s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
        run_id="run_0002", round_id="round_run_0002", batch_no=1,
        plan_ids=["rplan_test"], allow_simulation_post=True,
        enforce_global_repair_budget=False,
    )
    assert preflight["preview"]["global_repair_budget_enforced"] is False
    assert preflight["preview"]["repair_reserve_remaining"] is None
    assert preflight["preview"]["projected_new_posts"] == 1


def test_legacy_round_repair_still_enforces_global_repair_reserve(tmp_path, monkeypatch):
    s, c = make_store(tmp_path)
    _add_round(s)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; make_alpha_db(adb)
    with s.connect() as con:
        con.execute("UPDATE ppl_rounds SET repair_consumed=48 WHERE round_id='round_run_0002'")
    with pytest.raises(ConfigError, match="PRODUCTION_REPAIR_BUDGET_EXCEEDED"):
        preflight_round_repair_execution(
            s, c, _FakeMachine(), adb, ROOT / "machine_lib_V2_1.py",
            run_id="run_0002", round_id="round_run_0002", batch_no=1,
            plan_ids=["rplan_test"], allow_simulation_post=True,
            enforce_global_repair_budget=True,
        )
