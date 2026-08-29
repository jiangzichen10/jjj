"""Tests for V2.2 Production Logging Phase 2 (execution main-chain audit wiring).

All offline: temporary databases + mocked V2.1 adapter/transport. No real BRAIN
requests are ever issued. Uses fake IDs only (never replays real history).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
import machine_lib_V2_1 as real_machine

import ppl_engine.audit_log as al
from ppl_engine.config import load_effective_config
from ppl_engine.production_repair import execute_production_repair
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


@pytest.fixture(autouse=True)
def _reset_audit_log(monkeypatch):
    from ppl_engine.live_execution import EXPECTED_MACHINE_HASH
    monkeypatch.setattr("ppl_engine.live_execution._hash_file", lambda _path: EXPECTED_MACHINE_HASH)
    al._reset_handlers(al.get_audit_logger())
    al._CONFIGURED = False
    al._CONFIGURED_PATH = None
    al._WARNED = False
    yield
    al._reset_handlers(al.get_audit_logger())
    al._CONFIGURED = False
    al._CONFIGURED_PATH = None
    al._WARNED = False


def _cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def _full_settings(*, expr="ts_mean(f1, 2)", decay=0):
    conf = _cfg()
    defaults = conf.plan["simulation_settings"]
    return real_machine.build_settings(
        {"expr": expr, "decay": decay},
        neutralization=defaults["neutralization"], region=defaults["region"],
        universe=defaults["universe"], delay=int(defaults["delay"]),
        truncation=float(defaults["truncation"]),
        test_period=defaults.get("test_period", defaults.get("testPeriod", "P0Y")),
    )


def _configure_audit(tmp_path):
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))
    return tmp_path / "logs" / "ppl_v2_2.log"


def _events(path):
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def _actions(path):
    return [e["action"] for e in _events(path)]


# ---------------------------------------------------------------------------
# store fixtures (mirror the production_repair test setup)
# ---------------------------------------------------------------------------

def _make_alpha_db(path, rows=()):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, alpha_id TEXT, sharpe REAL, fitness REAL, turnover REAL, long_count INTEGER, short_count INTEGER)")
    c.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    for r in rows:
        c.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES (?,?,?,?,?,?,?,?,?)", r)
    c.commit(); c.close()


def _make_store(tmp_path, *, target_failure="SHARPE_NEAR_PASS", repair_type="SHARPE_RANK",
                operator_name="rank", operator_status="VERIFIED_PROJECT", parent_sim_key="parent_key"):
    s = RunnerStore(tmp_path / "r.db")
    s.initialize()
    conf = _cfg()
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        c.execute(
            """INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,
               context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,
               transform_family,operator,window,decay,vector_reducer,root_candidate_id,parent_candidate_id,
               parent_sim_key,repair_depth,lifecycle_state,simulation_status,selected_for_initial_search,
               execution_action,cache_classification,discovery_snapshot_id,dry_run_snapshot_id,
               structure_status,data_field_count_estimate,pp_total_operator_count_estimate,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand_parent", "run_0002", "rank(parent_key)", parent_sim_key, "{}", "sh", "ctx_parent",
             "pv30", "f1", "MATRIX", "RETURN", "NORMAL", "pv30/f1/IDENTITY/NORMAL/RANK", "RANK", "rank",
             None, 0, "IDENTITY", "cand_parent", None, None, 0, "SIMULATION_COMPLETE", "COMPLETE", 1,
             "CACHE_RESTORE", "CACHE_COMPLETE", "disc", "dry", "ELIGIBLE", 1, 1, NOW, NOW),
        )
        c.execute(
            "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) "
            "VALUES ('prov_parent','cand_parent','run_0002','parent_key','ctx_parent','disc','dry','{}',?,?)", (NOW, NOW),
        )
        c.execute("INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES (?,?,?,?)",
                  (operator_name, "h", "CORE_VERIFIED_OPERATOR", operator_status))
        spec = {
            "repair_type": repair_type, "repair_signature": "sig1", "repair_depth": 1,
            "expression_preview": "ts_mean(f1, 2)",
            "repair_path": ["RAW", f"{target_failure}:{repair_type}"],
            "direction_override": None, "transform_family_override": "ts_mean", "window_override": 2,
            "operator_requirements": [operator_name],
        }
        c.execute(
            """INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
               target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
               operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
               blocked_reason,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("rplan_test", "diag1", "run_0002", "cand_parent", "cand_parent", target_failure,
             repair_type, "sig1", json.dumps(["RAW"]), 1, json.dumps(spec),
             json.dumps([operator_name]), "DEFERRED_INITIAL_SEARCH", 1, 0, 0, None, NOW, NOW),
        )
    return s, conf


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


def _fake_execute_insert_complete(sim_key="child_key"):
    def fake(candidates, config, machine, *, session, cache_db, allow_simulation_post, remaining_initial_budget, stop_event=None):
        con = sqlite3.connect(cache_db)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES (?,?,?,?,?,?,?,?,?)",
                    (sim_key, "COMPLETE", "https://api.worldquantbrain.com/simulations/abc", "FAKEALPHA", 2.5, 1.2, 0.5, 10, 10))
        con.commit(); con.close()
        return []
    return fake


def _fake_resolved_check(final_status="RESOLVED", base="PASS", theme="PASS"):
    def fake(transport, *, alpha_id, phase, rules, budget, candidate_id=None, run_id=None,
             evidence_source="SYNTHETIC_TEST", wait=None, clock=None, store=None, **_kwargs):
        return {
            "check_session_id": "chk1", "run_id": run_id, "candidate_id": candidate_id, "alpha_id": alpha_id,
            "phase": phase, "session_status": final_status, "poll_count": 1, "http_request_count": 1,
            "pending_poll_requests": 0, "base_gate_result": base, "theme_gate_result": theme,
            "error_type": None if final_status == "RESOLVED" else "HTTP_5XX",
            "error_nature": None if final_status == "RESOLVED" else "TRANSIENT",
            "final": {"base_gate": {"status": base}, "theme_gate": {"status": theme}, "results": []},
        }
    return fake


# ---------------------------------------------------------------------------
# decision events
# ---------------------------------------------------------------------------

def test_decision_new_simulation_required(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    evs = _events(log)
    dec = [e for e in evs if e["action"] == "NEW_SIMULATION_REQUIRED"]
    assert dec and dec[0]["will_post"] is True and dec[0]["cache_classification"] == "CACHE_MISS"


def test_decision_reuse_existing_candidate(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    # pre-create a PLANNED/NONE candidate with the same child sim_key -> REUSE
    with s.connect() as con:
        con.execute(
            "INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,simulation_status,selected_for_initial_search,created_at,updated_at) "
            "VALUES ('cand_existing','run_0002','ts_mean(f1,2)','child_key','PLANNED','NONE',0,?,?)", (NOW, NOW),
        )
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    evs = _events(log)
    dec = [e for e in evs if e["action"] == "REUSE_EXISTING_CANDIDATE"]
    assert dec and dec[0]["will_post"] is True and dec[0]["reuse_existing"] is True


def test_decision_cache_restore(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    _make_alpha_db(adb, [("child_key", "COMPLETE", "u", "A1", 2.0, 1.0, 0.5, 10, 10)])
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    assert "CACHE_RESTORE" in _actions(log)


# ---------------------------------------------------------------------------
# budget events
# ---------------------------------------------------------------------------

def test_budget_check_allowed(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    checks = [e for e in _events(log) if e["action"] == "BUDGET_CHECK"]
    assert checks and checks[0]["allowed"] is True


def test_budget_blocked_no_post_attempt(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    # Exhaust the reserve via a DIFFERENT already-consumed plan (rplan_test keeps consumed=0).
    with s.connect() as con:
        con.execute(
            "INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,blocked_reason,created_at,updated_at) "
            "VALUES ('rplan_other','diag2','run_0002','cand_parent','cand_parent','X','Y','sig_other','[]',1,'{}','[]','EXECUTED',0,48,48,NULL,?,?)", (NOW, NOW),
        )
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    log = _configure_audit(tmp_path)
    with pytest.raises(Exception, match="BUDGET_EXCEEDED"):
        execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    acts = _actions(log)
    assert "BUDGET_BLOCKED" in acts
    assert "SIMULATION_POST_ATTEMPT" not in acts


def test_budget_consumed_after_complete(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    evs = [e for e in _events(log) if e["action"] == "BUDGET_CONSUMED"]
    assert evs and evs[0]["delta"] == 1 and evs[0]["budget_after"] == evs[0]["budget_before"] + 1


# ---------------------------------------------------------------------------
# POST outcome events
# ---------------------------------------------------------------------------

def test_post_success(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    evs = [e for e in _events(log) if e["action"] == "SIMULATION_POST_SUCCESS"]
    assert evs and evs[0]["http_status"] == 201 and "simulation_url" in evs[0]


def test_post_failed_when_no_fact(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", lambda *a, **k: [])  # posts nothing
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    assert "SIMULATION_POST_FAILED" in _actions(log)


def test_uncertain_not_failed(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())

    def fake_uncertain(candidates, config, machine, *, session, cache_db, allow_simulation_post, remaining_initial_budget, stop_event=None):
        con = sqlite3.connect(cache_db)
        con.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,long_count,short_count) VALUES ('child_key','UNCERTAIN_SUBMISSION',NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
        con.commit(); con.close()
        return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_uncertain)
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    acts = _actions(log)
    assert "SIMULATION_UNCERTAIN" in acts
    assert "SIMULATION_POST_FAILED" not in acts


def test_resume_no_post(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch, child_sim_key="child_key")
    adb = tmp_path / "a.db"
    _make_alpha_db(adb, [("child_key", "RUNNING", "https://api.worldquantbrain.com/simulations/xyz", "A1", 2.0, 1.0, 0.5, 10, 10)])
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    delegated = []
    def fake_resume(candidates, *a, **k):
        delegated.extend(x.get("execution_action") for x in candidates)
        return []
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", fake_resume)
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    assert delegated == ["RESUME_EXISTING"]  # delegated to V2.1 resume path, not a new POST
    assert "SIMULATION_POST_ATTEMPT" not in _actions(log)
    evs = [e for e in _events(log) if e["action"] == "SIMULATION_RESUME"]
    assert evs and evs[0]["simulation_url"].endswith("/simulations/xyz")
    assert out["post_attempted"] == 0


# ---------------------------------------------------------------------------
# state transition + simulation complete (via _sync_candidate_fact)
# ---------------------------------------------------------------------------

def _sync_candidate_fact_test(tmp_path, status, *, sim_url="u", alpha="A1"):
    from ppl_engine.live_execution import _sync_candidate_fact
    s = RunnerStore(tmp_path / "r.db"); s.initialize()
    with s.connect() as c:
        c.execute(
            "INSERT INTO ppl_runs(run_id,runner_goal,target_mode,atom_constraint_active,run_profile,current_stage,status,execution_hash,operational_hash,presentation_hash,rules_json,plan_json,budget_json,created_at,updated_at) "
            "VALUES ('run_0002','PPL','ATOM',0,'PRODUCTION_RESEARCH','INIT','CREATED','h1','h2','h3','{}','{}','{}',?,?)", (NOW, NOW),
        )
        c.execute("INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,simulation_status,created_at,updated_at) VALUES ('cand_x','run_0002','expr','sk','SIMULATION_PENDING','SUBMITTED',?,?)", (NOW, NOW))
    log = _configure_audit(tmp_path)
    _sync_candidate_fact(s, "cand_x", {"sim_key": "sk", "status": status, "simulation_url": sim_url, "alpha_id": alpha,
                                        "sharpe": 2.0, "fitness": 1.0, "turnover": 0.5, "returns": 0.1, "margin": 0.001,
                                        "long_count": 10, "short_count": 5}, source="TEST")
    return log


def test_state_transition_submitted_to_running(tmp_path):
    log = _sync_candidate_fact_test(tmp_path, "RUNNING")
    st = [e for e in _events(log) if e["action"] == "STATE_TRANSITION"]
    assert st and st[0]["old_state"] == "SIMULATION_PENDING" and st[0]["new_state"] == "SIMULATION_RUNNING"


def test_simulation_complete_event(tmp_path):
    log = _sync_candidate_fact_test(tmp_path, "COMPLETE")
    evs = [e for e in _events(log) if e["action"] == "SIMULATION_COMPLETE"]
    assert evs and evs[0]["alpha_id"] == "A1" and evs[0]["sharpe"] == 2.0


# ---------------------------------------------------------------------------
# HTTP 429 / retry / timeout
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


def _http_machine(responses):
    """Build a fake machine whose Session.request returns `responses` in order."""
    from ppl_engine.live_execution import _instrument_v21
    calls = {"n": 0}

    def fake_request(session, method, url, *args, **kwargs):
        idx = calls["n"]; calls["n"] += 1
        if idx >= len(responses):
            raise AssertionError("too many requests")
        r = responses[idx]
        if isinstance(r, BaseException):
            raise r
        return r

    class _Session:
        request = staticmethod(fake_request)
    class _Sessions:
        Session = _Session
    class _Requests:
        sessions = _Sessions()
    class _M:
        requests = _Requests()
        BRAIN_API_URL = "https://api.worldquantbrain.com"
        def cache_put(self, *a, **k): return None
        def cache_get(self, *a, **k): return None
    return _M()


def _run_instrumented(machine, calls, tmp_path):
    from ppl_engine.live_execution import _instrument_v21
    log = _configure_audit(tmp_path)
    with _instrument_v21(machine, None, {}, run_id="run_0002") as methods:
        req = machine.requests.sessions.Session.request
        for method, url in calls:
            try:
                req(None, method, url)
            except Exception:
                pass
    return log


_CHECK_URL = "https://api.worldquantbrain.com/alphas/FAKEALPHA/check"


def test_http_429(tmp_path):
    m = _http_machine([_FakeResponse(429, {"Retry-After": "5"})])
    log = _run_instrumented(m, [("GET", _CHECK_URL)], tmp_path)
    evs = [e for e in _events(log) if e["action"] == "HTTP_429"]
    assert evs and evs[0]["http_status"] == 429 and evs[0]["retry_after_seconds"] == "5"


def test_http_retry(tmp_path):
    m = _http_machine([_FakeResponse(429), _FakeResponse(200)])
    log = _run_instrumented(m, [("GET", _CHECK_URL), ("GET", _CHECK_URL)], tmp_path)
    evs = [e for e in _events(log) if e["action"] == "HTTP_RETRY"]
    assert evs and evs[0]["retry_count"] >= 1


def test_http_timeout(tmp_path):
    m = _http_machine([TimeoutError("read timed out")])
    log = _run_instrumented(m, [("GET", _CHECK_URL)], tmp_path)
    evs = [e for e in _events(log) if e["action"] == "HTTP_TIMEOUT"]
    assert evs and evs[0]["error_type"] == "TimeoutError"


# ---------------------------------------------------------------------------
# PRE-TAG check events (real run_one_pretag_check with mocked semantic_poll_check)
# ---------------------------------------------------------------------------

def _pretag_store(tmp_path):
    s = RunnerStore(tmp_path / "r.db"); s.initialize()
    conf = _cfg()
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        c.execute("INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,simulation_status,alpha_id,created_at,updated_at) VALUES ('cand_x','run_0002','expr','sk','LOCAL_PRE_GATE_PASS','COMPLETE','FAKEALPHA',?,?)", (NOW, NOW))
    return s, conf


def test_pretag_check_start_and_complete(tmp_path, monkeypatch):
    from ppl_engine.live_execution import run_one_pretag_check
    s, conf = _pretag_store(tmp_path)
    monkeypatch.setattr("ppl_engine.live_execution.semantic_poll_check", _fake_resolved_check())
    log = _configure_audit(tmp_path)
    run_one_pretag_check(s, conf, object(), object(), "run_0002", ["cand_x"], source="PRODUCTION_REPAIR", evidence_source="LIVE_CHECK")
    acts = _actions(log)
    assert "PRETAG_CHECK_START" in acts
    evs = [e for e in _events(log) if e["action"] == "PRETAG_CHECK_COMPLETE"]
    assert evs and evs[0]["session_status"] == "RESOLVED" and evs[0]["base_gate"] == "PASS"


def test_pretag_warning_not_failed(tmp_path, monkeypatch):
    from ppl_engine.live_execution import run_one_pretag_check
    s, conf = _pretag_store(tmp_path)
    # Resolved with a WARNING theme gate -> PRETAG_CHECK_COMPLETE, NOT FAILED
    monkeypatch.setattr("ppl_engine.live_execution.semantic_poll_check", _fake_resolved_check("RESOLVED", "PASS", "WARNING"))
    log = _configure_audit(tmp_path)
    run_one_pretag_check(s, conf, object(), object(), "run_0002", ["cand_x"], source="PRODUCTION_REPAIR", evidence_source="LIVE_CHECK")
    acts = _actions(log)
    assert "PRETAG_CHECK_COMPLETE" in acts
    assert "PRETAG_CHECK_FAILED" not in acts


def test_pretag_unresolved_is_failed(tmp_path, monkeypatch):
    from ppl_engine.live_execution import run_one_pretag_check
    s, conf = _pretag_store(tmp_path)
    monkeypatch.setattr("ppl_engine.live_execution.semantic_poll_check", _fake_resolved_check("PENDING", "PENDING", "PENDING"))
    log = _configure_audit(tmp_path)
    run_one_pretag_check(s, conf, object(), object(), "run_0002", ["cand_x"], source="PRODUCTION_REPAIR", evidence_source="LIVE_CHECK")
    evs = [e for e in _events(log) if e["action"] == "PRETAG_CHECK_FAILED"]
    assert evs and evs[0]["error_type"] == "HTTP_5XX"


# ---------------------------------------------------------------------------
# Local gate + repair plan created (via run_local_analysis)
# ---------------------------------------------------------------------------

def _local_analysis_store(tmp_path, turnover=0.5, sharpe=2.5):
    s = RunnerStore(tmp_path / "r.db"); s.initialize()
    conf = _cfg()
    s.create_run("run_0002", conf)
    with s.connect() as c:
        c.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        c.execute(
            "INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,operator,window,decay,vector_reducer,root_candidate_id,parent_candidate_id,parent_sim_key,repair_depth,lifecycle_state,simulation_status,selected_for_initial_search,execution_action,cache_classification,discovery_snapshot_id,dry_run_snapshot_id,structure_status,data_field_count_estimate,pp_total_operator_count_estimate,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("cand_x", "run_0002", "rank(sk)", "sk", "{}", "sh", "ctx", "pv30", "f1", "MATRIX", "RETURN", "NORMAL",
             "pv30/f1/IDENTITY/NORMAL/RANK", "RANK", "rank", None, 0, "IDENTITY", "cand_x", None, None, 0,
             "SIMULATION_COMPLETE", "COMPLETE", 1, "CACHE_RESTORE", "CACHE_COMPLETE", "disc", "dry", "ELIGIBLE", 1, 1, NOW, NOW),
        )
        c.execute("INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES ('ts_mean','h','CORE_VERIFIED_OPERATOR','VERIFIED_PROJECT')")
    return s, conf


def _local_alpha_db(path, turnover=0.5, sharpe=2.5):
    _make_alpha_db(path, [("sk", "COMPLETE", "u", "FAKEALPHA", sharpe, 1.2, turnover, 10, 10)])


def test_local_gate_pass(tmp_path):
    from ppl_engine.live_execution import run_local_analysis
    s, conf = _local_analysis_store(tmp_path)
    _local_alpha_db(tmp_path / "a.db", turnover=0.5, sharpe=2.5)
    log = _configure_audit(tmp_path)
    run_local_analysis(s, conf, tmp_path / "a.db", "run_0002", audit_source="PRODUCTION_REPAIR", candidate_ids=["cand_x"], repair_reserve_remaining=48)
    evs = [e for e in _events(log) if e["action"] == "LOCAL_GATE_PASS"]
    assert evs and evs[0]["gate_status"] == "PASS"


def test_local_gate_fail_and_repair_plan_created(tmp_path):
    from ppl_engine.live_execution import run_local_analysis
    s, conf = _local_analysis_store(tmp_path)
    _local_alpha_db(tmp_path / "a.db", turnover=0.95, sharpe=2.5)  # turnover > base max -> FAIL
    log = _configure_audit(tmp_path)
    run_local_analysis(s, conf, tmp_path / "a.db", "run_0002", audit_source="PRODUCTION_REPAIR", candidate_ids=["cand_x"], repair_reserve_remaining=48)
    acts = _actions(log)
    assert "LOCAL_GATE_FAIL" in acts
    evs = [e for e in _events(log) if e["action"] == "REPAIR_PLAN_CREATED"]
    assert evs and "parameter_change" in evs[0]


# ---------------------------------------------------------------------------
# Repair execution start / complete / outcome
# ---------------------------------------------------------------------------

def test_repair_execution_start_and_complete(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    acts = _actions(log)
    assert "REPAIR_EXECUTION_START" in acts
    evs = [e for e in _events(log) if e["action"] == "REPAIR_EXECUTION_COMPLETE"]
    assert evs and evs[0]["final_simulation_status"] == "COMPLETE"


def test_repair_outcome_for_ht_plan(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path, target_failure="HT_RETURNS_RATIO_FAIL", repair_type="HT_RATIO_TARGET_TVR",
                       operator_name="ts_target_tvr_decay", operator_status="VERIFIED_PROJECT")
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    from ppl_engine.config import ConfigError
    with pytest.raises(ConfigError, match="REPAIR_STRATEGY_RETIRED"):
        execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)


# ---------------------------------------------------------------------------
# audit failure must not change business result
# ---------------------------------------------------------------------------

def test_audit_failure_does_not_change_business(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.production_repair.run_one_pretag_check", lambda *a, **k: {"executed": True})
    _configure_audit(tmp_path)

    # Force the log WRITE to fail (inside audit_event's best-effort try/except).
    logger = al.get_audit_logger()
    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(logger, "log", boom)

    out = execute_production_repair(s, c, _FakeMachine(), None, adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)
    assert out["post_consumed"] == 1  # business result unchanged despite logging failure


# ---------------------------------------------------------------------------
# full happy-path event ordering + trace reconstruction
# ---------------------------------------------------------------------------

def test_full_repair_happy_path_event_order(tmp_path, monkeypatch):
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    # Let the real run_one_pretag_check run, but mock the poll to resolve PASS.
    monkeypatch.setattr("ppl_engine.live_execution.semantic_poll_check", _fake_resolved_check())
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)

    acts = _actions(log)
    order = ["REPAIR_EXECUTION_START", "BUDGET_CHECK", "SIMULATION_POST_ATTEMPT", "SIMULATION_POST_SUCCESS",
             "SIMULATION_COMPLETE", "LOCAL_GATE_PASS", "PRETAG_CHECK_START", "PRETAG_CHECK_COMPLETE",
             "REPAIR_EXECUTION_COMPLETE"]
    positions = {}
    for i, a in enumerate(acts):
        if a in order:
            positions.setdefault(a, i)
    for a in order:
        assert a in positions, f"missing action {a}"
    seq = [positions[a] for a in order]
    assert seq == sorted(seq), f"event order wrong: {[(order[i], seq[i]) for i in range(len(order))]}"


def test_trace_reconstruction_by_plan_and_alpha(tmp_path, monkeypatch):
    from ppl_engine.audit_log import read_audit_log
    s, c = _make_store(tmp_path)
    _mock_materialize(monkeypatch)
    adb = tmp_path / "a.db"; _make_alpha_db(adb)
    monkeypatch.setattr("ppl_engine.production_repair._instrument_v21", _fake_instrument())
    monkeypatch.setattr("ppl_engine.production_repair.execute_with_v21", _fake_execute_insert_complete())
    monkeypatch.setattr("ppl_engine.live_execution.semantic_poll_check", _fake_resolved_check())
    log = _configure_audit(tmp_path)
    execute_production_repair(s, c, _FakeMachine(), object(), adb, ROOT / "machine_lib_V2_1.py", "run_0002", ["rplan_test"], True)

    by_plan = [e["action"] for e in read_audit_log(log, run_id="run_0002", repair_plan_id="rplan_test", limit=1000)]
    assert "REPAIR_EXECUTION_START" in by_plan and "REPAIR_EXECUTION_COMPLETE" in by_plan

    by_alpha = [e["action"] for e in read_audit_log(log, run_id="run_0002", alpha_id="FAKEALPHA", limit=1000)]
    assert "SIMULATION_COMPLETE" in by_alpha
    assert "PRETAG_CHECK_COMPLETE" in by_alpha



def test_v304l_instrument_allows_brain_authentication_refresh_but_not_arbitrary_post():
    from ppl_engine.live_execution import _instrument_v21
    machine = _http_machine([_FakeResponse(201)])
    with _instrument_v21(machine, None, {}, run_id="run_0002") as methods:
        req = machine.requests.sessions.Session.request
        response = req(None, "POST", "https://api.worldquantbrain.com/authentication")
        assert response.status_code == 201
        with pytest.raises(RuntimeError, match="PHASE10A_UNEXPECTED_POST"):
            req(None, "POST", "https://api.worldquantbrain.com/alphas/FAKE")
        with pytest.raises(RuntimeError, match="PHASE10A_UNEXPECTED_POST"):
            req(None, "POST", "https://evil.example/authentication")
    assert [x["url"] for x in methods] == ["https://api.worldquantbrain.com/authentication"]


def test_v304o_round_repair_allows_only_valid_simulation_delete():
    from ppl_engine.live_execution import _instrument_v21
    machine = _http_machine([_FakeResponse(200)])
    valid = "https://api.worldquantbrain.com/simulations/sim123"
    with _instrument_v21(
        machine, None, {}, run_id="run_0002", allow_simulation_delete=True,
    ) as methods:
        req = machine.requests.sessions.Session.request
        assert req(None, "DELETE", valid).status_code == 200
        with pytest.raises(Exception):
            req(None, "DELETE", "https://api.worldquantbrain.com/alphas/not-a-simulation")
        with pytest.raises(RuntimeError, match="PHASE10A_FORBIDDEN_HTTP_METHOD:PATCH"):
            req(None, "PATCH", valid)
        with pytest.raises(RuntimeError, match="PHASE10A_FORBIDDEN_HTTP_METHOD:PUT"):
            req(None, "PUT", valid)
    assert [(x["method"], x["url"]) for x in methods] == [("DELETE", valid)]


def test_v304o_delete_remains_forbidden_without_explicit_round_repair_scope():
    from ppl_engine.live_execution import _instrument_v21
    machine = _http_machine([])
    with _instrument_v21(machine, None, {}, run_id="run_0002"):
        req = machine.requests.sessions.Session.request
        with pytest.raises(RuntimeError, match="PHASE10A_FORBIDDEN_HTTP_METHOD:DELETE"):
            req(None, "DELETE", "https://api.worldquantbrain.com/simulations/sim123")


def test_local_analysis_reconciles_planned_complete_before_fail_transition(tmp_path):
    """Historical COMPLETE cache facts must not jump PLANNED -> PRE_CHECK_REPAIR."""
    from ppl_engine.live_execution import run_local_analysis
    s, conf = _local_analysis_store(tmp_path)
    with s.connect() as c:
        c.execute(
            "UPDATE ppl_candidates SET lifecycle_state='PLANNED',simulation_status='NONE',execution_action='NEW_SIMULATION_REQUIRED',cache_classification='CACHE_MISS' WHERE candidate_id='cand_x'"
        )
    _local_alpha_db(tmp_path / "a.db", turnover=0.95, sharpe=2.5)
    _configure_audit(tmp_path)
    run_local_analysis(
        s, conf, tmp_path / "a.db", "run_0002", audit_source="PRODUCTION_REPAIR",
        candidate_ids=["cand_x"], repair_reserve_remaining=48,
    )
    row = next(x for x in s.load_candidates("run_0002") if x["candidate_id"] == "cand_x")
    assert row["lifecycle_state"] == "PRE_CHECK_REPAIR"
    with s.connect() as c:
        path = [tuple(r) for r in c.execute(
            "SELECT from_state,to_state FROM ppl_state_transitions WHERE candidate_id='cand_x' ORDER BY transition_id"
        ).fetchall()]
    assert ("PLANNED", "SIMULATION_COMPLETE") in path
    assert ("SIMULATION_COMPLETE", "SIGNAL_ANALYZED") in path
    assert ("SIGNAL_ANALYZED", "PRE_CHECK_REPAIR") in path
    assert ("PLANNED", "PRE_CHECK_REPAIR") not in path


def test_local_analysis_reconciles_planned_complete_before_pass_transition(tmp_path):
    """Historical COMPLETE cache facts must also use the legal path on local PASS."""
    from ppl_engine.live_execution import run_local_analysis
    s, conf = _local_analysis_store(tmp_path)
    with s.connect() as c:
        c.execute(
            "UPDATE ppl_candidates SET lifecycle_state='PLANNED',simulation_status='NONE',execution_action='NEW_SIMULATION_REQUIRED',cache_classification='CACHE_MISS' WHERE candidate_id='cand_x'"
        )
    _local_alpha_db(tmp_path / "a.db", turnover=0.5, sharpe=2.5)
    _configure_audit(tmp_path)
    run_local_analysis(
        s, conf, tmp_path / "a.db", "run_0002", audit_source="PRODUCTION_REPAIR",
        candidate_ids=["cand_x"], repair_reserve_remaining=48,
    )
    row = next(x for x in s.load_candidates("run_0002") if x["candidate_id"] == "cand_x")
    assert row["lifecycle_state"] == "LOCAL_PRE_GATE_PASS"
    with s.connect() as c:
        path = [tuple(r) for r in c.execute(
            "SELECT from_state,to_state FROM ppl_state_transitions WHERE candidate_id='cand_x' ORDER BY transition_id"
        ).fetchall()]
    assert ("PLANNED", "SIMULATION_COMPLETE") in path
    assert ("SIMULATION_COMPLETE", "SIGNAL_ANALYZED") in path
    assert ("SIGNAL_ANALYZED", "LOCAL_PRE_GATE_PASS") in path
    assert ("PLANNED", "LOCAL_PRE_GATE_PASS") not in path


def test_local_analysis_is_idempotent_after_pretag_queue_advance(tmp_path):
    """Re-seeing COMPLETE must not regress PRE_TAG_CHECK_PENDING -> LOCAL_PRE_GATE_PASS."""
    from ppl_engine.live_execution import run_local_analysis
    from ppl_engine.continuous_check import enqueue_pretag_checks
    s, conf = _local_analysis_store(tmp_path)
    _local_alpha_db(tmp_path / "a.db", turnover=0.5, sharpe=2.5)
    _configure_audit(tmp_path)
    with s.connect() as c:
        c.execute("UPDATE ppl_candidates SET alpha_id='FAKEALPHA' WHERE candidate_id='cand_x'")

    first = run_local_analysis(
        s, conf, tmp_path / "a.db", "run_0002", audit_source="ROUND_ORCHESTRATOR",
        candidate_ids=["cand_x"], repair_reserve_remaining=48,
    )
    assert first["local_pass_candidates"] == ["cand_x"]
    queued = enqueue_pretag_checks(s, "run_0002", first["local_pass_candidates"])
    assert queued["queued_count"] == 1
    assert s.load_candidates("run_0002")[0]["lifecycle_state"] == "PRE_TAG_CHECK_PENDING"

    second = run_local_analysis(
        s, conf, tmp_path / "a.db", "run_0002", audit_source="ROUND_ORCHESTRATOR",
        candidate_ids=["cand_x"], repair_reserve_remaining=48,
    )
    assert second["local_pass_candidates"] == []
    assert s.load_candidates("run_0002")[0]["lifecycle_state"] == "PRE_TAG_CHECK_PENDING"
    with s.connect() as c:
        regressions = c.execute(
            "SELECT COUNT(*) FROM ppl_state_transitions WHERE candidate_id='cand_x' AND from_state='PRE_TAG_CHECK_PENDING' AND to_state='LOCAL_PRE_GATE_PASS'"
        ).fetchone()[0]
    assert regressions == 0


def test_local_analysis_reuses_unqueued_local_pass_without_retransition(tmp_path):
    """Crash after LOCAL_PRE_GATE_PASS but before enqueue remains recoverable."""
    from ppl_engine.live_execution import run_local_analysis
    s, conf = _local_analysis_store(tmp_path)
    _local_alpha_db(tmp_path / "a.db", turnover=0.5, sharpe=2.5)
    _configure_audit(tmp_path)
    with s.connect() as c:
        c.execute("UPDATE ppl_candidates SET lifecycle_state='LOCAL_PRE_GATE_PASS' WHERE candidate_id='cand_x'")
    report = run_local_analysis(
        s, conf, tmp_path / "a.db", "run_0002", audit_source="ROUND_ORCHESTRATOR",
        candidate_ids=["cand_x"], repair_reserve_remaining=48,
    )
    assert report["local_pass_candidates"] == ["cand_x"]
    assert s.load_candidates("run_0002")[0]["lifecycle_state"] == "LOCAL_PRE_GATE_PASS"
