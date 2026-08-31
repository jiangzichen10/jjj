# -*- coding: utf-8 -*-
"""D2 closure: fail-closed remote-only reconciliation entrypoint tests.

Covers the standalone ``--reconcile-existing-remote-only`` path.  Every test
asserts the hard guards: only pre-existing remote work with a durable
simulation_url may be touched, only GET is allowed, and nothing else in the
system (scheduler, selector, Repair plan creation, Search candidate creation,
budgets, UNCERTAIN) may move.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import machine_lib_V2_1 as machine

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.continuous_remote import reconcile_existing_remote_only, sync_remote_work_from_durable_facts
from ppl_engine.research_telemetry import sync_simulation_ledger
from ppl_engine.round_store import create_round
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_ID = "repair_7382826da7fa62f831b9fc20"


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _candidate_identity(cfg, expr="rank(close)"):
    candidate = {"expr": expr, "decay": 0}
    sim = cfg.plan["simulation_settings"]
    settings = machine.build_settings(
        candidate,
        neutralization=sim["neutralization"], region=sim["region"], universe=sim["universe"],
        delay=sim["delay"], truncation=sim["truncation"], test_period=sim["test_period"],
    )
    return machine.simulation_key(expr, settings), settings


def _setup(tmp_path, *, status="SUBMITTED", url="https://api.worldquantbrain.com/simulations/2iRVcV9Br4VrauIXykoc5Z1",
           candidate_id=CANDIDATE_ID, lifecycle="SIMULATION_RUNNING"):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006",
        policy={"objective": "test", "batch_size": 4},
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,
                   operator,decay,neutralization,parent_candidate_id,lifecycle_state,simulation_status,
                   new_post_budget_consumed,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                candidate_id, "run_0006", "rank(close)", sim_key, json.dumps(settings),
                "close", "MATRIX", "rank", 0, settings["neutralization"],
                "cand_12b02a90cd36d2d0fa131393", lifecycle, status, 1, now, now,
            ),
        )
    alpha_db = tmp_path / "alpha.db"
    machine.init_cache(str(alpha_db))
    machine.cache_put(
        str(alpha_db), sim_key, {"expr": "rank(close)", "decay": 0}, settings,
        {
            "status": status,
            "simulation_url": url if status not in {"UNCERTAIN_SUBMISSION", "AUTH_ERROR"} else None,
            "submitted_at": now if status in {"RUNNING", "SUBMITTED"} else None,
            "error": None,
        },
    )
    # Durable ledger row matching a real Repair NEW_POST dispatch (batch 30).
    sync_simulation_ledger(
        store, alpha_db, "round_run_0006", "run_0006",
        batch_no=30, phase="REPAIR", candidate_ids=[candidate_id],
        origin_by_candidate={candidate_id: "NEW_POST"},
        selection_mode_by_candidate={candidate_id: "REPAIR_SELECTION"},
        repair_plan_by_candidate={candidate_id: "rplan_7382826da7fa62f831b9fc20"},
    )
    # Materialize the durable remote-work row from the cache facts (as a real
    # process start would), then force it due so reconciliation can pick it up.
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    return cfg, store, alpha_db, sim_key, settings


def _round_counters(store):
    with store.connect() as conn:
        rr = conn.execute(
            "SELECT search_consumed,repair_consumed,current_batch,status,phase FROM ppl_rounds WHERE run_id='run_0006'"
        ).fetchone()
        run = conn.execute(
            "SELECT post_attempted,post_confirmed,post_uncertain,post_consumed FROM ppl_runs WHERE run_id='run_0006'"
        ).fetchone()
    return {"search_consumed": rr[0], "repair_consumed": rr[1], "current_batch": rr[2],
            "round_status": rr[3], "phase": rr[4],
            "post_attempted": run[0], "post_confirmed": run[1],
            "post_uncertain": run[2], "post_consumed": run[3]}


class Response:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.get_calls = []
        self.post_calls = []

    def get(self, url, **kwargs):
        self.get_calls.append(url)
        if not self.responses:
            raise AssertionError("unexpected GET")
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def post(self, url, **kwargs):
        self.post_calls.append(url)
        raise AssertionError("reconcile must never POST")

    def request(self, method, url, **kwargs):
        method = str(method).upper()
        if method == "GET":
            return self.get(url, **kwargs)
        if method == "POST":
            return self.post(url, **kwargs)
        raise AssertionError(f"unexpected method {method}")


ALPHA_RESULT = {"is": {"sharpe": 1.42, "fitness": 0.71, "turnover": 0.3942, "margin": 0.0003},
                "regular": {"code": "rank(close)"}, "settings": {"decay": 0}}


def test_complete_existing_remote_work_reconciles_durably(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    before = _round_counters(store)
    session = Session([
        Response(200, {"status": "COMPLETE", "alpha": "j2jKo0gj"}),
        Response(200, ALPHA_RESULT),
    ])
    report = reconcile_existing_remote_only(
        store, cfg, machine, session, alpha_db, "run_0006",
        candidate_id=CANDIDATE_ID, sim_key=sim_key,
    )
    assert report["completed_candidate_ids"] == [CANDIDATE_ID]
    assert report["simulation_posts"] == 0 and session.post_calls == []
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["simulation_status"] == "COMPLETE"
    assert candidate["lifecycle_state"] == "SIMULATION_COMPLETE"
    assert candidate["alpha_id"] == "j2jKo0gj"
    with store.connect() as conn:
        rw = conn.execute(
            "SELECT queue_state,remote_status,simulation_url,reserved_slot FROM ppl_remote_work WHERE sim_key=?",
            (sim_key,),
        ).fetchone()
    assert rw[0] == "COMPLETE" and rw[1] == "COMPLETE" and rw[2] and rw[3] == 0
    with store.connect() as conn:
        row = conn.execute(
            """SELECT batch_no,phase,origin,selection_mode,simulation_status,alpha_id
               FROM ppl_round_simulation_ledger WHERE round_id='round_run_0006' AND sim_key=?""",
            (sim_key,),
        ).fetchone()
    assert tuple(row) == (30, "REPAIR", "NEW_POST", "REPAIR_SELECTION", "COMPLETE", "j2jKo0gj")
    assert _round_counters(store) == before  # budget / counters untouched


def test_running_existing_remote_work_stays_non_terminal(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    session = Session([Response(200, {"status": "RUNNING", "progress": 0.4}, {"Retry-After": "7"})])
    report = reconcile_existing_remote_only(
        store, cfg, machine, session, alpha_db, "run_0006",
        candidate_id=CANDIDATE_ID, sim_key=sim_key,
    )
    assert report["completed_candidate_ids"] == []
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["simulation_status"] in {"SUBMITTED", "RUNNING"}  # non-terminal
    assert candidate["lifecycle_state"] == "SIMULATION_RUNNING"
    assert candidate["alpha_id"] is None
    with store.connect() as conn:
        rw = conn.execute("SELECT queue_state,reserved_slot FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
    assert rw[0] == "WAIT_REMOTE" and rw[1] == 1
    assert session.post_calls == []


def test_remote_not_found_two_observations_and_never_reposts(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    session = Session([Response(404, {})])
    out1 = reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                          candidate_id=CANDIDATE_ID, sim_key=sim_key)
    assert out1["remote_missing_candidate_ids"] == []
    with store.connect() as conn:
        row = conn.execute("SELECT queue_state,missing_confirmations FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
        assert row[0] == "MISSING_CONFIRMATION_PENDING" and row[1] == 1
        conn.execute("UPDATE ppl_remote_work SET next_poll_at='2000-01-01T00:00:00+00:00' WHERE sim_key=?", (sim_key,))
    session2 = Session([Response(410, {})])
    out2 = reconcile_existing_remote_only(store, cfg, machine, session2, alpha_db, "run_0006",
                                          candidate_id=CANDIDATE_ID, sim_key=sim_key)
    assert out2["remote_missing_candidate_ids"] == [CANDIDATE_ID]
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["lifecycle_state"] == "SIMULATION_REMOTE_MISSING"
    assert session.post_calls == [] and session2.post_calls == []


def test_network_error_never_posts(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    session = Session([ConnectionError("boom")])
    report = reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                            candidate_id=CANDIDATE_ID, sim_key=sim_key)
    assert report["completed_candidate_ids"] == []
    with store.connect() as conn:
        row = conn.execute("SELECT queue_state,last_error FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
    assert row[0] == "WAIT_NETWORK" and "ConnectionError" in str(row[1])
    assert session.post_calls == []


def test_uncertain_never_reposted_and_not_selected_without_target(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(
        tmp_path, status="UNCERTAIN_SUBMISSION", url="",
        lifecycle="SIMULATION_PENDING",
    )
    # No explicit target: UNCERTAIN is not in the due set, so nothing is polled.
    session = Session([])
    report = reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006")
    assert report["polled"] == 0
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["lifecycle_state"] == "SIMULATION_PENDING"
    assert candidate["simulation_status"] == "UNCERTAIN_SUBMISSION"
    with store.connect() as conn:
        rw = conn.execute("SELECT queue_state,reserved_slot FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
    assert rw[0] == "QUARANTINED_UNCERTAIN" and rw[1] == 1
    assert session.post_calls == []
    # Explicitly targeting the UNCERTAIN candidate without a durable URL fails closed.
    with pytest.raises(ConfigError, match="RECONCILE_FAIL_CLOSED_NO_DURABLE_SIMULATION_URL"):
        reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                       candidate_id=CANDIDATE_ID, sim_key=sim_key)


def test_missing_simulation_url_fails_closed(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path, status="SUBMITTED", url="")
    # A durable remote-work row exists (e.g. stale handoff) but carries no URL:
    # reconciliation must fail closed instead of guessing or POSTing.
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_remote_work(
                   run_id,candidate_id,sim_key,simulation_url,remote_status,queue_state,next_poll_at,
                   poll_attempts,missing_confirmations,reserved_slot,retry_after_seconds,submitted_at,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,sim_key) DO UPDATE SET simulation_url=NULL,queue_state='WAIT_REMOTE'""",
            ("run_0006", CANDIDATE_ID, sim_key, None, "SUBMITTED", "WAIT_REMOTE", now,
             0, 0, 1, 0.5, now, now, now),
        )
    with pytest.raises(ConfigError, match="RECONCILE_FAIL_CLOSED_NO_DURABLE_SIMULATION_URL"):
        reconcile_existing_remote_only(store, cfg, machine, session=Session([]), alpha_db=alpha_db,
                                       run_id="run_0006", candidate_id=CANDIDATE_ID, sim_key=sim_key)


def test_sim_key_mismatch_fails_closed(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    with pytest.raises(ConfigError, match="RECONCILE_FAIL_CLOSED_SIM_KEY_MISMATCH"):
        reconcile_existing_remote_only(store, cfg, machine, session=Session([]), alpha_db=alpha_db,
                                       run_id="run_0006", candidate_id=CANDIDATE_ID,
                                       sim_key="F" * 64)


def test_candidate_mismatch_fails_closed(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    with pytest.raises(ConfigError, match="RECONCILE_FAIL_CLOSED_CANDIDATE_NO_REMOTE_WORK"):
        reconcile_existing_remote_only(store, cfg, machine, session=Session([]), alpha_db=alpha_db,
                                       run_id="run_0006", candidate_id="cand_no_such",
                                       sim_key=sim_key)


def test_scheduler_and_selector_never_entered(tmp_path, monkeypatch):
    import ppl_engine.round_orchestrator as orch
    for name in ("_select_search_batch", "_select_repair_batch", "_materialize_selector_virtual_plans",
                 "_scheduler_shadow_observation", "_continuous_analyze_and_enqueue_checks"):
        monkeypatch.setattr(orch, name, lambda *a, **k: pytest.fail(f"{name} must never be called"))
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    session = Session([Response(200, {"status": "COMPLETE", "alpha": "j2jKo0gj"}), Response(200, ALPHA_RESULT)])
    report = reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                            candidate_id=CANDIDATE_ID, sim_key=sim_key)
    assert report["scheduler_ticks"] == 0
    assert report["completed_candidate_ids"] == [CANDIDATE_ID]


def test_repair_plan_creation_never_happens(tmp_path, monkeypatch):
    import ppl_engine.check_derived_repair as cdr
    import ppl_engine.production_repair as pr
    monkeypatch.setattr(cdr, "derive_check_repair_proposals", lambda *a, **k: pytest.fail("no plan derivation"))
    monkeypatch.setattr(pr, "execute_production_repair", lambda *a, **k: pytest.fail("no repair execution"))
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    session = Session([Response(200, {"status": "COMPLETE", "alpha": "j2jKo0gj"}), Response(200, ALPHA_RESULT)])
    reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                   candidate_id=CANDIDATE_ID, sim_key=sim_key)
    with store.connect() as conn:
        plans = conn.execute("SELECT COUNT(*) FROM ppl_repair_plans WHERE run_id='run_0006'").fetchone()[0]
        children = conn.execute("SELECT COUNT(*) FROM ppl_candidates WHERE run_id='run_0006' AND candidate_id LIKE 'repair_%'").fetchone()[0]
    assert plans == 0 and children == 1  # no new RepairPlan, no new child


def test_budget_delta_zero_and_no_new_search_candidates(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    before = _round_counters(store)
    session = Session([Response(200, {"status": "COMPLETE", "alpha": "j2jKo0gj"}), Response(200, ALPHA_RESULT)])
    reconcile_existing_remote_only(store, cfg, machine, session, alpha_db, "run_0006",
                                   candidate_id=CANDIDATE_ID, sim_key=sim_key)
    after = _round_counters(store)
    assert after == before
    with store.connect() as conn:
        n = conn.execute("SELECT COUNT(*) FROM ppl_candidates WHERE run_id='run_0006'").fetchone()[0]
        nm = conn.execute("SELECT COUNT(*) FROM ppl_remote_work WHERE run_id='run_0006'").fetchone()[0]
    assert n == 1 and nm == 1  # no candidate/remote_work duplication
