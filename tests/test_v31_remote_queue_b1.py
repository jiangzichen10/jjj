import json
from datetime import datetime, timezone
from pathlib import Path

import machine_lib_V2_1 as machine

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_remote import (
    due_remote_work,
    poll_due_remote_work,
    remote_slot_snapshot,
    sync_remote_work_from_durable_facts,
)
from ppl_engine.live_execution import _v21_candidate, execute_continuous_remote_handoff
from ppl_engine.research_telemetry import sync_simulation_ledger
from ppl_engine.round_store import create_round
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]


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


def _setup(tmp_path, *, status="RUNNING", url="https://api.worldquantbrain.com/simulations/test1"):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,
                   operator,decay,neutralization,lifecycle_state,simulation_status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "cand1", "run_0006", "rank(close)", sim_key, json.dumps(settings), "close", "MATRIX",
                "rank", 0, settings["neutralization"], "SIMULATION_RUNNING", status, now, now,
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
    return cfg, store, alpha_db, sim_key, settings


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
        raise AssertionError("poll queue must never POST")

    def request(self, method, url, **kwargs):
        method = str(method).upper()
        if method == "GET":
            return self.get(url, **kwargs)
        if method == "POST":
            return self.post(url, **kwargs)
        raise AssertionError(f"unexpected method {method}")


def test_remote_queue_schema_and_startup_slot_accounting(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    out = sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    assert out["reserved"] == 1
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert slots.reserved_slots == 1
    assert slots.free_slots == 3
    due = due_remote_work(store, "run_0006")
    assert [x["sim_key"] for x in due] == [sim_key]


def test_uncertain_submission_reserves_one_slot_but_is_not_due_for_poll(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path, status="UNCERTAIN_SUBMISSION", url="")
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert slots.uncertain == 1
    assert slots.reserved_slots == 1
    assert slots.free_slots == 3
    assert due_remote_work(store, "run_0006") == []


def test_one_shot_running_poll_keeps_slot_and_returns_without_sleep_loop(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([Response(200, {"status": "RUNNING", "progress": 0.2}, {"Retry-After": "7"})])
    out = poll_due_remote_work(store, cfg, machine, session, alpha_db, "run_0006", limit=4)
    assert out["polled"] == 1
    assert out["completed_candidate_ids"] == []
    assert len(session.get_calls) == 1
    assert session.post_calls == []
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert slots.reserved_slots == 1
    with store.connect() as conn:
        row = conn.execute("SELECT queue_state,retry_after_seconds FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
    assert row[0] == "WAIT_REMOTE"
    assert float(row[1]) == 7.0


def test_complete_poll_fetches_metrics_releases_slot_and_updates_candidate(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([
        Response(200, {"status": "COMPLETE", "alpha": "A1"}),
        Response(200, {"is": {"sharpe": 1.9, "fitness": 1.1, "turnover": 0.4, "margin": 0.001},
                       "regular": {"code": "rank(close)"}, "settings": {"decay": 0}}),
    ])
    out = poll_due_remote_work(store, cfg, machine, session, alpha_db, "run_0006", limit=4)
    assert out["completed_candidate_ids"] == ["cand1"]
    assert len(session.get_calls) == 2
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert slots.reserved_slots == 0
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["simulation_status"] == "COMPLETE"
    assert candidate["lifecycle_state"] == "SIMULATION_COMPLETE"
    fact = machine.cache_get(str(alpha_db), sim_key)
    assert fact["alpha_id"] == "A1"
    assert fact["sharpe"] == 1.9


def test_async_poll_completion_refreshes_existing_ledger_without_reassigning_batch(tmp_path):
    from ppl_engine.round_orchestrator import _sync_research_telemetry

    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006",
        policy={"objective": "test", "batch_size": 4},
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_candidates
               SET data_field_count_estimate=1,pp_total_operator_count_estimate=1
               WHERE candidate_id='cand1'"""
        )
    sync_simulation_ledger(
        store, alpha_db, "round_run_0006", "run_0006",
        batch_no=7, phase="SEARCH", candidate_ids=["cand1"],
        origin_by_candidate={"cand1": "NEW_POST"},
        selection_mode_by_candidate={"cand1": "TEST_SELECTION"},
    )
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([
        Response(200, {"status": "COMPLETE", "alpha": "A1"}),
        Response(200, {"is": {"sharpe": 1.9, "fitness": 1.1, "turnover": 0.4, "margin": 0.001},
                       "regular": {"code": "rank(close)"}, "settings": {"decay": 0}}),
    ])
    out = poll_due_remote_work(store, cfg, machine, session, alpha_db, "run_0006", limit=4)
    assert out["completed_candidate_ids"] == ["cand1"]

    # This is the exact completion-side sync used by the Continuous orchestrator:
    # no new batch/phase is supplied, so the original dispatch attribution must
    # survive while terminal Simulation facts are refreshed.
    _sync_research_telemetry(
        store, cfg, alpha_db, "run_0006", "round_run_0006",
        candidate_ids=["cand1"],
    )
    with store.connect() as conn:
        row = conn.execute(
            """SELECT batch_no,phase,origin,selection_mode,simulation_status,alpha_id
               FROM ppl_round_simulation_ledger
               WHERE round_id='round_run_0006' AND sim_key=?""",
            (sim_key,),
        ).fetchone()
    assert tuple(row) == (7, "SEARCH", "NEW_POST", "TEST_SELECTION", "COMPLETE", "A1")


def test_404_requires_two_observations_and_never_reposts(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    first = Session([Response(404, {})])
    out1 = poll_due_remote_work(store, cfg, machine, first, alpha_db, "run_0006")
    assert out1["remote_missing_candidate_ids"] == []
    assert first.post_calls == []
    with store.connect() as conn:
        row = conn.execute("SELECT queue_state,missing_confirmations FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
        assert row[0] == "MISSING_CONFIRMATION_PENDING" and row[1] == 1
        conn.execute("UPDATE ppl_remote_work SET next_poll_at='2000-01-01T00:00:00+00:00' WHERE sim_key=?", (sim_key,))
    second = Session([Response(410, {})])
    out2 = poll_due_remote_work(store, cfg, machine, second, alpha_db, "run_0006")
    assert out2["remote_missing_candidate_ids"] == ["cand1"]
    assert second.post_calls == []
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["simulation_status"] == "REMOTE_NOT_FOUND"
    assert candidate["lifecycle_state"] == "SIMULATION_REMOTE_MISSING"
    assert remote_slot_snapshot(store, "run_0006", 4).reserved_slots == 0


def test_429_is_scoped_wait_and_preserves_remote_slot(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([Response(429, {}, {"Retry-After": "11"})])
    out = poll_due_remote_work(store, cfg, machine, session, alpha_db, "run_0006")
    assert out["waits"] == 1
    assert session.post_calls == []
    with store.connect() as conn:
        row = conn.execute("SELECT queue_state,retry_after_seconds FROM ppl_remote_work WHERE sim_key=?", (sim_key,)).fetchone()
    assert row[0] == "WAIT_RATE_LIMIT"
    assert float(row[1]) == 11.0
    assert remote_slot_snapshot(store, "run_0006", 4).reserved_slots == 1


def test_continuous_handoff_submits_and_returns_without_poll(monkeypatch, tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)",sim_key,json.dumps(settings),"close","MATRIX","rank",0,
             settings["neutralization"],"SIMULATION_PENDING",None,now,now),
        )
    alpha_db = tmp_path / "alpha.db"; machine.init_cache(str(alpha_db))
    row = store.load_candidates("run_0006")[0]
    calls = {"submit": 0, "poll": 0}

    def fake_submit(session, candidate, effective, **kwargs):
        calls["submit"] += 1
        # HOTFIX3 regression: direct Continuous handoff must pass the complete
        # V2.1 /simulations settings payload, not the compact adapter tuple.
        for required in (
            "instrumentType", "region", "universe", "delay", "decay",
            "neutralization", "truncation", "pasteurization", "testPeriod",
            "unitHandling", "nanHandling", "language", "visualization",
        ):
            assert required in effective
        assert effective["instrumentType"] == "EQUITY"
        # HOTFIX4: POST must use the exact durable full settings, not a rebuilt
        # or projected representation.
        assert effective == settings
        assert machine.simulation_key(candidate["expr"], effective) == sim_key
        machine.cache_put(
            str(alpha_db), sim_key, candidate, effective,
            {"status":"SUBMITTED","simulation_url":"https://api.worldquantbrain.com/simulations/X1",
             "submitted_at":now,"last_http_status":201,"last_retry_after":2.0},
        )
        kwargs.get("submission_meta", {}).update({"retry_after": 2.0})
        return "https://api.worldquantbrain.com/simulations/X1"

    monkeypatch.setattr(machine, "submit_simulation", fake_submit)
    monkeypatch.setattr(machine, "wait_simulation", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not poll")))
    wrapper = {"execution_action":"NEW_SIMULATION_REQUIRED", "v21_candidate":_v21_candidate(row, cfg.target_mode)}
    out = execute_continuous_remote_handoff(
        store, cfg, machine, object(), alpha_db, "run_0006", [wrapper], {sim_key:"cand1"},
        allow_simulation_post=True,
    )
    assert calls == {"submit": 1, "poll": 0}
    assert out["post_confirmed"] == 1
    candidate = store.load_candidates("run_0006")[0]
    assert candidate["simulation_status"] == "SUBMITTED"
    assert candidate["lifecycle_state"] == "SIMULATION_RUNNING"
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert slots.reserved_slots == 1 and slots.free_slots == 3


def test_search_execution_uses_nonblocking_handoff_in_continuous_mode(monkeypatch, tmp_path):
    from ppl_engine.round_orchestrator import _execute_search_rows

    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,signal_family,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)",sim_key,json.dumps(settings),"close","MATRIX","rank",0,
             settings["neutralization"],"PLANNED",None,"TEST_FAMILY",now,now),
        )
    alpha_db = tmp_path / "alpha.db"; machine.init_cache(str(alpha_db))
    calls = {"submit": 0, "poll": 0}

    def fake_submit(session, candidate, effective, **kwargs):
        calls["submit"] += 1
        machine.cache_put(
            str(alpha_db), sim_key, candidate, effective,
            {"status":"SUBMITTED","simulation_url":"https://api.worldquantbrain.com/simulations/S1",
             "submitted_at":now,"last_http_status":201,"last_retry_after":1.0},
        )
        kwargs.get("submission_meta", {}).update({"retry_after": 1.0})
        return "https://api.worldquantbrain.com/simulations/S1"

    monkeypatch.setattr(machine, "submit_simulation", fake_submit)
    monkeypatch.setattr(machine, "wait_simulation", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not poll")))
    row = store.load_candidates("run_0006")[0]
    out = _execute_search_rows(
        store, cfg, machine, object(), alpha_db, "run_0006", [row],
        allow_simulation_post=True, remaining_search_budget=1, nonblocking_remote=True,
    )
    assert calls == {"submit": 1, "poll": 0}
    assert out["post_consumed"] == 1
    assert out["complete_candidate_ids"] == []
    assert out["nonterminal_candidate_ids"] == ["cand1"]
    assert remote_slot_snapshot(store, "run_0006", 4).reserved_slots == 1


def test_continuous_runtime_guard_scopes_uncertain_instead_of_global_raise(tmp_path):
    from ppl_engine.round_orchestrator import _round_runtime_guard

    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path, status="UNCERTAIN_SUBMISSION", url="")
    scoped = _round_runtime_guard(store, "run_0006", global_hold=False)
    assert scoped["uncertain"] == 1
    import pytest
    with pytest.raises(Exception, match="ROUND_UNCERTAIN_SUBMISSION_HOLD"):
        _round_runtime_guard(store, "run_0006", global_hold=True)



def test_startup_reconcile_uses_alpha_cache_durable_first_identity(tmp_path):
    cfg, store, alpha_db, sim_key, settings = _setup(tmp_path)
    # Simulate the historical durable-first split: alpha cache has the URL,
    # while RunnerStore candidate sync never advanced after the POST.
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_candidates SET simulation_status='NONE',lifecycle_state='SIMULATION_PENDING',result_reference_json=NULL WHERE sim_key=?",
            (sim_key,),
        )
    out = sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    assert out["reserved"] == 1
    with store.connect() as conn:
        row = conn.execute(
            "SELECT simulation_url,queue_state,reserved_slot FROM ppl_remote_work WHERE sim_key=?", (sim_key,)
        ).fetchone()
    assert row[0] == "https://api.worldquantbrain.com/simulations/test1"
    assert row[1] == "POLL_DUE"
    assert row[2] == 1


def test_rate_limit_backoff_survives_startup_reconcile(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([Response(429, {}, {"Retry-After": "120"})])
    poll_due_remote_work(store, cfg, machine, session, alpha_db, "run_0006")
    with store.connect() as conn:
        before = conn.execute(
            "SELECT queue_state,next_poll_at,retry_after_seconds FROM ppl_remote_work WHERE sim_key=?", (sim_key,)
        ).fetchone()
    assert before[0] == "WAIT_RATE_LIMIT"
    # A process restart/reconcile must not force the record due and violate
    # the durable endpoint Retry-After window.
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    with store.connect() as conn:
        after = conn.execute(
            "SELECT queue_state,next_poll_at,retry_after_seconds FROM ppl_remote_work WHERE sim_key=?", (sim_key,)
        ).fetchone()
    assert after[0] == "WAIT_RATE_LIMIT"
    assert after[1] == before[1]
    assert float(after[2]) == 120.0
    assert due_remote_work(store, "run_0006") == []


def test_http_500_becomes_scoped_network_wait_and_preserves_slot(tmp_path):
    cfg, store, alpha_db, sim_key, _ = _setup(tmp_path)
    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    session = Session([Response(500, {})])
    out = poll_due_remote_work(
        store, cfg, machine, session, alpha_db, "run_0006",
        network_backoff_seconds=3, max_network_backoff_seconds=30,
    )
    assert out["waits"] == 1
    with store.connect() as conn:
        row = conn.execute(
            "SELECT queue_state,last_http_status,reserved_slot FROM ppl_remote_work WHERE sim_key=?", (sim_key,)
        ).fetchone()
    assert row[0] == "WAIT_NETWORK"
    assert row[1] == 500
    assert row[2] == 1
    assert remote_slot_snapshot(store, "run_0006", 4).free_slots == 3


def test_continuous_handoff_respects_execution_allow_new_simulations(monkeypatch, tmp_path):
    import pytest
    from ppl_engine.config import ConfigError

    cfg = _config()
    cfg.plan["execution"]["allow_new_simulations"] = False
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)",sim_key,json.dumps(settings),"close","MATRIX","rank",0,
             settings["neutralization"],"SIMULATION_PENDING",None,now,now),
        )
    alpha_db = tmp_path / "alpha.db"; machine.init_cache(str(alpha_db))
    row = store.load_candidates("run_0006")[0]
    wrapper = {"execution_action":"NEW_SIMULATION_REQUIRED", "v21_candidate":_v21_candidate(row, cfg.target_mode)}
    with pytest.raises(ConfigError, match="SIMULATION_POST_DISABLED"):
        execute_continuous_remote_handoff(
            store, cfg, machine, object(), alpha_db, "run_0006", [wrapper], {sim_key:"cand1"},
            allow_simulation_post=True,
        )


def test_hotfix3_reopens_only_definitive_missing_settings_400(tmp_path):
    from ppl_engine.candidate_factory import classify_cache_read_only

    alpha_db = tmp_path / "alpha_reopen.db"
    machine.init_cache(str(alpha_db))
    sim_key, settings = _candidate_identity(_config())
    candidate = {"expr": "rank(close)", "decay": 0}
    machine.cache_put(
        str(alpha_db), sim_key, candidate, settings,
        {
            "status": "INVALID",
            "last_http_status": 400,
            "error": (
                'category=INVALID | method=POST | url=https://api.worldquantbrain.com/simulations | status=400 | '
                'body={"settings":{"instrumentType":["This field is required."],"decay":["This field is required."],'
                '"pasteurization":["This field is required."],"unitHandling":["This field is required."],'
                '"nanHandling":["This field is required."],"language":["This field is required."],'
                '"visualization":["This field is required."]}}'
            ),
        },
    )
    out = classify_cache_read_only(alpha_db, sim_key)
    assert out["cache_classification"] == "CACHE_INVALID_CLIENT_PAYLOAD_REJECTED"
    assert out["execution_action"] == "RETRY_PER_V21_POLICY"


def test_hotfix3_generic_invalid_remains_terminal(tmp_path):
    from ppl_engine.candidate_factory import classify_cache_read_only

    alpha_db = tmp_path / "alpha_invalid.db"
    machine.init_cache(str(alpha_db))
    sim_key, settings = _candidate_identity(_config(), expr="ts_mean(close, 5)")
    candidate = {"expr": "ts_mean(close, 5)", "decay": 0}
    machine.cache_put(
        str(alpha_db), sim_key, candidate, settings,
        {"status": "INVALID", "last_http_status": 400, "error": "ordinary formula validation failure"},
    )
    out = classify_cache_read_only(alpha_db, sim_key)
    assert out["cache_classification"] == "CACHE_INVALID"
    assert out["execution_action"] == "STOP_INVALID"


def test_hotfix4_effective_settings_is_full_durable_identity(tmp_path):
    from ppl_engine.simulation_adapter import _effective_settings

    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)",sim_key,json.dumps(settings),"close","MATRIX","rank",0,
             settings["neutralization"],"SIMULATION_PENDING",None,now,now),
        )
    row = store.load_candidates("run_0006")[0]
    v21 = _v21_candidate(row, cfg.target_mode)
    effective = _effective_settings(v21, cfg.plan["simulation_settings"], machine)
    assert effective == settings
    assert set(settings).issubset(effective)
    assert machine.simulation_key(v21["expr"], effective) == sim_key


def test_hotfix4_final_post_identity_guard_blocks_mismatch(monkeypatch, tmp_path):
    import pytest
    import ppl_engine.simulation_adapter as adapter
    from ppl_engine.config import ConfigError

    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    sim_key, settings = _candidate_identity(cfg)
    tampered = dict(settings)
    tampered["neutralization"] = "MARKET"
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)",sim_key,json.dumps(tampered),"close","MATRIX","rank",0,
             tampered["neutralization"],"SIMULATION_PENDING",None,now,now),
        )
    alpha_db = tmp_path / "alpha.db"; machine.init_cache(str(alpha_db))
    row = store.load_candidates("run_0006")[0]
    called = {"submit": 0}

    # Bypass the earlier generic adapter identity check so this regression test
    # exercises the final guard located immediately before HTTP POST.
    monkeypatch.setattr(adapter, "_validate_expected_sim_key", lambda *a, **k: None)
    monkeypatch.setattr(
        machine, "submit_simulation",
        lambda *a, **k: called.__setitem__("submit", called["submit"] + 1),
    )
    wrapper = {"execution_action":"NEW_SIMULATION_REQUIRED", "v21_candidate":_v21_candidate(row, cfg.target_mode)}
    with pytest.raises(ConfigError, match="POST_SETTINGS_IDENTITY_MISMATCH"):
        execute_continuous_remote_handoff(
            store, cfg, machine, object(), alpha_db, "run_0006", [wrapper], {sim_key:"cand1"},
            allow_simulation_post=True,
        )
    assert called["submit"] == 0
