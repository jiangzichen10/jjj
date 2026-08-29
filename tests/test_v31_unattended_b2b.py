import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_check import enqueue_manual_refresh_checks, poll_due_checks
from ppl_engine.continuous_control import due_snapshot, recover_waiting_auth
from ppl_engine.continuous_discovery import (
    enqueue_discovery_refresh,
    materialize_discovery_result,
    poll_due_discovery_work,
    ready_discovery_work,
)
from ppl_engine.round_store import create_round
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _store(tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    return cfg, store


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, timeout=60):
        self.calls.append((url, timeout))
        if not self.responses:
            raise AssertionError("unexpected GET")
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def _dataset(ds="ds_new"):
    return {
        "id": ds,
        "name": "Price derived data",
        "description": "daily price and return features",
        "category": {"name": "Market"},
        "subcategory": {"name": "Price"},
        "userCount": 10,
        "alphaCount": 2,
    }


def _field(ds="ds_new", fid="f1", coverage=0.95):
    return {
        "id": fid,
        "type": "MATRIX",
        "description": "close price signal",
        "dataCoverage": coverage,
        "dataset": {"id": ds},
        "userCount": 3,
        "alphaCount": 1,
    }


def test_discovery_queue_uses_one_get_per_scheduler_call_and_materializes_result(tmp_path):
    cfg, store = _store(tmp_path)
    work = enqueue_discovery_refresh(
        store, "run_0006", "round_run_0006", refresh_no=1, batch_no=5,
        trigger="PERIODIC", excluded_dataset_ids=["old_ds"], probe_count=2, admit_count=1,
    )
    assert work["created_new"] is True
    session = Session([
        Response(200, {"results": [_dataset()]}),
        Response(200, {"count": 1, "results": [_field()]}),
    ])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()

    first = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert first["polled"] == 1
    assert len(session.calls) == 1
    with store.connect() as conn:
        row = dict(conn.execute("select * from ppl_discovery_work where discovery_work_id=?", (work["discovery_work_id"],)).fetchone())
    assert row["stage"] == "FIELDS" and row["queue_state"] == "DISCOVERY_DUE"

    second = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert second["polled"] == 1
    assert len(session.calls) == 2
    ready = ready_discovery_work(store, "run_0006")
    assert len(ready) == 1 and ready[0]["queue_state"] == "READY_APPLY"
    result = materialize_discovery_result(ready[0], cfg)
    assert result.snapshot["rolling_probe_dataset_ids"] == ["ds_new"]
    assert result.snapshot["rolling_admitted_dataset_ids"] == ["ds_new"]
    assert result.snapshot["field_count"] == 1


def test_discovery_429_is_durable_and_visible_to_due_scheduler(tmp_path):
    cfg, store = _store(tmp_path)
    enqueue_discovery_refresh(
        store, "run_0006", "round_run_0006", refresh_no=1, batch_no=0,
        trigger="LOW_POOL", excluded_dataset_ids=[], probe_count=1, admit_count=1,
    )
    session = Session([Response(429, {}, {"Retry-After": "19"})])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()
    out = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert out["waits"] == 1 and len(session.calls) == 1
    with store.connect() as conn:
        row = conn.execute("select queue_state,retry_after_seconds,last_http_status from ppl_discovery_work").fetchone()
    assert row[0] == "WAIT_RATE_LIMIT" and float(row[1]) == 19.0 and row[2] == 429
    snap = due_snapshot(store, "run_0006", default_wait_seconds=30, max_wait_seconds=300)
    assert snap.discovery_due == 0
    assert snap.next_due_at is not None


def test_discovery_wait_auth_is_released_by_single_auth_coordinator(tmp_path):
    cfg, store = _store(tmp_path)
    enqueue_discovery_refresh(
        store, "run_0006", "round_run_0006", refresh_no=1, batch_no=0,
        trigger="PERIODIC", excluded_dataset_ids=[], probe_count=1, admit_count=1,
    )
    session = Session([Response(401, {})])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()
    out = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert out["auth_waits"] == 1
    calls = {"n": 0}

    class AuthMachine:
        @staticmethod
        def ensure_session(s):
            calls["n"] += 1
            return s

    recovered = recover_waiting_auth(store, AuthMachine, session, "run_0006")
    assert recovered["success"] is True and recovered["discovery_released"] == 1
    assert calls["n"] == 1
    with store.connect() as conn:
        state = conn.execute("select queue_state from ppl_discovery_work").fetchone()[0]
    assert state == "DISCOVERY_DUE"


def test_manual_finalization_recheck_reopens_terminal_work_without_candidate_transition(tmp_path):
    cfg, store = _store(tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,alpha_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)","sk1",json.dumps(cfg.plan["simulation_settings"]),
             "close","MATRIX","rank",0,cfg.plan["simulation_settings"]["neutralization"],
             "PRE_TAG_CHECK_PASS","COMPLETE","A1",now,now),
        )
    first = enqueue_manual_refresh_checks(store, "run_0006", ["cand1"])
    assert first["queued_count"] == 1
    with store.connect() as conn:
        conn.execute("update ppl_check_work set queue_state='RESOLVED',attempt_count=9 where phase='RECHECK'")
    second = enqueue_manual_refresh_checks(store, "run_0006", ["cand1"])
    assert second["queued_count"] == 1
    with store.connect() as conn:
        row = conn.execute("select phase,queue_state,attempt_count from ppl_check_work where candidate_id='cand1'").fetchone()
    assert tuple(row) == ("RECHECK", "CHECK_DUE", 0)
    assert store.load_candidates("run_0006")[0]["lifecycle_state"] == "PRE_TAG_CHECK_PASS"


def test_continuous_auto_manual_refresh_ignores_lifetime_check_budget(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro

    cfg, store = _store(tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,alpha_id,signal_family,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("cand1","run_0006","rank(close)","sk1",json.dumps(cfg.plan["simulation_settings"]),
             "close","MATRIX","rank",0,cfg.plan["simulation_settings"]["neutralization"],
             "PRE_TAG_CHECK_PASS","COMPLETE","A1","fam1",now,now),
        )
    policy = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    # Deliberately make the legacy lifetime budget unusable. Continuous auto
    # refresh must still enqueue durable RECHECK work and perform zero GETs now.
    cfg.plan["budgets"]["max_check_candidates"] = 0
    cfg.plan["budgets"]["max_check_http_requests"] = 0
    monkeypatch.setattr(ro, "classify_run", lambda *a, **k: [
        {"candidate_id": "cand1", "classification": "PPL_READY_FOR_MANUAL_FINALIZATION"}
    ])
    out = ro._maybe_auto_refresh_manual_finalization(
        store, cfg, object(), object(), tmp_path / "alpha.db", "run_0006", "round_run_0006",
        policy, tmp_path, batch_no=10,
    )
    assert out["queue_mode"] == "V31_DURABLE_RECHECK"
    assert out["queued_check_count"] == 1 and out["executed_check_count"] == 0
    with store.connect() as conn:
        row = conn.execute("select phase,queue_state from ppl_check_work where candidate_id='cand1'").fetchone()
    assert tuple(row) == ("RECHECK", "CHECK_DUE")


def test_report_failure_degrades_without_pausing_continuous_research(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro

    cfg, store = _store(tmp_path)
    policy = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: (_ for _ in ()).throw(OSError("disk busy")))
    out = ro._write_reports_resilient(
        store, cfg, tmp_path / "alpha.db", "run_0006", "round_run_0006", policy, tmp_path,
        continuous_enabled=True, retry_seconds=12,
    )
    assert out["degraded"] is True and "disk busy" in out["error"]
    with store.connect() as conn:
        row = conn.execute(
            "select wait_state,retry_after_seconds,last_error from ppl_endpoint_waits where endpoint_type='REPORT'"
        ).fetchone()
    assert row[0] == "WAIT_REPORT" and float(row[1]) == 12.0 and "disk busy" in row[2]
    snap = due_snapshot(store, "run_0006", default_wait_seconds=30, max_wait_seconds=300)
    assert snap.next_due_at is not None


def test_report_sqlite_failure_remains_fail_closed(monkeypatch, tmp_path):
    import sqlite3
    import ppl_engine.round_orchestrator as ro

    cfg, store = _store(tmp_path)
    policy = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("readonly")))
    try:
        ro._write_reports_resilient(
            store, cfg, tmp_path / "alpha.db", "run_0006", "round_run_0006", policy, tmp_path,
            continuous_enabled=True,
        )
    except sqlite3.OperationalError as exc:
        assert "readonly" in str(exc)
    else:
        raise AssertionError("core SQLite failure must not be degraded")
