import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_discovery import poll_due_discovery_work
from ppl_engine.round_orchestrator import (
    _batch_by_no,
    _enqueue_continuous_rolling_discovery,
    _finalize_recovered_repair_batch,
    _finalize_recovered_search_batch,
)
from ppl_engine.round_store import create_round, start_batch
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _policy():
    return yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))


def _store(tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    policy = _policy()
    create_round(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        policy=policy,
        total_budget=2000,
        search_budget=1600,
        repair_budget=400,
    )
    return cfg, store, policy


def _alpha_db(path: Path, rows):
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE alpha_results(
            sim_key TEXT PRIMARY KEY, expr TEXT NOT NULL, settings_json TEXT NOT NULL,
            candidate_json TEXT, alpha_id TEXT, status TEXT, sharpe REAL, fitness REAL,
            turnover REAL, margin REAL, returns REAL, long_count INTEGER, short_count INTEGER,
            date_created TEXT, error TEXT, updated_at TEXT NOT NULL, simulation_url TEXT,
            submitted_at TEXT, retry_count INTEGER DEFAULT 0, last_http_status INTEGER,
            last_retry_after REAL, warning TEXT
        )"""
    )
    for row in rows:
        con.execute(
            """INSERT INTO alpha_results(
                   sim_key,expr,settings_json,alpha_id,status,sharpe,fitness,turnover,updated_at,
                   simulation_url,submitted_at,error,last_http_status,last_retry_after
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["sim_key"], row.get("expr", "rank(close)"), "{}", row.get("alpha_id"),
                row.get("status", "COMPLETE"), row.get("sharpe"), row.get("fitness"),
                row.get("turnover"), datetime.now(timezone.utc).isoformat(),
                row.get("simulation_url"), row.get("submitted_at"), row.get("error"),
                row.get("last_http_status"), row.get("last_retry_after"),
            ),
        )
    con.commit()
    con.close()


def _insert_candidate(store, cfg, cid, sim_key, *, state, sim_status, alpha_id=None, parent=None):
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,dataset_id,field_id,field_type,
                   semantic_class,direction,signal_family,transform_family,operator,window,vector_reducer,
                   lifecycle_state,simulation_status,alpha_id,parent_candidate_id,initial_selection_score,
                   structure_status,data_field_count_estimate,pp_total_operator_count_estimate,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                cid, "run_0006", "rank(close)", sim_key,
                json.dumps(cfg.plan["simulation_settings"], sort_keys=True),
                "ds1", "close", "MATRIX", "PRICE", "NORMAL", f"fam/{cid}", "IDENTITY",
                "rank", None, "IDENTITY", state, sim_status, alpha_id, parent, 10.0,
                "ELIGIBLE", 1, 1, now, now,
            ),
        )


def _insert_repair_lineage(store):
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_repair_plans(
                 repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                 repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "rp1", None, "run_0006", "parent", "parent", "TEST", "TEST_REPAIR", "sig1",
                "[]", 1, "{}", "[]", "DISPATCHED", 1, 1, 1,
                "REMOTE_EXECUTION_IN_PROGRESS", now, now,
            ),
        )
        con.execute(
            """INSERT INTO ppl_repairs(
                   repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,
                   repair_signature,repair_path_json,repair_depth,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?)""",
            ("edge1", "run_0006", "parent", "child", "TEST_REPAIR", "sig1", "[]", 1, now),
        )


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


def _dataset(ds="ds_missing"):
    return {
        "id": ds,
        "name": "temporary dataset",
        "description": "daily price features",
        "category": {"name": "Market"},
        "subcategory": {"name": "Price"},
        "userCount": 1,
        "alphaCount": 0,
    }


def test_continuous_recovered_search_uncertain_finalizes_locally_without_post(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro

    cfg, store, policy = _store(tmp_path)
    alpha = tmp_path / "alpha.db"
    _alpha_db(alpha, [{"sim_key": "sk_uncertain", "status": "UNCERTAIN_SUBMISSION"}])
    _insert_candidate(
        store, cfg, "cand_uncertain", "sk_uncertain",
        state="SIMULATION_PENDING", sim_status="UNCERTAIN_SUBMISSION",
    )
    start_batch(
        store, "round_run_0006", 1, "SEARCH", candidate_ids=["cand_uncertain"],
        projected_new_posts=1, planned_post_sim_keys=["sk_uncertain"], planned_resume_sim_keys=[],
    )
    with store.connect() as con:
        con.execute(
            """UPDATE ppl_round_batches SET status='RECOVERED',logical_posts_consumed=1
               WHERE round_id='round_run_0006' AND batch_no=1"""
        )

    monkeypatch.setattr(ro, "finalize_family_winners", lambda *a, **k: {"protected_total": 0})
    monkeypatch.setattr(ro, "_sync_research_telemetry", lambda *a, **k: {})
    monkeypatch.setattr(ro, "_reconcile_round_accounting", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: {})

    # object() has no HTTP/POST methods. If recovery tried to re-POST this
    # UNCERTAIN sim_key the test would fail before returning.
    out = _finalize_recovered_search_batch(
        store, cfg, object(), object(), alpha, "run_0006", "round_run_0006",
        _batch_by_no(store, "round_run_0006", 1), policy, tmp_path,
    )
    assert out["finalized"] is True
    assert out["completion_semantics"] == "REMOTE_HANDOFF_COMPLETE"
    assert out["remote_nonterminal_candidate_ids"] == ["cand_uncertain"]
    assert _batch_by_no(store, "round_run_0006", 1)["status"] == "COMPLETED"
    with store.connect() as con:
        remote = con.execute(
            "select queue_state,reserved_slot from ppl_remote_work where sim_key='sk_uncertain'"
        ).fetchone()
    assert tuple(remote) == ("QUARANTINED_UNCERTAIN", 1)


def test_continuous_recovered_repair_running_finalizes_as_remote_handoff(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro

    cfg, store, policy = _store(tmp_path)
    alpha = tmp_path / "alpha.db"
    _alpha_db(alpha, [{
        "sim_key": "sk_child", "status": "RUNNING",
        "simulation_url": "https://api.worldquantbrain.com/simulations/S1",
        "submitted_at": "2026-08-27T00:00:00+00:00",
    }])
    _insert_candidate(store, cfg, "parent", "sk_parent", state="PRE_CHECK_REPAIR", sim_status="COMPLETE")
    _insert_candidate(
        store, cfg, "child", "sk_child", state="SIMULATION_PENDING", sim_status="RUNNING", parent="parent",
    )
    _insert_repair_lineage(store)
    start_batch(
        store, "round_run_0006", 1, "REPAIR", plan_ids=["rp1"], projected_new_posts=1,
        planned_post_sim_keys=["sk_child"], planned_resume_sim_keys=[],
    )
    with store.connect() as con:
        con.execute(
            """UPDATE ppl_round_batches SET status='RECOVERED',logical_posts_consumed=1
               WHERE round_id='round_run_0006' AND batch_no=1"""
        )

    monkeypatch.setattr(ro, "finalize_family_winners", lambda *a, **k: {"protected_total": 0})
    monkeypatch.setattr(ro, "_maybe_auto_refresh_manual_finalization", lambda *a, **k: {"executed_check_count": 0})
    monkeypatch.setattr(ro, "_sync_research_telemetry", lambda *a, **k: {})
    monkeypatch.setattr(ro, "_reconcile_round_accounting", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: {})

    out = _finalize_recovered_repair_batch(
        store, cfg, object(), object(), alpha, "run_0006", "round_run_0006",
        _batch_by_no(store, "round_run_0006", 1), policy, tmp_path,
    )
    assert out["finalized"] is True
    assert out["completion_semantics"] == "REMOTE_HANDOFF_COMPLETE"
    assert out["remote_nonterminal_candidate_ids"] == ["child"]
    assert _batch_by_no(store, "round_run_0006", 1)["status"] == "COMPLETED"
    with store.connect() as con:
        remote = con.execute(
            "select queue_state,reserved_slot,simulation_url from ppl_remote_work where sim_key='sk_child'"
        ).fetchone()
    assert tuple(remote) == (
        "POLL_DUE", 1, "https://api.worldquantbrain.com/simulations/S1"
    )


def test_discovery_datafield_404_skips_only_dataset_and_finishes_refresh(tmp_path):
    cfg, store, _ = _store(tmp_path)
    work = _enqueue_continuous_rolling_discovery(
        store, cfg, "run_0006", "round_run_0006", _policy(),
        batch_no=0, trigger="LOW_SAFE_CANDIDATE_POOL",
    )
    assert work["created_new"] is True
    session = Session([
        Response(200, {"results": [_dataset()]}),
        Response(404, {}),
    ])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()

    first = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert first["polled"] == 1
    second = poll_due_discovery_work(store, cfg, machine, session, "run_0006")
    assert second["failed"] == []
    assert second["skipped_dataset_ids"] == ["ds_missing"]
    with store.connect() as con:
        row = con.execute(
            "select queue_state,stage,last_http_status,last_error from ppl_discovery_work where discovery_work_id=?",
            (work["discovery_work_id"],),
        ).fetchone()
    assert row[0] == "READY_APPLY" and row[1] == "FINALIZE" and row[2] == 404
    assert "DATASET_FIELDS_REMOTE_MISSING:ds_missing" in row[3]


def test_failed_discovery_refresh_cools_down_then_advances_refresh_number(tmp_path):
    cfg, store, policy = _store(tmp_path)
    first = _enqueue_continuous_rolling_discovery(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=0, trigger="LOW_SAFE_CANDIDATE_POOL",
    )
    session = Session([Response(400, {})])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()
    out = poll_due_discovery_work(
        store, cfg, machine, session, "run_0006",
        deterministic_failure_cooldown_seconds=60,
    )
    assert out["failed"] == [first["discovery_work_id"]]

    immediate = _enqueue_continuous_rolling_discovery(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=0, trigger="LOW_SAFE_CANDIDATE_POOL",
    )
    assert immediate["created_new"] is False
    assert immediate["reason"] == "DISCOVERY_FAILURE_COOLDOWN"
    assert immediate["refresh_no"] == 1

    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    with store.connect() as con:
        con.execute(
            """UPDATE ppl_endpoint_waits SET next_retry_at=?
               WHERE run_id='run_0006' AND endpoint_type='DISCOVERY'""",
            (past,),
        )
    second = _enqueue_continuous_rolling_discovery(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=0, trigger="LOW_SAFE_CANDIDATE_POOL",
    )
    assert second["created_new"] is True
    assert second["refresh_no"] == 2
    with store.connect() as con:
        states = [tuple(r) for r in con.execute(
            "select refresh_no,queue_state from ppl_discovery_work order by refresh_no"
        ).fetchall()]
    assert states == [(1, "FAILED"), (2, "DISCOVERY_DUE")]
