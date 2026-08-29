import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import yaml
import machine_lib_V2_1 as machine

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_check import enqueue_pretag_checks, poll_due_checks
from ppl_engine.continuous_control import recover_waiting_auth
from ppl_engine.continuous_discovery import poll_due_discovery_work
from ppl_engine.continuous_remote import (
    poll_due_remote_work,
    remote_slot_snapshot,
    sync_remote_work_from_durable_facts,
)
from ppl_engine.round_orchestrator import (
    _enqueue_continuous_rolling_discovery,
    _write_reports_resilient,
)
from ppl_engine.round_store import create_round
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _policy():
    return yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))


def _candidate_identity(cfg, expr="rank(close)"):
    candidate = {"expr": expr, "decay": 0}
    sim = cfg.plan["simulation_settings"]
    settings = machine.build_settings(
        candidate,
        neutralization=sim["neutralization"], region=sim["region"], universe=sim["universe"],
        delay=sim["delay"], truncation=sim["truncation"], test_period=sim["test_period"],
    )
    return machine.simulation_key(expr, settings), settings


class Response:
    def __init__(self, status=200, payload=None, *, text=None, headers=None):
        self.status_code = int(status)
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(self._payload)

    def json(self):
        return self._payload


class RouterSession:
    def __init__(self):
        self.routes = defaultdict(deque)
        self.get_calls = []
        self.post_calls = []

    def add(self, needle, *responses):
        self.routes[str(needle)].extend(responses)

    def get(self, url, timeout=60, **kwargs):
        self.get_calls.append(str(url))
        for needle, queue in self.routes.items():
            if needle in str(url) and queue:
                item = queue.popleft()
                if isinstance(item, BaseException):
                    raise item
                return item
        raise AssertionError(f"unexpected GET: {url}")

    def post(self, url, **kwargs):
        self.post_calls.append(str(url))
        raise AssertionError("unattended control-plane soak must never POST")

    def request(self, method, url, **kwargs):
        method = str(method).upper()
        if method == "GET":
            return self.get(url, **kwargs)
        if method == "POST":
            return self.post(url, **kwargs)
        raise AssertionError(f"unexpected method {method}")


def _force_due(store):
    past = "2000-01-01T00:00:00+00:00"
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_remote_work SET next_poll_at=?
               WHERE queue_state IN ('POLL_DUE','WAIT_REMOTE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH','MISSING_CONFIRMATION_PENDING')""",
            (past,),
        )
        conn.execute(
            """UPDATE ppl_check_work SET next_check_at=?
               WHERE queue_state IN ('CHECK_DUE','WAIT_CHECK','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH')""",
            (past,),
        )
        conn.execute(
            """UPDATE ppl_discovery_work SET next_attempt_at=?
               WHERE queue_state IN ('DISCOVERY_DUE','WAIT_RATE_LIMIT','WAIT_NETWORK','WAIT_AUTH')""",
            (past,),
        )
        conn.execute(
            """UPDATE ppl_endpoint_waits SET next_retry_at=?
               WHERE wait_state!='READY' AND next_retry_at IS NOT NULL""",
            (past,),
        )


def _insert_candidate(store, cfg, cid, sim_key, *, state, sim_status, settings, alpha_id=None):
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,alpha_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                cid, "run_0006", "rank(close)", sim_key, json.dumps(settings, sort_keys=True),
                "close", "MATRIX", "rank", 0, settings["neutralization"],
                state, sim_status, alpha_id, now, now,
            ),
        )


def test_mixed_unattended_control_plane_survives_restart_without_repost(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro

    cfg = _config()
    policy = _policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )

    remote_key, remote_settings = _candidate_identity(cfg, "rank(close)")
    uncertain_key, uncertain_settings = _candidate_identity(cfg, "rank(open)")
    check_key, check_settings = _candidate_identity(cfg, "rank(volume)")
    _insert_candidate(
        store, cfg, "remote", remote_key, state="SIMULATION_RUNNING", sim_status="RUNNING",
        settings=remote_settings,
    )
    _insert_candidate(
        store, cfg, "uncertain", uncertain_key, state="SIMULATION_PENDING", sim_status="UNCERTAIN_SUBMISSION",
        settings=uncertain_settings,
    )
    _insert_candidate(
        store, cfg, "check", check_key, state="LOCAL_PRE_GATE_PASS", sim_status="COMPLETE",
        settings=check_settings, alpha_id="A_CHECK",
    )

    alpha_db = tmp_path / "alpha.db"
    machine.init_cache(str(alpha_db))
    now = datetime.now(timezone.utc).isoformat()
    machine.cache_put(
        str(alpha_db), remote_key, {"expr": "rank(close)", "decay": 0}, remote_settings,
        {
            "status": "RUNNING",
            "simulation_url": "https://api.worldquantbrain.com/simulations/REMOTE1",
            "submitted_at": now,
        },
    )
    machine.cache_put(
        str(alpha_db), uncertain_key, {"expr": "rank(open)", "decay": 0}, uncertain_settings,
        {"status": "UNCERTAIN_SUBMISSION", "error": "ambiguous submit; no re-POST"},
    )

    sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=True)
    enqueue_pretag_checks(store, "run_0006", ["check"])
    discovery = _enqueue_continuous_rolling_discovery(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=0, trigger="LOW_SAFE_CANDIDATE_POOL",
    )
    assert discovery["created_new"] is True

    # Initial server accounting: one real RUNNING + one UNCERTAIN reservation.
    slots = remote_slot_snapshot(store, "run_0006", 4)
    assert (slots.reserved_slots, slots.uncertain, slots.free_slots) == (2, 1, 2)

    session = RouterSession()
    session.add(
        "/simulations/REMOTE1",
        Response(500),
        Response(429, headers={"Retry-After": "1"}),
        Response(401),
        Response(200, {"status": "RUNNING", "progress": 0.5}, headers={"Retry-After": "1"}),
        Response(200, {"status": "COMPLETE", "alpha": "A_REMOTE"}),
    )
    session.add(
        "/alphas/A_REMOTE",
        Response(200, {
            "is": {"sharpe": 1.8, "fitness": 1.0, "turnover": 0.4, "margin": 0.001},
            "regular": {"code": "rank(close)"}, "settings": {"decay": 0},
        }),
    )
    check_text = json.dumps(json.loads((ROOT / "tests/fixtures/check_all_pass.json").read_text(encoding="utf-8")))
    session.add(
        "/alphas/A_CHECK/check",
        Response(429, headers={"Retry-After": "1"}),
        Response(500),
        Response(401),
        Response(200, text=check_text),
    )
    session.add(
        "/data-sets?",
        Response(429, headers={"Retry-After": "1"}),
        Response(200, {"results": [{
            "id": "ds_disappears", "name": "temporary", "description": "price features",
            "category": {"name": "Market"}, "subcategory": {"name": "Price"},
            "userCount": 1, "alphaCount": 0,
        }]}),
    )
    session.add("dataset.id=ds_disappears", Response(404))

    # Derived report failure is degradation, not a research halt.
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: (_ for _ in ()).throw(OSError("disk busy")))
    report = _write_reports_resilient(
        store, cfg, alpha_db, "run_0006", "round_run_0006", policy, tmp_path,
        continuous_enabled=True, retry_seconds=1,
    )
    assert report["degraded"] is True

    auth_calls = {"n": 0}

    class AuthMachine:
        @staticmethod
        def ensure_session(s):
            auth_calls["n"] += 1
            return s

    # Five deterministic scheduler-style cycles.  Every recoverable response is
    # turned into durable WAIT state, then made due again without real sleeping.
    for cycle in range(5):
        _force_due(store)
        # Release any WAIT_AUTH from the previous cycle before polling again.
        recover_waiting_auth(store, AuthMachine, session, "run_0006", retry_seconds=1)
        poll_due_remote_work(
            store, cfg, machine, session, alpha_db, "run_0006", limit=4,
            poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
        )
        poll_due_checks(
            store, cfg, machine, session, "run_0006", limit=4,
            poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
        )
        poll_due_discovery_work(
            store, cfg, machine, session, "run_0006", limit=1,
            poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
            deterministic_failure_cooldown_seconds=2,
        )

        # Simulate a process restart in the middle of mixed endpoint backoff.
        if cycle == 1:
            store = RunnerStore(tmp_path / "runner.db")
            store.initialize()
            sync_remote_work_from_durable_facts(store, alpha_db, "run_0006", force_due_existing=False)
            with store.connect() as conn:
                url = conn.execute(
                    "select simulation_url from ppl_remote_work where sim_key=?", (remote_key,)
                ).fetchone()[0]
            assert url == "https://api.worldquantbrain.com/simulations/REMOTE1"

    # A final auth release handles a 401 emitted in the last mixed-error cycle,
    # then due work can converge without any POST.
    _force_due(store)
    recover_waiting_auth(store, AuthMachine, session, "run_0006", retry_seconds=1)
    poll_due_remote_work(
        store, cfg, machine, session, alpha_db, "run_0006", limit=4,
        poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
    )
    poll_due_checks(
        store, cfg, machine, session, "run_0006", limit=4,
        poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
    )
    poll_due_discovery_work(
        store, cfg, machine, session, "run_0006", limit=1,
        poll_interval_seconds=1, network_backoff_seconds=1, max_network_backoff_seconds=2,
        deterministic_failure_cooldown_seconds=2,
    )

    # Report retry succeeds later and clears its endpoint WAIT independently.
    monkeypatch.setattr(ro, "_write_reports", lambda *a, **k: {"ok": "report"})
    assert _write_reports_resilient(
        store, cfg, alpha_db, "run_0006", "round_run_0006", policy, tmp_path,
        continuous_enabled=True, retry_seconds=1,
    ) == {"ok": "report"}

    with store.connect() as conn:
        remote = conn.execute(
            "select queue_state,reserved_slot,simulation_url from ppl_remote_work where sim_key=?",
            (remote_key,),
        ).fetchone()
        uncertain = conn.execute(
            "select queue_state,reserved_slot from ppl_remote_work where sim_key=?",
            (uncertain_key,),
        ).fetchone()
        check = conn.execute(
            "select queue_state from ppl_check_work where candidate_id='check'"
        ).fetchone()[0]
        discovery_state = conn.execute(
            "select queue_state,last_error from ppl_discovery_work where discovery_work_id=?",
            (discovery["discovery_work_id"],),
        ).fetchone()
        report_state = conn.execute(
            "select wait_state from ppl_endpoint_waits where endpoint_type='REPORT'"
        ).fetchone()[0]

    assert remote[0] == "COMPLETE" and remote[1] == 0
    assert remote[2] == "https://api.worldquantbrain.com/simulations/REMOTE1"
    assert tuple(uncertain) == ("QUARANTINED_UNCERTAIN", 1)
    assert check == "RESOLVED"
    assert discovery_state[0] == "READY_APPLY"
    assert "DATASET_FIELDS_REMOTE_MISSING:ds_disappears" in discovery_state[1]
    assert report_state == "READY"
    assert session.post_calls == []
    assert remote_slot_snapshot(store, "run_0006", 4).reserved_slots == 1  # UNCERTAIN only
    assert auth_calls["n"] >= 1
