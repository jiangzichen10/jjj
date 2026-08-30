import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ppl_engine.check_transport import CheckBudget, CheckResponse, semantic_poll_check
from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.continuous_check import enqueue_pretag_checks, poll_due_checks
from ppl_engine.round_orchestrator import (
    _qualified_check_refresh_targets,
    _qualified_check_poll_state,
    _sync_qualified_refresh_resolution,
    load_round_policy,
    preflight_qualified_check_refresh,
)
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.store import RunnerStore
from ppl_engine.live_execution import _check_result_display_num

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


class _Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def fetch_check(self, alpha_id):
        self.calls += 1
        return self.responses.pop(0)


def test_semantic_poll_retries_http_200_empty_body_using_retry_after():
    payload = json.dumps({"is": {"checks": [{"name": "LOW_SHARPE", "result": "PASS", "limit": 1.0, "value": 2.0}]}})
    transport = _Transport([
        CheckResponse(200, "", retry_after_seconds=1.25),
        CheckResponse(200, payload),
    ])
    waits = []
    budget = CheckBudget(1, 4, 4, 1)
    observed = []
    out = semantic_poll_check(
        transport, alpha_id="A1", phase="PRE_TAG", rules=_config().rules,
        budget=budget, wait=waits.append, poll_observer=observed.append,
    )
    assert transport.calls == 2
    assert waits == [1.25]
    assert out["session_status"] == "RESOLVED"
    assert out["polls"][0]["parsed"]["parse_status"] == "HTTP_200_EMPTY_BODY_RETRY"
    assert out["polls"][0]["parsed"]["error_type"] == "HTTP_200_EMPTY_BODY_RETRY"
    assert [x["semantic_poll_index"] for x in observed] == [1, 2]
    assert out["error_type"] is None and out["error_nature"] is None
    assert out["transient_retry_seen"] is True
    assert out["last_transient_error"] == "HTTP_200_EMPTY_BODY_RETRY"


@pytest.mark.parametrize("value", [0.7715, 0.88, 0.9815, 0.6468])
def test_qualified_ppc_display_decodes_raw_json(value):
    assert _check_result_display_num(
        {"normalized_value": None, "raw_value_json": json.dumps(value)},
        normalized_key="normalized_value", raw_json_key="raw_value_json", raw_key="raw_value",
    ) == value


def test_qualified_ppc_display_prefers_normalized_value():
    assert _check_result_display_num(
        {"normalized_value": 0.625, "raw_value_json": "0.7715"},
        normalized_key="normalized_value", raw_json_key="raw_value_json", raw_key="raw_value",
    ) == 0.625
    assert _check_result_display_num(
        {"normalized_value": None, "raw_value": 0.88},
        normalized_key="normalized_value", raw_json_key="raw_value_json", raw_key="raw_value",
    ) == 0.88


def test_qualified_poll_progress_states_are_compact_and_payload_free():
    empty = _qualified_check_poll_state({
        "semantic_poll_index": 1, "http_status": 200, "retry_after_seconds": 1,
        "parsed": {"parse_status": "HTTP_200_EMPTY_BODY_RETRY", "raw_payload": {"secret": "not logged"}},
    })
    throttled = _qualified_check_poll_state({
        "semantic_poll_index": 2, "http_status": 429, "retry_after_seconds": 3,
        "parsed": {"session_semantic_status": "TRANSIENT_ERROR"},
    })
    resolved = _qualified_check_poll_state({
        "semantic_poll_index": 7, "http_status": 200,
        "parsed": {"session_semantic_status": "RESOLVED", "results": [{
            "normalized_name": "POWER_POOL_CORRELATION", "normalized_value": None, "raw_value": 0.7715,
        }]},
    })
    assert empty == "poll=1 | HTTP 200 EMPTY | retry_after=1.0s"
    assert throttled == "poll=2 | HTTP 429 | retry_after=3.0s | THROTTLED"
    assert resolved == "poll=7 | RESOLVED | PPC=0.7715"
    assert "secret" not in empty + throttled + resolved


def test_semantic_poll_429_progress_and_final_error_are_preserved():
    transport = _Transport([CheckResponse(429, "", retry_after_seconds=2.0)])
    observed = []
    out = semantic_poll_check(
        transport, alpha_id="A1", phase="PRE_TAG", rules=_config().rules,
        budget=CheckBudget(1, 1, 1, 1), poll_observer=observed.append,
        throttle_max_events=1,
    )
    assert observed[0]["http_status"] == 429
    assert observed[0]["retry_after_seconds"] == 2.0
    assert out["session_status"] == "BUDGET_EXHAUSTED"
    assert out["error_type"] == "HTTP_429_THROTTLE_DEFERRED"


class _Response:
    def __init__(self, status=200, text="", headers=None):
        self.status_code = status
        self.text = text
        self.headers = headers or {}


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, timeout=60):
        self.calls.append((url, timeout))
        return self.responses.pop(0)


def _setup_candidate(tmp_path, *, run_id="run_0006", candidate_id="cand1", alpha_id="A1", state="LOCAL_PRE_GATE_PASS"):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run(run_id, cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,alpha_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (candidate_id, run_id, "rank(close)", f"sk_{candidate_id}", json.dumps(cfg.plan["simulation_settings"]),
             "close", "MATRIX", "rank", 0, cfg.plan["simulation_settings"]["neutralization"],
             state, "COMPLETE", alpha_id, now, now),
        )
    return cfg, store


def test_continuous_queue_marks_200_empty_body_as_retry_not_json_error(tmp_path):
    cfg, store = _setup_candidate(tmp_path)
    enqueue_pretag_checks(store, "run_0006", ["cand1"])
    session = _Session([_Response(200, "", {"Retry-After": "1.5"})])
    machine = type("M", (), {"BRAIN_API_URL": "https://api.worldquantbrain.com"})()
    out = poll_due_checks(store, cfg, machine, session, "run_0006")
    assert out["polled"] == 1 and out["waits"] == 1
    with store.connect() as conn:
        work = conn.execute(
            "select queue_state,retry_after_seconds,last_http_status,last_error from ppl_check_work where candidate_id='cand1'"
        ).fetchone()
        check = conn.execute(
            "select session_status,error_type,error_nature from ppl_check_sessions where candidate_id='cand1' order by created_at desc limit 1"
        ).fetchone()
        poll = conn.execute(
            "select parse_status,error_type,raw_response_text from ppl_check_polls where candidate_id='cand1' order by poll_id desc limit 1"
        ).fetchone()
    assert tuple(work) == ("WAIT_CHECK", 1.5, 200, "HTTP_200_EMPTY_BODY_RETRY")
    assert tuple(check) == ("PENDING", "HTTP_200_EMPTY_BODY_RETRY", "TRANSIENT")
    assert poll[0] == poll[1] == "HTTP_200_EMPTY_BODY_RETRY" and poll[2] == ""


def test_qualified_refresh_preflight_requires_paused_round_and_run(tmp_path):
    cfg = _config()
    policy = load_round_policy(ROOT / "ppl_round_v31_d2e.yaml", cfg)
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); ensure_round_schema(store); store.create_run("run_0006", cfg)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    with pytest.raises(ConfigError, match="REQUIRES_PAUSED_ROUND"):
        preflight_qualified_check_refresh(
            store, cfg, ROOT / "machine_lib_V2_1.py", ROOT / "ppl_round_v31_d2e.yaml", run_id="run_0006",
        )
    with store.connect() as conn:
        conn.execute("update ppl_rounds set status='PAUSED' where round_id='round_run_0006'")
        conn.execute("update ppl_runs set status='PAUSED' where run_id='run_0006'")
    out = preflight_qualified_check_refresh(
        store, cfg, ROOT / "machine_lib_V2_1.py", ROOT / "ppl_round_v31_d2e.yaml", run_id="run_0006",
    )
    assert out["qualified_refresh_preflight"] is True
    assert out["round"]["status"] == "PAUSED"


def test_qualified_target_selection_excludes_repair_state_and_ambiguous_alpha(tmp_path, monkeypatch):
    cfg, store = _setup_candidate(tmp_path, candidate_id="c1", alpha_id="A1", state="PRE_TAG_CHECK_PENDING")
    now = datetime.now(timezone.utc).isoformat()
    settings = json.dumps(cfg.plan["simulation_settings"])
    with store.connect() as conn:
        for cid, alpha, state in [
            ("c2", "A2", "PRE_CHECK_REPAIR"),
            ("c3", "AX", "PRE_TAG_CHECK_PENDING"),
            ("c4", "AX", "PRE_TAG_CHECK_COMPLETE"),
        ]:
            conn.execute(
                """INSERT INTO ppl_candidates(
                       candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                       neutralization,lifecycle_state,simulation_status,alpha_id,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (cid, "run_0006", f"rank({cid})", f"sk_{cid}", settings, cid, "MATRIX", "rank", 0,
                 cfg.plan["simulation_settings"]["neutralization"], state, "COMPLETE", alpha, now, now),
            )
    import ppl_engine.round_orchestrator as ro
    rows = [
        {"candidate_id": "c1", "alpha_id": "A1", "classification": "PPL_CHECK_UNRESOLVED", "sharpe": 3.2, "fitness": 2.0, "turnover": 0.5},
        {"candidate_id": "c2", "alpha_id": "A2", "classification": "PPL_CHECK_UNRESOLVED", "sharpe": 4.0, "fitness": 2.0, "turnover": 0.8},
        {"candidate_id": "c3", "alpha_id": "AX", "classification": "PPL_CHECK_UNRESOLVED", "sharpe": 3.1, "fitness": 1.9, "turnover": 0.4},
        {"candidate_id": "c4", "alpha_id": "AX", "classification": "PPL_CHECK_UNRESOLVED", "sharpe": 3.0, "fitness": 1.8, "turnover": 0.4},
    ]
    monkeypatch.setattr(ro, "classify_run", lambda *a, **k: rows)
    monkeypatch.setattr(ro, "_protected_manual_queue_sets", lambda *a, **k: (set(), set(), set()))
    out = _qualified_check_refresh_targets(store, cfg, tmp_path / "alpha.db", "run_0006", "round_run_0006")
    assert [x["candidate_id"] for x in out["targets"]] == ["c1"]
    reasons = {(x.get("candidate_id"), x.get("reason")) for x in out["skipped"]}
    assert ("c2", "LIFECYCLE_NOT_QUALIFIED:PRE_CHECK_REPAIR") in reasons
    assert ("c3", "SAME_ALPHA_ID_MULTIPLE_CANDIDATES_NO_LOCAL_REUSE") in reasons
    assert ("c4", "SAME_ALPHA_ID_MULTIPLE_CANDIDATES_NO_LOCAL_REUSE") in reasons


def test_resolved_qualified_refresh_closes_pretag_work_and_advances_pending_candidate(tmp_path):
    cfg, store = _setup_candidate(tmp_path)
    enqueue_pretag_checks(store, "run_0006", ["cand1"])
    _sync_qualified_refresh_resolution(
        store, "run_0006", "cand1", "A1",
        {"session_status": "RESOLVED", "base_gate": "PASS", "theme_gate": "PASS"},
    )
    assert store.load_candidates("run_0006")[0]["lifecycle_state"] == "PRE_TAG_CHECK_PASS"
    with store.connect() as conn:
        row = conn.execute("select queue_state,next_check_at,last_error from ppl_check_work where candidate_id='cand1'").fetchone()
    assert tuple(row) == ("RESOLVED", None, None)


def test_cli_qualified_refresh_preflight_is_before_authentication_and_uses_continuous_policy():
    import ppl_runner
    source = inspect.getsource(ppl_runner.main)
    branch = source[source.index("if args.refresh_qualified_checks:"):
                    source.index("if args.recover_interrupted_batch:")]
    assert branch.index("preflight_qualified_check_refresh(") < branch.index("_login_with_authentication_meter(")
    assert "args.continuous_policy" in branch
    parser_source = inspect.getsource(ppl_runner._parser)
    assert "--refresh-qualified-checks" in parser_source
    assert "--qualified-check-limit" in parser_source
