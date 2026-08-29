"""Tests for V2.2 Production Logging / Execution Audit Trail (audit_log)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

import ppl_engine.audit_log as al

PROJECT_DIR = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_audit_log():
    al._reset_handlers(al.get_audit_logger())
    al._CONFIGURED = False
    al._CONFIGURED_PATH = None
    al._WARNED = False
    yield
    al._reset_handlers(al.get_audit_logger())
    al._CONFIGURED = False
    al._CONFIGURED_PATH = None
    al._WARNED = False


def _configure(tmp_path: Path) -> Path:
    # Explicitly enable (the test-suite conftest disables it globally via env).
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))
    return tmp_path / "logs" / "ppl_v2_2.log"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_lines(path: Path):
    return [json.loads(l) for l in _read_text(path).splitlines() if l.strip()]


# ---------------------------------------------------------------------------
# logger lifecycle / config
# ---------------------------------------------------------------------------

def test_configure_creates_logs_dir(tmp_path):
    assert not (tmp_path / "logs").exists()
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))
    assert (tmp_path / "logs").is_dir()


def test_repeated_get_logger_no_duplicate_handler(tmp_path):
    al.get_audit_logger()
    al.get_audit_logger()
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))  # idempotent
    rot = [h for h in al.get_audit_logger().handlers if isinstance(h, RotatingFileHandler)]
    assert len(rot) == 1


def test_rotating_handler_config(tmp_path):
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(max_bytes=1234, backup_count=7))
    rot = next(h for h in al.get_audit_logger().handlers if isinstance(h, RotatingFileHandler))
    assert rot.maxBytes == 1234
    assert rot.backupCount == 7
    assert rot.encoding == "utf-8"


def test_jsonl_valid_and_has_core_fields(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="SOME_ACTION", run_id="run_x")
    lines = _read_lines(log_path)
    assert len(lines) == 1
    rec = lines[0]
    assert "timestamp" in rec and "level" in rec and "action" in rec
    assert rec["action"] == "SOME_ACTION"
    assert rec["level"] == "INFO"
    assert rec["run_id"] == "run_x"


# ---------------------------------------------------------------------------
# redaction / sanitization
# ---------------------------------------------------------------------------

def test_authorization_redacted(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_http(action="HTTP_ERROR", method="POST", headers={"Authorization": "Basic abc123"})
    assert "abc123" not in _read_text(log_path)
    rec = _read_lines(log_path)[0]
    assert rec["headers"]["Authorization"] == "[REDACTED]"


def test_cookie_redacted(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_http(action="HTTP_ERROR", headers={"Cookie": "session=supersecret"})
    assert "supersecret" not in _read_text(log_path)


@pytest.mark.parametrize("key,value", [
    ("token", "t1"), ("password", "p1"), ("passwd", "p1"), ("secret", "s1"),
    ("api_key", "k1"), ("apikey", "k1"), ("session_token", "st1"),
])
def test_sensitive_keys_redacted(key, value):
    assert al._is_sensitive_key(key) is True
    out = al.sanitize({key: value})
    assert out[key] == "[REDACTED]"


def test_safe_workflow_keys_not_redacted():
    assert al._is_sensitive_key("session_status") is False
    assert al._is_sensitive_key("check_session_id") is False


def test_url_query_and_fragment_removed():
    assert al.sanitize_url("https://api.worldquantbrain.com/simulations/xyz?token=abc#frag") == \
        "https://api.worldquantbrain.com/simulations/xyz"
    assert al.sanitize_url("https://api.worldquantbrain.com/alphas/abc") == \
        "https://api.worldquantbrain.com/alphas/abc"


def test_bearer_secret_never_in_log(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_http(action="HTTP_ERROR", method="GET", url="https://api.worldquantbrain.com/x",
                  headers={"Authorization": "Bearer test-secret"})
    assert "test-secret" not in _read_text(log_path)


def test_error_does_not_expose_sensitive_header(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_error(error_type="AUTH_ERROR", error_message="failed", headers={"Authorization": "Bearer s3cr3t"})
    assert "s3cr3t" not in _read_text(log_path)


def test_error_body_length_limited():
    long_msg = "x" * 5000
    out = al.truncate_text(long_msg, 1000)
    assert len(out) < 1100
    assert "[truncated:" in out


# ---------------------------------------------------------------------------
# event structure
# ---------------------------------------------------------------------------

def test_simulation_post_attempt_structure(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="SIMULATION_POST_ATTEMPT", run_id="r", candidate_id="cand_x",
                   parent_candidate_id="cand_p", repair_plan_id="rplan_x", sim_key="sk",
                   cache_classification="NEW_SIMULATION_REQUIRED", budget_before=48)
    rec = _read_lines(log_path)[0]
    assert rec["action"] == "SIMULATION_POST_ATTEMPT"
    assert rec["candidate_id"] == "cand_x"
    assert rec["sim_key"] == "sk"
    assert rec["budget_before"] == 48


def test_simulation_post_success_has_url(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="SIMULATION_POST_SUCCESS", run_id="r", sim_key="sk",
                   http_status=201, simulation_url="https://api.worldquantbrain.com/simulations/abc",
                   budget_before=48, budget_after=47)
    rec = _read_lines(log_path)[0]
    assert rec["http_status"] == 201
    assert rec["simulation_url"].endswith("/simulations/abc")
    assert rec["budget_before"] == 48 and rec["budget_after"] == 47


def test_state_transition_records_old_new(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_state_transition("CANDIDATE", "cand_x", run_id="r", old_state="PLANNED",
                              new_state="SIMULATION_PENDING", reason="t", source="S")
    rec = _read_lines(log_path)[0]
    assert rec["action"] == "STATE_TRANSITION"
    assert rec["entity_type"] == "CANDIDATE"
    assert rec["old_state"] == "PLANNED" and rec["new_state"] == "SIMULATION_PENDING"


def test_pretag_check_complete_summary(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="PRETAG_CHECK_COMPLETE", run_id="r", candidate_id="cand_x",
                   alpha_id="A1", session_status="RESOLVED", poll_count=3, http_request_count=4,
                   base_gate="PENDING", theme_gate="WARNING",
                   ht_ratio={"value": 0.7374, "limit": 0.75, "outcome": "WARNING"},
                   pp_corr={"value": 0.19, "limit": 0.5, "outcome": "PASS"})
    rec = _read_lines(log_path)[0]
    assert rec["session_status"] == "RESOLVED"
    assert rec["ht_ratio"]["value"] == 0.7374
    assert rec["pp_corr"]["outcome"] == "PASS"


def test_repair_outcome_records_delta_verdict(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="REPAIR_OUTCOME", run_id="r", repair_plan_id="rplan_x",
                   parent_ht_ratio=0.7374, child_ht_ratio=0.7112, live_limit=0.75,
                   delta=-0.0262, repair_verdict="WORSE", repair_strategy="SAME_FAMILY_MICRO_TUNE")
    rec = _read_lines(log_path)[0]
    assert rec["repair_verdict"] == "WORSE"
    assert rec["delta"] == -0.0262
    assert rec["parent_ht_ratio"] == 0.7374


def test_near_pass_classified_normalized_gap(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="NEAR_PASS_CLASSIFIED", run_id="r", candidate_id="cand_x",
                   near_pass_class="STRONG_NEAR_PASS", normalized_gap=0.0168)
    rec = _read_lines(log_path)[0]
    assert rec["near_pass_class"] == "STRONG_NEAR_PASS"
    assert rec["normalized_gap"] == 0.0168


@pytest.mark.parametrize("prio", ["P1_MANUAL", "P2_MANUAL", "P3_ARCHIVE"])
def test_manual_review_escalated_priority(tmp_path, prio):
    log_path = _configure(tmp_path)
    al.audit_event(action="MANUAL_REVIEW_ESCALATED", run_id="r", candidate_id="cand_x",
                   manual_priority=prio, normalized_gap=0.02)
    rec = _read_lines(log_path)[0]
    assert rec["manual_priority"] == prio


# ---------------------------------------------------------------------------
# best-effort write failure
# ---------------------------------------------------------------------------

def test_write_failure_does_not_crash(tmp_path, monkeypatch):
    _configure(tmp_path)
    logger = al.get_audit_logger()

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(logger, "log", boom)
    # must not raise
    al.audit_event(action="X", run_id="r")
    al.audit_state_transition("CANDIDATE", "c", old_state="A", new_state="B")


# ---------------------------------------------------------------------------
# read / filter (CLI backing)
# ---------------------------------------------------------------------------

def _seed(tmp_path):
    log_path = _configure(tmp_path)
    al.audit_event(action="REPAIR_OUTCOME", run_id="run_0002", candidate_id="cand_a", alpha_id="AAA",
                   repair_plan_id="rplan_1")
    al.audit_event(action="REPAIR_OUTCOME", run_id="run_0002", candidate_id="cand_b", alpha_id="BBB",
                   repair_plan_id="rplan_2")
    al.audit_event(action="SIMULATION_POST_SUCCESS", run_id="run_0003", candidate_id="cand_c", alpha_id="CCC",
                   repair_plan_id="rplan_3")
    al.audit_event(action="ERROR", level="ERROR", run_id="run_0002", candidate_id="cand_a")
    return log_path


def test_read_filter_by_run_id(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, run_id="run_0002", limit=100))
    assert all(r["run_id"] == "run_0002" for r in recs)
    assert len(recs) == 3


def test_read_filter_by_action(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, action="REPAIR_OUTCOME", limit=100))
    assert len(recs) == 2 and all(r["action"] == "REPAIR_OUTCOME" for r in recs)


def test_read_filter_by_candidate_id(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, candidate_id="cand_a", limit=100))
    assert len(recs) == 2 and all(r["candidate_id"] == "cand_a" for r in recs)


def test_read_filter_by_alpha_id(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, alpha_id="BBB", limit=100))
    assert len(recs) == 1 and recs[0]["alpha_id"] == "BBB"


def test_read_filter_by_repair_plan_id(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, repair_plan_id="rplan_1", limit=100))
    assert len(recs) == 1 and recs[0]["repair_plan_id"] == "rplan_1"


def test_read_filter_by_level(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, level="ERROR", limit=100))
    assert len(recs) == 1 and recs[0]["level"] == "ERROR"


def test_read_limit(tmp_path):
    log_path = _seed(tmp_path)
    recs = list(al.read_audit_log(log_path, limit=2))
    assert len(recs) == 2


def test_read_empty_when_missing(tmp_path):
    recs = list(al.read_audit_log(tmp_path / "nonexistent" / "x.log", limit=10))
    assert recs == []


# ---------------------------------------------------------------------------
# store integration: unified STATE_TRANSITION hook
# ---------------------------------------------------------------------------

def test_store_transition_emits_state_transition(tmp_path):
    from ppl_engine.state_machine import CANDIDATE_TRANSITIONS
    from ppl_engine.store import RunnerStore

    db = tmp_path / "r.db"
    s = RunnerStore(db)
    s.initialize()
    now = "2026-01-01T00:00:00+00:00"
    with s.connect() as c:
        c.execute(
            """INSERT INTO ppl_runs(run_id,runner_goal,target_mode,atom_constraint_active,run_profile,
               current_stage,status,execution_hash,operational_hash,presentation_hash,rules_json,
               plan_json,budget_json,created_at,updated_at)
               VALUES ('run_0002','PPL','ATOM',0,'PRODUCTION_RESEARCH','INIT','CREATED','h1','h2','h3',
               '{}','{}','{}',?,?)""",
            (now, now),
        )
        c.execute(
            "INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,created_at,updated_at) "
            "VALUES ('cand_x','run_0002','expr','sk','PLANNED',?,?)",
            (now, now),
        )

    log_path = _configure(tmp_path)
    s.transition_candidate("cand_x", "SIMULATION_PENDING", reason="test", source="TEST",
                           allowed=CANDIDATE_TRANSITIONS)
    recs = _read_lines(log_path)
    transitions = [r for r in recs if r["action"] == "STATE_TRANSITION"]
    assert transitions, "expected a STATE_TRANSITION audit record"
    t = transitions[0]
    assert t["entity_type"] == "CANDIDATE"
    assert t["old_state"] == "PLANNED"
    assert t["new_state"] == "SIMULATION_PENDING"


# ---------------------------------------------------------------------------
# CLI --show-audit-log is read-only
# ---------------------------------------------------------------------------

def test_show_audit_log_cli_read_only(tmp_path):
    log_dir = tmp_path / "logdir"
    # Pre-write a record to the exact path the CLI will read (AUDIT_LOG_DIR is
    # an absolute directory override, so the file is <log_dir>/ppl_v2_2.log).
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True, directory=str(log_dir)))
    al.audit_event(action="REPAIR_OUTCOME", run_id="run_0002", candidate_id="cand_x",
                   alpha_id="AAA", repair_plan_id="rplan_1")

    db_path = tmp_path / "no_such_runner.db"  # never created: CLI does not touch it
    env = dict(os.environ)
    env["AUDIT_LOG_DIR"] = str(log_dir)
    env["AUDIT_LOG_FILENAME"] = "ppl_v2_2.log"
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "ppl_runner.py"), "--show-audit-log",
         "--run-id", "run_0002", "--audit-limit", "10",
         "--db", str(db_path)],
        cwd=str(PROJECT_DIR), env=env, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    report = json.loads(proc.stdout)
    assert report["mode"] == "SHOW_AUDIT_LOG"
    assert not db_path.exists()
    actions = {r["action"] for r in report["records"]}
    assert "REPAIR_OUTCOME" in actions
    # read-only: no simulation POST action was emitted by this command
    assert "SIMULATION_POST_ATTEMPT" not in actions
