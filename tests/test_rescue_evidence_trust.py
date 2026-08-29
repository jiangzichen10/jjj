"""Tests for external-vs-system rescue evidence trust boundaries.

External research evidence (EXTERNAL_CONFIRMED_EVIDENCE / MANUAL_OBSERVATION /
USER_CONFIRMED_EVIDENCE) is a priority hint only and must NEVER drive a
Production auto state-machine resolution (STOP_RESCUE_SUCCESS, lifecycle PASS,
budget change, Finalist fact). Only SYSTEM-confirmed facts may resolve a rescue.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import ppl_engine.audit_log as al
from ppl_engine.config import load_effective_config
from ppl_engine.near_pass import (
    evaluate_rescue_stop,
    evidence_source_is_external,
    load_external_evidence,
    preview_rescue,
)
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-01-01T00:00:00Z"


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


def _cfg():
    c = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)
    # This file validates the legacy/generic HT rescue engine trust boundary.
    # Re-activate the HT theme signal inside the test fixture; production
    # V3.0.4b keeps it inactive for the simplified GLB Liquid theme.
    req = c.rules["current_theme"]["live_theme_checks"]["required"]
    for name in ("HIGH_TURNOVER", "HIGH_TURNOVER_RETURNS_RATIO", "CLASSIFICATION_HIGH_TURNOVER"):
        if name not in req:
            req.append(name)
    return c


def _configure_audit(tmp_path):
    al.configure_audit_log(tmp_path, config=al.AuditLogConfig(enabled=True))
    return tmp_path / "logs" / "ppl_v2_2.log"


# ---------------------------------------------------------------------------
# evidence source trust boundary (pure)
# ---------------------------------------------------------------------------

def test_external_sources_are_external():
    assert evidence_source_is_external("EXTERNAL_CONFIRMED_EVIDENCE")
    assert evidence_source_is_external("MANUAL_OBSERVATION")
    assert evidence_source_is_external("USER_CONFIRMED_EVIDENCE")


def test_system_sources_are_not_external():
    assert not evidence_source_is_external("LIVE_CHECK_CONFIRMED")
    assert not evidence_source_is_external("DB_CONFIRMED")
    assert not evidence_source_is_external("SYSTEM_CONFIRMED")
    assert not evidence_source_is_external(None)
    assert not evidence_source_is_external("")


def test_external_target_pass_does_not_stop():
    assert evaluate_rescue_stop([("S", "TARGET_PASS", "EXTERNAL_CONFIRMED_EVIDENCE")], 1, 5) is None


def test_manual_observation_target_pass_does_not_stop():
    assert evaluate_rescue_stop([("S", "TARGET_PASS", "MANUAL_OBSERVATION")], 1, 5) is None


def test_user_confirmed_target_pass_does_not_stop():
    assert evaluate_rescue_stop([("S", "TARGET_PASS", "USER_CONFIRMED_EVIDENCE")], 1, 5) is None


def test_system_target_pass_stops():
    assert evaluate_rescue_stop([("S", "TARGET_PASS")], 1, 5) == "STOP_RESCUE_SUCCESS"
    assert evaluate_rescue_stop([("S", "TARGET_PASS", "LIVE_CHECK_CONFIRMED")], 1, 5) == "STOP_RESCUE_SUCCESS"
    assert evaluate_rescue_stop([{"strategy": "S", "verdict": "TARGET_PASS", "source": "DB_CONFIRMED"}], 1, 5) == "STOP_RESCUE_SUCCESS"


def test_external_target_pass_does_not_block_attempt_exhaustion():
    # Even with external TARGET_PASS, attempt exhaustion still stops on system facts.
    assert evaluate_rescue_stop([("S", "TARGET_PASS", "EXTERNAL_CONFIRMED_EVIDENCE")], 5, 5) == "STOP_AUTOMATIC_RESCUE"


def test_external_worse_does_not_stop():
    # External WORSE must not influence the consecutive-WORSE system stop rule either.
    assert evaluate_rescue_stop([("S", "WORSE", "EXTERNAL_CONFIRMED_EVIDENCE")], 1, 5) is None


# ---------------------------------------------------------------------------
# store fixture: a STRONG_NEAR_PASS candidate with HT-ratio WARNING
# ---------------------------------------------------------------------------

def _make_rescue_store(tmp_path):
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
            ("cand_parent", "run_0002", "ts_mean(predicted_first_quantile_one_day_return_2, 2)", "parent_key", "{}", "sh", "ctx",
             "techindi_model", "predicted_first_quantile_one_day_return_2", "MATRIX", "RETURN", "NORMAL",
             "techindi_model/predicted_first_quantile_one_day_return_2/IDENTITY/NORMAL/TS_MEAN", "TS_MEAN", "ts_mean", 2, 0,
             "IDENTITY", "cand_parent", None, None, 0, "SIMULATION_COMPLETE", "COMPLETE", 1, "CACHE_RESTORE",
             "CACHE_COMPLETE", "disc", "dry", "ELIGIBLE", 1, 1, NOW, NOW),
        )
    # alpha_results fact: COMPLETE with STRONG_NEAR_PASS metrics
    import sqlite3
    adb = tmp_path / "a.db"
    ac = sqlite3.connect(adb)
    ac.execute("CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, alpha_id TEXT, sharpe REAL, fitness REAL, turnover REAL, returns REAL, margin REAL, long_count INTEGER, short_count INTEGER)")
    ac.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    ac.execute("INSERT INTO alpha_results (sim_key,status,simulation_url,alpha_id,sharpe,fitness,turnover,returns,margin,long_count,short_count) VALUES ('parent_key','COMPLETE','u','ZYEVroY0',2.42,1.18,0.7137,0.15,0.0004,10,10)")
    ac.commit(); ac.close()
    # resolved PRE-TAG check with HT-ratio WARNING
    _save_ht_warning_check(s)
    return s, conf, adb


def _save_ht_warning_check(store):
    session = {
        "check_session_id": "chk1", "run_id": "run_0002", "candidate_id": "cand_parent",
        "alpha_id": "ZYEVroY0", "phase": "PRE_TAG", "session_status": "RESOLVED",
        "started_at": NOW, "resolved_at": NOW, "poll_count": 1, "http_request_count": 1,
        "pending_poll_requests": 0, "base_gate_result": "PENDING", "theme_gate_result": "WARNING",
        "error_type": None, "error_nature": None,
        "polls": [{
            "semantic_poll_index": 1, "http_request_count_delta": 1,
            "http_status": 200, "raw_response_text": "{}", "created_at": NOW,
            "parsed": {
                "session_semantic_status": "RESOLVED", "parse_status": "RESOLVED",
                "base_gate": {"status": "PENDING"}, "theme_gate": {"status": "WARNING"},
                "pending_check_names": [], "error_type": None, "error_nature": None,
                "check_parser_version": 1, "check_alias_version": 1, "raw_payload": {},
                "results": [
                    {"raw_name": "HT_HIGH_TURNOVER_RETURNS_RATIO", "normalized_name": "HIGH_TURNOVER_RETURNS_RATIO",
                     "category": "THEME", "raw_result": "WARNING", "normalized_result": "WARNING",
                     "raw_value": 0.7374, "raw_limit": 0.75, "normalized_value": 0.7374, "normalized_limit": 0.75,
                     "unit": None, "unit_confidence": "HIGH", "preset_limit": None, "live_limit": 0.75, "effective_limit": 0.75,
                     "limit_source": "LIVE_RAW_LIMIT", "status": None, "message": None, "parser_version": 1, "alias_version": 1,
                     "mapping_suggestion": None, "eligibility_outcome": "WARNING", "eligibility_reason": None,
                     "threshold_exceeded": 0, "diagnosis_outcome": None, "diagnosis_reason": None},
                ],
            },
        }],
    }
    store.save_check_session(session, evidence_source="LIVE_CHECK")


_EXTERNAL_EVIDENCE = [{
    "evidence_id": "ev_market", "source": "EXTERNAL_CONFIRMED_EVIDENCE",
    "strategy": "NEUTRALIZATION_MICRO_TUNE",
    "parameter_change": {"neutralization": "SUBINDUSTRY -> MARKET"},
    "parent_value": 0.7374, "child_value": 0.7634, "live_limit": 0.75,
    "delta": 0.026, "verdict": "TARGET_PASS", "alpha_id": None,
}]


# ---------------------------------------------------------------------------
# preview_rescue external evidence behavior
# ---------------------------------------------------------------------------

def test_preview_external_success_is_hint_only(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    _configure_audit(tmp_path)
    out = preview_rescue(s, conf, adb, "run_0002", "cand_parent", _EXTERNAL_EVIDENCE)
    assert out["classification"] == "STRONG_NEAR_PASS"
    assert out["auto_stop_reason"] is None  # NOT STOP_RESCUE_SUCCESS


def test_preview_external_success_fields(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    _configure_audit(tmp_path)
    out = preview_rescue(s, conf, adb, "run_0002", "cand_parent", _EXTERNAL_EVIDENCE)
    assert out["external_success_evidence"] is True
    assert out["external_success_strategy"] == "NEUTRALIZATION_MICRO_TUNE"
    assert out["external_success_change"] == {"neutralization": "SUBINDUSTRY -> MARKET"}
    assert out["external_target_value"] == 0.7634
    assert out["external_live_limit"] == 0.75
    assert out["external_evidence_source"] == "EXTERNAL_CONFIRMED_EVIDENCE"


def test_preview_external_success_boosts_recommendation_priority(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    _configure_audit(tmp_path)
    out = preview_rescue(s, conf, adb, "run_0002", "cand_parent", _EXTERNAL_EVIDENCE)
    rec = out["recommended_strategy"]
    assert rec is not None and rec["strategy"] == "NEUTRALIZATION_MICRO_TUNE"
    assert rec.get("priority_hint", {}).get("outcome") == "TARGET_PASS"


def test_preview_external_does_not_change_lifecycle(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    before = None
    with s.connect() as c:
        before = c.execute("SELECT lifecycle_state FROM ppl_candidates WHERE candidate_id='cand_parent'").fetchone()[0]
    _configure_audit(tmp_path)
    preview_rescue(s, conf, adb, "run_0002", "cand_parent", _EXTERNAL_EVIDENCE)
    with s.connect() as c:
        after = c.execute("SELECT lifecycle_state FROM ppl_candidates WHERE candidate_id='cand_parent'").fetchone()[0]
    assert before == after


def test_preview_external_no_rescue_stopped_event(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    log = _configure_audit(tmp_path)
    preview_rescue(s, conf, adb, "run_0002", "cand_parent", _EXTERNAL_EVIDENCE)
    events = [json.loads(l) for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
    actions = [e["action"] for e in events]
    assert "RESCUE_STOPPED" not in actions
    ext_ev = [e for e in events if e["action"] == "EXTERNAL_RESCUE_EVIDENCE"]
    assert ext_ev and ext_ev[0]["evidence_outcome"] == "TARGET_PASS"


def test_system_target_pass_still_stops_at_rule_level():
    # System-confirmed TARGET_PASS still resolves STOP_RESCUE_SUCCESS (the stop
    # rule itself, which is what drives the RESCUE_STOPPED audit event).
    assert evaluate_rescue_stop([("NEUTRALIZATION_MICRO_TUNE", "TARGET_PASS", "LIVE_CHECK_CONFIRMED")], 1, 5) == "STOP_RESCUE_SUCCESS"


def test_preview_uses_durable_system_repair_edge_for_stop(tmp_path):
    s, conf, adb = _make_rescue_store(tmp_path)
    _configure_audit(tmp_path)
    spec = {
        "repair_type": "NEUTRALIZATION_MICRO_TUNE",
        "repair_signature": "sig_market",
        "repair_depth": 1,
        "expression_preview": "ts_mean(predicted_first_quantile_one_day_return_2, 2)",
        "settings_override": {"neutralization": "MARKET"},
        "operator_requirements": ["ts_mean"],
    }
    with s.connect() as c:
        c.execute(
            """INSERT INTO ppl_repair_plans(
                repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                blocked_reason,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("rplan_market", "diag_market", "run_0002", "cand_parent", "cand_parent",
             "HT_RETURNS_RATIO_FAIL", "NEUTRALIZATION_MICRO_TUNE", "sig_market", "[]", 1,
             json.dumps(spec), json.dumps(["ts_mean"]), "EXECUTED", 1, 1, 1, None, NOW, NOW),
        )
        c.execute(
            """INSERT INTO ppl_repairs(
                repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,repair_signature,
                repair_path_json,repair_depth,before_json,after_json,delta_json,side_effect_verdict,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("repair_market", "run_0002", "cand_parent", "cand_child", "NEUTRALIZATION_MICRO_TUNE",
             "sig_market", "[]", 1, json.dumps({"ht_ratio": 0.7374}),
             json.dumps({"ht_ratio": 0.7732, "alpha_id": "A_CHILD"}),
             json.dumps({"ht_ratio": 0.0358, "live_limit": 0.75}), "TARGET_PASS", NOW),
        )
    out = preview_rescue(s, conf, adb, "run_0002", "cand_parent", [])
    assert out["auto_stop_reason"] == "STOP_RESCUE_SUCCESS"
    assert out["allowed_to_execute"] is False
    assert any(e.get("system_verdict") == "TARGET_PASS" for e in out["historical_evidence"])


def test_manual_review_excludes_system_success_even_when_attempt_limit_reached(tmp_path):
    from ppl_engine.near_pass import list_manual_review
    s, conf, adb = _make_rescue_store(tmp_path)
    spec = {
        "repair_type": "NEUTRALIZATION_MICRO_TUNE", "repair_signature": "sig_success",
        "repair_depth": 1, "expression_preview": "ts_mean(predicted_first_quantile_one_day_return_2, 2)",
        "settings_override": {"neutralization": "MARKET"}, "operator_requirements": ["ts_mean"],
    }
    with s.connect() as c:
        # Five executed plans reach the STRONG_NEAR_PASS allowance; the last one
        # carries a system TARGET_PASS edge. This must not become P1_MANUAL.
        for i in range(5):
            sig = f"sig_success_{i}"
            c.execute(
                """INSERT INTO ppl_repair_plans(repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,
                   target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                   operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                   blocked_reason,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (f"rplan_success_{i}", f"diag_success_{i}", "run_0002", "cand_parent", "cand_parent",
                 "HT_RETURNS_RATIO_FAIL", "NEUTRALIZATION_MICRO_TUNE", sig, "[]", 1, json.dumps(spec),
                 json.dumps(["ts_mean"]), "EXECUTED", 1, 1, 1, None, NOW, NOW),
            )
            c.execute(
                """INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,
                   repair_signature,repair_path_json,repair_depth,before_json,after_json,delta_json,side_effect_verdict,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (f"repair_success_{i}", "run_0002", "cand_parent", f"cand_child_{i}",
                 "NEUTRALIZATION_MICRO_TUNE", sig, "[]", 1, "{}", "{}", "{}",
                 "TARGET_PASS" if i == 4 else "IMPROVED", NOW),
            )
    out = list_manual_review(s, conf, adb, "run_0002")
    all_ids = {r["candidate_id"] for k in ("P1_MANUAL", "P2_MANUAL", "P3_ARCHIVE") for r in out[k]}
    assert "cand_parent" not in all_ids
