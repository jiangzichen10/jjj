import json
from pathlib import Path

import pytest

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.research_run_mode import (
    COMPATIBILITY_EVIDENCE_MODE,
    PHASE_COMPATIBILITY_AUTHORITY,
    SHADOW_ONLY,
    parse_research_run_policy,
    research_run_status,
    validate_durable_research_run_lock,
    validate_new_research_run,
)
from ppl_engine.round_orchestrator import load_round_policy, round_status
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.scheduler_evidence import record_scheduler_evaluation, refresh_scheduler_outcomes
from ppl_engine.scheduler_evidence_report import build_scheduler_evidence_report
from ppl_engine.scheduler_shadow import QueueFacts, ShadowSchedulerSnapshot, choose_shadow_action, policy_from_mapping
from ppl_engine.strategy_contracts import SchedulerActionType
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(
        ROOT / "ppl_rules.yaml",
        ROOT / "ppl_plan_v31.yaml",
        project_dir=ROOT,
    )


def _d2e_policy():
    return load_round_policy(ROOT / "ppl_round_v31_d2e.yaml", _config())


def test_d2e_policy_pre_registers_run_0006_authority_lock_and_state_maturation():
    policy = _d2e_policy()
    mode = parse_research_run_policy(policy)
    assert mode.mode == COMPATIBILITY_EVIDENCE_MODE
    assert mode.expected_run_id == "run_0006"
    assert mode.scheduler_authority == PHASE_COMPATIBILITY_AUTHORITY
    assert mode.scheduler_shadow == SHADOW_ONLY
    assert mode.adaptive_control == "DISABLED"
    assert mode.authority_transition_allowed is False
    assert mode.automatic_evidence_stop is False
    assert mode.maturation_semantics == "STATE_RESOLVED"
    assert mode.long_running_semantics == "RIGHT_CENSORED_UNTIL_RESOLVED"
    assert policy["strategy_integration"]["mode"] == "PHASE_COMPATIBILITY"
    assert policy["scheduler_shadow"]["mode"] == "SHADOW_ONLY"


def test_run_0006_is_reserved_and_requires_explicit_d2e_policy():
    standard = load_round_policy(ROOT / "ppl_round_v31.yaml", _config())
    with pytest.raises(ValueError, match="RUN_0006_RESERVED_FOR_D2E"):
        validate_new_research_run(standard, requested_run_id="run_0006")
    with pytest.raises(ValueError, match="RUN_0006_RESERVED_FOR_D2E"):
        validate_new_research_run(standard, requested_run_id=None, resolved_run_id="run_0006")
    with pytest.raises(ValueError, match="D2E_EXPLICIT_RUN_ID_REQUIRED"):
        validate_new_research_run(_d2e_policy(), requested_run_id=None)
    with pytest.raises(ValueError, match="D2E_RUN_ID_LOCK_MISMATCH"):
        validate_new_research_run(_d2e_policy(), requested_run_id="run_0007")
    parsed = validate_new_research_run(_d2e_policy(), requested_run_id="run_0006")
    assert parsed.mode == COMPATIBILITY_EVIDENCE_MODE


def test_d2e_resume_rejects_any_research_authority_lock_drift():
    stored = _d2e_policy()
    candidate = json.loads(json.dumps(stored))
    candidate["research_run"]["authority_transition_allowed"] = True
    with pytest.raises(ValueError, match="D2E_RESEARCH_RUN_LOCK_DRIFT"):
        validate_durable_research_run_lock(stored, candidate, run_id="run_0006")
    with pytest.raises(ValueError, match="D2E_DURABLE_RUN_ID_CONFLICT"):
        validate_durable_research_run_lock(stored, stored, run_id="run_0007")
    standard = load_round_policy(ROOT / "ppl_round_v31.yaml", _config())
    with pytest.raises(ValueError, match="RUN_0006_DURABLE_MODE_CONFLICT"):
        validate_durable_research_run_lock(standard, standard, run_id="run_0006")


def test_d2e_round_persists_lock_in_durable_config_and_status(tmp_path):
    cfg = _config()
    policy = _d2e_policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        policy=policy,
        total_budget=policy["total_budget"],
        search_budget=policy["search_budget"],
        repair_budget=policy["repair_budget"],
    )
    with store.connect() as conn:
        raw = conn.execute(
            "SELECT config_json FROM ppl_rounds WHERE run_id='run_0006'"
        ).fetchone()[0]
    durable = json.loads(raw)
    assert durable["research_run"]["mode"] == "COMPATIBILITY_EVIDENCE"
    assert durable["research_run"]["expected_run_id"] == "run_0006"
    status = research_run_status(durable, run_id="run_0006")
    assert status["authority_locked"] is True
    assert status["adaptive_activation_allowed"] is False
    assert status["d2e_evidence_only"] is True


def test_d2e_calibration_report_exposes_preregistered_maturation_and_never_autostops(tmp_path):
    cfg = _config()
    policy = _d2e_policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        policy=policy,
        total_budget=policy["total_budget"],
        search_budget=policy["search_budget"],
        repair_budget=policy["repair_budget"],
    )
    report = build_scheduler_evidence_report(store.path, run_id="run_0006")
    assert report["research_run"]["mode"] == "COMPATIBILITY_EVIDENCE"
    assert report["research_run"]["adaptive_activation_allowed"] is False
    assert report["maturation_protocol"]["semantics"] == "STATE_RESOLVED"
    assert report["maturation_protocol"]["minimum_observation_age_seconds"] is None
    assert report["maturation_protocol"]["post_hoc_age_threshold_forbidden"] is True
    assert report["authoritative"] is False
    assert report["activation_side_effect"] is False



def test_d2e_evidence_monitor_reports_qualitative_coverage_without_numeric_sufficiency_threshold(tmp_path):
    cfg = _config()
    policy = _d2e_policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=policy["total_budget"], search_budget=policy["search_budget"], repair_budget=policy["repair_budget"],
    )
    sched = policy_from_mapping(policy["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=3, oldest_age_seconds=90),
        repair_queue=QueueFacts(backlog=2, oldest_age_seconds=120),
        remote_slot_limit=1, remote_slots_reserved=0,
    )
    decision = choose_shadow_action(snap, sched)
    record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=1,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=1,
        selection_fingerprint="d2e-search-1", decision_timestamp="2026-08-28T12:00:00+00:00",
    )
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 post_started_at,simulation_status,classification,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("round_run_0006","run_0006",1,1,"SEARCH","c1","f1","s1","NEW_POST",
             "2026-08-28T12:00:00+00:00","COMPLETE","PPL_SUCCESS","{}",
             "2026-08-28T12:00:00+00:00","2026-08-28T12:10:00+00:00"),
        )
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")
    report = build_scheduler_evidence_report(store.path, run_id="run_0006")
    monitor = report["evidence_sufficiency_monitor"]
    assert monitor["automatic_stop"] is False
    assert monitor["automatic_pause"] is False
    assert monitor["automatic_ready_for_calibration"] is False
    assert monitor["quantitative_sufficiency_thresholds"] is None
    assert monitor["search_matured_evidence_present"] is True
    assert report["maturation_latency"]["SEARCH"]["matured_new_posts"] == 1
    assert report["maturation_latency"]["SEARCH"]["minimum_observation_age_seconds"] is None
    assert report["productivity_window_stability"]["automatic_stability_threshold"] is False
    assert report["productivity_window_stability"]["stability_thresholds"] is None


def test_orchestrator_has_creation_and_resume_d2e_lock_guards_and_no_shadow_execution_branch():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    assert "validate_new_research_run(policy, requested_run_id=requested_run_id, resolved_run_id=run_id)" in source
    assert "validate_durable_research_run_lock(stored_policy, policy, run_id=run_id)" in source
    assert ".shadow_action" not in source
