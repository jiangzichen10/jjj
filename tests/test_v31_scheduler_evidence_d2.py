import inspect
import json
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.round_orchestrator import _scheduler_shadow_observation, load_round_policy
from ppl_engine.round_store import ROUND_SCHEMA_VERSION, create_round, ensure_round_schema
from ppl_engine.scheduler_evidence import (
    COUNTERFACTUAL_PROXY_KIND,
    EVIDENCE_ONLY_MODE,
    build_safety_gate_report,
    deterministic_replay,
    evidence_policy_from_mapping,
    fallback_disposition,
    load_scheduler_evaluations,
    load_scheduler_outcomes,
    persist_safety_gate_report,
    record_scheduler_evaluation,
    refresh_scheduler_outcomes,
)
from ppl_engine.scheduler_shadow import (
    QueueFacts,
    ResearchAvailabilityFacts,
    ShadowSchedulerSnapshot,
    choose_shadow_action,
    policy_from_mapping,
    productivity_for_window,
    shadow_policy_hash,
)
from ppl_engine.store import RunnerStore
from ppl_engine.strategy_contracts import SchedulerActionType

ROOT = Path(__file__).resolve().parents[1]


def _config(project_dir=ROOT):
    return load_effective_config(
        Path(project_dir) / "ppl_rules.yaml",
        Path(project_dir) / "ppl_plan_v31.yaml",
        project_dir=Path(project_dir),
    )


def _policy(project_dir=ROOT):
    return load_round_policy(Path(project_dir) / "ppl_round_v31.yaml", _config(project_dir))


def _available(count, slots=4):
    return ResearchAvailabilityFacts(
        raw_backlog_count=count, selector_eligible_count=count, preview_safe_count=count,
        execution_eligible_count=count, evaluation_complete=True, reason="TEST",
        remote_slots_free=slots, immediately_dispatchable_count=min(count, slots),
    )


def _store(tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        policy=_policy(),
        total_budget=2000,
        search_budget=1600,
        repair_budget=400,
    )
    return store, cfg


def test_d2_policy_identity_keeps_scheduler_and_evidence_versions_separate():
    policy = _policy()
    sched = policy_from_mapping(policy["scheduler_shadow"])
    evidence = evidence_policy_from_mapping(policy["scheduler_evidence"])
    assert policy["scheduler_evidence"]["mode"] == EVIDENCE_ONLY_MODE
    assert sched.policy_version == "V31_SCHED_SHADOW_005"
    assert evidence.policy_version == "V31_SCHED_EVIDENCE_004"
    assert shadow_policy_hash(sched)
    # Evidence policy identity is not overloaded into Scheduler identity.
    assert evidence.policy_version != sched.policy_version
    assert policy["strategy_integration"]["mode"] == "PHASE_COMPATIBILITY"


def test_d2_matured_productivity_excludes_right_censored_running_rows():
    ledger = [
        {
            "logical_sequence_no": 1, "phase": "SEARCH", "origin": "NEW_POST", "sim_key": "old",
            "simulation_status": "COMPLETE", "classification": "PPL_SUCCESS", "family_id": "f1",
            "pretag_status": "PASS", "local_gate": "PASS",
        },
        {
            "logical_sequence_no": 2, "phase": "SEARCH", "origin": "NEW_POST", "sim_key": "new-running",
            "simulation_status": "RUNNING", "classification": "", "family_id": "f2",
        },
        {
            "logical_sequence_no": 3, "phase": "SEARCH", "origin": "NEW_POST", "sim_key": "uncertain",
            "simulation_status": "UNCERTAIN_SUBMISSION", "classification": "", "family_id": "f3",
        },
    ]
    metrics = productivity_for_window(ledger, SchedulerActionType.SEARCH, 100)
    assert metrics.attempts == 1  # matured cohort denominator
    assert metrics.observed_attempts == 3
    assert metrics.censored_attempts == 2
    assert metrics.completed == 1
    assert metrics.ready == 1


def test_d2_complete_search_waits_for_resolved_classification_before_maturing():
    ledger = [{
        "logical_sequence_no": 1, "phase": "SEARCH", "origin": "NEW_POST", "sim_key": "s1",
        "simulation_status": "COMPLETE", "classification": "", "family_id": "f1",
    }]
    metrics = productivity_for_window(ledger, SchedulerActionType.SEARCH, 100)
    assert metrics.attempts == 0
    assert metrics.observed_attempts == 1
    assert metrics.censored_attempts == 1


def test_d2_hard_starvation_guard_forces_opposite_research_side_when_both_backlogs_exist():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    search_streak = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=100, oldest_age_seconds=0),
        repair_queue=QueueFacts(backlog=1, oldest_age_seconds=0),
        search_availability=_available(100, 1), repair_availability=_available(1, 1),
        remote_slot_limit=1,
        remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.SEARCH,
        consecutive_count=policy.max_consecutive_same_action,
    )
    repair_streak = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.REPAIR,
        search_queue=QueueFacts(backlog=1, oldest_age_seconds=0),
        repair_queue=QueueFacts(backlog=100, oldest_age_seconds=0),
        search_availability=_available(1, 1), repair_availability=_available(100, 1),
        remote_slot_limit=1,
        remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.REPAIR,
        consecutive_count=policy.max_consecutive_same_action,
    )
    assert choose_shadow_action(search_streak, policy).shadow_action is SchedulerActionType.REPAIR
    assert choose_shadow_action(repair_streak, policy).shadow_action is SchedulerActionType.SEARCH
    assert choose_shadow_action(search_streak, policy).reason == "SHADOW_HARD_STARVATION_GUARD"


def test_d2_slot_one_and_all_remote_slots_reserved_yields_wait_without_arbitrating_runtime_obligations():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=4, oldest_age_seconds=100),
        repair_queue=QueueFacts(backlog=4, oldest_age_seconds=100),
        search_availability=_available(4, 0), repair_availability=_available(4, 0),
        remote_slot_limit=1,
        remote_slots_reserved=1,
    )
    decision = choose_shadow_action(snap, policy)
    assert decision.shadow_action is SchedulerActionType.WAIT
    assert decision.reason == "SHADOW_WAIT_SERVER_SLOT"
    assert decision.authoritative is False


def test_d2_deterministic_replay_same_facts_same_policy_same_decision_hash():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=7, oldest_age_seconds=321),
        repair_queue=QueueFacts(backlog=5, oldest_age_seconds=123),
        search_availability=_available(7, 2), repair_availability=_available(5, 2),
        remote_slot_limit=4,
        remote_slots_reserved=2,
        consecutive_action=SchedulerActionType.REPAIR,
        consecutive_count=2,
    )
    report = deterministic_replay(snap, policy, repetitions=8)
    assert report.passed is True
    assert len(set(report.hashes)) == 1
    assert report.scheduler_policy_hash == shadow_policy_hash(policy)


def test_d2_evaluation_ledger_records_required_frozen_facts_and_never_authoritative(tmp_path):
    store, _ = _store(tmp_path)
    policy = _policy()
    sched = policy_from_mapping(policy["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=9, oldest_age_seconds=90),
        repair_queue=QueueFacts(backlog=3, oldest_age_seconds=180),
        search_availability=_available(9, 3), repair_availability=_available(3, 3),
        remote_slot_limit=4,
        remote_slots_reserved=1,
        consecutive_action=SchedulerActionType.SEARCH,
        consecutive_count=2,
    )
    decision = choose_shadow_action(snap, sched)
    rec = record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=1,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=2,
        selection_fingerprint="fingerprint", decision_timestamp="2026-08-28T10:00:00+00:00",
    )
    rows = load_scheduler_evaluations(store, "round_run_0006")
    assert len(rows) == 1
    row = rows[0]
    assert row["decision_key"] == rec["decision_key"]
    assert row["actual_action"] == "SEARCH"
    assert row["scheduler_policy_version"] == "V31_SCHED_SHADOW_005"
    assert row["evidence_policy_version"] == "V31_SCHED_EVIDENCE_004"
    assert row["scheduler_policy_hash"] == shadow_policy_hash(sched)
    assert row["search_backlog"] == 9 and row["repair_backlog"] == 3
    assert row["remote_slots_free"] == 3
    assert row["consecutive_action"] == "SEARCH" and row["consecutive_count"] == 2
    assert row["authoritative"] == 0
    fairness = json.loads(row["fairness_state_json"])
    assert fairness["hard_starvation_guard"] is True


def test_d2_outcome_evaluation_uses_only_actual_action_and_labels_unexecuted_shadow_as_proxy(tmp_path):
    store, _ = _store(tmp_path)
    policy = _policy()
    sched = policy_from_mapping(policy["scheduler_shadow"])
    # Force a disagreement: actual SEARCH, shadow REPAIR.
    search_bad = productivity_for_window([
        {"logical_sequence_no": 1,"phase":"SEARCH","origin":"NEW_POST","sim_key":"old-s",
         "simulation_status":"COMPLETE","classification":"TERMINAL_FAIL","family_id":"oldf"}
    ], SchedulerActionType.SEARCH, 100)
    repair_good = productivity_for_window([
        {"logical_sequence_no": 2,"phase":"REPAIR","origin":"NEW_POST","sim_key":"old-r",
         "simulation_status":"COMPLETE","repair_verdict":"TARGET_PASS","family_id":"oldrf"}
    ], SchedulerActionType.REPAIR, 100)
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=2, oldest_age_seconds=0),
        repair_queue=QueueFacts(backlog=2, oldest_age_seconds=0),
        search_availability=_available(2), repair_availability=_available(2),
        search_productivity=(search_bad,), repair_productivity=(repair_good,),
        remote_slot_limit=4, remote_slots_reserved=0,
    )
    decision = choose_shadow_action(snap, sched)
    assert decision.shadow_action is SchedulerActionType.REPAIR
    record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=3,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=1,
        selection_fingerprint="s3", decision_timestamp="2026-08-28T10:01:00+00:00",
    )
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 simulation_status,classification,family_result,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("round_run_0006","run_0006",1,3,"SEARCH","c3","fam3","sk3","NEW_POST",
             "COMPLETE","PPL_SUCCESS","WINNER","{}","2026-08-28T10:01:00+00:00","2026-08-28T10:02:00+00:00"),
        )
        # A REPAIR row from another batch must not be attributed to this actual SEARCH decision.
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 simulation_status,repair_verdict,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("round_run_0006","run_0006",2,4,"REPAIR","r4","rf4","rk4","NEW_POST",
             "COMPLETE","TARGET_PASS","{}","2026-08-28T10:01:00+00:00","2026-08-28T10:02:00+00:00"),
        )
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")
    outcomes = load_scheduler_outcomes(store, "round_run_0006")
    assert len(outcomes) == 1
    row = outcomes[0]
    assert row["actual_action"] == "SEARCH"
    assert row["outcome_state"] == "MATURED"
    assert row["total_new_posts"] == 1 and row["complete_count"] == 1
    assert row["ready_count"] == 1 and row["family_winner_count"] == 1
    assert row["repair_target_pass_count"] == 0
    assert row["counterfactual_kind"] == COUNTERFACTUAL_PROXY_KIND
    proxy = json.loads(row["counterfactual_proxy_json"])
    assert proxy["action"] == "REPAIR"
    assert proxy["observed_outcome"] is False


def test_d2_outcome_keeps_running_new_posts_censored_not_low_yield(tmp_path):
    store, _ = _store(tmp_path)
    policy = _policy(); sched = policy_from_mapping(policy["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=1), repair_queue=QueueFacts(backlog=0),
        remote_slot_limit=4, remote_slots_reserved=0,
    )
    decision = choose_shadow_action(snap, sched)
    record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=5,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=1,
        selection_fingerprint="s5",
    )
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 simulation_status,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("round_run_0006","run_0006",1,5,"SEARCH","c5","fam5","sk5","NEW_POST",
             "RUNNING","{}","2026-08-28T10:01:00+00:00","2026-08-28T10:02:00+00:00"),
        )
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")
    row = load_scheduler_outcomes(store, "round_run_0006")[0]
    assert row["outcome_state"] == "PENDING"
    assert row["matured_new_posts"] == 0
    assert row["censored_new_posts"] == 1
    assert row["effective_simulation_ratio"] is None


def test_d2_safety_gate_is_ineligible_while_sample_thresholds_are_intentionally_unset(tmp_path):
    store, _ = _store(tmp_path)
    policy = _policy(); sched = policy_from_mapping(policy["scheduler_shadow"])
    report = build_safety_gate_report(
        scheduler_policy=sched, evidence_raw=policy["scheduler_evidence"],
        observation_count=999, search_samples=500, repair_samples=500,
        replay_pass=True, starvation_pass=True, slot_safety_pass=True,
        no_repost_pass=True, recovery_safety_pass=True, policy_identity_pass=True,
    )
    assert report.eligible is False
    assert report.status == "INELIGIBLE_THRESHOLDS_UNSET"
    key = persist_safety_gate_report(store, "round_run_0006", "run_0006", report)
    with store.connect() as conn:
        row = conn.execute("SELECT * FROM ppl_round_scheduler_gate_reports WHERE report_key=?", (key,)).fetchone()
    assert row is not None and row["eligible"] == 0


def test_d2_safety_gate_mechanism_can_evaluate_explicit_future_thresholds_without_activating():
    policy = _policy(); sched = policy_from_mapping(policy["scheduler_shadow"])
    raw = dict(policy["scheduler_evidence"])
    raw["activation_thresholds"] = {
        "minimum_observations": 10,
        "minimum_search_samples": 3,
        "minimum_repair_samples": 3,
    }
    report = build_safety_gate_report(
        scheduler_policy=sched, evidence_raw=raw,
        observation_count=10, search_samples=3, repair_samples=3,
        replay_pass=True, starvation_pass=True, slot_safety_pass=True,
        no_repost_pass=True, recovery_safety_pass=True, policy_identity_pass=True,
    )
    assert report.eligible is True
    assert report.status == "ELIGIBLE_FOR_FUTURE_CANARY_REVIEW"
    assert report.as_dict()["authoritative"] is False
    assert report.as_dict()["activation_side_effect"] is False


def test_d2_fallback_classification_preserves_fail_closed_core_invariants():
    assert fallback_disposition("SCHEDULER_EXCEPTION") == "PHASE_COMPATIBILITY"
    assert fallback_disposition("INVALID_DECISION") == "PHASE_COMPATIBILITY"
    assert fallback_disposition("POLICY_MISMATCH") == "PHASE_COMPATIBILITY"
    assert fallback_disposition("SLOT_CONFLICT") == "PHASE_COMPATIBILITY"
    assert fallback_disposition("MISSING_IDENTITY") == "PHASE_COMPATIBILITY"
    assert fallback_disposition("DUPLICATE_POST_RISK") == "FAIL_CLOSED_GLOBAL_HALT"
    assert fallback_disposition("DURABLE_IDENTITY_CONFLICT") == "FAIL_CLOSED_GLOBAL_HALT"
    assert fallback_disposition("DB_CORRUPTION") == "FAIL_CLOSED_GLOBAL_HALT"
    assert fallback_disposition("CORE_INVARIANT_FAILURE") == "FAIL_CLOSED_GLOBAL_HALT"


def test_d2_runtime_obligations_are_not_inputs_to_research_expected_value_model():
    import ppl_engine.round_orchestrator as orchestrator
    source = inspect.getsource(orchestrator._scheduler_shadow_observation)
    # Research arbitration may use conservative slot availability, but it must
    # not invoke or score durable runtime obligations.
    for forbidden in (
        "poll_due_remote_work(", "poll_due_checks(", "recover_waiting_auth(",
        "poll_due_discovery_work(", "REPORT_DEGRADED", "endpoint_retry",
    ):
        assert forbidden not in source
    # And D2 still cannot branch execution on the shadow recommendation.
    whole = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    assert ".shadow_action" not in whole


def test_d2_additive_evidence_schema_does_not_bump_round_schema_contract():
    assert ROUND_SCHEMA_VERSION == 4


def test_d2_module_contains_no_http_post_or_durable_workflow_transition_primitives():
    source = (ROOT / "ppl_engine" / "scheduler_evidence.py").read_text(encoding="utf-8")
    forbidden = ["requests.", "session.post", "httpx.", "transition_run(", "transition_candidate(", "execute_round_repair("]
    for token in forbidden:
        assert token not in source


def test_d2_replay_can_reconstruct_snapshot_from_durable_evaluation_row(tmp_path):
    from ppl_engine.scheduler_evidence import replay_evaluation_record
    store, _ = _store(tmp_path)
    policy = _policy(); sched = policy_from_mapping(policy["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.REPAIR,
        search_queue=QueueFacts(backlog=3, oldest_age_seconds=33),
        repair_queue=QueueFacts(backlog=7, oldest_age_seconds=77),
        remote_slot_limit=4, remote_slots_reserved=1,
        consecutive_action=SchedulerActionType.SEARCH, consecutive_count=2,
    )
    decision = choose_shadow_action(snap, sched)
    record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=8,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=2,
        selection_fingerprint="freeze8",
    )
    row = load_scheduler_evaluations(store, "round_run_0006")[0]
    replay = replay_evaluation_record(row, sched, repetitions=7)
    assert replay.passed is True
    assert replay.decision_hash == row["replay_decision_hash"]


def test_d2_starvation_stress_harness_covers_dominance_slot_remote_429_check_and_discovery_boundaries():
    from ppl_engine.scheduler_evidence import starvation_stress_report
    sched = policy_from_mapping(_policy()["scheduler_shadow"])
    report = starvation_stress_report(sched)
    assert report["passed"] is True
    assert report["search_not_starved"] is True
    assert report["repair_not_starved"] is True
    assert report["slot_one_full_wait"] is True
    assert report["remote_running_capacity_pressure_modeled_only_as_slot_reservation"] is True
    assert report["rate_limit_wait_is_runtime_obligation_not_value_input"] is True
    assert report["check_backlog_is_runtime_obligation_not_value_input"] is True
    assert report["discovery_wait_is_runtime_obligation_not_value_input"] is True
    assert report["authoritative"] is False


def test_d2_durable_gate_reads_replay_policy_identity_and_matured_sample_counts(tmp_path):
    from ppl_engine.scheduler_evidence import evaluate_durable_safety_gate
    store, _ = _store(tmp_path)
    policy = _policy(); sched = policy_from_mapping(policy["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=1), repair_queue=QueueFacts(backlog=1),
        remote_slot_limit=1, remote_slots_reserved=0,
    )
    decision = choose_shadow_action(snap, sched)
    record_scheduler_evaluation(
        store, round_id="round_run_0006", run_id="run_0006", batch_no=9,
        snapshot=snap, decision=decision, scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], selected_count=1,
        selection_fingerprint="freeze9",
    )
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 simulation_status,classification,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("round_run_0006","run_0006",1,9,"SEARCH","c9","fam9","sk9","NEW_POST",
             "COMPLETE","PPL_SUCCESS","{}","2026-08-28T10:01:00+00:00","2026-08-28T10:02:00+00:00"),
        )
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")
    gate = evaluate_durable_safety_gate(
        store, round_id="round_run_0006", scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"], no_repost_pass=True, recovery_safety_pass=True,
    )
    assert gate.observation_count == 1
    assert gate.search_samples == 1
    assert gate.repair_samples == 0
    assert gate.checks["deterministic_replay"] is True
    assert gate.checks["policy_identity"] is True
    assert gate.checks["starvation"] is True
    assert gate.checks["slot_safety"] is True
    # Production thresholds are intentionally unset in D2, so no activation.
    assert gate.eligible is False
    assert gate.status == "INELIGIBLE_THRESHOLDS_UNSET"
