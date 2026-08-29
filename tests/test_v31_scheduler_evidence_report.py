import hashlib
import json
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.round_orchestrator import load_round_policy
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.scheduler_evidence import (
    record_scheduler_evaluation,
    refresh_scheduler_outcomes,
)
from ppl_engine.scheduler_evidence_report import (
    REPORT_MODE,
    REPORT_SCHEMA,
    build_scheduler_evidence_report,
)
from ppl_engine.scheduler_shadow import (
    QueueFacts,
    ResearchAvailabilityFacts,
    ShadowSchedulerSnapshot,
    choose_shadow_action,
    policy_from_mapping,
)
from ppl_engine.store import RunnerStore
from ppl_engine.strategy_contracts import SchedulerActionType

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(
        ROOT / "ppl_rules.yaml",
        ROOT / "ppl_plan_v31.yaml",
        project_dir=ROOT,
    )


def _policy():
    return load_round_policy(ROOT / "ppl_round_v31.yaml", _config())


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
    return store


def _record(store, *, batch_no, actual_action, search_backlog, repair_backlog, slots_free=True, selected="x"):
    policy = _policy()
    sched = policy_from_mapping(policy["scheduler_shadow"])
    free = 1 if slots_free else 0
    search_availability = ResearchAvailabilityFacts(
        raw_backlog_count=search_backlog, selector_eligible_count=search_backlog,
        preview_safe_count=search_backlog, execution_eligible_count=search_backlog,
        evaluation_complete=True, reason="TEST", remote_slots_free=free,
        immediately_dispatchable_count=min(search_backlog, free),
    )
    repair_availability = ResearchAvailabilityFacts(
        raw_backlog_count=repair_backlog, selector_eligible_count=repair_backlog,
        preview_safe_count=repair_backlog, execution_eligible_count=repair_backlog,
        evaluation_complete=True, reason="TEST", remote_slots_free=free,
        immediately_dispatchable_count=min(repair_backlog, free),
    )
    snap = ShadowSchedulerSnapshot(
        actual_action=actual_action,
        search_queue=QueueFacts(backlog=search_backlog, oldest_age_seconds=30.0),
        repair_queue=QueueFacts(backlog=repair_backlog, oldest_age_seconds=60.0),
        search_availability=search_availability,
        repair_availability=repair_availability,
        remote_slot_limit=1,
        remote_slots_reserved=0 if slots_free else 1,
    )
    decision = choose_shadow_action(snap, sched)
    return record_scheduler_evaluation(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        batch_no=batch_no,
        snapshot=snap,
        decision=decision,
        scheduler_policy=sched,
        evidence_raw=policy["scheduler_evidence"],
        selected_count=1,
        selection_fingerprint=f"fp-{batch_no}-{selected}",
        decision_timestamp=f"2026-08-28T10:{batch_no:02d}:00+00:00",
    )


def _insert_ledger(store, *, seq, batch_no, phase, sim_key, status="COMPLETE", classification="", verdict=""):
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                 round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,origin,
                 simulation_status,classification,repair_verdict,details_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "round_run_0006","run_0006",seq,batch_no,phase,f"c{seq}",f"fam{seq}",sim_key,"NEW_POST",
                status,classification,verdict,"{}","2026-08-28T10:00:00+00:00","2026-08-28T10:10:00+00:00",
            ),
        )


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_calibration_report_waits_for_real_d2_tables_without_creating_them(tmp_path):
    store = _store(tmp_path)
    with store.connect() as conn:
        before_tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "ppl_round_scheduler_evaluations" not in before_tables
    before_hash = _sha(store.path)

    report = build_scheduler_evidence_report(store.path, run_id="run_0006")

    assert report["report_schema"] == REPORT_SCHEMA
    assert report["mode"] == REPORT_MODE
    assert report["status"] == "WAITING_FOR_D2_EVIDENCE_TABLES"
    assert report["authoritative"] is False
    assert report["activation_side_effect"] is False
    assert report["database_writes"] == 0
    assert _sha(store.path) == before_hash
    with store.connect() as conn:
        after_tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert after_tables == before_tables


def test_calibration_report_summarizes_current_identity_agreement_maturity_and_actual_yield(tmp_path):
    store = _store(tmp_path)
    _record(store, batch_no=1, actual_action=SchedulerActionType.SEARCH, search_backlog=4, repair_backlog=1)
    _record(store, batch_no=2, actual_action=SchedulerActionType.REPAIR, search_backlog=2, repair_backlog=5)
    _insert_ledger(store, seq=1, batch_no=1, phase="SEARCH", sim_key="s1", classification="PPL_SUCCESS")
    _insert_ledger(store, seq=2, batch_no=2, phase="REPAIR", sim_key="r2", verdict="IMPROVED")
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")

    report = build_scheduler_evidence_report(store.path, run_id="run_0006")

    assert report["status"] == "EVIDENCE_ACCUMULATING_THRESHOLDS_UNSET"
    assert report["policy_identity"]["matching_current_identity"] == 2
    assert report["observations"]["decision_count"] == 2
    assert report["observations"]["replay_fail_count"] == 0
    assert report["maturity"]["matured_new_posts"] == 2
    assert report["maturity"]["censored_new_posts"] == 0
    assert report["actual_outcomes"]["SEARCH"]["ready_count"] == 1
    assert report["actual_outcomes"]["SEARCH"]["ready_rate"] == 1.0
    assert report["actual_outcomes"]["REPAIR"]["repair_improved_count"] == 1
    assert report["actual_outcomes"]["REPAIR"]["repair_success_rate"] == 1.0
    assert report["activation_threshold_review"]["recommended_thresholds"] is None
    assert report["activation_threshold_review"]["automatic_threshold_recommendation"] is False
    assert report["authoritative"] is False
    assert report["threshold_mutations"] == 0


def test_calibration_report_keeps_running_observation_censored_and_out_of_yield_denominator(tmp_path):
    store = _store(tmp_path)
    _record(store, batch_no=3, actual_action=SchedulerActionType.SEARCH, search_backlog=4, repair_backlog=0)
    _insert_ledger(store, seq=3, batch_no=3, phase="SEARCH", sim_key="running", status="RUNNING")
    refresh_scheduler_outcomes(store, "round_run_0006", "run_0006")

    report = build_scheduler_evidence_report(store.path, run_id="run_0006")

    search = report["actual_outcomes"]["SEARCH"]
    assert search["total_new_posts"] == 1
    assert search["matured_new_posts"] == 0
    assert search["censored_new_posts"] == 1
    assert search["ready_rate"] is None
    assert search["ready_rate_ci95"] is None


def test_calibration_report_measures_zero_slot_wait_and_both_backlog_coverage(tmp_path):
    store = _store(tmp_path)
    _record(
        store, batch_no=4, actual_action=SchedulerActionType.SEARCH,
        search_backlog=3, repair_backlog=2, slots_free=False,
    )
    _record(
        store, batch_no=5, actual_action=SchedulerActionType.REPAIR,
        search_backlog=3, repair_backlog=2, slots_free=True,
    )

    report = build_scheduler_evidence_report(store.path, run_id="run_0006")
    coverage = report["fairness_and_slot_coverage"]
    assert coverage["zero_free_slot_observations"] == 1
    assert coverage["zero_free_slot_shadow_wait_count"] == 1
    assert coverage["observed_zero_slot_safety_pass"] is True
    assert coverage["both_raw_backlogs_with_free_slot_observations"] == 1
    assert coverage["both_execution_eligible_with_free_slot_observations"] == 1


def test_calibration_report_does_not_pool_same_version_different_hash_identity_conflict(tmp_path):
    store = _store(tmp_path)
    current = _record(store, batch_no=6, actual_action=SchedulerActionType.SEARCH, search_backlog=1, repair_backlog=1)
    _record(store, batch_no=7, actual_action=SchedulerActionType.REPAIR, search_backlog=1, repair_backlog=1)
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_round_scheduler_evaluations SET scheduler_policy_hash=? WHERE decision_key=?",
            ("conflicting-hash", current["decision_key"]),
        )

    report = build_scheduler_evidence_report(store.path, run_id="run_0006")
    identity = report["policy_identity"]
    assert identity["all_observations"] == 2
    assert identity["matching_current_identity"] == 1
    assert identity["excluded_other_identity"] == 1
    assert identity["scheduler_version_hash_conflict"] is True
    assert identity["policy_identity_pass"] is False


def test_calibration_report_is_byte_for_byte_read_only_on_populated_db(tmp_path):
    store = _store(tmp_path)
    _record(store, batch_no=8, actual_action=SchedulerActionType.SEARCH, search_backlog=2, repair_backlog=1)
    before = _sha(store.path)
    report = build_scheduler_evidence_report(store.path, run_id="run_0006")
    after = _sha(store.path)
    assert before == after
    assert report["database_writes"] == 0
    assert report["activation_side_effect"] is False


def test_calibration_report_source_contains_no_sql_write_or_activation_primitives():
    source = (ROOT / "ppl_engine" / "scheduler_evidence_report.py").read_text(encoding="utf-8")
    for forbidden in (
        "INSERT INTO", "UPDATE ppl_", "DELETE FROM", "CREATE TABLE", "ALTER TABLE",
        "session.post", "requests.", "transition_run(", "transition_candidate(",
        ".shadow_action", "activation_thresholds] =", "minimum_observations] =",
    ):
        assert forbidden not in source


def test_cli_exposes_read_only_scheduler_evidence_report_action_without_post_authorization():
    import ppl_runner
    parser = ppl_runner._parser()
    args = parser.parse_args(["--scheduler-evidence-report", "--run-id", "run_0006"])
    assert args.scheduler_evidence_report is True
    assert args.allow_simulation_post is False
