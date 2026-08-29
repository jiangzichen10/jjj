import json
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.research_telemetry import load_events
from ppl_engine.round_orchestrator import _scheduler_shadow_observation, load_round_policy
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.scheduler_shadow import (
    SHADOW_ONLY_MODE,
    QueueFacts,
    ResearchAvailabilityFacts,
    ShadowSchedulerSnapshot,
    choose_shadow_action,
    policy_from_mapping,
    productivity_for_window,
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


def test_d1_policy_is_explicit_shadow_only_and_does_not_replace_authoritative_scheduler_version():
    policy = _policy()
    shadow = policy["scheduler_shadow"]
    assert shadow["enabled"] is True
    assert shadow["mode"] == SHADOW_ONLY_MODE
    assert shadow["policy_version"] == "V31_SCHED_SHADOW_003"
    assert shadow["productivity_windows"] == [100, 500]
    # D1 is an observation policy, not the authoritative scheduler identity.
    assert policy["policy_versions"]["scheduler"] == "V31_SCHED_001"
    assert policy["strategy_integration"]["mode"] == "PHASE_COMPATIBILITY"


def test_d1_search_productivity_uses_new_post_ledger_and_marks_near_pass_as_proxy():
    ledger = [
        {
            "logical_sequence_no": 1, "phase": "SEARCH", "origin": "NEW_POST",
            "sim_key": "s1", "simulation_status": "COMPLETE", "family_id": "f1",
            "classification": "FIXED_REPAIRABLE", "pretag_status": "PENDING", "local_gate": "PASS",
        },
        {
            "logical_sequence_no": 2, "phase": "SEARCH", "origin": "NEW_POST",
            "sim_key": "s2", "simulation_status": "COMPLETE", "family_id": "f2",
            "classification": "READY_FOR_MANUAL_FINALIZATION", "pretag_status": "PASS", "local_gate": "PASS",
        },
        # CACHE rows must not distort paid productivity.
        {
            "logical_sequence_no": 3, "phase": "SEARCH", "origin": "CACHE",
            "sim_key": "s3", "simulation_status": "COMPLETE", "family_id": "f3",
            "classification": "READY_FOR_MANUAL_FINALIZATION", "pretag_status": "PASS", "local_gate": "PASS",
        },
    ]
    m = productivity_for_window(ledger, SchedulerActionType.SEARCH, 100)
    assert m.attempts == 2
    assert m.completed == 2
    assert m.ready == 1
    assert m.pretag_pass == 1
    assert m.local_pass == 2
    assert m.near_pass_proxy == 2
    assert m.distinct_families == 2
    assert m.score > 0


def test_d1_shadow_can_disagree_with_actual_action_without_becoming_authoritative():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    search_ledger = [
        {
            "logical_sequence_no": i, "phase": "SEARCH", "origin": "NEW_POST",
            "sim_key": f"s{i}", "simulation_status": "COMPLETE", "family_id": f"sf{i}",
            "classification": "TERMINAL_FAIL", "pretag_status": "FAILED", "local_gate": "FAIL",
        }
        for i in range(1, 31)
    ]
    repair_ledger = [
        {
            "logical_sequence_no": 100 + i, "phase": "REPAIR", "origin": "NEW_POST",
            "sim_key": f"r{i}", "simulation_status": "COMPLETE", "family_id": f"rf{i}",
            "repair_verdict": "TARGET_PASS",
        }
        for i in range(1, 31)
    ]
    from ppl_engine.scheduler_shadow import productivity_windows

    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=10, oldest_age_seconds=60),
        repair_queue=QueueFacts(backlog=10, oldest_age_seconds=60),
        search_availability=_available(10), repair_availability=_available(10),
        search_productivity=productivity_windows(search_ledger, SchedulerActionType.SEARCH, policy.productivity_windows),
        repair_productivity=productivity_windows(repair_ledger, SchedulerActionType.REPAIR, policy.productivity_windows),
        remote_slot_limit=4,
        remote_slots_reserved=0,
    )
    decision = choose_shadow_action(snap, policy)
    assert decision.shadow_action is SchedulerActionType.REPAIR
    assert decision.actual_action is SchedulerActionType.SEARCH
    assert decision.agreement is False
    assert decision.authoritative is False
    assert decision.execution_action_unchanged is True


def test_d1_fairness_and_slot_pressure_are_observational_inputs():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    snap = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=5, oldest_age_seconds=0),
        repair_queue=QueueFacts(backlog=5, oldest_age_seconds=3600),
        search_availability=_available(5, 0), repair_availability=_available(5, 0),
        remote_slot_limit=4,
        remote_slots_reserved=4,
        consecutive_action=SchedulerActionType.SEARCH,
        consecutive_count=policy.max_consecutive_same_action,
    )
    decision = choose_shadow_action(snap, policy)
    assert decision.shadow_action is SchedulerActionType.WAIT
    assert decision.actual_action is SchedulerActionType.SEARCH
    assert decision.authoritative is False
    assert decision.execution_action_unchanged is True


def test_d1_orchestrator_observation_is_durable_idempotent_and_keeps_selection_identity(tmp_path):
    cfg = _config()
    policy = _policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store,
        round_id="round_run_0006",
        run_id="run_0006",
        policy=policy,
        total_budget=2000,
        search_budget=1600,
        repair_budget=400,
    )
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_candidates(
                candidate_id,run_id,expression,sim_key,dataset_id,field_id,field_type,semantic_class,
                direction,signal_family,transform_family,operator,window,vector_reducer,lifecycle_state,
                simulation_status,initial_selection_score,structure_status,data_field_count_estimate,
                pp_total_operator_count_estimate,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "c1", "run_0006", "ts_mean(f1,5)", "sk1", "d1", "f1", "MATRIX", "RETURN",
                "NORMAL", "family_1", "TS_MEAN", "ts_mean", 5, "IDENTITY", "PLANNED", "NONE",
                10.0, "ELIGIBLE", 1, 1, "2026-08-28T00:00:00+00:00", "2026-08-28T00:00:00+00:00",
            ),
        )
        con.execute(
            """INSERT INTO ppl_round_simulation_ledger(
                round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,family_id,sim_key,
                origin,simulation_status,classification,details_json,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "round_run_0006", "run_0006", 1, 1, "SEARCH", "old1", "f_old", "sk_old",
                "NEW_POST", "COMPLETE", "FIXED_REPAIRABLE", "{}",
                "2026-08-28T00:00:00+00:00", "2026-08-28T00:00:00+00:00",
            ),
        )

    first = _scheduler_shadow_observation(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=2, actual_action=SchedulerActionType.SEARCH, selected_count=1,
    )
    second = _scheduler_shadow_observation(
        store, cfg, "run_0006", "round_run_0006", policy,
        batch_no=2, actual_action=SchedulerActionType.SEARCH, selected_count=1,
    )
    assert first["actual_action"] == "SEARCH"
    assert second["actual_action"] == "SEARCH"
    assert first["authoritative"] is False
    assert first["execution_action_unchanged"] is True
    assert first["selection_identity_unchanged"] is True
    assert first["shadow_policy_version"] == "V31_SCHED_SHADOW_003"

    events = [e for e in load_events(store, "round_run_0006") if e["event_type"] == "SCHEDULER_SHADOW_DECISION"]
    assert len(events) == 1
    payload = json.loads(events[0]["payload_json"])
    assert payload["actual_action"] == "SEARCH"
    assert payload["authoritative"] is False
    assert payload["selection_identity_unchanged"] is True


def test_d1_orchestrator_never_branches_on_shadow_action():
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    # D1 may persist the shadow payload, but the orchestrator must not inspect
    # the recommendation to select/replace executable work.
    assert ".shadow_action" not in source
    assert "SCHEDULER_SHADOW_DECISION" in source
    assert "strategy_integration" in source
