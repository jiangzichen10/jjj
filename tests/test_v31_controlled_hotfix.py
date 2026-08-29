import copy
import hashlib
import io
import sqlite3
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_discovery import enqueue_discovery_refresh
from ppl_engine.continuous_policy import parse_continuous_policy
from ppl_engine.continuous_progress import (
    ContinuousProgressRenderer, build_continuous_progress_snapshot, format_continuous_progress,
)
from ppl_engine.round_orchestrator import (
    _apply_ready_continuous_discovery,
    _completed_research_streak,
    _round_policy_upgrade_compatible,
    _search_availability_read_only,
    load_round_policy,
)
from ppl_engine.round_store import create_round, ensure_round_schema, finish_batch, start_batch
from ppl_engine.scheduler_shadow import (
    QueueFacts, ResearchAvailabilityFacts, ShadowSchedulerSnapshot,
    choose_shadow_action, policy_from_mapping,
)
from ppl_engine.store import RunnerStore
from ppl_engine.strategy_contracts import SchedulerActionType


ROOT = Path(__file__).resolve().parents[1]


def _config(project_dir=ROOT):
    return load_effective_config(
        Path(project_dir) / "ppl_rules.yaml", Path(project_dir) / "ppl_plan_v31.yaml",
        project_dir=Path(project_dir),
    )


def _policy(name="ppl_round_v31_d2e.yaml"):
    return load_round_policy(ROOT / name, _config())


def _store(tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_test", cfg); ensure_round_schema(store)
    create_round(
        store, round_id="round_test", run_id="run_test", policy=_policy("ppl_round_v31.yaml"),
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    return store, cfg


def _available(count, slots, *, complete=True, reason="TEST"):
    return ResearchAvailabilityFacts(
        raw_backlog_count=count, selector_eligible_count=count, preview_safe_count=count,
        execution_eligible_count=count if complete else 0, evaluation_complete=complete, reason=reason,
        remote_slots_free=slots,
        immediately_dispatchable_count=min(count if complete else 0, slots),
    )


def test_fixed_pool_policy_is_generic_and_run7_policy_restores_expansion():
    d2e = _policy(); standard = _policy("ppl_round_v31.yaml")
    assert parse_continuous_policy(d2e).allow_search_pool_expansion is False
    assert d2e["rolling_discovery"]["enabled"] is False
    assert parse_continuous_policy(standard).allow_search_pool_expansion is True
    assert standard["rolling_discovery"]["enabled"] is True
    source = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    assert 'run_id == "run_0006"' not in source


def test_ready_discovery_is_suppressed_not_applied_and_is_not_reprocessed(tmp_path):
    store, cfg = _store(tmp_path)
    work = enqueue_discovery_refresh(
        store, "run_test", "round_test", refresh_no=1, batch_no=7, trigger="PERIODIC",
        excluded_dataset_ids=[], probe_count=1, admit_count=1,
    )
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_discovery_work SET queue_state='READY_APPLY',stage='FINALIZE' WHERE discovery_work_id=?",
            (work["discovery_work_id"],),
        )
        before = conn.execute("SELECT COUNT(*) FROM ppl_candidates WHERE run_id='run_test'").fetchone()[0]
    reports = _apply_ready_continuous_discovery(
        store, cfg, object(), ROOT / "alpha_results.db", "run_test", "round_test", _policy(),
    )
    with store.connect() as conn:
        row = dict(conn.execute("SELECT * FROM ppl_discovery_work WHERE discovery_work_id=?", (work["discovery_work_id"],)).fetchone())
        after = conn.execute("SELECT COUNT(*) FROM ppl_candidates WHERE run_id='run_test'").fetchone()[0]
    assert reports == [{"refresh_no": 1, "suppressed": True, "reason": "EXPANSION_DISABLED", "new_candidate_count": 0}]
    assert row["queue_state"] == "SUPPRESSED_EXPANSION_DISABLED"
    assert row["queue_state"] != "APPLIED" and row["last_error"] == "EXPANSION_DISABLED"
    assert before == after
    assert _apply_ready_continuous_discovery(
        store, cfg, object(), ROOT / "alpha_results.db", "run_test", "round_test", _policy(),
    ) == []


def test_exact_durable_policy_hotfix_upgrade_allows_only_approved_drift():
    current = _policy()
    stored = copy.deepcopy(current)
    stored["continuous"].pop("allow_search_pool_expansion")
    stored["rolling_discovery"]["enabled"] = True
    stored["scheduler_shadow"]["policy_version"] = "V31_SCHED_SHADOW_002"
    stored["scheduler_evidence"]["evidence_policy_version"] = "V31_SCHED_EVIDENCE_001"
    assert _round_policy_upgrade_compatible(stored, current) is True
    bad = copy.deepcopy(current); bad["batch_size"] += 1
    assert _round_policy_upgrade_compatible(stored, bad) is False


def test_completed_research_streak_uses_durable_terminal_set_and_phase_switch(tmp_path):
    store, _ = _store(tmp_path)
    for batch_no, status in enumerate(("COMPLETED", "RECOVERED", "RECOVERED_PRE_DISPATCH", "COMPLETED"), 1):
        start_batch(store, "round_test", batch_no, "SEARCH")
        finish_batch(store, "round_test", batch_no, {}, logical_posts_consumed=0, status=status)
    assert _completed_research_streak(store, "round_test") == (SchedulerActionType.SEARCH, 4)
    start_batch(store, "round_test", 5, "REPAIR")
    finish_batch(store, "round_test", 5, {}, logical_posts_consumed=0)
    assert _completed_research_streak(store, "round_test") == (SchedulerActionType.REPAIR, 1)


def test_search_availability_preview_is_read_only_and_uses_pure_selector(tmp_path):
    store, _ = _store(tmp_path)
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                candidate_id,run_id,expression,sim_key,dataset_id,field_id,field_type,semantic_class,
                direction,signal_family,transform_family,operator,window,vector_reducer,lifecycle_state,
                simulation_status,initial_selection_score,structure_status,data_field_count_estimate,
                pp_total_operator_count_estimate,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "search_1", "run_test", "ts_mean(f1,5)", "search_sk1", "d1", "f1", "MATRIX", "RETURN",
                "NORMAL", "search_family_1", "TS_MEAN", "ts_mean", 5, "IDENTITY", "PLANNED", "NONE",
                10.0, "ELIGIBLE", 1, 1, "2026-08-29T00:00:00+00:00", "2026-08-29T00:00:00+00:00",
            ),
        )
    before = hashlib.sha256(Path(store.path).read_bytes()).hexdigest()
    alpha_db = tmp_path / "alpha.db"
    with sqlite3.connect(alpha_db) as conn:
        conn.execute(
            "CREATE TABLE alpha_results(sim_key TEXT PRIMARY KEY,status TEXT,simulation_url TEXT,alpha_id TEXT)"
        )
    availability = _search_availability_read_only(
        store, alpha_db, "run_test", "round_test",
        _policy("ppl_round_v31.yaml"), 2,
    )
    after = hashlib.sha256(Path(store.path).read_bytes()).hexdigest()
    assert before == after
    assert availability.raw_backlog_count == 1
    assert availability.selector_eligible_count == 1
    assert availability.preview_safe_count == 1
    assert availability.execution_eligible_count == 1
    assert availability.immediately_dispatchable_count == 1


def test_shadow_uses_execution_eligibility_and_slots_separately_in_both_directions():
    policy = policy_from_mapping(_policy()["scheduler_shadow"])
    no_slot = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=10), repair_queue=QueueFacts(backlog=433),
        search_availability=_available(10, 0), repair_availability=_available(3, 0),
        remote_slot_limit=1, remote_slots_reserved=1,
        consecutive_action=SchedulerActionType.SEARCH, consecutive_count=4,
    )
    decision = choose_shadow_action(no_slot, policy)
    assert no_slot.repair_availability.execution_eligible_count == 3
    assert no_slot.repair_availability.immediately_dispatchable_count == 0
    assert decision.shadow_action is SchedulerActionType.WAIT

    repair_unproved = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=10), repair_queue=QueueFacts(backlog=433),
        search_availability=_available(10, 1),
        repair_availability=ResearchAvailabilityFacts(
            raw_backlog_count=433, evaluation_complete=True,
            reason="NO_EXECUTABLE_REPAIR_EVIDENCE", remote_slots_free=1,
        ),
        remote_slot_limit=1, remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.SEARCH, consecutive_count=4,
    )
    decision = choose_shadow_action(repair_unproved, policy)
    assert decision.shadow_action is SchedulerActionType.SEARCH
    assert decision.reason == "NO_EXECUTABLE_REPAIR_EVIDENCE"

    repair_available = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.SEARCH,
        search_queue=QueueFacts(backlog=10), repair_queue=QueueFacts(backlog=433),
        search_availability=_available(10, 1), repair_availability=_available(3, 1),
        remote_slot_limit=1, remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.SEARCH, consecutive_count=4,
    )
    assert choose_shadow_action(repair_available, policy).reason == "SHADOW_HARD_STARVATION_GUARD"

    search_unproved = ShadowSchedulerSnapshot(
        actual_action=SchedulerActionType.REPAIR,
        search_queue=QueueFacts(backlog=50), repair_queue=QueueFacts(backlog=3),
        search_availability=ResearchAvailabilityFacts(
            raw_backlog_count=50, evaluation_complete=True,
            reason="NO_EXECUTABLE_SEARCH_EVIDENCE", remote_slots_free=1,
        ),
        repair_availability=_available(3, 1), remote_slot_limit=1, remote_slots_reserved=0,
        consecutive_action=SchedulerActionType.REPAIR, consecutive_count=4,
    )
    assert choose_shadow_action(search_unproved, policy).shadow_action is SchedulerActionType.REPAIR


def test_progress_labels_do_not_call_raw_backlog_eligible_or_remaining_and_renderer_is_safe():
    snapshot = {
        "post": {"confirmed": 1, "uncertain": 2, "failed_or_rejected": 0, "active": 1, "slot_limit": 4},
        "alpha": {"submitted": 0, "running": 1, "complete": 2, "remote_not_found": 0, "pending": 1},
        "check": {"queued_pending": 1, "checking": 1, "resolved": 2, "wait_check": 0,
                  "wait_rate_limit_transient": 0},
        "research": {"current_batch": 7, "actual_phase": "SEARCH", "search_consumed": 10,
                     "repair_consumed": 0, "search_planned_backlog": 8,
                     "allow_search_pool_expansion": False},
        "search_availability": _available(8, 3).as_dict(),
        "repair_availability": _available(2, 3).as_dict(),
        "d2_matured": {"search": 4, "repair": 0},
    }
    line = format_continuous_progress(snapshot)
    assert "Search PLANNED backlog=8" in line
    assert "Search selector eligible=8" in line and "Search execution eligible=8" in line
    assert "Search Pool remaining" not in line
    stream = io.StringIO(); renderer = ContinuousProgressRenderer(stream)
    assert renderer.emit(snapshot) is True and renderer.emit(snapshot) is False
    assert renderer.emit_completed_results([{
        "candidate_id": "c", "alpha_id": "a", "sharpe": 1.2, "fitness": 1.1, "turnover": 0.5,
    }]) == 1
    assert "ALPHA COMPLETE" in stream.getvalue()


def test_progress_snapshot_is_read_only_and_renderer_failure_isolated(tmp_path):
    store, _ = _store(tmp_path)
    before = hashlib.sha256(Path(store.path).read_bytes()).hexdigest()
    snapshot = build_continuous_progress_snapshot(
        store, "run_test", "round_test", _policy("ppl_round_v31.yaml"), remote_slot_limit=4,
    )
    after = hashlib.sha256(Path(store.path).read_bytes()).hexdigest()
    assert before == after
    assert snapshot["research"]["search_planned_backlog"] == 0

    class BrokenStream:
        def write(self, _value):
            raise OSError("redirect closed")

        def flush(self):
            raise OSError("redirect closed")

    assert ContinuousProgressRenderer(BrokenStream()).emit(snapshot) is False
