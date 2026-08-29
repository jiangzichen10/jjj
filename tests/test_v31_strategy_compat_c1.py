from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.round_orchestrator import load_round_policy
from ppl_engine.strategy_compat import (
    REPAIR_COMPAT_STRATEGY,
    SEARCH_COMPAT_STRATEGY,
    SCHEDULER_COMPAT_MODE,
    LegacyRepairStrategyAdapter,
    LegacySearchStrategyAdapter,
    choose_compatibility_strategy_action,
    policy_versions_from_policy,
    repair_decisions_from_selected_plans,
    search_decisions_from_selected_rows,
)
from ppl_engine.strategy_contracts import ResearchContext, SchedulerActionType


ROOT = Path(__file__).resolve().parents[1]


def _policy():
    cfg = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)
    return load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)


def test_c1_policy_explicitly_enables_phase_compatibility_integration():
    policy = _policy()
    integration = policy["strategy_integration"]
    assert integration == {
        "enabled": True,
        "mode": SCHEDULER_COMPAT_MODE,
        "search_adapter": SEARCH_COMPAT_STRATEGY,
        "repair_adapter": REPAIR_COMPAT_STRATEGY,
        "scheduler_gate": "V31_SCHEDULER_CONTRACT",
    }


def test_c1_search_adapter_is_pure_and_preserves_selected_order():
    policy = _policy()
    rows = [
        {
            "candidate_id": "c2", "sim_key": "sk2", "round_selection_mode": "EXPLORE",
            "round_cache_action": "NEW_SIMULATION_REQUIRED", "round_explore_score": 8.5,
        },
        {
            "candidate_id": "c1", "sim_key": "sk1", "round_selection_mode": "FREE_CACHE",
            "round_cache_action": "CACHE_RESTORE", "round_adaptive_score": 99.0,
        },
    ]
    decisions = search_decisions_from_selected_rows("run_0006", rows, policy)
    assert [x.candidate_id for x in decisions] == ["c2", "c1"]
    assert [x.strategy for x in decisions] == [SEARCH_COMPAT_STRATEGY, SEARCH_COMPAT_STRATEGY]
    assert all(x.policy_version == policy["policy_versions"]["search"] for x in decisions)
    assert decisions[0].metadata["requires_new_remote_slot"] is True
    assert decisions[1].metadata["requires_new_remote_slot"] is False

    # Contract-level adapter input contains immutable facts only; no Store/session is needed.
    ctx = ResearchContext(run_id="run_0006", candidate_facts=tuple(rows))
    direct = LegacySearchStrategyAdapter("S_TEST").propose(ctx)
    assert [x.candidate_id for x in direct] == ["c2", "c1"]
    assert all(x.policy_version == "S_TEST" for x in direct)


def test_c1_repair_adapter_preserves_plan_order_and_identity():
    policy = _policy()
    plans = [
        {"repair_plan_id": "rp_b", "parent_candidate_id": "p2", "repair_type": "PPC_CONTROLLED"},
        {"repair_plan_id": "rp_a", "parent_candidate_id": "p1", "repair_type": "TURNOVER_STAGE"},
    ]
    decisions = repair_decisions_from_selected_plans("run_0006", plans, policy)
    assert [x.metadata["repair_plan_id"] for x in decisions] == ["rp_b", "rp_a"]
    assert [x.candidate_id for x in decisions] == ["p2", "p1"]
    assert all(x.action == "EXECUTE_REPAIR_PLAN" for x in decisions)
    assert all(x.strategy == REPAIR_COMPAT_STRATEGY for x in decisions)
    assert all(x.policy_version == policy["policy_versions"]["repair"] for x in decisions)

    ctx = ResearchContext(run_id="run_0006", repair_history=tuple(plans))
    direct = LegacyRepairStrategyAdapter("R_TEST").propose(ctx)
    assert [x.metadata["repair_plan_id"] for x in direct] == ["rp_b", "rp_a"]
    assert all(x.policy_version == "R_TEST" for x in direct)


def test_c1_scheduler_is_real_execution_gate_for_declarative_search_and_repair():
    policy = _policy()
    search = search_decisions_from_selected_rows(
        "run_0006",
        [{"candidate_id": "c1", "round_selection_mode": "EXPLOIT", "round_cache_action": "NEW_SIMULATION_REQUIRED", "round_exploit_score": 3.0}],
        policy,
    )
    repair = repair_decisions_from_selected_plans(
        "run_0006",
        [{"repair_plan_id": "rp1", "parent_candidate_id": "p1", "repair_type": "PPC", "compat_selection_score": 7.0}],
        policy,
    )
    assert choose_compatibility_strategy_action(search_decisions=search).action is SchedulerActionType.SEARCH
    assert choose_compatibility_strategy_action(repair_decisions=repair).action is SchedulerActionType.REPAIR
    # C1 does not yet let remote-slot policy alter proven selector behavior; D will enable it.
    assert choose_compatibility_strategy_action(
        search_decisions=search, remote_slot_limit=4, remote_slots_reserved=4, enforce_remote_slots=False,
    ).action is SchedulerActionType.SEARCH
    assert choose_compatibility_strategy_action(
        search_decisions=search, remote_slot_limit=4, remote_slots_reserved=4, enforce_remote_slots=True,
    ).action is SchedulerActionType.WAIT


def test_c1_policy_versions_are_resolved_without_inventing_business_rules():
    versions = policy_versions_from_policy(_policy())
    assert versions.search == "V31_SEARCH_COMPAT_001"
    assert versions.repair == "V3_REPAIR_003"
    assert versions.scheduler == "V31_SCHED_001"
    assert versions.qualification == "V31_QUAL_COMPAT_002"
    # The hypothetical example from planning must not appear as a real rule.
    text = (ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8")
    assert "TWO_YEAR_SHARPE_MIN" not in text
    assert "value: 1.58" not in text


def test_c1_search_selection_persists_policy_version_attribution(tmp_path):
    import json
    import sqlite3

    from ppl_engine.round_orchestrator import _select_search_batch
    from ppl_engine.round_store import create_round, ensure_round_schema
    from ppl_engine.store import RunnerStore

    cfg = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", cfg)
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    ensure_round_schema(store)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
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
                "NORMAL", "d1/f1/IDENTITY/NORMAL/TS_MEAN", "TS_MEAN", "ts_mean", 5, "IDENTITY",
                "PLANNED", "NONE", 10.0, "ELIGIBLE", 1, 1, "now", "now",
            ),
        )

    alpha_db = tmp_path / "alpha.db"
    con = sqlite3.connect(alpha_db)
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
    con.commit(); con.close()

    rows = _select_search_batch(
        store, alpha_db, "run_0006", "round_run_0006", policy, 40,
        batch_no=1, skip_uncertain=True,
    )
    assert [x["candidate_id"] for x in rows] == ["c1"]
    with store.connect() as con:
        raw = con.execute(
            "SELECT context_json FROM ppl_round_candidate_decisions WHERE round_id=? AND batch_no=1 AND candidate_id='c1'",
            ("round_run_0006",),
        ).fetchone()[0]
    context = json.loads(raw)
    assert context["strategy_adapter"] == SEARCH_COMPAT_STRATEGY
    assert context["search_policy_version"] == policy["policy_versions"]["search"]
    from ppl_engine.policy_specs import search_policy_hash
    assert context["search_policy_hash"] == search_policy_hash(policy)
    assert context["scheduler_policy_version"] == policy["policy_versions"]["scheduler"]
