"""Regression contract for v3.0.4m PPC_CONTROLLED_BRANCH.

The filename is retained so older patch overlays overwrite the former V1 tests.
All tests are offline and use only local/in-memory state.
"""

import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from ppl_engine import ppc_controlled_branch as pcb
from ppl_engine import round_orchestrator as ro
from ppl_engine.repair_engine import same_family_micro_tune_spec


class _MiniStore:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE ppl_repair_plans (
              repair_plan_id TEXT, run_id TEXT, parent_candidate_id TEXT, root_candidate_id TEXT,
              target_failure TEXT, repair_type TEXT, repair_signature TEXT, plan_status TEXT,
              consumed_posts INTEGER, blocked_reason TEXT, candidate_spec_json TEXT,
              created_at TEXT, updated_at TEXT
            );
            CREATE TABLE ppl_repairs (
              run_id TEXT, repair_signature TEXT, child_candidate_id TEXT,
              side_effect_verdict TEXT, before_json TEXT, after_json TEXT, delta_json TEXT
            );
            """
        )

    @contextmanager
    def connect(self):
        yield self.conn
        self.conn.commit()

    def add_plan(self, *, pid, parent, target=pcb.PPC_TARGET_FAILURE, status="EXECUTED",
                 signature=None, child=None, verdict=None, spec=None, root="root"):
        sig = signature or f"sig_{pid}"
        self.conn.execute(
            "INSERT INTO ppl_repair_plans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (pid, "r", parent, root, target, "TEST", sig, status, 1, None,
             json.dumps(spec or {}), pid, pid),
        )
        if child is not None:
            self.conn.execute(
                "INSERT INTO ppl_repairs VALUES (?,?,?,?,?,?,?)",
                ("r", sig, child, verdict, "{}", "{}", "{}"),
            )
        self.conn.commit()


class _NoIOStore:
    """Fails loudly if a secondary PPC warning accidentally enters PPC state I/O."""
    def connect(self):  # pragma: no cover - should never be called
        raise AssertionError("PPC state must not be queried for non-primary PPC")


def _config(**overrides):
    branch = dict(pcb.DEFAULT_PPC_BRANCH_CONFIG)
    branch.update(overrides)
    return SimpleNamespace(rules={"near_pass": {"ppc_controlled_branch": branch}})


def _snap(ppc, sharpe=2.1, fitness=1.1, classification="PPL_FIXED_REPAIRABLE", fixed=None):
    return {
        "ready": True,
        "ppc": ppc,
        "sharpe": sharpe,
        "fitness": fitness,
        "classification": {
            "classification": classification,
            "fixed_blockers": [{"failure": x} for x in ([pcb.PPC_TARGET_FAILURE] if fixed is None else fixed)],
            "ppc_strategy_clean_max": 0.50,
        },
    }


def test_configurable_thresholds_attempts_and_windows():
    cfg = pcb.ppc_branch_config(_config(max_attempts=4, meaningful_improvement_min=0.005,
                                         meaningful_worsening_min=0.008,
                                         same_family_windows=[2, 3, 5]).rules)
    assert cfg["max_attempts"] == 4
    assert cfg["meaningful_improvement_min"] == pytest.approx(0.005)
    assert cfg["meaningful_worsening_min"] == pytest.approx(0.008)
    assert cfg["same_family_windows"] == [2, 3, 5]


def test_resolve_effective_window_recovers_legacy_null_metadata():
    assert pcb.resolve_effective_window({"operator": "ts_mean", "window": None,
                                         "expression": "ts_mean(x, 2)"}) == 2
    assert pcb.resolve_effective_window({"operator": "ts_mean", "window": None,
                                         "expression": "ts_mean(vec_avg(x), 3)"}) == 3
    assert pcb.resolve_effective_window({"operator": "ts_mean", "window": 4,
                                         "expression": "ts_mean(x, 2)"}) == 4
    assert pcb.resolve_effective_window({"operator": "rank", "window": None,
                                         "expression": "rank(x)"}) is None


def test_same_family_micro_tune_changes_window_axis():
    parent = {
        "candidate_id": "c", "root_candidate_id": "root", "field_id": "x", "field_type": "MATRIX",
        "vector_reducer": "IDENTITY", "operator": "ts_mean", "window": 3, "direction": "NORMAL",
        "expression": "ts_mean(x, 3)", "repair_depth": 1, "sim_key": "k",
    }
    spec = same_family_micro_tune_spec(parent, pcb.PPC_TARGET_FAILURE, 4)
    assert spec["repair_type"] == "SAME_FAMILY_MICRO_TUNE"
    assert spec["window_override"] == 4
    assert spec["target_failure"] == pcb.PPC_TARGET_FAILURE
    assert spec["expression_preview"] != parent["expression"]


def test_attempts_count_only_evaluated_ppc_outcomes_and_ignore_turnover():
    store = _MiniStore()
    # Historical turnover on the same lineage must never spend PPC quota.
    store.add_plan(pid="turn", parent="anchor", target="TURNOVER_ABOVE_BASE_MAX",
                   child="turn_child", verdict="WORSE")
    # An EXECUTED PPC result without durable outcome holds evaluation but consumes 0 attempts.
    store.add_plan(pid="pending", parent="anchor", child="pending_child", verdict=None)
    state = pcb.ppc_branch_state(store, _config(max_attempts=3), "r", "anchor")
    assert state["attempts_used"] == 0
    assert state["evaluation_pending"] is True

    # Once the PPC outcome is durable it becomes exactly one strategy attempt.
    store.conn.execute(
        "UPDATE ppl_repairs SET side_effect_verdict='WORSE' WHERE run_id='r' AND repair_signature='sig_pending'"
    )
    store.conn.commit()
    state = pcb.ppc_branch_state(store, _config(max_attempts=3), "r", "anchor")
    assert state["attempts_used"] == 1
    assert state["attempts_remaining"] == 2
    assert state["best_candidate_id"] == "anchor"
    assert state["evaluation_pending"] is False


def test_improved_child_promotes_best_but_worse_child_does_not():
    store = _MiniStore()
    store.add_plan(pid="p1", parent="anchor", child="worse", verdict="WORSE")
    store.add_plan(pid="p2", parent="anchor", child="better", verdict="IMPROVED")
    state = pcb.ppc_branch_state(store, _config(max_attempts=3), "r", "anchor")
    assert state["attempts_used"] == 2
    assert state["best_candidate_id"] == "better"


def test_ppc_anchor_traverses_only_ppc_edges():
    store = _MiniStore()
    # The branch starts at a child produced by some older non-PPC repair.
    store.add_plan(pid="ppc1", parent="turnover_child", child="ppc_child", verdict="WORSE",
                   root="original_root")
    assert pcb.infer_ppc_branch_anchor(store, "r", "ppc_child") == "turnover_child"
    assert pcb.infer_ppc_branch_anchor(store, "r", "turnover_child") == "turnover_child"


def test_secondary_ppc_warning_does_not_hijack_primary_turnover():
    item = {
        "candidate_id": "c1",
        "primary_failure": "TURNOVER_ABOVE_BASE_MAX",
        "repair_drivers": [{"failure": pcb.PPC_TARGET_FAILURE}],
    }
    assert ro._ppc_controlled_ranked_pool(_NoIOStore(), _config(), "r", [item]) == [item]


def test_meaningful_improvement_threshold_is_config_driven(monkeypatch):
    snapshots = {
        "parent": _snap(0.5452, sharpe=2.2),
        "child": _snap(0.5449, sharpe=2.2),
    }
    monkeypatch.setattr(pcb, "_candidate_snapshot", lambda _s, _c, _a, _r, cid: snapshots[cid])
    out = pcb.evaluate_ppc_repair_outcome(object(), _config(meaningful_improvement_min=0.01),
                                          None, "r", "parent", "child")
    assert out["verdict"] == "NO_MEANINGFUL_CHANGE"
    out = pcb.evaluate_ppc_repair_outcome(object(), _config(meaningful_improvement_min=0.0001),
                                          None, "r", "parent", "child")
    assert out["verdict"] == "IMPROVED"


def test_new_fixed_blocker_rejects_child_even_when_ppc_improves(monkeypatch):
    snapshots = {
        "parent": _snap(0.545, fixed=[pcb.PPC_TARGET_FAILURE]),
        "child": _snap(0.520, fixed=[pcb.PPC_TARGET_FAILURE, "LOW_FITNESS"]),
    }
    monkeypatch.setattr(pcb, "_candidate_snapshot", lambda _s, _c, _a, _r, cid: snapshots[cid])
    out = pcb.evaluate_ppc_repair_outcome(object(), _config(), None, "r", "parent", "child")
    assert out["verdict"] == "REJECT_SIDE_EFFECT"
    assert "LOW_FITNESS" in out["new_fixed_failures"]


def test_target_pass_wins_when_ppc_clean_and_no_new_blocker(monkeypatch):
    snapshots = {
        "parent": _snap(0.520, fixed=[pcb.PPC_TARGET_FAILURE]),
        "child": _snap(0.490, classification="PPL_READY_FOR_MANUAL_FINALIZATION", fixed=[]),
    }
    monkeypatch.setattr(pcb, "_candidate_snapshot", lambda _s, _c, _a, _r, cid: snapshots[cid])
    out = pcb.evaluate_ppc_repair_outcome(object(), _config(), None, "r", "parent", "child")
    assert out["verdict"] == "TARGET_PASS"


def test_high_ppc_child_is_worse_and_does_not_promote_best(monkeypatch):
    snapshots = {
        "parent": _snap(0.545, sharpe=2.2),
        "child": _snap(0.700, sharpe=2.1, classification="PPL_STRATEGY_REJECT_HIGH_PPC"),
    }
    monkeypatch.setattr(pcb, "_candidate_snapshot", lambda _s, _c, _a, _r, cid: snapshots[cid])
    out = pcb.evaluate_ppc_repair_outcome(object(), _config(), None, "r", "parent", "child")
    assert out["verdict"] == "WORSE"

    store = _MiniStore()
    store.add_plan(pid="p1", parent="anchor", child="child", verdict="WORSE")
    state = pcb.ppc_branch_state(store, _config(max_attempts=3), "r", "anchor")
    assert state["best_candidate_id"] == "anchor"
    assert state["attempts_used"] == 1
    assert state["attempts_remaining"] == 2


def test_pending_ppc_evaluation_locks_branch_selection():
    store = _MiniStore()
    store.add_plan(pid="pending", parent="anchor", child="pending_child", verdict=None)
    item = {
        "candidate_id": "anchor",
        "primary_failure": pcb.PPC_TARGET_FAILURE,
        "classification": "PPL_FIXED_REPAIRABLE",
        "repair_priority": "HIGH",
    }
    assert ro._ppc_controlled_ranked_pool(store, _config(), "r", [item]) == []


def test_dispatched_ppc_remote_execution_locks_branch_before_result_exists():
    store = _MiniStore()
    store.add_plan(pid="inflight", parent="anchor", status="DISPATCHED",
                   child="pending_child", verdict=None)
    state = pcb.ppc_branch_state(store, _config(max_attempts=3), "r", "anchor")
    assert state["attempts_used"] == 0
    assert state["evaluation_pending"] is True
    assert state["pending_plan_ids"] == ["inflight"]

    item = {
        "candidate_id": "anchor",
        "primary_failure": pcb.PPC_TARGET_FAILURE,
        "classification": "PPL_FIXED_REPAIRABLE",
        "repair_priority": "HIGH",
    }
    assert ro._ppc_controlled_ranked_pool(store, _config(), "r", [item]) == []


def test_max_attempts_is_branch_scoped_and_configurable():
    store = _MiniStore()
    store.add_plan(pid="p1", parent="anchor", child="c1", verdict="WORSE")
    store.add_plan(pid="p2", parent="anchor", child="c2", verdict="NO_MEANINGFUL_CHANGE")
    state2 = pcb.ppc_branch_state(store, _config(max_attempts=2), "r", "anchor")
    assert state2["attempts_used"] == 2
    assert state2["exhausted"] is True
    assert state2["attempts_remaining"] == 0
    state4 = pcb.ppc_branch_state(store, _config(max_attempts=4), "r", "anchor")
    assert state4["exhausted"] is False
    assert state4["attempts_remaining"] == 2


def test_policy_revision_uses_project_release_not_feature_v1_label():
    assert pcb.PPC_POLICY_NAME == "PPC_CONTROLLED_BRANCH"
    assert pcb.PPC_POLICY_VERSION == "v3.0.4m"
