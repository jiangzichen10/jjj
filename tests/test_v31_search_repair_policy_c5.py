import copy
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.policy_specs import (
    SEARCH_POLICY_SCHEMA, REPAIR_POLICY_SCHEMA,
    effective_repair_allocation, effective_repair_planning, effective_repair_ranking,
    effective_search_allocation, effective_search_ranking,
    search_policy_hash, repair_policy_hash,
)
from ppl_engine.repair_strategy import repair_value
from ppl_engine.round_orchestrator import load_round_policy
from ppl_engine.search_strategy import select_search_candidates

ROOT = Path(__file__).resolve().parents[1]


def _cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _policy():
    return load_round_policy(ROOT / "ppl_round_v31.yaml", _cfg())


def test_c5_explicit_search_repair_policy_sections_are_installed_with_legacy_parity():
    p = _policy()
    assert p["search_policy"]["schema"] == SEARCH_POLICY_SCHEMA
    assert p["repair_policy"]["schema"] == REPAIR_POLICY_SCHEMA
    assert effective_search_allocation(p) == {"batch_size": 40, "exploration_fraction": 0.3}
    assert effective_repair_allocation(p) == {"batch_size": 40}
    assert effective_repair_planning(p) == {
        "normal_near_pass_repair_cap_per_family": 1,
        "strong_near_pass_repair_cap_per_family": 2,
    }
    assert effective_search_ranking(p)["search_positive_sharpe_min"] == p["adaptive_ranking"]["search_positive_sharpe_min"]
    assert effective_repair_ranking(p)["repair_good_sharpe_min"] == p["adaptive_ranking"]["repair_good_sharpe_min"]
    assert len(search_policy_hash(p)) == 64
    assert len(repair_policy_hash(p)) == 64


def test_c5_search_strategy_prefers_dedicated_policy_over_legacy_aliases():
    p = _policy()
    changed = copy.deepcopy(p)
    changed["batch_size"] = 40  # historical alias intentionally unchanged
    changed["search_policy"]["allocation"]["batch_size"] = 2
    changed["search_policy"]["allocation"]["exploration_fraction"] = 0.0
    # This test isolates allocation precedence. Dedicated diversity is also
    # authoritative, so neutralize it here rather than weakening production
    # diversity enforcement.
    changed["search_policy"]["diversity"]["max_dataset_fraction"] = 1.0
    changed["search_policy"]["diversity"]["max_semantic_class_fraction"] = 1.0
    rows = []
    for i in range(5):
        rows.append({
            "candidate_id": f"c{i}", "dataset_id": f"d{i}", "field_id": f"f{i}",
            "semantic_class": "PRICE", "operator": "raw",
            "_strategy_family_id": f"fam{i}", "_strategy_requires_new_post": True,
            "round_cache_action": "NEW_SIMULATION_REQUIRED",
            "round_exploit_score": float(10-i), "round_explore_score": float(10-i),
            "round_exploit_eligible": True,
        })
    out = select_search_candidates(
        rows, protected_families=(), active_datasets=tuple(f"d{i}" for i in range(5)),
        attempted_families=(), initial_rules={"max_dataset_fraction": 1.0, "max_semantic_class_fraction": 1.0, "max_initial_candidates_per_field": 4},
        policy=changed, remaining=10, extension_batch_cap=10,
    )
    assert out.batch_size == 2
    assert len(out.selected) == 2
    assert all(x["round_selection_mode"] in {"EXPLOIT", "BACKFILL"} for x in out.selected)


def test_c5_repair_strategy_prefers_dedicated_policy_over_legacy_aliases():
    p = _policy()
    changed = copy.deepcopy(p)
    changed["adaptive_ranking"]["repair_good_sharpe_min"] = 2.0  # legacy alias unchanged
    changed["repair_policy"]["ranking"]["repair_good_sharpe_min"] = 2.8
    assert repair_value({"sharpe": 2.4, "turnover": 0.75}, changed)["band"] == "ORDINARY_REPAIR"
    assert repair_value({"sharpe": 3.2, "turnover": 0.75}, changed)["band"] == "ELITE_REPAIR"


def test_c5_continuous_phase_capacity_uses_dedicated_per_phase_policy():
    from ppl_engine.continuous_runtime import phase_capacity

    p = _policy()
    changed = copy.deepcopy(p)
    changed["batch_size"] = 40  # legacy audit alias remains unchanged
    changed["search_policy"]["allocation"]["batch_size"] = 7
    changed["repair_policy"]["allocation"]["batch_size"] = 3
    row = {
        "batch_size": 40,
        "search_budget": 1600, "repair_budget": 400,
        "search_consumed": 1600, "repair_consumed": 400,
    }
    assert phase_capacity(changed, row, "SEARCH").capacity == 7
    assert phase_capacity(changed, row, "REPAIR").capacity == 3


def test_c5_batch_snapshot_labels_legacy_aliases_and_reports_effective_policies():
    """A dedicated Search/Repair reload must not leave reports showing legacy aliases as active policy."""
    text = (ROOT / "ppl_engine" / "round_orchestrator.py").read_text(encoding="utf-8")
    assert '"legacy_aliases": {' in text
    assert '"effective_search_policy": {' in text
    assert '"effective_repair_policy": {' in text
    assert '"policy_hash": search_policy_hash(policy)' in text
    assert '"policy_hash": repair_policy_hash(policy)' in text
