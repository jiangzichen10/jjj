"""
Regression test:
--reconcile must use ppl_plan_v3.yaml rather than ppl_plan.yaml.

Protects against execution hash drift caused by loading the wrong
plan for an existing V3 round.
"""

from pathlib import Path

from ppl_engine.config import load_effective_config


ROOT = Path(__file__).resolve().parents[1]


def test_reconcile_uses_v3_round_plan():
    rules = ROOT / "ppl_rules.yaml"

    normal = load_effective_config(
        rules,
        ROOT / "ppl_plan.yaml",
        project_dir=ROOT,
    )

    v3 = load_effective_config(
        rules,
        ROOT / "ppl_plan_v3.yaml",
        project_dir=ROOT,
    )

    assert normal.plan["selection"]["max_datasets"] != v3.plan["selection"]["max_datasets"]
    assert normal.plan["selection"]["max_fields_per_dataset"] != v3.plan["selection"]["max_fields_per_dataset"]

    assert (
        normal.plan["budgets"]["max_repair_rounds"]
        != v3.plan["budgets"]["max_repair_rounds"]
    )

    assert v3.plan["selection"]["max_datasets"] == 15
    assert v3.plan["selection"]["max_fields_per_dataset"] == 50
    assert v3.plan["budgets"]["max_repair_rounds"] == 400
