import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import yaml

from ppl_engine.config import load_effective_config, simulation_budget_allocation
from ppl_engine.discovery import DiscoveryResult, classify_dataset_hint, discover_online
from ppl_engine.dry_run import estimate_candidate_plan


PROJECT = Path(__file__).resolve().parents[1]
RULES = PROJECT / "ppl_rules.yaml"
PLAN = PROJECT / "ppl_plan.yaml"


def config_with(*, selection=None, preselection=None, budgets=None):
    base = load_effective_config(RULES, PLAN, project_dir=PROJECT)
    rules = copy.deepcopy(base.rules)
    plan = copy.deepcopy(base.plan)
    if selection:
        plan["selection"].update(selection)
    if preselection:
        rules["heuristics"]["dataset_preselection"].update(preselection)
    if budgets:
        plan["budgets"].update(budgets)
    return SimpleNamespace(rules=rules, plan=plan, execution_hash=base.execution_hash)


def dataset(dataset_id, description="", category="Other", coverage=1.0, value_score=5.0):
    return {
        "id": dataset_id,
        "name": dataset_id.replace("_", " "),
        "description": description,
        "category": {"id": category.lower(), "name": category},
        "subcategory": {"id": "sub", "name": "Sub"},
        "coverage": coverage,
        "valueScore": value_score,
        "userCount": 20,
        "alphaCount": 20,
    }


def api_field(dataset_id, field_id, description, coverage=0.95, field_type="MATRIX"):
    return {
        "id": field_id,
        "description": description,
        "type": field_type,
        "coverage": coverage,
        "dateCoverage": 1.0,
        "dataset": {"id": dataset_id},
        "category": {"id": "market", "name": "Market"},
        "subcategory": {"id": "sub", "name": "Sub"},
        "themes": [],
        "userCount": 1,
        "alphaCount": 1,
    }


class FakeMachine:
    def __init__(self, datasets, fields):
        self.datasets = datasets
        self.fields = fields
        self.field_calls = []

    def get_datasets(self, session, **kwargs):
        return pd.DataFrame(self.datasets)

    def get_datafields(self, session, dataset_id, **kwargs):
        self.field_calls.append(dataset_id)
        return pd.DataFrame(self.fields.get(dataset_id, []))


def run_discovery(datasets, fields, config):
    fake = FakeMachine(datasets, fields)
    return discover_online(object(), config, fake), fake


def planned_field(dataset_id, index):
    return {
        "dataset_id": dataset_id,
        "field_id": f"{dataset_id}_{index}",
        "field_type": "MATRIX",
        "semantic_class": "RETURN",
        "classification_confidence": "HIGH",
        "classification_warning": None,
        "coverage": 0.95,
        "coverage_pass": True,
        "selected": True,
    }


class Phase21Tests(unittest.TestCase):
    def test_01_explicit_dataset_ids_are_not_overridden(self):
        cfg = config_with(selection={"dataset_ids": ["fund_ds"], "max_datasets": 1})
        result, fake = run_discovery(
            [dataset("fund_ds", "fundamental statements", "Fundamental"), dataset("intraday_ds", "intraday prices")],
            {"fund_ds": [api_field("fund_ds", "cash", "cash flow")]}, cfg,
        )
        self.assertEqual([x["dataset_id"] for x in result.datasets if x["selected"]], ["fund_ds"])
        self.assertEqual(fake.field_calls, ["fund_ds"])
        self.assertFalse(result.snapshot["automatic_preselection"])

    def test_02_empty_dataset_ids_enable_automatic_preselection(self):
        result, _ = run_discovery(
            [dataset("fund_ds", "fundamental statements", "Fundamental"), dataset("intraday_ds", "intraday prices")],
            {"fund_ds": [], "intraday_ds": []}, config_with(selection={"dataset_ids": [], "max_datasets": 1}),
        )
        self.assertTrue(result.snapshot["automatic_preselection"])
        self.assertEqual([x["dataset_id"] for x in result.datasets if x["selected"]], ["intraday_ds"])

    def test_03_dataset_id_is_only_final_tie_break(self):
        result, _ = run_discovery(
            [dataset("b_same"), dataset("a_same")], {"a_same": [], "b_same": []},
            config_with(selection={"max_datasets": 1}),
        )
        records = {x["dataset_id"]: x for x in result.datasets}
        self.assertEqual(records["a_same"]["dataset_preselection_score"], records["b_same"]["dataset_preselection_score"])
        self.assertTrue(records["a_same"]["selected"])

    def test_04_dataset_semantic_hint_intraday(self):
        cfg = config_with()
        hint = classify_dataset_hint(dataset("x", "Intraday minute bars"), cfg.rules["heuristics"]["dataset_preselection"])
        self.assertEqual(hint["dataset_semantic_hint"], "INTRADAY")

    def test_05_hint_source_confidence_and_match_are_saved(self):
        cfg = config_with()
        hint = classify_dataset_hint(dataset("x", "Order book microstructure"), cfg.rules["heuristics"]["dataset_preselection"])
        self.assertEqual((hint["dataset_hint_source"], hint["dataset_hint_confidence"]), ("DESCRIPTION", "LOW"))
        self.assertTrue(hint["dataset_hint_matched_text"])

    def test_06_fundamental_is_downweighted_not_banned(self):
        result, fake = run_discovery(
            [dataset("fundamental_ds", "fundamental statements", "Fundamental"), dataset("intraday_ds", "intraday prices")],
            {"fundamental_ds": [], "intraday_ds": []},
            config_with(selection={"max_datasets": 2}, preselection={"max_dataset_discovery_pool": 2}),
        )
        records = {x["dataset_id"]: x for x in result.datasets}
        self.assertIn("fundamental_ds", fake.field_calls)
        self.assertLess(records["fundamental_ds"]["dataset_preselection_score"], records["intraday_ds"]["dataset_preselection_score"])

    def test_07_unknown_retains_pool_opportunity(self):
        result, fake = run_discovery(
            [dataset("unknown_ds"), dataset("intraday_ds", "intraday prices")],
            {"unknown_ds": [], "intraday_ds": []},
            config_with(selection={"max_datasets": 2}, preselection={"max_dataset_discovery_pool": 2}),
        )
        self.assertIn("unknown_ds", fake.field_calls)
        self.assertEqual(next(x for x in result.datasets if x["dataset_id"] == "unknown_ds")["dataset_semantic_hint"], "UNKNOWN")

    def test_08_field_evidence_can_change_final_ranking(self):
        cfg = config_with(selection={"max_datasets": 1}, preselection={"max_dataset_discovery_pool": 2})
        result, _ = run_discovery(
            [dataset("a_intraday", "intraday prices"), dataset("b_price_volume", "price volume market data")],
            {
                "a_intraday": [api_field("a_intraday", f"cash_{i}", "cash flow") for i in range(10)],
                "b_price_volume": [api_field("b_price_volume", f"flow_{i}", "order flow") for i in range(10)],
            }, cfg,
        )
        self.assertEqual([x["dataset_id"] for x in result.datasets if x["selected"]], ["b_price_volume"])

    def test_09_stage_a_discovery_pool_is_limited(self):
        datasets = [dataset(f"ds{i}", "intraday prices") for i in range(5)]
        result, fake = run_discovery(datasets, {}, config_with(preselection={"max_dataset_discovery_pool": 2}))
        self.assertEqual(result.snapshot["discovery_pool_size"], 2)
        self.assertEqual(len(fake.field_calls), 2)

    def test_10_does_not_scan_all_139_dataset_fields(self):
        datasets = [dataset(f"ds{i:03d}", "intraday prices") for i in range(139)]
        _, fake = run_discovery(datasets, {}, config_with(preselection={"max_dataset_discovery_pool": 15}))
        self.assertEqual(len(fake.field_calls), 15)

    def test_11_budget_fractions_are_60_40(self):
        allocation = simulation_budget_allocation(config_with().plan)
        self.assertEqual((allocation["initial_search_budget"], allocation["repair_reserve_budget"]), (72, 48))

    def test_12_budget_120_equals_72_plus_48(self):
        allocation = simulation_budget_allocation(config_with().plan)
        self.assertEqual(allocation["initial_search_budget"] + allocation["repair_reserve_budget"], 120)

    def test_13_live_validation_10_equals_6_plus_4(self):
        plan = yaml.safe_load(PLAN.read_text(encoding="utf-8"))
        plan["run_profile"] = "LIVE_VALIDATION"
        plan["budgets"]["max_new_simulation_posts"] = 120
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.yaml"
            path.write_text(yaml.safe_dump(plan), encoding="utf-8")
            config = load_effective_config(RULES, path, project_dir=PROJECT)
        allocation = simulation_budget_allocation(config.plan)
        self.assertEqual((allocation["max_new_simulation_posts"], allocation["initial_search_budget"], allocation["repair_reserve_budget"]), (10, 6, 4))

    def test_14_initial_search_cannot_consume_repair_reserve(self):
        datasets = [
            {"dataset_id": "d1", "selected": True, "in_discovery_pool": True, "excluded": False, "raw_metadata": {}},
            {"dataset_id": "d2", "selected": True, "in_discovery_pool": True, "excluded": False, "raw_metadata": {}},
        ]
        fields = [planned_field("d1", i) for i in range(10)] + [planned_field("d2", i) for i in range(10)]
        discovery = DiscoveryResult(
            snapshot={"snapshot_id": "x", "source": "FIXTURE", "exclusion_status": {}},
            datasets=datasets, fields=fields,
        )
        report = estimate_candidate_plan(discovery, config_with(), {})
        self.assertEqual(report["initial_candidates_selected"], 72)
        self.assertEqual(report["repair_budget_reserved"], 48)

    def test_15_dry_run_displays_budget_layers(self):
        discovery = DiscoveryResult(
            snapshot={"snapshot_id": "x", "source": "FIXTURE", "exclusion_status": {}},
            datasets=[{"dataset_id": "d1", "selected": True, "in_discovery_pool": True, "excluded": False, "raw_metadata": {}}],
            fields=[planned_field("d1", i) for i in range(10)],
        )
        report = estimate_candidate_plan(discovery, config_with(), {})
        for key in ("max_new_simulation_posts", "initial_search_budget", "repair_reserve_budget", "initial_candidate_estimate", "initial_candidates_selected", "initial_candidates_truncated", "repair_budget_reserved"):
            self.assertIn(key, report)

    def test_16_review_flag_when_low_hint_dominates_over_high_hint_pool(self):
        datasets = [
            {
                "dataset_id": "fund", "selected": True, "in_discovery_pool": True,
                "excluded": False, "dataset_semantic_hint": "FUNDAMENTAL",
                "dataset_preselection_score": 20, "preselection_components": {"semantic_hint": 5, "coverage_metadata": 10, "novelty": 5, "field_evidence": 0},
                "raw_metadata": {},
            },
            {
                "dataset_id": "intraday", "selected": False, "in_discovery_pool": True,
                "excluded": False, "dataset_semantic_hint": "INTRADAY",
                "dataset_preselection_score": 19, "preselection_components": {"semantic_hint": 10, "coverage_metadata": 9, "novelty": 0, "field_evidence": 0},
                "raw_metadata": {},
            },
        ]
        discovery = DiscoveryResult(
            snapshot={"snapshot_id": "x", "source": "FIXTURE", "exclusion_status": {}},
            datasets=datasets, fields=[],
        )
        report = estimate_candidate_plan(discovery, config_with(), {})
        self.assertEqual(report["dataset_preselection_review"], "DATASET_PRESELECTION_NEEDS_REVIEW")
        self.assertIn("stage_a_score", report["dataset_discovery_pool"][0])


if __name__ == "__main__":
    unittest.main()
