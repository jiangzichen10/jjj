import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import ppl_runner
from ppl_engine.atomic import atomic_write_json, read_json
from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.contracts import (
    CandidateLifecycle,
    CheckPhase,
    LIVE_BASE_GATE_CHECKS,
    LIVE_THEME_GATE_CHECKS,
    LOCAL_PRE_GATE_FIELDS,
    RECONCILE_PRECEDENCE,
)
from ppl_engine.runner_lock import RunnerAlreadyActive, SingleRunnerLock
from ppl_engine.store import RunnerStore


RULES_PATH = PROJECT_DIR / "ppl_rules.yaml"
PLAN_PATH = PROJECT_DIR / "ppl_plan.yaml"
ALPHA_DB = PROJECT_DIR / "alpha_results.db"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class FoundationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.alpha_db_existed = ALPHA_DB.exists()
        cls.alpha_db_hash = sha256(ALPHA_DB) if cls.alpha_db_existed else None

    @classmethod
    def tearDownClass(cls):
        assert ALPHA_DB.exists() == cls.alpha_db_existed, "Phase-1 tests changed alpha_results.db presence"
        if cls.alpha_db_existed:
            assert sha256(ALPHA_DB) == cls.alpha_db_hash, "Phase-1 tests modified alpha_results.db"

    def write_yaml(self, directory, name, value):
        path = Path(directory) / name
        path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    def test_default_config_and_target_mode(self):
        config = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=PROJECT_DIR)
        self.assertEqual(config.target_mode, "POWER_POOL_ATOM")
        self.assertTrue(config.atom_constraint_active)
        self.assertEqual(config.plan["identity"]["runner_goal"], "PPL")
        self.assertEqual(config.plan["budgets"]["max_new_simulation_posts"], 120)

    def test_theme_required_settings_mismatch_fails(self):
        plan = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
        plan["simulation_settings"]["universe"] = "TOP1000"
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_yaml(tmp, "plan.yaml", plan)
            with self.assertRaisesRegex(ConfigError, "THEME_SETTINGS_MISMATCH"):
                load_effective_config(RULES_PATH, path, project_dir=PROJECT_DIR)

    def test_invalid_target_mode_uses_v21_validator(self):
        plan = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
        plan["strategy"]["target_mode"] = "MADE_UP_MODE"
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_yaml(tmp, "plan.yaml", plan)
            with self.assertRaises(ValueError):
                load_effective_config(RULES_PATH, path, project_dir=PROJECT_DIR)

    def test_live_validation_post_budget_is_hard_clamped(self):
        plan = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
        plan["run_profile"] = "LIVE_VALIDATION"
        plan["budgets"]["max_new_simulation_posts"] = 999
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_yaml(tmp, "plan.yaml", plan)
            config = load_effective_config(RULES_PATH, path, project_dir=PROJECT_DIR)
        self.assertEqual(config.plan["budgets"]["max_new_simulation_posts"], 10)
        self.assertTrue(config.adjustments)

    def test_unsafe_flags_are_rejected(self):
        rules = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
        rules["safety"]["auto_submit"] = True
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_yaml(tmp, "rules.yaml", rules)
            with self.assertRaisesRegex(ConfigError, "Unsafe settings"):
                load_effective_config(path, PLAN_PATH, project_dir=PROJECT_DIR)

    def test_hash_layers_are_independent(self):
        base = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=PROJECT_DIR)
        rules = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
        plan = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            presentation_rules = json.loads(json.dumps(rules))
            presentation_rules["summary_limits"]["top_candidates"] = 3
            cfg_p = load_effective_config(
                self.write_yaml(tmp, "rules-p.yaml", presentation_rules),
                PLAN_PATH,
                project_dir=PROJECT_DIR,
            )
            self.assertEqual(base.execution_hash, cfg_p.execution_hash)
            self.assertEqual(base.operational_hash, cfg_p.operational_hash)
            self.assertNotEqual(base.presentation_hash, cfg_p.presentation_hash)

            operational_plan = json.loads(json.dumps(plan))
            operational_plan["runtime"]["concurrency"] = 2
            cfg_o = load_effective_config(
                RULES_PATH,
                self.write_yaml(tmp, "plan-o.yaml", operational_plan),
                project_dir=PROJECT_DIR,
            )
            self.assertEqual(base.execution_hash, cfg_o.execution_hash)
            self.assertNotEqual(base.operational_hash, cfg_o.operational_hash)

            execution_plan = json.loads(json.dumps(plan))
            execution_plan["simulation_settings"]["default_decay"] = 1
            cfg_e = load_effective_config(
                RULES_PATH,
                self.write_yaml(tmp, "plan-e.yaml", execution_plan),
                project_dir=PROJECT_DIR,
            )
            self.assertNotEqual(base.execution_hash, cfg_e.execution_hash)

    def test_atomic_json_replaces_complete_document(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            atomic_write_json(path, {"version": 1, "items": [1, 2]})
            atomic_write_json(path, {"version": 2, "items": []})
            self.assertEqual(read_json(path), {"version": 2, "items": []})
            self.assertEqual(list(Path(tmp).glob("*.tmp")), [])

    def test_single_instance_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runner.lock"
            first = SingleRunnerLock(path).acquire()
            try:
                with self.assertRaises(RunnerAlreadyActive):
                    SingleRunnerLock(path).acquire()
            finally:
                first.release()
            self.assertFalse(path.exists())

    def test_runner_database_schema_is_independent(self):
        expected = {
            "ppl_schema_meta",
            "ppl_runs",
            "ppl_catalog",
            "ppl_candidates",
            "ppl_check_polls",
            "ppl_repairs",
            "ppl_operator_capabilities",
            "ppl_descriptions",
            "ppl_reference_pool",
            "ppl_manual_evidence",
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = RunnerStore(Path(tmp) / "ppl_runner.db")
            store.initialize()
            with store.connect() as conn:
                names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(expected.issubset(names))
            self.assertNotIn("alpha_results", names)

    def test_gate_and_lifecycle_contracts_are_separate(self):
        lifecycle_values = {item.value for item in CandidateLifecycle}
        for gate in LIVE_BASE_GATE_CHECKS + LIVE_THEME_GATE_CHECKS:
            self.assertNotIn(gate, lifecycle_values)
        self.assertNotIn("SUB_UNIVERSE", LOCAL_PRE_GATE_FIELDS)
        self.assertNotIn("POWER_POOL_CORRELATION", LOCAL_PRE_GATE_FIELDS)
        self.assertEqual({phase.value for phase in CheckPhase}, {"PRE_TAG", "RECHECK", "FINAL"})

    def test_reconcile_precedence_contract(self):
        self.assertEqual(RECONCILE_PRECEDENCE["workflow"][0], "PPL_RUNNER_DB")
        self.assertEqual(RECONCILE_PRECEDENCE["check"][0], "LIVE_CHECK")
        self.assertEqual(RECONCILE_PRECEDENCE["nonterminal_simulation"][0], "LIVE_BRAIN_SIMULATION")

    def test_cli_initialize_status_summary_are_offline(self):
        config = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=PROJECT_DIR)
        self.assertTrue(config.atom_constraint_active)
        with tempfile.TemporaryDirectory() as tmp, patch(
            "requests.sessions.Session.request", side_effect=AssertionError("network forbidden")
        ), patch("requests.sessions.Session.post", side_effect=AssertionError("network forbidden")):
            root = Path(tmp)
            db = root / "runner.db"
            lock = root / "runner.lock"
            output = root / "summary.json"
            state = root / "state.json"
            common = ["--db", str(db), "--lock", str(lock), "--state", str(state), "--rules", str(RULES_PATH), "--plan", str(PLAN_PATH)]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ppl_runner.main(["--initialize", "--run-id", "run_test", *common]), 0)
                self.assertEqual(ppl_runner.main(["--status", "--run-id", "run_test", *common]), 0)
                self.assertEqual(ppl_runner.main(["--summary", "--run-id", "run_test", "--output", str(output), *common]), 0)
            summary = json.loads(output.read_text(encoding="utf-8"))
            state_doc = json.loads(state.read_text(encoding="utf-8"))
            self.assertTrue(summary["atom_constraint_active"])
            self.assertTrue(state_doc["atom_constraint_active"])
            self.assertEqual(state_doc["current_stage"], "INIT")
            self.assertEqual(summary["next_action_context"]["implementation_status"], "PHASE_1_ONLY")


if __name__ == "__main__":
    unittest.main()
