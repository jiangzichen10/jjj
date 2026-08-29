import copy
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ppl_engine.config import (
    CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA,
    build_execution_material,
    build_research_policy_material,
    build_simulation_semantics_material,
    config_with_extension_execution_identity,
    load_effective_config,
    research_policy_hash,
    simulation_semantics_hash,
    validate_execution_hash_compatibility,
)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _cfg(plan_name="ppl_plan_v31.yaml"):
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / plan_name, project_dir=ROOT)


def test_c4_continuous_identity_has_explicit_simulation_and_research_layers():
    cfg = _cfg()
    assert cfg.simulation_semantics_hash == simulation_semantics_hash(cfg.plan, cfg.rules)
    assert cfg.research_policy_hash == research_policy_hash(cfg.plan, cfg.rules)
    sem = build_simulation_semantics_material(cfg.plan, cfg.rules)
    assert sem["schema"] == CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA
    assert set(sem) == {"schema", "runner_goal", "simulation_settings"}
    research = build_research_policy_material(cfg.plan, cfg.rules)
    assert "selection" in research and "heuristics" in research and "repair_cycle_control" in research
    assert "simulation_settings" not in research


def test_c4_search_policy_change_does_not_change_simulation_semantics():
    cfg = _cfg()
    plan2 = copy.deepcopy(cfg.plan)
    plan2["selection"]["max_datasets"] = int(plan2["selection"]["max_datasets"]) + 7
    assert simulation_semantics_hash(plan2, cfg.rules) == cfg.simulation_semantics_hash
    assert research_policy_hash(plan2, cfg.rules) != cfg.research_policy_hash
    assert _hash(build_execution_material(plan2, cfg.rules)) != cfg.execution_hash


def test_c4_repair_policy_change_does_not_change_simulation_semantics():
    cfg = _cfg()
    rules2 = copy.deepcopy(cfg.rules)
    rules2["repair_cycle_control"]["max_repair_depth"] = int(rules2["repair_cycle_control"]["max_repair_depth"]) + 1
    assert simulation_semantics_hash(cfg.plan, rules2) == cfg.simulation_semantics_hash
    assert research_policy_hash(cfg.plan, rules2) != cfg.research_policy_hash


def test_c4_continuous_resume_accepts_research_policy_drift_with_same_remote_semantics():
    cfg = _cfg()
    stored_plan = copy.deepcopy(cfg.plan)
    stored_rules = copy.deepcopy(cfg.rules)
    stored_plan["selection"]["max_datasets"] = 3
    stored_rules["repair_cycle_control"]["max_repair_depth"] = 99
    stored_hash = _hash(build_execution_material(stored_plan, stored_rules))
    result = validate_execution_hash_compatibility(cfg, stored_hash, stored_plan, stored_rules)
    assert result["status"] == "CONTINUOUS_SIMULATION_SEMANTICS_MATCH"
    assert result["execution_semantics_compatible"] is True
    assert result["stored_simulation_semantics_hash"] == result["current_simulation_semantics_hash"]
    assert result["stored_research_policy_hash"] != result["current_research_policy_hash"]


def test_c4_continuous_resume_rejects_true_simulation_setting_drift():
    cfg = _cfg()
    stored_plan = copy.deepcopy(cfg.plan)
    stored_rules = copy.deepcopy(cfg.rules)
    stored_plan["simulation_settings"]["neutralization"] = "MARKET"
    stored_hash = _hash(build_execution_material(stored_plan, stored_rules))
    result = validate_execution_hash_compatibility(cfg, stored_hash, stored_plan, stored_rules)
    assert result["status"] == "EXECUTION_DRIFT"
    assert result["execution_semantics_compatible"] is False
    assert "simulation_settings.neutralization" in result["reason"]


def test_c4_extension_context_stays_part_of_continuous_semantic_guard():
    cfg = _cfg()
    ctx = {
        "extension_policy_version": "EXT_1",
        "normalized_source_semantic_identity_digest": "a" * 64,
        "canonical_evidence_digest": "b" * 64,
    }
    ext_cfg = config_with_extension_execution_identity(cfg, ctx)
    assert ext_cfg.simulation_semantics_hash != cfg.simulation_semantics_hash
    assert ext_cfg.research_policy_hash == cfg.research_policy_hash
