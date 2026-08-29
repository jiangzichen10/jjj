"""Tests for execution-hash schema backward compatibility.

run_0002 was created under execution schema V1 (18 fields); the current runner
uses schema V2 (21 fields).  The three V2-added fields (validation_phase,
source_run_id, phase10a_run_id) are None for PRODUCTION_RESEARCH runs, so a
stored V1 hash must be accepted as LEGACY_SCHEMA_MATCH rather than falsely
reported as EXECUTION_DRIFT.  All tests are offline.
"""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ppl_engine.config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    EXECUTION_SCHEMA_V1,
    EXECUTION_SCHEMA_V2,
    EXECUTION_SCHEMA_V2_ADDED_FIELDS,
    build_execution_material,
    execution_hash_status_for_run,
    load_effective_config,
    validate_execution_hash_compatibility,
)
from ppl_engine.production_repair import preview_production_repair, validate_production_repair_context
from ppl_engine.reconcile import plan_offline_reconcile
from ppl_engine.store import RunnerStore

ROOT = PROJECT_ROOT
RULES_PATH = ROOT / "ppl_rules.yaml"
PLAN_PATH = ROOT / "ppl_plan.yaml"
EXPECTED_REPAIR_SIM_KEY = "538730546df4288f77934e424a5e99caf8e5c365394e79a4b0fa3394ab0646a9"
# run_0002's stored (V1) execution hash.
RUN_0002_LEGACY_HASH = "ceb10b63d1643f553f60962a3b6cc740a7fd683a9ddabbd0f8ac40cc9a5edd2a"


def _canon(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(_canon(value).encode()).hexdigest()


def _load_plan():
    return yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))


def _load_rules():
    return yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))


def _set(d, path, value):
    cur = d
    for key in path[:-1]:
        cur = cur[key]
    cur[path[-1]] = value


def _fake_config(plan, rules):
    return SimpleNamespace(
        plan=plan, rules=rules,
        execution_hash=_hash(build_execution_material(plan, rules, EXECUTION_SCHEMA_V2)),
    )


# ---- validator core ---------------------------------------------------------

def test_exact_match():
    conf = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=ROOT)
    r = validate_execution_hash_compatibility(conf, conf.execution_hash)
    assert r["status"] == "EXACT_MATCH"
    assert r["matched_schema_version"] == "V2"
    assert r["execution_semantics_compatible"] is True


def test_legacy_schema_match():
    conf = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=ROOT)
    legacy = _hash(build_execution_material(conf.plan, conf.rules, EXECUTION_SCHEMA_V1))
    r = validate_execution_hash_compatibility(conf, legacy)
    assert r["status"] == "LEGACY_SCHEMA_MATCH"
    assert r["matched_schema_version"] == "V1"
    assert r["reason"] == "HASH_SCHEMA_EVOLUTION_NO_EXECUTION_DRIFT"
    assert r["execution_semantics_compatible"] is True
    # The current theme policy is intentionally relaxed, so the current V1
    # hash no longer equals run_0002's historical V1 hash. Historical runs are
    # covered by the audited THEME_POLICY_RELAXATION_MATCH path using their
    # stored plan/rules snapshots.
    assert legacy != RUN_0002_LEGACY_HASH


@pytest.mark.parametrize("path,new_value", [
    (("simulation_settings", "region"), "USA"),
    (("simulation_settings", "universe"), "TOP1000"),
    (("simulation_settings", "neutralization"), "MARKET"),
    (("simulation_settings", "default_decay"), 3),
])
def test_legacy_hash_with_execution_field_drift(path, new_value):
    conf = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=ROOT)
    stored_plan = _load_plan()
    _set(stored_plan, path, new_value)
    stored_rules = _load_rules()
    stored_hash = _hash(build_execution_material(stored_plan, stored_rules, EXECUTION_SCHEMA_V1))
    r = validate_execution_hash_compatibility(conf, stored_hash, stored_plan, stored_rules)
    assert r["status"] == "EXECUTION_DRIFT"
    assert r["execution_semantics_compatible"] is False
    assert "EXECUTION_FIELDS_CHANGED" in r["reason"]


def test_legacy_hash_with_validation_phase_non_none_not_released():
    plan = _load_plan()
    plan["validation_phase"] = "PHASE_10B_INITIAL_VALIDATION"
    rules = _load_rules()
    fake = _fake_config(plan, rules)
    legacy = _hash(build_execution_material(plan, rules, EXECUTION_SCHEMA_V1))
    r = validate_execution_hash_compatibility(fake, legacy)
    # legacy hash matches, but a V2-added field now carries real semantics
    assert r["status"] == "UNRESOLVED"
    assert r["execution_semantics_compatible"] is False


def test_random_unknown_hash_is_unresolved():
    conf = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=ROOT)
    r = validate_execution_hash_compatibility(conf, "deadbeef" * 8)
    assert r["status"] == "UNRESOLVED"
    assert r["execution_semantics_compatible"] is False


def test_schema_is_explicit_not_naive_none_stripping():
    plan = _load_plan()
    rules = _load_rules()
    plan["not_a_schema_field"] = None  # must never be folded into the material
    v1 = build_execution_material(plan, rules, EXECUTION_SCHEMA_V1)
    v2 = build_execution_material(plan, rules, EXECUTION_SCHEMA_V2)
    assert "not_a_schema_field" not in v1
    assert "not_a_schema_field" not in v2
    for field in EXECUTION_SCHEMA_V2_ADDED_FIELDS:
        assert field not in v1
        assert field in v2
    assert len(v1) == 18 and len(v2) == 21


# ---- unified guard entry points --------------------------------------------

def _make_legacy_run(tmp_path):
    store = RunnerStore(tmp_path / "r.db")
    store.initialize()
    conf = load_effective_config(RULES_PATH, PLAN_PATH, project_dir=ROOT)
    legacy = _hash(build_execution_material(conf.plan, conf.rules, EXECUTION_SCHEMA_V1))
    store.create_run("run_0002", conf)
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_runs SET execution_hash=?,run_profile='PRODUCTION_RESEARCH',status='PAUSED',current_stage='PAUSED' "
            "WHERE run_id='run_0002'", (legacy,),
        )
    return store, conf, legacy


def _make_alpha_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, "
        "alpha_id TEXT, sharpe REAL, fitness REAL, turnover REAL, long_count INTEGER, short_count INTEGER)"
    )
    conn.execute("CREATE TABLE alpha_contexts (context_key TEXT PRIMARY KEY, sim_key TEXT)")
    conn.commit()
    conn.close()


def test_production_repair_accepts_legacy(tmp_path):
    store, conf, legacy = _make_legacy_run(tmp_path)
    ctx = validate_production_repair_context(store, conf, "run_0002")
    assert ctx["execution_hash_status"] == "LEGACY_SCHEMA_MATCH"
    assert ctx["execution_semantics_compatible"] is True
    assert ctx["matched_schema_version"] == "V1"


def test_production_repair_unresolved_blocked(tmp_path):
    store, conf, legacy = _make_legacy_run(tmp_path)
    with store.connect() as conn:
        conn.execute("UPDATE ppl_runs SET execution_hash=? WHERE run_id='run_0002'", ("deadbeef" * 8,))
    with pytest.raises(ConfigError, match="EXECUTION_HASH_UNRESOLVED"):
        validate_production_repair_context(store, conf, "run_0002")


def test_production_repair_execution_drift_blocked(tmp_path):
    store, conf, legacy = _make_legacy_run(tmp_path)
    drifted_plan = _load_plan()
    _set(drifted_plan, ("simulation_settings", "region"), "USA")
    drifted_hash = _hash(build_execution_material(drifted_plan, _load_rules(), EXECUTION_SCHEMA_V2))
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_runs SET execution_hash=?,plan_json=? WHERE run_id='run_0002'",
            (drifted_hash, json.dumps(drifted_plan, ensure_ascii=False, sort_keys=True)),
        )
    with pytest.raises(ConfigError, match="EXECUTION_HASH_EXECUTION_DRIFT"):
        validate_production_repair_context(store, conf, "run_0002")


def test_reconcile_accepts_legacy(tmp_path):
    store, conf, legacy = _make_legacy_run(tmp_path)
    adb = tmp_path / "a.db"
    _make_alpha_db(adb)
    out = plan_offline_reconcile(store, "run_0002", conf, adb)
    assert out["mode"] == "OFFLINE_RECONCILE"
    assert out["candidates_scanned"] == 0


def test_reconcile_unresolved_blocked(tmp_path):
    store, conf, legacy = _make_legacy_run(tmp_path)
    with store.connect() as conn:
        conn.execute("UPDATE ppl_runs SET execution_hash=? WHERE run_id='run_0002'", ("deadbeef" * 8,))
    adb = tmp_path / "a.db"
    _make_alpha_db(adb)
    with pytest.raises(ConfigError, match="EXECUTION_HASH_UNRESOLVED"):
        plan_offline_reconcile(store, "run_0002", conf, adb)


# ---- historical retired-plan compatibility (self-contained) -----------------


def test_run_0002_historical_target_tvr_plan_is_retained_but_execution_disabled(tmp_path):
    import machine_lib_V2_1 as machine_lib

    store, conf, _legacy = _make_legacy_run(tmp_path)
    adb = tmp_path / "alpha_results.db"
    _make_alpha_db(adb)
    plan_id = "rplan_historical_target_tvr"
    spec = {
        "repair_type": "HT_RATIO_TARGET_TVR",
        "repair_signature": "historical_target_tvr_sig",
        "repair_depth": 1,
        "expression_preview": "ts_target_tvr_decay(rank(f1), target_tvr=0.5, lambda_min=0, lambda_max=1)",
        "repair_path": ["RAW", "HT_RETURNS_RATIO_FAIL:HT_RATIO_TARGET_TVR"],
        "operator_requirements": ["ts_target_tvr_decay"],
    }
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_repair_plans(
                   repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                   repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                   operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                   blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (plan_id, "diag_historical", "run_0002", "historical_parent", "historical_parent",
             "HT_RETURNS_RATIO_FAIL", "HT_RATIO_TARGET_TVR", "historical_target_tvr_sig",
             json.dumps(spec["repair_path"]), 1, json.dumps(spec),
             json.dumps(["ts_target_tvr_decay"]), "READY", 1, 0, 0, None,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
    with pytest.raises(ConfigError, match="REPAIR_STRATEGY_RETIRED"):
        preview_production_repair(store, conf, adb, "run_0002", [plan_id], machine_lib)
    with store.connect() as conn:
        row = conn.execute(
            "SELECT plan_status,consumed_posts FROM ppl_repair_plans WHERE repair_plan_id=?",
            (plan_id,),
        ).fetchone()
    assert row is not None and row["plan_status"] == "READY" and int(row["consumed_posts"] or 0) == 0

