import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.d3_activation import evaluate_d3_activation_preflight
from ppl_engine.research_run_mode import (
    ADAPTIVE_ARMED,
    ADAPTIVE_AUTHORITY,
    ADAPTIVE_CANARY_MODE,
    PHASE_COMPATIBILITY_AUTHORITY,
    parse_research_run_policy,
    research_run_status,
    validate_durable_research_run_lock,
    validate_new_research_run,
)
from ppl_engine.round_orchestrator import execute_round, load_round_policy
from ppl_engine.round_store import (
    checkpoint_scheduler_authority_preflight,
    create_round,
    ensure_round_schema,
    load_scheduler_authority_state,
    update_round,
)
from ppl_engine.scheduler_evidence import SafetyGateReport, evidence_policy_hash, persist_safety_gate_report
from ppl_engine.scheduler_shadow import policy_from_mapping, shadow_policy_hash
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]
BASELINE = "7a6fb42a054a98d802bca05acbf0ab34eeb597d8"
MACHINE_HASH = "58634F1EB01880EDC88B7D9904EDF3716335C35C17D57AAA0215985D82FA34E4"


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _policy(gate_key: str = "gate_d2_pass"):
    policy = load_round_policy(ROOT / "ppl_round_v31.yaml", _config())
    policy["research_run"] = {
        "mode": ADAPTIVE_CANARY_MODE,
        "expected_run_id": "run_0007",
        "scheduler_authority": ADAPTIVE_AUTHORITY,
        "scheduler_shadow": PHASE_COMPATIBILITY_AUTHORITY,
        "adaptive_control": ADAPTIVE_ARMED,
        "authority_transition_allowed": True,
        "automatic_evidence_stop": True,
        "maturation_semantics": "STATE_RESOLVED",
        "long_running_semantics": "RIGHT_CENSORED_UNTIL_RESOLVED",
        "d2_source_run_id": "run_0006",
        "d2_gate_report_key": gate_key,
        "baseline_commit": BASELINE,
        "expected_machine_hash": MACHINE_HASH,
    }
    return policy


def _create_run(store: RunnerStore, run_id: str):
    store.create_run(run_id, _config())


def _create_d2_gate(store: RunnerStore) -> str:
    _create_run(store, "run_0006")
    d2_policy = load_round_policy(ROOT / "ppl_round_v31_d2e.yaml", _config())
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=d2_policy,
        total_budget=d2_policy["total_budget"], search_budget=d2_policy["search_budget"],
        repair_budget=d2_policy["repair_budget"],
    )
    update_round(store, "round_run_0006", status="PAUSED", phase="REPAIR")
    d3_policy = _policy()
    report = SafetyGateReport(
        eligible=True,
        status="ELIGIBLE_FOR_FUTURE_CANARY_REVIEW",
        scheduler_policy_version="V31_SCHED_SHADOW_005",
        scheduler_policy_hash=shadow_policy_hash(policy_from_mapping(d3_policy["scheduler_shadow"])),
        evidence_policy_version="V31_SCHED_EVIDENCE_004",
        evidence_policy_hash=evidence_policy_hash(d3_policy["scheduler_evidence"]),
        observation_count=30,
        search_samples=20,
        repair_samples=1,
        checks={
            "thresholds_configured": True,
            "deterministic_replay": True,
            "starvation": True,
            "slot_safety": True,
            "no_repost": True,
            "recovery_safety": True,
            "policy_identity": True,
            "authoritative": False,
        },
    )
    return persist_safety_gate_report(store, "round_run_0006", "run_0006", report)


def test_d3_identity_is_explicit_armed_and_reserved_to_run_0007():
    policy = _policy()
    parsed = parse_research_run_policy(policy)
    assert parsed.mode == ADAPTIVE_CANARY_MODE
    assert parsed.scheduler_authority == ADAPTIVE_AUTHORITY
    assert parsed.scheduler_shadow == PHASE_COMPATIBILITY_AUTHORITY
    assert validate_new_research_run(policy, requested_run_id="run_0007").adaptive_control == ADAPTIVE_ARMED
    status = research_run_status(policy, run_id="run_0007")
    assert status["authority_locked"] is True
    assert status["adaptive_activation_allowed"] is True
    assert status["d3_adaptive_canary"] is True

    with pytest.raises(ValueError, match="D3_RUN_ID_LOCK_MISMATCH"):
        validate_new_research_run(policy, requested_run_id="run_0008")
    standard = load_round_policy(ROOT / "ppl_round_v31.yaml", _config())
    with pytest.raises(ValueError, match="RUN_0007_RESERVED"):
        validate_new_research_run(standard, requested_run_id="run_0007")
    active = json.loads(json.dumps(policy))
    active["research_run"]["adaptive_control"] = "ACTIVE"
    with pytest.raises(ValueError, match="D3_NEW_RUN_MUST_START_ARMED"):
        validate_new_research_run(active, requested_run_id="run_0007")


def test_d3_durable_identity_rejects_resume_drift():
    stored = _policy()
    validate_durable_research_run_lock(stored, stored, run_id="run_0007")
    drift = json.loads(json.dumps(stored))
    drift["research_run"]["baseline_commit"] = "0" * 40
    with pytest.raises(ValueError, match="RESEARCH_RUN_LOCK_DRIFT"):
        validate_durable_research_run_lock(stored, drift, run_id="run_0007")


def test_d3_round_creation_atomically_persists_armed_authority_state(tmp_path):
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    _create_run(store, "run_0007")
    policy = _policy()
    create_round(
        store, round_id="round_run_0007", run_id="run_0007", policy=policy,
        total_budget=10, search_budget=6, repair_budget=4,
    )
    state = load_scheduler_authority_state(store, run_id="run_0007")
    assert state is not None
    assert state["state"] == "ARMED"
    assert state["active_authority"] == "NONE"
    assert state["pending_authority"] == "ADAPTIVE"
    assert state["shadow_authority"] == "PHASE_COMPATIBILITY"
    assert state["authority_epoch"] == 0

    reopened = RunnerStore(store.path)
    restored = load_scheduler_authority_state(reopened, round_id="round_run_0007")
    assert restored == state
    checkpoint = checkpoint_scheduler_authority_preflight(
        reopened, round_id="round_run_0007", run_id="run_0007", expected_epoch=0,
        preflight={"eligible": False, "status": "D3_CANARY_ARMED_PREFLIGHT_FAIL:test"},
    )
    assert checkpoint["state"] == "ARMED"
    assert checkpoint["active_authority"] == "NONE"
    assert checkpoint["authority_epoch"] == 0
    assert checkpoint["preflight"]["eligible"] is False


def test_d3_activation_preflight_consumes_durable_gate_without_writes(tmp_path):
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    gate_key = _create_d2_gate(store)
    alpha_db = tmp_path / "alpha.db"
    sqlite3.connect(alpha_db).close()
    before = hashlib.sha256(store.path.read_bytes()).hexdigest()
    result = evaluate_d3_activation_preflight(
        store, _policy(gate_key), requested_run_id="run_0007",
        current_baseline_commit=BASELINE, current_machine_hash=MACHINE_HASH, alpha_db=alpha_db,
    )
    after = hashlib.sha256(store.path.read_bytes()).hexdigest()
    assert result.eligible is True
    assert result.status == "D3_CANARY_ARMED_PREFLIGHT_PASS"
    assert result.evidence["authority_transition_performed"] is False
    assert result.evidence["database_writes"] == 0
    assert before == after


def test_d3_activation_preflight_fails_closed_on_identity_or_gate_failure(tmp_path):
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    _create_d2_gate(store)
    result = evaluate_d3_activation_preflight(
        store, _policy("missing_gate"), requested_run_id="run_0007",
        current_baseline_commit="0" * 40, current_machine_hash=MACHINE_HASH,
    )
    assert result.eligible is False
    assert result.checks["baseline_commit"] is False
    assert result.checks["d2_gate_present"] is False


def test_d3_execute_round_fails_before_run_creation(tmp_path):
    policy_path = tmp_path / "d3.yaml"
    # PyYAML preserves the production loader path; this file is test-only.
    import yaml
    policy_path.write_text(yaml.safe_dump(_policy(), sort_keys=False), encoding="utf-8")
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    with pytest.raises(ConfigError, match="D3_ADAPTIVE_EXECUTION_NOT_IMPLEMENTED"):
        execute_round(
            store, _config(), None, None, tmp_path / "alpha.db", ROOT / "machine_lib_V2_1.py",
            tmp_path / "evidence.json", policy_path, run_id="run_0007",
            allow_simulation_post=False,
        )
    assert store.get_run("run_0007") is None
