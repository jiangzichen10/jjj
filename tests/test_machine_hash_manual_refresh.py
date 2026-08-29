import copy
import inspect
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.live_execution import (
    EXPECTED_MACHINE_HASH,
    MACHINE_HASH_OPERATION_MANUAL_REFRESH,
    MACHINE_HASH_OPERATION_RESUME,
    validate_machine_lib_hash,
)
from ppl_engine.round_orchestrator import (
    _manual_refresh_policy_compatibility,
    load_round_policy,
    preflight_manual_finalization_refresh,
    refresh_manual_finalization_queue,
)
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.store import RunnerStore


APPROVED_HASH = "58634F1EB01880EDC88B7D9904EDF3716335C35C17D57AAA0215985D82FA34E4"


def _config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)


def _round(tmp_path):
    config = _config()
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", config)
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); ensure_round_schema(store)
    store.create_run("run_test", config)
    create_round(
        store, round_id="round_run_test", run_id="run_test", policy=policy,
        total_budget=10, search_budget=10, repair_budget=0,
    )
    policy_path = tmp_path / "round.yaml"
    policy_path.write_text(yaml.safe_dump(policy, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config, policy, store, policy_path


def test_operation_specific_machine_hash_guard(monkeypatch):
    import ppl_engine.live_execution as live

    machine_path = ROOT / "machine_lib_V2_1.py"
    assert live._hash_file(machine_path) == APPROVED_HASH
    resume = validate_machine_lib_hash(machine_path, operation=MACHINE_HASH_OPERATION_RESUME)
    refresh = validate_machine_lib_hash(machine_path, operation=MACHINE_HASH_OPERATION_MANUAL_REFRESH)
    assert resume["compatible_patch_id"] == refresh["compatible_patch_id"] == "STALE_RUNNING_RECOVERY_PATCH_V1"
    assert resume["operation"] == MACHINE_HASH_OPERATION_RESUME
    assert refresh["operation"] == MACHINE_HASH_OPERATION_MANUAL_REFRESH
    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        validate_machine_lib_hash(machine_path)

    monkeypatch.setattr(live, "_hash_file", lambda _path: EXPECTED_MACHINE_HASH)
    assert validate_machine_lib_hash(machine_path, operation=MACHINE_HASH_OPERATION_MANUAL_REFRESH)["compatible_patch"] is False
    monkeypatch.setattr(live, "_hash_file", lambda _path: "F" * 64)
    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        validate_machine_lib_hash(
            machine_path, operation=MACHINE_HASH_OPERATION_MANUAL_REFRESH,
            config=SimpleNamespace(plan={"runtime": {"machine_hash_policy": "STRICT"}},
                                   machine_hash_policy_override=None),
        )


def test_real_production_repair_guard_rejects_approved_mismatch():
    """Call the production predicate itself; do not mock its hash guard."""
    from ppl_engine.production_repair import execute_production_repair

    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        execute_production_repair(
            None, None, None, None, Path("unused.db"), ROOT / "machine_lib_V2_1.py",
            "run_test", [], True,
        )


def test_manual_refresh_policy_identity_is_bounded_and_fail_closed(tmp_path):
    _config_obj, policy, _store, _path = _round(tmp_path)
    allowed = copy.deepcopy(policy)
    allowed["batch_size"] = int(allowed["batch_size"]) + 1
    allowed["report_dir"] = "another-report-directory"
    allowed["ppl_classification"]["manual_finalization"]["auto_refresh_every_batches"] += 1
    assert _manual_refresh_policy_compatibility(policy, allowed)["compatible"] is True

    required_paths = [
        ("policy_versions", "ppl_classification"),
        ("ppl_classification", "fixed_gates"),
        ("ppl_classification", "theme_specific"),
        ("ppl_classification", "final_theme_check"),
        ("ppl_classification", "non_ppl_diagnostics"),
        ("ppl_classification", "automation_ignored"),
        ("ppl_classification", "repair_priority"),
    ]
    for outer, inner in required_paths:
        changed = copy.deepcopy(policy)
        changed.setdefault(outer, {})[inner] = {"changed": True}
        assert _manual_refresh_policy_compatibility(policy, changed)["compatible"] is False
    changed = copy.deepcopy(policy)
    changed["ppl_classification"]["manual_finalization"]["enabled"] = not bool(
        changed["ppl_classification"]["manual_finalization"]["enabled"]
    )
    assert _manual_refresh_policy_compatibility(policy, changed)["compatible"] is False
    changed = copy.deepcopy(policy)
    changed["ppl_classification"]["future_unknown_gate"] = {"x": 1}
    assert _manual_refresh_policy_compatibility(policy, changed)["compatible"] is False


def test_manual_refresh_preflight_and_compatible_audit_have_zero_business_writes(tmp_path, monkeypatch):
    import ppl_engine.live_execution as live
    import ppl_engine.round_orchestrator as orchestrator

    config, _policy, store, policy_path = _round(tmp_path)
    before_run = dict(store.get_run("run_test"))
    before_candidates = store.load_candidates("run_test")
    audits = []
    monkeypatch.setattr(live, "audit_event", lambda **payload: audits.append(payload))
    preflight = preflight_manual_finalization_refresh(
        store, config, ROOT / "machine_lib_V2_1.py", policy_path, run_id="run_test",
    )
    assert preflight["hash_result"]["compatible_patch"] is True

    monkeypatch.setattr(orchestrator, "_manual_queue_csv_candidates", lambda *_args: ["candidate"])
    monkeypatch.setattr(orchestrator, "_refresh_manual_finalization_candidates", lambda *_args, **_kwargs: {
        "executed_check_count": 1, "side_effects": {"simulation_posts": 0, "submit_requests": 0},
    })
    monkeypatch.setattr(orchestrator, "_write_reports", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(orchestrator, "round_status", lambda *_args, **_kwargs: {
        "ppl_classification": {"ready_for_manual_finalization": 0, "counts": {}},
    })
    report = refresh_manual_finalization_queue(
        store, config, object(), object(), tmp_path / "alpha.db", ROOT / "machine_lib_V2_1.py",
        policy_path, tmp_path, run_id="run_test", preflight=preflight,
        authentication_post_count=1,
    )
    counters = report["network_counters"]
    assert counters == {
        "authentication_post_count": 1,
        "simulation_post_count": 0, "delete_count": 0, "submit_count": 0,
        "power_pool_selected_count": 0, "repair_post_count": 0,
        "business_methods": ["GET"],
    }
    compatible = [x for x in audits if x.get("action") == "MACHINE_LIB_HASH_COMPATIBLE_PATCH"]
    assert compatible and compatible[0]["operation"] == MACHINE_HASH_OPERATION_MANUAL_REFRESH
    assert compatible[0]["compatible_patch_id"] == "STALE_RUNNING_RECOVERY_PATCH_V1"
    assert dict(store.get_run("run_test"))["execution_hash"] == before_run["execution_hash"]
    assert store.load_candidates("run_test") == before_candidates


def test_manual_refresh_preflight_rejects_semantic_drift_before_authentication(tmp_path):
    config, policy, store, policy_path = _round(tmp_path)
    policy["ppl_classification"]["fixed_gates"] = {"changed": True}
    policy_path.write_text(yaml.safe_dump(policy, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ConfigError, match="POLICY_DRIFT"):
        preflight_manual_finalization_refresh(
            store, config, ROOT / "machine_lib_V2_1.py", policy_path, run_id="run_test",
        )


def test_cli_orders_local_manual_refresh_gate_before_authentication():
    import ppl_runner

    source = inspect.getsource(ppl_runner.main)
    branch = source[source.index("if args.refresh_manual_finalization:"):
                    source.index("if args.recover_interrupted_batch:")]
    assert branch.index("preflight_manual_finalization_refresh(") < branch.index("_login_with_authentication_meter(")


def test_authentication_post_meter_separates_and_blocks_business_post():
    import ppl_runner

    class Session:
        def request(self, method, url, *args, **kwargs):
            return {"method": method, "url": url}

        def post(self, url, **kwargs):
            return self.request("POST", url, **kwargs)

    machine = SimpleNamespace(
        requests=SimpleNamespace(sessions=SimpleNamespace(Session=Session)),
        login=lambda: _auth_login(Session),
    )
    session, count = ppl_runner._login_with_authentication_meter(machine)
    assert isinstance(session, Session)
    assert count == 2

    machine.login = lambda: _business_login(Session)
    with pytest.raises(ConfigError, match="NON_AUTHENTICATION_POST_BLOCKED"):
        ppl_runner._login_with_authentication_meter(machine)


def _auth_login(session_type):
    session = session_type()
    session.post("https://api.worldquantbrain.com/authentication")
    session.post("https://api.worldquantbrain.com/authentication/biometrics")
    return session


def _business_login(session_type):
    session = session_type()
    session.post("https://api.worldquantbrain.com/simulations")
    return session
