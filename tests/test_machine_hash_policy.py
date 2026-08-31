import yaml
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ppl_engine.config import (
    ConfigError, config_with_machine_hash_policy_override, load_effective_config,
    validate_execution_hash_compatibility,
)
import ppl_engine.live_execution as live


EXPECTED_HASH = "58634F1EB01880EDC88B7D9904EDF3716335C35C17D57AAA0215985D82FA34E4"
UNKNOWN_HASH = "F" * 64


def _config(runtime_policy=None, cli_policy=None):
    runtime = {} if runtime_policy is None else {"machine_hash_policy": runtime_policy}
    return SimpleNamespace(plan={"runtime": runtime}, machine_hash_policy_override=cli_policy)


def test_registry_is_central_and_unregistered_fails_closed():
    assert live.classify_machine_hash_operation(live.MACHINE_HASH_OPERATION_START) == "CONFIGURABLE"
    assert live.classify_machine_hash_operation(live.MACHINE_HASH_OPERATION_ROUND_REPAIR) == "CONFIGURABLE"
    assert live.classify_machine_hash_operation(live.MACHINE_HASH_OPERATION_PRODUCTION_REPAIR) == "FORCED_STRICT"
    assert live.classify_machine_hash_operation(live.MACHINE_HASH_OPERATION_ROUND_STATUS) == "NO_GUARD"
    with pytest.raises(ConfigError, match="MACHINE_HASH_OPERATION_UNREGISTERED"):
        live.classify_machine_hash_operation("FUTURE_UNREGISTERED_OPERATION")


def test_no_guard_does_not_read_hash_or_write_audit(monkeypatch):
    monkeypatch.setattr(live, "_hash_file", lambda _path: pytest.fail("hash must not be read"))
    monkeypatch.setattr(live, "audit_event", lambda **_payload: pytest.fail("audit must not be written"))
    result = live.validate_machine_lib_hash(Path("unused"), operation=live.MACHINE_HASH_OPERATION_ROUND_STATUS)
    assert result["check_result"] == "NO_GUARD"


@pytest.mark.parametrize("policy", ["STRICT", "WARN", "OFF"])
def test_expected_hash_passes_every_policy(monkeypatch, policy):
    monkeypatch.setattr(live, "_hash_file", lambda _path: live.EXPECTED_MACHINE_HASH)
    result = live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START,
        config=_config(runtime_policy=policy),
    )
    assert result["check_result"] == "EXPECTED_HASH_MATCH"
    assert result["effective_policy"] == policy


def test_strict_warn_off_and_policy_sources(monkeypatch, capsys):
    audits = []
    monkeypatch.setattr(live, "_hash_file", lambda _path: UNKNOWN_HASH)
    monkeypatch.setattr(live, "audit_event", lambda **payload: audits.append(payload))
    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        live.validate_machine_lib_hash(
            Path("unused"), operation=live.MACHINE_HASH_OPERATION_START,
            config=_config(runtime_policy="STRICT"),
        )
    warn = live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START,
        config=_config(runtime_policy="WARN"), run_id="run_test",
    )
    off = live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START,
        config=_config(cli_policy="OFF"),
    )
    default = live.resolve_machine_hash_policy(live.MACHINE_HASH_OPERATION_START, _config())
    output = capsys.readouterr().err
    assert output.count("[MACHINE HASH WARNING]") == 1
    assert output.count("[MACHINE HASH CHECK DISABLED]") == 1
    assert warn["policy_source"] == "RUNTIME"
    assert off["policy_source"] == "CLI"
    assert default["policy_source"] == "DEFAULT" and default["effective_policy"] == "WARN"
    warning_audit = next(x for x in audits if x["action"] == "MACHINE_LIB_HASH_WARNING")
    assert warning_audit["actual_hash"] == UNKNOWN_HASH
    assert warning_audit["run_id"] == "run_test"
    assert warning_audit["operation_class"] == "CONFIGURABLE"


def test_invalid_runtime_and_cli_policy_fail_closed(tmp_path):
    with pytest.raises(ConfigError, match="MACHINE_HASH_POLICY_INVALID"):
        live.resolve_machine_hash_policy(live.MACHINE_HASH_OPERATION_START, _config(runtime_policy="maybe"))
    with pytest.raises(ConfigError, match="MACHINE_HASH_POLICY_INVALID"):
        live.resolve_machine_hash_policy(live.MACHINE_HASH_OPERATION_START, _config(cli_policy="maybe"))


def test_canonical_hash_matches_all_configurable_operations_and_compat_registry_is_retired(monkeypatch):
    audits = []
    monkeypatch.setattr(live, "_hash_file", lambda _path: EXPECTED_HASH)
    monkeypatch.setattr(live, "audit_event", lambda **payload: audits.append(payload))
    # 58634F... is now the canonical expected hash: every configurable operation
    # reports EXPECTED_HASH_MATCH (no MISMATCH_WARN, no compat exception).
    resume = live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_RESUME,
        config=_config(runtime_policy="STRICT"),
    )
    assert resume["check_result"] == "EXPECTED_HASH_MATCH"
    assert resume["compatible_patch"] is False
    assert "compatible_patch_id" not in resume
    start = live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START,
        config=_config(runtime_policy="WARN"),
    )
    assert start["check_result"] == "EXPECTED_HASH_MATCH"
    assert live._AUDITED_MACHINE_HASH_COMPATIBILITY == {}
    assert all(
        x["action"] != "MACHINE_LIB_HASH_COMPATIBLE_PATCH" for x in audits
    )
    assert all(
        x["action"] != "MACHINE_LIB_HASH_WARNING" for x in audits
    )


@pytest.mark.parametrize("operation", [
    live.MACHINE_HASH_OPERATION_PRODUCTION_REPAIR,
    live.MACHINE_HASH_OPERATION_PHASE10A,
    live.MACHINE_HASH_OPERATION_PHASE10B,
    live.MACHINE_HASH_OPERATION_PHASE10B_REPAIR,
    live.MACHINE_HASH_OPERATION_LIVE_VALIDATION,
    live.MACHINE_HASH_OPERATION_DESTRUCTIVE_MIGRATION,
])
@pytest.mark.parametrize("requested", ["WARN", "OFF"])
def test_forced_strict_overrides_runtime_and_cli(monkeypatch, operation, requested):
    monkeypatch.setattr(live, "_hash_file", lambda _path: UNKNOWN_HASH)
    config = _config(runtime_policy=requested, cli_policy=requested)
    resolved = live.resolve_machine_hash_policy(operation, config)
    assert resolved["requested_policy"] == requested
    assert resolved["effective_policy"] == "STRICT"
    assert resolved["forced_strict"] is True
    with pytest.raises(ConfigError, match="MACHINE_LIB_HASH_MISMATCH"):
        live.validate_machine_lib_hash(Path("unused"), operation=operation, config=config)


def test_cli_override_does_not_change_any_config_hash():
    config = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)
    before = (config.execution_hash, config.operational_hash, config.presentation_hash, config.run_snapshot())
    overridden = config_with_machine_hash_policy_override(config, "OFF")
    after = (overridden.execution_hash, overridden.operational_hash,
             overridden.presentation_hash, overridden.run_snapshot())
    assert before == after


def test_optional_runtime_policy_does_not_enter_execution_hash(tmp_path):
    original = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)
    plan = yaml.safe_load((ROOT / "ppl_plan_v3.yaml").read_text(encoding="utf-8"))
    plan.setdefault("runtime", {})["machine_hash_policy"] = "off"
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(yaml.safe_dump(plan, allow_unicode=True, sort_keys=False), encoding="utf-8")
    configured = load_effective_config(ROOT / "ppl_rules.yaml", plan_path, project_dir=ROOT)
    assert configured.plan["runtime"]["machine_hash_policy"] == "OFF"
    assert configured.execution_hash == original.execution_hash
    assert configured.operational_hash != original.operational_hash


def test_machine_off_does_not_bypass_simulation_budget_or_post_confirmation(monkeypatch):
    from ppl_engine.simulation_adapter import validate_execution_permission

    monkeypatch.setattr(live, "_hash_file", lambda _path: UNKNOWN_HASH)
    config = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)
    config = config_with_machine_hash_policy_override(config, "OFF")
    assert live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START, config=config,
    )["check_result"] == "MISMATCH_OFF"
    candidates = [{"execution_action": "NEW_SIMULATION_REQUIRED"}]
    with pytest.raises(ConfigError, match="SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG"):
        validate_execution_permission(
            config, candidates, dry_run=False, allow_simulation_post=False,
            remaining_initial_budget=1,
        )
    with pytest.raises(ConfigError, match="INITIAL_SIMULATION_BUDGET_EXHAUSTED"):
        validate_execution_permission(
            config, candidates, dry_run=False, allow_simulation_post=True,
            remaining_initial_budget=0,
        )


def test_machine_warn_does_not_make_execution_hash_drift_compatible(monkeypatch):
    monkeypatch.setattr(live, "_hash_file", lambda _path: UNKNOWN_HASH)
    config = load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)
    assert live.validate_machine_lib_hash(
        Path("unused"), operation=live.MACHINE_HASH_OPERATION_START, config=config,
    )["check_result"] == "MISMATCH_WARN"
    compatibility = validate_execution_hash_compatibility(
        config, "0" * 64, stored_plan=config.plan, stored_rules=config.rules,
    )
    assert compatibility["execution_semantics_compatible"] is False
    assert compatibility["status"] not in {"EXACT_MATCH", "LEGACY_SCHEMA_MATCH"}


def test_parser_accepts_case_insensitive_override():
    import ppl_runner
    args = ppl_runner._parser().parse_args(["--round-status", "--machine-hash-policy", "off"])
    assert args.machine_hash_policy == "OFF"
