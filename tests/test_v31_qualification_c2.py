import copy
import json
from pathlib import Path
from types import SimpleNamespace

import yaml

import machine_lib_V2_1 as machine_lib
from ppl_engine.ppl_classifier import (
    classify_ppl_candidate,
    load_ppl_classification_policy,
    load_ppl_classification_policy_for_config,
)
from ppl_engine.qualification_policy import (
    RuleEvaluationStatus,
    compile_rule_declarations,
    evaluate_ppl_qualification_compatibility,
    load_qualification_integration,
    qualification_policy_hash,
    load_qualification_policy_snapshot,
    clear_qualification_policy_runtime_cache,
)
from ppl_engine.strategy_contracts import RuleRole

ROOT = Path(__file__).resolve().parents[1]


def cand(**kw):
    base = {
        "candidate_id": "cand_q",
        "simulation_status": "COMPLETE",
        "structure_status": "ELIGIBLE",
        "pp_total_operator_count_estimate": 2,
        "data_field_count_estimate": 1,
        "sim_key": "sim_q",
    }
    base.update(kw)
    return base


def metrics(sharpe=2.2, turnover=0.5, fitness=1.1):
    return {"sharpe": sharpe, "turnover": turnover, "fitness": fitness}


def row(name, result, value=None, limit=None):
    return {
        "raw_name": name,
        "normalized_name": name,
        "raw_result": result,
        "normalized_result": result,
        "eligibility_outcome": result,
        "raw_value_json": value,
        "raw_limit_json": limit,
    }


def rows(*, theme="PASS", ppc=0.4, ppc_outcome="PASS", description=False):
    out = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 2.0, 1.5),
        row("POWER_POOL_CORRELATION", ppc_outcome, ppc, 0.5),
        row("MATCHES_THEMES", theme),
    ]
    if description:
        out.extend([
            row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
            row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
        ])
    return out


def policies():
    classification = load_ppl_classification_policy(ROOT, "ppl_round_v31.yaml")
    integration = load_qualification_integration(ROOT)
    return classification, integration


def eval_bundle(candidate=None, metric_values=None, check_rows=None):
    classification_policy, integration = policies()
    candidate = candidate or cand()
    metric_values = metric_values or metrics()
    check_rows = check_rows if check_rows is not None else rows()
    legacy = classify_ppl_candidate(candidate, metric_values, check_rows, classification_policy)
    bundle = evaluate_ppl_qualification_compatibility(
        candidate, metric_values, check_rows, classification_policy, integration, legacy,
    )
    return legacy, bundle


def rule_map(bundle):
    return {x.rule_id: x for x in bundle.rules}


def test_c2_declares_only_current_rules_and_separates_roles():
    classification_policy, integration = policies()
    declarations = compile_rule_declarations(integration)
    roles = {x.role for x in declarations}
    assert RuleRole.PLATFORM_HARD_RULE in roles
    assert RuleRole.LOCAL_QUALIFICATION_RULE in roles
    assert RuleRole.LOCAL_STRATEGY_RULE in roles
    assert RuleRole.DIAGNOSTIC_WARNING in roles
    assert integration["mode"] == "LEGACY_PARITY"
    assert integration["policy_version"] == "V31_QUAL_COMPAT_002"
    # The user's earlier 2Y Sharpe number was only an architecture example.
    assert "1.58" not in json.dumps(integration, sort_keys=True)
    # Current non-PPL diagnostics are still diagnostics, not a new hard rule.
    assert "LOW_2Y_SHARPE" in classification_policy["non_ppl_diagnostics"]


def test_c2_parity_ready_classification_and_contract_projection():
    legacy, bundle = eval_bundle(check_rows=rows(theme="PASS"))
    assert legacy["classification"] == "PPL_TECHNICALLY_READY"
    assert bundle.result.classification == legacy["classification"]
    assert bundle.result.qualified is True
    assert bundle.result.policy_version == "V31_QUAL_COMPAT_002"
    assert bundle.result.repairable_failure_codes == ()


def test_c2_missing_fact_is_explicit_unresolved_not_silent_fail():
    legacy, bundle = eval_bundle(metric_values=metrics(sharpe=None))
    rules_by_id = rule_map(bundle)
    assert rules_by_id["PPL_SHARPE_MIN"].status is RuleEvaluationStatus.UNRESOLVED
    assert "SHARPE_MISSING" in legacy["fixed_unresolved"]
    assert "SHARPE_MISSING" in bundle.result.unresolved
    assert "SHARPE_BELOW_PPL_MIN" not in bundle.result.blockers


def test_c2_platform_ppc_fact_and_local_ppc_strategy_are_separate():
    legacy, bundle = eval_bundle(
        metric_values=metrics(sharpe=3.24),
        check_rows=rows(theme="PASS", ppc=0.775, ppc_outcome="PASS"),
    )
    by_id = rule_map(bundle)
    assert legacy["classification"] == "PPL_STRATEGY_REJECT_HIGH_PPC"
    # Platform PASS remains PASS even though the local strategy rejects the value.
    assert by_id["PPL_POWER_POOL_CORRELATION"].status is RuleEvaluationStatus.PASS
    assert by_id["PPL_POWER_POOL_CORRELATION"].platform_outcome == "PASS"
    assert by_id["PPC_STRATEGY_BAND"].role is RuleRole.LOCAL_STRATEGY_RULE
    assert by_id["PPC_STRATEGY_BAND"].status is RuleEvaluationStatus.FAIL
    assert by_id["PPC_STRATEGY_BAND"].metadata["band"] == "HIGH"
    assert bundle.result.local_strategy_results["ppc_strategy_result"] == "REJECT_PPC_GE_0_65"


def test_c2_mid_ppc_sharpe_rule_matches_strict_current_behavior():
    legacy_at, bundle_at = eval_bundle(
        metric_values=metrics(sharpe=2.0),
        check_rows=rows(theme="WARNING", ppc=0.55, description=True),
    )
    legacy_over, bundle_over = eval_bundle(
        metric_values=metrics(sharpe=2.01),
        check_rows=rows(theme="WARNING", ppc=0.55, description=True),
    )
    assert legacy_at["classification"] == "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE"
    assert rule_map(bundle_at)["PPC_MID_SHARPE"].status is RuleEvaluationStatus.FAIL
    assert legacy_over["classification"] == "PPL_READY_FOR_MANUAL_FINALIZATION"
    assert rule_map(bundle_over)["PPC_MID_SHARPE"].status is RuleEvaluationStatus.PASS


def test_c2_regular_two_year_sharpe_check_remains_diagnostic_only():
    check_rows = rows(theme="PASS") + [row("LOW_2Y_SHARPE", "FAIL", 0.47, 1.58)]
    legacy, bundle = eval_bundle(check_rows=check_rows)
    assert legacy["classification"] == "PPL_TECHNICALLY_READY"
    assert "LOW_2Y_SHARPE" in bundle.result.diagnostics
    assert "LOW_2Y_SHARPE" not in bundle.result.blockers
    diag_rule = rule_map(bundle)["NON_PPL_DIAGNOSTICS"]
    assert diag_rule.role is RuleRole.DIAGNOSTIC_WARNING
    assert "LOW_2Y_SHARPE" in diag_rule.metadata["triggered_checks"]


def test_c2_current_classifier_public_output_is_not_rewritten_by_sidecar_evaluator():
    classification_policy, integration = policies()
    check_rows = rows(theme="PASS")
    before = classify_ppl_candidate(cand(), metrics(), check_rows, classification_policy)
    _ = evaluate_ppl_qualification_compatibility(
        cand(), metrics(), check_rows, classification_policy, integration, before,
    )
    after = classify_ppl_candidate(cand(), metrics(), check_rows, classification_policy)
    assert after == before
    assert "qualification_policy_version" not in after


def test_c2_pure_qualification_policy_edit_changes_policy_hash_not_sim_key():
    classification_policy, integration = policies()
    changed = copy.deepcopy(classification_policy)
    changed["manual_finalization"]["ppc_strategy"]["clean_max"] = 0.49

    before_hash = qualification_policy_hash(integration, classification_policy)
    after_hash = qualification_policy_hash(integration, changed)
    assert before_hash != after_hash

    expression = "rank(-ts_delta(close, 5))"
    settings = {
        "instrumentType": "EQUITY", "region": "GLB", "universe": "TOPDIV3000",
        "delay": 1, "neutralization": "SUBINDUSTRY", "decay": 0, "truncation": 0.08,
    }
    assert machine_lib.simulation_key(expression, settings) == machine_lib.simulation_key(expression, settings)


def test_c2_config_aware_policy_loader_uses_v31_only_for_continuous_profile():
    continuous = SimpleNamespace(project_dir=ROOT, plan={"run_profile": "CONTINUOUS_RESEARCH"})
    legacy = SimpleNamespace(project_dir=ROOT, plan={"run_profile": "PRODUCTION_RESEARCH"})
    assert load_ppl_classification_policy_for_config(continuous) == load_ppl_classification_policy(ROOT, "ppl_round_v31.yaml")
    assert load_ppl_classification_policy_for_config(legacy) == load_ppl_classification_policy(ROOT, "ppl_round_v3.yaml")


def test_c2_classify_run_real_continuous_path_attaches_qualification_projection(monkeypatch):
    import ppl_engine.near_pass as near_pass

    fake_context = {
        "candidates": {"cand_q": cand()},
        "metrics_by_key": {"sim_q": metrics()},
        "check_rows_by_cid": {"cand_q": rows(theme="PASS")},
        "repairs_by_parent": {"cand_q": []},
    }
    monkeypatch.setattr(near_pass, "build_rescue_context", lambda *a, **k: fake_context)
    monkeypatch.setattr(near_pass, "_latest_pretag_session_status_by_candidate", lambda *a, **k: {})

    cfg = SimpleNamespace(project_dir=ROOT, plan={"run_profile": "CONTINUOUS_RESEARCH"})
    out = near_pass.classify_run(object(), cfg, ROOT / "unused.db", "run_0006")
    assert len(out) == 1
    item = out[0]
    assert item["classification"] == "PPL_TECHNICALLY_READY"
    assert item["qualification_policy_version"] == "V31_QUAL_COMPAT_002"
    assert item["qualification_qualified"] is True
    assert len(item["qualification_policy_hash"]) == 64
    assert item["qualification_evaluator_version"].startswith("V31_QUAL_DECL_")


def test_c2_policy_version_is_single_source_between_integration_and_policy_versions():
    raw = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    assert raw["qualification_integration"]["policy_version"] == raw["policy_versions"]["qualification"]


def test_c2_runtime_snapshot_does_not_hot_reload_mid_batch(tmp_path):
    source = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    target = tmp_path / "ppl_round_v31.yaml"
    target.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
    clear_qualification_policy_runtime_cache()
    first = load_qualification_policy_snapshot(tmp_path)

    edited = copy.deepcopy(source)
    edited["qualification_integration"]["policy_version"] = "V31_QUAL_COMPAT_TEST_EDIT"
    edited["policy_versions"]["qualification"] = "V31_QUAL_COMPAT_TEST_EDIT"
    target.write_text(yaml.safe_dump(edited, sort_keys=False), encoding="utf-8")

    still_frozen = load_qualification_policy_snapshot(tmp_path)
    assert still_frozen.policy_hash == first.policy_hash
    assert still_frozen.integration["policy_version"] == "V31_QUAL_COMPAT_002"

    reloaded = load_qualification_policy_snapshot(tmp_path, force_reload=True)
    assert reloaded.policy_hash != first.policy_hash
    assert reloaded.integration["policy_version"] == "V31_QUAL_COMPAT_TEST_EDIT"
    clear_qualification_policy_runtime_cache()


def test_c2_runtime_snapshot_rejects_policy_version_mismatch(tmp_path):
    source = yaml.safe_load((ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8"))
    source["qualification_integration"]["policy_version"] = "Q_BAD"
    (tmp_path / "ppl_round_v31.yaml").write_text(
        yaml.safe_dump(source, sort_keys=False), encoding="utf-8"
    )
    clear_qualification_policy_runtime_cache()
    import pytest
    with pytest.raises(ValueError, match="QUALIFICATION_POLICY_VERSION_MISMATCH"):
        load_qualification_policy_snapshot(tmp_path)
    clear_qualification_policy_runtime_cache()
