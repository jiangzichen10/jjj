from pathlib import Path

from ppl_engine.ppl_classifier import classify_ppl_candidate, load_ppl_classification_policy
from ppl_engine.check_parser import normalize_check_name


def policy():
    return load_ppl_classification_policy(Path(__file__).resolve().parents[1])


def cand(**kw):
    base = {
        "simulation_status": "COMPLETE", "structure_status": "ELIGIBLE",
        "pp_total_operator_count_estimate": 2, "data_field_count_estimate": 1,
    }
    base.update(kw)
    return base


def metrics(sharpe=2.2, turnover=0.5, fitness=1.1):
    return {"sharpe": sharpe, "turnover": turnover, "fitness": fitness}


def row(name, result, value=None, limit=None):
    return {
        "raw_name": name, "normalized_name": name, "raw_result": result,
        "normalized_result": result, "eligibility_outcome": result,
        "raw_value_json": value, "raw_limit_json": limit,
    }


def common(pass_theme=False):
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 2.0, 1.5),
        row("POWER_POOL_CORRELATION", "PASS", 0.4, 0.5),
        row("MATCHES_THEMES", "PASS" if pass_theme else "WARNING"),
    ]
    return rows


def test_sharpe_below_one_is_terminal_even_before_repair():
    out = classify_ppl_candidate(cand(), metrics(sharpe=0.99), common(), policy())
    assert out["classification"] == "PPL_TERMINAL_FAIL"
    assert "SHARPE_BELOW_PPL_MIN" in out["reasons"]


def test_regular_low_sharpe_158_fail_does_not_override_ppl_min_one():
    rows = common() + [row("LOW_SHARPE", "FAIL", 1.3, 1.58)]
    out = classify_ppl_candidate(cand(), metrics(sharpe=1.3), rows, policy())
    assert out["classification"] == "PPL_THEME_UNRESOLVED"
    assert out["classification"] != "PPL_TERMINAL_FAIL"


def test_turnover_74_is_fixed_repairable_medium():
    out = classify_ppl_candidate(cand(), metrics(turnover=0.74), common(), policy())
    assert out["classification"] == "PPL_FIXED_REPAIRABLE"
    assert out["repair_priority"] == "MEDIUM"
    assert out["repair_drivers"] == ["TURNOVER_ABOVE_BASE_MAX"]


def test_sub_universe_small_fail_is_fixed_repairable_medium():
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "FAIL", 1.72, 1.85),
        row("POWER_POOL_CORRELATION", "PASS", 0.4, 0.5),
        row("MATCHES_THEMES", "WARNING"),
    ]
    out = classify_ppl_candidate(cand(), metrics(), rows, policy())
    assert out["classification"] == "PPL_FIXED_REPAIRABLE"
    assert out["repair_priority"] == "MEDIUM"
    assert "SUB_UNIVERSE_FAIL" in out["repair_drivers"]


def test_platform_pp_corr_pass_wins_over_numeric_limit():
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 2.05, 1.61),
        row("POWER_POOL_CORRELATION", "PASS", 0.5503, 0.5),
        row("MATCHES_THEMES", "PASS"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=2.63, turnover=0.6952), rows, policy())
    assert out["classification"] == "PPL_TECHNICALLY_READY"
    assert not out["fixed_blockers"]


def test_non_ppl_region_and_prod_fail_do_not_block_ppl():
    rows = common(pass_theme=True) + [
        row("LOW_GLB_EMEA_SHARPE", "FAIL", 0.76, 1.0),
        row("PROD_CORRELATION", "FAIL", 0.8562, 0.7),
        row("LOW_FITNESS", "FAIL", 0.9, 1.0),
    ]
    out = classify_ppl_candidate(cand(), metrics(), rows, policy())
    assert out["classification"] == "PPL_TECHNICALLY_READY"
    names = {x["check"] for x in out["quality_diagnostics"]}
    assert {"LOW_GLB_EMEA_SHARPE", "PROD_CORRELATION", "LOW_FITNESS"} <= names


def test_matches_themes_warning_is_final_outcome_not_automatic_discard():
    out = classify_ppl_candidate(cand(), metrics(), common(pass_theme=False), policy())
    assert out["classification"] == "PPL_THEME_UNRESOLVED"
    assert out["theme_match"] is False


def test_current_liquid_theme_ignores_legacy_ht_ratio_warning():
    rows = common() + [
        row("HT_TURNOVER", "PASS", 0.55, 0.2),
        row("HT_HIGH_TURNOVER_RETURNS_RATIO", "WARNING", 0.72, 0.75),
    ]
    out = classify_ppl_candidate(cand(), metrics(turnover=0.55), rows, policy())
    assert out["classification"] == "PPL_THEME_UNRESOLVED"
    assert out["repair_priority"] == "NONE"
    assert "HT_RETURNS_RATIO_FAIL" not in out["repair_drivers"]


def test_rrm_like_pp_above_strategy_max_is_rejected_legacy_ht_is_ignored():
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 1.72, 1.4),
        row("POWER_POOL_CORRELATION", "WARNING", 0.897, 0.5),
        row("HT_TURNOVER", "PASS", 0.671, 0.2),
        row("HT_HIGH_TURNOVER_RETURNS_RATIO", "WARNING", 0.6514, 0.75),
        row("LOW_GLB_AMER_SHARPE", "FAIL", 0.99, 1.0),
        row("LOW_GLB_EMEA_SHARPE", "FAIL", 0.88, 1.0),
        row("MATCHES_THEMES", "WARNING"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=2.28, turnover=0.671), rows, policy())
    assert out["classification"] == "PPL_STRATEGY_REJECT_HIGH_PPC"
    assert out["repair_priority"] == "NONE"
    assert out["ppc_value"] == 0.897
    assert out["ppc_strategy_result"] == "REJECT_PPC_GE_0_65"
    assert {x["check"] for x in out["quality_diagnostics"]} >= {"LOW_GLB_AMER_SHARPE", "LOW_GLB_EMEA_SHARPE"}


def test_unknown_ht_check_is_preserved_but_not_blocker():
    rows = common() + [row("HT_LIQUID_TOPDIV3000_SHARPE", "WARNING", 1.67, 1.7)]
    out = classify_ppl_candidate(cand(), metrics(), rows, policy())
    assert out["classification"] == "PPL_THEME_UNRESOLVED"
    assert [x["check"] for x in out["unmapped_theme_signals"]] == ["HT_LIQUID_TOPDIV3000_SHARPE"]
    assert not out["repair_blockers"]
    assert normalize_check_name("HT_FUTURE_NEW_CHECK")["normalized_name"] == "HT_FUTURE_NEW_CHECK"
    assert normalize_check_name("HT_FUTURE_NEW_CHECK")["category"] == "PPL_THEME_UNMAPPED"


def test_description_and_pure_theme_internal_checks_are_ignored_as_blockers_but_drive_manual_finalization():
    rows = common() + [
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
        row("PURE_POWER_POOL_THEME", "FAIL"),
    ]
    out = classify_ppl_candidate(cand(), metrics(), rows, policy())
    assert out["classification"] == "PPL_READY_FOR_MANUAL_FINALIZATION"
    names = {x["check"] for x in out["quality_diagnostics"]}
    assert "POWER_POOL_DESCRIPTION_LENGTH" not in names
    assert "POWER_POOL_DESCRIPTION_FORMAT" not in names
    assert "PURE_POWER_POOL_THEME" not in names


def test_description_pending_pretag_warning_becomes_manual_finalization_ready():
    rows = common(pass_theme=False) + [
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=1.66, turnover=0.5552), rows, policy())
    assert out["classification"] == "PPL_READY_FOR_MANUAL_FINALIZATION"
    assert out["manual_finalization_required"] is True
    assert out["description_pending"] is True
    assert out["theme_match"] is None
    assert out["repair_priority"] == "NONE"
    assert not out["repair_blockers"]


def test_description_pending_does_not_hide_fixed_ppl_blocker():
    rows = common(pass_theme=False) + [
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=2.0, turnover=0.74), rows, policy())
    assert out["classification"] == "PPL_FIXED_REPAIRABLE"
    assert out["manual_finalization_required"] is False


def test_final_theme_pass_overrides_description_warnings_and_is_technically_ready():
    rows = common(pass_theme=True) + [
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
        row("LOW_FITNESS", "FAIL", 0.62, 1.0),
        row("LOW_GLB_EMEA_SHARPE", "FAIL", 0.62, 1.0),
        row("LOW_GLB_APAC_SHARPE", "FAIL", 0.86, 1.0),
        row("LOW_2Y_SHARPE", "FAIL", 0.47, 1.58),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=1.66, turnover=0.5552), rows, policy())
    assert out["classification"] == "PPL_TECHNICALLY_READY"
    assert out["theme_match"] is True
    assert out["repair_priority"] == "NONE"
    assert {x["check"] for x in out["quality_diagnostics"]} >= {
        "LOW_FITNESS", "LOW_GLB_EMEA_SHARPE", "LOW_GLB_APAC_SHARPE", "LOW_2Y_SHARPE"
    }


def test_theme_warning_without_description_warning_stays_theme_unresolved():
    out = classify_ppl_candidate(cand(), metrics(), common(pass_theme=False), policy())
    assert out["classification"] == "PPL_THEME_UNRESOLVED"
    assert out["manual_finalization_required"] is False


def test_ppc_at_or_above_065_is_strategy_rejected_even_platform_pass_and_high_sharpe():
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 2.0, 1.0),
        row("POWER_POOL_CORRELATION", "PASS", 0.775, 0.5),
        row("MATCHES_THEMES", "PASS"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=3.24, turnover=0.52), rows, policy())
    assert out["classification"] == "PPL_STRATEGY_REJECT_HIGH_PPC"
    assert out["platform_ppc_outcome"] == "PASS"
    assert out["ppc_value"] == 0.775
    assert out["ppc_strategy_result"] == "REJECT_PPC_GE_0_65"
    assert out["theme_match"] is True


def test_mid_ppc_requires_sharpe_strictly_greater_than_two_for_manual_queue():
    rows = common(pass_theme=False) + [
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
    ]
    # Replace the common PPC row with a mid-band value.
    rows = [r for r in rows if r["raw_name"] != "POWER_POOL_CORRELATION"]
    rows.insert(1, row("POWER_POOL_CORRELATION", "PASS", 0.55, 0.5))
    at_two = classify_ppl_candidate(cand(), metrics(sharpe=2.0), rows, policy())
    above_two = classify_ppl_candidate(cand(), metrics(sharpe=2.01), rows, policy())
    assert at_two["classification"] == "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE"
    assert above_two["classification"] == "PPL_READY_FOR_MANUAL_FINALIZATION"
    assert above_two["ppc_policy_band"] == "MID"


def test_ppc_boundaries_050_clean_and_065_rejected():
    desc = [row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100), row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING")]
    rows_050 = [row("LOW_SUB_UNIVERSE_SHARPE","PASS",2,1), row("POWER_POOL_CORRELATION","PASS",0.50,0.5), row("MATCHES_THEMES","WARNING"), *desc]
    rows_065 = [row("LOW_SUB_UNIVERSE_SHARPE","PASS",2,1), row("POWER_POOL_CORRELATION","PASS",0.65,0.5), row("MATCHES_THEMES","WARNING"), *desc]
    assert classify_ppl_candidate(cand(), metrics(sharpe=1.2), rows_050, policy())["classification"] == "PPL_READY_FOR_MANUAL_FINALIZATION"
    assert classify_ppl_candidate(cand(), metrics(sharpe=4.0), rows_065, policy())["classification"] == "PPL_STRATEGY_REJECT_HIGH_PPC"


def test_missing_numeric_ppc_does_not_enter_manual_queue():
    rows = [
        row("LOW_SUB_UNIVERSE_SHARPE", "PASS", 2.0, 1.0),
        row("POWER_POOL_CORRELATION", "PASS", None, 0.5),
        row("MATCHES_THEMES", "WARNING"),
        row("POWER_POOL_DESCRIPTION_LENGTH", "WARNING", 0, 100),
        row("POWER_POOL_DESCRIPTION_FORMAT", "WARNING"),
    ]
    out = classify_ppl_candidate(cand(), metrics(sharpe=2.5), rows, policy())
    assert out["classification"] == "PPL_CHECK_UNRESOLVED"
    assert "PPC_VALUE_MISSING_FOR_STRATEGY" in out["reasons"]
    assert out["manual_finalization_candidate_pre_strategy"] is True
