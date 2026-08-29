import json
import inspect
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import machine_lib_V2_1 as machine_lib
from ppl_engine.candidate_factory import (
    TRANSFORM_FAMILIES,
    _prune_extension_candidates,
    build_expression,
    generate_candidate_preview,
)
from ppl_engine.config import (
    ConfigError,
    config_with_extension_execution_identity,
    load_effective_config,
    validate_execution_hash_compatibility,
)
from ppl_engine.research_telemetry import record_candidate_universe, sync_simulation_ledger
from ppl_engine.round_orchestrator import (
    _extension_context_from_source,
    _extension_source_context,
    _extension_specs_for_discovery,
    _prepare_new_round,
    _candidate_pool_preflight,
    _select_search_batch,
    _thaw_evidence,
    requires_extension_new_post,
)
from ppl_engine.round_store import create_round, ensure_round_schema
from ppl_engine.store import RunnerStore


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)


def alpha_db(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE alpha_results(sim_key TEXT PRIMARY KEY,status TEXT,simulation_url TEXT,alpha_id TEXT,sharpe REAL)")
    con.commit(); con.close()
    return path


def discovery():
    snapshot = {
        "snapshot_id": "disc_ext", "region": "GLB", "universe": "TOPDIV3000", "delay": 1,
        "instrument_type": "EQUITY", "source": "TEST", "dataset_count": 2, "field_count": 2,
        "metadata_hash": "x", "exclusion_status": {}, "automatic_preselection": True,
    }
    datasets = [
        {"dataset_id": "ds_a", "selected": True, "dataset_preselection_score": 20},
        {"dataset_id": "ds_b", "selected": True, "dataset_preselection_score": 10},
    ]
    fields = [
        {"dataset_id": "ds_a", "field_id": "same_field", "field_type": "MATRIX", "semantic_class": "RETURN",
         "classification_source": "TEST", "classification_rule_id": "a", "classification_confidence": "HIGH",
         "coverage": .95, "alphaCount": 1, "selected": True},
        {"dataset_id": "ds_b", "field_id": "same_field", "field_type": "MATRIX", "semantic_class": "RETURN",
         "classification_source": "TEST", "classification_rule_id": "b", "classification_confidence": "HIGH",
         "coverage": .95, "alphaCount": 1, "selected": True},
    ]
    return SimpleNamespace(snapshot=snapshot, datasets=datasets, fields=fields)


def dry(c):
    return {"dry_run_id": "dry_ext", "discovery_snapshot_id": "disc_ext", "execution_hash": c.execution_hash}


@pytest.mark.parametrize("operator", ["ts_std_dev", "ts_arg_min", "ts_arg_max", "ts_quantile"])
@pytest.mark.parametrize("window", [22, 66])
def test_extension_renderer_matrix_two_argument_signature(operator, window):
    assert build_expression("x", "MATRIX", "IDENTITY", operator, window) == f"{operator}(x, {window})"


def test_extension_family_mapping_and_vector_rejection():
    assert TRANSFORM_FAMILIES["ts_std_dev"] == "TS_STD_DEV"
    assert TRANSFORM_FAMILIES["ts_quantile"] == "TS_QUANTILE"
    assert TRANSFORM_FAMILIES["ts_arg_min"] == TRANSFORM_FAMILIES["ts_arg_max"] == "TS_ARG_EXTREME"
    # The renderer remains generally compatible with existing VECTOR handling;
    # first-stage MATRIX-only is enforced by extension candidate generation.
    assert build_expression("x", "VECTOR", "VEC_SUM", "ts_quantile", 22) == "ts_quantile(vec_sum(x), 22)"


def test_targeted_specs_use_dataset_and_field_identity_and_core_is_not_blocked():
    c = cfg(); d = discovery()
    source = {
        "source_run_id": "run_source",
        "evidence": {
            "dataset": {"ds_a": {"attempts": 10, "search_viable": 0, "signal_viable": 0}},
            "dataset_field": {("ds_a", "same_field"): {
                "attempts": 1, "ppl_success": 1, "ppl_strong_near_pass": 0,
                "ppl_near_pass": 0, "signal_viable": 1,
            }},
        },
    }
    specs = _extension_specs_for_discovery(d, c, {"rolling_discovery": {}}, source)
    core_a = [x for x in specs if x["operator"] == "ts_std_dev" and x["field"]["dataset_id"] == "ds_a"]
    assert {x["window"] for x in core_a} == {22, 66}
    assert all(x["score_adjustment"] < 0 for x in core_a)
    targeted = [x for x in specs if x["metadata"]["extension_source"] == "TARGETED_OPERATOR_EXTENSION"]
    assert {x["field"]["dataset_id"] for x in targeted} == {"ds_a"}
    assert {x["operator"] for x in targeted} == {"ts_arg_min", "ts_arg_max", "ts_quantile"}


def test_no_positive_evidence_means_no_targeted_extension(tmp_path):
    c = cfg(); d = discovery()
    specs = _extension_specs_for_discovery(d, c, {"rolling_discovery": {}}, {
        "source_run_id": "source", "evidence": {"dataset": {}, "dataset_field": {}},
    })
    assert {x["operator"] for x in specs} == {"ts_std_dev"}
    rows, report = generate_candidate_preview(
        d, dry(c), c, run_id="run_new", alpha_db=alpha_db(tmp_path / "alpha.db"), machine_lib=machine_lib,
        extension_specs=specs, extension_pool_state={"existing_base_count": 10},
    )
    assert {x["operator"] for x in rows} >= {"ts_std_dev"}
    assert report["extension_preflight"]["targeted_generated"] == 0


def test_candidate_extension_provenance_and_no_120(tmp_path):
    c = cfg(); d = discovery()
    field = d.fields[0]
    specs = [
        {"field": field, "operator": "ts_std_dev", "window": 22, "route_priority": "HIGH", "metadata": {
            "extension_source": "CORE_OPERATOR_RESTORE", "extension_priority": [0, 0, 0, 0],
        }},
        {"field": field, "operator": "ts_std_dev", "window": 66, "route_priority": "HIGH", "metadata": {
            "extension_source": "CORE_OPERATOR_RESTORE", "extension_priority": [0, 0, 0, 0],
        }},
        {"field": field, "operator": "ts_quantile", "window": 66, "route_priority": "HIGH", "metadata": {
            "extension_source": "TARGETED_OPERATOR_EXTENSION", "parent_dataset_id": "ds_a", "parent_field": "same_field",
            "trigger_evidence": {"paid_complete": 1, "ppl_success": 1}, "extension_priority": [1, 0, 0, 0],
        }},
    ]
    rows, _ = generate_candidate_preview(
        d, dry(c), c, run_id="run_new", alpha_db=alpha_db(tmp_path / "alpha.db"), machine_lib=machine_lib,
        extension_specs=specs, extension_pool_state={"existing_base_count": 20},
    )
    extended = [x for x in rows if x["operator"] in {"ts_std_dev", "ts_quantile"}]
    assert {x["window"] for x in extended} == {22, 66}
    assert all(x["window"] != 120 for x in extended)
    assert any(x["provenance"]["extension_source"] == "TARGETED_OPERATOR_EXTENSION" for x in extended)


def test_extension_generation_is_matrix_only(tmp_path):
    c = cfg(); d = discovery()
    d.fields[1]["field_type"] = "VECTOR"
    specs = _extension_specs_for_discovery(d, c, {"rolling_discovery": {}}, None)
    rows, _ = generate_candidate_preview(
        d, dry(c), c, run_id="run_new", alpha_db=alpha_db(tmp_path / "alpha.db"), machine_lib=machine_lib,
        extension_specs=specs,
    )
    assert not any(x["operator"] == "ts_std_dev" and x["dataset_id"] == "ds_b" for x in rows)


@pytest.mark.parametrize("bad_field", ["", None])
def test_selected_discovery_identity_fails_closed(bad_field):
    c = cfg(); d = discovery()
    d.fields[0]["field_id"] = bad_field
    with pytest.raises(ConfigError, match="IDENTITY_REQUIRED"):
        _extension_specs_for_discovery(d, c, {"rolling_discovery": {}}, None)


def test_extension_allowlist_rejects_120_and_vector_before_candidate_creation(tmp_path):
    c = cfg(); d = discovery(); field = d.fields[0]
    bad_120 = {"field": field, "operator": "ts_quantile", "window": 120, "metadata": {}}
    with pytest.raises(ConfigError, match="WINDOW_NOT_ALLOWED"):
        generate_candidate_preview(
            d, dry(c), c, run_id="run_new", alpha_db=alpha_db(tmp_path / "alpha.db"),
            machine_lib=machine_lib, extension_specs=[bad_120],
        )
    vector = dict(field, field_type="VECTOR")
    bad_vector = {"field": vector, "operator": "ts_std_dev", "window": 22, "metadata": {}}
    with pytest.raises(ConfigError, match="MATRIX_ONLY"):
        generate_candidate_preview(
            d, dry(c), c, run_id="run_new", alpha_db=alpha_db(tmp_path / "alpha2.db"),
            machine_lib=machine_lib, extension_specs=[bad_vector],
        )


def test_extension_pruning_is_deterministic_and_preserves_std_windows():
    base = [{"candidate_id": f"base{i}"} for i in range(20)]
    core = [
        {"candidate_id": "core22", "operator": "ts_std_dev", "window": 22, "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
        {"candidate_id": "core66", "operator": "ts_std_dev", "window": 66, "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
        {"candidate_id": "corex", "operator": "ts_std_dev", "window": 22, "dataset_id": "d2", "field_id": "f2", "initial_selection_score": 1},
    ]
    targeted = [{"candidate_id": f"target{i}", "dataset_id": f"t{i}", "field_id": "f", "initial_selection_score": 1,
                 "_extension_priority": (0, 0, 0, i)} for i in range(5)]
    kept, report = _prune_extension_candidates(base, core, targeted)
    assert report["extension_cap"] == 4
    assert {"core22", "core66"} <= {x["candidate_id"] for x in kept}
    assert len(kept) == 4
    with pytest.raises(ConfigError, match="CORE_WINDOWS"):
        _prune_extension_candidates([{"candidate_id": "base"}], core[:2], [])


def test_extension_pruning_reserves_targeted_slot_and_returns_unused_capacity():
    base = [{"candidate_id": f"base{i}"} for i in range(20)]
    core = [
        {"candidate_id": "core22", "operator": "ts_std_dev", "window": 22, "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
        {"candidate_id": "core66", "operator": "ts_std_dev", "window": 66, "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
        {"candidate_id": "core_extra", "operator": "ts_std_dev", "window": 22, "dataset_id": "d2", "field_id": "f2", "initial_selection_score": 1},
        {"candidate_id": "core_extra_2", "operator": "ts_std_dev", "window": 66, "dataset_id": "d3", "field_id": "f3", "initial_selection_score": 1},
    ]
    targeted = [{"candidate_id": "target", "operator": "ts_quantile", "window": 22,
                 "dataset_id": "t", "field_id": "f", "initial_selection_score": 1,
                 "_extension_priority": (1, 0, 0, 0)}]
    kept, report = _prune_extension_candidates(base, core, targeted)
    assert report["extension_cap"] == 4
    assert report["targeted_reserved_capacity"] == 1
    assert "target" in {x["candidate_id"] for x in kept}
    assert len(kept) == 4

    kept_no_targeted, report_no_targeted = _prune_extension_candidates(base, core, [])
    assert report_no_targeted["unused_targeted_capacity_returned_to_core"] == 1
    assert len(kept_no_targeted) == 4


def test_extension_pruning_retains_same_operator_window_set_across_run_specific_ids():
    base = [{"candidate_id": f"base{i}"} for i in range(20)]

    def rows(prefix):
        return [
            {"candidate_id": prefix + "std22", "operator": "ts_std_dev", "window": 22, "dataset_id": "a", "field_id": "f", "initial_selection_score": 1},
            {"candidate_id": prefix + "std66", "operator": "ts_std_dev", "window": 66, "dataset_id": "a", "field_id": "f", "initial_selection_score": 1},
            {"candidate_id": prefix + "argmax", "operator": "ts_arg_max", "window": 22, "dataset_id": "b", "field_id": "f", "initial_selection_score": 1, "_extension_priority": (1, 0, 0, 0)},
            {"candidate_id": prefix + "quant", "operator": "ts_quantile", "window": 66, "dataset_id": "c", "field_id": "f", "initial_selection_score": 1, "_extension_priority": (1, 0, 0, 0)},
        ]

    def retained_shape(prefix):
        candidates = rows(prefix)
        kept, _ = _prune_extension_candidates(base, candidates[:2], candidates[2:])
        return {(x["operator"], x["window"], x["dataset_id"], x["field_id"]) for x in kept}

    assert retained_shape("run_a_") == retained_shape("run_b_")


def test_extension_execution_identity_is_opt_in_and_frozen_digest_drives_hash():
    c = cfg()
    historic_hash = c.execution_hash
    context = {
        "extension_policy_version": "CANDIDATE_EXTENSION_POLICY_V1",
        "normalized_source_semantic_identity_digest": "a" * 64,
        "canonical_evidence_digest": "b" * 64,
    }
    extension_config = config_with_extension_execution_identity(c, context)
    assert c.execution_hash == historic_hash
    assert extension_config.execution_hash != historic_hash
    assert validate_execution_hash_compatibility(c, historic_hash)["status"] == "EXACT_MATCH"
    assert validate_execution_hash_compatibility(
        c, extension_config.execution_hash, extension_identity=context,
    )["status"] == "EXTENSION_CONTEXT_MATCH"
    changed = dict(context, canonical_evidence_digest="c" * 64)
    assert validate_execution_hash_compatibility(
        c, extension_config.execution_hash, extension_identity=changed,
    )["status"] == "EXECUTION_DRIFT"


def test_frozen_extension_evidence_keeps_snapshot_identity_and_does_not_alias_source():
    source = {
        "source_run_id": "run_source", "source_round_id": "round_source",
        "source_last_completed_batch": 7, "source_search_consumed": 42,
        "source_ledger_fact_count": 99,
        "normalized_semantic_identity": {"region": "GLB"},
        "evidence": {"dataset_field": {("ds", "f"): {"attempts": 1, "ppl_success": 1}}},
    }
    context = _extension_context_from_source(source)
    assert context["source_run_id"] == "run_source"
    assert context["source_round_id"] == "round_source"
    assert context["source_last_completed_batch"] == 7
    assert context["source_search_consumed"] == 42
    assert context["source_ledger_fact_count"] == 99
    assert context["evidence_snapshot_created_at"]
    source["evidence"]["dataset_field"][("ds", "f")]["ppl_success"] = 999
    thawed = _thaw_evidence(context["evidence_snapshot"])
    assert thawed["dataset_field"][("ds", "f")]["ppl_success"] == 1


def test_rolling_specs_use_frozen_source_evidence_not_live_source_state():
    c = cfg(); d = discovery()
    source = {
        "source_run_id": "run_source",
        "evidence": {"dataset": {}, "dataset_field": {
            ("ds_a", "same_field"): {"attempts": 1, "ppl_success": 1},
        }},
        "normalized_semantic_identity": {"region": "GLB"},
    }
    context = _extension_context_from_source(source)
    frozen = {"source_run_id": "run_source", "evidence": _thaw_evidence(context["evidence_snapshot"])}
    # This simulates a source run gaining/changing evidence after the new
    # round was created.  Rolling must consult the frozen manifest snapshot.
    source["evidence"]["dataset_field"][("ds_a", "same_field")]["ppl_success"] = 0
    specs = _extension_specs_for_discovery(d, c, {"rolling_discovery": {}}, frozen)
    assert any(
        row["metadata"].get("extension_source") == "TARGETED_OPERATOR_EXTENSION"
        and row["field"]["dataset_id"] == "ds_a"
        for row in specs
    )


@pytest.mark.parametrize("action,expected", [
    ("NEW_SIMULATION_REQUIRED", True), ("RETRY_PER_V21_POLICY", True),
    ("CACHE_RESTORE", False), ("RESUME_EXISTING", False),
    ("HOLD_REMOTE_NOT_FOUND", False), ("STOP_INVALID", False),
    ("HOLD_UNCERTAIN", False),
])
def test_extension_post_quota_only_counts_real_post_actions(action, expected):
    assert requires_extension_new_post(action) is expected


def test_preflight_failure_leaves_no_round_artifacts(tmp_path, monkeypatch):
    """The preflight boundary must precede all run/round/candidate writes."""
    import ppl_engine.round_orchestrator as orchestrator

    c = cfg(); store = RunnerStore(tmp_path / "runner.db"); store.initialize(); ensure_round_schema(store)
    adb = alpha_db(tmp_path / "alpha.db")
    monkeypatch.setattr(orchestrator, "validate_machine_lib_hash", lambda *args, **kwargs: {"compatible": True})
    monkeypatch.setattr(
        orchestrator, "_candidate_pool_preflight",
        lambda *args, **kwargs: (_ for _ in ()).throw(ConfigError("CANDIDATE_EXTENSION_POOL_CAP_INSUFFICIENT_FOR_CORE_WINDOWS")),
    )
    policy = {"objective": "test", "total_budget": 12, "search_budget": 12, "repair_budget": 0}
    with pytest.raises(ConfigError, match="POOL_CAP"):
        _prepare_new_round(
            store, c, policy, machine_lib, None, adb, tmp_path / "evidence.json",
            requested_run_id="preflight_should_not_persist", offline=True,
        )
    with store.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM ppl_runs").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM ppl_candidates").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM ppl_rounds").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM ppl_dry_run_snapshots").fetchone()[0] == 0


def test_candidate_preflight_has_no_durable_dry_run_write():
    source = inspect.getsource(_candidate_pool_preflight)
    assert "save_dry_run" not in source


def test_extension_cap_uses_only_eligible_incoming_base_denominator():
    base = [{"candidate_id": f"base{i}"} for i in range(20)]
    core = [
        {"candidate_id": "core22", "operator": "ts_std_dev", "window": 22,
         "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
        {"candidate_id": "core66", "operator": "ts_std_dev", "window": 66,
         "dataset_id": "d", "field_id": "f", "initial_selection_score": 1},
    ]
    with pytest.raises(ConfigError, match="POOL_CAP"):
        _prune_extension_candidates(
            base, core, [], eligible_incoming_base_count=5,
            duplicate_skipped=3, persisted_duplicate_skipped=2, cache_only_base_excluded=10,
        )
    _kept, report = _prune_extension_candidates(
        base, core, [], eligible_incoming_base_count=10,
        duplicate_skipped=3, persisted_duplicate_skipped=2, cache_only_base_excluded=10,
    )
    assert report["base_count"] == 10
    assert report["eligible_incoming_base_count"] == 10
    assert report["duplicate_skipped"] == 3
    assert report["persisted_duplicate_skipped"] == 2
    assert report["cache_only_base_excluded"] == 10


def _setup_source(tmp_path):
    c = cfg()
    # The production YAML used by this focused fixture does not carry a PPL
    # classification block.  Add the minimum durable semantic context before
    # persisting the source run so the compatibility test exercises the real
    # normalized PPL-gate comparison rather than an absent-key edge case.
    c.rules["ppl_classification"] = {
        "fixed_gates": {"sharpe_min": 1.0},
        "final_theme_check": "TEST_THEME_GATE",
    }
    c.rules.setdefault("policy_versions", {})["ppl_classification"] = "TEST_PPL_V1"
    store = RunnerStore(tmp_path / "runner.db"); store.initialize(); ensure_round_schema(store)
    store.create_run("source", c)
    policy = {
        "ppl_classification": json.loads(json.dumps(c.rules["ppl_classification"])),
        "policy_versions": {"ppl_classification": "TEST_PPL_V1"},
        "objective": "x", "batch_size": 12,
    }
    create_round(store, round_id="round_source", run_id="source", policy=policy, total_budget=12, search_budget=12, repair_budget=0)
    return c, store, policy


def test_extension_source_semantics_fail_closed(tmp_path):
    c, store, policy = _setup_source(tmp_path)
    context = _extension_source_context(store, c, policy, "source")
    assert context["source_run_id"] == "source"
    # Pure theme presentation does not change source evidence semantics.
    c.rules["current_theme"] = {"changed": True}
    _extension_source_context(store, c, policy, "source")
    c.rules["ppl_classification"]["fixed_gates"]["sharpe_min"] = 999
    with pytest.raises(ConfigError, match="ppl_evidence_semantics"):
        _extension_source_context(store, c, policy, "source")


@pytest.mark.parametrize("key,value", [
    ("region", "USA"), ("universe", "TOP3000"), ("delay", 0),
    ("neutralization", "MARKET"), ("instrument_type", "FUTURES"),
])
def test_extension_source_simulation_semantics_each_fail_closed(tmp_path, key, value):
    c, store, policy = _setup_source(tmp_path)
    c.plan["simulation_settings"][key] = value
    with pytest.raises(ConfigError, match=key):
        _extension_source_context(store, c, policy, "source")


def test_extension_source_target_and_ppl_context_each_fail_closed(tmp_path):
    c, store, policy = _setup_source(tmp_path)
    c.plan["strategy"]["target_mode"] = "PPL"
    with pytest.raises(ConfigError, match="target_mode"):
        _extension_source_context(store, c, policy, "source")
    c, store, policy = _setup_source(tmp_path / "again")
    policy["ppl_classification"] = {"different": True}
    with pytest.raises(ConfigError, match="ppl_evidence_semantics"):
        _extension_source_context(store, c, policy, "source")


def test_extension_evidence_cli_is_start_round_only():
    from ppl_runner import _parser
    args = _parser().parse_args(["--start-round", "--extension-evidence-run", "run_0005"])
    assert args.extension_evidence_run == "run_0005"


def _insert_candidate(store, run_id, cid, operator, field, *, extension_source=None):
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
                dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,operator,window,decay,
                neutralization,legacy_unique_operator_count,pp_total_operator_count_estimate,pp_operator_estimator_version,
                lifecycle_state,simulation_status,created_at,updated_at,structure_status,cache_classification,execution_action,
                initial_selection_score,selected_for_initial_search,available_result_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PLANNED','NONE','now','now','ELIGIBLE','CACHE_MISS','NEW_SIMULATION_REQUIRED',100,0,'null')""",
            (cid, run_id, f"{operator}({field},22)", f"sk_{cid}", "{}", "h", f"ctx_{cid}", "d", field, "MATRIX", "RETURN",
             "NORMAL", f"d/{field}/IDENTITY/NORMAL/TS", "TS_ARG_EXTREME", operator, 22, 0, "SUBINDUSTRY", 1, 1, 1),
        )
        if extension_source:
            con.execute(
                "INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (f"p_{cid}", cid, run_id, f"sk_{cid}", f"ctx_{cid}", "d", "dry", json.dumps({"extension_source": extension_source}), "now", "now"),
            )


def test_targeted_selection_is_explore_only_and_capped_and_telemetry_is_attributed(tmp_path):
    c, store, _ = _setup_source(tmp_path)
    # Reuse the empty source round as a deterministic test round.
    for idx in range(3):
        _insert_candidate(store, "source", f"target{idx}", "ts_arg_min", f"f{idx}", extension_source="TARGETED_OPERATOR_EXTENSION")
    alpha = alpha_db(tmp_path / "alpha.db")
    policy = {
        "batch_size": 12, "exploration_fraction": 1.0, "adaptive_ranking": {},
        "ppl_classification": c.rules.get("ppl_classification", {}),
    }
    batch = _select_search_batch(store, alpha, "source", "round_source", policy, 12, batch_no=1)
    assert len(batch) == 1
    assert batch[0]["round_selection_mode"] == "EXPLORE"
    record_candidate_universe(store, "round_source", "source", store.load_candidates("source"))
    with store.connect() as con:
        row = con.execute("SELECT context_json FROM ppl_round_candidate_decisions WHERE candidate_id='target0' AND batch_no=0").fetchone()
    assert json.loads(row[0])["extension_source"] == "TARGETED_OPERATOR_EXTENSION"


def test_extension_provenance_is_copied_to_simulation_ledger_details(tmp_path):
    c, store, _ = _setup_source(tmp_path)
    _insert_candidate(store, "source", "ledger_target", "ts_quantile", "field_a",
                      extension_source="TARGETED_OPERATOR_EXTENSION")
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidate_provenance SET provenance_json=? WHERE candidate_id='ledger_target'",
            (json.dumps({
                "extension_source": "TARGETED_OPERATOR_EXTENSION",
                "parent_dataset_id": "d", "parent_field": "field_a",
                "trigger_evidence": {"paid_complete": 1, "ppl_success": 1},
            }),),
        )
    adb = alpha_db(tmp_path / "alpha.db")
    with sqlite3.connect(adb) as con:
        con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id,sharpe) VALUES (?,?,?,?,?)",
                    ("sk_ledger_target", "COMPLETE", "u", "a", 1.0))
    sync_simulation_ledger(
        store, adb, "round_source", "source", candidate_ids=["ledger_target"],
        origin_by_candidate={"ledger_target": "NEW_POST"},
    )
    with store.connect() as con:
        row = con.execute(
            "SELECT details_json FROM ppl_round_simulation_ledger WHERE round_id='round_source' AND sim_key='sk_ledger_target'"
        ).fetchone()
    details = json.loads(row[0])
    assert details["extension"]["extension_source"] == "TARGETED_OPERATOR_EXTENSION"
    assert details["extension"]["parent_dataset_id"] == "d"
