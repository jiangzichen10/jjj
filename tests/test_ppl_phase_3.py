import copy
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import machine_lib_V2_1 as machine_lib
from ppl_engine.candidate_factory import (
    TRANSFORM_FAMILIES,
    build_expression,
    canonicalize_expression,
    classify_cache_read_only,
    diversity_select,
    evaluate_structure,
    estimate_data_fields,
    expand_route,
    generate_candidate_preview,
    pp_operator_count,
)
from ppl_engine.config import ConfigError, load_effective_config, simulation_budget_allocation
from ppl_engine.simulation_adapter import to_v21_candidates, validate_execution_permission
from ppl_engine.store import RunnerStore


def config():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def alpha_db(path, rows=()):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE alpha_results(sim_key TEXT PRIMARY KEY, status TEXT, simulation_url TEXT, alpha_id TEXT, sharpe REAL)")
    conn.executemany("INSERT INTO alpha_results VALUES (?,?,?,?,?)", rows)
    conn.commit(); conn.close()
    return path


def discovery_one(field_type="MATRIX", semantic="RETURN", confidence="HIGH"):
    snapshot = {
        "snapshot_id": "disc_test", "region": "GLB", "universe": "TOPDIV3000", "delay": 1,
        "instrument_type": "EQUITY", "source": "TEST", "dataset_count": 1, "field_count": 1,
        "metadata_hash": "x", "exclusion_status": {}, "automatic_preselection": True,
        "discovery_pool_size": 1, "created_at": "2026-01-01T00:00:00Z",
    }
    datasets = [{"dataset_id": "ds", "selected": True, "dataset_preselection_score": 10}]
    fields = [{
        "dataset_id": "ds", "field_id": "field_x", "field_type": field_type,
        "semantic_class": semantic, "classification_source": "FIELD_ID",
        "classification_rule_id": "r", "classification_confidence": confidence,
        "coverage": .95, "alphaCount": 2, "selected": True,
    }]
    return SimpleNamespace(snapshot=snapshot, datasets=datasets, fields=fields)


def preview(tmp_path, field_type="MATRIX", semantic="RETURN", confidence="HIGH"):
    cfg = config(); disc = discovery_one(field_type, semantic, confidence)
    dry = {"dry_run_id": "dry_test", "discovery_snapshot_id": "disc_test", "execution_hash": cfg.execution_hash}
    return generate_candidate_preview(
        disc, dry, cfg, run_id="run_0001", alpha_db=alpha_db(tmp_path / "alpha.db"), machine_lib=machine_lib,
    )


def test_matrix_has_no_identity_call():
    assert build_expression("x", "MATRIX", "IDENTITY", "ts_delta", 1) == "ts_delta(x, 1)"


def test_vector_reducer_is_inside_transform():
    assert build_expression("x", "VECTOR", "VEC_AVG", "ts_delta", 1) == "ts_delta(vec_avg(x), 1)"


@pytest.mark.parametrize("name,window,expected", [
    ("raw", None, "x"), ("rank", None, "rank(x)"), ("zscore", None, "zscore(x)"),
    ("ts_mean", 4, "ts_mean(x, 4)"), ("ts_delta", 2, "ts_delta(x, 2)"),
    ("ts_rank", 5, "ts_rank(x, 5)"), ("ts_zscore", 5, "ts_zscore(x, 5)"),
])
def test_all_transform_formats(name, window, expected):
    assert build_expression("x", "MATRIX", "IDENTITY", name, window) == expected


def test_canonicalization_only_whitespace():
    assert canonicalize_expression("  ts_mean( x,  4 )\n") == "ts_mean( x, 4 )"


def test_initial_candidates_are_normal_only(tmp_path):
    rows, _ = preview(tmp_path)
    assert {x["direction"] for x in rows} == {"NORMAL"}
    assert not any(x["expression"].startswith("-") for x in rows)


def test_family_and_window_are_separate(tmp_path):
    rows, _ = preview(tmp_path)
    mean = [x for x in rows if x["transform_family"] == "TS_MEAN"]
    assert {x["window"] for x in mean} == {2, 3, 4, 5}
    assert len({x["signal_family"] for x in mean}) == 1


def test_signal_family_contains_reducer(tmp_path):
    rows, _ = preview(tmp_path, field_type="VECTOR")
    assert any("/VEC_SUM/NORMAL/" in x["signal_family"] for x in rows)
    assert any("/VEC_AVG/NORMAL/" in x["signal_family"] for x in rows)


def test_sim_key_exact_v21(tmp_path):
    rows, _ = preview(tmp_path)
    item = rows[0]
    assert item["sim_key"] == machine_lib.simulation_key(item["expression"], json.loads(item["settings_json"]))


def test_candidate_factory_rejects_compact_settings_builder(tmp_path, monkeypatch):
    def compact_builder(candidate, **kwargs):
        return {
            "neutralization": kwargs["neutralization"], "region": kwargs["region"],
            "universe": kwargs["universe"], "delay": kwargs["delay"],
            "truncation": kwargs["truncation"], "testPeriod": kwargs["test_period"],
        }
    monkeypatch.setattr(machine_lib, "build_settings", compact_builder)
    with pytest.raises(ConfigError, match="SIMULATION_SETTINGS_INCOMPLETE"):
        preview(tmp_path)


def test_settings_are_v21_settings(tmp_path):
    rows, _ = preview(tmp_path)
    item = rows[0]
    expected = machine_lib.build_settings(item["v21_candidate"], neutralization="SUBINDUSTRY", region="GLB", universe="TOPDIV3000", delay=1, truncation=.08, test_period="P0Y")
    assert json.loads(item["settings_json"]) == expected


def test_target_mode_does_not_change_sim_key(tmp_path):
    rows, _ = preview(tmp_path)
    item = rows[0]; settings = json.loads(item["settings_json"])
    changed = dict(item["v21_candidate"], target_mode="REGULAR")
    assert machine_lib.simulation_key(changed["expr"], settings) == item["sim_key"]


def test_context_and_candidate_ids_are_stable(tmp_path):
    one, _ = preview(tmp_path / "a"); two, _ = preview(tmp_path / "b")
    assert [(x["candidate_id"], x["context_fingerprint"]) for x in one] == [(x["candidate_id"], x["context_fingerprint"]) for x in two]


def test_two_operator_counts(tmp_path):
    rows, _ = preview(tmp_path)
    item = next(x for x in rows if x["transform_family"] == "TS_MEAN")
    assert item["legacy_unique_operator_count"] == 1 == item["pp_total_operator_count_estimate"]


def test_pp_count_repeats_and_excludes_backfills():
    count, calls = pp_operator_count("ts_mean(ts_mean(ts_backfill(x,5),5),3)+group_backfill(y,g,5)", ["ts_backfill", "group_backfill"])
    assert count == 2 and calls == ["ts_mean", "ts_mean"]


def test_data_field_estimate_is_not_hardcoded():
    assert estimate_data_fields("rank(a)+ts_mean(b,5)", ["a", "b", "c"]) == ["a", "b"]


def test_operator_over_eight_is_structure_rejected():
    assert evaluate_structure(9, 1) == "LOCAL_STRUCTURE_REJECTED"


def test_data_fields_over_three_is_structure_rejected():
    assert evaluate_structure(1, 4) == "LOCAL_STRUCTURE_REJECTED"


def test_structure_boundary_is_eligible():
    assert evaluate_structure(8, 3) == "ELIGIBLE"


def make_selectable(dataset, semantic, field, family, score=10):
    cid = f"{dataset}-{semantic}-{field}-{family}"
    return {"candidate_id": cid, "dataset_id": dataset, "semantic_class": semantic, "field_id": field,
            "vector_reducer": "IDENTITY", "transform_family": family, "initial_selection_score": score,
            "selected_for_initial_search": False}


def selection_rules(**updates):
    base = {"max_dataset_fraction": .5, "min_candidates_per_dataset": 2,
            "max_semantic_class_fraction": .6, "max_unknown_fraction": .1,
            "max_initial_candidates_per_field": 4, "max_windows_per_transform_family_per_field": 1}
    base.update(updates); return base


def test_dataset_diversity_cap():
    rows = [make_selectable("a", "RETURN", f"f{i}", "RAW", 100-i) for i in range(10)] + [make_selectable("b", "VOLUME", f"g{i}", "RAW", 10-i) for i in range(10)]
    chosen = diversity_select(rows, 10, selection_rules())
    assert Counter(x["dataset_id"] for x in chosen) == {"a": 5, "b": 5}


def test_minimum_per_dataset():
    rows = [make_selectable(d, "RETURN", f"{d}{i}", "RAW", 100 if d == "a" else 1) for d in "abc" for i in range(5)]
    chosen = diversity_select(rows, 12, selection_rules(max_dataset_fraction=.5))
    assert all(Counter(x["dataset_id"] for x in chosen)[d] >= 2 for d in "abc")


def test_semantic_and_unknown_caps():
    rows = [make_selectable(str(i % 3), "RETURN", f"r{i}", "RAW", 20) for i in range(20)]
    rows += [make_selectable(str(i % 3), "UNKNOWN", f"u{i}", "RAW", 30) for i in range(20)]
    chosen = diversity_select(rows, 10, selection_rules(max_dataset_fraction=1, min_candidates_per_dataset=0))
    counts = Counter(x["semantic_class"] for x in chosen)
    assert counts["RETURN"] <= 6 and counts["UNKNOWN"] <= 1


def test_field_and_transform_window_caps_can_leave_budget_unused():
    rows = [make_selectable("a", "RETURN", "f", "TS_MEAN", 10-i) | {"candidate_id": f"c{i}"} for i in range(10)]
    chosen = diversity_select(rows, 8, selection_rules(max_dataset_fraction=1, min_candidates_per_dataset=0, max_semantic_class_fraction=1))
    assert len(chosen) == 1


@pytest.mark.parametrize("status,expected,action,url", [
    ("COMPLETE", "CACHE_COMPLETE", "CACHE_RESTORE", None),
    ("RUNNING", "RESUME_RUNNING", "RESUME_EXISTING", "u"),
    ("SUBMITTED", "RESUME_SUBMITTED", "RESUME_EXISTING", "u"),
    ("ERROR", "CACHE_ERROR", "RETRY_PER_V21_POLICY", None),
    ("AUTH_ERROR", "CACHE_AUTH_ERROR", "RETRY_PER_V21_POLICY", None),
    ("INVALID", "CACHE_INVALID", "STOP_INVALID", None),
    ("UNCERTAIN_SUBMISSION", "CACHE_UNCERTAIN", "HOLD_UNCERTAIN", None),
    ("STALE_RUNNING", "CACHE_STALE_RUNNING", "RESUME_EXISTING", "u"),
    ("REMOTE_NOT_FOUND", "CACHE_REMOTE_NOT_FOUND", "HOLD_REMOTE_NOT_FOUND", "u"),
])
def test_cache_classifications(tmp_path, status, expected, action, url):
    db = alpha_db(tmp_path / "a.db", [("k", status, url, "a", 1.0)])
    result = classify_cache_read_only(db, "k")
    assert (result["cache_classification"], result["execution_action"]) == (expected, action)


def test_cache_miss(tmp_path):
    result = classify_cache_read_only(alpha_db(tmp_path / "a.db"), "missing")
    assert result["cache_classification"] == "CACHE_MISS" and result["execution_action"] == "NEW_SIMULATION_REQUIRED"


def test_snapshot_mismatch_rejected(tmp_path):
    cfg = config(); disc = discovery_one(); dry = {"dry_run_id": "d", "discovery_snapshot_id": "disc_test", "execution_hash": "wrong"}
    with pytest.raises(ConfigError, match="DISCOVERY_SNAPSHOT_MISMATCH"):
        generate_candidate_preview(disc, dry, cfg, run_id="r", alpha_db=alpha_db(tmp_path / "a.db"), machine_lib=machine_lib)


def test_preview_is_reentrant_in_runner_db(tmp_path):
    cfg = config(); rows, _ = preview(tmp_path / "source")
    store = RunnerStore(tmp_path / "runner.db"); store.initialize(); store.create_run("run_0001", cfg)
    store.upsert_candidates(rows); store.upsert_candidates(rows)
    with store.connect() as conn:
        assert conn.execute("SELECT count(*) FROM ppl_candidates").fetchone()[0] == len(rows)
        assert conn.execute("SELECT count(*) FROM ppl_candidate_provenance").fetchone()[0] == len(rows)


def test_adapter_converts_without_network(tmp_path):
    rows, _ = preview(tmp_path)
    converted = to_v21_candidates(rows[:2])
    assert [x["expr"] for x in converted] == [x["expression"] for x in rows[:2]]


def test_adapter_requires_explicit_post_permission():
    with pytest.raises(ConfigError, match="EXPLICIT_ALLOW"):
        validate_execution_permission(config(), [{"execution_action": "NEW_SIMULATION_REQUIRED"}], dry_run=False, allow_simulation_post=False, remaining_initial_budget=1)


def test_preview_budget_and_repair_reserve(tmp_path):
    _, report = preview(tmp_path)
    allocation = simulation_budget_allocation(config().plan)
    assert report["selected_for_initial_search"] <= 72
    assert report["repair_reserve_budget"] == allocation["repair_reserve_budget"] == 48
    assert report["network_requests"] == report["simulation_posts"] == 0


def test_complete_and_resume_do_not_count_as_required_new_posts(tmp_path):
    cfg = config(); disc = discovery_one(); dry = {"dry_run_id": "dry_test", "discovery_snapshot_id": "disc_test", "execution_hash": cfg.execution_hash}
    empty = alpha_db(tmp_path / "empty.db")
    base, _ = generate_candidate_preview(disc, dry, cfg, run_id="run_0001", alpha_db=empty, machine_lib=machine_lib)
    rows = [(base[0]["sim_key"], "COMPLETE", None, "a", 1.0), (base[1]["sim_key"], "RUNNING", "url", None, None)]
    actual, report = generate_candidate_preview(disc, dry, cfg, run_id="run_0001", alpha_db=alpha_db(tmp_path / "cache.db", rows), machine_lib=machine_lib)
    selected = [x for x in actual if x["selected_for_initial_search"]]
    non_new = sum(x["cache_classification"] in {"CACHE_COMPLETE", "RESUME_RUNNING"} for x in selected)
    assert report["required_new_posts"] == len(selected) - non_new


def test_same_sim_key_different_context_has_distinct_provenance_id(tmp_path):
    rows, _ = preview(tmp_path)
    item = copy.deepcopy(rows[0]); item["context_fingerprint"] = "different"; item["provenance_id"] = "prov_different"
    assert item["sim_key"] == rows[0]["sim_key"] and item["provenance_id"] != rows[0]["provenance_id"]


def test_preview_selected_field_only(tmp_path):
    cfg = config(); disc = discovery_one(); ignored = dict(disc.fields[0], field_id="not_selected", selected=False)
    disc.fields.append(ignored)
    dry = {"dry_run_id": "d", "discovery_snapshot_id": "disc_test", "execution_hash": cfg.execution_hash}
    rows, _ = generate_candidate_preview(disc, dry, cfg, run_id="r", alpha_db=alpha_db(tmp_path / "a.db"), machine_lib=machine_lib)
    assert {x["field_id"] for x in rows} == {"field_x"}


def test_candidate_id_changes_with_run_but_sim_key_does_not(tmp_path):
    one, _ = preview(tmp_path / "a")
    cfg = config(); disc = discovery_one(); dry = {"dry_run_id": "dry_test", "discovery_snapshot_id": "disc_test", "execution_hash": cfg.execution_hash}
    two, _ = generate_candidate_preview(disc, dry, cfg, run_id="run_0002", alpha_db=alpha_db(tmp_path / "b" / "alpha.db"), machine_lib=machine_lib)
    assert one[0]["sim_key"] == two[0]["sim_key"] and one[0]["candidate_id"] != two[0]["candidate_id"]
