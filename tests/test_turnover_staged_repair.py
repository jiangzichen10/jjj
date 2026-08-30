"""Offline regression coverage for TURNOVER_STAGED_POLICY_V1."""

import hashlib
import json
import sqlite3
from pathlib import Path

import machine_lib_V2_1 as real_machine

from ppl_engine.config import load_effective_config
from ppl_engine.repair_engine import (
    TURNOVER_DECAY_STEP_1, TURNOVER_HUMP,
    canonical_high_turnover_evidence_state, is_retired_auto_repair_plan,
    plan_repairs, turnover_stage_spec,
)
from ppl_engine.store import RunnerStore
from ppl_engine.turnover_staged_repair import preview_turnover_staged_plans, sync_turnover_staged_plans

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-08-23T00:00:00+00:00"


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def candidate(cid="parent", *, decay=1, expression="rank(f1)", path=None):
    settings = real_machine.build_settings(
        {"expr": expression, "decay": int(decay)},
        neutralization="SUBINDUSTRY", region="USA", universe="TOP3000",
        delay=1, truncation=0.08, test_period="P0Y",
    )
    settings_json = json.dumps(settings, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "candidate_id": cid, "root_candidate_id": "parent", "dataset_id": "ds", "field_id": "f1",
        "field_type": "MATRIX", "vector_reducer": "IDENTITY", "operator": "rank", "window": None,
        "direction": "NORMAL", "expression": expression,
        "sim_key": real_machine.simulation_key(expression, settings),
        "settings_hash": hashlib.sha256(settings_json.encode("utf-8")).hexdigest(),
        "signal_family": "ds/f1/IDENTITY/NORMAL/RANK", "repair_depth": 0,
        "repair_path": path or ["RAW"], "decay": decay,
        "settings_json": settings_json,
    }


def _insert_candidate(store, row, *, status="COMPLETE", alpha_id="A1"):
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_candidates(
                 candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
                 dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,
                 operator,window,decay,vector_reducer,data_field_count_estimate,pp_total_operator_count_estimate,
                 structure_status,alpha_id,root_candidate_id,repair_depth,repair_path_json,lifecycle_state,
                 simulation_status,selected_for_initial_search,execution_action,cache_classification,
                 discovery_snapshot_id,dry_run_snapshot_id,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["candidate_id"], "run_x", row["expression"], row["sim_key"], row["settings_json"], row["settings_hash"],
             f"ctx_{row['candidate_id']}", row["dataset_id"], row["field_id"], row["field_type"], "RETURN",
             row["direction"], row["signal_family"], "RANK", row["operator"], row["window"], row["decay"],
             row["vector_reducer"], 1, 1, "ELIGIBLE", alpha_id, row["root_candidate_id"], row["repair_depth"],
             json.dumps(row["repair_path"]), "PRE_CHECK_REPAIR", status, 0, "CACHE_RESTORE", "CACHE_COMPLETE",
             "disc", "dry", NOW, NOW),
        )


def _alpha_db(path, rows):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE alpha_results (sim_key TEXT PRIMARY KEY,status TEXT)")
    for row in rows:
        c.execute("INSERT INTO alpha_results VALUES (?,?)", row)
    c.commit(); c.close()


def _resolved_turnover_check(store, cid, *, outcome="FAIL", raw="HIGH_TURNOVER", category="PPL_BASE"):
    sid = f"check_{cid}_{outcome}"
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_check_sessions(check_session_id,run_id,candidate_id,alpha_id,phase,
                 session_status,started_at,resolved_at,poll_count,http_request_count,pending_poll_requests,
                 base_gate_result,theme_gate_result,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, "run_x", cid, f"A_{cid}", "PRE_TAG", "RESOLVED", NOW, NOW, 1, 1, 0,
             "FAIL" if outcome in {"FAIL", "WARNING"} else "PASS", "PASS", NOW, NOW),
        )
        c.execute("INSERT INTO ppl_check_polls(poll_id,check_session_id,phase,semantic_poll_index,http_request_delta,pending,created_at) VALUES ((SELECT COALESCE(MAX(poll_id),0)+1 FROM ppl_check_polls),?,'PRE_TAG',1,1,0,?)", (sid, NOW))
        poll = c.execute("SELECT MAX(poll_id) FROM ppl_check_polls").fetchone()[0]
        c.execute(
            """INSERT INTO ppl_check_results(check_session_id,poll_id,candidate_id,alpha_id,phase,raw_name,
                 normalized_name,category,raw_result,normalized_result,unit_confidence,parser_version,
                 alias_version,evidence_source,threshold_exceeded,eligibility_outcome,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, poll, cid, f"A_{cid}", "PRE_TAG", raw, "TURNOVER", category, outcome, outcome,
             "UNKNOWN", 3, 3, "LIVE_CHECK", int(outcome in {"FAIL", "WARNING"}), outcome, NOW),
        )


def _store_with_executed_stage1(tmp_path):
    conf = cfg(); store = RunnerStore(tmp_path / "runner.db"); store.initialize(); store.create_run("run_x", conf)
    origin = candidate(); child = candidate("stage1", decay=3, path=["RAW", "TURNOVER_ABOVE_BASE_MAX:TURNOVER_DECAY_STEP_1"])
    _insert_candidate(store, origin); _insert_candidate(store, child)
    spec = turnover_stage_spec(origin, 1)
    signature = spec["repair_signature"]
    with store.connect() as c:
        c.execute("INSERT INTO ppl_operator_capabilities(operator_name,signature_hash,capability_class,status) VALUES ('hump','h','VERIFIED','VERIFIED_PROJECT')")
        c.execute(
            """INSERT INTO ppl_repair_plans(repair_plan_id,run_id,parent_candidate_id,root_candidate_id,
                 target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("p1", "run_x", "parent", "parent", "TURNOVER_ABOVE_BASE_MAX", TURNOVER_DECAY_STEP_1,
             signature, json.dumps(spec["repair_path"] if "repair_path" in spec else []), 1, json.dumps(spec), "[]",
             "EXECUTED", 1, 1, 1, NOW, NOW),
        )
        c.execute("INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,repair_signature,repair_path_json,repair_depth,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  ("e1", "run_x", "parent", "stage1", TURNOVER_DECAY_STEP_1, signature, "[]", 1, NOW))
    adb = tmp_path / "alpha.db"; _alpha_db(adb, [(child["sim_key"], "COMPLETE")])
    return store, conf, adb


def test_canonical_predicate_excludes_ht_aliases_and_unknown():
    good = {"raw_name": "HIGH_TURNOVER", "normalized_name": "TURNOVER", "category": "PPL_BASE", "eligibility_outcome": "WARNING"}
    assert canonical_high_turnover_evidence_state(good) == "BLOCKED"
    for raw, category in (("HT_TURNOVER", "PPL_THEME"), ("HT_HIGH_TURNOVER_RETURNS_RATIO", "PPL_THEME"), ("HIGH_TURNOVER_RETURNS_RATIO", "PPL_THEME")):
        assert canonical_high_turnover_evidence_state({**good, "raw_name": raw, "category": category}) == "INSUFFICIENT"
    assert canonical_high_turnover_evidence_state({}) == "INSUFFICIENT"


def test_ht_ratio_and_historical_turnover_plans_are_retired():
    assert plan_repairs(candidate(), {"primary_failure": "HT_RETURNS_RATIO_FAIL"}, cfg().rules)["plans"] == []
    assert is_retired_auto_repair_plan({"repair_type": "HT_RATIO_TARGET_TVR", "candidate_spec_json": "{}"})
    assert is_retired_auto_repair_plan({"repair_type": "TURNOVER_HIGH_TS_MEAN_2", "candidate_spec_json": "{}"})
    assert is_retired_auto_repair_plan({"repair_type": "X", "candidate_spec_json": '{"expression_preview":"target_tvr(x)"}'})


def test_stage1_uses_base_plus_two_and_preserves_other_settings():
    spec = plan_repairs(candidate(decay=1), {"primary_failure": "TURNOVER_ABOVE_BASE_MAX"}, cfg().rules)["plans"][0]
    assert spec["repair_type"] == "TURNOVER_DECAY_STEP_1"
    assert spec["settings_override"] == {"decay": 3}
    assert spec["expression_preview"] == "rank(f1)"


def test_existing_durable_turnover_diagnosis_bootstraps_one_stage1_per_family(tmp_path):
    conf = cfg(); store = RunnerStore(tmp_path / "runner.db"); store.initialize(); store.create_run("run_x", conf)
    parent = candidate(); _insert_candidate(store, parent)
    with store.connect() as c:
        c.execute(
            """INSERT INTO ppl_diagnoses(diagnosis_id,run_id,candidate_id,alpha_id,source_phase,evidence_source,
                 primary_failure,secondary_failures_json,severity,repairability,root_cause,metrics_snapshot_json,
                 check_result_ids_json,diagnosis_rule_version,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("d1", "run_x", "parent", "A1", "SIMULATION", "LIVE_SIMULATION",
             "TURNOVER_ABOVE_BASE_MAX", "[]", "MEDIUM", "REPAIRABLE", "TURNOVER_ABOVE_BASE_MAX",
             json.dumps({"sharpe": 2.0, "turnover": 0.9}), "[]", 2, NOW),
        )
    adb = tmp_path / "alpha.db"; _alpha_db(adb, [(parent["sim_key"], "COMPLETE")])
    out = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert len(out["bootstrapped_stage1_plan_ids"]) == 1
    assert sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")["bootstrapped_stage1_plan_ids"] == []


def test_existing_resolved_check_unlocks_stage2_without_refresh(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    _resolved_turnover_check(store, "stage1", outcome="FAIL")
    calls = []
    out = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x",
                                     refresh_check=lambda cid: calls.append(cid) or {})
    assert calls == [] and len(out["created_plan_ids"]) == 1
    plan = next(p for p in store.load_repair_plans("run_x") if p["repair_plan_id"] == out["created_plan_ids"][0])
    spec = json.loads(plan["candidate_spec_json"])
    assert plan["repair_type"] == "TURNOVER_DECAY_STEP_2"
    assert spec["settings_override"]["decay"] == 5


def test_clear_stage1_check_stops_stage2(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    _resolved_turnover_check(store, "stage1", outcome="PASS")
    out = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert out["created_plan_ids"] == []


def test_missing_check_uses_single_conditional_refresh(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    calls = []
    def refresh(cid):
        calls.append(cid); _resolved_turnover_check(store, cid, outcome="FAIL"); return {"executed": True}
    out = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x", refresh_check=refresh)
    assert calls == ["stage1"] and len(out["created_plan_ids"]) == 1


def test_planned_stage_does_not_unlock_and_stage3_template_keeps_base_plus_four(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    with store.connect() as c:
        c.execute("UPDATE ppl_repair_plans SET plan_status='PLANNED' WHERE repair_plan_id='p1'")
    assert sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")["created_plan_ids"] == []
    spec = turnover_stage_spec(candidate("stage2", decay=5), 3, origin_candidate_id="parent",
                               origin_signal_family="ds/f1/IDENTITY/NORMAL/RANK", base_decay=1,
                               previous_stage_candidate_id="stage2")
    assert spec["repair_type"] == TURNOVER_HUMP
    assert spec["expression_preview"] == "hump(rank(f1), hump=0.01)"
    assert spec["settings_override"]["decay"] == 5


def test_stage2_unlocks_one_hump_and_stage3_exhaustion_is_scoped(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    _resolved_turnover_check(store, "stage1", outcome="FAIL")
    stage2_id = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")["created_plan_ids"][0]
    stage2_plan = next(p for p in store.load_repair_plans("run_x") if p["repair_plan_id"] == stage2_id)
    stage2 = candidate("stage2", decay=5, path=["RAW", "TURNOVER_ABOVE_BASE_MAX:TURNOVER_DECAY_STEP_1",
                                                "TURNOVER_ABOVE_BASE_MAX:TURNOVER_DECAY_STEP_2"])
    _insert_candidate(store, stage2, alpha_id="A2")
    with store.connect() as c:
        c.execute("UPDATE ppl_repair_plans SET plan_status='EXECUTED',committed_posts=1,consumed_posts=1 WHERE repair_plan_id=?", (stage2_id,))
        c.execute("INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,repair_signature,repair_path_json,repair_depth,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  ("e2", "run_x", "stage1", "stage2", "TURNOVER_DECAY_STEP_2",
                   stage2_plan["repair_signature"], "[]", 2, NOW))
    with sqlite3.connect(adb) as c:
        c.execute("INSERT INTO alpha_results VALUES (?,'COMPLETE')", (stage2["sim_key"],))
    _resolved_turnover_check(store, "stage2", outcome="FAIL")
    stage3_out = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert len(stage3_out["created_plan_ids"]) == 1
    stage3_id = stage3_out["created_plan_ids"][0]
    stage3_plan = next(p for p in store.load_repair_plans("run_x") if p["repair_plan_id"] == stage3_id)
    stage3_spec = json.loads(stage3_plan["candidate_spec_json"])
    assert stage3_plan["repair_type"] == "TURNOVER_HUMP"
    assert stage3_spec["expression_preview"] == "hump(rank(f1), hump=0.01)"
    assert stage3_spec["settings_override"]["decay"] == 5

    stage3 = candidate("stage3", decay=5, expression="hump(rank(f1), hump=0.01)",
                       path=stage3_spec["repair_path"])
    _insert_candidate(store, stage3, alpha_id="A3")
    with store.connect() as c:
        c.execute("UPDATE ppl_repair_plans SET plan_status='EXECUTED',committed_posts=1,consumed_posts=1 WHERE repair_plan_id=?", (stage3_id,))
        c.execute("INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,repair_signature,repair_path_json,repair_depth,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  ("e3", "run_x", "stage2", "stage3", "TURNOVER_HUMP",
                   stage3_plan["repair_signature"], "[]", 3, NOW))
    with sqlite3.connect(adb) as c:
        c.execute("INSERT INTO alpha_results VALUES (?,'COMPLETE')", (stage3["sim_key"],))
    _resolved_turnover_check(store, "stage3", outcome="FAIL")
    exhausted = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")["exhausted"]
    assert exhausted == [{
        "origin_candidate_id": "parent",
        "origin_signal_family": "ds/f1/IDENTITY/NORMAL/RANK",
        "policy": "TURNOVER_STAGED_POLICY_V1", "run_id": "run_x", "round_id": "round_x",
    }]


def test_read_only_turnover_preview_matches_stage2_materialization_without_writes(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    _resolved_turnover_check(store, "stage1", outcome="FAIL")
    before = len(store.load_repair_plans("run_x"))
    preview = preview_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert preview["evaluation_complete"] is True
    assert preview["writes"] == 0 and preview["network_requests"] == 0
    assert len(store.load_repair_plans("run_x")) == before
    virtual = preview["virtual_plan_rows"]
    assert len(virtual) == 1 and virtual[0]["repair_type"] == "TURNOVER_DECAY_STEP_2"
    actual = sync_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert actual["created_plan_ids"] == [virtual[0]["repair_plan_id"]]


def test_read_only_turnover_preview_marks_refresh_required_incomplete(tmp_path):
    store, conf, adb = _store_with_executed_stage1(tmp_path)
    before = len(store.load_repair_plans("run_x"))
    preview = preview_turnover_staged_plans(store, conf, adb, "run_x", "round_x")
    assert preview["evaluation_complete"] is False
    assert preview["virtual_plan_rows"] == []
    assert preview["incomplete_reasons"][0]["reason"] == "TURNOVER_CHECK_REFRESH_REQUIRED"
    assert preview["network_requests"] == 0 and preview["check_requests"] == 0 and preview["writes"] == 0
    assert len(store.load_repair_plans("run_x")) == before
