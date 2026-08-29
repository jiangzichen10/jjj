import copy
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.contracts import ExecutionAction, SimulationFreshness, SimulationStatus
from ppl_engine.reconcile import (
    IMMUTABLE_CANDIDATE_FIELDS,
    MUTABLE_WORKFLOW_FIELDS,
    apply_offline_reconcile,
    build_state_from_db,
    derive_simulation_state,
    plan_offline_reconcile,
    write_reconciled_outputs,
)
from ppl_engine.runner_lock import RunnerAlreadyActive, SingleRunnerLock
from ppl_engine.state_machine import CANDIDATE_TRANSITIONS, RUN_TRANSITIONS
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan.yaml", project_dir=ROOT)


def alpha_db(path, rows=()):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE alpha_results(sim_key TEXT PRIMARY KEY,status TEXT,simulation_url TEXT,alpha_id TEXT)")
    conn.executemany("INSERT INTO alpha_results VALUES (?,?,?,?)", rows)
    conn.commit(); conn.close(); return path


def setup_store(tmp_path, statuses=(None,), selected=True, with_snapshot=True):
    config = cfg(); store = RunnerStore(tmp_path / "runner.db"); store.initialize(); store.create_run("run_0001", config)
    now = "2026-01-01T00:00:00Z"
    with store.connect() as conn:
        if with_snapshot:
            conn.execute("""INSERT INTO ppl_discovery_snapshots VALUES
                ('disc', 'GLB','TOPDIV3000',1,'EQUITY','TEST',1,1,'h','{}',1,1,?)""", (now,))
        for i, status in enumerate(statuses):
            conn.execute("""INSERT INTO ppl_candidates(
                candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,
                dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,
                lifecycle_state,simulation_status,created_at,updated_at,discovery_snapshot_id,
                dry_run_snapshot_id,selected_for_initial_search,execution_action,cache_classification
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f"c{i}","run_0001",f"rank(f{i})",f"k{i}","{}","sh",f"ctx{i}","ds",f"f{i}",
             "MATRIX","RETURN","NORMAL",f"ds/f{i}/IDENTITY/NORMAL/RANK","RANK","PLANNED",
             "NONE",now,now,"disc","dry",int(selected),"NEW_SIMULATION_REQUIRED","CACHE_MISS"))
    rows=[]
    for i,status in enumerate(statuses):
        if status:
            url = f"url{i}" if status in {"RUNNING","SUBMITTED"} else None
            rows.append((f"k{i}",status,url,f"a{i}" if status=="COMPLETE" else None))
    return store, config, alpha_db(tmp_path / "alpha.db", rows)


def test_three_state_dimensions_are_separate(tmp_path):
    store, config, adb = setup_store(tmp_path, ("SUBMITTED",))
    plan = plan_offline_reconcile(store,"run_0001",config,adb); desired=plan["changes"][0]["desired"]
    assert desired["lifecycle_state"] == "PLANNED"
    assert desired["simulation_status"] == "SUBMITTED"
    assert desired["execution_action"] == "RESUME_EXISTING"


def test_allowed_candidate_transition_atomic_audit(tmp_path):
    store, _, _ = setup_store(tmp_path)
    assert store.transition_candidate("c0","SIMULATION_COMPLETE",reason="cache",source="CACHE_RECONCILE",allowed=CANDIDATE_TRANSITIONS)
    with store.connect() as c:
        assert c.execute("select lifecycle_state from ppl_candidates where candidate_id='c0'").fetchone()[0] == "SIMULATION_COMPLETE"
        assert c.execute("select count(*) from ppl_state_transitions").fetchone()[0] == 1


def test_illegal_transition_rejected(tmp_path):
    store, _, _ = setup_store(tmp_path)
    with pytest.raises(ValueError, match="STATE_TRANSITION_REJECTED"):
        store.transition_candidate("c0","SUBMITTED",reason="bad",source="TEST",allowed=CANDIDATE_TRANSITIONS)


def test_transition_rolls_back_if_audit_fails(tmp_path):
    store, _, _ = setup_store(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        store.transition_candidate("c0","SIMULATION_COMPLETE",reason=None,source="TEST",allowed=CANDIDATE_TRANSITIONS)
    with store.connect() as c:
        assert c.execute("select lifecycle_state from ppl_candidates").fetchone()[0] == "PLANNED"
        assert c.execute("select count(*) from ppl_state_transitions").fetchone()[0] == 0


@pytest.mark.parametrize("status,action,fresh,live", [
    ("COMPLETE","CACHE_RESTORE","CONFIRMED_COMPLETE",False),
    ("RUNNING","RESUME_EXISTING","STALE_NONTERMINAL",True),
    ("SUBMITTED","RESUME_EXISTING","STALE_NONTERMINAL",True),
    ("INVALID","STOP_INVALID","LOCAL_ERROR",False),
    ("UNCERTAIN_SUBMISSION","HOLD_UNCERTAIN","UNKNOWN",True),
    ("ERROR","RETRY_PER_V21_POLICY","LOCAL_ERROR",False),
    ("AUTH_ERROR","RETRY_PER_V21_POLICY","LOCAL_ERROR",False),
    ("STALE_RUNNING","RESUME_EXISTING","STALE_NONTERMINAL",True),
    ("REMOTE_NOT_FOUND","HOLD_REMOTE_NOT_FOUND","REMOTE_TERMINAL",False),
])
def test_offline_status_semantics(status, action, fresh, live):
    result=derive_simulation_state({"status":status,"simulation_url":"u"})
    assert (result["execution_action"],result["simulation_freshness"],result["live_reconcile_required"]) == (action,fresh,live)


def test_cache_miss_stays_planned(tmp_path):
    store, config, adb=setup_store(tmp_path)
    plan=plan_offline_reconcile(store,"run_0001",config,adb)
    assert plan["changes"] == [] and plan["selected_new_simulation_required"] == 1


def test_complete_transitions_only_to_simulation_complete(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",))
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    assert result["candidate_transitions_applied"] == 1
    assert store.load_candidates("run_0001")[0]["lifecycle_state"] == "SIMULATION_COMPLETE"


def test_preview_has_zero_writes(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",))
    before=store.load_candidates("run_0001")[0].copy()
    plan=plan_offline_reconcile(store,"run_0001",config,adb)
    after=store.load_candidates("run_0001")[0]
    assert before == after and store.transition_counts("run_0001") == {} and plan["network_requests"] == 0


def test_reconcile_is_idempotent(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",None))
    first=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    second=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    assert first["candidate_transitions_applied"] == 1 and second["candidate_transitions_applied"] == 0
    assert second["run_transitions_applied"] == 0


def test_run_transitions_are_audited(tmp_path):
    store, config, adb=setup_store(tmp_path)
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    assert result["run_transitions_applied"] == 3
    assert store.get_run("run_0001")["status"] == "READY_FOR_EXECUTION"
    assert store.transition_counts("run_0001")["RUN"] == 3


def test_execution_hash_mismatch_rejected(tmp_path):
    store, config, adb=setup_store(tmp_path)
    # Contract update: a hash that matches neither the current (V2) nor the
    # legacy (V1) schema is now classified UNRESOLVED (still hard-blocked).
    with pytest.raises(ConfigError,match="EXECUTION_HASH_UNRESOLVED"):
        plan_offline_reconcile(store,"run_0001",replace(config,execution_hash="changed"),adb)


@pytest.mark.parametrize("field", ["operational_hash","presentation_hash"])
def test_non_execution_hash_changes_allowed(tmp_path, field):
    store, config, adb=setup_store(tmp_path)
    changed=replace(config, **{field:"changed"})
    plan=plan_offline_reconcile(store,"run_0001",changed,adb)
    assert plan["operational_revision_changed"] if field=="operational_hash" else plan["presentation_changed"]


def test_missing_snapshot_is_warning_not_failure(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",),with_snapshot=False)
    plan=plan_offline_reconcile(store,"run_0001",config,adb)
    assert "PROVENANCE_SNAPSHOT_MISSING" in plan["warnings"]
    assert plan["changes"][0]["desired"]["simulation_status"] == "COMPLETE"


def test_immutable_fields_unchanged(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",))
    before=store.load_candidates("run_0001")[0]
    apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    after=store.load_candidates("run_0001")[0]
    assert all(before.get(k)==after.get(k) for k in IMMUTABLE_CANDIDATE_FIELDS)
    assert "simulation_status" in MUTABLE_WORKFLOW_FIELDS


def test_selection_is_preserved(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",None),selected=True)
    apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    assert sum(x["selected_for_initial_search"] for x in store.load_candidates("run_0001")) == 2


def test_budget_consumed_projected_and_reserve_are_distinct(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",None,"RUNNING"))
    plan=plan_offline_reconcile(store,"run_0001",config,adb); b=plan["budget"]
    assert b["budget_consumed"] == 0 and b["budget_projected"] == 1
    assert b["initial_post_budget_remaining_current"] == 72
    assert b["initial_post_budget_remaining_after_plan"] == 71
    assert b["repair_reserve_current"] == 48


def test_state_missing_is_rebuilt(tmp_path):
    store, config, adb=setup_store(tmp_path)
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    out=write_reconciled_outputs(tmp_path/"state.json",tmp_path/"plan.json",store,"run_0001",config,result)
    assert out["state_file_previous"]["status"] == "MISSING" and (tmp_path/"state.json").exists()


def test_corrupt_state_is_quarantined_and_rebuilt(tmp_path):
    store, config, adb=setup_store(tmp_path); state=tmp_path/"state.json"; state.write_text('{broken',encoding='utf-8')
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    out=write_reconciled_outputs(state,tmp_path/"plan.json",store,"run_0001",config,result)
    assert out["state_file_previous"]["status"] == "STATE_FILE_CORRUPT"
    assert json.loads(state.read_text(encoding='utf-8'))["workflow_source"] == "PPL_RUNNER_DB"


def test_stale_state_is_replaced_from_db(tmp_path):
    store, config, adb=setup_store(tmp_path); state=tmp_path/"state.json"
    state.write_text(json.dumps({"run_id":"old","status":"FAILED"}),encoding='utf-8')
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    write_reconciled_outputs(state,tmp_path/"plan.json",store,"run_0001",config,result)
    rebuilt=json.loads(state.read_text(encoding='utf-8'))
    assert rebuilt["run_id"] == "run_0001" and rebuilt["status"] == "READY_FOR_EXECUTION"


def test_execution_plan_is_low_volume(tmp_path):
    store, config, adb=setup_store(tmp_path,("COMPLETE",None))
    result=apply_offline_reconcile(store,plan_offline_reconcile(store,"run_0001",config,adb),config)
    out=write_reconciled_outputs(tmp_path/"state.json",tmp_path/"plan.json",store,"run_0001",config,result)
    plan=out["execution_plan"]
    assert "expressions" not in plan and plan["cache_complete"] == 1 and plan["new_simulation_required"] == 1


def test_exclusive_lock_but_status_remains_readable(tmp_path):
    store, _, _=setup_store(tmp_path); lock=tmp_path/"runner.lock"
    with SingleRunnerLock(lock):
        with pytest.raises(RunnerAlreadyActive):
            SingleRunnerLock(lock).acquire()
        assert store.status("run_0001")["runs"]


def test_contract_enums_are_distinct():
    assert SimulationStatus.SUBMITTED.value == "SUBMITTED"
    assert ExecutionAction.RESUME_EXISTING.value != SimulationStatus.RUNNING.value
    assert SimulationFreshness.STALE_NONTERMINAL.value == "STALE_NONTERMINAL"


def test_no_network_or_check_accounting(tmp_path):
    store, config, adb=setup_store(tmp_path)
    plan=plan_offline_reconcile(store,"run_0001",config,adb)
    assert plan["network_requests"] == plan["simulation_posts"] == plan["check_requests"] == 0
