import json
import sqlite3
import hashlib
from pathlib import Path

import pytest

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.round_orchestrator import (
    _next_run_id,
    _reconcile_round_accounting,
    _round_runtime_guard,
    _select_search_batch,
    _refresh_trigger,
    _round_policy_upgrade_compatible,
    _adaptive_scores,
    _current_rule_search_outcome,
    _repair_value,
    _direction_repair_value,
    _sync_research_telemetry,
    _check_progress_line,
    _round_research_evidence,
    _resume_preflight_remote_missing,
    _resume_preflight_terminal_failure,
    _next_recovered_batch,
    _finalize_recovered_repair_batch,
    _batch_by_no,
    cancel_remote_simulation,
    recover_interrupted_batch_undispatched_tail,
    recover_interrupted_repair_batch,
    repair_interrupted_batch_ledger_attribution,
    finalize_family_winners,
    load_round_policy,
    round_status,
    protect_submitted_alpha,
    authorize_uncertain_repair_retry,
)
from ppl_engine.round_store import create_round, ensure_round_schema, get_round, load_batches, load_winners, set_batch_intent, start_batch, finish_batch, update_round, upsert_dataset_state, load_dataset_states
from ppl_engine.research_telemetry import record_event
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v3.yaml", project_dir=ROOT)


def make_alpha_db(path: Path, facts=()):
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE alpha_results(
            sim_key TEXT PRIMARY KEY, expr TEXT NOT NULL, settings_json TEXT NOT NULL,
            candidate_json TEXT, alpha_id TEXT, status TEXT, sharpe REAL, fitness REAL,
            turnover REAL, margin REAL, returns REAL, long_count INTEGER, short_count INTEGER,
            date_created TEXT, error TEXT, updated_at TEXT NOT NULL, simulation_url TEXT,
            submitted_at TEXT, retry_count INTEGER DEFAULT 0, last_http_status INTEGER,
            last_retry_after REAL, warning TEXT
        )"""
    )
    for row in facts:
        con.execute(
            "INSERT INTO alpha_results(sim_key,expr,settings_json,alpha_id,status,sharpe,fitness,turnover,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (row["sim_key"], row.get("expr", "x"), "{}", row.get("alpha_id"), row.get("status", "COMPLETE"),
             row.get("sharpe"), row.get("fitness"), row.get("turnover"), "now"),
        )
    con.commit(); con.close()


def insert_candidate(store, run_id, cid, sim_key, *, dataset="d1", field="f1", transform="TS_MEAN",
                     operator="ts_mean", window=5, state="PLANNED", alpha_id=None, sim_status="NONE", score=10.0):
    signal = f"{dataset}/{field}/IDENTITY/NORMAL/{transform}"
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_candidates(
                candidate_id,run_id,expression,sim_key,dataset_id,field_id,field_type,semantic_class,
                direction,signal_family,transform_family,operator,window,vector_reducer,lifecycle_state,
                simulation_status,alpha_id,initial_selection_score,structure_status,data_field_count_estimate,
                pp_total_operator_count_estimate,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cid,run_id,f"{operator}({field},{window})",sim_key,dataset,field,"MATRIX","RETURN","NORMAL",signal,
             transform,operator,window,"IDENTITY",state,sim_status,alpha_id,score,"ELIGIBLE",1,1,"now","now"),
        )


def setup_round(tmp_path):
    c = cfg()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0001", c)
    ensure_round_schema(store)
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", c)
    create_round(store, round_id="round_run_0001", run_id="run_0001", policy=policy,
                 total_budget=2000, search_budget=1600, repair_budget=400)
    return c, store, policy


def _insert_repair_recovery_plan(store, plan_id="rp1", signature="sig1"):
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_repair_plans(
                 repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                 repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (plan_id, None, "run_0001", "parent", "parent", "TEST", "TEST_REPAIR", signature,
             "[]", 1, "{}", "[]", "READY", 1, 0, 0, None,
             "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"),
        )


def test_v3_policy_is_2000_1600_400():
    c = cfg(); p = load_round_policy(ROOT / "ppl_round_v3.yaml", c)
    assert p["total_budget"] == 2000
    assert p["search_budget"] == 1600
    assert p["repair_budget"] == 400
    assert p["batch_size"] == 40


def test_round_schema_is_additive_and_core_schema_version_unchanged(tmp_path):
    _, store, _ = setup_round(tmp_path)
    with store.connect() as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"ppl_rounds","ppl_round_batches","ppl_round_family_winners","ppl_round_meta"} <= tables
        assert con.execute("SELECT max(schema_version) FROM ppl_schema_meta").fetchone()[0] == 13
        assert con.execute("SELECT max(schema_version) FROM ppl_round_meta").fetchone()[0] == 4


def test_search_batch_dedups_family_and_respects_protected_winner(tmp_path):
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    # Same family, two windows: only one can enter a batch.
    insert_candidate(store,"run_0001","a","sk_a",field="same",window=5,score=20)
    insert_candidate(store,"run_0001","b","sk_b",field="same",window=22,score=19)
    insert_candidate(store,"run_0001","c","sk_c",field="other",window=5,score=18)
    # Protect the "other" family; only one representative from "same" remains.
    from ppl_engine.round_store import upsert_winner
    from ppl_engine.family import family_id
    c_row = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "c")
    upsert_winner(store,"round_run_0001",family_id=family_id(c_row),signal_family=c_row["signal_family"],
                  candidate_id="old",alpha_id="OLD",winner_state="SUBMITTED",source="TEST",score={})
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 40)
    assert len(batch) == 1
    assert batch[0]["candidate_id"] == "a"


def test_initial_search_never_spends_second_post_on_already_tested_family(tmp_path):
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store,"run_0001","tested","sk_tested",field="same",window=5,
                     state="SIMULATION_COMPLETE",alpha_id="A0",sim_status="COMPLETE",score=20)
    insert_candidate(store,"run_0001","sibling","sk_sibling",field="same",window=22,score=50)
    insert_candidate(store,"run_0001","fresh","sk_fresh",field="fresh",window=5,score=10)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 40)
    assert [x["candidate_id"] for x in batch] == ["fresh"]


def test_cached_siblings_are_free_to_compare_before_family_winner(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [
        {"sim_key":"sk5","alpha_id":"A5","fitness":1.05,"sharpe":2.1,"turnover":0.4},
        {"sim_key":"sk22","alpha_id":"A22","fitness":1.25,"sharpe":2.0,"turnover":0.4},
    ])
    insert_candidate(store,"run_0001","w5","sk5",field="same",window=5,score=10)
    insert_candidate(store,"run_0001","w22","sk22",field="same",window=22,score=9)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 40)
    assert {x["candidate_id"] for x in batch} == {"w5","w22"}
    assert all(x["round_cache_action"] == "CACHE_RESTORE" for x in batch)


def test_search_batch_hard_clamps_new_posts_to_remaining_budget(tmp_path):
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    for i in range(20):
        insert_candidate(store,"run_0001",f"c{i}",f"sk{i}",dataset=f"d{i%4}",field=f"f{i}",score=100-i)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 3)
    assert len(batch) <= 3
    assert len({x["signal_family"] for x in batch}) == len(batch)


def test_finalize_winner_transitions_and_stops_sibling(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk_w","alpha_id":"A1","sharpe":2.4,"fitness":1.2,"turnover":0.4}])
    insert_candidate(store,"run_0001","winner","sk_w",field="same",window=5,state="PRE_TAG_CHECK_PASS",alpha_id="A1",sim_status="COMPLETE")
    insert_candidate(store,"run_0001","sibling","sk_s",field="same",window=22,state="PLANNED",score=9)
    out = finalize_family_winners(store, alpha, "run_0001", "round_run_0001")
    rows = {x["candidate_id"]: x for x in store.load_candidates("run_0001")}
    assert rows["winner"]["lifecycle_state"] == "PRE_TAG_FINALIST"
    assert rows["sibling"]["lifecycle_state"] == "STOPPED"
    assert any(w["alpha_id"] == "A1" for w in load_winners(store,"round_run_0001"))
    assert out["protected_total"] == 1


def test_round_status_is_observational_for_round_meta(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    with store.connect() as con:
        before = con.execute("SELECT updated_at FROM ppl_round_meta WHERE schema_version=4").fetchone()[0]
    status = round_status(store,c,alpha,run_id="run_0001")
    with store.connect() as con:
        after = con.execute("SELECT updated_at FROM ppl_round_meta WHERE schema_version=4").fetchone()[0]
    assert before == after
    assert status["project_version"] == "v3.0.4o"
    assert status["budget"]["total_budget"] == 2000


def test_interrupted_search_batch_recovers_logical_budget_from_fact(tmp_path):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"posted_key","alpha_id":"A1","status":"COMPLETE",
                           "sharpe":2.1,"fitness":1.1,"turnover":0.4}])
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1"],projected_new_posts=1,
                planned_post_sim_keys=["posted_key"])
    out = _reconcile_round_accounting(store,alpha,"run_0001","round_run_0001")
    assert out["search_consumed"] == 1
    rr = store.get_run("run_0001")
    assert rr["post_consumed"] == 1
    batch = load_batches(store,"round_run_0001")[0]
    assert batch["logical_posts_consumed"] == 1
    assert batch["status"] == "RECOVERED"


def test_interrupted_post_intent_without_fact_fails_closed(tmp_path):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1"],projected_new_posts=1,
                planned_post_sim_keys=["ambiguous_key"])
    with pytest.raises(ConfigError, match="ROUND_UNRESOLVED_POST_INTENT"):
        _reconcile_round_accounting(store,alpha,"run_0001","round_run_0001")


def test_pre_dispatch_crash_is_recoverable_without_budget_charge(tmp_path):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1"],projected_new_posts=1)
    out = _reconcile_round_accounting(store,alpha,"run_0001","round_run_0001")
    assert out["total_consumed"] == 0
    assert load_batches(store,"round_run_0001")[0]["status"] == "RECOVERED_PRE_DISPATCH"

def test_recovered_batch_remains_pending_for_normal_finalization(tmp_path):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"k1","alpha_id":"A1","status":"COMPLETE","sharpe":2.0,"fitness":1.0,"turnover":0.4}])
    insert_candidate(store,"run_0001","c1","k1",state="SIMULATION_COMPLETE",alpha_id="A1",sim_status="COMPLETE")
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1"],projected_new_posts=1,
                planned_post_sim_keys=["k1"])
    _reconcile_round_accounting(store,alpha,"run_0001","round_run_0001")
    batch = _next_recovered_batch(store,"round_run_0001")
    assert batch is not None
    assert batch["batch_no"] == 1
    assert batch["status"] == "RECOVERED"


def test_explicit_interrupted_tail_recovery_keeps_durable_posts_and_releases_only_confirmed_tail(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"k1","alpha_id":"A1","status":"RUNNING","sharpe":None,"fitness":None,"turnover":None}])
    insert_candidate(store,"run_0001","c1","k1",state="SIMULATION_RUNNING",alpha_id="A1",sim_status="RUNNING")
    insert_candidate(store,"run_0001","c2","k2",state="SIMULATION_PENDING",alpha_id=None,sim_status="NONE")
    with store.connect() as con:
        con.execute("UPDATE ppl_candidates SET selected_for_initial_search=1 WHERE candidate_id IN ('c1','c2')")
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1","c2"],projected_new_posts=2,
                planned_post_sim_keys=["k1","k2"])
    with pytest.raises(ConfigError, match="EXPLICIT_CONFIRMATION"):
        recover_interrupted_batch_undispatched_tail(
            store,c,alpha,run_id="run_0001",batch_no=1,confirm_undispatched_tail=False
        )
    out = recover_interrupted_batch_undispatched_tail(
        store,c,alpha,run_id="run_0001",batch_no=1,confirm_undispatched_tail=True
    )
    assert out["durable_dispatched"] == 1
    assert out["released_undispatched"] == 1
    assert out["reconciled"]["search_consumed"] == 1
    batch = load_batches(store,"round_run_0001")[0]
    assert json.loads(batch["selected_candidate_ids_json"]) == ["c1"]
    assert json.loads(batch["planned_post_sim_keys_json"]) == ["k1"]
    assert batch["status"] == "RECOVERED"
    rows = {r["candidate_id"]: r for r in store.load_candidates("run_0001")}
    assert rows["c2"]["lifecycle_state"] == "PLANNED"
    assert int(rows["c2"].get("selected_for_initial_search") or 0) == 0


def test_repair_intent_recovery_closes_only_never_dispatched_batch_and_keeps_sequence(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    _insert_repair_recovery_plan(store)
    update_round(store, "round_run_0001", status="PAUSED", phase="REPAIR", current_batch=42)
    start_batch(
        store, "round_run_0001", 42, "REPAIR", plan_ids=["rp1"], projected_new_posts=1,
        planned_post_sim_keys=["rk1"],
    )
    record_event(
        store, "round_run_0001", "run_0001", "SIMULATION_POST_INTENT",
        batch_no=42, phase="REPAIR", sim_key="rk1", payload={"plan_ids": ["rp1"]},
    )
    out = recover_interrupted_repair_batch(
        store, c, alpha, tmp_path, run_id="run_0001", batch_no=42,
        confirm_undispatched_repair_tail=True,
    )
    assert out["released_count"] == 1
    batch = _batch_by_no(store, "round_run_0001", 42)
    assert batch["status"] == "RECOVERED_REPAIR_PRE_DISPATCH"
    assert json.loads(batch["planned_post_sim_keys_json"]) == []
    assert json.loads(batch["selected_plan_ids_json"]) == ["rp1"]
    rr = get_round(store, round_id="round_run_0001")
    assert int(rr["current_batch"]) == 42
    assert _next_recovered_batch(store, "round_run_0001") is None
    with store.connect() as con:
        assert con.execute(
            "SELECT count(*) FROM ppl_round_events WHERE event_type='SIMULATION_POST_INTENT' AND batch_no=42"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM ppl_round_events WHERE event_type='REPAIR_POST_INTENT_RECOVERY' AND batch_no=42"
        ).fetchone()[0] == 1


def test_repair_intent_recovery_checks_18_unique_keys_and_preserves_22_historical_intents(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    _insert_repair_recovery_plan(store)
    keys = [f"rk{i:02d}" for i in range(18)]
    update_round(store, "round_run_0001", status="PAUSED", phase="REPAIR", current_batch=42)
    start_batch(
        store, "round_run_0001", 42, "REPAIR", plan_ids=["rp1"], projected_new_posts=18,
        planned_post_sim_keys=keys,
    )
    for index in range(22):
        key = keys[index % len(keys)]
        record_event(
            store, "round_run_0001", "run_0001", "SIMULATION_POST_INTENT",
            batch_no=42, phase="REPAIR", sim_key=key,
            payload={"repair_plan_id": "rp1", "ordinal": index},
            source_event_key=f"historical-repair-intent:{index}",
        )
    out = recover_interrupted_repair_batch(
        store, c, alpha, tmp_path, run_id="run_0001", batch_no=42,
        confirm_undispatched_repair_tail=True,
    )
    assert out["released_count"] == 18
    assert out["historical_intent_event_count"] == 22
    assert set(out["plan_associations"]) == set(keys)
    assert all(value == ["rp1"] for value in out["plan_associations"].values())
    with store.connect() as con:
        assert con.execute(
            "SELECT count(*) FROM ppl_round_events WHERE event_type='SIMULATION_POST_INTENT' AND batch_no=42"
        ).fetchone()[0] == 22


@pytest.mark.parametrize("unsafe_kind", ["ATTEMPT", "CONFIRMED", "ALPHA", "LEDGER", "PLAN_CONSUMED"])
def test_repair_intent_recovery_fails_closed_on_remote_dispatch_evidence(tmp_path, unsafe_kind):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    facts = ([{"sim_key": "rk1", "status": "RUNNING"}] if unsafe_kind == "ALPHA" else [])
    make_alpha_db(alpha, facts)
    _insert_repair_recovery_plan(store)
    update_round(store, "round_run_0001", status="PAUSED", phase="REPAIR", current_batch=42)
    start_batch(
        store, "round_run_0001", 42, "REPAIR", plan_ids=["rp1"], projected_new_posts=1,
        planned_post_sim_keys=["rk1"],
    )
    if unsafe_kind in {"ATTEMPT", "CONFIRMED"}:
        record_event(
            store, "round_run_0001", "run_0001",
            "SIMULATION_POST_ATTEMPT" if unsafe_kind == "ATTEMPT" else "SIMULATION_POST_CONFIRMED",
            batch_no=42, phase="REPAIR", sim_key="rk1",
        )
    elif unsafe_kind == "LEDGER":
        with store.connect() as con:
            con.execute(
                """INSERT INTO ppl_round_simulation_ledger(
                     round_id,run_id,logical_sequence_no,batch_no,phase,sim_key,origin,
                     details_json,created_at,updated_at
                   ) VALUES ('round_run_0001','run_0001',1,42,'REPAIR','rk1','NEW_POST','{}','now','now')"""
            )
    elif unsafe_kind == "PLAN_CONSUMED":
        with store.connect() as con:
            con.execute(
                "UPDATE ppl_repair_plans SET committed_posts=1,consumed_posts=1 WHERE repair_plan_id='rp1'"
            )
    with pytest.raises(ConfigError, match="RECOVER_INTERRUPTED_REPAIR_UNSAFE"):
        recover_interrupted_repair_batch(
            store, c, alpha, tmp_path, run_id="run_0001", batch_no=42,
            confirm_undispatched_repair_tail=True,
        )
    assert _batch_by_no(store, "round_run_0001", 42)["status"] == "RUNNING"


def test_next_run_id_skips_all_existing_profiles(tmp_path):
    c = cfg(); store = RunnerStore(tmp_path / "runner.db"); store.initialize()
    for rid in ("run_0001","run_0002","run_0004"):
        store.create_run(rid,c)
    assert _next_run_id(store) == "run_0005"


def test_cli_round_status_does_not_mutate_runner_db(tmp_path, capsys):
    _, store, _ = setup_round(tmp_path)
    before = hashlib.sha256(store.path.read_bytes()).hexdigest()
    from ppl_runner import main
    rc = main(["--round-status", "--db", str(store.path), "--run-id", "run_0001"])
    capsys.readouterr()
    after = hashlib.sha256(store.path.read_bytes()).hexdigest()
    assert rc == 0
    assert before == after


@pytest.mark.parametrize("status,code", [
    ("UNCERTAIN_SUBMISSION", "ROUND_UNCERTAIN_SUBMISSION_HOLD"),
    ("AUTH_ERROR", "ROUND_AUTH_ERROR"),
])
def test_round_runtime_guard_blocks_unsafe_global_simulation_state(tmp_path, status, code):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store,"run_0001","unsafe","sk",field="f",state="SIMULATION_PENDING",sim_status=status)
    with pytest.raises(ConfigError, match=code):
        _round_runtime_guard(store,"run_0001")


def test_batch_intent_requires_existing_durable_batch(tmp_path):
    _, store, _ = setup_round(tmp_path)
    with pytest.raises(RuntimeError, match="ROUND_BATCH_INTENT_NOT_DURABLE"):
        set_batch_intent(store, "round_run_0001", 99,
                         planned_post_sim_keys=["sk_missing"], planned_resume_sim_keys=[])


def test_start_batch_never_overwrites_existing_batch_history(tmp_path):
    _, store, _ = setup_round(tmp_path)
    start_batch(store, "round_run_0001", 1, "SEARCH", candidate_ids=["c1"], projected_new_posts=1)
    with pytest.raises(sqlite3.IntegrityError):
        start_batch(store, "round_run_0001", 1, "SEARCH", candidate_ids=["c2"], projected_new_posts=1)
    batch = load_batches(store, "round_run_0001")[0]
    assert json.loads(batch["selected_candidate_ids_json"]) == ["c1"]


def test_v3_telemetry_schema_tables_exist(tmp_path):
    _, store, _ = setup_round(tmp_path)
    with store.connect() as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {
        "ppl_round_events", "ppl_round_candidate_decisions", "ppl_round_simulation_ledger",
        "ppl_round_snapshots", "ppl_round_manifests",
    } <= tables


def test_search_batch_records_selected_and_skipped_decisions(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store,"run_0001","best","sk_best",field="same",window=5,score=20)
    insert_candidate(store,"run_0001","sibling","sk_sibling",field="same",window=22,score=10)
    insert_candidate(store,"run_0001","fresh","sk_fresh",dataset="d2",field="fresh",window=5,score=19)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 4, batch_no=1)
    assert {x["candidate_id"] for x in batch} == {"best","fresh"}
    with store.connect() as con:
        rows = {r["candidate_id"]: dict(r) for r in con.execute(
            "SELECT * FROM ppl_round_candidate_decisions WHERE round_id='round_run_0001' AND batch_no=1"
        )}
    assert rows["best"]["decision"] == "SELECTED"
    assert rows["fresh"]["decision"] == "SELECTED"
    assert rows["sibling"]["decision"] == "SKIP_REDUNDANT_FAMILY"
    assert rows["best"]["selection_mode"] in {"EXPLOIT","EXPLORE","BACKFILL"}
    assert rows["best"]["selection_score"] is not None


def test_telemetry_ledger_records_one_row_per_sim_key(tmp_path):
    from ppl_engine.research_telemetry import sync_simulation_ledger, load_ledger
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk1","alpha_id":"A1","sharpe":2.2,"fitness":1.2,"turnover":0.41}])
    insert_candidate(store,"run_0001","c1","sk1",field="f1",state="SIMULATION_COMPLETE",alpha_id="A1",sim_status="COMPLETE")
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["c1"], origin_by_candidate={"c1":"NEW_POST"},
                           selection_mode_by_candidate={"c1":"EXPLOIT"})
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["c1"], origin_by_candidate={"c1":"CACHE"})
    rows = load_ledger(store, "round_run_0001")
    assert len(rows) == 1
    assert rows[0]["origin"] == "NEW_POST"
    assert rows[0]["selection_mode"] == "EXPLOIT"
    assert rows[0]["alpha_id"] == "A1"
    assert rows[0]["fitness"] == 1.2


def test_manifest_and_snapshot_are_durable(tmp_path):
    from ppl_engine.research_telemetry import upsert_manifest, upsert_snapshot, load_manifest, load_snapshots
    _, store, _ = setup_round(tmp_path)
    digest = upsert_manifest(store, "round_run_0001", "run_0001", {"policy":"V3_RANK_001","budget":2000})
    upsert_snapshot(store, "round_run_0001", "run_0001", 1, "SEARCH", "BATCH_END", {"used":40})
    row = load_manifest(store, "round_run_0001")
    assert row and row["manifest_hash"] == digest
    assert json.loads(row["manifest_json"])["budget"] == 2000
    snaps = load_snapshots(store, "round_run_0001")
    assert len(snaps) == 1
    assert json.loads(snaps[0]["payload_json"])["used"] == 40


def test_durable_state_transitions_mirror_to_round_timeline_once(tmp_path):
    from ppl_engine.research_telemetry import sync_durable_events, load_events
    from ppl_engine.state_machine import RUN_TRANSITIONS
    _, store, _ = setup_round(tmp_path)
    store.transition_run("run_0001", "PLANNED", reason="test", source="TEST", allowed=RUN_TRANSITIONS)
    first = sync_durable_events(store, "round_run_0001", "run_0001")
    second = sync_durable_events(store, "round_run_0001", "run_0001")
    events = load_events(store, "round_run_0001")
    assert first >= 1
    assert second == 0
    assert any(e["event_type"] == "STATE_PLANNED" for e in events)


def test_research_report_bundle_contains_replay_files(tmp_path):
    from ppl_engine.round_orchestrator import _write_reports
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    out = _write_reports(store, cfg(), alpha, "run_0001", "round_run_0001", policy, tmp_path)
    rich = Path(out["research_dir"])
    expected = {
        "manifest.json", "summary.json", "summary.md", "timeline.jsonl",
        "simulation_ledger.csv", "candidate_decisions.csv", "batch_snapshots.jsonl",
        "ppl_family_winners.csv", "manual_tag_queue.csv", "near_pass_queue.csv",
        "repair_history.csv", "failure_matrix.csv", "budget_audit.csv", "candidates_final.csv",
        "search_productivity.csv",
    }
    assert expected <= {p.name for p in rich.iterdir() if p.is_file()}


def _insert_pretag_raw_metric(store, run_id, cid, alpha_id, name, value, limit, *, outcome="PASS"):
    with store.connect() as con:
        con.execute(
            """INSERT INTO ppl_check_sessions(
                   check_session_id,run_id,candidate_id,alpha_id,phase,session_status,started_at,
                   resolved_at,poll_count,http_request_count,pending_poll_requests,base_gate_result,
                   theme_gate_result,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f"chk_{cid}", run_id, cid, alpha_id, "PRE_TAG", "RESOLVED", "now", "now", 1, 1, 0,
             "PASS", "PASS", "now", "now"),
        )
        poll = con.execute(
            """INSERT INTO ppl_check_polls(
                   check_session_id,candidate_id,alpha_id,phase,semantic_poll_index,http_request_delta,
                   raw_payload_json,parsed_payload_json,pending,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (f"chk_{cid}", cid, alpha_id, "PRE_TAG", 1, 1, "{}", "{}", 0, "now"),
        )
        poll_id = int(poll.lastrowid)
        con.execute(
            """INSERT INTO ppl_check_results(
                   check_session_id,poll_id,candidate_id,alpha_id,phase,raw_name,normalized_name,
                   category,raw_result,normalized_result,raw_value_json,raw_limit_json,
                   normalized_value,normalized_limit,unit_confidence,preset_limit_json,live_limit_json,
                   effective_limit_json,limit_source,parser_version,alias_version,evidence_source,
                   created_at,eligibility_outcome,threshold_exceeded,diagnosis_outcome,diagnosis_reason
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f"chk_{cid}", poll_id, cid, alpha_id, "PRE_TAG", name, name, "PPL_BASE", outcome, outcome,
             json.dumps(value), json.dumps(limit), None, None, "UNKNOWN", "null", json.dumps(limit),
             json.dumps(limit), "LIVE_CHECK", 3, 3, "LIVE_CHECK", "now", outcome, 0, "NONE", "NONE"),
        )


def test_telemetry_check_metric_falls_back_to_raw_json_columns(tmp_path):
    from ppl_engine.research_telemetry import sync_simulation_ledger, load_ledger
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk1","alpha_id":"A1","sharpe":2.2,"fitness":1.2,"turnover":0.41}])
    insert_candidate(store,"run_0001","c1","sk1",field="f1",state="PRE_TAG_CHECK_COMPLETE",alpha_id="A1",sim_status="COMPLETE")
    _insert_pretag_raw_metric(store, "run_0001", "c1", "A1", "HIGH_TURNOVER_RETURNS_RATIO", 0.6849, 0.75, outcome="WARNING")
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["c1"], origin_by_candidate={"c1":"NEW_POST"})
    row = load_ledger(store, "round_run_0001")[0]
    assert row["ht_ratio"] == pytest.approx(0.6849)
    details = json.loads(row["details_json"])
    assert details["check_metrics"]["ht_ratio"]["limit"] == pytest.approx(0.75)
    assert details["check_metrics"]["ht_ratio"]["value_source"] == "RAW_VALUE_JSON"


def test_round_status_separates_workflow_and_ppl_near_pass_semantics(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store,"run_0001","weak","sk",state="NEAR_PASS",sim_status="COMPLETE")
    status = round_status(store, c, alpha, run_id="run_0001")
    assert status["candidates"]["workflow_near_pass_count"] == 1
    assert status["ppl_near_pass"]["strong"] == 0
    assert status["ppl_near_pass"]["near"] == 0
    assert status["status_query_side_effects"]["simulation_posts"] == 0
    assert "network_requests" not in status


def test_failure_matrix_has_explicit_batch_and_historical_scopes(tmp_path):
    from ppl_engine.research_telemetry import sync_simulation_ledger, failure_matrix
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [
        {"sim_key":"new","alpha_id":"N","sharpe":2.0,"fitness":1.1,"turnover":0.4},
        {"sim_key":"cache","alpha_id":"C","sharpe":2.0,"fitness":1.1,"turnover":0.4},
        {"sim_key":"hist","alpha_id":"H","sharpe":2.0,"fitness":1.1,"turnover":0.4},
    ])
    insert_candidate(store,"run_0001","newc","new",dataset="dnew",field="fnew",state="SIMULATION_COMPLETE",alpha_id="N",sim_status="COMPLETE")
    insert_candidate(store,"run_0001","cachec","cache",dataset="dcache",field="fcache",state="SIMULATION_COMPLETE",alpha_id="C",sim_status="COMPLETE")
    insert_candidate(store,"run_0001","histc","hist",dataset="dhist",field="fhist",state="SIMULATION_COMPLETE",alpha_id="H",sim_status="COMPLETE")
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["newc"], origin_by_candidate={"newc":"NEW_POST"})
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["cachec"], origin_by_candidate={"cachec":"CACHE"})
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", candidate_ids=["histc"])
    rows = failure_matrix(store, "run_0001", round_id="round_run_0001", batch_no=1)
    all_rows = {(r["scope"], r["dimension"], r["value"]): r for r in rows if r["failure"] == "NONE_RECORDED"}
    assert all_rows[("BATCH_NEW_POST","ALL","ALL")]["tested"] == 1
    assert all_rows[("BATCH_CACHE","ALL","ALL")]["tested"] == 1
    assert all_rows[("ROUND_CUMULATIVE","ALL","ALL")]["tested"] == 2
    assert all_rows[("HISTORICAL_BASELINE","ALL","ALL")]["tested"] == 1


def test_rebuild_round_reports_backfills_without_network_or_post(tmp_path, monkeypatch):
    from ppl_engine.round_orchestrator import rebuild_round_reports
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk1","alpha_id":"A1","sharpe":2.2,"fitness":1.2,"turnover":0.41}])
    insert_candidate(store,"run_0001","c1","sk1",field="f1",state="SIMULATION_COMPLETE",alpha_id="A1",sim_status="COMPLETE")
    start_batch(store,"round_run_0001",1,"SEARCH",candidate_ids=["c1"],projected_new_posts=1)
    from ppl_engine.round_store import set_batch_intent, finish_batch
    set_batch_intent(store,"round_run_0001",1,planned_post_sim_keys=["sk1"],planned_resume_sim_keys=[])
    finish_batch(store,"round_run_0001",1,status="COMPLETE",logical_posts_consumed=1,cache_hits=0,resume_count=0,check_count=0,report={})
    # Seed explicit origin as an already executed round would have done.
    from ppl_engine.research_telemetry import sync_simulation_ledger
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["c1"], origin_by_candidate={"c1":"NEW_POST"})
    out = rebuild_round_reports(store, c, alpha, ROOT / "ppl_round_v3.yaml", tmp_path, run_id="run_0001")
    assert out["side_effects"]["network_requests"] == 0
    assert out["side_effects"]["simulation_posts"] == 0
    assert Path(out["reports"]["simulation_ledger_csv"]).exists()



def test_v302_dataset_pool_tables_are_additive(tmp_path):
    _, store, _ = setup_round(tmp_path)
    with store.connect() as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"ppl_round_dataset_states", "ppl_round_dataset_refreshes"} <= tables


def test_search_batch_paid_candidates_respect_dataset_cooldown(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store, "run_0001", "cold", "sk_cold", dataset="d_cold", field="f1", score=100)
    insert_candidate(store, "run_0001", "active", "sk_active", dataset="d_active", field="f2", score=10)
    upsert_dataset_state(store, "round_run_0001", "d_cold", state="COOLDOWN", reason="TEST")
    upsert_dataset_state(store, "round_run_0001", "d_active", state="ACTIVE", reason="TEST")
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 40, batch_no=1)
    assert [x["candidate_id"] for x in batch] == ["active"]
    with store.connect() as con:
        decision = con.execute(
            "SELECT decision FROM ppl_round_candidate_decisions WHERE round_id=? AND batch_no=1 AND candidate_id='cold'",
            ("round_run_0001",),
        ).fetchone()[0]
    assert decision == "SKIP_DATASET_COOLDOWN"


def test_periodic_rolling_refresh_triggers_after_five_completed_search_batches(tmp_path):
    _, store, policy = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "c", "sk", dataset="d1", field="f", score=1)
    upsert_dataset_state(store, "round_run_0001", "d1", state="ACTIVE", reason="TEST")
    for i in range(1, 6):
        start_batch(store, "round_run_0001", i, "SEARCH")
        finish_batch(store, "round_run_0001", i, {}, logical_posts_consumed=0)
    assert _refresh_trigger(store, "run_0001", "round_run_0001", policy) == "PERIODIC"


def test_v302_policy_can_upgrade_additively_to_v303_online_evidence_ranking():
    current = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    stored = json.loads(json.dumps(current))
    stored.pop("adaptive_ranking")
    stored["policy_versions"]["ranking"] = "V3_RANK_001"
    stored["policy_versions"]["allocation"] = "V3_ALLOC_002"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["batch_size"] = 99
    assert not _round_policy_upgrade_compatible(bad, current)


def test_v303_exploit_prefers_real_paid_evidence_over_small_static_prior_advantage():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    empty = lambda: {
        "attempts": 0, "local_pass": 0, "pretag_resolved": 0,
        "ppl_near_pass": 0, "ppl_strong_near_pass": 0, "ppl_success": 0,
    }
    evidence = {k: {} for k in ("dataset","operator","dataset_operator","operator_window","dataset_operator_window")}
    good = {"attempts": 5, "local_pass": 5, "pretag_resolved": 5, "ppl_near_pass": 1, "ppl_strong_near_pass": 0, "ppl_success": 0}
    bad = {"attempts": 6, "local_pass": 0, "pretag_resolved": 0, "ppl_near_pass": 0, "ppl_strong_near_pass": 0, "ppl_success": 0}
    ds = {"attempts": 11, "local_pass": 5, "pretag_resolved": 5, "ppl_near_pass": 1, "ppl_strong_near_pass": 0, "ppl_success": 0}
    op_good = {"attempts": 20, "local_pass": 14, "pretag_resolved": 14, "ppl_near_pass": 1, "ppl_strong_near_pass": 0, "ppl_success": 0}
    op_bad = dict(bad)
    evidence["dataset"]["techindi"] = ds
    evidence["operator"]["ts_mean"] = op_good
    evidence["operator"]["raw"] = op_bad
    evidence["dataset_operator"][("techindi","ts_mean")] = good
    evidence["dataset_operator"][("techindi","raw")] = bad
    evidence["operator_window"][("ts_mean","3")] = good
    evidence["operator_window"][("raw","NONE")] = bad
    evidence["dataset_operator_window"][("techindi","ts_mean","3")] = good
    evidence["dataset_operator_window"][("techindi","raw","NONE")] = bad
    ts = _adaptive_scores({"dataset_id":"techindi","operator":"ts_mean","window":3,"initial_selection_score":102.0}, evidence, policy)
    raw = _adaptive_scores({"dataset_id":"techindi","operator":"raw","window":None,"initial_selection_score":104.0}, evidence, policy)
    assert ts["exploit"] > raw["exploit"]
    assert ts["online_evidence"] > raw["online_evidence"]


def test_v303_exploit_has_no_exploration_bonus_but_explore_rewards_novelty():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    evidence = {k: {} for k in ("dataset","operator","dataset_operator","operator_window","dataset_operator_window")}
    row = {"dataset_id":"new_ds","operator":"new_op","window":7,"initial_selection_score":100.0}
    scores = _adaptive_scores(row, evidence, policy)
    assert scores["online_evidence"] == 0.0
    assert scores["exploit"] == pytest.approx(30.0)
    assert scores["explore"] > 12.0  # static floor + explicit novelty term


def test_protect_submitted_alpha_marks_whole_family_and_is_idempotent(tmp_path):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "submitted", "sk_sub", field="same", window=5,
                     alpha_id="P07TEST", sim_status="COMPLETE", state="PRE_TAG_CHECK_COMPLETE")
    insert_candidate(store, "run_0001", "sibling", "sk_sib", field="same", window=22)
    first = protect_submitted_alpha(store, run_id="run_0001", alpha_id="P07TEST")
    second = protect_submitted_alpha(store, run_id="run_0001", alpha_id="P07TEST")
    assert first["protected"] is True
    assert first["family_id"] == second["family_id"]
    winners = load_winners(store, "round_run_0001")
    protected = [w for w in winners if int(w.get("protected") or 0)]
    assert len(protected) == 1
    assert protected[0]["alpha_id"] == "P07TEST"
    assert protected[0]["winner_state"] == "USER_CONFIRMED_SUBMITTED"
    with store.connect() as con:
        events = con.execute(
            "SELECT count(*) FROM ppl_round_events WHERE event_type='USER_CONFIRMED_ALPHA_PROTECTED'"
        ).fetchone()[0]
    assert events == 1


def test_cli_protect_alpha_is_local_only_and_updates_protected_count(tmp_path, capsys):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "submitted", "sk_sub", field="same", window=5,
                     alpha_id="P07TEST", sim_status="COMPLETE", state="PRE_TAG_CHECK_COMPLETE")
    from ppl_runner import main
    rc = main(["--protect-alpha", "--db", str(store.path), "--run-id", "run_0001", "--alpha-id", "P07TEST"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert out["project_version"] == "v3.0.4o"
    assert out["protected"] is True
    assert out["side_effects"]["network_requests"] == 0
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk_sub","alpha_id":"P07TEST","status":"COMPLETE",
                           "sharpe":2.0,"fitness":1.1,"turnover":0.5}])
    status = round_status(store, cfg(), alpha, run_id="run_0001")
    assert status["families"]["protected"] == 1


def _blank_failure_aware_stat(**overrides):
    base = {
        "attempts": 0, "signal_viable": 0, "local_pass": 0,
        "fixed_repairable": 0, "search_viable": 0,
        "search_strong": 0, "search_elite": 0,
        "repair_viable": 0, "repair_elite": 0, "terminal_fail": 0,
        "pretag_resolved": 0, "ppl_near_pass": 0,
        "ppl_strong_near_pass": 0, "ppl_success": 0,
    }
    base.update(overrides)
    return base


def test_v304c_repeated_terminal_failures_score_below_untested_static_prior():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    empty = {k: {} for k in ("dataset","operator","dataset_operator","operator_window","dataset_operator_window")}
    bad = {k: {} for k in empty}
    stat = _blank_failure_aware_stat(attempts=8, terminal_fail=8)
    bad["dataset"]["bad_ds"] = dict(stat)
    bad["operator"]["bad_op"] = dict(stat)
    bad["dataset_operator"][("bad_ds","bad_op")] = dict(stat)
    bad["operator_window"][("bad_op","5")] = dict(stat)
    bad["dataset_operator_window"][("bad_ds","bad_op","5")] = dict(stat)
    row = {"dataset_id":"bad_ds","operator":"bad_op","window":5,"initial_selection_score":100.0}
    failed = _adaptive_scores(row, bad, policy)
    untested = _adaptive_scores(row, empty, policy)
    assert failed["online_evidence"] < 0
    assert failed["exploit"] < untested["exploit"]
    assert failed["explore"] < untested["explore"]
    assert failed["exploit_eligible"] is False


def test_v304c_exploit_requires_positive_dataset_operator_evidence():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    evidence = {k: {} for k in ("dataset","operator","dataset_operator","operator_window","dataset_operator_window")}
    positive = _blank_failure_aware_stat(attempts=2, signal_viable=1, local_pass=1, search_viable=1, terminal_fail=1)
    evidence["dataset"]["d"] = dict(positive)
    evidence["operator"]["ts_mean"] = dict(positive)
    evidence["dataset_operator"][("d","ts_mean")] = dict(positive)
    evidence["operator_window"][("ts_mean","3")] = dict(positive)
    evidence["dataset_operator_window"][("d","ts_mean","3")] = dict(positive)
    scores = _adaptive_scores({"dataset_id":"d","operator":"ts_mean","window":3,"initial_selection_score":100}, evidence, policy)
    assert scores["exploit_eligible"] is True
    assert scores["exploit_gate_reason"] == "POSITIVE_COMBO_EVIDENCE"


def test_v304c_current_rule_evidence_ignores_stale_high_turnover_local_gate(tmp_path):
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk_old_theme","alpha_id":"AOLD","sharpe":1.5,"fitness":0.7,"turnover":0.10}])
    insert_candidate(store, "run_0001", "old_theme", "sk_old_theme", dataset="d", field="f", operator="ts_mean", window=3,
                     state="SIMULATION_COMPLETE", alpha_id="AOLD", sim_status="COMPLETE")
    from ppl_engine.research_telemetry import sync_simulation_ledger
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["old_theme"], origin_by_candidate={"old_theme":"NEW_POST"})
    with store.connect() as con:
        con.execute("UPDATE ppl_round_simulation_ledger SET local_gate='FAIL' WHERE round_id=? AND candidate_id=?",
                    ("round_run_0001","old_theme"))
    evidence = _round_research_evidence(store, "run_0001", "round_run_0001", policy)
    stat = evidence["dataset_operator"][("d","ts_mean")]
    assert stat["attempts"] == 1
    assert stat["signal_viable"] == 1
    assert stat["local_pass"] == 1  # 10% turnover is valid in simplified GLB Liquid PPL
    assert stat["search_viable"] == 0  # Sharpe 1.5 is PPL-valid but below the 1.6 SEARCH reward floor
    assert stat["terminal_fail"] == 0


def test_v304i_search_reward_floor_and_quality_bands():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    candidate = {"structure_status":"ELIGIBLE", "pp_total_operator_count_estimate":1, "data_field_count_estimate":1}
    low = _current_rule_search_outcome({"sharpe":1.59,"turnover":0.40}, candidate, policy)
    pos = _current_rule_search_outcome({"sharpe":1.60,"turnover":0.40}, candidate, policy)
    strong = _current_rule_search_outcome({"sharpe":2.10,"turnover":0.40}, candidate, policy)
    elite = _current_rule_search_outcome({"sharpe":3.10,"turnover":0.40}, candidate, policy)
    assert low["local_pass"] is True and low["search_viable"] is False
    assert pos["search_viable"] is True and pos["search_strong"] is False
    assert strong["search_viable"] is True and strong["search_strong"] is True and strong["search_elite"] is False
    assert elite["search_viable"] is True and elite["search_strong"] is True and elite["search_elite"] is True


def test_v304i_high_turnover_elite_is_repair_positive_not_search_positive():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    candidate = {"structure_status":"ELIGIBLE", "pp_total_operator_count_estimate":1, "data_field_count_estimate":1}
    out = _current_rule_search_outcome({"sharpe":3.20,"turnover":0.75}, candidate, policy)
    assert out["signal_viable"] is True
    assert out["local_pass"] is False
    assert out["search_viable"] is False
    assert out["repair_viable"] is True
    assert out["repair_elite"] is True
    rv = _repair_value({"sharpe":3.20,"turnover":0.75}, policy)
    assert rv["band"] == "ELITE_REPAIR"
    assert rv["score"] == 5  # base 3 + near-turnover bonus 2


def test_v304i_repair_only_combo_cannot_unlock_search_exploit():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    evidence = {k: {} for k in ("dataset","operator","dataset_operator","operator_window","dataset_operator_window")}
    repair_only = _blank_failure_aware_stat(
        attempts=6, signal_viable=6, fixed_repairable=6,
        repair_viable=5, repair_elite=3, search_viable=0,
    )
    evidence["dataset"]["d"] = dict(repair_only)
    evidence["operator"]["raw"] = dict(repair_only)
    evidence["dataset_operator"][("d","raw")] = dict(repair_only)
    evidence["operator_window"][("raw","NONE")] = dict(repair_only)
    evidence["dataset_operator_window"][("d","raw","NONE")] = dict(repair_only)
    scores = _adaptive_scores({"dataset_id":"d","operator":"raw","window":None,"initial_selection_score":100}, evidence, policy)
    assert scores["combo_viable"] == 0
    assert scores["exploit_eligible"] is False
    assert scores["exploit_gate_reason"] == "NO_VIABLE_COMBO_EVIDENCE"


def test_v304i_good_repair_band_and_turnover_proximity():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    near = _repair_value({"sharpe":2.40,"turnover":0.75}, policy)
    mid = _repair_value({"sharpe":2.40,"turnover":0.90}, policy)
    far = _repair_value({"sharpe":2.40,"turnover":1.20}, policy)
    assert (near["band"], near["score"]) == ("GOOD_REPAIR", 3)
    assert (mid["band"], mid["score"]) == ("GOOD_REPAIR", 2)
    assert (far["band"], far["score"]) == ("GOOD_REPAIR", 1)


def test_v304c_unproven_combo_cannot_fill_exploit_after_round_has_evidence(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [
        {"sim_key":"sk_good1","alpha_id":"G1","sharpe":2.0,"fitness":1.0,"turnover":0.40},
        {"sim_key":"sk_good2","alpha_id":"G2","sharpe":2.1,"fitness":1.1,"turnover":0.42},
    ])
    insert_candidate(store,"run_0001","good1","sk_good1",dataset="proven",field="g1",operator="ts_mean",window=3,
                     state="SIMULATION_COMPLETE",alpha_id="G1",sim_status="COMPLETE")
    insert_candidate(store,"run_0001","good2","sk_good2",dataset="proven",field="g2",operator="ts_mean",window=3,
                     state="SIMULATION_COMPLETE",alpha_id="G2",sim_status="COMPLETE")
    from ppl_engine.research_telemetry import sync_simulation_ledger
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["good1","good2"], origin_by_candidate={"good1":"NEW_POST","good2":"NEW_POST"})
    # New planned representative in the proven combo plus a much higher-static-prior unseen combo.
    insert_candidate(store,"run_0001","proven_next","sk_pn",dataset="proven",field="g3",operator="ts_mean",window=3,score=20)
    insert_candidate(store,"run_0001","unseen","sk_unseen",dataset="unseen",field="u1",operator="rank",window=5,score=500)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 4, batch_no=2)
    by_id = {x["candidate_id"]: x for x in batch}
    assert by_id["proven_next"]["round_selection_mode"] == "EXPLOIT"
    assert by_id["unseen"]["round_selection_mode"] == "EXPLORE"
    assert by_id["unseen"]["round_exploit_eligible"] is False


def test_v304c_unproven_exploration_combo_is_capped_per_batch(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk_fail","alpha_id":"F1","sharpe":0.2,"fitness":0.0,"turnover":0.4}])
    insert_candidate(store,"run_0001","fail","sk_fail",dataset="seen",field="f0",operator="raw",window=5,
                     state="SIMULATION_COMPLETE",alpha_id="F1",sim_status="COMPLETE")
    from ppl_engine.research_telemetry import sync_simulation_ledger
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
                           candidate_ids=["fail"], origin_by_candidate={"fail":"NEW_POST"})
    for i in range(10):
        insert_candidate(store,"run_0001",f"u{i}",f"sk_u{i}",dataset="newds",field=f"u{i}",operator="rank",window=5,score=100-i)
    batch = _select_search_batch(store, alpha, "run_0001", "round_run_0001", policy, 40, batch_no=2)
    chosen = [x for x in batch if x["dataset_id"] == "newds" and x["operator"] == "rank"]
    assert len(chosen) <= policy["adaptive_ranking"]["max_unproven_combo_per_batch"]
    assert all(x["round_selection_mode"] == "EXPLORE" for x in chosen)


def test_v304i_policy_can_upgrade_in_progress_v3_rank_003_round():
    current = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    stored = json.loads(json.dumps(current))
    adaptive = stored["adaptive_ranking"]
    for key in (
        "recent_zero_positive_min_attempts", "recent_zero_positive_penalty",
        "recent_zero_positive_consecutive_batches_cooldown",
        "direction_repair_positive_abs_sharpe_min", "direction_repair_strong_abs_sharpe_min",
        "direction_repair_elite_abs_sharpe_min",
        "search_positive_sharpe_min", "search_strong_sharpe_min", "search_elite_sharpe_min",
        "repair_good_sharpe_min", "repair_elite_sharpe_min",
        "repair_turnover_near_max", "repair_turnover_mid_max",
    ):
        adaptive.pop(key, None)
    for key in ("search_viable", "search_strong", "search_elite", "repair_viable", "repair_elite"):
        adaptive["stage_weights"].pop(key, None)
    adaptive["stage_weights"].update({"signal_viable":8.0, "local_pass":22.0, "fixed_repairable":4.0})
    stored["policy_versions"]["ranking"] = "V3_RANK_003"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["batch_size"] = 41
    assert not _round_policy_upgrade_compatible(bad, current)


def test_v304b_policy_can_upgrade_additively_to_v304c_failure_aware_ranking():
    current = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    # First reconstruct V3_RANK_003 from the current V3_RANK_004 shape, then
    # scope this legacy test to the original b->c migration.
    current = json.loads(json.dumps(current))
    adaptive = current["adaptive_ranking"]
    for key in (
        "recent_zero_positive_min_attempts", "recent_zero_positive_penalty",
        "recent_zero_positive_consecutive_batches_cooldown",
        "direction_repair_positive_abs_sharpe_min", "direction_repair_strong_abs_sharpe_min",
        "direction_repair_elite_abs_sharpe_min",
        "search_positive_sharpe_min", "search_strong_sharpe_min", "search_elite_sharpe_min",
        "repair_good_sharpe_min", "repair_elite_sharpe_min",
        "repair_turnover_near_max", "repair_turnover_mid_max",
    ):
        adaptive.pop(key, None)
    for key in ("search_viable", "search_strong", "search_elite", "repair_viable", "repair_elite"):
        adaptive["stage_weights"].pop(key, None)
    adaptive["stage_weights"].update({"signal_viable":8.0, "local_pass":22.0, "fixed_repairable":4.0})
    current["policy_versions"]["ranking"] = "V3_RANK_003"
    current["ppl_classification"].pop("manual_finalization", None)
    current["policy_versions"]["ppl_classification"] = "V3_PPL_CLASS_002"
    stored = json.loads(json.dumps(current))
    for key in (
        "terminal_fail_penalty", "zero_viable_penalty", "zero_viable_min_attempts",
        "exploit_min_combo_attempts", "exploit_min_combo_viable",
        "max_unproven_dataset_fraction", "max_unproven_combo_per_batch",
    ):
        stored["adaptive_ranking"].pop(key, None)
    stored["adaptive_ranking"]["stage_weights"].pop("signal_viable", None)
    stored["adaptive_ranking"]["stage_weights"].pop("fixed_repairable", None)
    for key in ("cooldown_viable_rate_max", "cooldown_without_admission", "max_cooldown_per_refresh"):
        stored["rolling_discovery"].pop(key, None)
    stored["policy_versions"]["ranking"] = "V3_RANK_002"
    stored["policy_versions"]["dataset_discovery"] = "V3_DATASET_001"
    stored["policy_versions"]["telemetry"] = "V3_TELEMETRY_002"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["batch_size"] = 41
    assert not _round_policy_upgrade_compatible(bad, current)


class _FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload
    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _FakeSession:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = []
    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        item = self.statuses.pop(0)
        if isinstance(item, tuple):
            return _FakeResponse(item[0], item[1])
        return _FakeResponse(item)


def test_resume_preflight_quarantines_confirmed_404_without_repost(tmp_path, monkeypatch):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "stale", "sk_stale", state="SIMULATION_RUNNING",
                     sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='stale'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/stale123"}),),
        )
    monkeypatch.setattr("ppl_engine.round_orchestrator.time.sleep", lambda *_: None)
    session = _FakeSession([404, 404])
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "stale"]
    resumable, quarantined = _resume_preflight_remote_missing(
        store, session, "run_0001", rows, round_id="round_run_0001"
    )
    assert resumable == []
    assert quarantined == ["stale"]
    row = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "stale")
    assert row["simulation_status"] == "REMOTE_NOT_FOUND"
    assert row["lifecycle_state"] == "SIMULATION_REMOTE_MISSING"
    assert row["execution_action"] == "HOLD_REMOTE_NOT_FOUND"
    assert len(session.calls) == 2
    assert all(call[0] == "GET" for call in session.calls)


def test_resume_preflight_single_404_then_200_is_not_quarantined(tmp_path, monkeypatch):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "transient", "sk_transient", state="SIMULATION_RUNNING",
                     sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='transient'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/transient123"}),),
        )
    monkeypatch.setattr("ppl_engine.round_orchestrator.time.sleep", lambda *_: None)
    session = _FakeSession([404, 200])
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "transient"]
    resumable, quarantined = _resume_preflight_remote_missing(
        store, session, "run_0001", rows, round_id="round_run_0001"
    )
    assert [x["candidate_id"] for x in resumable] == ["transient"]
    assert quarantined == []
    row = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "transient")
    assert row["simulation_status"] == "RUNNING"


def test_resume_preflight_200_uses_one_get_and_keeps_resume(tmp_path):
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "live", "sk_live", state="SIMULATION_RUNNING",
                     sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='live'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/live123"}),),
        )
    session = _FakeSession([200])
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "live"]
    resumable, quarantined = _resume_preflight_remote_missing(
        store, session, "run_0001", rows, round_id="round_run_0001"
    )
    assert [x["candidate_id"] for x in resumable] == ["live"]
    assert quarantined == []
    assert len(session.calls) == 1


def test_manual_cancel_audit_normalizes_resolver_identity_without_duplicate_keyword(tmp_path, monkeypatch):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha)
    url = "https://api.worldquantbrain.com/simulations/sim_audit_1"
    with sqlite3.connect(alpha) as con:
        con.execute(
            """INSERT INTO alpha_results(
                   sim_key,expr,settings_json,candidate_json,status,updated_at,
                   simulation_url,submitted_at
               ) VALUES (?,?,?,?,?,?,?,?)""",
            ("sk_audit", "rank(f1)", "{}", json.dumps({"expr": "rank(f1)"}),
             "STALE_RUNNING", "now", url, "2026-08-21T00:00:00+00:00"),
        )
    insert_candidate(
        store, "run_0001", "auditrow", "sk_audit",
        state="SIMULATION_RUNNING", sim_status="STALE_RUNNING", score=1,
    )

    class Resolution:
        def to_dict(self):
            return {
                "simulation_id": "sim_audit_1", "simulation_url": url,
                "trigger_source": "MANUAL_CANCEL", "resolution_result": "UNRESOLVED",
                "resolution_reason": None, "error_reason": "REMOTE_CANCEL_HTTP_ERROR",
                # Collision candidates deliberately returned by the fake
                # resolver; caller context must be overlaid exactly once.
                "run_id": "resolver_run", "round_id": "resolver_round",
                "candidate_id": "resolver_candidate", "sim_key": "resolver_key",
                "action": "resolver_action",
            }

    captured = []
    monkeypatch.setattr("ppl_engine.round_orchestrator.resolve_remote_simulation", lambda *a, **k: Resolution())
    monkeypatch.setattr("ppl_engine.round_orchestrator.audit_event", lambda **payload: captured.append(payload))
    result = cancel_remote_simulation(
        store, c, object(), object(), alpha, run_id="run_0001",
        simulation_id="sim_audit_1", confirmed=True,
    )
    assert result["resolved"] is False
    assert len(captured) == 1
    audit = captured[0]
    assert audit["action"] == "REMOTE_SIMULATION_RESOLUTION"
    assert audit["simulation_id"] == "sim_audit_1"
    assert audit["run_id"] == "run_0001"
    assert audit["round_id"] == "round_run_0001"
    assert audit["candidate_id"] == "auditrow"
    assert audit["sim_key"] == "sk_audit"
    assert audit["simulation_url"] == url
    assert audit["trigger_source"] == "MANUAL_CANCEL"


class _FakeMachineCache:
    def __init__(self):
        self.calls = []
    def cache_put(self, db_path, sim_key, candidate, settings, result):
        self.calls.append((db_path, sim_key, candidate, settings, result))
        con = sqlite3.connect(db_path)
        con.execute(
            "UPDATE alpha_results SET status=?,error=?,simulation_url=?,last_http_status=?,updated_at='now2' WHERE sim_key=?",
            (result.get("status"), result.get("error"), result.get("simulation_url"), result.get("last_http_status"), sim_key),
        )
        con.commit(); con.close()


def test_resume_preflight_terminal_fail_is_finalized_without_repost(tmp_path, monkeypatch):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha)
    con = sqlite3.connect(alpha)
    con.execute(
        "INSERT INTO alpha_results(sim_key,expr,settings_json,candidate_json,status,updated_at,simulation_url) VALUES (?,?,?,?,?,?,?)",
        ("sk_fail","rank(f1)","{}",json.dumps({"expr":"rank(f1)"}),"RUNNING","now","https://api.worldquantbrain.com/simulations/fail123"),
    )
    con.commit(); con.close()
    insert_candidate(store, "run_0001", "failrow", "sk_fail", state="SIMULATION_RUNNING", sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='failrow'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/fail123"}),),
        )
    monkeypatch.setattr("ppl_engine.round_orchestrator.time.sleep", lambda *_: None)
    session = _FakeSession([(200,{"status":"FAIL"}), (200,{"status":"FAIL"})])
    machine = _FakeMachineCache()
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "failrow"]
    resumable, failed = _resume_preflight_terminal_failure(
        store, machine, session, alpha, "run_0001", rows, round_id="round_run_0001"
    )
    assert resumable == []
    assert failed == ["failrow"]
    row = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "failrow")
    assert row["simulation_status"] == "ERROR"
    assert row["lifecycle_state"] == "SIMULATION_REMOTE_FAILED"
    assert row["execution_action"] == "HOLD_REMOTE_FAILED"
    assert len(session.calls) == 2
    assert len(machine.calls) == 1
    con = sqlite3.connect(alpha)
    status = con.execute("SELECT status FROM alpha_results WHERE sim_key='sk_fail'").fetchone()[0]
    con.close()
    assert status == "ERROR"


def test_resume_preflight_terminal_fail_then_running_is_not_finalized(tmp_path, monkeypatch):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store, "run_0001", "maybe", "sk_maybe", state="SIMULATION_RUNNING", sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='maybe'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/maybe123"}),),
        )
    monkeypatch.setattr("ppl_engine.round_orchestrator.time.sleep", lambda *_: None)
    session = _FakeSession([(200,{"status":"FAIL"}), (200,{"status":"RUNNING"})])
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "maybe"]
    resumable, failed = _resume_preflight_terminal_failure(
        store, None, session, alpha, "run_0001", rows, round_id="round_run_0001"
    )
    assert [x["candidate_id"] for x in resumable] == ["maybe"]
    assert failed == []
    row = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "maybe")
    assert row["simulation_status"] == "RUNNING"


def test_resume_preflight_running_uses_one_get_and_keeps_resume(tmp_path):
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    insert_candidate(store, "run_0001", "live2", "sk_live2", state="SIMULATION_RUNNING", sim_status="RUNNING", score=1)
    with store.connect() as con:
        con.execute(
            "UPDATE ppl_candidates SET result_reference_json=? WHERE candidate_id='live2'",
            (json.dumps({"simulation_url":"https://api.worldquantbrain.com/simulations/live2"}),),
        )
    session = _FakeSession([(200,{"status":"RUNNING"})])
    rows = [x for x in store.load_candidates("run_0001") if x["candidate_id"] == "live2"]
    resumable, failed = _resume_preflight_terminal_failure(
        store, None, session, alpha, "run_0001", rows, round_id="round_run_0001"
    )
    assert [x["candidate_id"] for x in resumable] == ["live2"]
    assert failed == []
    assert len(session.calls) == 1


def test_v304e_policy_can_upgrade_additively_to_v304f_manual_finalization():
    current_g = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    current = json.loads(json.dumps(current_g))
    current["ppl_classification"]["manual_finalization"].pop("auto_refresh_every_batches", None)
    current["ppl_classification"]["manual_finalization"].pop("ppc_strategy", None)
    current["policy_versions"]["ppl_classification"] = "V3_PPL_CLASS_003"
    stored = json.loads(json.dumps(current))
    stored["ppl_classification"].pop("manual_finalization", None)
    stored["policy_versions"]["ppl_classification"] = "V3_PPL_CLASS_002"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["search_budget"] = int(bad["search_budget"]) - 1
    assert not _round_policy_upgrade_compatible(bad, current)


def test_manual_finalization_rows_generate_copy_ready_description_and_dedup_alpha():
    from ppl_engine.round_orchestrator import _manual_finalization_rows
    candidates = {
        "c1": {"candidate_id":"c1","alpha_id":"A1","field_id":"f1","expression":"ts_mean(f1,3)","transform_family":"TS_MEAN","window":3},
        "c2": {"candidate_id":"c2","alpha_id":"A2","field_id":"f2","expression":"rank(f2)","transform_family":"RANK"},
    }
    cls = [
        {"candidate_id":"c1","alpha_id":"A1","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":2.0,"fitness":1.0,"turnover":0.5,"final_theme_outcome":"WARNING","description_pending_checks":[]},
        {"candidate_id":"c2","alpha_id":"A2","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":1.7,"fitness":0.8,"turnover":0.4,"final_theme_outcome":"WARNING","description_pending_checks":[]},
    ]
    rows = _manual_finalization_rows(candidates, cls)
    assert len(rows) == 2
    assert all(r["description_validation_status"] == "VALID" for r in rows)
    assert all(r["manual_action"] == "COPY_DESCRIPTION_SAVE_AND_CHECK_SUBMISSION" for r in rows)
    assert all(r["platform_property_write_performed"] is False for r in rows)


def test_manual_finalization_rows_refuse_ambiguous_same_alpha_mapping():
    from ppl_engine.round_orchestrator import _manual_finalization_rows
    candidates = {
        "c1": {"candidate_id":"c1","alpha_id":"A1","field_id":"f1","expression":"rank(f1)"},
        "c2": {"candidate_id":"c2","alpha_id":"A1","field_id":"f2","expression":"rank(f2)"},
    }
    cls = [
        {"candidate_id":"c1","alpha_id":"A1","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":2.0},
        {"candidate_id":"c2","alpha_id":"A1","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":1.9},
    ]
    rows = _manual_finalization_rows(candidates, cls)
    assert len(rows) == 1
    assert rows[0]["identity_ambiguous"] is True
    assert rows[0]["candidate_mapping_count"] == 2
    assert rows[0]["generated_description"] == ""
    assert rows[0]["manual_action"] == "VERIFY_ALPHA_CODE_BEFORE_DESCRIPTION"


def test_v304f_policy_can_upgrade_additively_to_v304g_ppc_and_refresh():
    current = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    stored = json.loads(json.dumps(current))
    manual = stored["ppl_classification"]["manual_finalization"]
    manual.pop("auto_refresh_every_batches", None)
    manual.pop("ppc_strategy", None)
    stored["policy_versions"]["ppl_classification"] = "V3_PPL_CLASS_003"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["batch_size"] = int(bad["batch_size"]) + 1
    assert not _round_policy_upgrade_compatible(bad, current)


def test_manual_finalization_rows_exclude_protected_family_and_include_ppc_fields():
    from ppl_engine.round_orchestrator import _manual_finalization_rows
    candidates = {
        "c1": {"candidate_id":"c1","alpha_id":"A1","signal_family":"sf1","field_id":"f1","expression":"rank(f1)"},
        "c2": {"candidate_id":"c2","alpha_id":"A2","signal_family":"sf2","field_id":"f2","expression":"rank(f2)"},
    }
    cls = [
        {"candidate_id":"c1","alpha_id":"A1","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":2.5,"ppc_value":0.40,"platform_ppc_outcome":"PASS","ppc_policy_band":"CLEAN","ppc_strategy_result":"PASS_CLEAN_PPC"},
        {"candidate_id":"c2","alpha_id":"A2","classification":"PPL_READY_FOR_MANUAL_FINALIZATION","sharpe":2.6,"ppc_value":0.55,"platform_ppc_outcome":"PASS","ppc_policy_band":"MID","ppc_strategy_result":"PASS_MID_PPC_SHARPE_GT_2"},
    ]
    rows = _manual_finalization_rows(
        candidates, cls, protected_signal_families={"sf1"},
        check_meta_by_candidate={"c2":{"check_session_id":"chk2","updated_at":"2026-08-20T00:00:00+00:00"}},
    )
    assert [r["alpha_id"] for r in rows] == ["A2"]
    assert rows[0]["ppc"] == 0.55
    assert rows[0]["ppc_policy_band"] == "MID"
    assert rows[0]["last_check_session_id"] == "chk2"


def test_manual_queue_auto_refresh_only_on_tenth_batch_and_skips_fresh(monkeypatch, tmp_path):
    import ppl_engine.round_orchestrator as ro
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"; make_alpha_db(alpha)
    calls = []
    monkeypatch.setattr(ro, "_manual_queue_csv_candidates", lambda project_dir, round_id: ["old", "fresh"])
    def fake_refresh(*args, **kwargs):
        calls.append(kwargs)
        return {"executed_check_count": 1, "requested": 2}
    monkeypatch.setattr(ro, "_refresh_manual_finalization_candidates", fake_refresh)
    no = ro._maybe_auto_refresh_manual_finalization(
        store, cfg(), object(), object(), alpha, "run_0001", "round_run_0001", policy, tmp_path,
        batch_no=9, fresh_candidate_ids=["fresh"],
    )
    assert no["triggered"] is False
    yes = ro._maybe_auto_refresh_manual_finalization(
        store, cfg(), object(), object(), alpha, "run_0001", "round_run_0001", policy, tmp_path,
        batch_no=10, fresh_candidate_ids=["fresh"],
    )
    assert yes["triggered"] is True
    assert yes["interval"] == 10
    assert calls[0]["exclude_candidate_ids"] == ["fresh"]


# ---------------------------------------------------------------------------
# v3.0.4m / V3_RANK_005 regression coverage
# ---------------------------------------------------------------------------

def test_v304k_policy_can_upgrade_in_progress_v3_rank_004_round():
    current = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    stored = json.loads(json.dumps(current))
    adaptive = stored["adaptive_ranking"]
    for key in (
        "recent_zero_positive_min_attempts", "recent_zero_positive_penalty",
        "recent_zero_positive_consecutive_batches_cooldown",
        "direction_repair_positive_abs_sharpe_min", "direction_repair_strong_abs_sharpe_min",
        "direction_repair_elite_abs_sharpe_min",
    ):
        adaptive.pop(key, None)
    stored["policy_versions"]["ranking"] = "V3_RANK_004"
    assert _round_policy_upgrade_compatible(stored, current)
    bad = json.loads(json.dumps(stored)); bad["batch_size"] = 41
    assert not _round_policy_upgrade_compatible(bad, current)


def test_v304k_exploit_gate_is_dataset_operator_window_scoped():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    evidence = {k: {} for k in (
        "dataset", "operator", "dataset_operator", "operator_window",
        "dataset_operator_window", "dataset_operator_window_batch",
    )}
    positive = _blank_failure_aware_stat(attempts=8, search_viable=4, search_strong=2, local_pass=4, signal_viable=4)
    evidence["dataset"]["d"] = dict(positive)
    evidence["operator"]["ts_mean"] = dict(positive)
    evidence["dataset_operator"][("d", "ts_mean")] = dict(positive)
    # Window 3 is proven, but window 2 is not. The old Dataset×Operator gate
    # would have unlocked window 2; V3_RANK_005 must keep it EXPLORE-only.
    evidence["operator_window"][("ts_mean", "3")] = dict(positive)
    evidence["dataset_operator_window"][("d", "ts_mean", "3")] = dict(positive)
    scores = _adaptive_scores({"dataset_id":"d", "operator":"ts_mean", "window":2, "initial_selection_score":100}, evidence, policy)
    assert scores["combo_scope"] == "DATASET_OPERATOR_WINDOW"
    assert scores["combo_attempts"] == 0
    assert scores["exploit_eligible"] is False
    assert scores["exploit_gate_reason"] == "INSUFFICIENT_COMBO_EVIDENCE"


def test_v304k_recent_zero_positive_batch_penalizes_then_cools_exact_window():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    evidence = {k: {} for k in (
        "dataset", "operator", "dataset_operator", "operator_window",
        "dataset_operator_window", "dataset_operator_window_batch",
    )}
    aggregate = _blank_failure_aware_stat(attempts=34, search_viable=5, search_strong=3, local_pass=5, signal_viable=5, terminal_fail=29)
    for dim, key in (
        ("dataset", "d"), ("operator", "ts_mean"),
        ("dataset_operator", ("d", "ts_mean")),
        ("operator_window", ("ts_mean", "2")),
        ("dataset_operator_window", ("d", "ts_mean", "2")),
    ):
        evidence[dim][key] = dict(aggregate)
    zero14 = _blank_failure_aware_stat(attempts=14, search_viable=0, terminal_fail=14)
    evidence["dataset_operator_window_batch"][("d", "ts_mean", "2", 12)] = dict(zero14)
    one_bad = _adaptive_scores({"dataset_id":"d", "operator":"ts_mean", "window":2, "initial_selection_score":100}, evidence, policy)
    assert one_bad["exploit_eligible"] is True
    assert one_bad["recent_zero_positive_penalty"] == pytest.approx(18.0)
    assert one_bad["recent_zero_positive_streak"] == 1
    evidence["dataset_operator_window_batch"][("d", "ts_mean", "2", 11)] = dict(zero14)
    two_bad = _adaptive_scores({"dataset_id":"d", "operator":"ts_mean", "window":2, "initial_selection_score":100}, evidence, policy)
    assert two_bad["exploit_eligible"] is False
    assert two_bad["recent_zero_positive_streak"] == 2
    assert two_bad["exploit_gate_reason"] == "RECENT_ZERO_SEARCH_POSITIVE_COOLDOWN"


def test_v304k_direction_repair_bands_require_legal_turnover():
    policy = load_round_policy(ROOT / "ppl_round_v3.yaml", cfg())
    assert _direction_repair_value({"sharpe":-1.73, "turnover":0.3178}, policy)["band"] == "DIRECTION_REPAIR_POSITIVE"
    assert _direction_repair_value({"sharpe":-2.20, "turnover":0.40}, policy)["band"] == "DIRECTION_REPAIR_STRONG"
    assert _direction_repair_value({"sharpe":-3.20, "turnover":0.40}, policy)["band"] == "DIRECTION_REPAIR_ELITE"
    assert _direction_repair_value({"sharpe":-1.08, "turnover":0.40}, policy)["score"] == 0
    assert _direction_repair_value({"sharpe":-2.20, "turnover":0.8361}, policy)["score"] == 0


def test_v304k_batch_scoped_telemetry_does_not_absorb_other_batch_fact(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [
        {"sim_key":"sk12", "alpha_id":"A12", "status":"COMPLETE", "sharpe":0.8, "fitness":0.2, "turnover":0.3},
        {"sim_key":"sk13", "alpha_id":None, "status":"RUNNING", "sharpe":None, "fitness":None, "turnover":None},
    ])
    insert_candidate(store, "run_0001", "c12", "sk12", state="SIMULATION_COMPLETE", alpha_id="A12", sim_status="COMPLETE")
    insert_candidate(store, "run_0001", "c13", "sk13", field="f13", state="SIMULATION_RUNNING", alpha_id=None, sim_status="RUNNING")
    start_batch(store, "round_run_0001", 12, "SEARCH", candidate_ids=["c12"], projected_new_posts=1, planned_post_sim_keys=["sk12"])
    start_batch(store, "round_run_0001", 13, "SEARCH", candidate_ids=["c13"], projected_new_posts=1, planned_post_sim_keys=["sk13"])
    _sync_research_telemetry(
        store, cfg(), alpha, "run_0001", "round_run_0001", batch_no=12, phase="SEARCH",
        origin_by_candidate={"c12":"NEW_POST"}, selection_mode_by_candidate={"c12":"EXPLOIT"}, policy=policy,
    )
    with store.connect() as con:
        rows = [dict(r) for r in con.execute("SELECT candidate_id,batch_no,origin FROM ppl_round_simulation_ledger ORDER BY logical_sequence_no")]
    assert rows == [{"candidate_id":"c12", "batch_no":12, "origin":"NEW_POST"}]


def test_v304k_repairs_interrupted_batch_ledger_attribution_locally(tmp_path):
    from ppl_engine.research_telemetry import sync_simulation_ledger, upsert_candidate_decision
    _, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk13", "alpha_id":None, "status":"RUNNING", "sharpe":None, "fitness":None, "turnover":None}])
    insert_candidate(store, "run_0001", "c13", "sk13", state="SIMULATION_RUNNING", alpha_id=None, sim_status="RUNNING")
    start_batch(store, "round_run_0001", 13, "SEARCH", candidate_ids=["c13"], projected_new_posts=1, planned_post_sim_keys=["sk13"])
    cand = next(x for x in store.load_candidates("run_0001") if x["candidate_id"] == "c13")
    upsert_candidate_decision(store, "round_run_0001", "run_0001", 13, cand,
                              decision="SELECTED", decision_reason="NEW_SIMULATION_REQUIRED", selection_mode="EXPLOIT")
    # Reproduce the v3.0.4j contamination: target batch-13 fact wrongly tagged as batch 12/HISTORICAL.
    sync_simulation_ledger(store, alpha, "round_run_0001", "run_0001", batch_no=12, phase="SEARCH",
                           candidate_ids=["c13"], origin_by_candidate={"c13":"HISTORICAL"})
    out = repair_interrupted_batch_ledger_attribution(
        store, cfg(), alpha, run_id="run_0001", batch_no=13, confirm_ledger_reattribution=True,
    )
    with store.connect() as con:
        row = dict(con.execute("SELECT batch_no,phase,origin,selection_mode FROM ppl_round_simulation_ledger WHERE sim_key='sk13'").fetchone())
    assert row == {"batch_no":13, "phase":"SEARCH", "origin":"NEW_POST", "selection_mode":"EXPLOIT"}
    assert out["corrected_rows"] == 1
    assert out["network_requests"] == out["simulation_posts"] == out["check_requests"] == 0


def test_v304k_check_progress_bar_is_visible(capsys):
    _check_progress_line("PRE-TAG CHECK", 1, 2, candidate_id="c1", alpha_id="A1", state="RESOLVED")
    text = capsys.readouterr().out
    assert "PRE-TAG CHECK" in text
    assert "1/2" in text
    assert "50.0%" in text
    assert "alpha=A1" in text


def test_v304l_server_slot_marker_releases_only_explicit_v21_deferred_rows():
    from ppl_engine.simulation_adapter import server_slot_deferred_sim_keys
    frame = [
        {"sim_key": "k1", "status": "RUNNING", "error": None},
        {"sim_key": "k2", "status": "NEW", "error": "Deferred: an existing server-side simulation is still RUNNING and occupies a concurrency slot."},
        {"sim_key": "k3", "status": "NEW", "error": "some unrelated error"},
    ]
    assert server_slot_deferred_sim_keys(frame, {"k1", "k2", "k3"}) == ["k2"]


def test_v304l_release_server_slot_tail_shrinks_old_post_intent_and_replans_candidate(tmp_path):
    from ppl_engine.round_orchestrator import _release_batch_undispatched_keys
    _, store, _ = setup_round(tmp_path)
    insert_candidate(store, "run_0001", "c1", "k1", state="SIMULATION_RUNNING", alpha_id="A1", sim_status="RUNNING")
    insert_candidate(store, "run_0001", "c2", "k2", state="SIMULATION_PENDING", alpha_id=None, sim_status="NONE")
    with store.connect() as con:
        con.execute("UPDATE ppl_candidates SET selected_for_initial_search=1,selection_rank=2 WHERE candidate_id='c2'")
    start_batch(store, "round_run_0001", 1, "SEARCH", candidate_ids=["c1", "c2"], projected_new_posts=2,
                planned_post_sim_keys=["k1", "k2"])
    out = _release_batch_undispatched_keys(
        store, run_id="run_0001", round_id="round_run_0001", batch_no=1, sim_keys=["k2"],
        release_reason="V3_SERVER_SLOT_DEFERRED_RELEASED", event_type="SERVER_SLOT_DEFERRED_TAIL_RELEASED",
    )
    assert out["released_candidate_ids"] == ["c2"]
    batch = load_batches(store, "round_run_0001")[0]
    assert json.loads(batch["selected_candidate_ids_json"]) == ["c1"]
    assert json.loads(batch["planned_post_sim_keys_json"]) == ["k1"]
    assert int(batch["projected_new_posts"]) == 1
    rows = {r["candidate_id"]: r for r in store.load_candidates("run_0001")}
    assert rows["c2"]["lifecycle_state"] == "PLANNED"
    assert rows["c2"]["simulation_status"] == "NONE"
    assert int(rows["c2"].get("selected_for_initial_search") or 0) == 0
    assert rows["c2"]["selection_rank"] is None


def test_v304l_explicit_tail_recovery_can_reopen_completed_server_slot_batch(tmp_path):
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key": "k1", "alpha_id": "A1", "status": "RUNNING", "sharpe": None, "fitness": None, "turnover": None}])
    insert_candidate(store, "run_0001", "c1", "k1", state="SIMULATION_RUNNING", alpha_id="A1", sim_status="RUNNING")
    insert_candidate(store, "run_0001", "c2", "k2", state="SIMULATION_PENDING", alpha_id=None, sim_status="NONE")
    with store.connect() as con:
        con.execute("UPDATE ppl_candidates SET selected_for_initial_search=1 WHERE candidate_id IN ('c1','c2')")
    start_batch(store, "round_run_0001", 1, "SEARCH", candidate_ids=["c1", "c2"], projected_new_posts=2,
                planned_post_sim_keys=["k1", "k2"])
    finish_batch(store, "round_run_0001", 1, {}, logical_posts_consumed=1)
    # Reproduce the old normal-finalization telemetry bug: a deferred no-fact
    # candidate could be mirrored as NEW_POST solely because an origin map was
    # supplied. Recovery must remove that false ledger row before reopening.
    from ppl_engine.research_telemetry import sync_simulation_ledger
    sync_simulation_ledger(
        store, alpha, "round_run_0001", "run_0001", batch_no=1, phase="SEARCH",
        candidate_ids=["c2"], origin_by_candidate={"c2": "NEW_POST"},
        selection_mode_by_candidate={"c2": "EXPLORE"},
    )
    with store.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM ppl_round_simulation_ledger WHERE round_id='round_run_0001' AND sim_key='k2'").fetchone()[0] == 1
    out = recover_interrupted_batch_undispatched_tail(
        store, c, alpha, run_id="run_0001", batch_no=1, confirm_undispatched_tail=True
    )
    assert out["source_batch_status"] == "COMPLETED"
    assert out["durable_dispatched"] == 1
    assert out["released_undispatched"] == 1
    batch = load_batches(store, "round_run_0001")[0]
    assert batch["status"] == "RECOVERED"
    assert json.loads(batch["planned_post_sim_keys_json"]) == ["k1"]
    assert batch["completed_at"] is None
    with store.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM ppl_round_simulation_ledger WHERE round_id='round_run_0001' AND sim_key='k2'").fetchone()[0] == 0


def test_v304l_resume_report_exposes_still_nonterminal_candidates(tmp_path, monkeypatch):
    import ppl_engine.round_orchestrator as ro
    c, store, _ = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key": "k1", "alpha_id": None, "status": "RUNNING", "sharpe": None, "fitness": None, "turnover": None}])
    insert_candidate(store, "run_0001", "c1", "k1", state="SIMULATION_RUNNING", alpha_id=None, sim_status="RUNNING")
    monkeypatch.setattr(ro, "_resume_preflight_remote_missing", lambda store, session, run_id, rows, **kw: (rows, []))
    monkeypatch.setattr(ro, "_resume_preflight_terminal_failure", lambda store, machine, session, alpha_db, run_id, rows, **kw: (rows, []))
    monkeypatch.setattr(ro, "_execute_search_rows", lambda *a, **kw: {
        "resume_count": 1, "complete_candidate_ids": [], "nonterminal_candidate_ids": ["c1"]
    })
    monkeypatch.setattr(ro, "_analyze_and_check", lambda *a, **kw: {"check_count": 0})
    out = ro._resume_nonterminal(store, c, object(), object(), alpha, "run_0001", 400, round_id="round_run_0001")
    assert out["still_nonterminal_candidate_ids"] == ["c1"]
    assert out["resumed"] == 1


def test_v304l_round_repair_deferred_tail_shrinks_batch_intent(tmp_path):
    from ppl_engine.round_orchestrator import _shrink_repair_batch_deferred_intent
    _, store, _ = setup_round(tmp_path)
    start_batch(
        store, "round_run_0001", 1, "REPAIR", plan_ids=["p1", "p2"], projected_new_posts=2,
        planned_post_sim_keys=["k1", "k2"],
    )
    out = _shrink_repair_batch_deferred_intent(
        store, run_id="run_0001", round_id="round_run_0001", batch_no=1,
        deferred_sim_keys=["k2"], deferred_plan_ids=["p2"],
    )
    assert out["remaining_post_sim_keys"] == ["k1"]
    assert out["remaining_plan_ids"] == ["p1"]
    batch = load_batches(store, "round_run_0001")[0]
    assert json.loads(batch["planned_post_sim_keys_json"]) == ["k1"]
    assert json.loads(batch["selected_plan_ids_json"]) == ["p1"]
    assert int(batch["projected_new_posts"]) == 1


def test_v304l_repair_telemetry_accepts_explicit_candidate_scope_when_batch_has_only_plan_ids(tmp_path):
    _, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [
        {"sim_key":"rk1", "alpha_id":"RA1", "status":"COMPLETE", "sharpe":1.2, "fitness":0.4, "turnover":0.3},
    ])
    insert_candidate(store, "run_0001", "rc1", "rk1", state="SIMULATION_COMPLETE", alpha_id="RA1", sim_status="COMPLETE")
    start_batch(
        store, "round_run_0001", 1, "REPAIR", plan_ids=["rp1"], projected_new_posts=1,
        planned_post_sim_keys=["rk1"],
    )
    # REPAIR batches intentionally do not populate selected_candidate_ids_json.
    # Passing the effective repair child scope explicitly must still produce
    # correctly attributed telemetry instead of ROUND_TELEMETRY_BATCH_SCOPE_EMPTY.
    _sync_research_telemetry(
        store, cfg(), alpha, "run_0001", "round_run_0001", batch_no=1, phase="REPAIR",
        origin_by_candidate={"rc1":"REPAIR_POST"}, selection_mode_by_candidate={"rc1":"REPAIR"},
        policy=policy, candidate_ids=["rc1"],
    )
    with store.connect() as con:
        row = dict(con.execute(
            "SELECT candidate_id,batch_no,phase,origin FROM ppl_round_simulation_ledger WHERE candidate_id='rc1'"
        ).fetchone())
    assert row == {"candidate_id":"rc1", "batch_no":1, "phase":"REPAIR", "origin":"REPAIR_POST"}


def test_uncertain_repair_retry_release_preserves_budget_and_not_strategy_attempt(tmp_path):
    _, store, _ = setup_round(tmp_path)
    update_round(store, "round_run_0001", status="PAUSED", phase="REPAIR")
    with store.connect() as con:
        con.execute("UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',post_uncertain=1,post_consumed=1 WHERE run_id='run_0001'")
    insert_candidate(store, "run_0001", "parent", "parent_key", state="PRE_CHECK_REPAIR", sim_status="COMPLETE")
    insert_candidate(store, "run_0001", "child", "child_key", state="SIMULATION_PENDING", sim_status="UNCERTAIN_SUBMISSION")
    with store.connect() as con:
        con.execute("UPDATE ppl_candidates SET parent_candidate_id='parent' WHERE candidate_id='child'")
        con.execute(
            """INSERT INTO ppl_repair_plans(
                 repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                 repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                 operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                 blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("rp_uncertain", None, "run_0001", "parent", "parent", "PP_CORRELATION_FAIL",
             "NEUTRALIZATION_MICRO_TUNE", "sig_uncertain", "[]", 1, "{}", "[]", "READY", 1, 1, 1,
             "UNCERTAIN_SUBMISSION_HOLD", "now", "now"),
        )
        con.execute(
            """INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,
               repair_signature,repair_path_json,repair_depth,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            ("edge1", "run_0001", "parent", "child", "NEUTRALIZATION_MICRO_TUNE",
             "sig_uncertain", "[]", 1, "now"),
        )
    alpha_db = tmp_path / "alpha.db"
    make_alpha_db(alpha_db, [{"sim_key": "child_key", "status": "UNCERTAIN_SUBMISSION"}])

    out = authorize_uncertain_repair_retry(
        store, alpha_db, run_id="run_0001", confirm_duplicate_risk=True,
    )
    assert out["released_count"] == 1
    assert out["post_uncertain_active_before"] == 1
    assert out["post_uncertain_active_after"] == 0
    assert out["original_post_budget_preserved"] is True
    with sqlite3.connect(alpha_db) as con:
        assert con.execute("SELECT status FROM alpha_results WHERE sim_key='child_key'").fetchone()[0] == "ERROR"
    with store.connect() as con:
        run = con.execute("SELECT post_uncertain,post_consumed FROM ppl_runs WHERE run_id='run_0001'").fetchone()
        assert tuple(run) == (0, 1)
        plan = con.execute("SELECT plan_status,consumed_posts,blocked_reason FROM ppl_repair_plans WHERE repair_plan_id='rp_uncertain'").fetchone()
        assert tuple(plan) == ("READY", 0, "UNCERTAIN_RETRY_AUTHORIZED")
        cand = con.execute("SELECT simulation_status,execution_action FROM ppl_candidates WHERE candidate_id='child'").fetchone()
        assert tuple(cand) == ("ERROR", "RETRY_PER_V21_POLICY")
        event_count = con.execute("SELECT COUNT(*) FROM ppl_round_events WHERE event_type='UNCERTAIN_REPAIR_RETRY_AUTHORIZED'").fetchone()[0]
        assert event_count == 1

    # Simulate the one authorized retry becoming uncertain again.  The durable
    # authorization event makes a third POST fail closed.
    with sqlite3.connect(alpha_db) as con:
        con.execute("UPDATE alpha_results SET status='UNCERTAIN_SUBMISSION' WHERE sim_key='child_key'")
        con.commit()
    with store.connect() as con:
        con.execute("UPDATE ppl_candidates SET simulation_status='UNCERTAIN_SUBMISSION' WHERE candidate_id='child'")
        con.execute("UPDATE ppl_repair_plans SET consumed_posts=1,committed_posts=1,blocked_reason='UNCERTAIN_SUBMISSION_HOLD' WHERE repair_plan_id='rp_uncertain'")
    with pytest.raises(ConfigError, match="UNCERTAIN_REPAIR_RETRY_EXHAUSTED"):
        authorize_uncertain_repair_retry(
            store, alpha_db, run_id="run_0001", confirm_duplicate_risk=True,
        )


def test_recovered_repair_batch_finalizes_without_reposting(tmp_path, monkeypatch):
    c, store, policy = setup_round(tmp_path)
    alpha = tmp_path / "alpha.db"
    make_alpha_db(alpha, [{"sim_key":"sk_child","alpha_id":"A_CHILD","status":"COMPLETE",
                           "sharpe":1.5,"fitness":0.8,"turnover":0.4}])
    insert_candidate(store,"run_0001","parent","sk_parent",state="PRE_TAG_CHECK_COMPLETE",
                     alpha_id="A_PARENT",sim_status="COMPLETE")
    insert_candidate(store,"run_0001","child","sk_child",state="PRE_TAG_CHECK_COMPLETE",
                     alpha_id="A_CHILD",sim_status="COMPLETE")
    _insert_repair_recovery_plan(store, plan_id="rp1", signature="sig1")
    with store.connect() as con:
        con.execute("UPDATE ppl_repair_plans SET plan_status='EXECUTED',consumed_posts=1 WHERE repair_plan_id='rp1'")
        con.execute(
            """INSERT INTO ppl_repairs(repair_id,run_id,parent_candidate_id,child_candidate_id,repair_type,
               repair_signature,repair_path_json,repair_depth,before_json,after_json,delta_json,
               side_effect_verdict,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("repair1","run_0001","parent","child","TEST_REPAIR","sig1","[]",1,"{}","{}","{}","WORSE","now"),
        )
    start_batch(store,"round_run_0001",1,"REPAIR",plan_ids=["rp1"],projected_new_posts=1,
                planned_post_sim_keys=["sk_child"],planned_resume_sim_keys=[])
    with store.connect() as con:
        con.execute("UPDATE ppl_round_batches SET status='RECOVERED',logical_posts_consumed=1 WHERE round_id='round_run_0001' AND batch_no=1")
    calls={"refresh":0,"telemetry":0,"reports":0}
    monkeypatch.setattr("ppl_engine.round_orchestrator.finalize_family_winners",lambda *a,**k:{"protected_total":0})
    monkeypatch.setattr("ppl_engine.round_orchestrator._maybe_auto_refresh_manual_finalization",
                        lambda *a,**k:{"executed_check_count":0})
    def fake_tel(*a,**k): calls["telemetry"]+=1; return {}
    monkeypatch.setattr("ppl_engine.round_orchestrator._sync_research_telemetry",fake_tel)
    monkeypatch.setattr("ppl_engine.round_orchestrator._write_reports",lambda *a,**k:calls.__setitem__("reports",calls["reports"]+1) or {})
    out=_finalize_recovered_repair_batch(
        store,c,object(),object(),alpha,"run_0001","round_run_0001",
        _batch_by_no(store,"round_run_0001",1),policy,tmp_path,
    )
    assert out["finalized"] is True
    assert out["logical_posts_consumed"]==1
    batch=_batch_by_no(store,"round_run_0001",1)
    assert batch["status"]=="COMPLETED"
    assert calls["telemetry"]==1 and calls["reports"]==1
