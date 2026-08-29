import copy
import json
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.ppl_classifier import load_ppl_classification_policy_for_config
from ppl_engine.policy_runtime import (
    QUALIFICATION_POLICY_TYPE,
    QualificationReloadStatus,
    apply_qualification_policy_safe_checkpoint,
    initialize_or_restore_qualification_policy,
    qualification_only_drift,
)
from ppl_engine.qualification_policy import (
    clear_qualification_policy_runtime_cache,
    load_qualification_policy_snapshot,
)
from ppl_engine.research_telemetry import load_events
from ppl_engine.round_orchestrator import load_round_policy
from ppl_engine.round_store import create_round, get_round, load_policy_state
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config(project_dir=ROOT):
    return load_effective_config(
        Path(project_dir) / "ppl_rules.yaml", Path(project_dir) / "ppl_plan_v31.yaml",
        project_dir=Path(project_dir),
    )


def _policy(project_dir=ROOT):
    return load_round_policy(Path(project_dir) / "ppl_round_v31.yaml", _config(project_dir))


def _setup(tmp_path, policy=None):
    cfg = _config()
    policy = copy.deepcopy(policy or _policy())
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize()
    store.create_run("run_0006", cfg)
    create_round(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        total_budget=2000, search_budget=1600, repair_budget=400,
    )
    return store, cfg, policy


def _edited_qualification(policy, version="V31_QUAL_COMPAT_003", *, clean_max=0.49):
    out = copy.deepcopy(policy)
    out["qualification_integration"]["policy_version"] = version
    out["policy_versions"]["qualification"] = version
    out["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] = clean_max
    return out


def test_c3_qualification_only_drift_scope_is_strict():
    base = _policy()
    qual = _edited_qualification(base)
    assert qualification_only_drift(base, qual) is True
    search = copy.deepcopy(qual)
    search["exploration_fraction"] = 0.31
    assert qualification_only_drift(base, search) is False


def test_c3_initializes_durable_active_policy_and_runtime_snapshot(tmp_path):
    store, cfg, policy = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    result = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    assert result.status is QualificationReloadStatus.INITIALIZED
    state = load_policy_state(store, "round_run_0006", QUALIFICATION_POLICY_TYPE)
    assert state["policy_version"] == "V31_QUAL_COMPAT_002"
    assert len(state["policy_hash"]) == 64
    snap = load_qualification_policy_snapshot(ROOT)
    assert snap.policy_hash == state["policy_hash"]
    clear_qualification_policy_runtime_cache()


def test_c3_unsafe_checkpoint_defers_change_without_mutating_active_state(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initial = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    edited = _edited_qualification(base)
    result = apply_qualification_policy_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006",
        active_policy=base, candidate_policy=edited, project_dir=ROOT,
        source_path=ROOT / "ppl_round_v31.yaml", batch_no=7, phase="SEARCH",
        checkpoint_safe=False,
    )
    assert result.status is QualificationReloadStatus.DEFERRED_UNSAFE_CHECKPOINT
    state = load_policy_state(store, "round_run_0006", QUALIFICATION_POLICY_TYPE)
    assert state["policy_hash"] == initial.active_hash
    assert state["policy_version"] == "V31_QUAL_COMPAT_002"
    clear_qualification_policy_runtime_cache()


def test_c3_changed_rules_require_explicit_policy_version_bump(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initial = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    bad = copy.deepcopy(base)
    bad["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] = 0.49
    result = apply_qualification_policy_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=bad, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=8, phase="SEARCH", checkpoint_safe=True,
    )
    assert result.status is QualificationReloadStatus.REJECTED_VERSION_BUMP_REQUIRED
    assert result.active_hash == initial.active_hash
    state = load_policy_state(store, "round_run_0006", QUALIFICATION_POLICY_TYPE)
    assert state["policy_version"] == "V31_QUAL_COMPAT_002"
    clear_qualification_policy_runtime_cache()


def test_c3_nonqualification_edit_is_rejected_by_qualification_controller(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    bad = _edited_qualification(base)
    bad["batch_size"] = 41
    result = apply_qualification_policy_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=bad, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=8, phase="SEARCH", checkpoint_safe=True,
    )
    assert result.status is QualificationReloadStatus.REJECTED_NON_QUALIFICATION_DRIFT
    assert result.policy["batch_size"] == 40
    clear_qualification_policy_runtime_cache()


def test_c3_valid_safe_reload_is_durable_attributed_and_updates_runtime_only_at_checkpoint(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    first = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    edited = _edited_qualification(base)
    result = apply_qualification_policy_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=edited, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=9, phase="SEARCH", checkpoint_safe=True,
    )
    assert result.status is QualificationReloadStatus.RELOADED
    assert result.previous_hash == first.active_hash
    assert result.active_version == "V31_QUAL_COMPAT_003"
    assert result.active_hash != first.active_hash
    state = load_policy_state(store, "round_run_0006", QUALIFICATION_POLICY_TYPE)
    assert state["policy_version"] == "V31_QUAL_COMPAT_003"
    assert int(state["activated_batch_no"]) == 9
    rr = get_round(store, round_id="round_run_0006")
    active_round_policy = json.loads(rr["config_json"])
    assert active_round_policy["policy_versions"]["qualification"] == "V31_QUAL_COMPAT_003"
    assert active_round_policy["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] == 0.49
    snap = load_qualification_policy_snapshot(ROOT)
    assert snap.integration["policy_version"] == "V31_QUAL_COMPAT_003"
    assert snap.policy_hash == result.active_hash
    events = load_events(store, "round_run_0006")
    reloads = [x for x in events if x["event_type"] == "QUALIFICATION_POLICY_RELOADED"]
    assert len(reloads) == 1
    payload = json.loads(reloads[0]["payload_json"])
    assert payload["simulation_identity_unchanged"] is True
    assert payload["from_version"] == "V31_QUAL_COMPAT_002"
    assert payload["to_version"] == "V31_QUAL_COMPAT_003"
    clear_qualification_policy_runtime_cache()


def test_c3_restart_restores_durable_old_policy_before_candidate_file_is_activated(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initialized = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    # Simulate operator editing YAML to Q3 while the process is down and an
    # unfinished batch still needs recovery. Durable Q2 must remain active.
    file_candidate = _edited_qualification(base)
    clear_qualification_policy_runtime_cache()
    restored = initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=file_candidate,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    assert restored.status is QualificationReloadStatus.RESTORED
    assert restored.active_hash == initialized.active_hash
    assert restored.policy["policy_versions"]["qualification"] == "V31_QUAL_COMPAT_002"
    assert restored.policy["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] == 0.5
    snap = load_qualification_policy_snapshot(ROOT)
    assert snap.integration["policy_version"] == "V31_QUAL_COMPAT_002"
    clear_qualification_policy_runtime_cache()



def test_c3_new_classification_uses_reloaded_policy_version(monkeypatch, tmp_path):
    import ppl_engine.near_pass as near_pass

    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    edited = _edited_qualification(base)
    reloaded = apply_qualification_policy_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=edited, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=10, phase="SEARCH", checkpoint_safe=True,
    )
    assert reloaded.status is QualificationReloadStatus.RELOADED

    fake_context = {
        "candidates": {
            "cand_q": {
                "candidate_id": "cand_q", "simulation_status": "COMPLETE",
                "structure_status": "ELIGIBLE", "pp_total_operator_count_estimate": 2,
                "data_field_count_estimate": 1, "sim_key": "sim_q",
            }
        },
        "metrics_by_key": {"sim_q": {"sharpe": 2.2, "turnover": 0.5, "fitness": 1.1}},
        "check_rows_by_cid": {
            "cand_q": [
                {"raw_name": "LOW_SUB_UNIVERSE_SHARPE", "normalized_name": "LOW_SUB_UNIVERSE_SHARPE", "raw_result": "PASS", "normalized_result": "PASS", "eligibility_outcome": "PASS", "raw_value_json": 2.0, "raw_limit_json": 1.5},
                {"raw_name": "POWER_POOL_CORRELATION", "normalized_name": "POWER_POOL_CORRELATION", "raw_result": "PASS", "normalized_result": "PASS", "eligibility_outcome": "PASS", "raw_value_json": 0.4, "raw_limit_json": 0.5},
                {"raw_name": "MATCHES_THEMES", "normalized_name": "MATCHES_THEMES", "raw_result": "PASS", "normalized_result": "PASS", "eligibility_outcome": "PASS"},
            ]
        },
        "repairs_by_parent": {"cand_q": []},
    }
    monkeypatch.setattr(near_pass, "build_rescue_context", lambda *a, **k: fake_context)
    monkeypatch.setattr(near_pass, "_latest_pretag_session_status_by_candidate", lambda *a, **k: {})
    out = near_pass.classify_run(object(), cfg, ROOT / "unused.db", "run_0006")
    assert out[0]["qualification_policy_version"] == "V31_QUAL_COMPAT_003"
    assert out[0]["qualification_policy_hash"] == reloaded.active_hash
    clear_qualification_policy_runtime_cache()


def test_c3_repeated_same_rejected_candidate_does_not_spam_events(tmp_path):
    store, cfg, base = _setup(tmp_path)
    clear_qualification_policy_runtime_cache()
    initialize_or_restore_qualification_policy(
        store, round_id="round_run_0006", run_id="run_0006", policy=base,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    bad = copy.deepcopy(base)
    bad["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] = 0.49
    for _ in range(2):
        result = apply_qualification_policy_safe_checkpoint(
            store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
            candidate_policy=bad, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
            batch_no=8, phase="SEARCH", checkpoint_safe=True,
        )
        assert result.status is QualificationReloadStatus.REJECTED_VERSION_BUMP_REQUIRED
    events = load_events(store, "round_run_0006")
    rejected = [x for x in events if x["event_type"] == "QUALIFICATION_POLICY_RELOAD_REJECTED"]
    assert len(rejected) == 1
    clear_qualification_policy_runtime_cache()

def test_c3_does_not_add_hypothetical_two_year_rule():
    text = (ROOT / "ppl_round_v31.yaml").read_text(encoding="utf-8")
    assert "TWO_YEAR_SHARPE_MIN" not in text
    assert "value: 1.58" not in text
