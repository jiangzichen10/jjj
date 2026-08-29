import copy
import json
from pathlib import Path

import pytest

from ppl_engine.config import load_effective_config
from ppl_engine.policy_runtime import (
    HOT_POLICY_TYPES,
    PolicyBundleReloadStatus,
    apply_policy_bundle_safe_checkpoint,
    hot_policy_bundle_only_drift,
    initialize_or_restore_policy_bundle,
)
from ppl_engine.qualification_policy import clear_qualification_policy_runtime_cache
from ppl_engine.research_telemetry import load_events
from ppl_engine.round_orchestrator import load_round_policy
from ppl_engine.round_store import (
    activate_policy_bundle_atomic, create_round, get_round, load_policy_state,
)
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_v31.yaml", project_dir=ROOT)


def _policy():
    return load_round_policy(ROOT / "ppl_round_v31.yaml", _cfg())


def _setup(tmp_path):
    cfg = _cfg(); policy = _policy()
    store = RunnerStore(tmp_path / "runner.db")
    store.initialize(); store.create_run("run_0006", cfg)
    create_round(store, round_id="round_run_0006", run_id="run_0006", policy=policy,
                 total_budget=2000, search_budget=1600, repair_budget=400)
    clear_qualification_policy_runtime_cache()
    init = initialize_or_restore_policy_bundle(
        store, round_id="round_run_0006", run_id="run_0006", policy=policy,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    return store, cfg, policy, init


def _edit_search(base, version="V31_SEARCH_COMPAT_002"):
    out = copy.deepcopy(base)
    out["search_policy"]["allocation"]["exploration_fraction"] = 0.35
    out["policy_versions"]["search"] = version
    return out


def _edit_repair(base, version="V31_REPAIR_POLICY_002"):
    out = copy.deepcopy(base)
    out["repair_policy"]["ranking"]["repair_good_sharpe_min"] = 2.1
    out["policy_versions"]["repair"] = version
    return out


def _edit_qualification(base, version="V31_QUAL_COMPAT_003"):
    out = copy.deepcopy(base)
    out["qualification_integration"]["policy_version"] = version
    out["policy_versions"]["qualification"] = version
    out["ppl_classification"]["manual_finalization"]["ppc_strategy"]["clean_max"] = 0.49
    return out


def test_c6_initializes_three_durable_policy_types_atomically(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    assert init.status is PolicyBundleReloadStatus.INITIALIZED
    assert set(init.changed_types) == set(HOT_POLICY_TYPES)
    for ptype in HOT_POLICY_TYPES:
        state = load_policy_state(store, "round_run_0006", ptype)
        assert state is not None
        assert len(state["policy_hash"]) == 64
    events = load_events(store, "round_run_0006")
    assert len([e for e in events if e["event_type"] == "POLICY_BUNDLE_INITIALIZED"]) == 1
    clear_qualification_policy_runtime_cache()


def test_c6_hot_scope_allows_search_repair_qualification_but_rejects_scheduler_drift():
    base = _policy()
    candidate = _edit_repair(_edit_search(_edit_qualification(base)))
    assert hot_policy_bundle_only_drift(base, candidate) is True
    candidate["scheduler_shadow"]["backlog_weight"] = 9.0
    assert hot_policy_bundle_only_drift(base, candidate) is False


def test_c6_search_only_reload_is_atomic_and_does_not_reactivate_unchanged_qr(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    old_q = load_policy_state(store, "round_run_0006", "QUALIFICATION")
    old_r = load_policy_state(store, "round_run_0006", "REPAIR")
    edited = _edit_search(base)
    result = apply_policy_bundle_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=edited, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=11, phase="SEARCH", checkpoint_safe=True,
    )
    assert result.status is PolicyBundleReloadStatus.RELOADED
    assert result.changed_types == ("SEARCH",)
    s = load_policy_state(store, "round_run_0006", "SEARCH")
    q = load_policy_state(store, "round_run_0006", "QUALIFICATION")
    r = load_policy_state(store, "round_run_0006", "REPAIR")
    assert s["policy_version"] == "V31_SEARCH_COMPAT_002"
    assert int(s["activated_batch_no"]) == 11
    assert q["activated_at"] == old_q["activated_at"]
    assert r["activated_at"] == old_r["activated_at"]
    rr = get_round(store, round_id="round_run_0006")
    active = json.loads(rr["config_json"])
    assert active["search_policy"]["allocation"]["exploration_fraction"] == 0.35
    assert active["policy_versions"]["repair"] == base["policy_versions"]["repair"]
    events = [e for e in load_events(store, "round_run_0006") if e["event_type"] == "POLICY_BUNDLE_RELOADED"]
    assert len(events) == 1
    payload = json.loads(events[0]["payload_json"])
    assert payload["atomic_activation"] is True
    assert payload["simulation_identity_unchanged"] is True
    assert payload["changed_types"] == ["SEARCH"]
    clear_qualification_policy_runtime_cache()


def test_c6_combined_qsr_change_activates_as_one_bundle(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    edited = _edit_repair(_edit_search(_edit_qualification(base)))
    result = apply_policy_bundle_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=edited, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=12, phase="REPAIR", checkpoint_safe=True,
    )
    assert result.status is PolicyBundleReloadStatus.RELOADED
    assert set(result.changed_types) == set(HOT_POLICY_TYPES)
    for ptype in HOT_POLICY_TYPES:
        state = load_policy_state(store, "round_run_0006", ptype)
        assert int(state["activated_batch_no"]) == 12
    rr = json.loads(get_round(store, round_id="round_run_0006")["config_json"])
    assert rr["policy_versions"]["qualification"] == "V31_QUAL_COMPAT_003"
    assert rr["policy_versions"]["search"] == "V31_SEARCH_COMPAT_002"
    assert rr["policy_versions"]["repair"] == "V31_REPAIR_POLICY_002"
    clear_qualification_policy_runtime_cache()


def test_c6_semantic_change_requires_version_bump_and_preserves_all_active_states(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    old = {p: load_policy_state(store, "round_run_0006", p) for p in HOT_POLICY_TYPES}
    bad = copy.deepcopy(base)
    bad["repair_policy"]["ranking"]["repair_good_sharpe_min"] = 2.1
    result = apply_policy_bundle_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=bad, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=13, phase="REPAIR", checkpoint_safe=True,
    )
    assert result.status is PolicyBundleReloadStatus.REJECTED_VERSION_BUMP_REQUIRED
    for ptype in HOT_POLICY_TYPES:
        state = load_policy_state(store, "round_run_0006", ptype)
        assert state["policy_hash"] == old[ptype]["policy_hash"]
        assert state["policy_version"] == old[ptype]["policy_version"]
    clear_qualification_policy_runtime_cache()


def test_c6_non_hot_drift_rejected_last_known_good_continues(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    bad = _edit_search(base)
    bad["scheduler_shadow"]["backlog_weight"] = 9.0
    result = apply_policy_bundle_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=bad, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=14, phase="SEARCH", checkpoint_safe=True,
    )
    assert result.status is PolicyBundleReloadStatus.REJECTED_NON_HOT_POLICY_DRIFT
    assert result.policy["search_policy"]["allocation"]["exploration_fraction"] == 0.3
    clear_qualification_policy_runtime_cache()


def test_c6_unsafe_checkpoint_defers_entire_bundle(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    edited = _edit_repair(_edit_search(base))
    result = apply_policy_bundle_safe_checkpoint(
        store, round_id="round_run_0006", run_id="run_0006", active_policy=base,
        candidate_policy=edited, project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
        batch_no=15, phase="SEARCH", checkpoint_safe=False,
    )
    assert result.status is PolicyBundleReloadStatus.DEFERRED_UNSAFE_CHECKPOINT
    assert load_policy_state(store, "round_run_0006", "SEARCH")["policy_version"] == base["policy_versions"]["search"]
    assert load_policy_state(store, "round_run_0006", "REPAIR")["policy_version"] == base["policy_versions"]["repair"]
    clear_qualification_policy_runtime_cache()


def test_c6_restart_restores_durable_qsr_before_new_file_candidate(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    file_candidate = _edit_repair(_edit_search(_edit_qualification(base)))
    clear_qualification_policy_runtime_cache()
    restored = initialize_or_restore_policy_bundle(
        store, round_id="round_run_0006", run_id="run_0006", policy=file_candidate,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )
    assert restored.status is PolicyBundleReloadStatus.RESTORED
    assert restored.policy["policy_versions"]["qualification"] == base["policy_versions"]["qualification"]
    assert restored.policy["policy_versions"]["search"] == base["policy_versions"]["search"]
    assert restored.policy["policy_versions"]["repair"] == base["policy_versions"]["repair"]
    assert restored.policy["search_policy"]["allocation"]["exploration_fraction"] == 0.3
    assert restored.policy["repair_policy"]["ranking"]["repair_good_sharpe_min"] == 2.0
    clear_qualification_policy_runtime_cache()


def test_c6_atomic_store_rolls_back_policy_rows_and_round_config_on_late_failure(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    before = {p: load_policy_state(store, "round_run_0006", p) for p in HOT_POLICY_TYPES}
    rr_before = get_round(store, round_id="round_run_0006")["config_json"]
    rows = []
    for ptype in HOT_POLICY_TYPES:
        state = before[ptype]
        payload = copy.deepcopy(state["payload"])
        rows.append({
            "policy_type": ptype, "policy_version": state["policy_version"] + "_X",
            "policy_hash": "f" * 64, "payload": payload, "source_path": state.get("source_path"),
            "activated_batch_no": 99,
        })
    with pytest.raises(ValueError, match="POLICY_BUNDLE_EVENT_KEY_REQUIRED"):
        activate_policy_bundle_atomic(
            store, "round_run_0006", "run_0006", states=rows,
            active_config={**base, "objective": "SHOULD_ROLLBACK"}, activated_batch_no=99,
            event={"event_type": "POLICY_BUNDLE_RELOADED", "payload": {}},
        )
    for ptype in HOT_POLICY_TYPES:
        state = load_policy_state(store, "round_run_0006", ptype)
        assert state["policy_version"] == before[ptype]["policy_version"]
        assert state["policy_hash"] == before[ptype]["policy_hash"]
    assert get_round(store, round_id="round_run_0006")["config_json"] == rr_before
    clear_qualification_policy_runtime_cache()


def test_c6_restart_uses_durable_bundle_even_if_current_file_hot_policy_is_invalid(tmp_path):
    store, cfg, base, init = _setup(tmp_path)
    broken_file_candidate = copy.deepcopy(base)
    broken_file_candidate["search_policy"]["schema"] = "BROKEN_SCHEMA"
    broken_file_candidate["repair_policy"]["allocation"]["batch_size"] = 0
    broken_file_candidate["qualification_integration"]["policy_version"] = "BROKEN_FILE_EDIT"
    broken_file_candidate["policy_versions"]["qualification"] = "BROKEN_FILE_EDIT"
    clear_qualification_policy_runtime_cache()

    restored = initialize_or_restore_policy_bundle(
        store, round_id="round_run_0006", run_id="run_0006", policy=broken_file_candidate,
        project_dir=ROOT, source_path=ROOT / "ppl_round_v31.yaml",
    )

    assert restored.status is PolicyBundleReloadStatus.RESTORED
    assert restored.policy["policy_versions"]["qualification"] == base["policy_versions"]["qualification"]
    assert restored.policy["policy_versions"]["search"] == base["policy_versions"]["search"]
    assert restored.policy["policy_versions"]["repair"] == base["policy_versions"]["repair"]
    assert restored.policy["search_policy"]["schema"] == base["search_policy"]["schema"]
    assert restored.policy["repair_policy"]["allocation"]["batch_size"] == base["repair_policy"]["allocation"]["batch_size"]
    clear_qualification_policy_runtime_cache()
