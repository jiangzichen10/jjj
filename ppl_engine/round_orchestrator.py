"""V3 reusable, resumable Production Research round orchestrator.

The orchestrator is intentionally thin: V2.2 remains the workflow/simulation
engine. V3 coordinates batches, budget partitions, family protection, adaptive
selection, resumability and reports without modifying machine_lib_V2_1.py.

Real network behavior is still explicit: callers must pass
allow_simulation_post=True. Final Submit, PowerPoolSelected and PATCH are never
performed here. DELETE is restricted to the explicit Remote Simulation
Resolver and always follows GET plus post-delete verification.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml

from .audit_log import (
    audit_event, audit_state_transition, audit_log_path, resolve_audit_config,
)
from .candidate_factory import classify_cache_read_only, generate_candidate_preview
from .check_derived_repair import derive_check_repair_proposals
from .config import (
    ConfigError, config_with_extension_execution_identity,
    execution_hash_status_for_run, simulation_budget_allocation,
)
from .continuous_policy import continuous_policy_dict, parse_continuous_policy
from .continuous_progress import ContinuousProgressRenderer, build_continuous_progress_snapshot
from .continuous_runtime import budget_view, phase_capacity, resolve_invocation_batch_limit
from .continuous_remote import (
    poll_due_remote_work, remote_slot_snapshot, sync_remote_work_from_durable_facts,
)
from .continuous_check import enqueue_manual_refresh_checks, enqueue_pretag_checks, poll_due_checks
from .continuous_control import due_snapshot, recover_waiting_auth
from .continuous_discovery import (
    enqueue_discovery_refresh, mark_discovery_applied, mark_discovery_applying,
    mark_discovery_expansion_suppressed,
    materialize_discovery_result, poll_due_discovery_work, ready_discovery_work,
)
from .discovery import DiscoveryResult, ReadOnlySession, discover_online, discover_rolling_online
from .dry_run import estimate_candidate_plan
from .description import draft_power_pool_description
from .family import family_id
from .live_execution import (
    EXPECTED_MACHINE_HASH,
    MACHINE_HASH_OPERATION_START,
    MACHINE_HASH_OPERATION_MANUAL_REFRESH,
    MACHINE_HASH_OPERATION_RESUME,
    _alpha_facts,
    _check_result_display_num,
    _instrument_v21,
    _sync_candidate_fact,
    _v21_candidate,
    execute_continuous_remote_handoff,
    validate_machine_lib_hash,
    run_local_analysis,
    run_one_pretag_check,
    refresh_one_pretag_check,
)
from .near_pass import classify_run, load_external_evidence, preview_rescue
from .operator_registry import build_project_operator_evidence
from .production_repair import (
    execute_round_repair, preflight_round_repair_execution,
    preview_production_repair, preview_production_repair_read_only,
    preview_production_repair_plan_rows_read_only, reconcile_completed_repair_outcomes,
)
from .repair_engine import neutralization_micro_tune_spec, same_family_micro_tune_spec
from .repair_engine import TURNOVER_STAGED_STRATEGIES, is_retired_auto_repair_plan
from .ppc_controlled_branch import (
    PPC_POLICY_NAME, PPC_POLICY_VERSION, PPC_TARGET_FAILURE, infer_ppc_branch_anchor, ppc_branch_config,
    ppc_branch_state, resolve_effective_window,
)
from .turnover_staged_repair import preview_turnover_staged_plans, sync_turnover_staged_plans
from .round_store import (
    COMPLETED_RESEARCH_BATCH_STATUSES,
    create_round,
    ensure_round_schema,
    finish_batch,
    get_round,
    load_batches,
    load_winners,
    load_dataset_states,
    load_dataset_refreshes,
    load_policy_state,
    record_dataset_refresh,
    upsert_dataset_state,
    set_batch_intent,
    start_batch,
    update_round,
    upsert_winner,
)
from .simulation_adapter import (
    POST_ACTIONS, execute_with_v21, production_remote_resolution_status,
    server_slot_deferred_sim_keys,
)
from .remote_simulation import (
    remote_resolution_audit_payload, resolve_remote_simulation,
    validate_simulation_url,
)
from .state_machine import CANDIDATE_TRANSITIONS, RUN_TRANSITIONS
from .strategy_contracts import SchedulerActionType
from .search_strategy import (
    adaptive_scores as search_adaptive_scores,
    stage_evidence_score as search_stage_evidence_score,
    select_search_candidates,
)
from .repair_strategy import (
    direction_repair_value as strategy_direction_repair_value,
    rank_direction_repair_candidates,
    rank_repair_candidates,
    repair_value as strategy_repair_value,
)
from .scheduler_shadow import (
    SHADOW_ONLY_MODE, QueueFacts, ResearchAvailabilityFacts, ShadowSchedulerSnapshot,
    choose_shadow_action, policy_from_mapping, productivity_windows, shadow_policy_hash,
)
from .scheduler_evidence import (
    deterministic_replay, evidence_policy_from_mapping, evidence_policy_hash,
    record_scheduler_evaluation, refresh_scheduler_outcomes,
)
from .policy_specs import (
    effective_repair_allocation, effective_repair_planning, effective_repair_ranking,
    effective_search_allocation, effective_search_diversity, effective_search_ranking,
    install_dedicated_policy_sections,
    repair_policy_hash, search_policy_hash,
)
from .policy_runtime import (
    apply_policy_bundle_safe_checkpoint, hot_policy_bundle_only_drift,
    initialize_or_restore_policy_bundle, restore_policy_bundle_runtime_from_durable_state,
)
from .strategy_compat import (
    SEARCH_COMPAT_STRATEGY, REPAIR_COMPAT_STRATEGY, SCHEDULER_COMPAT_MODE,
    choose_compatibility_strategy_action, policy_versions_from_policy,
    repair_decisions_from_selected_plans, search_decisions_from_selected_rows,
)
from .research_run_mode import (
    COMPATIBILITY_EVIDENCE_MODE, parse_research_run_policy, research_run_status,
    validate_durable_research_run_lock, validate_new_research_run,
)
from .research_telemetry import (
    TELEMETRY_VERSION,
    failure_matrix,
    _latest_check_rows,
    load_decisions,
    load_events,
    load_ledger,
    load_manifest,
    load_snapshots,
    record_candidate_universe,
    record_event,
    sync_durable_events,
    sync_simulation_ledger,
    upsert_candidate_decision,
    upsert_manifest,
    upsert_snapshot,
)

ROUND_SOURCE = "V3_ROUND"
SUCCESS_STATES = {
    "PRE_TAG_CHECK_PASS", "FAMILY_DEDUP", "PRE_TAG_FINALIST", "DESCRIPTION_DRAFT",
    "DESCRIPTION_VALIDATED", "AWAITING_MANUAL_PROPERTIES", "PPL_TAGGED",
    "FINAL_CHECK_PENDING", "FINAL_CHECK_COMPLETE", "FINAL_CHECK_PASS",
    "READY_FOR_MANUAL_SUBMIT", "SUBMITTED",
}
EXECUTABLE_REPAIR_STATUSES = {
    "DEFERRED_INITIAL_SEARCH", "DEFERRED_PHASE_END", "PLANNED",
    "BLOCKED_OPERATOR_VALIDATION", "BLOCKED_BUDGET", "READY",
}
LOGICAL_CONSUMED_STATUSES = {"COMPLETE", "RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"}
EXTENSION_SELECTION_POLICY = {
    # First-stage canary: named policy rather than a selector-local magic number.
    "max_new_operator_per_batch": 1,
    "max_new_operator_fraction": 0.10,
}
EXTENSION_POLICY_VERSION = "CANDIDATE_EXTENSION_POLICY_V1"
EXTENSION_CONTEXT_KEY = "candidate_extension_context_v1"


def _durable_confirmed_post(fact: Mapping[str, Any]) -> bool:
    """A status cannot create/refund budget; durable submit identity can."""
    if fact.get("simulation_url") and fact.get("submitted_at"):
        return True
    # Legacy cache rows can predate submitted_at persistence.  A durable alpha
    # identity plus a COMPLETE result is itself proof that the POST occurred;
    # neither field is created by the resolver's lifecycle transition.
    return bool(
        fact.get("alpha_id")
        and str(fact.get("status") or "").upper() in LOGICAL_CONSUMED_STATUSES
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_round_policy(path: Path, config: Any) -> Dict[str, Any]:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or int(raw.get("schema_version", 0)) != 1:
        raise ConfigError("ROUND_POLICY_SCHEMA_UNSUPPORTED")
    required = (
        "objective", "batch_size", "exploration_fraction",
        "normal_near_pass_repair_cap_per_family",
        "strong_near_pass_repair_cap_per_family", "report_dir",
    )
    missing = [k for k in required if k not in raw]
    if missing:
        raise ConfigError("ROUND_POLICY_MISSING: " + ",".join(missing))
    batch = int(raw["batch_size"])
    if batch <= 0:
        raise ConfigError("ROUND_BATCH_SIZE_MUST_BE_POSITIVE")
    explore = float(raw["exploration_fraction"])
    if not 0.0 <= explore <= 1.0:
        raise ConfigError("ROUND_EXPLORATION_FRACTION_OUT_OF_RANGE")
    rolling = dict(raw.get("rolling_discovery") or {})
    defaults = {
        "enabled": False,
        "refresh_every_search_batches": 5,
        "min_search_batches_before_refresh": 5,
        "max_refreshes": 12,
        "probe_new_datasets_per_refresh": 6,
        "admit_new_datasets_per_refresh": 3,
        "min_active_datasets": 10,
        "low_pool_trigger_families": 80,
        "low_pool_min_batches_since_refresh": 2,
        "cooldown_min_attempts": 8,
        "cooldown_checked_rate_max": 0.10,
        "cooldown_viable_rate_max": 0.10,
        "cooldown_without_admission": True,
        "max_cooldown_per_refresh": 3,
        "preserve_success_datasets": True,
        "never_revisit_dataset_within_round": True,
    }
    defaults.update(rolling)
    rolling = defaults
    if int(rolling["refresh_every_search_batches"]) <= 0:
        raise ConfigError("ROUND_ROLLING_DISCOVERY_REFRESH_INTERVAL_INVALID")
    if int(rolling["probe_new_datasets_per_refresh"]) < int(rolling["admit_new_datasets_per_refresh"]):
        raise ConfigError("ROUND_ROLLING_DISCOVERY_PROBE_LT_ADMIT")
    if int(rolling["admit_new_datasets_per_refresh"]) <= 0:
        raise ConfigError("ROUND_ROLLING_DISCOVERY_ADMIT_INVALID")
    if not 0.0 <= float(rolling["cooldown_checked_rate_max"]) <= 1.0:
        raise ConfigError("ROUND_ROLLING_DISCOVERY_CHECKED_RATE_INVALID")
    if not 0.0 <= float(rolling["cooldown_viable_rate_max"]) <= 1.0:
        raise ConfigError("ROUND_ROLLING_DISCOVERY_VIABLE_RATE_INVALID")
    if int(rolling["max_cooldown_per_refresh"]) < 0:
        raise ConfigError("ROUND_ROLLING_DISCOVERY_COOLDOWN_CAP_INVALID")
    raw["rolling_discovery"] = rolling

    adaptive = dict(raw.get("adaptive_ranking") or {})
    adaptive_defaults = {
        "prior_initial_scale": 0.30,
        "prior_min_scale": 0.12,
        "prior_half_life_attempts": 8.0,
        "evidence_shrinkage_attempts": 10.0,
        "exploration_evidence_fraction": 0.20,
        "exploration_novelty_weight": 10.0,
        "terminal_fail_penalty": 32.0,
        "zero_viable_penalty": 8.0,
        "zero_viable_min_attempts": 4,
        "recent_zero_positive_min_attempts": 8,
        "recent_zero_positive_penalty": 18.0,
        "recent_zero_positive_consecutive_batches_cooldown": 2,
        "exploit_min_combo_attempts": 2,
        "exploit_min_combo_viable": 1,
        "search_positive_sharpe_min": 1.60,
        "search_strong_sharpe_min": 2.00,
        "search_elite_sharpe_min": 3.00,
        "repair_good_sharpe_min": 2.00,
        "repair_elite_sharpe_min": 3.00,
        "repair_turnover_near_max": 0.80,
        "repair_turnover_mid_max": 1.00,
        "direction_repair_positive_abs_sharpe_min": 1.60,
        "direction_repair_strong_abs_sharpe_min": 2.00,
        "direction_repair_elite_abs_sharpe_min": 3.00,
        "max_unproven_dataset_fraction": 0.15,
        "max_unproven_combo_per_batch": 4,
        "dimension_weights": {
            "dataset": 0.10,
            "operator": 0.20,
            "dataset_operator": 0.35,
            "operator_window": 0.15,
            "dataset_operator_window": 0.20,
        },
        "stage_weights": {
            # SEARCH evidence only. Broad signal/fixed-repairable counts remain
            # telemetry but must not make a Dataset×Operator exploitable.
            "signal_viable": 0.0,
            "local_pass": 0.0,
            "fixed_repairable": 0.0,
            "search_viable": 22.0,
            "search_strong": 14.0,
            "search_elite": 24.0,
            "repair_viable": 0.0,
            "repair_elite": 0.0,
            "pretag_resolved": 10.0,
            "ppl_near_pass": 35.0,
            "ppl_strong_near_pass": 55.0,
            "ppl_success": 100.0,
        },
    }
    for key, value in adaptive_defaults.items():
        if key not in adaptive:
            adaptive[key] = value
    adaptive["dimension_weights"] = {**adaptive_defaults["dimension_weights"], **dict(adaptive.get("dimension_weights") or {})}
    adaptive["stage_weights"] = {**adaptive_defaults["stage_weights"], **dict(adaptive.get("stage_weights") or {})}
    if float(adaptive["prior_initial_scale"]) < float(adaptive["prior_min_scale"]):
        raise ConfigError("ROUND_ADAPTIVE_PRIOR_SCALE_INVALID")
    if float(adaptive["prior_half_life_attempts"]) <= 0 or float(adaptive["evidence_shrinkage_attempts"]) <= 0:
        raise ConfigError("ROUND_ADAPTIVE_SHRINKAGE_INVALID")
    if not 0.0 <= float(adaptive["exploration_evidence_fraction"]) <= 1.0:
        raise ConfigError("ROUND_ADAPTIVE_EXPLORATION_EVIDENCE_FRACTION_INVALID")
    if float(adaptive["terminal_fail_penalty"]) < 0 or float(adaptive["zero_viable_penalty"]) < 0:
        raise ConfigError("ROUND_ADAPTIVE_FAILURE_PENALTY_INVALID")
    if int(adaptive["zero_viable_min_attempts"]) < 1 or int(adaptive["exploit_min_combo_attempts"]) < 1:
        raise ConfigError("ROUND_ADAPTIVE_FAILURE_SAMPLE_INVALID")
    if int(adaptive["recent_zero_positive_min_attempts"]) < 1:
        raise ConfigError("ROUND_ADAPTIVE_RECENT_FAILURE_SAMPLE_INVALID")
    if float(adaptive["recent_zero_positive_penalty"]) < 0:
        raise ConfigError("ROUND_ADAPTIVE_RECENT_FAILURE_PENALTY_INVALID")
    if int(adaptive["recent_zero_positive_consecutive_batches_cooldown"]) < 2:
        raise ConfigError("ROUND_ADAPTIVE_RECENT_FAILURE_COOLDOWN_INVALID")
    if int(adaptive["exploit_min_combo_viable"]) < 1 or int(adaptive["max_unproven_combo_per_batch"]) < 1:
        raise ConfigError("ROUND_ADAPTIVE_EXPLOIT_GATE_INVALID")
    search_pos = float(adaptive["search_positive_sharpe_min"])
    search_strong = float(adaptive["search_strong_sharpe_min"])
    search_elite = float(adaptive["search_elite_sharpe_min"])
    repair_good = float(adaptive["repair_good_sharpe_min"])
    repair_elite = float(adaptive["repair_elite_sharpe_min"])
    repair_near = float(adaptive["repair_turnover_near_max"])
    repair_mid = float(adaptive["repair_turnover_mid_max"])
    direction_positive = float(adaptive["direction_repair_positive_abs_sharpe_min"])
    direction_strong = float(adaptive["direction_repair_strong_abs_sharpe_min"])
    direction_elite = float(adaptive["direction_repair_elite_abs_sharpe_min"])
    if not (1.0 <= search_pos <= search_strong <= search_elite):
        raise ConfigError("ROUND_ADAPTIVE_SEARCH_SHARPE_BANDS_INVALID")
    if not (1.0 <= repair_good <= repair_elite):
        raise ConfigError("ROUND_ADAPTIVE_REPAIR_SHARPE_BANDS_INVALID")
    if not (0.70 < repair_near <= repair_mid):
        raise ConfigError("ROUND_ADAPTIVE_REPAIR_TURNOVER_BANDS_INVALID")
    if not (1.0 <= direction_positive <= direction_strong <= direction_elite):
        raise ConfigError("ROUND_ADAPTIVE_DIRECTION_REPAIR_SHARPE_BANDS_INVALID")
    if not 0.0 < float(adaptive["max_unproven_dataset_fraction"]) <= 1.0:
        raise ConfigError("ROUND_ADAPTIVE_UNPROVEN_DATASET_FRACTION_INVALID")
    if abs(sum(float(x) for x in adaptive["dimension_weights"].values()) - 1.0) > 1e-9:
        raise ConfigError("ROUND_ADAPTIVE_DIMENSION_WEIGHTS_MUST_SUM_TO_ONE")
    raw["adaptive_ranking"] = adaptive

    # C5: V3.1 Continuous owns explicit Search/Repair policy sections. Older
    # snapshots may omit them, so synthesize parity sections from legacy knobs.
    if "continuous" in raw:
        try:
            raw = install_dedicated_policy_sections(raw)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc

    # V3.1 policy is opt-in. Legacy V3.0.x policies omit this section and
    # therefore keep byte-for-byte legacy lifecycle semantics after loading.
    if "continuous" in raw:
        raw["continuous"] = continuous_policy_dict(parse_continuous_policy(raw))

    # D2E research-run identity is optional for normal Continuous runs, but
    # when present it is a hard authority lock rather than a scheduler hint.
    try:
        parse_research_run_policy(raw)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    if "scheduler_shadow" in raw:
        shadow_cfg = dict(raw.get("scheduler_shadow") or {})
        if bool(shadow_cfg.get("enabled", False)) and "continuous" not in raw:
            raise ConfigError("SCHEDULER_SHADOW_REQUIRES_CONTINUOUS_POLICY")
        try:
            policy_from_mapping(shadow_cfg)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc

    manual_cfg = dict((raw.get("ppl_classification") or {}).get("manual_finalization") or {})
    refresh_every = int(manual_cfg.get("auto_refresh_every_batches", 10))
    if refresh_every <= 0:
        raise ConfigError("ROUND_MANUAL_FINALIZATION_REFRESH_INTERVAL_INVALID")
    ppc_cfg = dict(manual_cfg.get("ppc_strategy") or {})
    clean_max = float(ppc_cfg.get("clean_max", 0.50))
    mid_max = float(ppc_cfg.get("mid_max", 0.65))
    mid_sharpe = float(ppc_cfg.get("mid_min_sharpe", 2.00))
    if not (0.0 <= clean_max < mid_max <= 1.0):
        raise ConfigError("ROUND_MANUAL_FINALIZATION_PPC_BANDS_INVALID")
    if mid_sharpe < 0:
        raise ConfigError("ROUND_MANUAL_FINALIZATION_PPC_SHARPE_INVALID")

    allocation = simulation_budget_allocation(config.plan)
    raw = dict(raw)
    raw["total_budget"] = int(allocation["max_new_simulation_posts"])
    raw["search_budget"] = int(allocation["initial_search_budget"])
    raw["repair_budget"] = int(allocation["repair_reserve_budget"])
    if raw["total_budget"] != raw["search_budget"] + raw["repair_budget"]:
        raise ConfigError("V3_REQUIRES_FULL_BUDGET_ALLOCATION")
    return raw



def _round_policy_upgrade_compatible(stored: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    """Allow only audited additive V3 round-policy hotfix upgrades.

    V3.0.1 -> V3.0.2 added rolling discovery.  V3.0.2 -> V3.0.3 adds
    adaptive-ranking knobs and bumps policy version labels; budgets, batch size,
    exploration fraction and all execution-semantic policy stay unchanged.
    """
    stored = dict(stored)
    current = dict(current)
    stored_versions = dict(stored.get("policy_versions") or {})
    current_versions = dict(current.get("policy_versions") or {})

    # Qualified Check Refresh polling resilience is operational GET-only
    # configuration. Permit only the exact additive introduction of this
    # nested block; every scheduler, ranking, budget and execution field must
    # remain byte-equivalent after removing it.
    current_qcr = dict((current.get("continuous") or {}).get("qualified_check_refresh") or {})
    stored_qcr = dict((stored.get("continuous") or {}).get("qualified_check_refresh") or {})
    if current_qcr and not stored_qcr:
        legacy_qcr = json.loads(_json(current))
        legacy_continuous = dict(legacy_qcr.get("continuous") or {})
        legacy_continuous.pop("qualified_check_refresh", None)
        legacy_qcr["continuous"] = legacy_continuous
        if (_json(legacy_qcr) == _json(stored)
                or _round_policy_upgrade_compatible(stored, legacy_qcr)):
            return True

    # OPEN1 Scheduler/Evidence semantic upgrades. These change only
    # observational availability/evidence semantics; Actual PHASE_COMPATIBILITY,
    # Simulation POST identity, sim_key, durable URL, budgets and batch sizing
    # remain unchanged. Every allowed transition is exact-pair + full-policy
    # byte-equivalence after reconstructing the stored identity.
    stored_shadow_open1 = str((stored.get("scheduler_shadow") or {}).get("policy_version") or "")
    current_shadow_open1 = str((current.get("scheduler_shadow") or {}).get("policy_version") or "")
    stored_evidence_open1 = str((stored.get("scheduler_evidence") or {}).get("evidence_policy_version") or "")
    current_evidence_open1 = str((current.get("scheduler_evidence") or {}).get("evidence_policy_version") or "")
    stored_open1_identity = (stored_shadow_open1, stored_evidence_open1)
    current_open1_identity = (current_shadow_open1, current_evidence_open1)
    allowed_open1_transitions = {
        (("V31_SCHED_SHADOW_003", "V31_SCHED_EVIDENCE_002"),
         ("V31_SCHED_SHADOW_004", "V31_SCHED_EVIDENCE_003")),
        (("V31_SCHED_SHADOW_003", "V31_SCHED_EVIDENCE_002"),
         ("V31_SCHED_SHADOW_005", "V31_SCHED_EVIDENCE_004")),
        (("V31_SCHED_SHADOW_004", "V31_SCHED_EVIDENCE_003"),
         ("V31_SCHED_SHADOW_005", "V31_SCHED_EVIDENCE_004")),
    }
    if (stored_open1_identity, current_open1_identity) in allowed_open1_transitions:
        legacy_open1 = json.loads(_json(current))
        legacy_open1_shadow = dict(legacy_open1.get("scheduler_shadow") or {})
        legacy_open1_evidence = dict(legacy_open1.get("scheduler_evidence") or {})
        legacy_open1_shadow["policy_version"] = stored_shadow_open1
        legacy_open1_evidence["evidence_policy_version"] = stored_evidence_open1
        legacy_open1["scheduler_shadow"] = legacy_open1_shadow
        legacy_open1["scheduler_evidence"] = legacy_open1_evidence
        if _json(legacy_open1) == _json(stored):
            return True

    # Controlled fixed-pool/Shadow-evidence HOTFIX.  This is intentionally
    # policy-shaped rather than run-id-shaped.  Reconstruct the prior snapshot
    # and require byte-equivalence after removing only the approved changes.
    legacy_hotfix = json.loads(_json(current))
    legacy_continuous = dict(legacy_hotfix.get("continuous") or {})
    expansion_enabled = bool(legacy_continuous.pop("allow_search_pool_expansion", True))
    legacy_hotfix["continuous"] = legacy_continuous
    stored_rolling = dict(stored.get("rolling_discovery") or {})
    legacy_rolling = dict(legacy_hotfix.get("rolling_discovery") or {})
    if not expansion_enabled and bool(stored_rolling.get("enabled", False)):
        legacy_rolling["enabled"] = True
        legacy_hotfix["rolling_discovery"] = legacy_rolling
    stored_shadow = dict(stored.get("scheduler_shadow") or {})
    legacy_shadow = dict(legacy_hotfix.get("scheduler_shadow") or {})
    stored_evidence = dict(stored.get("scheduler_evidence") or {})
    legacy_evidence = dict(legacy_hotfix.get("scheduler_evidence") or {})
    legacy_scheduler_evidence_identity = (
        str(legacy_shadow.get("policy_version") or ""),
        str(legacy_evidence.get("evidence_policy_version") or ""),
    )
    if (
        str(stored_shadow.get("policy_version") or "") == "V31_SCHED_SHADOW_002"
        and str(stored_evidence.get("evidence_policy_version") or "") == "V31_SCHED_EVIDENCE_001"
        and legacy_scheduler_evidence_identity in {
            ("V31_SCHED_SHADOW_003", "V31_SCHED_EVIDENCE_002"),
            ("V31_SCHED_SHADOW_004", "V31_SCHED_EVIDENCE_003"),
            ("V31_SCHED_SHADOW_005", "V31_SCHED_EVIDENCE_004"),
        }
    ):
        legacy_shadow["policy_version"] = stored_shadow["policy_version"]
        legacy_evidence["evidence_policy_version"] = stored_evidence["evidence_policy_version"]
        legacy_hotfix["scheduler_shadow"] = legacy_shadow
        legacy_hotfix["scheduler_evidence"] = legacy_evidence
        if _json(legacy_hotfix) == _json(stored):
            return True

    # V3.0.4f -> V3.0.4g: add local PPC quality policy plus periodic/manual
    # GET-only refresh of the manual-finalization queue. Simulation settings,
    # ranking, budgets, POST/resume semantics and machine_lib remain unchanged.
    if (stored_versions.get("ppl_classification") == "V3_PPL_CLASS_003"
            and current_versions.get("ppl_classification") == "V3_PPL_CLASS_004"):
        legacy304g = json.loads(_json(current))
        manual = dict((legacy304g.get("ppl_classification") or {}).get("manual_finalization") or {})
        manual.pop("auto_refresh_every_batches", None)
        manual.pop("ppc_strategy", None)
        legacy304g["ppl_classification"]["manual_finalization"] = manual
        legacy_versions = dict(legacy304g.get("policy_versions") or {})
        legacy_versions["ppl_classification"] = stored_versions.get("ppl_classification")
        legacy304g["policy_versions"] = legacy_versions
        if _json(legacy304g) == _json(stored):
            return (
                current_versions.get("ranking") == stored_versions.get("ranking")
                and current_versions.get("allocation") == stored_versions.get("allocation")
                and current_versions.get("ppl_classification") == "V3_PPL_CLASS_004"
            )

    # V3.0.4e -> V3.0.4f: PRE-TAG Description warnings are reclassified
    # as a manual-finalization dependency. This changes derived PPL labels and
    # report output only; Simulation settings, ranking, budget, POST and resume
    # semantics remain unchanged.
    if (stored_versions.get("ppl_classification") == "V3_PPL_CLASS_002"
            and current_versions.get("ppl_classification") == "V3_PPL_CLASS_003"):
        legacy304f = json.loads(_json(current))
        legacy304f_pc = dict(legacy304f.get("ppl_classification") or {})
        legacy304f_pc.pop("manual_finalization", None)
        legacy304f["ppl_classification"] = legacy304f_pc
        legacy304f_versions = dict(legacy304f.get("policy_versions") or {})
        legacy304f_versions["ppl_classification"] = stored_versions.get("ppl_classification")
        legacy304f["policy_versions"] = legacy304f_versions
        if _json(legacy304f) == _json(stored):
            return (
                current_versions.get("ranking") == stored_versions.get("ranking")
                and current_versions.get("allocation") == stored_versions.get("allocation")
                and current_versions.get("ppl_classification") == "V3_PPL_CLASS_003"
            )

    # V3_RANK_003 -> V3_RANK_005 is the audited composition of the
    # V3_RANK_003 -> 004 and 004 -> 005 additive upgrades. This preserves
    # resume compatibility for older in-progress rounds without weakening the
    # exact-policy comparison.
    if (stored_versions.get("ranking") == "V3_RANK_003"
            and current_versions.get("ranking") == "V3_RANK_005"):
        intermediate = json.loads(_json(current))
        adaptive = dict(intermediate.get("adaptive_ranking") or {})
        for key in (
            "recent_zero_positive_min_attempts", "recent_zero_positive_penalty",
            "recent_zero_positive_consecutive_batches_cooldown",
            "direction_repair_positive_abs_sharpe_min",
            "direction_repair_strong_abs_sharpe_min",
            "direction_repair_elite_abs_sharpe_min",
        ):
            adaptive.pop(key, None)
        intermediate["adaptive_ranking"] = adaptive
        intermediate_versions = dict(intermediate.get("policy_versions") or {})
        intermediate_versions["ranking"] = "V3_RANK_004"
        intermediate["policy_versions"] = intermediate_versions
        return _round_policy_upgrade_compatible(stored, intermediate)

    # V3.0.4j -> V3_RANK_005: add window-scoped recent SEARCH failure
    # feedback/cooldown plus negative-direction REPAIR priority bands.  These
    # knobs only affect future selection/ranking. Existing Simulation facts,
    # budget accounting, POST/resume semantics and PPL classification remain
    # unchanged, so in-progress rounds may upgrade additively.
    if (stored_versions.get("ranking") == "V3_RANK_004"
            and current_versions.get("ranking") == "V3_RANK_005"):
        legacy_rank005 = json.loads(_json(current))
        adaptive = dict(legacy_rank005.get("adaptive_ranking") or {})
        for key in (
            "recent_zero_positive_min_attempts", "recent_zero_positive_penalty",
            "recent_zero_positive_consecutive_batches_cooldown",
            "direction_repair_positive_abs_sharpe_min",
            "direction_repair_strong_abs_sharpe_min",
            "direction_repair_elite_abs_sharpe_min",
        ):
            adaptive.pop(key, None)
        legacy_rank005["adaptive_ranking"] = adaptive
        legacy_versions = dict(legacy_rank005.get("policy_versions") or {})
        legacy_versions["ranking"] = stored_versions.get("ranking")
        legacy_rank005["policy_versions"] = legacy_versions
        if _json(legacy_rank005) == _json(stored):
            return True

    # V3.0.4g/h -> V3_RANK_004: split SEARCH evidence from REPAIR evidence.
    # Existing paid Simulation facts are re-derived under the new thresholds;
    # no historical fact, budget, POST/resume or PPL-classification semantics
    # are rewritten. Restore the V3_RANK_003 knobs/weights for exact policy
    # compatibility with an in-progress round such as run_0005.
    if (stored_versions.get("ranking") == "V3_RANK_003"
            and current_versions.get("ranking") == "V3_RANK_004"):
        legacy_rank004 = json.loads(_json(current))
        adaptive = dict(legacy_rank004.get("adaptive_ranking") or {})
        for key in (
            "search_positive_sharpe_min", "search_strong_sharpe_min",
            "search_elite_sharpe_min", "repair_good_sharpe_min",
            "repair_elite_sharpe_min", "repair_turnover_near_max",
            "repair_turnover_mid_max",
        ):
            adaptive.pop(key, None)
        stage_weights = dict(adaptive.get("stage_weights") or {})
        for key in ("search_viable", "search_strong", "search_elite", "repair_viable", "repair_elite"):
            stage_weights.pop(key, None)
        stage_weights["signal_viable"] = 8.0
        stage_weights["local_pass"] = 22.0
        stage_weights["fixed_repairable"] = 4.0
        adaptive["stage_weights"] = stage_weights
        legacy_rank004["adaptive_ranking"] = adaptive
        legacy_versions = dict(legacy_rank004.get("policy_versions") or {})
        legacy_versions["ranking"] = stored_versions.get("ranking")
        legacy_rank004["policy_versions"] = legacy_versions
        if _json(legacy_rank004) == _json(stored):
            return True

    # V3.0.4b -> V3.0.4c: failure-aware future SEARCH allocation.
    # This changes only how *future* candidates are ranked/capped and how weak
    # Datasets may cool down. Historical Simulation facts, budget accounting,
    # batch size, 70/30 target, cache/resume semantics and POST guards remain
    # unchanged.  Strip the newly-added knobs and restore version labels for an
    # exact compatibility comparison with the stored V3.0.4b round policy.
    if (stored_versions.get("ranking") == "V3_RANK_002"
            and current_versions.get("ranking") == "V3_RANK_003"
            and stored_versions.get("ppl_classification") == "V3_PPL_CLASS_002"
            and current_versions.get("ppl_classification") == "V3_PPL_CLASS_002"):
        legacy304c = json.loads(_json(current))
        adaptive = dict(legacy304c.get("adaptive_ranking") or {})
        for key in (
            "terminal_fail_penalty", "zero_viable_penalty", "zero_viable_min_attempts",
            "exploit_min_combo_attempts", "exploit_min_combo_viable",
            "max_unproven_dataset_fraction", "max_unproven_combo_per_batch",
        ):
            adaptive.pop(key, None)
        stage_weights = dict(adaptive.get("stage_weights") or {})
        stage_weights.pop("signal_viable", None)
        stage_weights.pop("fixed_repairable", None)
        adaptive["stage_weights"] = stage_weights
        legacy304c["adaptive_ranking"] = adaptive
        rolling = dict(legacy304c.get("rolling_discovery") or {})
        for key in ("cooldown_viable_rate_max", "cooldown_without_admission", "max_cooldown_per_refresh"):
            rolling.pop(key, None)
        legacy304c["rolling_discovery"] = rolling
        legacy_versions = dict(legacy304c.get("policy_versions") or {})
        for key in ("ranking", "dataset_discovery", "telemetry"):
            if key in stored_versions:
                legacy_versions[key] = stored_versions[key]
        legacy304c["policy_versions"] = legacy_versions
        if _json(legacy304c) == _json(stored):
            return (
                current_versions.get("dataset_discovery") == "V3_DATASET_002"
                and current_versions.get("telemetry") == "V3_TELEMETRY_003"
            )

    # V3.0.4(a) -> V3.0.4b: official GLB Liquid theme simplification.
    # Only the derived PPL theme-specific classification block and its policy
    # version labels may change here; budgets/ranking/discovery/POST semantics
    # must remain byte-equivalent.
    if (stored_versions.get("ppl_classification") == "V3_PPL_CLASS_001"
            and current_versions.get("ppl_classification") == "V3_PPL_CLASS_002"):
        legacy304b = json.loads(_json(current))
        legacy304b["ppl_classification"]["theme_specific"] = json.loads(
            _json((stored.get("ppl_classification") or {}).get("theme_specific") or {})
        )
        legacy304b_versions = dict(legacy304b.get("policy_versions") or {})
        legacy304b_versions["ppl_classification"] = stored_versions.get("ppl_classification")
        legacy304b_versions["repair"] = stored_versions.get("repair")
        legacy304b["policy_versions"] = legacy304b_versions
        if _json(legacy304b) == _json(stored):
            return (
                current_versions.get("repair") == "V3_REPAIR_003"
                and current_versions.get("ppl_classification") == "V3_PPL_CLASS_002"
            )

    # V3.0.3(a) -> V3.0.4: classification policy is additive and changes
    # derived research labels only. It does not alter budget, batching, POST
    # semantics, discovery, cache/resume, or the V2.1 execution contract.
    if "ppl_classification" not in stored:
        legacy304 = dict(current)
        legacy304.pop("ppl_classification", None)
        legacy_versions = dict(legacy304.get("policy_versions") or {})
        legacy_versions.pop("ppl_classification", None)
        for key in ("ranking", "allocation", "repair", "telemetry", "dataset_discovery", "winner", "family"):
            if key in stored_versions:
                legacy_versions[key] = stored_versions[key]
        legacy304["policy_versions"] = legacy_versions
        if _json(legacy304) == _json(stored):
            return (
                current_versions.get("ppl_classification") == "V3_PPL_CLASS_001"
                and current_versions.get("repair") == "V3_REPAIR_002"
                and current_versions.get("telemetry") in {"V3_TELEMETRY_002", "V3_TELEMETRY_003"}
            )

    # Legacy V3.0.1 -> V3.0.2 compatibility.
    if "rolling_discovery" not in stored:
        legacy = dict(current)
        legacy.pop("rolling_discovery", None)
        legacy.pop("adaptive_ranking", None)
        if "ppl_classification" not in stored:
            legacy.pop("ppl_classification", None)
        legacy_versions = dict(legacy.get("policy_versions") or {})
        legacy_versions.pop("dataset_discovery", None)
        if "ppl_classification" not in stored_versions:
            legacy_versions.pop("ppl_classification", None)
        for key in ("allocation", "ranking", "repair", "telemetry"):
            if key in stored_versions:
                legacy_versions[key] = stored_versions[key]
        legacy["policy_versions"] = legacy_versions
        return (
            _json(legacy) == _json(stored)
            and current_versions.get("dataset_discovery") in {"V3_DATASET_001", "V3_DATASET_002"}
            and current_versions.get("ranking") in {"V3_RANK_001", "V3_RANK_002", "V3_RANK_003", "V3_RANK_004", "V3_RANK_005"}
        )

    # V3.0.2 -> V3.0.3: strip only the newly-added adaptive block and restore
    # the old version labels before exact comparison.
    legacy = dict(current)
    legacy.pop("adaptive_ranking", None)
    if "rolling_discovery" in stored:
        legacy["rolling_discovery"] = json.loads(_json(stored.get("rolling_discovery") or {}))
    if "ppl_classification" not in stored:
        legacy.pop("ppl_classification", None)
    legacy_versions = dict(legacy.get("policy_versions") or {})
    if "ppl_classification" not in stored_versions:
        legacy_versions.pop("ppl_classification", None)
    for key in ("ranking", "allocation", "repair", "telemetry", "dataset_discovery"):
        if key in stored_versions:
            legacy_versions[key] = stored_versions[key]
    legacy["policy_versions"] = legacy_versions
    return (
        _json(legacy) == _json(stored)
        and current_versions.get("ranking") in {"V3_RANK_002", "V3_RANK_003", "V3_RANK_004", "V3_RANK_005"}
        and current_versions.get("allocation") == "V3_ALLOC_003"
    )


def _migrate_round_policy_if_allowed(store: Any, round_id: str, run_id: str,
                                     stored: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    if hashlib.sha256(_json(stored).encode()).hexdigest() == hashlib.sha256(_json(current).encode()).hexdigest():
        return False
    if not _round_policy_upgrade_compatible(stored, current):
        raise ConfigError("ROUND_POLICY_DRIFT")
    from_ranking = (stored.get("policy_versions") or {}).get("ranking")
    to_ranking = (current.get("policy_versions") or {}).get("ranking")
    if ((stored.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_003"
            and (current.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_004"):
        change = "MANUAL_QUEUE_REFRESH_AND_PPC_STRATEGY"
    elif ((stored.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_002"
            and (current.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_003"):
        change = "PRETAG_DESCRIPTION_MANUAL_FINALIZATION"
    elif (from_ranking == "V3_RANK_002" and to_ranking == "V3_RANK_003"):
        change = "FAILURE_AWARE_SEARCH_RANKING_AND_COOLDOWN"
    elif ((stored.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_001"
            and (current.get("policy_versions") or {}).get("ppl_classification") == "V3_PPL_CLASS_002"):
        change = "OFFICIAL_GLB_LIQUID_THEME_RELAXATION"
    elif "ppl_classification" not in stored and "ppl_classification" in current:
        change = "PLATFORM_DRIVEN_PPL_CLASSIFICATION_ADDITIVE"
    elif "rolling_discovery" not in stored:
        change = "ROLLING_DATASET_DISCOVERY_ADDITIVE"
    elif (
        ((stored.get("scheduler_shadow") or {}).get("policy_version"),
         (stored.get("scheduler_evidence") or {}).get("evidence_policy_version"))
        in {
            ("V31_SCHED_SHADOW_003", "V31_SCHED_EVIDENCE_002"),
            ("V31_SCHED_SHADOW_004", "V31_SCHED_EVIDENCE_003"),
        }
        and (current.get("scheduler_shadow") or {}).get("policy_version") == "V31_SCHED_SHADOW_005"
        and (current.get("scheduler_evidence") or {}).get("evidence_policy_version") == "V31_SCHED_EVIDENCE_004"
    ):
        change = "OPEN1_SHARED_REPAIR_ELIGIBILITY_CORE"
    elif (
        (stored.get("scheduler_shadow") or {}).get("policy_version") == "V31_SCHED_SHADOW_002"
        and (stored.get("scheduler_evidence") or {}).get("evidence_policy_version") == "V31_SCHED_EVIDENCE_001"
        and (
            (
                (current.get("scheduler_shadow") or {}).get("policy_version") == "V31_SCHED_SHADOW_003"
                and (current.get("scheduler_evidence") or {}).get("evidence_policy_version") == "V31_SCHED_EVIDENCE_002"
            )
            or (
                (current.get("scheduler_shadow") or {}).get("policy_version") == "V31_SCHED_SHADOW_004"
                and (current.get("scheduler_evidence") or {}).get("evidence_policy_version") == "V31_SCHED_EVIDENCE_003"
            )
            or (
                (current.get("scheduler_shadow") or {}).get("policy_version") == "V31_SCHED_SHADOW_005"
                and (current.get("scheduler_evidence") or {}).get("evidence_policy_version") == "V31_SCHED_EVIDENCE_004"
            )
        )
    ):
        change = "FIXED_SEARCH_POOL_AND_SHADOW_EVIDENCE_UPGRADE"
    elif (
        not (stored.get("continuous") or {}).get("qualified_check_refresh")
        and (current.get("continuous") or {}).get("qualified_check_refresh")
    ):
        change = "QUALIFIED_CHECK_REFRESH_RETRY_FLOOR"
    else:
        change = "ONLINE_EVIDENCE_RANKING_ADDITIVE"
    update_round(
        store, round_id,
        config_json=_json(dict(current)),
        config_hash=hashlib.sha256(_json(dict(current)).encode()).hexdigest(),
    )
    record_event(
        store, round_id, run_id, "ROUND_POLICY_UPGRADED", phase="SEARCH",
        payload={
            "from_ranking": from_ranking, "to_ranking": to_ranking, "change": change,
            "allocation_policy": (current.get("policy_versions") or {}).get("allocation"),
            "dataset_discovery_policy": (current.get("policy_versions") or {}).get("dataset_discovery"),
            "scheduler_shadow_policy": (current.get("scheduler_shadow") or {}).get("policy_version"),
            "scheduler_evidence_policy": (current.get("scheduler_evidence") or {}).get("evidence_policy_version"),
            "allow_search_pool_expansion": (current.get("continuous") or {}).get("allow_search_pool_expansion"),
            "qualified_check_refresh": (current.get("continuous") or {}).get("qualified_check_refresh"),
        },
        source_event_key=(
            f"round_policy_upgrade:{round_id}:{to_ranking}:"
            f"{(current.get('policy_versions') or {}).get('allocation')}:"
            f"{(current.get('policy_versions') or {}).get('ppl_classification')}"
            f":{(current.get('scheduler_shadow') or {}).get('policy_version')}"
            f":{(current.get('scheduler_evidence') or {}).get('evidence_policy_version')}"
            f":{(current.get('continuous') or {}).get('allow_search_pool_expansion')}"
            f":{_json((current.get('continuous') or {}).get('qualified_check_refresh') or {})}"
        ),
    )
    return True

def _integrity(path: Path) -> str:
    uri = f"file:{Path(path).resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        return str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        conn.close()


def _next_run_id(store: Any) -> str:
    with store.connect() as conn:
        nums = []
        for row in conn.execute("SELECT run_id FROM ppl_runs"):
            value = str(row[0])
            if value.startswith("run_") and value[4:].isdigit():
                nums.append(int(value[4:]))
    return f"run_{max(nums, default=0) + 1:04d}"


def _signal_family_to_candidate(signal_family: str) -> Optional[Dict[str, str]]:
    parts = str(signal_family or "").split("/")
    if len(parts) != 5:
        return None
    return dict(zip(("dataset_id", "field_id", "vector_reducer", "direction", "transform_family"), parts))


def _family_id_from_signal(signal_family: str) -> Optional[str]:
    c = _signal_family_to_candidate(signal_family)
    return family_id(c) if c else None


def _protected_families(store: Any, external_evidence_path: Path, *, current_run_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """Build a policy-level protected family set without rewriting system facts."""
    protected: Dict[str, Dict[str, Any]] = {}
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT c.*,r.run_profile FROM ppl_candidates c
               JOIN ppl_runs r ON r.run_id=c.run_id
               WHERE r.run_profile='PRODUCTION_RESEARCH'"""
        ).fetchall()
    for row in rows:
        item = dict(row)
        if current_run_id and item.get("run_id") == current_run_id:
            continue
        state = str(item.get("lifecycle_state") or "")
        if state not in SUCCESS_STATES:
            continue
        fid = family_id(item)
        protected[fid] = {
            "family_id": fid, "signal_family": item.get("signal_family"),
            "candidate_id": item.get("candidate_id"), "alpha_id": item.get("alpha_id"),
            "winner_state": state, "source": "HISTORICAL_PRODUCTION_DB",
        }
    for evidence in load_external_evidence(external_evidence_path):
        submitted = str(evidence.get("submission_status") or "").upper()
        verdict = str(evidence.get("verdict") or "").upper()
        if submitted != "USER_CONFIRMED_SUBMITTED" and verdict != "TARGET_PASS":
            continue
        signal_family = evidence.get("parent_signal_family")
        fid = _family_id_from_signal(str(signal_family or ""))
        if not fid:
            continue
        protected[fid] = {
            "family_id": fid, "signal_family": signal_family,
            "candidate_id": None,
            "alpha_id": evidence.get("child_alpha_id") or evidence.get("alpha_id"),
            "winner_state": "USER_CONFIRMED_SUBMITTED",
            "source": "USER_CONFIRMED_EVIDENCE",
        }
    return protected


def _seed_protected_winners(store: Any, round_id: str, external_evidence_path: Path, run_id: str) -> int:
    protected = _protected_families(store, external_evidence_path, current_run_id=run_id)
    for fid, item in protected.items():
        upsert_winner(
            store, round_id, family_id=fid, signal_family=str(item.get("signal_family") or ""),
            candidate_id=item.get("candidate_id"), alpha_id=item.get("alpha_id"),
            winner_state=str(item.get("winner_state") or "PROTECTED"), source=str(item.get("source") or "PROTECTED"),
            score={"historical_protection": True}, protected=True,
        )
        record_event(
            store, round_id, run_id, "FAMILY_PROTECTED_AT_ROUND_START", candidate_id=item.get("candidate_id"),
            alpha_id=item.get("alpha_id"), family_id_value=fid,
            payload={"signal_family": item.get("signal_family"), "winner_state": item.get("winner_state"),
                     "source": item.get("source")},
            source_event_key=f"protected_seed:{round_id}:{fid}",
        )
    update_round(store, round_id, protected_family_count=len(protected), winner_count=len(protected))
    return len(protected)



def protect_submitted_alpha(store: Any, *, run_id: str, alpha_id: str) -> Dict[str, Any]:
    """Locally protect a user-confirmed submitted Alpha and its whole signal family.

    This is intentionally local-only: it performs no network request and does not
    rewrite the candidate's historical lifecycle/check classification. The repair
    selector already skips every family stored as protected in
    ppl_round_family_winners. Repeating the same command is idempotent.
    """
    store.initialize()
    ensure_round_schema(store)
    row = get_round(store, run_id=run_id)
    if not row:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    alpha_id = str(alpha_id or "").strip()
    if not alpha_id:
        raise ConfigError("PROTECT_ALPHA_REQUIRES_ALPHA_ID")
    matches = [c for c in store.load_candidates(run_id) if str(c.get("alpha_id") or "") == alpha_id]
    if not matches:
        raise ConfigError(f"ALPHA_NOT_FOUND_IN_RUN:{alpha_id}")
    if len(matches) > 1:
        raise ConfigError(f"ALPHA_ID_NOT_UNIQUE_IN_RUN:{alpha_id}:{len(matches)}")
    candidate = matches[0]
    fid = family_id(candidate)
    signal = str(candidate.get("signal_family") or "")
    round_id = str(row["round_id"])
    upsert_winner(
        store, round_id, family_id=fid, signal_family=signal,
        candidate_id=str(candidate.get("candidate_id") or "") or None,
        alpha_id=alpha_id, winner_state="USER_CONFIRMED_SUBMITTED",
        source="USER_MANUAL_CONFIRMATION",
        score={"manual_protection": True, "submission_confirmed_by_user": True},
        protected=True,
    )
    record_event(
        store, round_id, run_id, "USER_CONFIRMED_ALPHA_PROTECTED",
        candidate_id=str(candidate.get("candidate_id") or "") or None,
        alpha_id=alpha_id, family_id_value=fid,
        payload={"signal_family": signal, "winner_state": "USER_CONFIRMED_SUBMITTED",
                 "source": "USER_MANUAL_CONFIRMATION"},
        source_event_key=f"manual_protect:{round_id}:{fid}:{alpha_id}",
    )
    winners = load_winners(store, round_id)
    protected_count = sum(1 for w in winners if int(w.get("protected") or 0))
    update_round(store, round_id, protected_family_count=protected_count, winner_count=len(winners))
    return {
        "project_version": "v3.0.4o",
        "action": "PROTECT_ALPHA",
        "run_id": run_id,
        "round_id": round_id,
        "alpha_id": alpha_id,
        "candidate_id": candidate.get("candidate_id"),
        "family_id": fid,
        "signal_family": signal,
        "winner_state": "USER_CONFIRMED_SUBMITTED",
        "protected": True,
        "protected_family_count": protected_count,
        "repair_effect": "THIS_FAMILY_WILL_BE_SKIPPED_BY_REPAIR_SELECTION",
        "side_effects": {
            "network_requests": 0, "simulation_posts": 0, "check_requests": 0,
            "submit_requests": 0, "power_pool_selected_requests": 0,
            "local_db_writes": "protected winner + idempotent audit event only",
        },
    }

def _check_budget_used(store: Any, run_id: str) -> Tuple[int, int]:
    with store.connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*),coalesce(SUM(http_request_count),0) FROM ppl_check_sessions WHERE run_id=?",
            (run_id,),
        ).fetchone()
    return int(row[0]), int(row[1])


def _latest_check_metrics(store: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    names = {
        "HIGH_TURNOVER_RETURNS_RATIO", "POWER_POOL_CORRELATION", "PROD_CORRELATION",
        "SUB_UNIVERSE", "LOW_SUB_UNIVERSE_SHARPE", "TWO_YEAR_SHARPE", "LOW_2Y_SHARPE",
    }
    rows = _latest_check_rows(store, run_id, candidate_id)
    return {name: dict(payload) for name, payload in rows.items() if name in names}


def _winner_key(metrics: Mapping[str, Any], checks: Mapping[str, Any], expression: str) -> Tuple[Any, ...]:
    """Qualification is handled before this key; prefer quality, safety, simplicity."""
    fitness = float(metrics.get("fitness") or -1e9)
    sharpe = float(metrics.get("sharpe") or -1e9)
    ht = checks.get("HIGH_TURNOVER_RETURNS_RATIO") or {}
    ht_margin = -1e9
    if ht.get("value") is not None and ht.get("limit") not in (None, 0):
        ht_margin = (float(ht["value"]) - float(ht["limit"])) / max(abs(float(ht["limit"])), 1e-12)
    pp = checks.get("POWER_POOL_CORRELATION") or {}
    prod = checks.get("PROD_CORRELATION") or {}
    pp_safety = -float(pp.get("value")) if pp.get("value") is not None else -1e9
    prod_safety = -float(prod.get("value")) if prod.get("value") is not None else -1e9
    turnover = float(metrics.get("turnover") or 999.0)
    turnover_safety = -abs(turnover - 0.45)
    simplicity = -len(str(expression or ""))
    return (fitness, sharpe, ht_margin, pp_safety, prod_safety, turnover_safety, simplicity)


def _stop_family_siblings(store: Any, run_id: str, signal_family: str, winner_id: str) -> int:
    rows = [x for x in store.load_candidates(run_id) if x.get("signal_family") == signal_family and x["candidate_id"] != winner_id]
    stopped = 0
    for row in rows:
        state = str(row.get("lifecycle_state") or "")
        if "STOPPED" in CANDIDATE_TRANSITIONS.get(state, set()):
            try:
                store.transition_candidate(
                    row["candidate_id"], "STOPPED", reason="V3 family already has protected winner",
                    source=ROUND_SOURCE, allowed=CANDIDATE_TRANSITIONS,
                    metadata={"winner_candidate_id": winner_id, "signal_family": signal_family},
                )
                stopped += 1
            except ValueError:
                pass
    return stopped


def finalize_family_winners(store: Any, alpha_db: Path, run_id: str, round_id: str, *, config: Any = None) -> Dict[str, Any]:
    rows = store.load_candidates(run_id)
    platform_class = ({str(x.get("candidate_id")): x for x in classify_run(store, config, alpha_db, run_id)}
                      if config is not None else {})
    if config is None:
        # Backward-compatible helper behavior for legacy unit tests/tools.
        success = [x for x in rows if str(x.get("lifecycle_state") or "") in SUCCESS_STATES]
    else:
        # V3.0.4c current-round protection is driven by the platform-derived
        # classifier, not the legacy PRE_TAG_CHECK_PASS gate semantics.
        success = [
            x for x in rows
            if (platform_class.get(str(x.get("candidate_id"))) or {}).get("classification")
            == "PPL_TECHNICALLY_READY"
        ]
    by_family: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in success:
        by_family[str(row.get("signal_family") or family_id(row))].append(row)
    facts = _alpha_facts(alpha_db, [x.get("sim_key") for x in success if x.get("sim_key")])
    winners = []
    for signal_family, items in by_family.items():
        scored = []
        for item in items:
            metrics = facts.get(str(item.get("sim_key"))) or {}
            checks = _latest_check_metrics(store, run_id, item["candidate_id"])
            scored.append((_winner_key(metrics, checks, item.get("expression") or ""), item, metrics, checks))
        scored.sort(key=lambda x: x[0], reverse=True)
        _, winner, metrics, checks = scored[0]
        state = str(winner.get("lifecycle_state") or "")
        ppl_cls = platform_class.get(str(winner.get("candidate_id"))) or {}
        if state == "PRE_TAG_CHECK_COMPLETE" and ppl_cls.get("classification") == "PPL_TECHNICALLY_READY":
            store.transition_candidate(winner["candidate_id"], "PRE_TAG_CHECK_PASS",
                                       reason="V3.0.4c platform-driven PPL technically ready",
                                       source=ROUND_SOURCE, allowed=CANDIDATE_TRANSITIONS)
            state = "PRE_TAG_CHECK_PASS"
        if state == "PRE_TAG_CHECK_PASS":
            store.transition_candidate(winner["candidate_id"], "FAMILY_DEDUP", reason="V3 family winner selected", source=ROUND_SOURCE, allowed=CANDIDATE_TRANSITIONS)
            state = "FAMILY_DEDUP"
        if state == "FAMILY_DEDUP":
            store.transition_candidate(winner["candidate_id"], "PRE_TAG_FINALIST", reason="V3 protected family finalist", source=ROUND_SOURCE, allowed=CANDIDATE_TRANSITIONS)
            state = "PRE_TAG_FINALIST"
        fid = family_id(winner)
        score = {
            "fitness": metrics.get("fitness"), "sharpe": metrics.get("sharpe"),
            "turnover": metrics.get("turnover"), "checks": checks,
            "ppl_classification": ppl_cls.get("classification"),
            "classifier_version": ppl_cls.get("classifier_version"),
            "winner_key": list(_winner_key(metrics, checks, winner.get("expression") or "")),
        }
        upsert_winner(
            store, round_id, family_id=fid, signal_family=signal_family,
            candidate_id=winner["candidate_id"], alpha_id=winner.get("alpha_id"),
            winner_state=state, source="V3_LIVE_PRETAG", score=score, protected=True,
        )
        record_event(
            store, round_id, run_id, "FAMILY_WINNER_PROTECTED", candidate_id=winner["candidate_id"],
            alpha_id=winner.get("alpha_id"), family_id_value=fid, sim_key=winner.get("sim_key"),
            payload={"signal_family": signal_family, "winner_state": state, "score": score},
            source_event_key=f"family_winner:{round_id}:{fid}:{winner['candidate_id']}:{state}",
        )
        siblings_stopped = _stop_family_siblings(store, run_id, signal_family, winner["candidate_id"])
        winners.append({"family_id": fid, "candidate_id": winner["candidate_id"], "alpha_id": winner.get("alpha_id"),
                        "signal_family": signal_family, "siblings_stopped": siblings_stopped, **score})
    all_winners = load_winners(store, round_id)
    update_round(store, round_id, protected_family_count=len(all_winners), winner_count=len(all_winners))
    return {"new_or_current_winners": winners, "protected_total": len(all_winners)}


def _empirical_combo_stats(store: Any, run_id: str) -> Dict[Tuple[str, str], Dict[str, int]]:
    rows = store.load_candidates(run_id)
    checked = set()
    passed = set()
    with store.connect() as conn:
        checked = {str(r[0]) for r in conn.execute("SELECT DISTINCT candidate_id FROM ppl_check_sessions WHERE run_id=?", (run_id,)) if r[0]}
    for row in rows:
        if str(row.get("lifecycle_state") or "") in SUCCESS_STATES:
            passed.add(str(row["candidate_id"]))
    stats: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"attempts": 0, "checked": 0, "success": 0})
    for row in rows:
        if str(row.get("simulation_status") or "").upper() != "COMPLETE":
            continue
        key = (str(row.get("dataset_id") or ""), str(row.get("operator") or ""))
        stats[key]["attempts"] += 1
        if row["candidate_id"] in checked:
            stats[key]["checked"] += 1
        if row["candidate_id"] in passed:
            stats[key]["success"] += 1
    return stats


def _empirical_dimension_stats(store: Any, run_id: str, key_name: str) -> Dict[str, Dict[str, int]]:
    rows = store.load_candidates(run_id)
    checked = set()
    passed = set()
    with store.connect() as conn:
        checked = {str(r[0]) for r in conn.execute(
            "SELECT DISTINCT candidate_id FROM ppl_check_sessions WHERE run_id=?", (run_id,)
        ) if r[0]}
    for row in rows:
        if str(row.get("lifecycle_state") or "") in SUCCESS_STATES:
            passed.add(str(row["candidate_id"]))
    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"attempts": 0, "checked": 0, "success": 0})
    for row in rows:
        if str(row.get("simulation_status") or "").upper() != "COMPLETE":
            continue
        key = str(row.get(key_name) or "UNKNOWN")
        stats[key]["attempts"] += 1
        if str(row["candidate_id"]) in checked:
            stats[key]["checked"] += 1
        if str(row["candidate_id"]) in passed:
            stats[key]["success"] += 1
    return stats


def _smoothed_dimension_score(stat: Mapping[str, int]) -> float:
    attempts = int(stat.get("attempts", 0))
    checked = int(stat.get("checked", 0))
    success = int(stat.get("success", 0))
    return round(14.0 * ((success + 0.5) / (attempts + 2.0)) + 3.0 * ((checked + 1.0) / (attempts + 2.0)), 6)


def _window_key(value: Any) -> str:
    if value is None or str(value).strip() in {"", "None", "nan"}:
        return "NONE"
    try:
        f = float(value)
        return str(int(f)) if f.is_integer() else str(f)
    except (TypeError, ValueError):
        return str(value)


def _new_evidence_stat() -> Dict[str, int]:
    return {
        "attempts": 0,
        "signal_viable": 0,
        "local_pass": 0,
        "fixed_repairable": 0,
        "search_viable": 0,
        "search_strong": 0,
        "search_elite": 0,
        "repair_viable": 0,
        "repair_elite": 0,
        "terminal_fail": 0,
        "pretag_resolved": 0,
        "ppl_near_pass": 0,
        "ppl_strong_near_pass": 0,
        "ppl_success": 0,
    }


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip().lower() in {"", "none", "nan", "null"}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _current_rule_search_outcome(row: Mapping[str, Any], candidate: Mapping[str, Any],
                                 policy: Mapping[str, Any]) -> Dict[str, bool]:
    """Re-evaluate paid SEARCH facts under the current PPL fixed-gate policy.

    Historical ledger ``local_gate`` values are intentionally not reused here:
    they may have been produced under an older Theme (for example the retired
    High-Turnover 20% floor).  Simulation facts themselves are immutable, while
    this derived label is allowed to follow the current round policy.
    """
    fixed = dict((policy.get("ppl_classification") or {}).get("fixed_gates") or {})
    sharpe_min = float(fixed.get("sharpe_min", 1.0))
    turnover_min = float(fixed.get("turnover_min", 0.01))
    turnover_max = float(fixed.get("turnover_max", 0.70))
    operator_max = int(fixed.get("operator_count_max", 8))
    field_max = int(fixed.get("data_field_count_max", 3))
    sharpe = _float_or_none(row.get("sharpe"))
    turnover = _float_or_none(row.get("turnover"))
    structure_ok = str(candidate.get("structure_status") or "ELIGIBLE").upper() != "INVALID"
    operator_ok = int(candidate.get("pp_total_operator_count_estimate") or 0) <= operator_max
    field_ok = int(candidate.get("data_field_count_estimate") or 0) <= field_max
    search_ranking = effective_search_ranking(policy)
    repair_ranking = effective_repair_ranking(policy)
    search_positive_min = float(search_ranking.get("search_positive_sharpe_min", 1.60))
    search_strong_min = float(search_ranking.get("search_strong_sharpe_min", 2.00))
    search_elite_min = float(search_ranking.get("search_elite_sharpe_min", 3.00))
    repair_good_min = float(repair_ranking.get("repair_good_sharpe_min", 2.00))
    repair_elite_min = float(repair_ranking.get("repair_elite_sharpe_min", 3.00))

    signal_viable = bool(sharpe is not None and sharpe >= sharpe_min and structure_ok and operator_ok and field_ok)
    turnover_base_pass = bool(turnover is not None and turnover_min <= turnover <= turnover_max)
    local_pass = bool(signal_viable and turnover_base_pass)
    fixed_repairable = bool(signal_viable and turnover is not None and not turnover_base_pass)

    # SEARCH evidence is intentionally stricter than PPL eligibility. A valid
    # 1.0-1.59 Alpha may still proceed to PRE-TAG/manual finalization, but it
    # does not earn future EXPLOIT budget. High-turnover signals never become
    # SEARCH-positive merely because their Sharpe is high.
    search_viable = bool(local_pass and sharpe is not None and sharpe >= search_positive_min)
    search_strong = bool(local_pass and sharpe is not None and sharpe >= search_strong_min)
    search_elite = bool(local_pass and sharpe is not None and sharpe >= search_elite_min)

    # REPAIR evidence is separate. Only high-turnover (> fixed max) signals
    # with Sharpe >=2 earn positive repair evidence; Sharpe >=3 is elite.
    # This never contributes to SEARCH exploit eligibility.
    high_turnover = bool(turnover is not None and turnover > turnover_max)
    repair_viable = bool(signal_viable and high_turnover and sharpe is not None and sharpe >= repair_good_min)
    repair_elite = bool(signal_viable and high_turnover and sharpe is not None and sharpe >= repair_elite_min)
    terminal_fail = bool(sharpe is not None and sharpe < sharpe_min)
    return {
        "signal_viable": signal_viable,
        "local_pass": local_pass,
        "fixed_repairable": fixed_repairable,
        "search_viable": search_viable,
        "search_strong": search_strong,
        "search_elite": search_elite,
        "repair_viable": repair_viable,
        "repair_elite": repair_elite,
        "terminal_fail": terminal_fail,
    }


def _round_research_evidence(store: Any, run_id: str, round_id: str,
                             policy: Optional[Mapping[str, Any]] = None) -> Dict[str, Dict[Any, Dict[str, int]]]:
    """Aggregate current-round paid SEARCH evidence under the current policy.

    Only logical NEW_POST + COMPLETE rows count.  Historical/cache facts remain
    static priors.  Crucially, ``local_pass`` is re-derived from immutable
    Simulation metrics using the *current* PPL fixed gates, so an old Theme's
    turnover/HT rule cannot poison future ranking after a Theme change.
    """
    policy = dict(policy or {})
    candidates = {str(c.get("candidate_id")): dict(c) for c in store.load_candidates(run_id)}
    winner_ids = {str(w.get("candidate_id")) for w in load_winners(store, round_id) if w.get("candidate_id")}
    dims: Dict[str, Dict[Any, Dict[str, int]]] = {
        "dataset": defaultdict(_new_evidence_stat),
        "dataset_field": defaultdict(_new_evidence_stat),
        "operator": defaultdict(_new_evidence_stat),
        "dataset_operator": defaultdict(_new_evidence_stat),
        "operator_window": defaultdict(_new_evidence_stat),
        "dataset_operator_window": defaultdict(_new_evidence_stat),
        # Batch-scoped evidence is used only for recent-failure feedback.
        # It is deliberately excluded from the weighted online score dimensions.
        "dataset_operator_window_batch": defaultdict(_new_evidence_stat),
    }
    skipped_invalid_identity = 0
    for item in load_ledger(store, round_id):
        row = dict(item)
        if str(row.get("run_id") or "") != str(run_id):
            continue
        if str(row.get("phase") or "SEARCH").upper() != "SEARCH":
            continue
        if str(row.get("origin") or "").upper() != "NEW_POST":
            continue
        if str(row.get("simulation_status") or "").upper() != "COMPLETE":
            continue
        cid = str(row.get("candidate_id") or "")
        cand = candidates.get(cid, {})
        dataset = str(row.get("dataset_id") or cand.get("dataset_id") or "").strip()
        field_id = str(cand.get("field_id") or "").strip()
        if not dataset or not field_id:
            skipped_invalid_identity += 1
            continue
        operator = str(row.get("operator") or cand.get("operator") or "UNKNOWN")
        window = _window_key(cand.get("window"))
        classification = str(row.get("classification") or "").upper()
        derived = _current_rule_search_outcome(row, cand, policy)
        pretag = str(row.get("pretag_status") or "").upper() == "RESOLVED"
        success = (classification == "PPL_SUCCESS" or cid in winner_ids
                   or str(cand.get("lifecycle_state") or "") in SUCCESS_STATES)
        batch_no = int(row.get("batch_no") or 0)
        keys = {
            "dataset": dataset,
            "dataset_field": (dataset, field_id),
            "operator": operator,
            "dataset_operator": (dataset, operator),
            "operator_window": (operator, window),
            "dataset_operator_window": (dataset, operator, window),
            "dataset_operator_window_batch": (dataset, operator, window, batch_no),
        }
        for dim, key in keys.items():
            stat = dims[dim][key]
            stat["attempts"] += 1
            stat["signal_viable"] += int(derived["signal_viable"])
            stat["local_pass"] += int(derived["local_pass"])
            stat["fixed_repairable"] += int(derived["fixed_repairable"])
            stat["search_viable"] += int(derived["search_viable"])
            stat["search_strong"] += int(derived["search_strong"])
            stat["search_elite"] += int(derived["search_elite"])
            stat["repair_viable"] += int(derived["repair_viable"])
            stat["repair_elite"] += int(derived["repair_elite"])
            stat["terminal_fail"] += int(derived["terminal_fail"])
            stat["pretag_resolved"] += int(pretag)
            stat["ppl_near_pass"] += int(classification in {"NEAR_PASS", "STRONG_NEAR_PASS"})
            stat["ppl_strong_near_pass"] += int(classification == "STRONG_NEAR_PASS")
            stat["ppl_success"] += int(success)
    dims["diagnostics"] = {"SOURCE_IDENTITY": {"skipped_invalid_identity": skipped_invalid_identity}}
    return dims


def _load_json_mapping(value: Any, *, error: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ConfigError(error) from exc
    if not isinstance(parsed, dict):
        raise ConfigError(error)
    return dict(parsed)


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _normalized_required_text(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_SEMANTICS_MISSING:" + field)
    return normalized


def _normalized_delay(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_SEMANTICS_INVALID:" + field)
    try:
        numeric = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_SEMANTICS_INVALID:" + field) from exc
    if not numeric.is_integer():
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_SEMANTICS_INVALID:" + field)
    return int(numeric)


def _ppl_evidence_semantics(rules: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep only values that affect source evidence interpretation."""
    return {
        "rules_ppl_classification_version": str((rules.get("policy_versions") or {}).get("ppl_classification") or ""),
        "round_ppl_classification_version": str((policy.get("policy_versions") or {}).get("ppl_classification") or ""),
        "fixed_gates": dict((rules.get("ppl_classification") or {}).get("fixed_gates") or {}),
        "round_fixed_gates": dict(((policy.get("ppl_classification") or {}).get("fixed_gates") or {})),
        "final_theme_check": (rules.get("ppl_classification") or {}).get("final_theme_check"),
    }


def _normalized_extension_semantic_identity(
    plan: Mapping[str, Any], rules: Mapping[str, Any], policy: Mapping[str, Any],
) -> Dict[str, Any]:
    settings = dict(plan.get("simulation_settings") or {})
    return {
        "region": _normalized_required_text(settings.get("region"), field="region"),
        "universe": _normalized_required_text(settings.get("universe"), field="universe"),
        "delay": _normalized_delay(settings.get("delay"), field="delay"),
        "neutralization": _normalized_required_text(settings.get("neutralization"), field="neutralization"),
        "instrument_type": _normalized_required_text(settings.get("instrument_type"), field="instrument_type"),
        "runner_goal": _normalized_required_text((plan.get("identity") or {}).get("runner_goal"), field="runner_goal"),
        "target_mode": _normalized_required_text((plan.get("strategy") or {}).get("target_mode"), field="target_mode"),
        "ppl_evidence_semantics": _ppl_evidence_semantics(rules, policy),
    }


def _freeze_evidence(evidence: Mapping[str, Mapping[Any, Mapping[str, int]]]) -> Dict[str, Any]:
    dimensions: Dict[str, List[Dict[str, Any]]] = {}
    for dimension, values in sorted(evidence.items()):
        rows = []
        for key, stat in values.items():
            rows.append({
                "key": list(key) if isinstance(key, tuple) else key,
                "stat": dict(stat),
            })
        dimensions[dimension] = sorted(rows, key=lambda row: _json(row["key"]))
    return {"format": "EXTENSION_EVIDENCE_SNAPSHOT_V1", "dimensions": dimensions}


def _thaw_evidence(snapshot: Mapping[str, Any]) -> Dict[str, Dict[Any, Dict[str, int]]]:
    dimensions = dict(snapshot.get("dimensions") or {})
    tuple_dimensions = {
        "dataset_field", "dataset_operator", "operator_window",
        "dataset_operator_window", "dataset_operator_window_batch",
    }
    out: Dict[str, Dict[Any, Dict[str, int]]] = {}
    for dimension, rows in dimensions.items():
        restored: Dict[Any, Dict[str, int]] = {}
        for row in rows or []:
            if not isinstance(row, Mapping):
                continue
            key = row.get("key")
            if dimension in tuple_dimensions and isinstance(key, list):
                key = tuple(key)
            restored[key] = dict(row.get("stat") or {})
        out[str(dimension)] = restored
    return out


def _extension_context_from_source(source_context: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    source = dict(source_context or {})
    evidence = dict(source.get("evidence") or {})
    frozen_evidence = _freeze_evidence(evidence)
    normalized_identity = dict(source.get("normalized_semantic_identity") or {
        "source": "NO_TARGETED_EVIDENCE_SOURCE",
    })
    return {
        "enabled": True,
        "schema": "CANDIDATE_EXTENSION_CONTEXT_V1",
        "extension_policy_version": EXTENSION_POLICY_VERSION,
        "normalized_source_semantic_identity": normalized_identity,
        "normalized_source_semantic_identity_digest": _canonical_digest(normalized_identity),
        "evidence_snapshot": frozen_evidence,
        "canonical_evidence_digest": _canonical_digest(frozen_evidence),
        "source_run_id": source.get("source_run_id"),
        "source_round_id": source.get("source_round_id"),
        "source_last_completed_batch": source.get("source_last_completed_batch"),
        "source_search_consumed": source.get("source_search_consumed"),
        "source_ledger_fact_count": source.get("source_ledger_fact_count"),
        "evidence_snapshot_created_at": _now(),
    }


def _extension_source_from_manifest(store: Any, round_id: str) -> Optional[Dict[str, Any]]:
    manifest_row = load_manifest(store, round_id)
    if not manifest_row:
        return None
    manifest = _load_json_mapping(manifest_row.get("manifest_json"), error="EXTENSION_MANIFEST_INVALID")
    context = manifest.get(EXTENSION_CONTEXT_KEY)
    if not isinstance(context, Mapping) or not context.get("enabled"):
        return None
    if str(context.get("extension_policy_version") or "") != EXTENSION_POLICY_VERSION:
        raise ConfigError("EXTENSION_POLICY_VERSION_MISMATCH")
    evidence_snapshot = context.get("evidence_snapshot")
    if not isinstance(evidence_snapshot, Mapping):
        raise ConfigError("EXTENSION_EVIDENCE_SNAPSHOT_MISSING")
    if _canonical_digest(evidence_snapshot) != str(context.get("canonical_evidence_digest") or ""):
        raise ConfigError("EXTENSION_EVIDENCE_SNAPSHOT_DIGEST_MISMATCH")
    return {
        "source_run_id": context.get("source_run_id"),
        "source_round_id": context.get("source_round_id"),
        "evidence": _thaw_evidence(evidence_snapshot),
        "manifest_context": dict(context),
    }


def _extension_source_context(store: Any, config: Any, policy: Mapping[str, Any], source_run_id: str) -> Dict[str, Any]:
    """Validate a source run before its evidence may influence a new universe."""
    source = store.get_run(source_run_id)
    source_round = get_round(store, run_id=source_run_id)
    if not source or not source_round:
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_RUN_OR_ROUND_NOT_FOUND")
    source_plan = _load_json_mapping(source.get("plan_json"), error="EXTENSION_EVIDENCE_SOURCE_PLAN_UNAVAILABLE")
    source_rules = _load_json_mapping(source.get("rules_json"), error="EXTENSION_EVIDENCE_SOURCE_RULES_UNAVAILABLE")
    source_policy = _load_json_mapping(source_round.get("config_json"), error="EXTENSION_EVIDENCE_SOURCE_POLICY_UNAVAILABLE")
    source_identity = _normalized_extension_semantic_identity(source_plan, source_rules, source_policy)
    current_identity = _normalized_extension_semantic_identity(config.plan, config.rules, policy)
    if source_identity != current_identity:
        changed = [key for key in source_identity if source_identity.get(key) != current_identity.get(key)]
        raise ConfigError("EXTENSION_EVIDENCE_SOURCE_SEMANTICS_MISMATCH:" + ",".join(changed))
    batches = load_batches(store, str(source_round["round_id"]))
    completed_batches = [int(x.get("batch_no") or 0) for x in batches if str(x.get("status") or "").upper() == "COMPLETED"]
    ledger = load_ledger(store, str(source_round["round_id"]))
    return {
        "source_run_id": str(source_run_id), "source_round_id": str(source_round["round_id"]),
        "normalized_semantic_identity": source_identity,
        "source_last_completed_batch": max(completed_batches, default=0),
        "source_search_consumed": int(source_round.get("search_consumed") or 0),
        "source_ledger_fact_count": len(ledger),
        "evidence": _round_research_evidence(store, str(source_run_id), str(source_round["round_id"]), policy),
    }


def _extension_specs_for_discovery(
    discovery: Any, config: Any, policy: Mapping[str, Any], source_context: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Return first-stage Core and evidence-gated Targeted extension specs."""
    source_evidence = dict((source_context or {}).get("evidence") or {})
    dataset_stats = dict(source_evidence.get("dataset") or {})
    field_stats = dict(source_evidence.get("dataset_field") or {})
    rolling = dict(policy.get("rolling_discovery") or {})
    min_attempts = int(rolling.get("cooldown_min_attempts", 8))
    weak_rate = float(rolling.get("cooldown_viable_rate_max", 0.10))
    specs: List[Dict[str, Any]] = []
    for field in discovery.fields:
        if not field.get("selected") or str(field.get("field_type") or "").upper() != "MATRIX":
            continue
        dataset_id = str(field.get("dataset_id") or "").strip()
        field_id = str(field.get("field_id") or "").strip()
        if not dataset_id or not field_id:
            raise ConfigError("DISCOVERY_SELECTED_FIELD_IDENTITY_REQUIRED")
        route = dict(config.rules["candidate_routes"].get(field.get("semantic_class"), {}))
        ds_stat = dict(dataset_stats.get(dataset_id) or _new_evidence_stat())
        attempts = int(ds_stat.get("attempts", 0))
        viable_rate = (_evidence_viable_count(ds_stat) / attempts) if attempts else 1.0
        # Cross-operator weakness may lower a Core candidate's initial rank but
        # is never evidence that ts_std_dev itself failed.
        core_adjustment = -5.0 if attempts >= min_attempts and viable_rate <= weak_rate else 0.0
        for window in (22, 66):
            specs.append({
                "field": field, "operator": "ts_std_dev", "window": window,
                "route_priority": route.get("priority", "LOW"), "score_adjustment": core_adjustment,
                "metadata": {
                    "extension_source": "CORE_OPERATOR_RESTORE",
                    "extension_priority": [0.0, 0.0, 0.0, 0.0],
                    "source_dataset_negative_adjustment": core_adjustment,
                },
            })
        if not source_context:
            continue
        stat = dict(field_stats.get((dataset_id, field_id)) or _new_evidence_stat())
        trigger = {
            "paid_complete": int(stat.get("attempts", 0)),
            "ppl_success": int(stat.get("ppl_success", 0)),
            "strong_near": int(stat.get("ppl_strong_near_pass", 0)),
            "near": int(stat.get("ppl_near_pass", 0)),
            "signal_viable": int(stat.get("signal_viable", 0)),
        }
        positive = (
            trigger["paid_complete"] >= 1 and (
                trigger["ppl_success"] >= 1 or trigger["strong_near"] >= 1
                or trigger["near"] >= 1 or trigger["signal_viable"] >= 2
            )
        )
        if not positive:
            continue
        priority = [trigger["ppl_success"], trigger["strong_near"], trigger["near"], trigger["signal_viable"]]
        for operator in ("ts_arg_min", "ts_arg_max", "ts_quantile"):
            for window in (22, 66):
                specs.append({
                    "field": field, "operator": operator, "window": window,
                    "route_priority": route.get("priority", "LOW"), "score_adjustment": 0.0,
                    "metadata": {
                        "extension_source": "TARGETED_OPERATOR_EXTENSION",
                        "parent_dataset_id": dataset_id, "parent_field": field_id,
                        "trigger_evidence": trigger, "source_run_id": source_context["source_run_id"],
                        "extension_priority": priority,
                    },
                })
    return specs


def _extension_provenance_by_candidate(store: Any, run_id: str, candidate_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    ids = [str(x) for x in candidate_ids if x]
    if not ids:
        return {}
    marks = ",".join("?" for _ in ids)
    with store.connect() as conn:
        rows = conn.execute(
            f"SELECT candidate_id,provenance_json FROM ppl_candidate_provenance WHERE run_id=? AND candidate_id IN ({marks})",
            [run_id] + ids,
        ).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        try:
            metadata = json.loads(row[1] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(metadata, dict) and metadata.get("extension_source"):
            out[str(row[0])] = metadata
    return out


def _extension_pool_state(store: Any, run_id: str) -> Dict[str, Any]:
    """Return all-round extension accounting without altering existing rows."""
    candidates = [dict(row) for row in store.load_candidates(run_id)]
    provenance = _extension_provenance_by_candidate(
        store, run_id, [str(row.get("candidate_id") or "") for row in candidates],
    )
    core = []
    targeted = []
    base = []
    core_windows = set()
    for row in candidates:
        metadata = provenance.get(str(row.get("candidate_id") or "")) or {}
        source = str(metadata.get("extension_source") or "")
        if source == "CORE_OPERATOR_RESTORE":
            core.append(row)
            if str(row.get("operator") or "") == "ts_std_dev" and row.get("window") is not None:
                core_windows.add(int(row["window"]))
        elif source == "TARGETED_OPERATOR_EXTENSION":
            targeted.append(row)
        else:
            if (str(row.get("structure_status") or "ELIGIBLE") == "ELIGIBLE"
                    and requires_extension_new_post(row.get("execution_action"))):
                base.append(row)
    return {
        "extension_enabled": True,
        "existing_base_count": len(base),
        "existing_core_count": len(core),
        "existing_targeted_count": len(targeted),
        "existing_core_windows": sorted(core_windows),
        "existing_sim_keys": sorted({str(row.get("sim_key")) for row in candidates if row.get("sim_key")}),
    }


def _extension_batch_cap(batch_size: int) -> int:
    by_fraction = max(1, math.floor(int(batch_size) * float(EXTENSION_SELECTION_POLICY["max_new_operator_fraction"])))
    return min(int(EXTENSION_SELECTION_POLICY["max_new_operator_per_batch"]), by_fraction)


def requires_extension_new_post(action: Any) -> bool:
    """True only for actions that the V2.1 adapter may POST remotely."""
    return str(action or "") in POST_ACTIONS


def _evidence_viable_count(stat: Mapping[str, int]) -> int:
    """SEARCH-positive evidence count, with compatibility for legacy fixtures.

    V3_RANK_004 deliberately excludes broad ``signal_viable`` and
    ``fixed_repairable`` evidence. Real current-round stats contain
    ``search_viable``; older test fixtures fall back to Local/PRE-TAG success.
    """
    if "search_viable" in stat:
        return int(stat.get("search_viable", 0))
    return max(
        int(stat.get("local_pass", 0)), int(stat.get("pretag_resolved", 0)),
        int(stat.get("ppl_near_pass", 0)), int(stat.get("ppl_strong_near_pass", 0)),
        int(stat.get("ppl_success", 0)),
    )


def _stage_evidence_score(stat: Mapping[str, int], adaptive: Mapping[str, Any]) -> float:
    """Compatibility export; C4 strategy implementation lives in search_strategy."""
    return search_stage_evidence_score(stat, adaptive)

def _adaptive_scores(row: Mapping[str, Any], evidence: Mapping[str, Mapping[Any, Mapping[str, int]]],
                     policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Compatibility export; C4 strategy implementation lives in search_strategy."""
    return search_adaptive_scores(row, evidence, policy)

def _adaptive_score(row: Mapping[str, Any], combo: Mapping[str, int]) -> float:
    """Backward-compatible V3.0.2 helper retained for external callers/tests.

    The production V3.0.3 selector uses _adaptive_scores() instead.
    """
    base = float(row.get("initial_selection_score") or 0.0)
    attempts = int(combo.get("attempts", 0))
    checked = int(combo.get("checked", 0))
    success = int(combo.get("success", 0))
    success_post = (success + 0.5) / (attempts + 2.0)
    check_post = (checked + 1.0) / (attempts + 2.0)
    return round(base + 14.0 * success_post + 3.0 * check_post, 6)



def _bootstrap_dataset_states(store: Any, run_id: str, round_id: str) -> int:
    """Seed ACTIVE Dataset state for legacy/in-progress V3 rounds exactly once."""
    existing = load_dataset_states(store, round_id)
    if existing:
        return len(existing)
    rows = store.load_candidates(run_id)
    by_dataset: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ds = str(row.get("dataset_id") or "")
        if not ds:
            continue
        by_dataset.setdefault(ds, row)
    stats = _empirical_dimension_stats(store, run_id, "dataset_id")
    for ds, row in sorted(by_dataset.items()):
        st = stats.get(ds, {})
        upsert_dataset_state(
            store, round_id, ds, state="ACTIVE", admitted_batch=0, last_refresh_batch=0,
            source_snapshot_id=row.get("discovery_snapshot_id"),
            productivity_score=_smoothed_dimension_score(st), attempts=int(st.get("attempts", 0)),
            checked=int(st.get("checked", 0)), success=int(st.get("success", 0)),
            reason="INITIAL_OR_LEGACY_ROUND_DATASET",
            metadata={"bootstrap": True},
        )
    return len(by_dataset)


def _active_dataset_ids(store: Any, round_id: str) -> set[str]:
    states = load_dataset_states(store, round_id)
    return {str(x["dataset_id"]) for x in states if str(x.get("state") or "").upper() == "ACTIVE"}


def _active_dataset_ids_read_only(store: Any, round_id: str,
                                  candidates: Sequence[Mapping[str, Any]]) -> set[str]:
    """Read the active pool without schema bootstrap or any durable write."""
    with store.connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_dataset_states'"
        ).fetchone()
        rows = list(conn.execute(
            "SELECT dataset_id,state FROM ppl_round_dataset_states WHERE round_id=?", (round_id,),
        )) if exists else []
    if rows:
        return {str(row[0]) for row in rows if str(row[1] or "").upper() == "ACTIVE"}
    return {str(row.get("dataset_id")) for row in candidates if str(row.get("dataset_id") or "")}


def _completed_search_batches(store: Any, round_id: str) -> int:
    return sum(
        1 for b in load_batches(store, round_id)
        if str(b.get("phase") or "").upper() == "SEARCH"
        and str(b.get("status") or "").upper() in COMPLETED_RESEARCH_BATCH_STATUSES
    )


def _eligible_paid_search_families(store: Any, run_id: str, round_id: str) -> int:
    active = _active_dataset_ids(store, round_id)
    protected = {w["family_id"] for w in load_winners(store, round_id) if int(w.get("protected") or 0)}
    rows = store.load_candidates(run_id)
    attempted = {
        family_id(x) for x in rows
        if str(x.get("simulation_status") or "NONE").upper() not in {"", "NONE"}
        or x.get("alpha_id") or int(x.get("new_post_budget_consumed") or 0) > 0
    }
    out = set()
    for row in rows:
        if str(row.get("lifecycle_state") or "") != "PLANNED":
            continue
        if str(row.get("structure_status") or "ELIGIBLE") != "ELIGIBLE":
            continue
        if active and str(row.get("dataset_id") or "") not in active:
            continue
        fid = family_id(row)
        if fid in protected or fid in attempted:
            continue
        out.add(fid)
    return len(out)


def _dataset_success_ids(store: Any, run_id: str, round_id: str) -> set[str]:
    winners = {str(w.get("candidate_id") or "") for w in load_winners(store, round_id) if w.get("candidate_id")}
    rows = {str(x.get("candidate_id")): x for x in store.load_candidates(run_id)}
    return {str(rows[cid].get("dataset_id")) for cid in winners if cid in rows and rows[cid].get("dataset_id")}


def _refresh_trigger(store: Any, run_id: str, round_id: str, policy: Mapping[str, Any]) -> Optional[str]:
    cfg = dict(policy.get("rolling_discovery") or {})
    if not cfg.get("enabled"):
        return None
    completed = _completed_search_batches(store, round_id)
    refreshes = load_dataset_refreshes(store, round_id)
    max_refreshes = cfg.get("max_refreshes", 12)
    if max_refreshes is not None and len(refreshes) >= int(max_refreshes):
        return None
    last_batch = max([int(x.get("batch_no") or 0) for x in refreshes], default=0)
    if (completed >= int(cfg.get("min_search_batches_before_refresh", 5))
            and completed - last_batch >= int(cfg.get("refresh_every_search_batches", 5))):
        return "PERIODIC"
    safe = _eligible_paid_search_families(store, run_id, round_id)
    if (safe < int(cfg.get("low_pool_trigger_families", 80))
            and completed >= int(cfg.get("low_pool_min_batches_since_refresh", 2))
            and completed - last_batch >= int(cfg.get("low_pool_min_batches_since_refresh", 2))):
        return "LOW_SAFE_CANDIDATE_POOL"
    return None


def _enqueue_continuous_rolling_discovery(
    store: Any, config: Any, run_id: str, round_id: str, policy: Mapping[str, Any], *,
    batch_no: int, trigger: str,
) -> Dict[str, Any]:
    """Create one durable non-blocking Dataset refresh for Continuous mode.

    Active work is reused idempotently.  A deterministic FAILED refresh never
    pins the round forever: after its durable endpoint cooldown expires the next
    scheduler pass allocates a *new* refresh number.  Before expiry, no duplicate
    work is created even if the low-pool trigger remains continuously true.
    """
    cfg = dict(policy.get("rolling_discovery") or {})
    continuous = parse_continuous_policy(policy)
    _bootstrap_dataset_states(store, run_id, round_id)
    before_states = load_dataset_states(store, round_id)
    seen = {str(x.get("dataset_id")) for x in before_states}
    active_before = {
        str(x.get("dataset_id")) for x in before_states
        if str(x.get("state") or "").upper() == "ACTIVE"
    }

    active_work_states = {
        "DISCOVERY_DUE", "WAIT_RATE_LIMIT", "WAIT_NETWORK", "WAIT_AUTH", "READY_APPLY", "APPLYING"
    }
    with store.connect() as conn:
        latest = conn.execute(
            """SELECT * FROM ppl_discovery_work
               WHERE run_id=? AND round_id=? ORDER BY refresh_no DESC LIMIT 1""",
            (run_id, round_id),
        ).fetchone()
        endpoint = conn.execute(
            """SELECT wait_state,next_retry_at,last_error FROM ppl_endpoint_waits
               WHERE run_id=? AND endpoint_type='DISCOVERY'""",
            (run_id,),
        ).fetchone()
    latest_work = dict(latest) if latest else {}
    latest_state = str(latest_work.get("queue_state") or "")
    if latest_work and latest_state in active_work_states:
        return {
            "queued": True, "created_new": False,
            "refresh_no": int(latest_work.get("refresh_no") or 0), "trigger": trigger,
            "queue_state": latest_state, "stage": latest_work.get("stage"),
            "discovery_work_id": latest_work.get("discovery_work_id"),
            "reason": "DISCOVERY_WORK_ALREADY_ACTIVE",
        }

    if latest_work and latest_state == "FAILED" and endpoint:
        wait_state = str(endpoint[0] or "")
        retry_at_raw = endpoint[1]
        if wait_state == "WAIT_DISCOVERY_REFRESH" and retry_at_raw:
            try:
                retry_at = datetime.fromisoformat(str(retry_at_raw).replace("Z", "+00:00"))
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                now_dt = datetime.now(timezone.utc)
            except (TypeError, ValueError):
                retry_at = None
                now_dt = datetime.now(timezone.utc)
            if retry_at is not None and retry_at > now_dt:
                return {
                    "queued": False, "created_new": False,
                    "refresh_no": int(latest_work.get("refresh_no") or 0), "trigger": trigger,
                    "queue_state": "FAILED", "stage": latest_work.get("stage"),
                    "discovery_work_id": latest_work.get("discovery_work_id"),
                    "reason": "DISCOVERY_FAILURE_COOLDOWN",
                    "next_retry_at": retry_at.isoformat(),
                    "last_error": endpoint[2],
                }

    durable_refreshes = load_dataset_refreshes(store, round_id)
    max_applied_refresh = max([int(x.get("refresh_no") or 0) for x in durable_refreshes], default=0)
    max_work_refresh = int(latest_work.get("refresh_no") or 0) if latest_work else 0
    refresh_no = max(max_applied_refresh, max_work_refresh) + 1
    work = enqueue_discovery_refresh(
        store, run_id, round_id, refresh_no=refresh_no, batch_no=int(batch_no), trigger=str(trigger),
        excluded_dataset_ids=sorted(seen),
        probe_count=int(cfg.get("probe_new_datasets_per_refresh", 6)),
        admit_count=int(cfg.get("admit_new_datasets_per_refresh", 3)),
    )
    if work.get("created_new"):
        # The cooldown gate has been crossed; the new durable work item itself
        # now owns the next due-time.  Clear a stale endpoint-level failure wait.
        with store.connect() as conn:
            conn.execute(
                """UPDATE ppl_endpoint_waits SET wait_state='READY',next_retry_at=NULL,
                       retry_after_seconds=NULL,consecutive_failures=0,last_error=NULL,updated_at=?
                   WHERE run_id=? AND endpoint_type='DISCOVERY' AND wait_state='WAIT_DISCOVERY_REFRESH'""",
                (datetime.now(timezone.utc).isoformat(), run_id),
            )
        record_event(
            store, round_id, run_id, "DATASET_REFRESH_STARTED", batch_no=int(batch_no), phase="SEARCH",
            payload={
                "refresh_no": refresh_no, "trigger": trigger, "active_before": sorted(active_before),
                "seen_dataset_count": len(seen), "execution_mode": "V31_DURABLE_DISCOVERY_QUEUE",
                "simulation_posts": 0,
                "failure_cooldown_seconds": float(continuous.discovery_failure_cooldown_seconds),
            },
        )
    return {
        "queued": bool(work), "created_new": bool(work.get("created_new")),
        "refresh_no": refresh_no, "trigger": trigger,
        "queue_state": work.get("queue_state"), "stage": work.get("stage"),
        "discovery_work_id": work.get("discovery_work_id"),
    }

def _apply_ready_continuous_discovery(
    store: Any, config: Any, machine: Any, alpha_db: Path, run_id: str, round_id: str,
    policy: Mapping[str, Any], *, limit: int = 1,
) -> List[Dict[str, Any]]:
    """Apply durable discovery metadata to the candidate universe idempotently."""
    applied: List[Dict[str, Any]] = []
    expansion_allowed = parse_continuous_policy(policy).allow_search_pool_expansion
    for work in ready_discovery_work(store, run_id, limit=max(1, int(limit))):
        if str(work.get("round_id")) != str(round_id):
            continue
        if not expansion_allowed:
            mark_discovery_expansion_suppressed(store, int(work["discovery_work_id"]))
            record_event(
                store, round_id, run_id, "DATASET_REFRESH_SUPPRESSED",
                batch_no=int(work.get("batch_no") or 0), phase="SEARCH",
                payload={
                    "refresh_no": int(work.get("refresh_no") or 0),
                    "reason": "EXPANSION_DISABLED", "new_candidate_count": 0,
                    "simulation_posts": 0,
                },
                source_event_key=f"discovery_suppressed:{work['discovery_work_id']}:EXPANSION_DISABLED",
            )
            applied.append({
                "refresh_no": int(work.get("refresh_no") or 0),
                "suppressed": True, "reason": "EXPANSION_DISABLED", "new_candidate_count": 0,
            })
            continue
        mark_discovery_applying(store, int(work["discovery_work_id"]))
        discovery = materialize_discovery_result(work, config)
        report = _append_rolling_candidates(
            store, config, machine, None, alpha_db, run_id, round_id, policy,
            batch_no=int(work.get("batch_no") or 0), trigger=str(work.get("trigger") or "CONTINUOUS"),
            discovery_override=discovery, refresh_no_override=int(work.get("refresh_no") or 0),
            network_get_count_override=int(work.get("network_get_count") or 0),
            emit_started_event=False,
        )
        mark_discovery_applied(store, int(work["discovery_work_id"]))
        applied.append(report)
    return applied


def _append_rolling_candidates(store: Any, config: Any, machine: Any, ro: Any, alpha_db: Path,
                               run_id: str, round_id: str, policy: Mapping[str, Any], *, batch_no: int,
                               trigger: str, discovery_override: Optional[DiscoveryResult] = None,
                               refresh_no_override: Optional[int] = None,
                               network_get_count_override: Optional[int] = None,
                               emit_started_event: bool = True) -> Dict[str, Any]:
    """Probe unseen Datasets and add candidates without rebuilding prior universe."""
    if not parse_continuous_policy(policy).allow_search_pool_expansion:
        return {
            "refresh_no": int(refresh_no_override or 0), "trigger": trigger,
            "suppressed": True, "reason": "EXPANSION_DISABLED", "new_candidate_count": 0,
            "network_get_count": int(network_get_count_override or 0),
        }
    cfg = dict(policy.get("rolling_discovery") or {})
    _bootstrap_dataset_states(store, run_id, round_id)
    before_states = load_dataset_states(store, round_id)
    seen = {str(x.get("dataset_id")) for x in before_states}
    active_before = {str(x.get("dataset_id")) for x in before_states if str(x.get("state") or "").upper() == "ACTIVE"}
    refresh_no = int(refresh_no_override) if refresh_no_override is not None else len(load_dataset_refreshes(store, round_id)) + 1
    if emit_started_event:
        record_event(store, round_id, run_id, "DATASET_REFRESH_STARTED", batch_no=batch_no, phase="SEARCH",
                     payload={"refresh_no": refresh_no, "trigger": trigger, "active_before": sorted(active_before),
                              "seen_dataset_count": len(seen)})
    get_count_before = len(getattr(ro, "methods", [])) if discovery_override is None else 0
    discovery = discovery_override or discover_rolling_online(
        ro, config, machine, excluded_dataset_ids=sorted(seen),
        probe_count=int(cfg.get("probe_new_datasets_per_refresh", 6)),
        admit_count=int(cfg.get("admit_new_datasets_per_refresh", 3)),
    )
    store.save_discovery_snapshot(discovery.snapshot, discovery.datasets, discovery.fields)
    admitted = list(discovery.snapshot.get("rolling_admitted_dataset_ids") or [])
    probed = list(discovery.snapshot.get("rolling_probe_dataset_ids") or [])
    new_candidates: List[Dict[str, Any]] = []
    preview: Dict[str, Any] = {}
    rolling_pruned = 0
    if admitted:
        frozen_extension_source = _extension_source_from_manifest(store, round_id)
        extension_config = (
            config_with_extension_execution_identity(config, frozen_extension_source["manifest_context"])
            if frozen_extension_source is not None else config
        )
        dry = estimate_candidate_plan(discovery, extension_config, store.operator_registry_summary())
        dry["execution_hash"] = extension_config.execution_hash
        store.save_dry_run(dry["dry_run_id"], discovery.snapshot["snapshot_id"], extension_config.execution_hash, dry["source"], dry)
        extension_specs = _extension_specs_for_discovery(
            discovery, extension_config, policy, frozen_extension_source,
        ) if frozen_extension_source is not None else []
        new_candidates, preview = generate_candidate_preview(
            discovery, dry, extension_config, run_id=run_id, alpha_db=alpha_db, machine_lib=machine,
            extension_specs=extension_specs,
            extension_pool_state=(_extension_pool_state(store, run_id)
                                  if frozen_extension_source is not None else None),
        )
        store.upsert_candidates(new_candidates)
        ids = [str(x.get("candidate_id")) for x in new_candidates]
        if ids:
            with store.connect() as conn:
                conn.executemany(
                    "UPDATE ppl_candidates SET selected_for_initial_search=0,selection_rank=NULL,selection_reason='V3_ROLLING_DATASET_POOL' WHERE candidate_id=?",
                    [(cid,) for cid in ids],
                )
        # Capture discovery decisions without rewriting batch-0 ranks.
        for rank, cand in enumerate(sorted(new_candidates, key=lambda x: (-float(x.get("initial_selection_score") or 0.0), str(x.get("candidate_id") or ""))), 1):
            upsert_candidate_decision(
                store, round_id, run_id, int(batch_no), cand,
                decision="DISCOVERED_ROLLING", decision_reason=f"DATASET_REFRESH_{trigger}",
                selection_rank=rank, selection_score=float(cand.get("initial_selection_score") or 0.0),
                quality_score=float(cand.get("initial_selection_score") or 0.0), selection_mode="ROLLING_DISCOVERY",
                context={
                    "refresh_no": refresh_no, "snapshot_id": discovery.snapshot.get("snapshot_id"),
                    "dataset_id": cand.get("dataset_id"), "sim_key": cand.get("sim_key"),
                    **{
                        key: (cand.get("provenance") or {}).get(key)
                        for key in ("extension_source", "parent_dataset_id", "parent_field", "trigger_evidence")
                        if (cand.get("provenance") or {}).get(key) is not None
                    },
                },
            )

        extension_preflight = dict(preview.get("extension_preflight") or {})
        rolling_pruned = int(extension_preflight.get("core_pruned") or 0) + int(extension_preflight.get("targeted_pruned") or 0)
        if frozen_extension_source is not None and rolling_pruned:
            record_event(
                store, round_id, run_id, "ROUND_EXTENSION_CAP_EXHAUSTED", batch_no=batch_no, phase="SEARCH",
                payload={"refresh_no": refresh_no, "pruned_capacity": rolling_pruned,
                         "extension_preflight": extension_preflight},
            )

    # Failure-aware cooldown.  Weak Datasets may now cool down even when the
    # refresh did not admit a replacement, provided the active pool stays above
    # ``min_active_datasets``.  This prevents repeatedly buying from a Dataset
    # that has accumulated enough current-round paid evidence with no viable
    # signal.  When new Datasets are admitted, the legacy rotation behavior is
    # retained after weak candidates are considered first.
    legacy_stats = _empirical_dimension_stats(store, run_id, "dataset_id")
    research_evidence = _round_research_evidence(store, run_id, round_id, policy)
    dataset_evidence = dict(research_evidence.get("dataset") or {})
    success_ds = _dataset_success_ids(store, run_id, round_id)
    adaptive_cfg = effective_search_ranking(policy)
    candidates_for_cooldown = []
    weak_count = 0
    for ds in sorted(active_before):
        st = dict(dataset_evidence.get(ds) or _new_evidence_stat())
        legacy = legacy_stats.get(ds, {})
        attempts = int(st.get("attempts", 0))
        viable = _evidence_viable_count(st)
        viable_rate = viable / attempts if attempts else 1.0
        score = _stage_evidence_score(st, adaptive_cfg)
        protected = ds in success_ds and bool(cfg.get("preserve_success_datasets", True))
        weak = (
            attempts >= int(cfg.get("cooldown_min_attempts", 8))
            and viable_rate <= float(cfg.get("cooldown_viable_rate_max", 0.10))
        )
        weak_count += int(weak and not protected)
        candidates_for_cooldown.append((protected, not weak, score, -attempts, viable_rate, ds, st, legacy, weak))
    candidates_for_cooldown.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4], x[5]))
    min_active = int(cfg.get("min_active_datasets", 10))
    max_cool = max(0, len(active_before) + len(admitted) - min_active)
    cooldown_cap = int(cfg.get("max_cooldown_per_refresh", 3))
    if admitted:
        cool_n = min(max_cool, max(len(admitted), min(cooldown_cap, weak_count)))
        allow_rotation = True
    elif bool(cfg.get("cooldown_without_admission", True)):
        cool_n = min(max_cool, cooldown_cap, weak_count)
        allow_rotation = False
    else:
        cool_n = 0
        allow_rotation = False

    cooled = []
    for protected, not_weak, score, neg_attempts, viable_rate, ds, st, legacy, weak in candidates_for_cooldown:
        if len(cooled) >= cool_n:
            break
        if protected:
            continue
        if not weak and not allow_rotation:
            continue
        attempts = int(st.get("attempts", 0))
        cooled.append(ds)
        upsert_dataset_state(
            store, round_id, ds, state="COOLDOWN", admitted_batch=0, last_refresh_batch=batch_no,
            productivity_score=score, attempts=attempts, checked=int(legacy.get("checked", 0)),
            success=int(st.get("ppl_success", 0)),
            reason="ZERO_VIABLE_DATASET_COOLDOWN" if weak else "ROTATION_COOLDOWN",
            metadata={"viable": _evidence_viable_count(st), "viable_rate": viable_rate,
                      "current_rule_local_pass": int(st.get("local_pass", 0)),
                      "terminal_fail": int(st.get("terminal_fail", 0)),
                      "refresh_no": refresh_no, "trigger": trigger},
        )
    for ds in active_before - set(cooled):
        st = dict(dataset_evidence.get(ds) or _new_evidence_stat())
        legacy = legacy_stats.get(ds, {})
        attempts = int(st.get("attempts", 0))
        viable_rate = _evidence_viable_count(st) / attempts if attempts else 1.0
        upsert_dataset_state(
            store, round_id, ds, state="ACTIVE", admitted_batch=0, last_refresh_batch=batch_no,
            productivity_score=_stage_evidence_score(st, adaptive_cfg), attempts=attempts,
            checked=int(legacy.get("checked", 0)), success=int(st.get("ppl_success", 0)), reason="RETAINED_ACTIVE",
            metadata={"viable": _evidence_viable_count(st), "viable_rate": viable_rate,
                      "current_rule_local_pass": int(st.get("local_pass", 0)),
                      "terminal_fail": int(st.get("terminal_fail", 0)),
                      "refresh_no": refresh_no, "trigger": trigger},
        )
    for ds in admitted:
        st = dict(dataset_evidence.get(ds) or _new_evidence_stat())
        legacy = legacy_stats.get(ds, {})
        upsert_dataset_state(
            store, round_id, ds, state="ACTIVE", admitted_batch=batch_no, last_refresh_batch=batch_no,
            source_snapshot_id=discovery.snapshot.get("snapshot_id"),
            productivity_score=_stage_evidence_score(st, adaptive_cfg),
            attempts=int(st.get("attempts", 0)), checked=int(legacy.get("checked", 0)),
            success=int(st.get("ppl_success", 0)), reason="ROLLING_DISCOVERY_ADMISSION",
            metadata={"viable": _evidence_viable_count(st), "refresh_no": refresh_no, "trigger": trigger},
        )
    active_after = _active_dataset_ids(store, round_id)
    network_get_count = (
        int(network_get_count_override) if network_get_count_override is not None
        else max(0, len(getattr(ro, "methods", [])) - get_count_before)
    )
    record_dataset_refresh(
        store, round_id, refresh_no=refresh_no, batch_no=batch_no, trigger=trigger,
        status="ADMITTED" if admitted else "NO_USABLE_NEW_DATASET", source_snapshot_id=discovery.snapshot.get("snapshot_id"),
        probed_dataset_ids=probed, admitted_dataset_ids=admitted, cooled_dataset_ids=cooled,
        retained_dataset_ids=sorted(active_after - set(admitted)), new_candidate_count=len(new_candidates),
        network_get_count=network_get_count,
        stats={"active_before": sorted(active_before), "active_after": sorted(active_after),
               "dataset_stats_legacy": legacy_stats,
               "dataset_paid_search_evidence": dataset_evidence,
               "candidate_preview_summary": preview,
               "rolling_extension_pruned_capacity": int(rolling_pruned)},
    )
    record_event(store, round_id, run_id, "DATASET_REFRESH_COMPLETE", batch_no=batch_no, phase="SEARCH",
                 payload={"refresh_no": refresh_no, "trigger": trigger, "probed": probed, "admitted": admitted,
                          "cooled": cooled, "active_after": sorted(active_after), "new_candidate_count": len(new_candidates),
                          "network_get_count": network_get_count,
                          "rolling_extension_pruned_capacity": int(rolling_pruned)})
    return {"refresh_no": refresh_no, "trigger": trigger, "probed": probed, "admitted": admitted,
            "cooled": cooled, "active_after": sorted(active_after), "new_candidate_count": len(new_candidates),
            "snapshot_id": discovery.snapshot.get("snapshot_id"), "network_get_count": network_get_count,
            "rolling_extension_pruned_capacity": int(rolling_pruned)}

def _select_search_batch(store: Any, alpha_db: Path, run_id: str, round_id: str, policy: Mapping[str, Any], remaining: int, *,
                         batch_no: Optional[int] = None, skip_uncertain: bool = False) -> List[Dict[str, Any]]:
    if remaining <= 0:
        return []
    protected = {w["family_id"] for w in load_winners(store, round_id) if int(w.get("protected") or 0)}
    all_rows = store.load_candidates(run_id)
    active_datasets = _active_dataset_ids_read_only(store, round_id, all_rows)
    attempted_families = {
        family_id(x) for x in all_rows
        if str(x.get("simulation_status") or "NONE").upper() not in {"", "NONE"}
           or x.get("alpha_id")
           or int(x.get("new_post_budget_consumed") or 0) > 0
    }
    rows = [x for x in all_rows
            if str(x.get("lifecycle_state") or "") == "PLANNED"
            and str(x.get("structure_status") or "ELIGIBLE") == "ELIGIBLE"
            and family_id(x) not in protected]
    if not rows:
        return []

    extension_metadata = _extension_provenance_by_candidate(
        store, run_id, [str(x.get("candidate_id") or "") for x in rows],
    )
    evidence = _round_research_evidence(store, run_id, round_id, policy)
    paid_search_attempts = sum(
        int(stat.get("attempts", 0)) for stat in (evidence.get("dataset") or {}).values()
    )
    bootstrap_static_prior = paid_search_attempts == 0
    classified: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        row["extension_metadata"] = dict(extension_metadata.get(str(row.get("candidate_id") or "")) or {})
        row["extension_source"] = row["extension_metadata"].get("extension_source")
        adaptive_scores = _adaptive_scores(row, evidence, policy)
        row["round_adaptive_score"] = adaptive_scores["exploit"]  # compatibility alias
        row["round_exploit_score"] = adaptive_scores["exploit"]
        row["round_explore_score"] = adaptive_scores["explore"]
        row["round_online_evidence_score"] = adaptive_scores["online_evidence"]
        row["round_combo_scope"] = str(adaptive_scores.get("combo_scope") or "DATASET_OPERATOR_WINDOW")
        row["round_combo_attempts"] = int(adaptive_scores["combo_attempts"])
        row["round_combo_viable"] = int(adaptive_scores["combo_viable"])
        row["round_recent_zero_positive_penalty"] = float(adaptive_scores.get("recent_zero_positive_penalty") or 0.0)
        row["round_recent_zero_positive_streak"] = int(adaptive_scores.get("recent_zero_positive_streak") or 0)
        row["round_recent_window_batch"] = adaptive_scores.get("recent_window_batch")
        is_targeted_extension = row["extension_source"] == "TARGETED_OPERATOR_EXTENSION"
        row["round_exploit_eligible"] = bool(
            adaptive_scores["exploit_eligible"] if is_targeted_extension
            else (adaptive_scores["exploit_eligible"] or bootstrap_static_prior)
        )
        row["round_exploit_gate_reason"] = (
            "TARGETED_OWN_COMBO_EVIDENCE_REQUIRED" if is_targeted_extension and not adaptive_scores["exploit_eligible"]
            else "ROUND_BOOTSTRAP_STATIC_PRIOR" if bootstrap_static_prior
            else str(adaptive_scores["exploit_gate_reason"])
        )
        row["round_quality_score"] = float(row.get("initial_selection_score") or 0.0)
        row["round_novelty_score"] = float(adaptive_scores["novelty"])
        row["round_prior_scale"] = float(adaptive_scores["prior_scale"])
        row["round_evidence_components"] = adaptive_scores["components"]
        row["round_dataset_score"] = _stage_evidence_score(
            (evidence.get("dataset") or {}).get(str(row.get("dataset_id") or "UNKNOWN"), {}),
            effective_search_ranking(policy),
        )
        row["round_operator_score"] = _stage_evidence_score(
            (evidence.get("operator") or {}).get(str(row.get("operator") or "UNKNOWN"), {}),
            effective_search_ranking(policy),
        )
        row["round_family_score"] = 0.0 if family_id(row) in attempted_families else 1.0
        row["round_repair_risk_score"] = 0.0
        cache = classify_cache_read_only(alpha_db, str(row["sim_key"]))
        row["round_cache_action"] = cache["execution_action"]
        row["round_cache_classification"] = cache["cache_classification"]
        row["round_cache_record"] = cache.get("record") or {}
        # C4 strategy input is immutable research fact, not a Store/session.
        row["_strategy_family_id"] = family_id(row)
        row["_strategy_requires_new_post"] = requires_extension_new_post(row["round_cache_action"])
        classified.append(row)

    uncertain = [x for x in classified if x["round_cache_action"] == "HOLD_UNCERTAIN"]
    if uncertain and not skip_uncertain:
        raise ConfigError("ROUND_UNCERTAIN_SUBMISSION_HOLD")
    if uncertain and skip_uncertain:
        uncertain_keys = {str(x.get("sim_key") or "") for x in uncertain}
        classified = [x for x in classified if str(x.get("sim_key") or "") not in uncertain_keys]

    # C4: engine owns durable fact gathering/cache safety; the actual Search
    # scoring/allocation decision is now performed by a pure strategy module.
    # The diversity envelope is read from the immutable run snapshot and passed
    # as data rather than exposing RunnerStore to the strategy.
    initial_rules = None
    with store.connect() as conn:
        raw = conn.execute("SELECT rules_json FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
    if raw:
        try:
            initial_rules = json.loads(raw[0]).get("initial_selection", {})
        except (TypeError, ValueError, json.JSONDecodeError):
            initial_rules = {}
    initial_rules = initial_rules or {}
    search_allocation = effective_search_allocation(policy)
    extension_cap = _extension_batch_cap(int(search_allocation["batch_size"]))
    search_result = select_search_candidates(
        classified,
        protected_families=tuple(protected),
        active_datasets=tuple(active_datasets),
        attempted_families=tuple(attempted_families),
        initial_rules=initial_rules,
        policy=policy,
        remaining=int(remaining),
        extension_batch_cap=extension_cap,
    )
    bounded = [dict(x) for x in search_result.selected]
    if not bounded:
        return []
    batch_size = int(search_result.batch_size)
    paid_rep_by_family = {str(k): dict(v) for k, v in search_result.paid_rep_by_family.items()}

    if batch_no is not None:
        active_search_policy_hash = search_policy_hash(policy)
        classified_by_id = {str(x.get("candidate_id")): x for x in classified}
        selected_by_id = {str(x.get("candidate_id")): x for x in bounded}
        rank_map = {str(row.get("candidate_id")): rank for rank, row in enumerate(bounded, 1)}
        planned_eligible = [
            dict(x) for x in all_rows
            if str(x.get("lifecycle_state") or "") == "PLANNED"
            and str(x.get("structure_status") or "ELIGIBLE") == "ELIGIBLE"
        ]
        for raw in planned_eligible:
            cid = str(raw.get("candidate_id") or "")
            fid = family_id(raw)
            row = classified_by_id.get(cid, raw)
            if cid in selected_by_id:
                chosen = selected_by_id[cid]
                decision = "SELECTED"
                reason = str(chosen.get("round_cache_action") or "NEW_SIMULATION_REQUIRED")
                mode = chosen.get("round_selection_mode")
            elif fid in protected:
                decision, reason, mode = "SKIP_PROTECTED_FAMILY", "FAMILY_ALREADY_PROTECTED", None
            elif (row.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}
                  and active_datasets and str(row.get("dataset_id") or "") not in active_datasets):
                decision, reason, mode = "SKIP_DATASET_COOLDOWN", "DATASET_NOT_ACTIVE_IN_ROLLING_POOL", None
            elif (row.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}
                  and fid in attempted_families):
                decision, reason, mode = "SKIP_ALREADY_TESTED_FAMILY", "PAID_INITIAL_SEARCH_ALREADY_USED_FOR_FAMILY", None
            elif (row.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}
                  and fid in paid_rep_by_family
                  and str(paid_rep_by_family[fid].get("candidate_id")) != cid):
                decision, reason, mode = "SKIP_REDUNDANT_FAMILY", "LOWER_SCORING_SIBLING_IN_SAME_FAMILY", None
            elif row.get("round_cache_action") == "HOLD_UNCERTAIN":
                decision, reason, mode = "HOLD_UNCERTAIN", "UNCERTAIN_SIMULATION_STATE", None
            elif (row.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}
                  and not requires_extension_new_post(row.get("round_cache_action"))):
                decision, reason, mode = "SKIP_NON_POST_CACHE_ACTION", str(row.get("round_cache_action")), None
            elif (row.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}
                  and not bool(row.get("round_exploit_eligible"))):
                decision, reason, mode = (
                    "SKIP_EXPLORE_ONLY",
                    "NO_POSITIVE_WINDOW_COMBO_EVIDENCE_OR_RECENT_COOLDOWN_AND_NOT_SELECTED_FOR_EXPLORATION",
                    None,
                )
            else:
                decision, reason, mode = "SKIP_BATCH_LIMIT_OR_DIVERSITY", "NOT_SELECTED_IN_CURRENT_ADAPTIVE_BATCH", None
            if mode == "EXPLORE":
                decision_score = row.get("round_explore_score")
            elif mode in {"EXPLOIT", "BACKFILL"}:
                decision_score = row.get("round_exploit_score")
            else:
                decision_score = row.get("round_adaptive_score")
            if decision_score is None:
                decision_score = row.get("initial_selection_score") or 0.0
            upsert_candidate_decision(
                store, round_id, run_id, int(batch_no), row,
                decision=decision, decision_reason=reason, selection_rank=rank_map.get(cid),
                selection_score=float(decision_score),
                quality_score=float(row.get("round_quality_score") or row.get("initial_selection_score") or 0.0),
                novelty_score=float(row.get("round_novelty_score") or 0.0),
                family_score=float(row.get("round_family_score") or 0.0),
                dataset_score=float(row.get("round_dataset_score") or 0.0),
                operator_score=float(row.get("round_operator_score") or 0.0),
                repair_risk_score=float(row.get("round_repair_risk_score") or 0.0),
                selection_mode=mode,
                context={
                    "cache_action": row.get("round_cache_action"),
                    "cache_classification": row.get("round_cache_classification"),
                    "combo_scope": row.get("round_combo_scope"),
                    "combo_attempts": row.get("round_combo_attempts"),
                    "combo_viable": row.get("round_combo_viable"),
                    "recent_zero_positive_penalty": row.get("round_recent_zero_positive_penalty"),
                    "recent_zero_positive_streak": row.get("round_recent_zero_positive_streak"),
                    "recent_window_batch": row.get("round_recent_window_batch"),
                    "exploit_eligible": row.get("round_exploit_eligible"),
                    "exploit_gate_reason": row.get("round_exploit_gate_reason"),
                    "exploit_score": row.get("round_exploit_score"),
                    "explore_score": row.get("round_explore_score"),
                    "online_evidence_score": row.get("round_online_evidence_score"),
                    "prior_scale": row.get("round_prior_scale"),
                    "evidence_components": row.get("round_evidence_components"),
                    "semantic_class": row.get("semantic_class"),
                    "window": row.get("window"),
                    "sim_key": row.get("sim_key"),
                    "extension_source": row.get("extension_source"),
                    "extension_metadata": row.get("extension_metadata"),
                    "extension_batch_cap": extension_cap if row.get("extension_source") else None,
                    "remaining_search_budget_before_batch": int(remaining),
                    "batch_size": int(effective_search_allocation(policy)["batch_size"]),
                    "exploration_fraction": float(effective_search_allocation(policy)["exploration_fraction"]),
                    "dataset_active": (not active_datasets or str(row.get("dataset_id") or "") in active_datasets),
                    "strategy_adapter": SEARCH_COMPAT_STRATEGY if (policy.get("policy_versions") or {}).get("search") else None,
                    "search_policy_version": (policy.get("policy_versions") or {}).get("search"),
                    "search_policy_hash": active_search_policy_hash,
                    "scheduler_policy_version": (policy.get("policy_versions") or {}).get("scheduler"),
                },
            )
        record_event(
            store, round_id, run_id, "BATCH_RANKING_COMPLETE", batch_no=int(batch_no), phase="SEARCH",
            payload={
                "planned_eligible": len(planned_eligible),
                "selected": len(bounded),
                "selected_new_posts": sum(1 for x in bounded if x.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"}),
                "selected_cache_resume": sum(1 for x in bounded if x.get("round_cache_action") in {"CACHE_RESTORE", "RESUME_EXISTING"}),
                "selection_modes": dict(Counter(str(x.get("round_selection_mode") or "NONE") for x in bounded)),
                "exploit_eligible_pool": int(search_result.exploit_pool_count),
                "unproven_pool": int(search_result.unproven_pool_count),
                "paid_search_attempts_before_batch": int(paid_search_attempts),
                "bootstrap_static_prior": bool(bootstrap_static_prior),
                "paid_batch_underfilled": max(0, batch_size - sum(1 for x in bounded if x.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"})),
                "protected_families": len(protected),
                "attempted_families": len(attempted_families),
            },
        )
    return bounded


def _promote_search_selection(store: Any, run_id: str, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    with store.connect() as conn:
        base = int(conn.execute("SELECT coalesce(max(selection_rank),0) FROM ppl_candidates WHERE run_id=?", (run_id,)).fetchone()[0])
        for offset, row in enumerate(rows, 1):
            conn.execute(
                """UPDATE ppl_candidates SET selected_for_initial_search=1,selection_rank=?,selection_reason=?,
                       execution_action=?,cache_classification=?,updated_at=? WHERE candidate_id=?""",
                (base + offset, "V3_ADAPTIVE_BATCH", row.get("round_cache_action"), row.get("round_cache_classification"), _now(), row["candidate_id"]),
            )


def _update_run_post_counters(store: Any, run_id: str, *, attempted: int, confirmed: int, uncertain: int, consumed: int) -> None:
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_runs SET post_attempted=post_attempted+?,post_confirmed=post_confirmed+?,
                   post_uncertain=post_uncertain+?,post_consumed=post_consumed+?,updated_at=? WHERE run_id=?""",
            (int(attempted), int(confirmed), int(uncertain), int(consumed), _now(), run_id),
        )


def _parse_json_list(value: Any) -> List[str]:
    try:
        raw = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    return [str(x) for x in raw if x]


def _reconcile_round_accounting(store: Any, alpha_db: Path, run_id: str, round_id: str,
                                *, fail_on_unresolved_intent: bool = True,
                                enforce_budget_limits: bool = True) -> Dict[str, Any]:
    """Rebuild V3 logical budget counters from durable batch intent + facts.

    The critical failure mode for a 2000-POST round is a process death after a
    remote Simulation is created but before the in-memory batch result is
    committed.  V3 therefore persists exact intended post sim_keys before the
    V2.1 call.  On resume we derive consumption from alpha_results.db and never
    trust a stale in-memory counter.

    If an interrupted RUNNING batch contains a post intent with no durable
    alpha fact, V3 fails closed.  Absence of a local fact cannot prove that the
    remote POST never reached BRAIN, so automatic re-POST would violate the
    project's resume-first rule.
    """
    batches = load_batches(store, round_id)
    all_keys = sorted({key for b in batches for key in _parse_json_list(b.get("planned_post_sim_keys_json"))})
    facts = _alpha_facts(alpha_db, all_keys) if all_keys else {}
    unresolved: List[Dict[str, Any]] = []
    search_consumed = 0
    repair_consumed = 0
    max_batch = 0
    recovered_batches = 0
    with store.connect() as conn:
        for batch in batches:
            batch_no = int(batch.get("batch_no") or 0)
            max_batch = max(max_batch, batch_no)
            keys = _parse_json_list(batch.get("planned_post_sim_keys_json"))
            consumed_from_facts = sum(_durable_confirmed_post(facts.get(key) or {}) for key in keys)
            durable = int(batch.get("logical_posts_consumed") or 0)
            phase = str(batch.get("phase") or "").upper()
            # Production Repair additionally persists consumption on the plan.
            # Use it when available; the fact count is a crash fallback for the
            # narrow window before plan status is committed.
            plan_consumed = 0
            if phase == "REPAIR":
                plan_ids = _parse_json_list(batch.get("selected_plan_ids_json"))
                if plan_ids:
                    marks = ",".join("?" for _ in plan_ids)
                    row = conn.execute(
                        f"SELECT coalesce(sum(consumed_posts),0) FROM ppl_repair_plans WHERE run_id=? AND repair_plan_id IN ({marks})",
                        [run_id] + plan_ids,
                    ).fetchone()
                    plan_consumed = int(row[0] or 0)
            reconciled = max(durable, consumed_from_facts, plan_consumed)
            if reconciled != durable:
                conn.execute(
                    "UPDATE ppl_round_batches SET logical_posts_consumed=? WHERE round_id=? AND batch_no=?",
                    (reconciled, round_id, batch_no),
                )
                recovered_batches += 1
            if phase == "SEARCH":
                search_consumed += reconciled
            elif phase == "REPAIR":
                repair_consumed += reconciled

            if str(batch.get("status") or "").upper() == "RUNNING":
                missing = [key for key in keys if key not in facts]
                if missing:
                    unresolved.append({"batch_no": batch_no, "phase": phase, "sim_keys": missing})
                elif keys:
                    # A prior process died after the network-intent checkpoint.
                    # Facts now make the logical budget recoverable.  Mark the
                    # old batch as recovered; regular resume/check processing
                    # will advance any RUNNING/SUBMITTED candidates.
                    conn.execute(
                        "UPDATE ppl_round_batches SET status='RECOVERED',completed_at=NULL WHERE round_id=? AND batch_no=?",
                        (round_id, batch_no),
                    )
                else:
                    # No durable post intent means this process died before the
                    # V2.1 network-dispatch checkpoint (or the batch was wholly
                    # cache-only).  It is safe to recover with zero consumption.
                    conn.execute(
                        "UPDATE ppl_round_batches SET status='RECOVERED_PRE_DISPATCH',completed_at=NULL WHERE round_id=? AND batch_no=?",
                        (round_id, batch_no),
                    )

        rr = conn.execute("SELECT total_budget,search_budget,repair_budget FROM ppl_rounds WHERE round_id=?", (round_id,)).fetchone()
        if not rr:
            raise ConfigError("V3_ROUND_NOT_FOUND")
        if enforce_budget_limits and (
            search_consumed > int(rr["search_budget"]) or repair_consumed > int(rr["repair_budget"])
        ):
            raise ConfigError("ROUND_RECONCILED_BUDGET_EXCEEDED")
        total_consumed = search_consumed + repair_consumed
        if enforce_budget_limits and total_consumed > int(rr["total_budget"]):
            raise ConfigError("ROUND_RECONCILED_TOTAL_BUDGET_EXCEEDED")
        conn.execute(
            """UPDATE ppl_rounds SET search_consumed=?,repair_consumed=?,current_batch=max(current_batch,?),updated_at=?
               WHERE round_id=?""",
            (search_consumed, repair_consumed, max_batch, _now(), round_id),
        )
        # The V3 run starts at zero and all its Simulation POSTs are represented
        # by V3 batches.  Never decrease a higher durable run counter.
        run_row = conn.execute("SELECT post_consumed FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
        if run_row and int(run_row[0] or 0) < total_consumed:
            conn.execute(
                "UPDATE ppl_runs SET post_consumed=?,updated_at=? WHERE run_id=?",
                (total_consumed, _now(), run_id),
            )
    if unresolved and fail_on_unresolved_intent:
        raise ConfigError("ROUND_UNRESOLVED_POST_INTENT:" + _json(unresolved))
    return {
        "search_consumed": search_consumed,
        "repair_consumed": repair_consumed,
        "total_consumed": search_consumed + repair_consumed,
        "recovered_batches": recovered_batches,
        "unresolved_intents": unresolved,
        "max_batch": max_batch,
    }



def _batch_by_no(store: Any, round_id: str, batch_no: int) -> Optional[Dict[str, Any]]:
    for batch in load_batches(store, round_id):
        if int(batch.get("batch_no") or 0) == int(batch_no):
            return dict(batch)
    return None


def _next_recovered_batch(store: Any, round_id: str) -> Optional[Dict[str, Any]]:
    """Return the oldest crash-recovered batch that still needs normal finalization.

    RECOVERED is deliberately not a terminal batch status.  It only means that
    durable Simulation facts are sufficient to account for already dispatched
    work.  The original batch must still resume/cache-restore its selected
    candidates, run local/PRE-TAG analysis, persist telemetry, and finish as a
    normal COMPLETED batch before a new batch number may be allocated.
    """
    pending = [
        dict(b) for b in load_batches(store, round_id)
        if str(b.get("status") or "").upper() in {"RECOVERED", "RECOVERED_PRE_DISPATCH"}
    ]
    if not pending:
        return None
    return min(pending, key=lambda b: int(b.get("batch_no") or 0))


def _candidate_ids_without_diagnosis(store: Any, run_id: str, candidate_ids: Sequence[str]) -> List[str]:
    ids = [str(x) for x in candidate_ids if x]
    if not ids:
        return []
    marks = ",".join("?" for _ in ids)
    with store.connect() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT candidate_id FROM ppl_diagnoses WHERE run_id=? AND candidate_id IN ({marks})",
            [run_id] + ids,
        ).fetchall()
    done = {str(r[0]) for r in rows}
    return [cid for cid in ids if cid not in done]


def recover_interrupted_batch_undispatched_tail(
    store: Any,
    config: Any,
    alpha_db: Path,
    *,
    run_id: str,
    batch_no: int,
    confirm_undispatched_tail: bool = False,
) -> Dict[str, Any]:
    """Locally release a human-confirmed, never-dispatched SEARCH-batch tail.

    This supports both a process-interrupted RUNNING batch and an older
    COMPLETED batch whose V2.1 SERVER SLOT GUARD explicitly reported that a
    tail was deferred without POSTing.  It is intentionally an explicit repair
    command, not an automatic heuristic.
    A planned POST intent with no durable fact is normally ambiguous and remains
    fail-closed.  The command is only for the narrow case where the operator has
    direct console evidence that a concurrency-limited batch was interrupted
    while the first workers were still pending, so the unscheduled tail never
    reached POST dispatch.

    No network request is made.  Durable facts are preserved.  Only candidates
    with no alpha_id and simulation_status NONE are eligible to be released.
    """
    if not confirm_undispatched_tail:
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_REQUIRES_EXPLICIT_CONFIRMATION")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    round_id = str(rr["round_id"])
    batch = _batch_by_no(store, round_id, int(batch_no))
    if not batch:
        raise ConfigError(f"ROUND_BATCH_NOT_FOUND:{batch_no}")
    if str(batch.get("phase") or "").upper() != "SEARCH":
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_SEARCH_ONLY")
    batch_status = str(batch.get("status") or "").upper()
    if batch_status not in {"RUNNING", "COMPLETED"}:
        raise ConfigError(f"RECOVER_INTERRUPTED_BATCH_STATUS_UNSUPPORTED:{batch.get('status')}")

    selected_ids = _parse_json_list(batch.get("selected_candidate_ids_json"))
    planned_post_keys = _parse_json_list(batch.get("planned_post_sim_keys_json"))
    planned_resume_keys = _parse_json_list(batch.get("planned_resume_sim_keys_json"))
    if planned_resume_keys:
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_WITH_RESUME_INTENT_UNSUPPORTED")
    if not selected_ids or len(selected_ids) != len(planned_post_keys):
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_SELECTION_INTENT_MISMATCH")

    candidates = {str(x["candidate_id"]): dict(x) for x in store.load_candidates(run_id)}
    selected_rows = [candidates.get(cid) for cid in selected_ids]
    if any(row is None for row in selected_rows):
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_CANDIDATE_MISSING")
    by_key = {str(row.get("sim_key")): row for row in selected_rows if row and row.get("sim_key")}
    if set(by_key) != set(planned_post_keys):
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_SIM_KEY_MISMATCH")

    facts = _alpha_facts(alpha_db, planned_post_keys)
    durable_keys = [
        key for key in planned_post_keys
        if str((facts.get(key) or {}).get("status") or "").upper() in LOGICAL_CONSUMED_STATUSES
    ]
    missing_keys = [key for key in planned_post_keys if key not in set(durable_keys)]
    if not missing_keys:
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_NO_UNDISPATCHED_TAIL")
    if not durable_keys:
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_NO_DURABLE_DISPATCH")

    unsafe = []
    for key in missing_keys:
        row = by_key[key]
        sim_status = str(row.get("simulation_status") or "NONE").upper()
        lifecycle = str(row.get("lifecycle_state") or "").upper()
        if row.get("alpha_id") or sim_status not in {"", "NONE"} or lifecycle not in {"SIMULATION_PENDING", "PLANNED"}:
            unsafe.append({
                "candidate_id": row.get("candidate_id"), "sim_key": key,
                "simulation_status": sim_status, "lifecycle_state": lifecycle,
                "alpha_id": row.get("alpha_id"),
            })
    if unsafe:
        raise ConfigError("RECOVER_INTERRUPTED_BATCH_UNSAFE_TAIL:" + _json(unsafe))

    durable_set = set(durable_keys)
    durable_candidate_ids = [cid for cid in selected_ids if str(candidates[cid].get("sim_key")) in durable_set]
    released_candidate_ids = [cid for cid in selected_ids if str(candidates[cid].get("sim_key")) not in durable_set]
    durable_consumed = len(durable_keys)

    release = _release_batch_undispatched_keys(
        store, run_id=run_id, round_id=round_id, batch_no=int(batch_no), sim_keys=missing_keys,
        release_reason=("V3_SERVER_SLOT_DEFERRED_RELEASED" if batch_status == "COMPLETED"
                        else "V3_INTERRUPTED_UNDISPATCHED_RELEASED"),
        event_type=("SERVER_SLOT_DEFERRED_TAIL_CONFIRMED" if batch_status == "COMPLETED"
                    else "INTERRUPTED_BATCH_UNDISPATCHED_TAIL_RELEASED"),
        reopen_batch=True,
    )
    # Preserve or raise the already-consumed budget on the reopened batch.
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_round_batches
               SET logical_posts_consumed=max(logical_posts_consumed,?),planned_resume_sim_keys_json='[]'
               WHERE round_id=? AND batch_no=?""",
            (durable_consumed, round_id, int(batch_no)),
        )
    reconciled = _reconcile_round_accounting(
        store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True
    )
    sync_durable_events(store, round_id, run_id)
    return {
        "project_version": "v3.0.4o",
        "action": ("RECOVER_SERVER_SLOT_DEFERRED_TAIL" if batch_status == "COMPLETED"
                   else "RECOVER_INTERRUPTED_BATCH_UNDISPATCHED_TAIL"),
        "run_id": run_id,
        "round_id": round_id,
        "batch_no": int(batch_no),
        "durable_dispatched": len(durable_candidate_ids),
        "released_undispatched": len(released_candidate_ids),
        "durable_candidate_ids": durable_candidate_ids,
        "released_candidate_ids": released_candidate_ids,
        "source_batch_status": batch_status,
        "release": release,
        "reconciled": reconciled,
        "network_requests": 0,
        "simulation_posts": 0,
        "check_requests": 0,
    }


def _repair_recovery_audit_attempts(project_dir: Path, run_id: str, sim_keys: Sequence[str]) -> Dict[str, Any]:
    """Read current/rotated JSONL audit files for target Repair POST facts."""
    target = set(str(x) for x in sim_keys)
    base = audit_log_path(project_dir, resolve_audit_config(project_dir))
    files = sorted(base.parent.glob(base.name + "*")) if base.parent.exists() else []
    matches: List[Dict[str, Any]] = []
    actions = {"SIMULATION_POST_ATTEMPT", "SIMULATION_POST_CONFIRMED", "SIMULATION_POST_SUCCESS"}
    for path in files:
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                    if not isinstance(row, dict) or row.get("run_id") != run_id:
                        continue
                    if str(row.get("action") or "") not in actions:
                        continue
                    keys = {str(row.get("sim_key") or "")}
                    keys.update(str(x) for x in (row.get("sim_keys") or []) if x)
                    overlap = sorted((keys - {""}) & target)
                    if overlap:
                        matches.append({"file": path.name, "action": row.get("action"), "sim_keys": overlap})
        except OSError as exc:
            raise ConfigError(f"REPAIR_RECOVERY_AUDIT_LOG_UNREADABLE:{path}:{exc}") from exc
    return {"files_checked": [str(x) for x in files], "matches": matches}


def recover_interrupted_repair_batch(
    store: Any,
    config: Any,
    alpha_db: Path,
    project_dir: Path,
    *,
    run_id: str,
    batch_no: int,
    confirm_undispatched_repair_tail: bool = False,
) -> Dict[str, Any]:
    """Release one wholly-never-dispatched REPAIR batch intent, locally only."""
    if not confirm_undispatched_repair_tail:
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_BATCH_REQUIRES_EXPLICIT_CONFIRMATION")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    round_id = str(rr["round_id"])
    if str(rr.get("run_id")) != run_id or str(rr.get("phase") or "").upper() != "REPAIR":
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_ROUND_CONTEXT_MISMATCH")
    batch = _batch_by_no(store, round_id, int(batch_no))
    if not batch:
        raise ConfigError(f"ROUND_BATCH_NOT_FOUND:{batch_no}")
    if str(batch.get("phase") or "").upper() != "REPAIR":
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_BATCH_REPAIR_ONLY")
    if str(batch.get("status") or "").upper() != "RUNNING":
        raise ConfigError(f"RECOVER_INTERRUPTED_REPAIR_STATUS_UNSUPPORTED:{batch.get('status')}")
    if _parse_json_list(batch.get("planned_resume_sim_keys_json")):
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_WITH_RESUME_INTENT_UNSUPPORTED")
    sim_keys = sorted(set(_parse_json_list(batch.get("planned_post_sim_keys_json"))))
    plan_ids = list(dict.fromkeys(_parse_json_list(batch.get("selected_plan_ids_json"))))
    if not sim_keys or not plan_ids:
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_EMPTY_SCOPE")

    facts = _alpha_facts(alpha_db, sim_keys)
    unsafe: List[Dict[str, Any]] = []
    for key, fact in facts.items():
        unsafe.append({
            "sim_key": key, "reason": "ALPHA_RESULT_FACT_EXISTS",
            "status": fact.get("status"), "simulation_url": fact.get("simulation_url"),
            "submitted_at": fact.get("submitted_at"), "alpha_id": fact.get("alpha_id"),
        })
    started_at = str(batch.get("started_at") or "")
    evidence: Dict[str, Any] = {
        "unique_sim_keys": len(sim_keys), "selected_plan_count": len(plan_ids),
        "alpha_result_rows": len(facts),
    }
    with store.connect() as conn:
        marks = ",".join("?" for _ in sim_keys)
        plan_marks = ",".join("?" for _ in plan_ids)
        event_rows = conn.execute(
            f"""SELECT event_type,sim_key FROM ppl_round_events
                WHERE round_id=? AND sim_key IN ({marks})
                  AND event_type IN ('SIMULATION_POST_ATTEMPT','SIMULATION_POST_CONFIRMED','SIMULATION_POST_SUCCESS')""",
            [round_id] + sim_keys,
        ).fetchall()
        live_rows = conn.execute(
            f"""SELECT event_type,sim_key FROM ppl_live_execution_audits
                WHERE run_id=? AND sim_key IN ({marks})
                  AND event_type IN ('SIMULATION_POST_ATTEMPT','SIMULATION_POST_CONFIRMED','SIMULATION_POST_SUCCESS')""",
            [run_id] + sim_keys,
        ).fetchall()
        ledger_rows = conn.execute(
            f"SELECT * FROM ppl_round_simulation_ledger WHERE round_id=? AND sim_key IN ({marks})",
            [round_id] + sim_keys,
        ).fetchall()
        candidate_rows = conn.execute(
            f"SELECT * FROM ppl_candidates WHERE run_id=? AND sim_key IN ({marks})",
            [run_id] + sim_keys,
        ).fetchall()
        plans = conn.execute(
            f"SELECT * FROM ppl_repair_plans WHERE run_id=? AND repair_plan_id IN ({plan_marks})",
            [run_id] + plan_ids,
        ).fetchall()
        repair_edges = conn.execute(
            f"""SELECT r.* FROM ppl_repairs r
                JOIN ppl_repair_plans p ON p.run_id=r.run_id AND p.repair_signature=r.repair_signature
                WHERE r.run_id=? AND p.repair_plan_id IN ({plan_marks})""",
            [run_id] + plan_ids,
        ).fetchall()
        intent_rows = conn.execute(
            f"""SELECT sim_key,payload_json FROM ppl_round_events
                WHERE round_id=? AND batch_no=? AND phase='REPAIR'
                  AND event_type='SIMULATION_POST_INTENT' AND sim_key IN ({marks})""",
            [round_id, int(batch_no)] + sim_keys,
        ).fetchall()
        prior_other_consumed = int(conn.execute(
            """SELECT coalesce(sum(logical_posts_consumed),0) FROM ppl_round_batches
               WHERE round_id=? AND phase='REPAIR' AND batch_no<>?""",
            (round_id, int(batch_no)),
        ).fetchone()[0] or 0)
        run_row = conn.execute("SELECT post_uncertain FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()

    intent_plan_associations: Dict[str, List[str]] = {key: [] for key in sim_keys}
    for row in intent_rows:
        key = str(row["sim_key"] or "")
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        linked = payload.get("plan_ids") if isinstance(payload.get("plan_ids"), list) else []
        if payload.get("repair_plan_id"):
            linked = [*linked, payload.get("repair_plan_id")]
        intent_plan_associations.setdefault(key, []).extend(str(x) for x in linked if x)
    intent_plan_associations = {
        key: sorted(set(values)) for key, values in sorted(intent_plan_associations.items())
    }
    missing_intent_keys = sorted(key for key, values in intent_plan_associations.items() if not values)
    unknown_intent_plans = sorted({
        plan_id for values in intent_plan_associations.values() for plan_id in values
        if plan_id not in set(plan_ids)
    })
    if missing_intent_keys:
        unsafe.append({"reason": "REPAIR_INTENT_ASSOCIATION_MISSING", "sim_keys": missing_intent_keys})
    if unknown_intent_plans:
        unsafe.append({"reason": "REPAIR_INTENT_PLAN_OUTSIDE_BATCH", "plan_ids": unknown_intent_plans})
    if event_rows:
        unsafe.extend({"sim_key": r["sim_key"], "reason": r["event_type"]} for r in event_rows)
    if live_rows:
        unsafe.extend({"sim_key": r["sim_key"], "reason": r["event_type"]} for r in live_rows)
    for row in ledger_rows:
        unsafe.append({"sim_key": row["sim_key"], "reason": "SIMULATION_LEDGER_ROW_EXISTS",
                       "post_started_at": row["post_started_at"], "status": row["simulation_status"],
                       "alpha_id": row["alpha_id"]})
    for row in candidate_rows:
        result_ref = str(row["result_reference_json"] or "").strip()
        safe = (
            str(row["created_at"] or "") < started_at
            and str(row["updated_at"] or "") < started_at
            and str(row["lifecycle_state"] or "").upper() == "PLANNED"
            and str(row["simulation_status"] or "NONE").upper() in {"", "NONE"}
            and not row["alpha_id"] and result_ref in {"", "{}", "null"}
            and int(row["new_post_budget_consumed"] or 0) == 0
        )
        if not safe:
            unsafe.append({"sim_key": row["sim_key"], "candidate_id": row["candidate_id"],
                           "reason": "CANDIDATE_REMOTE_OR_POST_STATE_EXISTS"})
    if len(plans) != len(plan_ids):
        unsafe.append({"reason": "REPAIR_PLAN_SCOPE_MISSING", "expected": len(plan_ids), "actual": len(plans)})
    for row in plans:
        if int(row["committed_posts"] or 0) or int(row["consumed_posts"] or 0):
            unsafe.append({"repair_plan_id": row["repair_plan_id"], "reason": "REPAIR_PLAN_POST_CONSUMED",
                           "committed_posts": row["committed_posts"], "consumed_posts": row["consumed_posts"]})
    if repair_edges:
        unsafe.extend({"repair_id": r["repair_id"], "reason": "REPAIR_EDGE_EXISTS"} for r in repair_edges)
    if int(batch.get("logical_posts_consumed") or 0) != 0:
        unsafe.append({"reason": "BATCH_LOGICAL_POST_CONSUMED"})
    if int(rr.get("repair_consumed") or 0) != prior_other_consumed:
        unsafe.append({"reason": "ROUND_REPAIR_CONSUMPTION_NOT_EXPLAINED_BY_OTHER_BATCHES",
                       "round_repair_consumed": rr.get("repair_consumed"),
                       "other_batch_consumed": prior_other_consumed})
    if run_row and int(run_row[0] or 0) != 0:
        unsafe.append({"reason": "RUN_HAS_UNCERTAIN_POSTS", "post_uncertain": int(run_row[0] or 0)})

    log_evidence = _repair_recovery_audit_attempts(Path(project_dir), run_id, sim_keys)
    if log_evidence["matches"]:
        unsafe.extend({**x, "reason": "AUDIT_REMOTE_POST_FACT"} for x in log_evidence["matches"])
    evidence.update({
        "round_event_post_facts": len(event_rows), "live_audit_post_facts": len(live_rows),
        "ledger_rows": len(ledger_rows), "candidate_rows": len(candidate_rows),
        "repair_edges": len(repair_edges), "repair_plan_consumed": sum(int(x["consumed_posts"] or 0) for x in plans),
        "historical_intent_event_count": len(intent_rows),
        "intent_plan_associations": intent_plan_associations,
        "audit_log_files": log_evidence["files_checked"],
        "audit_log_post_facts": len(log_evidence["matches"]),
    })
    if unsafe:
        raise ConfigError("RECOVER_INTERRUPTED_REPAIR_UNSAFE:" + _json(unsafe))

    now = _now()
    report = {
        "action": "REPAIR_POST_INTENT_RECOVERY", "reason": "NEVER_DISPATCHED_HASH_GUARD_BLOCK",
        "run_id": run_id, "round_id": round_id, "batch_no": int(batch_no), "phase": "REPAIR",
        "sim_keys": sim_keys, "plan_ids": plan_ids, "released_count": len(sim_keys),
        "historical_intent_event_count": len(intent_rows),
        "plan_associations": intent_plan_associations, "evidence_checked": evidence,
        "confirmation": "CONFIRM_UNDISPATCHED_REPAIR_TAIL",
        "network_requests": 0, "simulation_posts": 0,
    }
    event_key = hashlib.sha256(
        f"repair_intent_recovery|{round_id}|{int(batch_no)}".encode()
    ).hexdigest()
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_round_batches SET status='RECOVERED_REPAIR_PRE_DISPATCH',
                   planned_post_sim_keys_json='[]',planned_resume_sim_keys_json='[]',
                   projected_new_posts=0,logical_posts_consumed=0,report_json=?,completed_at=?
               WHERE round_id=? AND batch_no=? AND status='RUNNING' AND phase='REPAIR'""",
            (_json(report), now, round_id, int(batch_no)),
        )
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise ConfigError("RECOVER_INTERRUPTED_REPAIR_CONCURRENT_STATE_CHANGE")
        conn.execute(
            """UPDATE ppl_rounds SET status='PAUSED',phase='REPAIR',
                   stop_reason='REPAIR_POST_INTENT_RECOVERED_NEVER_DISPATCHED',updated_at=?
               WHERE round_id=? AND current_batch=?""",
            (now, round_id, int(batch_no)),
        )
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise ConfigError("RECOVER_INTERRUPTED_REPAIR_ROUND_SEQUENCE_CHANGED")
        conn.execute(
            """INSERT INTO ppl_round_events(
                   event_key,round_id,run_id,batch_no,phase,event_type,payload_json,created_at
               ) VALUES (?,?,?,?,?,'REPAIR_POST_INTENT_RECOVERY',?,?)""",
            (event_key, round_id, run_id, int(batch_no), "REPAIR", _json(report), now),
        )
    audit_event(**report)
    return report


def repair_interrupted_batch_ledger_attribution(
    store: Any,
    config: Any,
    alpha_db: Path,
    *,
    run_id: str,
    batch_no: int,
    confirm_ledger_reattribution: bool = False,
) -> Dict[str, Any]:
    """Repair local research-ledger attribution for a known interrupted SEARCH batch.

    This command is intentionally local-only. It never contacts BRAIN and never
    changes Simulation facts. The target batch's durable selected candidates are
    authoritative; only stale UNKNOWN/HISTORICAL ledger rows may be moved from a
    wrong batch number to the target batch. Existing explicit NEW_POST/REPAIR
    attribution in another batch is treated as unsafe and remains fail-closed.
    """
    if not confirm_ledger_reattribution:
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_REQUIRES_EXPLICIT_CONFIRMATION")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    round_id = str(rr["round_id"])
    batch = _batch_by_no(store, round_id, int(batch_no))
    if not batch:
        raise ConfigError(f"ROUND_BATCH_NOT_FOUND:{batch_no}")
    if str(batch.get("phase") or "").upper() != "SEARCH":
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_SEARCH_ONLY")

    selected_ids = _parse_json_list(batch.get("selected_candidate_ids_json"))
    planned_post_keys = _parse_json_list(batch.get("planned_post_sim_keys_json"))
    if not selected_ids or not planned_post_keys:
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_EMPTY_BATCH_SCOPE")
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    selected = [candidates.get(cid) for cid in selected_ids]
    if any(row is None for row in selected):
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_CANDIDATE_MISSING")
    key_to_cid = {str(row.get("sim_key")): str(row.get("candidate_id")) for row in selected if row and row.get("sim_key")}
    if set(planned_post_keys) != set(key_to_cid):
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_SIM_KEY_MISMATCH")

    facts = _alpha_facts(alpha_db, planned_post_keys)
    durable_keys = [
        key for key in planned_post_keys
        if str((facts.get(key) or {}).get("status") or "").upper() in LOGICAL_CONSUMED_STATUSES
    ]
    if not durable_keys:
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_NO_DURABLE_FACTS")

    decision_modes: Dict[str, str] = {}
    with store.connect() as conn:
        decision_rows = conn.execute(
            """SELECT candidate_id,selection_mode FROM ppl_round_candidate_decisions
               WHERE round_id=? AND batch_no=?""",
            (round_id, int(batch_no)),
        ).fetchall()
    for row in decision_rows:
        if row[1]:
            decision_modes[str(row[0])] = str(row[1])

    before_rows: List[Dict[str, Any]] = []
    unsafe: List[Dict[str, Any]] = []
    with store.connect() as conn:
        for key in durable_keys:
            row = conn.execute(
                """SELECT batch_no,phase,candidate_id,origin,selection_mode,post_started_at
                   FROM ppl_round_simulation_ledger WHERE round_id=? AND sim_key=?""",
                (round_id, key),
            ).fetchone()
            if not row:
                continue
            item = {
                "sim_key": key, "candidate_id": str(row[2] or ""),
                "batch_no": row[0], "phase": row[1], "origin": row[3],
                "selection_mode": row[4], "post_started_at": row[5],
            }
            before_rows.append(item)
            expected_cid = key_to_cid[key]
            if item["candidate_id"] and item["candidate_id"] != expected_cid:
                unsafe.append({**item, "reason": "CANDIDATE_ID_MISMATCH", "expected_candidate_id": expected_cid})
                continue
            if item["batch_no"] not in {None, int(batch_no)} and str(item["origin"] or "").upper() not in {"", "UNKNOWN", "HISTORICAL"}:
                unsafe.append({**item, "reason": "EXPLICIT_OTHER_BATCH_ATTRIBUTION"})
    if unsafe:
        raise ConfigError("REPAIR_LEDGER_ATTRIBUTION_UNSAFE:" + _json(unsafe))

    corrected = 0
    inserted = 0
    origin_map = {key_to_cid[key]: "NEW_POST" for key in durable_keys}
    mode_map = {cid: decision_modes.get(cid, "RECOVERED_BATCH") for cid in origin_map}

    # Insert any missing ledger rows using the authoritative target-batch scope.
    existing_keys = {str(x.get("sim_key")) for x in before_rows}
    missing_cids = [key_to_cid[key] for key in durable_keys if key not in existing_keys]
    if missing_cids:
        inserted = sync_simulation_ledger(
            store, alpha_db, round_id, run_id, batch_no=int(batch_no), phase="SEARCH",
            candidate_ids=missing_cids,
            origin_by_candidate={cid: "NEW_POST" for cid in missing_cids},
            selection_mode_by_candidate={cid: mode_map.get(cid, "RECOVERED_BATCH") for cid in missing_cids},
        )

    now = _now()
    with store.connect() as conn:
        for key in durable_keys:
            cid = key_to_cid[key]
            fact = facts.get(key) or {}
            submitted_at = fact.get("submitted_at") or fact.get("date_created")
            current = conn.execute(
                """SELECT batch_no,phase,origin,selection_mode FROM ppl_round_simulation_ledger
                   WHERE round_id=? AND sim_key=?""",
                (round_id, key),
            ).fetchone()
            if not current:
                raise ConfigError(f"REPAIR_LEDGER_ATTRIBUTION_ROW_MISSING_AFTER_SYNC:{key}")
            needs_change = (
                current[0] != int(batch_no)
                or str(current[1] or "").upper() != "SEARCH"
                or str(current[2] or "").upper() != "NEW_POST"
                or (not current[3] and mode_map.get(cid))
            )
            if needs_change:
                conn.execute(
                    """UPDATE ppl_round_simulation_ledger
                       SET batch_no=?,phase='SEARCH',candidate_id=?,origin='NEW_POST',
                           selection_mode=?,post_started_at=coalesce(post_started_at,?),updated_at=?
                       WHERE round_id=? AND sim_key=?""",
                    (int(batch_no), cid, mode_map.get(cid, "RECOVERED_BATCH"), submitted_at, now, round_id, key),
                )
                corrected += 1

    # Refresh mutable metrics/classification in-place after attribution repair.
    near = classify_run(store, config, alpha_db, run_id)
    class_map = {str(x.get("candidate_id")): str(x.get("evidence_label") or x.get("classification"))
                 for x in near if x.get("candidate_id")}
    sync_simulation_ledger(
        store, alpha_db, round_id, run_id, batch_no=int(batch_no), phase="SEARCH",
        candidate_ids=list(origin_map), origin_by_candidate=origin_map,
        selection_mode_by_candidate=mode_map, classification_by_candidate=class_map,
    )
    reconciled = _reconcile_round_accounting(
        store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True
    )
    record_event(
        store, round_id, run_id, "INTERRUPTED_BATCH_LEDGER_ATTRIBUTION_REPAIRED",
        batch_no=int(batch_no), phase="SEARCH",
        payload={
            "durable_candidates": len(origin_map), "corrected_rows": corrected,
            "inserted_rows": inserted, "candidate_ids": list(origin_map),
            "network_requests": 0, "simulation_posts": 0, "check_requests": 0,
        },
    )
    sync_durable_events(store, round_id, run_id)
    return {
        "project_version": "v3.0.4o",
        "action": "REPAIR_INTERRUPTED_BATCH_LEDGER_ATTRIBUTION",
        "run_id": run_id, "round_id": round_id, "batch_no": int(batch_no),
        "durable_candidates": len(origin_map), "corrected_rows": corrected,
        "inserted_rows": inserted, "candidate_ids": list(origin_map),
        "selection_modes": mode_map, "before_rows": before_rows,
        "reconciled": reconciled,
        "network_requests": 0, "simulation_posts": 0, "check_requests": 0,
        "property_writes": 0, "submit_requests": 0,
    }


def _finalize_recovered_search_batch(
    store: Any,
    config: Any,
    machine: Any,
    session: Any,
    alpha_db: Path,
    run_id: str,
    round_id: str,
    batch: Mapping[str, Any],
    policy: Mapping[str, Any],
    project_dir: Path,
) -> Dict[str, Any]:
    """Continue one recovered SEARCH batch without regressing Continuous semantics.

    Legacy V3 keeps the historical behavior: a recovered batch is finalized only
    after every remote Simulation is terminal.  V3.1 Continuous instead reuses
    the durable non-blocking handoff path, scopes UNCERTAIN to the affected
    candidate, and may close the batch after all *dispatch intent* has been
    durably handed off.  A NEW_POST tail that cannot fit in the currently free
    server slots remains on the recovered batch and is retried later; it is never
    silently dropped and never reclassified as completed work.
    """
    batch_no = int(batch.get("batch_no") or 0)
    if str(batch.get("phase") or "").upper() != "SEARCH":
        raise ConfigError(f"ROUND_RECOVERED_NON_SEARCH_BATCH_REQUIRES_MANUAL_REVIEW:{batch_no}")
    candidate_ids = _parse_json_list(batch.get("selected_candidate_ids_json"))
    candidates = {str(x["candidate_id"]): dict(x) for x in store.load_candidates(run_id)}
    missing = [cid for cid in candidate_ids if cid not in candidates]
    if missing:
        raise ConfigError("ROUND_RECOVERED_BATCH_CANDIDATE_MISSING:" + _json(missing))
    rows = [candidates[cid] for cid in candidate_ids]
    rr = get_round(store, round_id=round_id) or {}
    continuous = parse_continuous_policy(policy)
    search_remaining = phase_capacity(policy, rr, "SEARCH").capacity
    repair_remaining = phase_capacity(policy, rr, "REPAIR").capacity

    execution_rows = list(rows)
    slot_deferred_candidate_ids: List[str] = []
    if continuous.enabled and continuous.poll_remote_without_blocking_worker:
        # Rebuild remote slot truth before any recovered NEW_POST intent can be
        # dispatched.  Existing saved URLs and UNCERTAIN identities reserve
        # capacity, exactly as they do on the normal Continuous path.
        sync_remote_work_from_durable_facts(store, alpha_db, run_id, force_due_existing=False)
        slot_limit = int(config.plan["runtime"].get(
            "glb_max_concurrency"
            if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
            else "other_max_concurrency",
            config.plan["runtime"].get("concurrency", 1),
        ))
        slots = remote_slot_snapshot(store, run_id, slot_limit)
        free_new_posts = int(slots.free_slots)
        selected: List[Mapping[str, Any]] = []
        selected_new_posts = 0
        for row in rows:
            action = str(classify_cache_read_only(alpha_db, str(row.get("sim_key") or "")).get("execution_action") or "")
            if requires_extension_new_post(action):
                if selected_new_posts >= free_new_posts:
                    slot_deferred_candidate_ids.append(str(row.get("candidate_id") or ""))
                    continue
                selected_new_posts += 1
            selected.append(row)
        execution_rows = selected

    result = _execute_search_rows(
        store, config, machine, session, alpha_db, run_id, execution_rows,
        allow_simulation_post=True,
        remaining_search_budget=max(0, int(search_remaining)),
        round_id=round_id,
        batch_no=batch_no,
        nonblocking_remote=bool(continuous.enabled and continuous.poll_remote_without_blocking_worker),
        global_hold_on_uncertain=not bool(continuous.enabled and continuous.recoverable_failures_wait),
    )

    if slot_deferred_candidate_ids:
        record_event(
            store, round_id, run_id, "RECOVERED_BATCH_SERVER_SLOT_DEFERRED",
            batch_no=batch_no, phase="SEARCH",
            payload={
                "candidate_ids": sorted(x for x in slot_deferred_candidate_ids if x),
                "reason": "WAIT_SERVER_SLOT",
                "simulation_posts": int(result.get("post_attempted") or 0),
                "batch_finalized": False,
            },
        )
        return {
            "finalized": False,
            "batch_no": batch_no,
            "reason": "WAIT_SERVER_SLOT",
            "slot_deferred_candidate_ids": sorted(x for x in slot_deferred_candidate_ids if x),
            "nonterminal_candidate_ids": list(result.get("nonterminal_candidate_ids") or []),
        }

    facts = _alpha_facts(alpha_db, [str(r.get("sim_key")) for r in rows if r.get("sim_key")])
    nonterminal = [
        str(r.get("candidate_id")) for r in rows
        if str((facts.get(str(r.get("sim_key"))) or {}).get("status") or "").upper()
        in {"RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"}
    ]
    if nonterminal and not (continuous.enabled and continuous.poll_remote_without_blocking_worker):
        record_event(
            store, round_id, run_id, "RECOVERED_BATCH_STILL_NONTERMINAL",
            batch_no=batch_no, phase="SEARCH",
            payload={"candidate_ids": nonterminal},
        )
        return {"finalized": False, "batch_no": batch_no, "nonterminal_candidate_ids": nonterminal}

    complete_ids = [
        str(r.get("candidate_id")) for r in rows
        if str((facts.get(str(r.get("sim_key"))) or {}).get("status") or "").upper() == "COMPLETE"
    ]
    needs_analysis = _candidate_ids_without_diagnosis(store, run_id, complete_ids)
    if continuous.enabled and continuous.check_queue_enabled:
        analyzed = _continuous_analyze_and_enqueue_checks(
            store, config, alpha_db, run_id, needs_analysis, repair_remaining
        )
    else:
        analyzed = _analyze_and_check(
            store, config, machine, session, alpha_db, run_id, needs_analysis, repair_remaining
        )
    derive_check_repair_proposals(store, config, alpha_db, run_id, persist=True)
    winners = finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
    fresh_check_cids = [
        str(x.get("candidate_id")) for x in analyzed.get("checks", [])
        if x.get("executed") and x.get("candidate_id")
    ]
    manual_refresh = _maybe_auto_refresh_manual_finalization(
        store, config, machine, session, alpha_db, run_id, round_id, policy, project_dir,
        batch_no=batch_no, fresh_candidate_ids=fresh_check_cids,
    )
    total_check_count = int(analyzed.get("check_count") or 0) + int(manual_refresh.get("executed_check_count") or 0)
    logical_consumed = max(
        int(batch.get("logical_posts_consumed") or 0),
        int(result.get("post_consumed") or 0),
    )
    completion_semantics = (
        "REMOTE_HANDOFF_COMPLETE"
        if continuous.enabled and continuous.poll_remote_without_blocking_worker
        else "SIMULATION_WORKFLOW_COMPLETE"
    )
    report = {
        "phase": "SEARCH",
        "recovery_finalization": True,
        "completion_semantics": completion_semantics,
        "remote_nonterminal_candidate_ids": nonterminal,
        "execution": result,
        "analysis": analyzed,
        "manual_finalization_refresh": manual_refresh,
        "winners": winners,
    }
    finish_batch(
        store, round_id, batch_no, report,
        logical_posts_consumed=logical_consumed,
        cache_hits=int(result.get("cache_hits") or 0),
        resume_count=int(result.get("resume_count") or 0),
        check_count=total_check_count,
    )
    record_event(
        store, round_id, run_id, "BATCH_RECOVERY_FINALIZED",
        batch_no=batch_no, phase="SEARCH",
        payload={
            "logical_posts_consumed": logical_consumed,
            "cache_hits": int(result.get("cache_hits") or 0),
            "resume_count": int(result.get("resume_count") or 0),
            "check_count": total_check_count,
            "newly_analyzed": len(needs_analysis),
            "completion_semantics": completion_semantics,
            "remote_nonterminal_count": len(nonterminal),
        },
    )
    original_posts = set(_parse_json_list(batch.get("planned_post_sim_keys_json")))
    origin_map = {
        str(r.get("candidate_id")): ("NEW_POST" if str(r.get("sim_key")) in original_posts else "RESUME")
        for r in rows
    }
    mode_map = {str(r.get("candidate_id")): "RECOVERED_BATCH" for r in rows}
    _sync_research_telemetry(
        store, config, alpha_db, run_id, round_id,
        batch_no=batch_no, phase="SEARCH",
        origin_by_candidate=origin_map, selection_mode_by_candidate=mode_map, policy=policy,
    )
    reconciled = _reconcile_round_accounting(
        store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True,
        enforce_budget_limits=continuous.global_budget_enforced,
    )
    _write_reports_resilient(
        store, config, alpha_db, run_id, round_id, policy, project_dir,
        continuous_enabled=continuous.enabled,
        retry_seconds=max(30.0, float(continuous.idle_wait_seconds)),
    )
    return {
        "finalized": True,
        "batch_no": batch_no,
        "logical_posts_consumed": logical_consumed,
        "newly_analyzed": len(needs_analysis),
        "remote_nonterminal_candidate_ids": nonterminal,
        "completion_semantics": completion_semantics,
        "reconciled": reconciled,
    }


def _finalize_recovered_repair_batch(
    store: Any,
    config: Any,
    machine: Any,
    session: Any,
    alpha_db: Path,
    run_id: str,
    round_id: str,
    batch: Mapping[str, Any],
    policy: Mapping[str, Any],
    project_dir: Path,
) -> Dict[str, Any]:
    """Finalize an interrupted REPAIR batch from durable local facts only.

    Legacy V3 waits for terminal children before closing the recovered batch.
    V3.1 Continuous may close the batch after durable remote handoff: RUNNING,
    SUBMITTED and UNCERTAIN children remain owned by the Remote Queue and are
    reconciled later, while this recovery tail performs zero Simulation POSTs.
    """
    batch_no = int(batch.get("batch_no") or 0)
    if str(batch.get("phase") or "").upper() != "REPAIR":
        raise ConfigError(f"ROUND_RECOVERED_NON_REPAIR_BATCH:{batch_no}")
    continuous = parse_continuous_policy(policy)
    plan_ids = _parse_json_list(batch.get("selected_plan_ids_json"))
    if not plan_ids:
        raise ConfigError(f"ROUND_RECOVERED_REPAIR_PLAN_IDS_MISSING:{batch_no}")
    marks = ",".join("?" for _ in plan_ids)
    with store.connect() as conn:
        plan_rows = conn.execute(
            f"""SELECT p.repair_plan_id,p.repair_signature,r.child_candidate_id
                FROM ppl_repair_plans p
                LEFT JOIN ppl_repairs r
                  ON r.run_id=p.run_id AND r.repair_signature=p.repair_signature
                WHERE p.run_id=? AND p.repair_plan_id IN ({marks})""",
            [run_id] + plan_ids,
        ).fetchall()
    if len({str(r["repair_plan_id"]) for r in plan_rows}) != len(set(plan_ids)):
        raise ConfigError(f"ROUND_RECOVERED_REPAIR_PLAN_LINEAGE_INCOMPLETE:{batch_no}")
    plan_by_child: Dict[str, str] = {}
    child_ids: List[str] = []
    for row in plan_rows:
        cid = str(row["child_candidate_id"] or "")
        if not cid:
            continue
        if cid not in child_ids:
            child_ids.append(cid)
        plan_by_child.setdefault(cid, str(row["repair_plan_id"]))
    if not child_ids:
        # No durable child identity means there is no proof that the selected
        # Repair intent ever crossed the dispatch boundary.  Keep fail-closed;
        # Continuous must not invent a child or silently consume the batch.
        raise ConfigError(f"ROUND_RECOVERED_REPAIR_CHILDREN_MISSING:{batch_no}")
    candidates = {str(x["candidate_id"]): dict(x) for x in store.load_candidates(run_id)}
    missing = [cid for cid in child_ids if cid not in candidates]
    if missing:
        raise ConfigError("ROUND_RECOVERED_REPAIR_CANDIDATE_MISSING:" + _json(missing))
    rows = [candidates[cid] for cid in child_ids]
    facts = _alpha_facts(alpha_db, [str(r.get("sim_key")) for r in rows if r.get("sim_key")])
    nonterminal = [
        str(r.get("candidate_id")) for r in rows
        if str((facts.get(str(r.get("sim_key"))) or {}).get("status") or "").upper()
        in {"RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"}
    ]
    if nonterminal and not (continuous.enabled and continuous.poll_remote_without_blocking_worker):
        return {"finalized": False, "batch_no": batch_no, "nonterminal_candidate_ids": nonterminal}
    if continuous.enabled and continuous.poll_remote_without_blocking_worker:
        sync_remote_work_from_durable_facts(store, alpha_db, run_id, force_due_existing=False)

    winners = finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
    started_at = str(batch.get("started_at") or "")
    fresh_check_cids: List[str] = []
    check_count = 0
    if child_ids:
        cmarks = ",".join("?" for _ in child_ids)
        with store.connect() as conn:
            if started_at:
                check_rows = conn.execute(
                    f"""SELECT candidate_id,COUNT(*) AS n
                        FROM ppl_check_sessions
                        WHERE run_id=? AND candidate_id IN ({cmarks}) AND created_at>=?
                        GROUP BY candidate_id""",
                    [run_id] + child_ids + [started_at],
                ).fetchall()
            else:
                check_rows = []
        fresh_check_cids = [str(r["candidate_id"]) for r in check_rows if r["candidate_id"]]
        check_count = sum(int(r["n"] or 0) for r in check_rows)
    manual_refresh = _maybe_auto_refresh_manual_finalization(
        store, config, machine, session, alpha_db, run_id, round_id, policy, project_dir,
        batch_no=batch_no, fresh_candidate_ids=fresh_check_cids,
    )
    total_check_count = check_count + int(manual_refresh.get("executed_check_count") or 0)
    logical_consumed = int(batch.get("logical_posts_consumed") or 0)
    completion_semantics = (
        "REMOTE_HANDOFF_COMPLETE"
        if continuous.enabled and continuous.poll_remote_without_blocking_worker
        else "SIMULATION_WORKFLOW_COMPLETE"
    )
    report = {
        "phase": "REPAIR",
        "recovery_finalization": True,
        "completion_semantics": completion_semantics,
        "selected_plan_ids": plan_ids,
        "child_candidate_ids": child_ids,
        "remote_nonterminal_candidate_ids": nonterminal,
        "manual_finalization_refresh": manual_refresh,
        "winners": winners,
    }
    finish_batch(
        store, round_id, batch_no, report,
        logical_posts_consumed=logical_consumed,
        cache_hits=0, resume_count=0, check_count=total_check_count,
    )
    planned_post = set(_parse_json_list(batch.get("planned_post_sim_keys_json")))
    planned_resume = set(_parse_json_list(batch.get("planned_resume_sim_keys_json")))
    origin_map: Dict[str, str] = {}
    mode_map: Dict[str, str] = {}
    for row in rows:
        cid = str(row.get("candidate_id"))
        sk = str(row.get("sim_key") or "")
        origin_map[cid] = "NEW_POST" if sk in planned_post else "RESUME" if sk in planned_resume else "CACHE"
        mode_map[cid] = "REPAIR_RECOVERY"
    _sync_research_telemetry(
        store, config, alpha_db, run_id, round_id,
        batch_no=batch_no, phase="REPAIR", candidate_ids=sorted(origin_map),
        origin_by_candidate=origin_map, selection_mode_by_candidate=mode_map,
        repair_plan_by_candidate=plan_by_child, policy=policy,
    )
    record_event(
        store, round_id, run_id, "BATCH_RECOVERY_FINALIZED",
        batch_no=batch_no, phase="REPAIR",
        payload={
            "logical_posts_consumed": logical_consumed,
            "child_candidate_ids": child_ids,
            "check_count": total_check_count,
            "simulation_posts": 0,
            "completion_semantics": completion_semantics,
            "remote_nonterminal_count": len(nonterminal),
        },
    )
    reconciled = _reconcile_round_accounting(
        store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True,
        enforce_budget_limits=continuous.global_budget_enforced,
    )
    _write_reports_resilient(
        store, config, alpha_db, run_id, round_id, policy, project_dir,
        continuous_enabled=continuous.enabled,
        retry_seconds=max(30.0, float(continuous.idle_wait_seconds)),
    )
    return {
        "finalized": True,
        "batch_no": batch_no,
        "logical_posts_consumed": logical_consumed,
        "child_candidate_ids": child_ids,
        "remote_nonterminal_candidate_ids": nonterminal,
        "completion_semantics": completion_semantics,
        "reconciled": reconciled,
    }

def _release_batch_undispatched_keys(
    store: Any,
    *,
    run_id: str,
    round_id: str,
    batch_no: int,
    sim_keys: Sequence[str],
    release_reason: str,
    event_type: str,
    reopen_batch: bool = False,
) -> Dict[str, Any]:
    """Release positively-known never-POSTed keys without weakening fail-closed recovery.

    The batch POST-intent scope is shrunk together with the candidate lifecycle.
    This is critical: leaving the same sim_key on the old batch and later POSTing
    it from a new batch would make round reconciliation count one durable fact in
    two batches.  Original SELECTED decision rows are intentionally preserved as
    audit history; effective execution scope lives on ``ppl_round_batches``.
    """
    keys = sorted({str(x) for x in sim_keys if x})
    if not keys:
        return {"released_candidate_ids": [], "released_sim_keys": [], "remaining_post_sim_keys": []}
    batch = _batch_by_no(store, round_id, int(batch_no))
    if not batch:
        raise ConfigError(f"ROUND_BATCH_NOT_FOUND:{batch_no}")
    if str(batch.get("phase") or "").upper() != "SEARCH":
        raise ConfigError("ROUND_UNDISPATCHED_RELEASE_SEARCH_ONLY")
    planned_post = _parse_json_list(batch.get("planned_post_sim_keys_json"))
    if not set(keys).issubset(set(planned_post)):
        raise ConfigError("ROUND_UNDISPATCHED_RELEASE_OUTSIDE_POST_INTENT:" + _json(keys))

    candidates = {str(x.get("sim_key") or ""): dict(x) for x in store.load_candidates(run_id) if x.get("sim_key")}
    unsafe = []
    released_rows: List[Dict[str, Any]] = []
    for key in keys:
        row = candidates.get(key)
        if not row:
            unsafe.append({"sim_key": key, "reason": "CANDIDATE_MISSING"})
            continue
        sim_status = str(row.get("simulation_status") or "NONE").upper()
        lifecycle = str(row.get("lifecycle_state") or "").upper()
        if row.get("alpha_id") or sim_status not in {"", "NONE"} or lifecycle not in {"SIMULATION_PENDING", "PLANNED"}:
            unsafe.append({
                "candidate_id": row.get("candidate_id"), "sim_key": key,
                "simulation_status": sim_status, "lifecycle_state": lifecycle,
                "alpha_id": row.get("alpha_id"),
            })
            continue
        released_rows.append(row)
    if unsafe:
        raise ConfigError("ROUND_UNDISPATCHED_RELEASE_UNSAFE:" + _json(unsafe))

    released_ids = {str(r.get("candidate_id")) for r in released_rows}
    selected_ids = _parse_json_list(batch.get("selected_candidate_ids_json"))
    remaining_selected = [cid for cid in selected_ids if cid not in released_ids]
    remaining_post = [key for key in planned_post if key not in set(keys)]
    transitions: List[Tuple[str, str, str]] = []
    now = _now()
    with store.connect() as conn:
        status_sql = ",status='RECOVERED',completed_at=NULL" if reopen_batch else ""
        conn.execute(
            f"""UPDATE ppl_round_batches
                   SET selected_candidate_ids_json=?,planned_post_sim_keys_json=?,projected_new_posts=?{status_sql}
                   WHERE round_id=? AND batch_no=?""",
            (_json(remaining_selected), _json(remaining_post), len(remaining_post), round_id, int(batch_no)),
        )
        # If an older completed build already mirrored the deferred rows into
        # telemetry, remove only those positively-known no-POST ledger records.
        marks = ",".join("?" for _ in keys)
        conn.execute(
            f"DELETE FROM ppl_round_simulation_ledger WHERE round_id=? AND sim_key IN ({marks})",
            [round_id] + keys,
        )
        conn.execute(
            "DELETE FROM ppl_round_snapshots WHERE round_id=? AND batch_no=? AND snapshot_type='BATCH_END'",
            (round_id, int(batch_no)),
        )
        for row in released_rows:
            cid = str(row["candidate_id"])
            old = str(row.get("lifecycle_state") or "")
            conn.execute(
                """UPDATE ppl_candidates
                   SET lifecycle_state='PLANNED',selected_for_initial_search=0,selection_rank=NULL,
                       selection_reason=?,execution_action='NEW_SIMULATION_REQUIRED',updated_at=?
                   WHERE run_id=? AND candidate_id=?""",
                (release_reason, now, run_id, cid),
            )
            if old != "PLANNED":
                conn.execute(
                    """INSERT INTO ppl_state_transitions(
                           run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                       ) VALUES (?,?,'CANDIDATE',?,'PLANNED',?,?,?,?)""",
                    (run_id, cid, old, release_reason, ROUND_SOURCE,
                     _json({"round_id": round_id, "batch_no": int(batch_no), "sim_key": row.get("sim_key")}), now),
                )
                transitions.append((cid, old, "PLANNED"))
    for cid, old, new in transitions:
        audit_state_transition("CANDIDATE", cid, run_id=run_id, old_state=old, new_state=new,
                               reason=release_reason, source=ROUND_SOURCE)
    record_event(
        store, round_id, run_id, event_type, batch_no=int(batch_no), phase="SEARCH",
        payload={
            "released_undispatched": len(released_rows),
            "released_candidate_ids": sorted(released_ids),
            "released_sim_keys": keys,
            "remaining_post_intent": len(remaining_post),
            "reopened_batch": bool(reopen_batch),
        },
    )
    return {
        "released_candidate_ids": sorted(released_ids),
        "released_sim_keys": keys,
        "remaining_post_sim_keys": remaining_post,
        "effective_selected_candidate_ids": remaining_selected,
    }


def _shrink_repair_batch_deferred_intent(
    store: Any,
    *,
    run_id: str,
    round_id: str,
    batch_no: int,
    deferred_sim_keys: Sequence[str],
    deferred_plan_ids: Sequence[str],
) -> Dict[str, Any]:
    """Remove positively-known never-POSTed Repair work from one V3 batch intent."""
    keys = sorted({str(x) for x in deferred_sim_keys if x})
    plans = sorted({str(x) for x in deferred_plan_ids if x})
    if not keys and not plans:
        return {"released_sim_keys": [], "released_plan_ids": []}
    batch = _batch_by_no(store, round_id, int(batch_no))
    if not batch or str(batch.get("phase") or "").upper() != "REPAIR":
        raise ConfigError("ROUND_REPAIR_DEFERRED_BATCH_NOT_FOUND")
    planned_post = _parse_json_list(batch.get("planned_post_sim_keys_json"))
    selected_plans = _parse_json_list(batch.get("selected_plan_ids_json"))
    if not set(keys).issubset(set(planned_post)):
        raise ConfigError("ROUND_REPAIR_DEFERRED_OUTSIDE_POST_INTENT:" + _json(keys))
    if plans and not set(plans).issubset(set(selected_plans)):
        raise ConfigError("ROUND_REPAIR_DEFERRED_PLAN_SCOPE_MISMATCH:" + _json(plans))
    remaining_post = [k for k in planned_post if k not in set(keys)]
    remaining_plans = [pid for pid in selected_plans if pid not in set(plans)]
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_round_batches
               SET planned_post_sim_keys_json=?,selected_plan_ids_json=?,projected_new_posts=?
               WHERE round_id=? AND batch_no=?""",
            (_json(remaining_post), _json(remaining_plans), len(remaining_post), round_id, int(batch_no)),
        )
        if keys:
            marks = ",".join("?" for _ in keys)
            conn.execute(
                f"DELETE FROM ppl_round_simulation_ledger WHERE round_id=? AND sim_key IN ({marks})",
                [round_id] + keys,
            )
        conn.execute(
            "DELETE FROM ppl_round_snapshots WHERE round_id=? AND batch_no=? AND snapshot_type='BATCH_END'",
            (round_id, int(batch_no)),
        )
    record_event(
        store, round_id, run_id, "REPAIR_SERVER_SLOT_DEFERRED_INTENT_RELEASED",
        batch_no=int(batch_no), phase="REPAIR",
        payload={"released_sim_keys": keys, "released_plan_ids": plans,
                 "remaining_post_intent": len(remaining_post)},
    )
    return {"released_sim_keys": keys, "released_plan_ids": plans,
            "remaining_post_sim_keys": remaining_post, "remaining_plan_ids": remaining_plans}


def _execute_search_rows(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                         run_id: str, rows: Sequence[Mapping[str, Any]], *, allow_simulation_post: bool,
                         remaining_search_budget: int,
                         round_id: Optional[str] = None, batch_no: Optional[int] = None,
                         extension_new_post_cap: Optional[int] = None,
                         nonblocking_remote: bool = False,
                         global_hold_on_uncertain: bool = True) -> Dict[str, Any]:
    if not rows:
        return {"post_attempted": 0, "post_consumed": 0, "cache_hits": 0, "resume_count": 0,
                "complete_candidate_ids": [], "http_audit": [], "results": []}
    if not allow_simulation_post:
        projected = sum(1 for row in rows if classify_cache_read_only(alpha_db, str(row["sim_key"]))["execution_action"] not in {"CACHE_RESTORE", "RESUME_EXISTING"})
        return {"preview": True, "projected_new_posts": projected, "post_attempted": 0, "post_consumed": 0,
                "cache_hits": 0, "resume_count": 0, "complete_candidate_ids": [], "http_audit": [], "results": []}
    wrappers = []
    actual_post_keys = set()
    resume_keys = set()
    cache_hits = 0
    rows_by_key = {str(r["sim_key"]): dict(r) for r in rows}
    # Final TOCTOU cache classification immediately before V2.1.
    for row in rows:
        key = str(row["sim_key"])
        cache = classify_cache_read_only(alpha_db, key)
        action = str(cache["execution_action"])
        record = cache.get("record") or {}
        if action == "HOLD_UNCERTAIN":
            if global_hold_on_uncertain:
                raise ConfigError(f"ROUND_UNCERTAIN_SUBMISSION_HOLD:{key}")
            if round_id is not None:
                record_event(
                    store, round_id, run_id, "SEARCH_UNCERTAIN_QUARANTINED", batch_no=batch_no, phase="SEARCH",
                    candidate_id=row.get("candidate_id"), family_id_value=family_id(row), sim_key=key,
                    payload={"execution_action": action, "global_hold": False, "simulation_posts": 0},
                )
            continue
        if action == "CACHE_RESTORE":
            cache_hits += 1
            _sync_candidate_fact(store, row["candidate_id"], {**record, "sim_key": key}, source=ROUND_SOURCE + "_CACHE")
            if round_id is not None:
                record_event(
                    store, round_id, run_id, "CACHE_HIT", batch_no=batch_no, phase="SEARCH",
                    candidate_id=row.get("candidate_id"), alpha_id=record.get("alpha_id"),
                    family_id_value=family_id(row), sim_key=key,
                    payload={"cache_classification": cache.get("cache_classification"), "status": record.get("status")},
                )
            continue
        if action == "RESUME_EXISTING":
            resume_keys.add(key)
        elif requires_extension_new_post(action):
            if len(actual_post_keys) >= remaining_search_budget:
                raise ConfigError("ROUND_SEARCH_BUDGET_EXCEEDED_AFTER_TOCTOU")
            actual_post_keys.add(key)
        else:
            # Explicit hold/stop actions have durable cache semantics but no
            # remote POST path.  Do not wrap them for V2.1.
            if round_id is not None:
                record_event(
                    store, round_id, run_id, "SEARCH_NON_POST_ACTION", batch_no=batch_no, phase="SEARCH",
                    candidate_id=row.get("candidate_id"), family_id_value=family_id(row), sim_key=key,
                    payload={"execution_action": action, "cache_classification": cache.get("cache_classification")},
                )
            continue
        wrappers.append({"execution_action": action, "v21_candidate": _v21_candidate(row, config.target_mode)})
    if len(actual_post_keys) > remaining_search_budget:
        raise ConfigError("ROUND_SEARCH_BUDGET_EXCEEDED")
    effective_extension_cap = (
        int(extension_new_post_cap)
        if extension_new_post_cap is not None else _extension_batch_cap(max(1, len(rows)))
    )
    actual_extension_posts = {
        key for key in actual_post_keys
        if (rows_by_key.get(key) or {}).get("extension_source")
    }
    if len(actual_extension_posts) > effective_extension_cap:
        raise ConfigError("ROUND_EXTENSION_NEW_POST_CAP_EXCEEDED_AFTER_TOCTOU")
    methods: List[Dict[str, Any]] = []
    frame: Any = []
    if wrappers:
        # Durable network-intent checkpoint.  Nothing below may create a new
        # Simulation unless its sim_key is already recorded on the batch.
        if round_id is not None and batch_no is not None:
            existing_batch = _batch_by_no(store, round_id, int(batch_no)) or {}
            durable_post_intent = set(_parse_json_list(existing_batch.get("planned_post_sim_keys_json")))
            durable_resume_intent = set(_parse_json_list(existing_batch.get("planned_resume_sim_keys_json")))
            # A retry/resume must never erase the original POST-intent evidence.
            # Existing NEW_POST keys can classify as RESUME_EXISTING on a later
            # invocation; replacing the list here would make consumed budget
            # disappear during the next reconciliation.
            set_batch_intent(
                store, round_id, int(batch_no),
                planned_post_sim_keys=durable_post_intent | actual_post_keys,
                planned_resume_sim_keys=durable_resume_intent | resume_keys,
            )
            for key in sorted(actual_post_keys):
                target = rows_by_key[key]
                record_event(
                    store, round_id, run_id, "SIMULATION_POST_INTENT", batch_no=int(batch_no), phase="SEARCH",
                    candidate_id=target.get("candidate_id"), family_id_value=family_id(target), sim_key=key,
                    payload={"execution_action": "NEW_SIMULATION_REQUIRED", "remaining_search_budget": remaining_search_budget},
                )
            for key in sorted(resume_keys):
                target = rows_by_key[key]
                record_event(
                    store, round_id, run_id, "RESUME_EXISTING_INTENT", batch_no=int(batch_no), phase="SEARCH",
                    candidate_id=target.get("candidate_id"), family_id_value=family_id(target), sim_key=key,
                    payload={"execution_action": "RESUME_EXISTING"},
                )
        for row in rows:
            key = str(row["sim_key"])
            if key in actual_post_keys | resume_keys and str(row.get("lifecycle_state") or "") == "PLANNED":
                store.transition_candidate(row["candidate_id"], "SIMULATION_PENDING", reason="V3 search batch scheduled", source=ROUND_SOURCE, allowed=CANDIDATE_TRANSITIONS)
        by_key = {k: rows_by_key[k]["candidate_id"] for k in actual_post_keys | resume_keys}
        if nonblocking_remote:
            handoff = execute_continuous_remote_handoff(
                store, config, machine, session, alpha_db, run_id, wrappers, by_key,
                allow_simulation_post=True,
            )
            methods = list(handoff.get("http_audit") or [])
            frame = list(handoff.get("results") or [])
        else:
            with _instrument_v21(machine, store, by_key, run_id=run_id,
                                 allow_simulation_delete=True) as methods:
                frame = execute_with_v21(
                    wrappers, config, machine, session=session, cache_db=str(alpha_db),
                    allow_simulation_post=True, remaining_initial_budget=max(1, remaining_search_budget),
                )
    deferred_keys = server_slot_deferred_sim_keys(frame, actual_post_keys)
    deferred_release = {"released_candidate_ids": [], "released_sim_keys": [], "remaining_post_sim_keys": []}
    if deferred_keys:
        if round_id is None or batch_no is None:
            raise RuntimeError("ROUND_SERVER_SLOT_DEFERRED_WITHOUT_BATCH_SCOPE")
        deferred_release = _release_batch_undispatched_keys(
            store, run_id=run_id, round_id=round_id, batch_no=int(batch_no), sim_keys=deferred_keys,
            release_reason="V3_SERVER_SLOT_DEFERRED_RELEASED",
            event_type="SERVER_SLOT_DEFERRED_TAIL_RELEASED",
            reopen_batch=False,
        )
        # These keys are positively known never to have reached POST dispatch.
        # Remove them from this call's effective POST scope as well as the
        # durable batch intent updated above.
        actual_post_keys.difference_update(deferred_keys)
    facts = _alpha_facts(alpha_db, rows_by_key)
    for key, row in rows_by_key.items():
        fact = facts.get(key)
        if fact:
            _sync_candidate_fact(store, row["candidate_id"], fact, source=ROUND_SOURCE + "_RECONCILE")
            if str(fact.get("status") or "").upper() == "REMOTE_NOT_FOUND":
                try:
                    resolution = json.loads(str(fact.get("error") or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    resolution = {}
                _quarantine_remote_missing_resume(
                    store, run_id, row, round_id=round_id,
                    http_status=int(fact.get("last_http_status") or 404),
                    simulation_url=str(fact.get("simulation_url") or ""),
                    trigger_source=str(resolution.get("trigger_source") or "AUTO_TIMEOUT"),
                    resolution_reason=str(resolution.get("resolution_reason") or "REMOTE_ALREADY_ABSENT"),
                    resolution_metadata=resolution,
                )
    attempted = sum(1 for x in methods if x.get("method") == "POST" and str(x.get("url") or "").rstrip("/").endswith("/simulations"))
    confirmed = sum(_durable_confirmed_post(facts.get(key) or {}) for key in actual_post_keys)
    uncertain = sum(str((facts.get(key) or {}).get("status") or "").upper() == "UNCERTAIN_SUBMISSION" for key in actual_post_keys)
    consumed = confirmed + uncertain
    if consumed > remaining_search_budget:
        raise RuntimeError("ROUND_SEARCH_BUDGET_INVARIANT_BREACH")
    _update_run_post_counters(store, run_id, attempted=attempted, confirmed=confirmed, uncertain=uncertain, consumed=consumed)
    complete_ids = [rows_by_key[key]["candidate_id"] for key in rows_by_key if str((facts.get(key) or {}).get("status") or "").upper() == "COMPLETE"]
    nonterminal_ids = [
        rows_by_key[key]["candidate_id"] for key in rows_by_key
        if str((facts.get(key) or {}).get("status") or "").upper() in {"RUNNING", "SUBMITTED", "UNCERTAIN_SUBMISSION"}
    ]
    if round_id is not None:
        for key, target in rows_by_key.items():
            fact = facts.get(key) or {}
            if not fact:
                continue
            event_type = "SIMULATION_COMPLETE" if str(fact.get("status") or "").upper() == "COMPLETE" else "SIMULATION_STATE_SYNC"
            record_event(
                store, round_id, run_id, event_type, batch_no=batch_no, phase="SEARCH",
                candidate_id=target.get("candidate_id"), alpha_id=fact.get("alpha_id"),
                family_id_value=family_id(target), sim_key=key,
                payload={"status": fact.get("status"), "sharpe": fact.get("sharpe"), "fitness": fact.get("fitness"),
                         "turnover": fact.get("turnover"), "origin": ("NEW_POST" if key in actual_post_keys else "RESUME" if key in resume_keys else "CACHE")},
            )
    return {
        "post_attempted": attempted, "post_confirmed": confirmed, "post_uncertain": uncertain,
        "post_consumed": consumed, "cache_hits": cache_hits, "resume_count": len(resume_keys),
        "complete_candidate_ids": complete_ids, "nonterminal_candidate_ids": nonterminal_ids,
        "deferred_candidate_ids": deferred_release.get("released_candidate_ids", []),
        "deferred_sim_keys": deferred_release.get("released_sim_keys", []),
        "http_audit": methods,
        "dataframe_rows": len(frame) if hasattr(frame, "__len__") else None,
        "results": [{"candidate_id": rows_by_key[k]["candidate_id"], "sim_key": k,
                     "status": (facts.get(k) or {}).get("status"), "alpha_id": (facts.get(k) or {}).get("alpha_id")}
                    for k in rows_by_key],
    }


def _check_progress_line(label: str, index: int, total: int, *, candidate_id: str = "",
                         alpha_id: str = "", state: str = "") -> None:
    """Emit a stable text progress bar for PRE-TAG /check work."""
    total = max(1, int(total))
    index = max(0, min(int(index), total))
    width = 24
    filled = int(round(width * index / total))
    bar = "#" * filled + "-" * (width - filled)
    pct = 100.0 * index / total
    identity = f"alpha={alpha_id}" if alpha_id else f"candidate={candidate_id}"
    suffix = f" | {state}" if state else ""
    print(f"{label} [{bar}] {index}/{total} ({pct:5.1f}%) | {identity}{suffix}", flush=True)


def _analyze_and_check(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                       run_id: str, candidate_ids: Sequence[str], repair_remaining: int) -> Dict[str, Any]:
    if not candidate_ids:
        return {"local": {"analyzed": [], "local_pass_candidates": []}, "checks": [], "check_count": 0}
    local = run_local_analysis(store, config, alpha_db, run_id, audit_source=ROUND_SOURCE,
                               candidate_ids=candidate_ids, repair_reserve_remaining=repair_remaining)
    max_candidates = int(config.plan["budgets"].get("max_check_candidates", 0))
    max_requests = int(config.plan["budgets"].get("max_check_http_requests", 0))
    used_candidates, used_requests = _check_budget_used(store, run_id)
    pass_ids = [str(x) for x in local.get("local_pass_candidates", []) if x]
    remaining_candidate_budget = max(0, max_candidates - used_candidates)
    progress_total = min(len(pass_ids), remaining_candidate_budget)
    candidates = {str(x.get("candidate_id")): x for x in store.load_candidates(run_id)}
    reports = []
    for cid in pass_ids:
        if used_candidates >= max_candidates or used_requests >= max_requests:
            audit_event(action="ROUND_CHECK_BUDGET_BLOCKED", run_id=run_id, candidate_id=cid,
                        used_candidates=used_candidates, used_requests=used_requests)
            if progress_total:
                _check_progress_line("PRE-TAG CHECK", len(reports), progress_total, candidate_id=cid,
                                     alpha_id=str((candidates.get(cid) or {}).get("alpha_id") or ""),
                                     state="BUDGET STOP")
            break
        alpha_id = str((candidates.get(cid) or {}).get("alpha_id") or "")
        ordinal = len(reports) + 1
        _check_progress_line("PRE-TAG CHECK", max(0, ordinal - 1), max(1, progress_total),
                             candidate_id=cid, alpha_id=alpha_id, state=f"START {ordinal}")
        report = run_one_pretag_check(store, config, machine, session, run_id, [cid],
                                      source=ROUND_SOURCE, evidence_source="LIVE_CHECK")
        reports.append(report)
        state = str(report.get("session_status") or ("EXECUTED" if report.get("executed") else "DONE"))
        _check_progress_line("PRE-TAG CHECK", len(reports), max(1, progress_total),
                             candidate_id=cid, alpha_id=alpha_id, state=state)
        used_candidates, used_requests = _check_budget_used(store, run_id)
    return {"local": local, "checks": reports, "check_count": len(reports)}


def _continuous_analyze_and_enqueue_checks(
    store: Any, config: Any, alpha_db: Path, run_id: str,
    candidate_ids: Sequence[str], repair_remaining: int,
) -> Dict[str, Any]:
    """Run local COMPLETE analysis, but hand PRE_TAG work to the durable queue.

    Continuous mode must never enter the legacy semantic-poll sleep loop here.
    Local deterministic analysis remains synchronous because it is DB/CPU only.
    """
    local = run_local_analysis(
        store, config, alpha_db, run_id, audit_source=ROUND_SOURCE,
        candidate_ids=candidate_ids, repair_reserve_remaining=repair_remaining,
    )
    queued = enqueue_pretag_checks(
        store, run_id, local.get("local_pass_candidates", []), source="V31_CONTINUOUS_CHECK",
    )
    return {
        "local": local,
        "checks": [],
        "check_count": 0,
        "queued_checks": queued,
    }



def _candidate_simulation_url(row: Mapping[str, Any], alpha_db: Optional[Path] = None) -> Optional[str]:
    """Return the durable saved Simulation URL for a workflow candidate."""
    raw = row.get("result_reference_json")
    if raw:
        try:
            payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        url = payload.get("simulation_url")
        if url:
            return str(url)
    if alpha_db is not None and row.get("sim_key"):
        try:
            uri = f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            try:
                hit = conn.execute(
                    "SELECT simulation_url FROM alpha_results WHERE sim_key=?",
                    (str(row.get("sim_key")),),
                ).fetchone()
            finally:
                conn.close()
            if hit and hit[0]:
                return str(hit[0])
        except (sqlite3.Error, OSError):
            pass
    return None


def _quarantine_remote_missing_resume(store: Any, run_id: str, row: Mapping[str, Any], *,
                                      round_id: Optional[str], http_status: int,
                                      simulation_url: str,
                                      trigger_source: str = "RESUME_PREFLIGHT",
                                      resolution_reason: str = "REMOTE_ALREADY_ABSENT",
                                      resolution_metadata: Optional[Mapping[str, Any]] = None) -> None:
    """Quarantine a confirmed-missing remote Simulation without re-POSTing it.

    A saved Simulation URL that returns 404/410 twice is no longer a live server
    slot.  We keep the original consumed POST/accounting intact, preserve the
    stale alpha_results fact as historical truth, and move only the workflow
    candidate into a local review state so resume-first cannot loop forever.
    """
    cid = str(row.get("candidate_id") or "")
    if not cid:
        return
    now = _now()
    reference = {
        "sim_key": row.get("sim_key"),
        "alpha_id": row.get("alpha_id"),
        "simulation_url": simulation_url,
        "status": "REMOTE_NOT_FOUND",
        "http_status": int(http_status),
        "resume_resolution": "CONFIRMED_404_410_NO_REPOST",
        "trigger_source": trigger_source,
        "resolution_reason": resolution_reason,
        "resolution_metadata": dict(resolution_metadata or {}),
        "updated_at": now,
    }
    with store.connect() as conn:
        current = conn.execute(
            "SELECT lifecycle_state,new_post_budget_consumed FROM ppl_candidates WHERE candidate_id=?",
            (cid,),
        ).fetchone()
        old_state = str(current[0]) if current else str(row.get("lifecycle_state") or "SIMULATION_RUNNING")
        conn.execute(
            """UPDATE ppl_candidates
               SET lifecycle_state='SIMULATION_REMOTE_MISSING',
                   simulation_status='REMOTE_NOT_FOUND',
                   execution_action='HOLD_REMOTE_NOT_FOUND',
                   cache_classification='CACHE_REMOTE_NOT_FOUND',
                   stop_reason='REMOTE_SIMULATION_URL_NOT_FOUND',
                   result_reference_json=?,updated_at=?
               WHERE candidate_id=?""",
            (_json(reference), now, cid),
        )
        if old_state != "SIMULATION_REMOTE_MISSING":
            conn.execute(
                """INSERT INTO ppl_state_transitions(
                       run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                   ) VALUES (?,?,'CANDIDATE',?,?,?,?,?,?)""",
                (run_id, cid, old_state, "SIMULATION_REMOTE_MISSING",
                 "Saved Simulation URL returned 404/410 twice; quarantined without re-POST",
                 ROUND_SOURCE, _json(reference), now),
            )
    audit_event(
        action="RESUME_REMOTE_NOT_FOUND", run_id=run_id, candidate_id=cid,
        alpha_id=row.get("alpha_id"), sim_key=row.get("sim_key"),
        simulation_url=simulation_url, http_status=int(http_status),
        resolution="QUARANTINE_NO_REPOST", trigger_source=trigger_source,
        resolution_reason=resolution_reason,
    )
    if round_id is not None:
        record_event(
            store, round_id, run_id, "RESUME_REMOTE_NOT_FOUND",
            candidate_id=cid, alpha_id=row.get("alpha_id"),
            family_id_value=family_id(row), sim_key=row.get("sim_key"),
            payload={"simulation_url": simulation_url, "http_status": int(http_status),
                     "resolution": "QUARANTINE_NO_REPOST", "trigger_source": trigger_source,
                     "resolution_reason": resolution_reason,
                     "resolution_metadata": dict(resolution_metadata or {})},
        )


def _resume_preflight_remote_missing(store: Any, session: Any, run_id: str,
                                     rows: Sequence[Mapping[str, Any]], *,
                                     machine: Any = None,
                                     round_id: Optional[str] = None,
                                     alpha_db: Optional[Path] = None) -> Tuple[List[Mapping[str, Any]], List[str]]:
    """GET-only preflight that removes confirmed stale 404/410 resume URLs.

    Two identical 404/410 responses are required before quarantine to avoid
    reacting to a one-off edge/cache anomaly. Other HTTP outcomes are left to
    unchanged V2.1 resume/poll behavior.
    """
    if session is None:
        return list(rows), []
    ro = ReadOnlySession(session)
    resumable: List[Mapping[str, Any]] = []
    quarantined: List[str] = []
    for row in rows:
        url = _candidate_simulation_url(row, alpha_db)
        if not url:
            resumable.append(row)
            continue
        statuses: List[int] = []
        for attempt in range(2):
            try:
                response = ro.get(url, timeout=30)
                status = int(getattr(response, "status_code", 0) or 0)
            except Exception as exc:
                audit_event(
                    action="RESUME_PREFLIGHT_ERROR", run_id=run_id,
                    candidate_id=row.get("candidate_id"), sim_key=row.get("sim_key"),
                    simulation_url=url, attempt=attempt + 1, error_type=type(exc).__name__,
                )
                statuses = []
                break
            statuses.append(status)
            if status not in {404, 410}:
                break
            if attempt == 0:
                time.sleep(1.0)
        if len(statuses) == 2 and all(x in {404, 410} for x in statuses):
            facts = _alpha_facts(alpha_db, [str(row.get("sim_key") or "")]) if alpha_db else {}
            fact = facts.get(str(row.get("sim_key") or "")) or {}
            if fact and machine is not None and hasattr(machine, "cache_put"):
                try:
                    candidate = json.loads(str(fact.get("candidate_json") or "{}"))
                    settings = json.loads(str(fact.get("settings_json") or "{}"))
                    machine.cache_put(
                        str(alpha_db), str(row.get("sim_key")), candidate, settings,
                        {"alpha_id": fact.get("alpha_id"), "status": "REMOTE_NOT_FOUND",
                         "simulation_url": url, "submitted_at": fact.get("submitted_at"),
                         "retry_count": fact.get("retry_count"), "last_http_status": statuses[-1],
                         "error": _json({"trigger_source": "RESUME_PREFLIGHT",
                                         "resolution_reason": "REMOTE_ALREADY_ABSENT",
                                         "verification_statuses": statuses})},
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            _quarantine_remote_missing_resume(
                store, run_id, row, round_id=round_id,
                http_status=statuses[-1], simulation_url=url,
            )
            quarantined.append(str(row.get("candidate_id")))
        else:
            resumable.append(row)
    return resumable, quarantined


def _persist_remote_terminal_failure_fact(machine: Any, alpha_db: Path, row: Mapping[str, Any], *,
                                          payload: Mapping[str, Any], simulation_url: str,
                                          http_status: int) -> bool:
    """Persist a terminal remote Simulation failure through V2.1 cache_put.

    This keeps alpha_results as the Simulation Fact Truth while preserving the
    original consumed POST and durable Simulation URL.  machine_lib_V2_1.py is
    not modified; we only reuse its existing writer-locked cache_put API.
    """
    sim_key = str(row.get("sim_key") or "")
    if not sim_key or machine is None or not hasattr(machine, "cache_put"):
        return False
    facts = _alpha_facts(alpha_db, [sim_key])
    fact = facts.get(sim_key) or {}
    if not fact:
        return False
    try:
        candidate = json.loads(str(fact.get("candidate_json") or "{}"))
        settings = json.loads(str(fact.get("settings_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(candidate, dict) or not candidate.get("expr") or not isinstance(settings, dict):
        return False
    result = {
        "alpha_id": payload.get("alpha") or fact.get("alpha_id"),
        "status": "ERROR",
        "sharpe": fact.get("sharpe"),
        "fitness": fact.get("fitness"),
        "turnover": fact.get("turnover"),
        "margin": fact.get("margin"),
        "returns": fact.get("returns"),
        "long_count": fact.get("long_count"),
        "short_count": fact.get("short_count"),
        "date_created": fact.get("date_created"),
        "error": _json({"remote_status": payload.get("status"), "payload": dict(payload)}),
        "simulation_url": simulation_url,
        "retry_count": fact.get("retry_count"),
        "last_http_status": int(http_status),
        "last_retry_after": 0.0,
        "warning": fact.get("warning"),
    }
    machine.cache_put(str(alpha_db), sim_key, candidate, settings, result)
    return True


def _quarantine_remote_failed_resume(store: Any, run_id: str, row: Mapping[str, Any], *,
                                     round_id: Optional[str], http_status: int,
                                     simulation_url: str, payload: Mapping[str, Any],
                                     fact_persisted: bool) -> None:
    """Finalize a remote terminal FAIL/FAILED/ERROR without re-POSTing."""
    cid = str(row.get("candidate_id") or "")
    if not cid:
        return
    now = _now()
    remote_status = str(payload.get("status") or "FAIL").upper()
    reference = {
        "sim_key": row.get("sim_key"),
        "alpha_id": payload.get("alpha") or row.get("alpha_id"),
        "simulation_url": simulation_url,
        "status": "ERROR",
        "remote_status": remote_status,
        "http_status": int(http_status),
        "resume_resolution": "CONFIRMED_REMOTE_TERMINAL_FAILURE_NO_REPOST",
        "fact_persisted": bool(fact_persisted),
        "updated_at": now,
    }
    with store.connect() as conn:
        current = conn.execute(
            "SELECT lifecycle_state FROM ppl_candidates WHERE candidate_id=?", (cid,)
        ).fetchone()
        old_state = str(current[0]) if current else str(row.get("lifecycle_state") or "SIMULATION_RUNNING")
        conn.execute(
            """UPDATE ppl_candidates
               SET lifecycle_state='SIMULATION_REMOTE_FAILED',
                   simulation_status='ERROR',
                   execution_action='HOLD_REMOTE_FAILED',
                   cache_classification='CACHE_ERROR',
                   stop_reason='REMOTE_SIMULATION_TERMINAL_FAILURE',
                   result_reference_json=?,updated_at=?
               WHERE candidate_id=?""",
            (_json(reference), now, cid),
        )
        if old_state != "SIMULATION_REMOTE_FAILED":
            conn.execute(
                """INSERT INTO ppl_state_transitions(
                       run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
                   ) VALUES (?,?,'CANDIDATE',?,?,?,?,?,?)""",
                (run_id, cid, old_state, "SIMULATION_REMOTE_FAILED",
                 f"Remote Simulation returned terminal status {remote_status}; held without re-POST",
                 ROUND_SOURCE, _json(reference), now),
            )
    audit_event(
        action="RESUME_REMOTE_FAILED", run_id=run_id, candidate_id=cid,
        alpha_id=payload.get("alpha") or row.get("alpha_id"), sim_key=row.get("sim_key"),
        simulation_url=simulation_url, http_status=int(http_status), remote_status=remote_status,
        resolution="HOLD_NO_REPOST", fact_persisted=bool(fact_persisted),
    )
    if round_id is not None:
        record_event(
            store, round_id, run_id, "RESUME_REMOTE_FAILED",
            candidate_id=cid, alpha_id=payload.get("alpha") or row.get("alpha_id"),
            family_id_value=family_id(row), sim_key=row.get("sim_key"),
            payload={"simulation_url": simulation_url, "http_status": int(http_status),
                     "remote_status": remote_status, "resolution": "HOLD_NO_REPOST",
                     "fact_persisted": bool(fact_persisted)},
        )


def _resume_preflight_terminal_failure(store: Any, machine: Any, session: Any, alpha_db: Path,
                                       run_id: str, rows: Sequence[Mapping[str, Any]], *,
                                       round_id: Optional[str] = None) -> Tuple[List[Mapping[str, Any]], List[str]]:
    """GET-only preflight for remote terminal statuses that V2.1 does not normalize.

    The BRAIN Simulation endpoint can return JSON ``status=FAIL``.  V2.1 treats
    ``FAILED`` and ``ERROR`` as terminal, but ``FAIL`` falls through to the
    generic polling branch and can loop until timeout.  Two consecutive 200
    responses with FAIL/FAILED/ERROR are required before finalizing the local
    workflow fact.  No Simulation POST is issued.
    """
    if session is None:
        return list(rows), []
    ro = ReadOnlySession(session)
    resumable: List[Mapping[str, Any]] = []
    failed: List[str] = []
    for row in rows:
        url = _candidate_simulation_url(row, alpha_db)
        if not url:
            resumable.append(row)
            continue
        confirmations: List[Tuple[int, str, Mapping[str, Any]]] = []
        for attempt in range(2):
            try:
                response = ro.get(url, timeout=30)
                status = int(getattr(response, "status_code", 0) or 0)
                if status != 200:
                    confirmations = []
                    break
                try:
                    payload = response.json()
                except (ValueError, TypeError):
                    confirmations = []
                    break
                if not isinstance(payload, Mapping):
                    confirmations = []
                    break
                remote_status = str(payload.get("status") or "").upper()
                if production_remote_resolution_status(payload) != "TERMINAL_FAILURE":
                    confirmations = []
                    break
                confirmations.append((status, remote_status, dict(payload)))
            except Exception as exc:
                audit_event(
                    action="RESUME_TERMINAL_PREFLIGHT_ERROR", run_id=run_id,
                    candidate_id=row.get("candidate_id"), sim_key=row.get("sim_key"),
                    simulation_url=url, attempt=attempt + 1, error_type=type(exc).__name__,
                )
                confirmations = []
                break
            if attempt == 0:
                time.sleep(1.0)
        if len(confirmations) == 2:
            payload = confirmations[-1][2]
            persisted = _persist_remote_terminal_failure_fact(
                machine, alpha_db, row, payload=payload, simulation_url=url, http_status=confirmations[-1][0]
            )
            _quarantine_remote_failed_resume(
                store, run_id, row, round_id=round_id, http_status=confirmations[-1][0],
                simulation_url=url, payload=payload, fact_persisted=persisted,
            )
            failed.append(str(row.get("candidate_id")))
        else:
            resumable.append(row)
    return resumable, failed


def _resume_nonterminal(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                        run_id: str, repair_remaining: int, *, round_id: Optional[str] = None) -> Dict[str, Any]:
    rows = [x for x in store.load_candidates(run_id)
            if str(x.get("simulation_status") or "").upper() in {"RUNNING", "SUBMITTED", "STALE_RUNNING"}]
    if not rows:
        return {"resumed": 0, "completed": 0, "checks": 0, "remote_missing": 0,
                "still_nonterminal_candidate_ids": []}
    original_ids = [str(r.get("candidate_id")) for r in rows]
    rows, remote_missing_ids = _resume_preflight_remote_missing(
        store, session, run_id, rows, machine=machine, round_id=round_id, alpha_db=alpha_db
    )
    rows, remote_failed_ids = _resume_preflight_terminal_failure(
        store, machine, session, alpha_db, run_id, rows, round_id=round_id
    )
    if round_id is not None:
        for row in rows:
            record_event(store, round_id, run_id, "RESUME_FIRST", candidate_id=row.get("candidate_id"),
                         alpha_id=row.get("alpha_id"), family_id_value=family_id(row), sim_key=row.get("sim_key"),
                         payload={"simulation_status": row.get("simulation_status")})
    if not rows:
        return {"resumed": 0, "completed": 0, "checks": 0,
                "remote_missing": len(remote_missing_ids),
                "remote_missing_candidate_ids": remote_missing_ids,
                "remote_failed": len(remote_failed_ids),
                "remote_failed_candidate_ids": remote_failed_ids,
                "candidate_ids": original_ids,
                "still_nonterminal_candidate_ids": []}
    result = _execute_search_rows(store, config, machine, session, alpha_db, run_id, rows,
                                  allow_simulation_post=True, remaining_search_budget=0, round_id=round_id)
    analyzed = _analyze_and_check(store, config, machine, session, alpha_db, run_id,
                                  result.get("complete_candidate_ids", []), repair_remaining)
    return {"resumed": result.get("resume_count", 0), "completed": len(result.get("complete_candidate_ids", [])),
            "checks": analyzed.get("check_count", 0), "remote_missing": len(remote_missing_ids),
            "remote_missing_candidate_ids": remote_missing_ids,
            "remote_failed": len(remote_failed_ids),
            "remote_failed_candidate_ids": remote_failed_ids,
            "candidate_ids": original_ids,
            "still_nonterminal_candidate_ids": list(result.get("nonterminal_candidate_ids") or [])}


def _round_runtime_guard(store: Any, run_id: str, *, global_hold: bool = True) -> Dict[str, int]:
    """Classify durable runtime hazards; legacy mode may still fail globally.

    V3.0.x keeps its historical fail-closed semantics.  V3.1 Continuous uses
    the same facts for slot reservation/local quarantine and therefore requests
    ``global_hold=False``.  Core DB/invariant failures are handled outside this
    candidate-status guard and still halt globally.
    """
    rows = store.load_candidates(run_id)
    counts = Counter(str(x.get("simulation_status") or "NONE").upper() for x in rows)
    uncertain = int(counts.get("UNCERTAIN_SUBMISSION", 0))
    auth_error = int(counts.get("AUTH_ERROR", 0))
    if global_hold and uncertain:
        raise ConfigError(f"ROUND_UNCERTAIN_SUBMISSION_HOLD:{uncertain}")
    if global_hold and auth_error:
        raise ConfigError(f"ROUND_AUTH_ERROR:{auth_error}")
    return {"uncertain": uncertain, "auth_error": auth_error}


def cancel_remote_simulation(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path, *,
    run_id: str, simulation_id: str, confirmed: bool,
) -> Dict[str, Any]:
    """Human-confirmed use of the common resolver for one owned Simulation."""
    run = store.get_run(run_id)
    if not run:
        raise ConfigError("REMOTE_CANCEL_RUN_NOT_FOUND")
    round_row = get_round(store, run_id=run_id)
    candidates = [dict(x) for x in store.load_candidates(run_id)]
    facts = _alpha_facts(alpha_db, [str(x.get("sim_key") or "") for x in candidates])
    matches: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []
    for candidate in candidates:
        fact = facts.get(str(candidate.get("sim_key") or "")) or {}
        url = str(fact.get("simulation_url") or _candidate_simulation_url(candidate, alpha_db) or "")
        if not url:
            continue
        try:
            checked = validate_simulation_url(url, simulation_id)
        except ValueError:
            continue
        matches.append((candidate, fact, checked))
    if len(matches) != 1:
        raise ConfigError("REMOTE_CANCEL_SIMULATION_NOT_FOUND_OR_NOT_UNIQUE")
    row, fact, url = matches[0]
    local_status = str(fact.get("status") or row.get("simulation_status") or "").upper()
    if local_status == "COMPLETE" or str(row.get("lifecycle_state") or "") == "SIMULATION_COMPLETE":
        raise ConfigError("REMOTE_CANCEL_COMPLETE_FORBIDDEN")
    if local_status == "REMOTE_NOT_FOUND" or str(row.get("lifecycle_state") or "") == "SIMULATION_REMOTE_MISSING":
        return {"action": "CANCEL_SIMULATION", "result": "already resolved", "run_id": run_id,
                "candidate_id": row.get("candidate_id"), "simulation_id": simulation_id,
                "simulation_url": url, "network_requests": 0}
    preview = {"action": "CANCEL_SIMULATION", "confirmed": bool(confirmed), "run_id": run_id,
               "round_id": (round_row or {}).get("round_id"), "candidate_id": row.get("candidate_id"),
               "sim_key": row.get("sim_key"), "simulation_id": simulation_id,
               "simulation_url": url, "local_status": local_status,
               "budget_consumed_before": int(row.get("new_post_budget_consumed") or 0)}
    if not confirmed:
        return {**preview, "executed": False,
                "reason": "--confirm-cancel-simulation is required; no network request was made"}
    if session is None:
        raise ConfigError("REMOTE_CANCEL_SESSION_REQUIRED")
    resolution = resolve_remote_simulation(
        session, url, trigger_source="MANUAL_CANCEL", submitted_at=fact.get("submitted_at"),
        cancellation_reason="USER_CANCELLED",
        verify_attempts=max(2, int(config.plan["runtime"].get("simulation_auto_cancel_verify_attempts", 2))),
        status_policy=production_remote_resolution_status,
    ).to_dict()
    audit_payload = remote_resolution_audit_payload(
        resolution, run_id=run_id, round_id=(round_row or {}).get("round_id"),
        candidate_id=row.get("candidate_id"), sim_key=row.get("sim_key"),
        simulation_id=simulation_id,
    )
    audit_event(**audit_payload)
    outcome = str(resolution.get("resolution_result") or "UNRESOLVED")
    if outcome == "REMOTE_NOT_FOUND":
        try:
            candidate_json = json.loads(str(fact.get("candidate_json") or "{}"))
            settings_json = json.loads(str(fact.get("settings_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ConfigError("REMOTE_CANCEL_CACHE_FACT_INVALID") from exc
        machine.cache_put(
            str(alpha_db), str(row.get("sim_key")), candidate_json, settings_json,
            {"alpha_id": fact.get("alpha_id"), "status": "REMOTE_NOT_FOUND",
             "simulation_url": url, "submitted_at": fact.get("submitted_at"),
             "retry_count": fact.get("retry_count"),
             "last_http_status": (resolution.get("verification_statuses") or [404])[-1],
             "error": _json(resolution)},
        )
        _quarantine_remote_missing_resume(
            store, run_id, row, round_id=(round_row or {}).get("round_id"),
            http_status=int((resolution.get("verification_statuses") or [404])[-1]),
            simulation_url=url, trigger_source="MANUAL_CANCEL",
            resolution_reason=str(resolution.get("resolution_reason")),
            resolution_metadata=resolution,
        )
        if round_row:
            update_round(store, str(round_row["round_id"]), status="PAUSED",
                         stop_reason="REMOTE_RESOLUTION_COMPLETED:" + str(resolution.get("resolution_reason")))
    elif outcome == "DELEGATE_TERMINAL_FAILURE":
        payload = dict(resolution.get("payload") or {})
        persisted = _persist_remote_terminal_failure_fact(
            machine, alpha_db, row, payload=payload, simulation_url=url, http_status=200,
        )
        _quarantine_remote_failed_resume(
            store, run_id, row, round_id=(round_row or {}).get("round_id"), http_status=200,
            simulation_url=url, payload=payload, fact_persisted=persisted,
        )
        if round_row:
            update_round(store, str(round_row["round_id"]), status="PAUSED",
                         stop_reason="REMOTE_RESOLUTION_COMPLETED:REMOTE_TERMINAL_FAILURE")
    elif outcome == "DELEGATE_RESULT":
        execution = _execute_search_rows(
            store, config, machine, session, alpha_db, run_id, [row],
            allow_simulation_post=True, remaining_search_budget=0,
            round_id=(round_row or {}).get("round_id"),
        )
        if not execution.get("complete_candidate_ids"):
            raise ConfigError("REMOTE_COMPLETE_DELEGATION_DID_NOT_COMPLETE")
        repair_remaining = max(
            0, int((round_row or {}).get("repair_budget") or 0)
            - int((round_row or {}).get("repair_consumed") or 0),
        )
        _analyze_and_check(
            store, config, machine, session, alpha_db, run_id,
            execution.get("complete_candidate_ids", []), repair_remaining,
        )
        if round_row:
            update_round(store, str(round_row["round_id"]), status="PAUSED",
                         stop_reason="REMOTE_RESOLUTION_COMPLETED:REMOTE_COMPLETE")
    else:
        if round_row:
            update_round(store, str(round_row["round_id"]), status="PAUSED",
                         stop_reason=str(resolution.get("error_reason") or "REMOTE_RESOLUTION_UNRESOLVED"))
        return {**preview, "executed": True, "resolved": False, "resolution": resolution}
    if round_row:
        sync_simulation_ledger(
            store, alpha_db, str(round_row["round_id"]), run_id,
            candidate_ids=[str(row.get("candidate_id"))],
        )
        _reconcile_round_accounting(
            store, alpha_db, run_id, str(round_row["round_id"]),
            fail_on_unresolved_intent=False,
        )
    after = {x["candidate_id"]: x for x in store.load_candidates(run_id)}.get(row["candidate_id"], {})
    return {**preview, "executed": True, "resolved": True, "resolution": resolution,
            "candidate_status_after": after.get("simulation_status"),
            "budget_consumed_after": int(after.get("new_post_budget_consumed") or 0)}


def _candidate_pool_preflight(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                              run_id: str, *, offline: bool = False,
                              extension_source: Optional[Mapping[str, Any]] = None,
                              policy: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Build a candidate pool without writing any run, round, or snapshot rows."""
    evidence = build_project_operator_evidence(alpha_db)
    if offline:
        cached = store.load_latest_discovery(config.plan["simulation_settings"])
        if cached is None:
            raise ConfigError("OFFLINE_DISCOVERY_CACHE_MISS")
        discovery = DiscoveryResult(**cached)
        network = "OFFLINE"
    else:
        ro = ReadOnlySession(session)
        discovery = discover_online(ro, config, machine)
        network = "ONLINE_READ_ONLY"
    dry = estimate_candidate_plan(discovery, config, store.operator_registry_summary())
    # Dry-run snapshots need their execution identity for candidate validation.
    dry["execution_hash"] = config.execution_hash
    extension_specs = _extension_specs_for_discovery(discovery, config, policy or {}, extension_source)
    candidates, preview = generate_candidate_preview(
        discovery, dry, config, run_id=run_id, alpha_db=alpha_db, machine_lib=machine,
        extension_specs=extension_specs,
    )
    return {
        "network_mode": network, "discovery": discovery, "dry_run": dry,
        "candidates": candidates, "candidate_preview": preview,
        "operator_evidence": evidence,
        "extension_source_run_id": (extension_source or {}).get("source_run_id"),
    }


def _persist_candidate_pool(store: Any, preflight: Mapping[str, Any], run_id: str) -> Dict[str, Any]:
    """Persist a prevalidated pool; callers must create the run/round first."""
    discovery = preflight["discovery"]
    dry = dict(preflight["dry_run"])
    candidates = list(preflight["candidates"])
    store.upsert_operator_evidence(preflight["operator_evidence"])
    if preflight.get("network_mode") == "ONLINE_READ_ONLY":
        store.save_discovery_snapshot(discovery.snapshot, discovery.datasets, discovery.fields)
    store.save_dry_run(dry["dry_run_id"], discovery.snapshot["snapshot_id"], dry["execution_hash"], dry["source"], dry)
    store.upsert_candidates(candidates)
    # V3 promotes candidates batch-by-batch. Clear the V2.2 one-shot selection
    # flag for this new run only; candidate identity and preview evidence stay intact.
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_candidates SET selected_for_initial_search=0,selection_rank=NULL,
                   selection_reason='V3_ROUND_POOL' WHERE run_id=?""", (run_id,)
        )
    return {"network_mode": preflight["network_mode"], "discovery": discovery.snapshot, "dry_run": dry,
            "candidate_preview": preflight["candidate_preview"], "candidate_pool": len(candidates),
            "extension_source_run_id": preflight.get("extension_source_run_id")}


def _ppc_spec_metadata(store: Any, config: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    anchor = infer_ppc_branch_anchor(store, run_id, candidate_id) or candidate_id
    cfg = ppc_branch_config(config.rules)
    return {
        "repair_policy": PPC_POLICY_NAME,
        "repair_policy_version": PPC_POLICY_VERSION,
        "ppc_branch_anchor_candidate_id": anchor,
        "ppc_base_candidate_id": candidate_id,
        "ppc_policy_snapshot": {
            "project_version": PPC_POLICY_VERSION,
            "max_attempts": int(cfg["max_attempts"]),
            "meaningful_improvement_min": float(cfg["meaningful_improvement_min"]),
            "meaningful_worsening_min": float(cfg["meaningful_worsening_min"]),
            "require_no_new_fixed_blockers": bool(cfg.get("require_no_new_fixed_blockers", True)),
            "require_not_strategy_rejected": bool(cfg.get("require_not_strategy_rejected", True)),
            "max_sharpe_drop_abs": cfg.get("max_sharpe_drop_abs"),
            "max_fitness_drop_abs": cfg.get("max_fitness_drop_abs"),
            "same_family_windows": list(cfg["same_family_windows"]),
        },
    }


def _ensure_neutralization_plan(store: Any, config: Any, run_id: str, candidate_id: str,
                                target_failure: str, neutralization: str) -> Optional[str]:
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    parent = candidates.get(candidate_id)
    if not parent:
        return None
    spec = neutralization_micro_tune_spec(parent, target_failure, neutralization)
    if str(target_failure or "").upper() == PPC_TARGET_FAILURE:
        spec.update(_ppc_spec_metadata(store, config, run_id, candidate_id))
    signature = str(spec["repair_signature"])
    plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
    with store.connect() as conn:
        existing = conn.execute("SELECT repair_plan_id FROM ppl_repair_plans WHERE run_id=? AND repair_signature=?", (run_id, signature)).fetchone()
        if existing:
            return str(existing[0])
        diag = conn.execute(
            "SELECT diagnosis_id FROM ppl_diagnoses WHERE run_id=? AND candidate_id=? ORDER BY created_at DESC LIMIT 1",
            (run_id, candidate_id),
        ).fetchone()
        conn.execute(
            """INSERT INTO ppl_repair_plans(
                   repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                   repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                   operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                   blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (plan_id, diag[0] if diag else None, run_id, candidate_id, parent.get("root_candidate_id") or candidate_id,
             target_failure, spec["repair_type"], signature, _json(spec.get("repair_path", [])), int(spec.get("repair_depth") or 1),
             _json(spec), "[]", "PLANNED", 1, 0, 0, None, _now(), _now()),
        )
    audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id, repair_plan_id=plan_id,
                parent_candidate_id=candidate_id, candidate_id=candidate_id,
                repair_strategy=spec["repair_type"], target_failure=target_failure,
                parameter_change={"neutralization": neutralization},
                repair_policy=(PPC_POLICY_NAME if str(target_failure or "").upper() == PPC_TARGET_FAILURE else None))
    return plan_id


def _ensure_same_family_micro_tune_plan(store: Any, config: Any, run_id: str, candidate_id: str,
                                        target_failure: str) -> Optional[str]:
    """Materialize one untried low-destruction ts_mean window branch.

    The effective parent window is recovered from the expression for legacy
    rows whose window metadata is NULL.  Windows already present anywhere in
    this PPC branch are considered visited, preventing a promoted Best Node
    from simply reverting to an ancestor simulation identity.
    """
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    parent = candidates.get(candidate_id)
    if not parent or str(parent.get("operator") or "").lower() != "ts_mean":
        return None
    current = resolve_effective_window(parent)
    if current is None:
        audit_event(action="PPC_SAME_FAMILY_BLOCKED", run_id=run_id, candidate_id=candidate_id,
                    target_failure=target_failure, reason="EFFECTIVE_WINDOW_UNRESOLVED")
        return None
    anchor = infer_ppc_branch_anchor(store, run_id, candidate_id)
    if not anchor:
        audit_event(action="PPC_SAME_FAMILY_BLOCKED", run_id=run_id, candidate_id=candidate_id,
                    target_failure=target_failure, reason="PPC_BRANCH_ANCHOR_AMBIGUOUS")
        return None
    state = ppc_branch_state(store, config, run_id, anchor)
    visited_windows = {current}
    anchor_row = candidates.get(anchor)
    anchor_window = resolve_effective_window(anchor_row or {})
    if anchor_window is not None:
        visited_windows.add(anchor_window)
    for row in state.get("branch_rows", []):
        for cid in (row.get("parent_candidate_id"), row.get("child_candidate_id")):
            w = resolve_effective_window(candidates.get(str(cid or "")) or {})
            if w is not None:
                visited_windows.add(w)
        try:
            old_spec = json.loads(row.get("candidate_spec_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            old_spec = {}
        if old_spec.get("window_override") is not None:
            try:
                visited_windows.add(int(old_spec["window_override"]))
            except (TypeError, ValueError):
                pass
    cfg = ppc_branch_config(config.rules)
    # The configured window list is an explicit low-destruction ladder.
    # If the current window is outside that ladder, fail closed instead of
    # making a potentially large jump (for example 22 -> 5).
    if current not in cfg["same_family_windows"]:
        audit_event(action="PPC_SAME_FAMILY_BLOCKED", run_id=run_id, candidate_id=candidate_id,
                    target_failure=target_failure, reason="CURRENT_WINDOW_OUTSIDE_CONFIGURED_POOL",
                    current_window=current, configured_windows=list(cfg["same_family_windows"]))
        return None
    candidate_windows = [w for w in cfg["same_family_windows"] if w != current and w not in visited_windows]
    if not candidate_windows:
        return None
    target_window = min(candidate_windows, key=lambda w: (abs(w - current), 0 if w > current else 1, w))
    spec = same_family_micro_tune_spec(parent, target_failure, target_window)
    spec.update(_ppc_spec_metadata(store, config, run_id, candidate_id))
    # Cheap local no-op defense before the Production Repair sim-key guard.
    if str(spec.get("expression_preview") or "") == str(parent.get("expression") or ""):
        audit_event(action="PPC_SAME_FAMILY_BLOCKED", run_id=run_id, candidate_id=candidate_id,
                    target_failure=target_failure, reason="NO_EFFECTIVE_EXPRESSION_CHANGE",
                    window_from=current, window_to=target_window)
        return None
    signature = str(spec["repair_signature"])
    plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
    with store.connect() as conn:
        existing = conn.execute(
            "SELECT repair_plan_id FROM ppl_repair_plans WHERE run_id=? AND repair_signature=?",
            (run_id, signature),
        ).fetchone()
        if existing:
            return str(existing[0])
        diag = conn.execute(
            "SELECT diagnosis_id FROM ppl_diagnoses WHERE run_id=? AND candidate_id=? ORDER BY created_at DESC LIMIT 1",
            (run_id, candidate_id),
        ).fetchone()
        conn.execute(
            """INSERT INTO ppl_repair_plans(
                   repair_plan_id,diagnosis_id,run_id,parent_candidate_id,root_candidate_id,target_failure,
                   repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,
                   operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,
                   blocked_reason,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (plan_id, diag[0] if diag else None, run_id, candidate_id, parent.get("root_candidate_id") or candidate_id,
             target_failure, spec["repair_type"], signature, _json(spec.get("repair_path", [])), int(spec.get("repair_depth") or 1),
             _json(spec), _json(spec.get("operator_requirements", [])), "PLANNED", 1, 0, 0, None, _now(), _now()),
        )
    audit_event(action="REPAIR_PLAN_CREATED", run_id=run_id, repair_plan_id=plan_id,
                parent_candidate_id=candidate_id, candidate_id=candidate_id,
                repair_strategy=spec["repair_type"], target_failure=target_failure,
                parameter_change={"window": {"from": current, "to": target_window}},
                repair_policy=PPC_POLICY_NAME, ppc_branch_anchor_candidate_id=anchor)
    return plan_id


def _ppc_controlled_ranked_pool(store: Any, config: Any, run_id: str,
                                ranked_pool: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Keep exactly the durable Best Node for each active PPC branch.

    PPC policy only owns candidates whose *primary* failure is PP correlation.
    Secondary PPC warnings therefore cannot hijack HIGH_TURNOVER or another
    canonical repair policy.
    """
    non_ppc: List[Dict[str, Any]] = []
    by_anchor: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for source in ranked_pool:
        item = dict(source)
        if str(item.get("primary_failure") or "").upper() != PPC_TARGET_FAILURE:
            non_ppc.append(item)
            continue
        anchor = infer_ppc_branch_anchor(store, run_id, str(item.get("candidate_id") or ""))
        if not anchor:
            continue
        item["_ppc_branch_anchor_candidate_id"] = anchor
        by_anchor[anchor].append(item)

    selected_ppc: List[Dict[str, Any]] = []
    for anchor, nodes in by_anchor.items():
        state = ppc_branch_state(store, config, run_id, anchor)
        if state.get("success") or state.get("exhausted") or state.get("evaluation_pending"):
            continue
        best_id = str(state.get("best_candidate_id") or anchor)
        best = next((x for x in nodes if str(x.get("candidate_id") or "") == best_id), None)
        if best is None:
            # Best Node is not currently HIGH/MEDIUM repairable; do not repair a
            # worse descendant merely because it remains in the queue.
            continue
        best["_ppc_attempts_used"] = int(state["attempts_used"])
        best["_ppc_attempts_remaining"] = int(state["attempts_remaining"])
        best["_ppc_max_attempts"] = int(state["max_attempts"])
        selected_ppc.append(best)
    return non_ppc + selected_ppc

def _pending_selected_initial_posts(store: Any, run_id: str) -> int:
    with store.connect() as conn:
        return int(conn.execute(
            "SELECT COUNT(*) FROM ppl_candidates WHERE run_id=? AND selected_for_initial_search=1 AND execution_action='NEW_SIMULATION_REQUIRED'",
            (run_id,),
        ).fetchone()[0])


def _repair_attempts_by_family(store: Any, run_id: str, round_id: Optional[str] = None) -> Dict[str, int]:
    out = Counter()
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT c.signal_family,coalesce(sum(p.consumed_posts),0)
               FROM ppl_repair_plans p JOIN ppl_candidates c ON c.candidate_id=p.parent_candidate_id
               WHERE p.run_id=? GROUP BY c.signal_family""", (run_id,)
        ).fetchall()
    for signal, consumed in rows:
        out[str(signal or "")] = int(consumed or 0)
    # A repair POST that was actually selected/dispatched but failed before
    # producing a consumable remote fact must not be selected forever.  Count
    # V3 repair batches with projected POSTs as an operational attempt for the
    # per-family cap, while cache/resume-only batches remain free.
    if round_id:
        with store.connect() as conn:
            plan_rows = conn.execute(
                """SELECT p.repair_plan_id,c.signal_family
                   FROM ppl_repair_plans p JOIN ppl_candidates c ON c.candidate_id=p.parent_candidate_id
                   WHERE p.run_id=?""", (run_id,)
            ).fetchall()
        plans = {str(r[0]): str(r[1] or "") for r in plan_rows}
        # Count distinct logical repair plans, not dispatch attempts.  An
        # operator-authorized retry of the same plan after UNCERTAIN_SUBMISSION
        # may appear in a later batch and consume another remote/budget unit,
        # but it is still the same strategy attempt for the per-family cap.
        attempted_plan_ids: Dict[str, set] = defaultdict(set)
        for batch in load_batches(store, round_id):
            if str(batch.get("phase") or "").upper() != "REPAIR" or int(batch.get("projected_new_posts") or 0) <= 0:
                continue
            for pid in _parse_json_list(batch.get("selected_plan_ids_json")):
                signal = plans.get(pid)
                if signal:
                    attempted_plan_ids[signal].add(pid)
        for signal, plan_ids in attempted_plan_ids.items():
            out[signal] = max(int(out.get(signal, 0)), len(plan_ids))
    return dict(out)


def _repair_value(item: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Compatibility export; C5 Repair prioritization lives in repair_strategy."""
    return strategy_repair_value(item, policy)

def _direction_repair_value(item: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Compatibility export; C5 Repair prioritization lives in repair_strategy."""
    return strategy_direction_repair_value(item, policy)


def _proposal_to_virtual_plan_row(proposal: Mapping[str, Any], candidates: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    """Convert a persist=False Check proposal into Production Repair plan-row semantics."""
    cid = str(proposal.get("parent_candidate_id") or "")
    parent = dict(candidates.get(cid) or {})
    spec = dict(proposal.get("candidate_spec") or {})
    return {
        "repair_plan_id": str(proposal.get("repair_plan_id") or ""),
        "diagnosis_id": proposal.get("diagnosis_id"), "run_id": str(proposal.get("run_id") or ""),
        "parent_candidate_id": cid, "root_candidate_id": parent.get("root_candidate_id") or cid,
        "target_failure": proposal.get("target_failure"), "repair_type": proposal.get("repair_type"),
        "repair_signature": proposal.get("repair_signature"),
        "repair_path_json": _json(proposal.get("repair_path") or []),
        "repair_depth": int(proposal.get("repair_depth") or 1),
        "candidate_spec_json": _json(spec),
        "operator_requirements_json": _json(proposal.get("operator_requirements") or []),
        "plan_status": str(proposal.get("plan_status") or "PLANNED"),
        "projected_new_posts": int(proposal.get("projected_new_posts") or 0),
        "committed_posts": 0, "consumed_posts": 0, "blocked_reason": None,
        "_virtual_source": "CHECK_DERIVED_PREVIEW",
    }


def _virtual_neutralization_plan_row(
    store: Any, config: Any, run_id: str, candidate_id: str,
    target_failure: str, neutralization: str,
    candidates: Mapping[str, Mapping[str, Any]], existing_signatures: set,
) -> Optional[Dict[str, Any]]:
    parent = dict(candidates.get(candidate_id) or {})
    if not parent:
        return None
    spec = neutralization_micro_tune_spec(parent, target_failure, neutralization)
    if str(target_failure or "").upper() == PPC_TARGET_FAILURE:
        spec.update(_ppc_spec_metadata(store, config, run_id, candidate_id))
    signature = str(spec["repair_signature"])
    if signature in existing_signatures:
        return None
    plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
    return {
        "repair_plan_id": plan_id, "diagnosis_id": None, "run_id": run_id,
        "parent_candidate_id": candidate_id,
        "root_candidate_id": parent.get("root_candidate_id") or candidate_id,
        "target_failure": target_failure, "repair_type": spec["repair_type"],
        "repair_signature": signature, "repair_path_json": _json(spec.get("repair_path", [])),
        "repair_depth": int(spec.get("repair_depth") or 1), "candidate_spec_json": _json(spec),
        "operator_requirements_json": "[]", "plan_status": "PLANNED",
        "projected_new_posts": 1, "committed_posts": 0, "consumed_posts": 0,
        "blocked_reason": None, "_virtual_source": "RESCUE_NEUTRALIZATION_PREVIEW",
        "_materialize": {"strategy": "NEUTRALIZATION_MICRO_TUNE", "target_failure": target_failure,
                         "neutralization": neutralization},
    }


def _virtual_same_family_micro_tune_plan_row(
    store: Any, config: Any, run_id: str, candidate_id: str, target_failure: str,
    candidates: Mapping[str, Mapping[str, Any]], existing_signatures: set,
) -> Optional[Dict[str, Any]]:
    parent = dict(candidates.get(candidate_id) or {})
    if not parent or str(parent.get("operator") or "").lower() != "ts_mean":
        return None
    current = resolve_effective_window(parent)
    if current is None:
        return None
    anchor = infer_ppc_branch_anchor(store, run_id, candidate_id)
    if not anchor:
        return None
    state = ppc_branch_state(store, config, run_id, anchor)
    visited_windows = {current}
    anchor_row = candidates.get(anchor)
    anchor_window = resolve_effective_window(anchor_row or {})
    if anchor_window is not None:
        visited_windows.add(anchor_window)
    for row in state.get("branch_rows", []):
        for cid in (row.get("parent_candidate_id"), row.get("child_candidate_id")):
            w = resolve_effective_window(candidates.get(str(cid or "")) or {})
            if w is not None:
                visited_windows.add(w)
        try:
            old_spec = json.loads(row.get("candidate_spec_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            old_spec = {}
        if old_spec.get("window_override") is not None:
            try:
                visited_windows.add(int(old_spec["window_override"]))
            except (TypeError, ValueError):
                pass
    cfg = ppc_branch_config(config.rules)
    if current not in cfg["same_family_windows"]:
        return None
    candidate_windows = [w for w in cfg["same_family_windows"] if w != current and w not in visited_windows]
    if not candidate_windows:
        return None
    target_window = min(candidate_windows, key=lambda w: (abs(w - current), 0 if w > current else 1, w))
    spec = same_family_micro_tune_spec(parent, target_failure, target_window)
    spec.update(_ppc_spec_metadata(store, config, run_id, candidate_id))
    if str(spec.get("expression_preview") or "") == str(parent.get("expression") or ""):
        return None
    signature = str(spec["repair_signature"])
    if signature in existing_signatures:
        return None
    plan_id = "rplan_" + hashlib.sha256(f"{run_id}|{signature}".encode()).hexdigest()[:24]
    return {
        "repair_plan_id": plan_id, "diagnosis_id": None, "run_id": run_id,
        "parent_candidate_id": candidate_id,
        "root_candidate_id": parent.get("root_candidate_id") or candidate_id,
        "target_failure": target_failure, "repair_type": spec["repair_type"],
        "repair_signature": signature, "repair_path_json": _json(spec.get("repair_path", [])),
        "repair_depth": int(spec.get("repair_depth") or 1), "candidate_spec_json": _json(spec),
        "operator_requirements_json": _json(spec.get("operator_requirements", [])),
        "plan_status": "PLANNED", "projected_new_posts": 1, "committed_posts": 0,
        "consumed_posts": 0, "blocked_reason": None,
        "_virtual_source": "RESCUE_SAME_FAMILY_PREVIEW",
        "_materialize": {"strategy": "SAME_FAMILY_MICRO_TUNE", "target_failure": target_failure},
    }


def _read_only_repair_preparation(
    store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
) -> Dict[str, Any]:
    """Preview planning-only Repair rows without DB writes or network requests."""
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    durable = [dict(x) for x in store.load_repair_plans(run_id)]
    signatures = {str(x.get("repair_signature") or "") for x in durable}
    virtual: List[Dict[str, Any]] = []
    check_preview = derive_check_repair_proposals(store, config, alpha_db, run_id, persist=False)
    for proposal in check_preview.get("proposals", []):
        row = _proposal_to_virtual_plan_row(proposal, candidates)
        signature = str(row.get("repair_signature") or "")
        if signature and signature not in signatures:
            virtual.append(row); signatures.add(signature)
    turnover_preview = preview_turnover_staged_plans(store, config, alpha_db, run_id, round_id)
    for row in turnover_preview.get("virtual_plan_rows", []):
        signature = str(row.get("repair_signature") or "")
        if signature and signature not in signatures:
            virtual.append(dict(row)); signatures.add(signature)
    return {
        "virtual_plan_rows": virtual,
        "evaluation_complete": bool(turnover_preview.get("evaluation_complete", True)),
        "incomplete_reasons": list(turnover_preview.get("incomplete_reasons") or []),
        "network_requests": 0, "check_requests": 0, "writes": 0,
    }


def _evaluate_repair_eligibility_core(
    store: Any, config: Any, alpha_db: Path, machine: Any, run_id: str,
    round_id: str, policy: Mapping[str, Any], remaining: int,
    external_evidence_path: Path, *, skip_uncertain: bool = False,
    extra_plan_rows: Sequence[Mapping[str, Any]] = (), emit_audit: bool = False,
) -> Dict[str, Any]:
    """Shared complete Repair eligibility logic for Actual and Shadow.

    The function never materializes RepairPlans, never performs HTTP, never
    transitions workflow state, and never submits a Simulation.  ``emit_audit``
    only preserves selector telemetry for the authoritative compatibility path.
    """
    if remaining <= 0:
        return {"selected": [], "eligible_plan_ids": [], "selected_projected": 0,
                "evaluation_complete": True, "materialization_rows": [], "ranked": [],
                "direction_ranked": [], "direction_decisions": {}, "candidates": {},
                "attempts": {}, "protected": set(), "preview_safe_plan_ids": []}
    items = classify_run(store, config, alpha_db, run_id, emit_audit=emit_audit)
    attempts = _repair_attempts_by_family(store, run_id, round_id)
    protected = {w["family_id"] for w in load_winners(store, round_id) if int(w.get("protected") or 0)}
    ext = load_external_evidence(external_evidence_path)
    ranked_pool = [dict(x) for x in items if x.get("classification") in {
        "PPL_FIXED_REPAIRABLE", "PPL_THEME_REPAIRABLE", "PPL_FIXED_AND_THEME_REPAIRABLE"
    } and x.get("repair_priority") in {"HIGH", "MEDIUM"}]
    ranked_pool = _ppc_controlled_ranked_pool(store, config, run_id, ranked_pool)
    ranked = [dict(x) for x in rank_repair_candidates(ranked_pool, policy)]

    durable = [dict(p) for p in store.load_repair_plans(run_id)]
    plans_by_signature: Dict[str, Dict[str, Any]] = {}
    plans_by_id: Dict[str, Dict[str, Any]] = {}
    for source in list(durable) + [dict(x) for x in extra_plan_rows]:
        sig = str(source.get("repair_signature") or "")
        pid = str(source.get("repair_plan_id") or "")
        if sig and sig in plans_by_signature:
            continue
        if pid and pid in plans_by_id:
            continue
        if sig: plans_by_signature[sig] = source
        if pid: plans_by_id[pid] = source
    plans = list(plans_by_id.values())
    by_parent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for plan in plans:
        if not is_retired_auto_repair_plan(plan):
            by_parent[str(plan.get("parent_candidate_id"))].append(plan)
    candidates = {str(x["candidate_id"]): dict(x) for x in store.load_candidates(run_id)}

    reverse_plans = [p for p in plans if str(p.get("repair_type") or "").upper() == "REVERSE_DIRECTION"
                     and str(p.get("plan_status") or "") in EXECUTABLE_REPAIR_STATUSES
                     and int(p.get("consumed_posts") or 0) == 0]
    reverse_keys = [str((candidates.get(str(p.get("parent_candidate_id"))) or {}).get("sim_key") or "") for p in reverse_plans]
    reverse_facts = _alpha_facts(alpha_db, [x for x in reverse_keys if x])
    direction_ranked: List[Dict[str, Any]] = []
    for plan in reverse_plans:
        cid = str(plan.get("parent_candidate_id") or "")
        parent = candidates.get(cid)
        if not parent or not parent.get("sim_key"):
            continue
        fact = reverse_facts.get(str(parent.get("sim_key"))) or {}
        dv = _direction_repair_value(fact, policy)
        if int(dv.get("score") or 0) <= 0:
            continue
        direction_ranked.append({"candidate_id": cid, "plan": plan, "sharpe": dv.get("sharpe"),
                                 "turnover": dv.get("turnover"), "round_direction_band": dv.get("band"),
                                 "round_direction_score": int(dv.get("score") or 0)})
    direction_ranked = [dict(x) for x in rank_direction_repair_candidates(direction_ranked)]

    def plan_preview(plan: Mapping[str, Any], *, audit: bool) -> Dict[str, Any]:
        return preview_production_repair_plan_rows_read_only(
            store, config, alpha_db, run_id, [dict(plan)], machine,
            emit_audit=bool(audit), enforce_global_repair_budget=bool(audit),
        )

    batch_size = int(effective_repair_allocation(policy)["batch_size"])
    selected: List[str] = []
    selected_projected = 0
    used_families = set()
    eligible_plan_ids: List[str] = []
    eligible_families = set()
    preview_safe_plan_ids: List[str] = []
    materialization_rows: List[Dict[str, Any]] = []
    direction_decisions: Dict[str, Dict[str, Any]] = {}
    direction_selection_open = True

    for item in direction_ranked:
        cid = str(item["candidate_id"]); parent = candidates.get(cid)
        if not parent: continue
        fid = family_id(parent); signal = str(parent.get("signal_family") or "")
        plan = item["plan"]; pid = str(plan.get("repair_plan_id") or "")
        reason = None; projected = 0
        if fid in protected: reason = "FAMILY_ALREADY_PROTECTED"
        elif attempts.get(signal, 0) >= 1: reason = "DIRECTION_REPAIR_ALREADY_ATTEMPTED_FOR_FAMILY"
        else:
            try:
                prev = plan_preview(plan, audit=(emit_audit and direction_selection_open))
            except (ConfigError, ValueError) as exc:
                reason = f"DIRECTION_REPAIR_PREVIEW_BLOCKED:{type(exc).__name__}"; prev = {}
            if reason is None and not prev.get("items"): reason = "DIRECTION_REPAIR_PREVIEW_EMPTY"
            if reason is None and skip_uncertain and any(str(x.get("required_action") or "") == "HOLD_UNCERTAIN" for x in (prev.get("items") or [])):
                reason = "UNCERTAIN_SUBMISSION_LOCAL_QUARANTINE"
            projected = int(prev.get("projected_new_posts") or 0) if reason is None else 0
            if reason is None:
                preview_safe_plan_ids.append(pid)
                if signal not in eligible_families and projected <= remaining:
                    eligible_plan_ids.append(pid); eligible_families.add(signal)
                elif projected > remaining:
                    reason = "REPAIR_BUDGET_REMAINING_TOO_SMALL"
        decision_reason = reason
        if direction_selection_open:
            if reason is None and signal in used_families:
                decision_reason = "ANOTHER_PARENT_FROM_FAMILY_SELECTED"
            elif reason is None and selected_projected + projected <= remaining:
                selected.append(pid); selected_projected += projected; used_families.add(signal)
            elif reason is None:
                decision_reason = "REPAIR_BUDGET_REMAINING_TOO_SMALL"
            direction_decisions[cid] = {**item, "selected": pid in selected, "reason": decision_reason or pid}
            if len(selected) >= batch_size:
                direction_selection_open = False

    existing_signatures = {str(p.get("repair_signature") or "") for p in plans}
    normal_selection_open = True
    for item in ranked:
        cid = str(item["candidate_id"]); parent = candidates.get(cid)
        if not parent: continue
        fid = family_id(parent); signal = str(parent.get("signal_family") or "")
        if fid in protected or signal in used_families:
            continue
        repair_planning = effective_repair_planning(policy)
        cap = int(repair_planning["strong_near_pass_repair_cap_per_family"] if item.get("repair_priority") == "HIGH"
                  else repair_planning["normal_near_pass_repair_cap_per_family"])
        staged_for_parent = [p for p in by_parent.get(cid, []) if str(p.get("repair_type") or "") in TURNOVER_STAGED_STRATEGIES]
        uncertain_retry_plans = [p for p in by_parent.get(cid, []) if str(p.get("blocked_reason") or "") == "UNCERTAIN_RETRY_AUTHORIZED"
                                 and str(p.get("plan_status") or "") in EXECUTABLE_REPAIR_STATUSES
                                 and int(p.get("consumed_posts") or 0) == 0]
        is_ppc_branch = str(item.get("primary_failure") or "").upper() == PPC_TARGET_FAILURE
        ppc_attempts_used = int(item.get("_ppc_attempts_used") or 0)
        ppc_max_attempts = int(item.get("_ppc_max_attempts") or ppc_branch_config(config.rules)["max_attempts"])
        cap_reached = (ppc_attempts_used >= ppc_max_attempts) if is_ppc_branch else (attempts.get(signal, 0) >= cap)
        if cap_reached and not staged_for_parent and not uncertain_retry_plans:
            continue
        if not staged_for_parent and not uncertain_retry_plans:
            rescue = preview_rescue(
                store, config, alpha_db, run_id, cid, ext,
                emit_audit=(emit_audit and normal_selection_open),
            )
            if not rescue.get("allowed_to_execute"):
                continue
            recommendation = rescue.get("recommendation")
            if not isinstance(recommendation, Mapping):
                legacy = rescue.get("recommended_strategy")
                if isinstance(legacy, Mapping): recommendation = dict(legacy)
                elif isinstance(legacy, str) and legacy:
                    recommendation = {"strategy": legacy, "change": rescue.get("recommended_change") or {}}
                else: recommendation = {}
            virtual = None
            if recommendation.get("strategy") == "NEUTRALIZATION_MICRO_TUNE":
                target = (recommendation.get("change") or {}).get("neutralization")
                rescue_failure = str(rescue.get("target_failure") or item.get("primary_failure") or "")
                if target and rescue_failure != "HT_RETURNS_RATIO_FAIL":
                    virtual = _virtual_neutralization_plan_row(store, config, run_id, cid, rescue_failure,
                                                               str(target), candidates, existing_signatures)
            elif recommendation.get("strategy") == "SAME_FAMILY_MICRO_TUNE":
                rescue_failure = str(rescue.get("target_failure") or item.get("primary_failure") or "")
                if rescue_failure == "PP_CORRELATION_FAIL":
                    virtual = _virtual_same_family_micro_tune_plan_row(store, config, run_id, cid, rescue_failure,
                                                                       candidates, existing_signatures)
            if virtual is not None:
                if normal_selection_open:
                    materialization_rows.append(virtual)
                plans.append(virtual); by_parent[cid].append(virtual)
                existing_signatures.add(str(virtual.get("repair_signature") or ""))
        candidate_plans = [p for p in by_parent.get(cid, []) if str(p.get("plan_status") or "") in EXECUTABLE_REPAIR_STATUSES
                           and int(p.get("consumed_posts") or 0) == 0 and not is_retired_auto_repair_plan(p)
                           and not (skip_uncertain and str(p.get("blocked_reason") or "") == "UNCERTAIN_SUBMISSION_HOLD")]
        type_rank = {"NEUTRALIZATION_MICRO_TUNE": 0, "SAME_FAMILY_MICRO_TUNE": 1,
                     "TURNOVER_DECAY_STEP_1": 2, "TURNOVER_DECAY_STEP_2": 2, "TURNOVER_HUMP": 2}
        candidate_plans.sort(key=lambda p: (type_rank.get(str(p.get("repair_type")), 9), str(p.get("repair_plan_id"))))
        chosen = None; best_preview = None
        for plan in candidate_plans:
            try:
                prev = plan_preview(plan, audit=(emit_audit and normal_selection_open))
            except (ConfigError, ValueError) as exc:
                if emit_audit and normal_selection_open:
                    audit_event(action="REPAIR_PREVIEW_BLOCKED", run_id=run_id, round_id=round_id,
                                candidate_id=cid, repair_plan_id=str(plan.get("repair_plan_id") or ""),
                                repair_strategy=str(plan.get("repair_type") or ""), reason=f"{type(exc).__name__}: {exc}")
                continue
            if not prev.get("items"): continue
            action = prev["items"][0].get("required_action")
            if skip_uncertain and str(action or "") == "HOLD_UNCERTAIN":
                if emit_audit and normal_selection_open:
                    audit_event(action="REPAIR_UNCERTAIN_QUARANTINED", run_id=run_id, round_id=round_id,
                                candidate_id=cid, repair_plan_id=str(plan.get("repair_plan_id") or ""),
                                repair_strategy=str(plan.get("repair_type") or ""),
                                reason="UNCERTAIN_SUBMISSION_HOLD", simulation_posts=0, global_hold=False)
                continue
            action_rank = {"CACHE_COMPLETE": 0, "RESUME_EXISTING": 1, "CACHE_RESTORE": 0,
                           "NEW_SIMULATION_REQUIRED": 2, "RETRY_PER_V21_POLICY": 3}.get(str(action), 4)
            rank = (action_rank, type_rank.get(str(plan.get("repair_type")), 9))
            if best_preview is None or rank < best_preview[0]:
                best_preview = (rank, prev); chosen = plan
        if not chosen:
            continue
        pid = str(chosen["repair_plan_id"])
        projected = int((best_preview[1] if best_preview else {}).get("projected_new_posts") or 0)
        preview_safe_plan_ids.append(pid)
        if signal not in eligible_families and projected <= remaining:
            eligible_plan_ids.append(pid); eligible_families.add(signal)
        if normal_selection_open:
            if selected_projected + projected > remaining:
                continue
            selected.append(pid); selected_projected += projected; used_families.add(signal)
            if len(selected) >= batch_size:
                normal_selection_open = False

    return {
        "selected": selected, "eligible_plan_ids": eligible_plan_ids,
        "selected_projected": selected_projected, "materialization_rows": materialization_rows,
        "ranked": ranked, "direction_ranked": direction_ranked, "direction_decisions": direction_decisions,
        "candidates": candidates, "attempts": attempts, "protected": protected,
        "preview_safe_plan_ids": preview_safe_plan_ids, "evaluation_complete": True,
    }


def _materialize_selector_virtual_plans(
    store: Any, config: Any, run_id: str, rows: Sequence[Mapping[str, Any]],
) -> None:
    """Persist only rescue plans that the shared core deterministically derived."""
    for row in rows:
        materialize = dict(row.get("_materialize") or {})
        strategy = str(materialize.get("strategy") or "")
        cid = str(row.get("parent_candidate_id") or "")
        expected = str(row.get("repair_plan_id") or "")
        actual = None
        if strategy == "NEUTRALIZATION_MICRO_TUNE":
            actual = _ensure_neutralization_plan(
                store, config, run_id, cid, str(materialize.get("target_failure") or ""),
                str(materialize.get("neutralization") or ""),
            )
        elif strategy == "SAME_FAMILY_MICRO_TUNE":
            actual = _ensure_same_family_micro_tune_plan(
                store, config, run_id, cid, str(materialize.get("target_failure") or ""),
            )
        if expected and str(actual or "") != expected:
            raise ConfigError(f"REPAIR_SELECTOR_VIRTUAL_PLAN_PARITY_MISMATCH:{expected}:{actual}")


def _select_repair_batch(store: Any, config: Any, alpha_db: Path, machine: Any, run_id: str,
                         round_id: str, policy: Mapping[str, Any], remaining: int,
                         external_evidence_path: Path, *, batch_no: Optional[int] = None,
                         session: Any = None, skip_uncertain: bool = False) -> List[str]:
    if remaining <= 0:
        return []
    # Authoritative preparation is planning-only/idempotent but may persist
    # Check-derived and staged-turnover plans, and may perform an explicit GET-only
    # turnover Check refresh.  Eligibility itself is delegated to the shared core.
    derive_check_repair_proposals(store, config, alpha_db, run_id, persist=True)
    staged = sync_turnover_staged_plans(
        store, config, alpha_db, run_id, round_id,
        refresh_check=(
            (lambda candidate_id: refresh_one_pretag_check(
                store, config, machine, session, run_id, candidate_id,
                source="ROUND_REPAIR_TURNOVER_STAGE_REFRESH",
                evidence_source="GET_ONLY_HIGH_TURNOVER_REFRESH",
            )) if session is not None else None
        ),
    )
    for item in staged.get("exhausted", []):
        record_event(
            store, round_id, run_id, "HIGH_TURNOVER_AUTO_REPAIR_EXHAUSTED",
            batch_no=batch_no, phase="REPAIR", family_id_value=item["origin_signal_family"],
            payload=item,
            source_event_key=(
                f"turnover_exhausted:{run_id}:{round_id}:"
                f"{item['policy']}:{item['origin_signal_family']}"
            ),
        )

    evaluation = _evaluate_repair_eligibility_core(
        store, config, alpha_db, machine, run_id, round_id, policy, remaining,
        external_evidence_path, skip_uncertain=skip_uncertain,
        extra_plan_rows=(), emit_audit=True,
    )
    # Preserve the historical Actual selector behavior: rescue recommendations
    # are materialized durably before the selected plan IDs leave this function.
    _materialize_selector_virtual_plans(
        store, config, run_id, evaluation.get("materialization_rows") or [],
    )
    selected = [str(x) for x in (evaluation.get("selected") or [])]
    selected_projected = int(evaluation.get("selected_projected") or 0)
    ranked = [dict(x) for x in (evaluation.get("ranked") or [])]
    direction_ranked = [dict(x) for x in (evaluation.get("direction_ranked") or [])]
    direction_decisions = dict(evaluation.get("direction_decisions") or {})
    candidates = {str(k): dict(v) for k, v in (evaluation.get("candidates") or {}).items()}
    attempts = dict(evaluation.get("attempts") or {})
    protected = set(evaluation.get("protected") or set())
    if batch_no is not None:
        active_repair_policy_hash = repair_policy_hash(policy)
        selected_plans = {str(p.get("repair_plan_id")): p for p in store.load_repair_plans(run_id)
                          if str(p.get("repair_plan_id")) in set(selected)}
        selected_parent_to_plan = {str(p.get("parent_candidate_id")): pid for pid, p in selected_plans.items()}
        selected_signals = {str(candidates.get(cid, {}).get("signal_family") or "") for cid in selected_parent_to_plan}
        for rank_no, item in enumerate(direction_ranked, 1):
            cid = str(item.get("candidate_id") or "")
            parent = candidates.get(cid)
            if not parent:
                continue
            decision_info = direction_decisions.get(cid, {})
            pid = str((item.get("plan") or {}).get("repair_plan_id") or "")
            selected_direction = pid in selected
            upsert_candidate_decision(
                store, round_id, run_id, int(batch_no), parent,
                decision="SELECTED_DIRECTION_REPAIR" if selected_direction else "SKIP_DIRECTION_REPAIR",
                decision_reason=str(decision_info.get("reason") or pid),
                selection_rank=rank_no,
                selection_score=float(int(item.get("round_direction_score") or 0) * 1000) + abs(float(item.get("sharpe") or 0.0)),
                quality_score=float(parent.get("initial_selection_score") or 0.0), novelty_score=0.0,
                family_score=1.0, dataset_score=0.0, operator_score=0.0, repair_risk_score=0.0,
                selection_mode="DIRECTION_REPAIR" if selected_direction else None,
                context={
                    "repair_type": "REVERSE_DIRECTION", "direction_repair_band": item.get("round_direction_band"),
                    "direction_repair_score": item.get("round_direction_score"), "sharpe": item.get("sharpe"),
                    "turnover": item.get("turnover"), "repair_plan_id": pid,
                    "remaining_repair_budget_before_batch": int(remaining),
                    "strategy_adapter": REPAIR_COMPAT_STRATEGY if (policy.get("policy_versions") or {}).get("scheduler") else None,
                    "repair_policy_version": (policy.get("policy_versions") or {}).get("repair"),
                    "repair_policy_hash": active_repair_policy_hash,
                    "scheduler_policy_version": (policy.get("policy_versions") or {}).get("scheduler"),
                },
            )
        for rank_no, item in enumerate(ranked, len(direction_ranked) + 1):
            cid = str(item.get("candidate_id") or "")
            parent = candidates.get(cid)
            if not parent:
                continue
            fid = family_id(parent); signal = str(parent.get("signal_family") or "")
            repair_planning = effective_repair_planning(policy)
            cap = int(repair_planning["strong_near_pass_repair_cap_per_family"] if item.get("repair_priority") == "HIGH"
                      else repair_planning["normal_near_pass_repair_cap_per_family"])
            if cid in selected_parent_to_plan:
                decision, reason, mode = "SELECTED_REPAIR", selected_parent_to_plan[cid], "REPAIR"
            elif fid in protected:
                decision, reason, mode = "SKIP_PROTECTED_FAMILY", "FAMILY_ALREADY_PROTECTED", None
            elif (str(item.get("primary_failure") or "").upper() == PPC_TARGET_FAILURE
                  and int(item.get("_ppc_attempts_used") or 0) >= int(item.get("_ppc_max_attempts") or ppc_branch_config(config.rules)["max_attempts"])):
                decision, reason, mode = "SKIP_REPAIR_CAP", "PPC_BRANCH_REPAIR_CAP_REACHED", None
            elif (str(item.get("primary_failure") or "").upper() != PPC_TARGET_FAILURE
                  and attempts.get(signal, 0) >= cap):
                decision, reason, mode = "SKIP_REPAIR_CAP", f"FAMILY_REPAIR_CAP_{cap}_REACHED", None
            elif signal in selected_signals:
                decision, reason, mode = "SKIP_REDUNDANT_FAMILY", "ANOTHER_PARENT_FROM_FAMILY_SELECTED", None
            else:
                decision, reason, mode = "SKIP_NO_SAFE_REPAIR_PLAN", "NO_EXECUTABLE_EVIDENCE_BACKED_REPAIR_SELECTED", None
            gap = float(item.get("max_normalized_gap") or 999.0)
            repair_value_score = int(item.get("round_repair_value_score") or 0)
            upsert_candidate_decision(
                store, round_id, run_id, int(batch_no), parent, decision=decision, decision_reason=reason,
                selection_rank=rank_no, selection_score=float(repair_value_score * 1000) - gap,
                quality_score=float(parent.get("initial_selection_score") or 0.0), novelty_score=0.0,
                family_score=1.0 if fid not in protected else 0.0, dataset_score=0.0, operator_score=0.0,
                repair_risk_score=gap, selection_mode=mode,
                context={"classification": item.get("classification"), "repair_priority": item.get("repair_priority"),
                         "repair_drivers": item.get("repair_drivers"), "max_normalized_gap": item.get("max_normalized_gap"),
                         "primary_failure": item.get("primary_failure"), "attempts_used_family": attempts.get(signal, 0),
                         "ppc_branch_anchor_candidate_id": item.get("_ppc_branch_anchor_candidate_id"),
                         "ppc_attempts_used": item.get("_ppc_attempts_used"),
                         "ppc_attempts_remaining": item.get("_ppc_attempts_remaining"),
                         "repair_cap": (item.get("_ppc_max_attempts") if str(item.get("primary_failure") or "").upper() == PPC_TARGET_FAILURE else cap),
                         "remaining_repair_budget_before_batch": int(remaining),
                         "repair_value_band": item.get("round_repair_value_band"),
                         "repair_value_score": repair_value_score, "sharpe": item.get("sharpe"),
                         "turnover": item.get("turnover"),
                         "strategy_adapter": REPAIR_COMPAT_STRATEGY if (policy.get("policy_versions") or {}).get("scheduler") else None,
                         "repair_policy_version": (policy.get("policy_versions") or {}).get("repair"),
                         "repair_policy_hash": active_repair_policy_hash,
                         "scheduler_policy_version": (policy.get("policy_versions") or {}).get("scheduler")},
            )
        record_event(store, round_id, run_id, "REPAIR_RANKING_COMPLETE", batch_no=int(batch_no), phase="REPAIR",
                     payload={"repairable_high_medium_candidates": len(ranked),
                              "direction_repair_candidates": len(direction_ranked),
                              "selected_direction_repairs": sum(1 for x in direction_ranked if str((x.get("plan") or {}).get("repair_plan_id") or "") in set(selected)),
                              "selected_plans": len(selected), "projected_new_posts": selected_projected,
                              "remaining_repair_budget": int(remaining)})
    return selected


def _sha256_file(path: Path) -> Optional[str]:
    path = Path(path)
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _code_tree_hash(project_dir: Path) -> str:
    project_dir = Path(project_dir)
    files = [project_dir / "ppl_runner.py"] + sorted((project_dir / "ppl_engine").glob("*.py"))
    h = hashlib.sha256()
    for path in files:
        if not path.exists():
            continue
        rel = path.relative_to(project_dir).as_posix().encode("utf-8")
        h.update(rel + b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def _build_manifest_payload(config: Any, policy: Mapping[str, Any], project_dir: Path,
                            run_id: str, round_id: str,
                            *, extension_context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    project_dir = Path(project_dir)
    tracked = [
        "machine_lib_V2_1.py", "ppl_runner.py", "ppl_plan_v3.yaml", "ppl_round_v3.yaml",
        "ppl_rules.yaml", "rescue_evidence.json", "VERSION.txt",
    ]
    file_hashes = {name: _sha256_file(project_dir / name) for name in tracked if (project_dir / name).exists()}
    versions = dict(policy.get("policy_versions") or {})
    versions.setdefault("telemetry", TELEMETRY_VERSION)
    payload = {
        "manifest_schema_version": 1,
        "project_version": "v3.0.4o",
        "round_id": round_id,
        "run_id": run_id,
        "captured_at": _now(),
        "execution_identity": {
            # ``execution_hash`` is retained as the historical broad V3 artifact
            # identity. V3.1 C4 adds an explicit remote Simulation semantic
            # identity plus a separate research-policy attribution hash.
            "execution_hash": config.execution_hash,
            "simulation_semantics_hash": getattr(config, "simulation_semantics_hash", ""),
            "research_policy_hash": getattr(config, "research_policy_hash", ""),
            "operational_hash": config.operational_hash,
            "presentation_hash": config.presentation_hash,
            "target_mode": config.target_mode,
            "atom_constraint_active": config.atom_constraint_active,
        },
        "code": {
            "machine_lib_expected_sha256": EXPECTED_MACHINE_HASH,
            "machine_lib_actual_sha256": _sha256_file(project_dir / "machine_lib_V2_1.py"),
            "v3_code_tree_sha256": _code_tree_hash(project_dir),
            "file_sha256": file_hashes,
        },
        "policy_versions": versions,
        "round_policy": dict(policy),
        "plan": config.plan,
        "rules": config.rules,
        "simulation_settings": dict(config.plan.get("simulation_settings") or {}),
        "budgets": dict(config.plan.get("budgets") or {}),
        "runtime": dict(config.plan.get("runtime") or {}),
        "current_theme_snapshot": dict(config.rules.get("current_theme") or {}),
        "live_rule_note": "Live /check facts recorded during the round remain authoritative over preset fallback values.",
    }
    if extension_context is not None:
        payload[EXTENSION_CONTEXT_KEY] = dict(extension_context)
    return payload


def _search_productivity_rows(store: Any, run_id: str, round_id: str, policy: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Scope-aware paid SEARCH productivity by dataset/operator/window hierarchy."""
    evidence = _round_research_evidence(store, run_id, round_id, policy)
    rows: List[Dict[str, Any]] = []
    for dimension in ("dataset", "operator", "dataset_operator", "operator_window", "dataset_operator_window"):
        for key, stat in (evidence.get(dimension) or {}).items():
            attempts = int(stat.get("attempts", 0))
            if attempts <= 0:
                continue
            row: Dict[str, Any] = {
                "scope": "ROUND_SEARCH_NEW_POST_COMPLETE",
                "dimension": dimension,
                "attempts": attempts,
                "signal_viable": int(stat.get("signal_viable", 0)),
                "local_pass": int(stat.get("local_pass", 0)),
                "fixed_repairable": int(stat.get("fixed_repairable", 0)),
                "search_viable": int(stat.get("search_viable", 0)),
                "search_strong": int(stat.get("search_strong", 0)),
                "search_elite": int(stat.get("search_elite", 0)),
                "repair_viable": int(stat.get("repair_viable", 0)),
                "repair_elite": int(stat.get("repair_elite", 0)),
                "terminal_fail": int(stat.get("terminal_fail", 0)),
                "pretag_resolved": int(stat.get("pretag_resolved", 0)),
                "ppl_near_pass": int(stat.get("ppl_near_pass", 0)),
                "ppl_strong_near_pass": int(stat.get("ppl_strong_near_pass", 0)),
                "ppl_success": int(stat.get("ppl_success", 0)),
            }
            if dimension == "dataset":
                row["dataset_id"] = key
            elif dimension == "operator":
                row["operator"] = key
            elif dimension == "dataset_operator":
                row["dataset_id"], row["operator"] = key
            elif dimension == "operator_window":
                row["operator"], row["window"] = key
            elif dimension == "dataset_operator_window":
                row["dataset_id"], row["operator"], row["window"] = key
            for count_name, rate_name in (
                ("signal_viable", "signal_viable_per_post"),
                ("local_pass", "local_pass_per_post"),
                ("fixed_repairable", "fixed_repairable_per_post"),
                ("search_viable", "search_viable_per_post"),
                ("search_strong", "search_strong_per_post"),
                ("search_elite", "search_elite_per_post"),
                ("repair_viable", "repair_viable_per_post"),
                ("repair_elite", "repair_elite_per_post"),
                ("terminal_fail", "terminal_fail_per_post"),
                ("pretag_resolved", "pretag_per_post"),
                ("ppl_near_pass", "ppl_near_per_post"),
                ("ppl_strong_near_pass", "ppl_strong_near_per_post"),
                ("ppl_success", "ppl_success_per_post"),
            ):
                row[rate_name] = round(int(row[count_name]) / attempts, 6)
            rows.append(row)
    return sorted(
        rows,
        key=lambda r: (
            r["dimension"], -r["ppl_success_per_post"], -r["ppl_near_per_post"],
            -r["pretag_per_post"], -r["local_pass_per_post"],
            str(r.get("dataset_id") or ""), str(r.get("operator") or ""), str(r.get("window") or ""),
        ),
    )


def _productivity_snapshot(store: Any, run_id: str, round_id: str, policy: Mapping[str, Any]) -> Dict[str, Any]:
    rows = _search_productivity_rows(store, run_id, round_id, policy)
    out: Dict[str, Any] = {}
    for dimension in ("dataset", "operator", "dataset_operator", "operator_window", "dataset_operator_window"):
        out[dimension] = [dict(r) for r in rows if r.get("dimension") == dimension]
    return out


def _capture_batch_snapshot(store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
                            batch_no: int, phase: str, policy: Mapping[str, Any]) -> Dict[str, Any]:
    rr = get_round(store, round_id=round_id) or {}
    candidates = [dict(c) for c in store.load_candidates(run_id)]
    winners = load_winners(store, round_id)
    near = classify_run(store, config, alpha_db, run_id)
    states = Counter(str(c.get("lifecycle_state") or "") for c in candidates)
    sim_states = Counter(str(c.get("simulation_status") or "NONE").upper() for c in candidates)
    current_decisions = [d for d in load_decisions(store, round_id) if int(d.get("batch_no") or 0) == int(batch_no)]
    decision_counts = Counter(str(d.get("decision") or "UNKNOWN") for d in current_decisions)
    selection_modes = Counter(str(d.get("selection_mode") or "NONE") for d in current_decisions if d.get("decision") == "SELECTED")
    extension_selected = Counter()
    extension_cache_restore = 0
    for decision in current_decisions:
        if str(decision.get("decision") or "") != "SELECTED":
            continue
        context = _load_json_mapping(decision.get("context_json"), error="ROUND_DECISION_CONTEXT_INVALID")
        source = str(context.get("extension_source") or "")
        if source == "CORE_OPERATOR_RESTORE":
            extension_selected["core"] += 1
        elif source == "TARGETED_OPERATOR_EXTENSION":
            extension_selected["targeted"] += 1
        if source and str(context.get("cache_action") or "") == "CACHE_RESTORE":
            extension_cache_restore += 1
    candidate_extension_source = {}
    for candidate in candidates:
        metadata = _extension_provenance_by_candidate(store, run_id, [str(candidate.get("candidate_id") or "")]).get(str(candidate.get("candidate_id") or "")) or {}
        candidate_extension_source[str(candidate.get("candidate_id") or "")] = str(metadata.get("extension_source") or "")
    extension_new_posts = Counter()
    for item in load_ledger(store, round_id):
        row = dict(item)
        if int(row.get("batch_no") or 0) != int(batch_no) or str(row.get("origin") or "").upper() != "NEW_POST":
            continue
        source = candidate_extension_source.get(str(row.get("candidate_id") or ""), "")
        if source == "CORE_OPERATOR_RESTORE":
            extension_new_posts["core"] += 1
        elif source == "TARGETED_OPERATOR_EXTENSION":
            extension_new_posts["targeted"] += 1
    rolling_extension_pruned_capacity = 0
    for refresh in load_dataset_refreshes(store, round_id):
        try:
            refresh_stats = json.loads(refresh.get("stats_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        preflight = dict((refresh_stats.get("candidate_preview_summary") or {}).get("extension_preflight") or {})
        rolling_extension_pruned_capacity += int(preflight.get("core_pruned") or 0)
        rolling_extension_pruned_capacity += int(preflight.get("targeted_pruned") or 0)
    snapshot = {
        "captured_at": _now(),
        "round_id": round_id,
        "run_id": run_id,
        "batch_no": int(batch_no),
        "phase": phase,
        "budget": {
            "total": rr.get("total_budget"), "search": rr.get("search_budget"), "repair": rr.get("repair_budget"),
            "search_consumed": rr.get("search_consumed"), "repair_consumed": rr.get("repair_consumed"),
            "remaining_total": int(rr.get("total_budget") or 0) - int(rr.get("search_consumed") or 0) - int(rr.get("repair_consumed") or 0),
        },
        "candidate_pool": {
            "total": len(candidates),
            "lifecycle": dict(states),
            "simulation": dict(sim_states),
            "eligible_planned": sum(str(c.get("lifecycle_state") or "") == "PLANNED" and str(c.get("structure_status") or "ELIGIBLE") == "ELIGIBLE" for c in candidates),
        },
        "families": {
            "protected": len([w for w in winners if int(w.get("protected") or 0)]),
            "winners": len(winners),
            "tested_distinct": len({family_id(c) for c in candidates if str(c.get("simulation_status") or "NONE").upper() not in {"", "NONE"}}),
        },
        "dataset_pool": {
            "states": load_dataset_states(store, round_id),
            "refreshes": load_dataset_refreshes(store, round_id),
            "eligible_paid_families": _eligible_paid_search_families(store, run_id, round_id),
        },
        "ppl_classification": dict(Counter(str(x.get("classification") or "UNKNOWN") for x in near)),
        "repair_priority": dict(Counter(str(x.get("repair_priority") or "NONE") for x in near)),
        "near_pass": {
            "strong": sum(x.get("evidence_label") == "STRONG_NEAR_PASS" for x in near),
            "near": sum(x.get("evidence_label") == "NEAR_PASS" for x in near),
        },
        "ppl_technically_ready": sum(x.get("classification") == "PPL_TECHNICALLY_READY" for x in near),
        "decisions": {"counts": dict(decision_counts), "selection_modes": dict(selection_modes)},
        "extensions": {
            "core_extension_selected": int(extension_selected["core"]),
            "targeted_extension_selected": int(extension_selected["targeted"]),
            "core_extension_new_post": int(extension_new_posts["core"]),
            "targeted_extension_new_post": int(extension_new_posts["targeted"]),
            "extension_cache_restore": int(extension_cache_restore),
            "rolling_extension_pruned_capacity": int(rolling_extension_pruned_capacity),
        },
        "productivity": _productivity_snapshot(store, run_id, round_id, policy),
        "failure_matrix": failure_matrix(store, run_id, round_id=round_id, batch_no=batch_no),
        "policy": {
            "objective": policy.get("objective"),
            # Historical aliases are retained for V3 snapshot compatibility,
            # but must not be mistaken for the active V3.1 Search/Repair policy.
            "legacy_aliases": {
                "batch_size": policy.get("batch_size"),
                "exploration_fraction": policy.get("exploration_fraction"),
            },
            "policy_versions": policy.get("policy_versions") or {},
            "effective_search_policy": {
                "policy_version": (policy.get("policy_versions") or {}).get("search"),
                "policy_hash": search_policy_hash(policy),
                "allocation": effective_search_allocation(policy),
                "ranking": effective_search_ranking(policy),
                "diversity": effective_search_diversity(policy),
            },
            "effective_repair_policy": {
                "policy_version": (policy.get("policy_versions") or {}).get("repair"),
                "policy_hash": repair_policy_hash(policy),
                "allocation": effective_repair_allocation(policy),
                "ranking": effective_repair_ranking(policy),
                "planning": effective_repair_planning(policy),
            },
            "active_policy_bundle": {
                ptype.lower(): (lambda state: {
                    "policy_version": state.get("policy_version") if state else None,
                    "policy_hash": state.get("policy_hash") if state else None,
                    "activated_batch_no": state.get("activated_batch_no") if state else None,
                })(load_policy_state(store, round_id, ptype))
                for ptype in ("QUALIFICATION", "SEARCH", "REPAIR")
            },
            "active_qualification": (lambda state: {
                "policy_version": state.get("policy_version") if state else None,
                "policy_hash": state.get("policy_hash") if state else None,
                "activated_batch_no": state.get("activated_batch_no") if state else None,
            })(load_policy_state(store, round_id, "QUALIFICATION")),
        },
    }
    upsert_snapshot(store, round_id, run_id, int(batch_no), phase, "BATCH_END", snapshot)
    record_event(store, round_id, run_id, "BATCH_SNAPSHOT_CAPTURED", batch_no=int(batch_no), phase=phase,
                 payload={"decision_counts": dict(decision_counts), "protected_families": snapshot["families"]["protected"]})
    return snapshot


def _sync_research_telemetry(store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
                             *, batch_no: Optional[int] = None, phase: Optional[str] = None,
                             candidate_ids: Optional[Sequence[str]] = None,
                             origin_by_candidate: Optional[Mapping[str, str]] = None,
                             selection_mode_by_candidate: Optional[Mapping[str, str]] = None,
                             repair_plan_by_candidate: Optional[Mapping[str, str]] = None,
                             policy: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    # Batch-scoped telemetry must never scan the entire run. That old behavior
    # allowed a later interrupted batch's durable facts to be inserted under
    # the batch currently being finalized. If a batch number is supplied, the
    # selected IDs stored on that batch are the authoritative scope.
    scoped_ids = [str(x) for x in (candidate_ids or []) if x]
    if batch_no is not None and not scoped_ids:
        batch = _batch_by_no(store, round_id, int(batch_no))
        if not batch:
            raise ConfigError(f"ROUND_TELEMETRY_BATCH_NOT_FOUND:{batch_no}")
        scoped_ids = _parse_json_list(batch.get("selected_candidate_ids_json"))
        if not scoped_ids:
            raise ConfigError(f"ROUND_TELEMETRY_BATCH_SCOPE_EMPTY:{batch_no}")
    near = classify_run(store, config, alpha_db, run_id)
    class_map = {str(x.get("candidate_id")): str(x.get("evidence_label") or x.get("classification"))
                 for x in near if x.get("candidate_id")}
    ledger_count = sync_simulation_ledger(
        store, alpha_db, round_id, run_id, batch_no=batch_no, phase=phase, candidate_ids=scoped_ids or None,
        origin_by_candidate=origin_by_candidate, selection_mode_by_candidate=selection_mode_by_candidate,
        repair_plan_by_candidate=repair_plan_by_candidate, classification_by_candidate=class_map,
    )
    mirrored = sync_durable_events(store, round_id, run_id)
    snapshot = None
    if batch_no is not None and phase is not None and policy is not None:
        snapshot = _capture_batch_snapshot(store, config, alpha_db, run_id, round_id, int(batch_no), phase, policy)
    return {"ledger_rows_synced": ledger_count, "durable_events_mirrored": mirrored, "snapshot": snapshot}



def _manual_queue_csv_candidates(project_dir: Path, round_id: str) -> List[str]:
    """Read the currently published manual queue exactly as the user sees it."""
    report_dir = Path(project_dir) / "reports"
    paths = [
        report_dir / round_id / "manual_finalization_queue.csv",
        report_dir / f"{round_id}_manual_finalization_queue.csv",
    ]
    for path in paths:
        if not path.exists():
            continue
        out: List[str] = []
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    cid = str((row or {}).get("candidate_id") or "").strip()
                    if cid and cid not in out:
                        out.append(cid)
        except (OSError, csv.Error):
            continue
        return out
    return []


def _latest_check_meta(store: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    with store.connect() as conn:
        row = conn.execute(
            """SELECT check_session_id,session_status,updated_at,created_at,http_request_count
               FROM ppl_check_sessions
               WHERE run_id=? AND candidate_id=? AND phase='PRE_TAG'
               ORDER BY updated_at DESC, check_session_id DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
    return dict(row) if row else {}


def _transient_unresolved_pretag_candidates(
    store: Any, run_id: str, *, min_age_seconds: float, limit: int,
) -> List[Dict[str, Any]]:
    """Return bounded latest PRE_TAG sessions that are safe to retry GET-only.

    This is intentionally derived from durable check sessions rather than the
    report classifier so a process restart can recover throttled checks before
    any new Repair POST is allocated.
    """
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT s.candidate_id,s.alpha_id,s.session_status,s.error_type,
                      s.error_nature,s.updated_at,s.check_session_id
               FROM ppl_check_sessions s
               JOIN ppl_candidates c
                 ON c.run_id=s.run_id AND c.candidate_id=s.candidate_id
               WHERE s.run_id=? AND s.phase='PRE_TAG'
                 AND upper(coalesce(s.session_status,''))!='RESOLVED'
                 AND upper(coalesce(c.simulation_status,''))='COMPLETE'
                 AND c.alpha_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM ppl_check_sessions newer
                     WHERE newer.run_id=s.run_id
                       AND newer.candidate_id=s.candidate_id
                       AND newer.phase='PRE_TAG'
                       AND (newer.updated_at>s.updated_at OR
                            (newer.updated_at=s.updated_at AND newer.check_session_id>s.check_session_id))
                 )
               ORDER BY s.updated_at ASC, s.check_session_id ASC""",
            (run_id,),
        ).fetchall()
    now = datetime.now(timezone.utc)
    out: List[Dict[str, Any]] = []
    retryable_statuses = {"BUDGET_EXHAUSTED", "TIMEOUT", "TRANSIENT_ERROR", "PENDING"}
    for raw in rows:
        row = dict(raw)
        if str(row.get("session_status") or "").upper() not in retryable_statuses:
            continue
        try:
            updated = datetime.fromisoformat(str(row.get("updated_at") or "").replace("Z", "+00:00"))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            age = max(0.0, (now - updated.astimezone(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            age = float("inf")
        if age < max(0.0, float(min_age_seconds)):
            continue
        row["age_seconds"] = age
        out.append(row)
        if len(out) >= max(0, int(limit)):
            break
    return out


def _recover_transient_pretag_checks(
    store: Any, config: Any, machine: Any, session: Any,
    run_id: str, round_id: str, *, phase: str,
) -> Dict[str, Any]:
    """Retry a small, aged slice of unresolved PRE_TAG checks without POSTs.

    The recovery is deliberately bounded so an unhealthy /check endpoint does
    not monopolize an unattended round.  Each attempt is GET-only and uses the
    same shared adaptive 429 gate as normal checks.
    """
    runtime = config.plan.get("runtime", {})
    enabled = bool(runtime.get("check_unresolved_recovery_enabled", True))
    per_cycle = max(0, int(runtime.get("check_unresolved_recovery_per_cycle", 2) or 0))
    min_age = max(0.0, float(runtime.get("check_unresolved_retry_min_age_seconds", 300) or 0))
    if not enabled or per_cycle <= 0 or session is None or str(phase).upper() != "REPAIR":
        return {"enabled": enabled, "requested": 0, "executed": 0, "resolved": 0, "reports": []}
    pending = _transient_unresolved_pretag_candidates(
        store, run_id, min_age_seconds=min_age, limit=per_cycle,
    )
    if not pending:
        return {"enabled": enabled, "requested": 0, "executed": 0, "resolved": 0, "reports": []}
    reports: List[Dict[str, Any]] = []
    for item in pending:
        cid = str(item.get("candidate_id") or "")
        try:
            report = refresh_one_pretag_check(
                store, config, machine, session, run_id, cid,
                source=f"{ROUND_SOURCE}_THROTTLE_RECOVERY",
                evidence_source="LIVE_CHECK_THROTTLE_RECOVERY",
            )
        except Exception as exc:
            # Recovery is best-effort.  Durable simulation/repair facts must
            # never be rolled back or the whole round stopped by a GET-only
            # refresh failure.
            report = {
                "executed": False, "candidate_id": cid,
                "error": f"{type(exc).__name__}: {exc}",
            }
        reports.append(report)
    resolved = sum(str(x.get("session_status") or "").upper() == "RESOLVED" for x in reports)
    record_event(
        store, round_id, run_id, "TRANSIENT_PRETAG_RECOVERY", phase=str(phase),
        payload={
            "requested": len(pending), "executed": sum(bool(x.get("executed")) for x in reports),
            "resolved": resolved, "candidate_ids": [x.get("candidate_id") for x in pending],
            "network": "GET-only /check", "simulation_posts": 0,
        },
    )
    return {
        "enabled": enabled, "requested": len(pending),
        "executed": sum(bool(x.get("executed")) for x in reports),
        "resolved": resolved, "reports": reports,
    }


def _protected_manual_queue_sets(store: Any, round_id: str) -> Tuple[set, set, set]:
    winners = load_winners(store, round_id)
    return (
        {str(w.get("candidate_id") or "") for w in winners if w.get("candidate_id")},
        {str(w.get("alpha_id") or "") for w in winners if w.get("alpha_id")},
        {str(w.get("signal_family") or "") for w in winners if w.get("signal_family")},
    )


def _refresh_manual_finalization_candidates(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
    run_id: str, round_id: str, candidate_ids: Sequence[str], *,
    trigger: str, batch_no: Optional[int] = None, exclude_candidate_ids: Sequence[str] = (),
) -> Dict[str, Any]:
    """GET-only refresh of an existing manual-finalization queue slice."""
    candidates = {str(x.get("candidate_id")): x for x in store.load_candidates(run_id)}
    protected_cids, protected_alphas, protected_signals = _protected_manual_queue_sets(store, round_id)
    excluded = {str(x) for x in exclude_candidate_ids}
    max_candidates = int(config.plan["budgets"].get("max_check_candidates", 0))
    max_requests = int(config.plan["budgets"].get("max_check_http_requests", 0))
    used_candidates, used_requests = _check_budget_used(store, run_id)
    reports: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    seen_alpha: set = set()
    requested_ids = [str(x) for x in candidate_ids if x]
    progress_total = max(1, len(requested_ids))

    for cid in requested_ids:
        if cid in excluded:
            skipped.append({"candidate_id": cid, "reason": "ALREADY_FRESH_THIS_BATCH"})
            continue
        cand = candidates.get(cid)
        if not cand:
            skipped.append({"candidate_id": cid, "reason": "CANDIDATE_NOT_FOUND"})
            continue
        alpha_id = str(cand.get("alpha_id") or "")
        signal = str(cand.get("signal_family") or "")
        if cid in protected_cids or alpha_id in protected_alphas or (signal and signal in protected_signals):
            skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "PROTECTED_OR_SUBMITTED"})
            continue
        if not alpha_id:
            skipped.append({"candidate_id": cid, "reason": "ALPHA_ID_MISSING"})
            continue
        if alpha_id in seen_alpha:
            skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "DUPLICATE_ALPHA_ID"})
            continue
        seen_alpha.add(alpha_id)
        if used_candidates >= max_candidates or used_requests >= max_requests:
            skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "CHECK_BUDGET_EXHAUSTED"})
            break
        ordinal = len(reports) + 1
        _check_progress_line("PRE-TAG REFRESH", max(0, ordinal - 1), progress_total,
                             candidate_id=cid, alpha_id=alpha_id, state=f"START {ordinal}")
        try:
            report = refresh_one_pretag_check(
                store, config, machine, session, run_id, cid,
                source=f"{ROUND_SOURCE}_{trigger}", evidence_source=f"LIVE_CHECK_REFRESH_{trigger}",
            )
        except Exception as exc:
            # A GET-only queue refresh is observational.  It must never abort
            # an otherwise durable SEARCH/REPAIR round; leave the candidate
            # unresolved and allow the transient recovery loop to revisit it.
            report = {
                "executed": False, "candidate_id": cid, "alpha_id": alpha_id,
                "session_status": "TRANSIENT_ERROR",
                "error": f"{type(exc).__name__}: {exc}",
            }
        reports.append(report)
        state = str(report.get("session_status") or ("EXECUTED" if report.get("executed") else "DONE"))
        _check_progress_line("PRE-TAG REFRESH", len(reports), progress_total,
                             candidate_id=cid, alpha_id=alpha_id, state=state)
        used_candidates, used_requests = _check_budget_used(store, run_id)

    record_event(
        store, round_id, run_id, "MANUAL_FINALIZATION_QUEUE_REFRESH",
        batch_no=batch_no, phase=str((get_round(store, round_id=round_id) or {}).get("phase") or "SEARCH"),
        payload={
            "trigger": trigger, "requested": len(requested_ids),
            "executed": sum(bool(x.get("executed")) for x in reports),
            "resolved": sum(str(x.get("session_status") or "").upper() == "RESOLVED" for x in reports),
            "skipped": skipped, "simulation_posts": 0, "property_writes": 0, "submit_requests": 0,
        },
    )
    return {
        "trigger": trigger, "batch_no": batch_no, "requested": len(requested_ids),
        "executed_check_count": sum(bool(x.get("executed")) for x in reports),
        "resolved_check_count": sum(str(x.get("session_status") or "").upper() == "RESOLVED" for x in reports),
        "reports": reports, "skipped": skipped,
        "side_effects": {
            "network": "GET-only /check plus authentication session",
            "simulation_posts": 0, "patch_requests": 0, "submit_requests": 0,
            "power_pool_selected_requests": 0, "local_db_writes": "new check sessions + audit/report facts only",
        },
    }


def _manual_refresh_interval(policy: Mapping[str, Any]) -> int:
    return int((((policy.get("ppl_classification") or {}).get("manual_finalization") or {}).get("auto_refresh_every_batches") or 10))


_MANUAL_REFRESH_PPL_KEYS = {
    "fixed_gates", "theme_specific", "final_theme_check", "non_ppl_diagnostics",
    "automation_ignored", "manual_finalization", "repair_priority",
}
_MANUAL_REFRESH_MANUAL_KEYS = {
    "enabled", "description_checks", "pretag_theme_outcomes", "ppc_strategy",
}


def _manual_refresh_semantic_policy_identity(policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Bounded policy identity for interpreting check/manual eligibility facts."""
    ppl = dict(policy.get("ppl_classification") or {})
    manual = dict(ppl.get("manual_finalization") or {})
    return {
        "policy_version": (policy.get("policy_versions") or {}).get("ppl_classification"),
        "fixed_gates": ppl.get("fixed_gates"),
        "theme_specific": ppl.get("theme_specific"),
        "final_theme_check": ppl.get("final_theme_check"),
        "non_ppl_diagnostics": ppl.get("non_ppl_diagnostics"),
        "automation_ignored": ppl.get("automation_ignored"),
        "manual_finalization": {key: manual.get(key) for key in sorted(_MANUAL_REFRESH_MANUAL_KEYS)},
        "repair_priority": ppl.get("repair_priority"),
    }


def _manual_refresh_policy_compatibility(
    stored: Mapping[str, Any], current: Mapping[str, Any],
) -> Dict[str, Any]:
    """Fail closed on check-semantic or unknown PPL policy drift.

    Round scheduling/report fields are intentionally outside this identity.
    The one explicitly non-semantic manual queue field is refresh cadence.
    """
    stored_ppl = dict(stored.get("ppl_classification") or {})
    current_ppl = dict(current.get("ppl_classification") or {})
    unknown_ppl_keys = sorted((set(stored_ppl) | set(current_ppl)) - _MANUAL_REFRESH_PPL_KEYS)
    changed_unknown = [key for key in unknown_ppl_keys if stored_ppl.get(key) != current_ppl.get(key)]
    stored_manual = dict(stored_ppl.get("manual_finalization") or {})
    current_manual = dict(current_ppl.get("manual_finalization") or {})
    allowed_manual = _MANUAL_REFRESH_MANUAL_KEYS | {"auto_refresh_every_batches"}
    unknown_manual_keys = sorted((set(stored_manual) | set(current_manual)) - allowed_manual)
    changed_unknown.extend(
        f"manual_finalization.{key}"
        for key in unknown_manual_keys if stored_manual.get(key) != current_manual.get(key)
    )
    stored_identity = _manual_refresh_semantic_policy_identity(stored)
    current_identity = _manual_refresh_semantic_policy_identity(current)
    compatible = not changed_unknown and _json(stored_identity) == _json(current_identity)
    return {
        "compatible": compatible,
        "stored_identity": stored_identity,
        "current_identity": current_identity,
        "changed_unknown_paths": changed_unknown,
        "allowed_drift": ["ppl_classification.manual_finalization.auto_refresh_every_batches",
                          "round scheduling/report/telemetry fields"],
    }


def preflight_manual_finalization_refresh(
    store: Any, config: Any, machine_path: Path, round_policy_path: Path, *, run_id: str,
) -> Dict[str, Any]:
    """Local, read-only safety gate that must run before authentication."""
    if not store.path.exists():
        raise ConfigError("ppl_runner.db does not exist")
    if not store.get_run(run_id):
        raise ConfigError("V3_RUN_NOT_FOUND")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    hash_result = validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_MANUAL_REFRESH,
        config=config, run_id=run_id, round_id=str(rr["round_id"]),
    )
    current_policy = load_round_policy(round_policy_path, config)
    stored_policy = json.loads(rr["config_json"])
    policy_compatibility = _manual_refresh_policy_compatibility(stored_policy, current_policy)
    if not policy_compatibility["compatible"]:
        raise ConfigError("MANUAL_FINALIZATION_REFRESH_POLICY_DRIFT")
    return {
        "run_id": run_id, "round_id": str(rr["round_id"]),
        "round": rr, "stored_policy": stored_policy, "current_policy": current_policy,
        "hash_result": hash_result, "policy_compatibility": policy_compatibility,
    }


_QUALIFIED_CHECK_REFRESH_CLASSIFICATIONS = {
    "PPL_CHECK_UNRESOLVED",
    "PPL_THEME_UNRESOLVED",
    "PPL_TECHNICALLY_READY",
    "PPL_READY_FOR_MANUAL_FINALIZATION",
    "PPL_STRATEGY_REJECT_HIGH_PPC",
    "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE",
}
_QUALIFIED_CHECK_REFRESH_LIFECYCLES = {
    "PRE_TAG_CHECK_PENDING",
    "PRE_TAG_CHECK_COMPLETE",
    "PRE_TAG_CHECK_PASS",
}


def _qualified_check_poll_state(poll: Mapping[str, Any]) -> str:
    """Format one compact Qualified Refresh GET observation; never include payload JSON."""
    poll_no = int(poll.get("semantic_poll_index") or 0)
    http_status = int(poll.get("http_status") or 0)
    parsed = dict(poll.get("parsed") or {})
    parse_status = str(parsed.get("parse_status") or "")
    semantic = str(parsed.get("session_semantic_status") or "").upper()
    server_retry_after = poll.get("server_retry_after_seconds", poll.get("retry_after_seconds"))
    effective_retry_after = poll.get("effective_retry_after_seconds")

    def seconds(value: Any) -> str:
        try:
            return f"{float(value):.1f}s"
        except (TypeError, ValueError):
            return "unknown"

    if parse_status == "HTTP_200_EMPTY_BODY_RETRY":
        server = f" | server_retry_after={seconds(server_retry_after)}" if server_retry_after is not None else ""
        effective = (
            f" | retry_after={seconds(effective_retry_after)}"
            if effective_retry_after is not None else ""
        )
        return f"poll={poll_no} | HTTP {http_status} EMPTY{server}{effective}"
    if http_status == 429:
        server = f" | server_retry_after={seconds(server_retry_after)}" if server_retry_after is not None else ""
        return f"poll={poll_no} | HTTP 429{server} | THROTTLED"
    if semantic == "RESOLVED":
        results = {str(x.get("normalized_name") or ""): x for x in parsed.get("results", [])}
        pp = results.get("POWER_POOL_CORRELATION")
        ppc = _check_result_display_num(
            pp, normalized_key="normalized_value", raw_json_key="raw_value_json", raw_key="raw_value",
        )
        suffix = f" | PPC={ppc:g}" if ppc is not None else ""
        return f"poll={poll_no} | RESOLVED{suffix}"
    return f"poll={poll_no} | HTTP {http_status} | {semantic or parse_status or 'PENDING'}"


def preflight_qualified_check_refresh(
    store: Any, config: Any, machine_path: Path, round_policy_path: Path, *, run_id: str,
) -> Dict[str, Any]:
    """Read-only safety gate for an explicit paused-run qualified Check refresh."""
    out = dict(preflight_manual_finalization_refresh(
        store, config, machine_path, round_policy_path, run_id=run_id,
    ))
    rr = dict(out["round"])
    run = dict(store.get_run(run_id) or {})
    if str(rr.get("status") or "").upper() != "PAUSED":
        raise ConfigError("QUALIFIED_CHECK_REFRESH_REQUIRES_PAUSED_ROUND")
    if run and str(run.get("status") or "").upper() != "PAUSED":
        raise ConfigError("QUALIFIED_CHECK_REFRESH_REQUIRES_PAUSED_RUN")
    out["run"] = run
    out["qualified_refresh_preflight"] = True
    return out


def _qualified_check_refresh_targets(
    store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
) -> Dict[str, Any]:
    """Pick high-value PRE_TAG Alpha identities that are worth refreshing now.

    Selection is intentionally workflow-aware: candidates that never passed the
    local pre-gate (for example HIGH_TURNOVER repair parents) are not allowed to
    consume /check capacity.  One platform GET is emitted per unambiguous Alpha
    identity within the run; local candidate evidence is never guessed across
    duplicate candidate identities.
    """
    candidates = {str(x.get("candidate_id")): dict(x) for x in store.load_candidates(run_id)}
    classified = classify_run(store, config, alpha_db, run_id, emit_audit=False)
    protected_cids, protected_alphas, protected_signals = _protected_manual_queue_sets(store, round_id)
    by_alpha: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    skipped: List[Dict[str, Any]] = []

    for raw in classified:
        row = dict(raw)
        classification = str(row.get("classification") or "")
        if classification not in _QUALIFIED_CHECK_REFRESH_CLASSIFICATIONS:
            continue
        cid = str(row.get("candidate_id") or "")
        cand = candidates.get(cid) or {}
        lifecycle = str(cand.get("lifecycle_state") or "")
        if lifecycle not in _QUALIFIED_CHECK_REFRESH_LIFECYCLES:
            skipped.append({
                "candidate_id": cid,
                "alpha_id": str(row.get("alpha_id") or cand.get("alpha_id") or ""),
                "reason": f"LIFECYCLE_NOT_QUALIFIED:{lifecycle}",
            })
            continue
        alpha_id = str(row.get("alpha_id") or cand.get("alpha_id") or "").strip()
        signal = str(cand.get("signal_family") or "")
        if not alpha_id:
            skipped.append({"candidate_id": cid, "reason": "ALPHA_ID_MISSING"})
            continue
        if cid in protected_cids or alpha_id in protected_alphas or (signal and signal in protected_signals):
            skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "PROTECTED_OR_SUBMITTED"})
            continue
        item = {
            "candidate_id": cid,
            "alpha_id": alpha_id,
            "classification": classification,
            "lifecycle_state": lifecycle,
            "sharpe": float(row.get("sharpe") or 0.0),
            "fitness": float(row.get("fitness") or 0.0),
            "turnover": row.get("turnover"),
            "signal_family": signal,
        }
        by_alpha[alpha_id].append(item)

    targets: List[Dict[str, Any]] = []
    for alpha_id, items in sorted(by_alpha.items()):
        if len(items) != 1:
            for item in items:
                skipped.append({
                    "candidate_id": item["candidate_id"], "alpha_id": alpha_id,
                    "reason": "SAME_ALPHA_ID_MULTIPLE_CANDIDATES_NO_LOCAL_REUSE",
                })
            continue
        targets.append(items[0])

    targets.sort(key=lambda x: (
        -float(x.get("sharpe") or 0.0),
        -float(x.get("fitness") or 0.0),
        float(x.get("turnover") if x.get("turnover") is not None else 999.0),
        str(x.get("candidate_id") or ""),
    ))
    return {"targets": targets, "skipped": skipped}


_QUALIFIED_CAMPAIGN_STARTED = "QUALIFIED_CHECK_REFRESH_CAMPAIGN_STARTED"
_QUALIFIED_CAMPAIGN_RESOLVED = "QUALIFIED_CHECK_REFRESH_TARGET_RESOLVED"
_QUALIFIED_CAMPAIGN_DEFERRED = "QUALIFIED_CHECK_REFRESH_TARGET_DEFERRED"
_QUALIFIED_CAMPAIGN_INTERRUPTED = "QUALIFIED_CHECK_REFRESH_CAMPAIGN_INTERRUPTED"
_QUALIFIED_CAMPAIGN_COMPLETED = "QUALIFIED_CHECK_REFRESH_CAMPAIGN_COMPLETED"
_QUALIFIED_CAMPAIGN_SUPERSEDED = "QUALIFIED_CHECK_REFRESH_CAMPAIGN_SUPERSEDED"


def _qualified_campaign_evidence_source(campaign_id: str) -> str:
    return f"LIVE_CHECK_REFRESH_QUALIFIED:{campaign_id}"


def _load_active_qualified_refresh_campaign(
    store: Any, run_id: str, round_id: str,
) -> Optional[Dict[str, Any]]:
    """Rebuild the latest active campaign from generic durable round events."""
    event_types = (
        _QUALIFIED_CAMPAIGN_STARTED, _QUALIFIED_CAMPAIGN_RESOLVED,
        _QUALIFIED_CAMPAIGN_DEFERRED, _QUALIFIED_CAMPAIGN_INTERRUPTED,
        _QUALIFIED_CAMPAIGN_COMPLETED, _QUALIFIED_CAMPAIGN_SUPERSEDED,
    )
    placeholders = ",".join("?" for _ in event_types)
    with store.connect() as conn:
        rows = conn.execute(
            f"""SELECT event_id,event_type,candidate_id,alpha_id,payload_json,created_at
                  FROM ppl_round_events
                 WHERE run_id=? AND round_id=? AND event_type IN ({placeholders})
                 ORDER BY event_id""",
            (run_id, round_id, *event_types),
        ).fetchall()
    starts: List[Dict[str, Any]] = []
    terminal: set[str] = set()
    resolved_by_event: Dict[str, set[Tuple[str, str]]] = defaultdict(set)
    deferred_by_event: Dict[str, set[Tuple[str, str]]] = defaultdict(set)
    interrupted_by_event: Dict[str, set[Tuple[str, str]]] = defaultdict(set)
    for raw in rows:
        row = dict(raw)
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        campaign_id = str(payload.get("campaign_id") or "")
        if not campaign_id:
            continue
        if row["event_type"] == _QUALIFIED_CAMPAIGN_STARTED:
            starts.append({**payload, "created_at": row.get("created_at")})
        elif row["event_type"] in {_QUALIFIED_CAMPAIGN_COMPLETED, _QUALIFIED_CAMPAIGN_SUPERSEDED}:
            terminal.add(campaign_id)
        elif row["event_type"] == _QUALIFIED_CAMPAIGN_RESOLVED:
            resolved_by_event[campaign_id].add((
                str(row.get("candidate_id") or payload.get("candidate_id") or ""),
                str(row.get("alpha_id") or payload.get("alpha_id") or ""),
            ))
        elif row["event_type"] == _QUALIFIED_CAMPAIGN_DEFERRED:
            deferred_by_event[campaign_id].add((
                str(row.get("candidate_id") or payload.get("candidate_id") or ""),
                str(row.get("alpha_id") or payload.get("alpha_id") or ""),
            ))
        elif row["event_type"] == _QUALIFIED_CAMPAIGN_INTERRUPTED:
            interrupted_by_event[campaign_id].add((
                str(row.get("candidate_id") or payload.get("candidate_id") or ""),
                str(row.get("alpha_id") or payload.get("alpha_id") or ""),
            ))
    active = next((item for item in reversed(starts)
                   if str(item.get("campaign_id") or "") not in terminal), None)
    if active is None:
        return None
    campaign_id = str(active["campaign_id"])
    targets = [dict(item) for item in (active.get("target_snapshot") or [])]
    resolved = set(resolved_by_event.get(campaign_id) or set())
    # A resolved campaign-scoped Check session is also authoritative. This
    # closes the tiny crash window between saving its poll/session transaction
    # and appending the corresponding round event.
    evidence_source = _qualified_campaign_evidence_source(campaign_id)
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT DISTINCT s.candidate_id,s.alpha_id
                 FROM ppl_check_sessions s
                 JOIN ppl_check_polls p ON p.check_session_id=s.check_session_id
                WHERE s.run_id=? AND s.session_status='RESOLVED' AND p.evidence_source=?""",
            (run_id, evidence_source),
        ).fetchall()
    resolved.update((str(row[0] or ""), str(row[1] or "")) for row in rows)
    valid_keys = {(str(item.get("candidate_id") or ""), str(item.get("alpha_id") or "")) for item in targets}
    resolved.intersection_update(valid_keys)
    deferred = set(deferred_by_event.get(campaign_id) or set()).intersection(valid_keys) - resolved
    interrupted = set(interrupted_by_event.get(campaign_id) or set()).intersection(valid_keys) - resolved
    return {
        "campaign_id": campaign_id,
        "started_at": active.get("started_at") or active.get("created_at"),
        "targets": targets,
        "skipped": [dict(item) for item in (active.get("selector_skipped") or [])],
        "total_count": len(targets),
        "resolved_keys": resolved,
        "deferred_keys": deferred,
        "interrupted_keys": interrupted,
    }


def _start_qualified_refresh_campaign(
    store: Any, run_id: str, round_id: str, targets: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    started_at = _now()
    snapshot = [dict(item) for item in targets]
    identity = _json([
        [str(item.get("candidate_id") or ""), str(item.get("alpha_id") or "")]
        for item in snapshot
    ])
    campaign_id = "qcr_" + hashlib.sha256(
        f"{run_id}|{round_id}|{started_at}|{identity}".encode("utf-8")
    ).hexdigest()[:24]
    payload = {
        "campaign_id": campaign_id, "run_id": run_id, "started_at": started_at,
        "target_snapshot": snapshot, "target_order": [
            str(item.get("candidate_id") or "") for item in snapshot
        ],
        "total_count": len(snapshot), "campaign_status": "ACTIVE",
        "selector_skipped": [dict(item) for item in skipped],
    }
    record_event(
        store, round_id, run_id, _QUALIFIED_CAMPAIGN_STARTED,
        payload=payload,
        source_event_key=f"qualified_check_campaign_started:{campaign_id}",
    )
    return {
        "campaign_id": campaign_id, "started_at": started_at,
        "targets": snapshot, "skipped": [dict(item) for item in skipped],
        "total_count": len(snapshot), "resolved_keys": set(),
        "deferred_keys": set(), "interrupted_keys": set(),
    }


def _close_qualified_refresh_campaign(
    store: Any, round_id: str, run_id: str, campaign_id: str, event_type: str,
    *, payload: Optional[Mapping[str, Any]] = None,
) -> None:
    body = {"campaign_id": campaign_id, **dict(payload or {})}
    record_event(
        store, round_id, run_id, event_type, payload=body,
        source_event_key=f"{event_type.lower()}:{campaign_id}",
    )


def _sync_qualified_refresh_resolution(
    store: Any, run_id: str, candidate_id: str, alpha_id: str, report: Mapping[str, Any],
) -> None:
    """Close stale PRE_TAG work only after a fresh qualified refresh resolves."""
    if str(report.get("session_status") or "").upper() != "RESOLVED":
        return
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_check_work
               SET queue_state='RESOLVED',next_check_at=NULL,last_error=NULL,
                   retry_after_seconds=NULL,updated_at=?
               WHERE run_id=? AND candidate_id=? AND alpha_id=? AND phase='PRE_TAG'""",
            (now, run_id, candidate_id, alpha_id),
        )
    cand = next((dict(x) for x in store.load_candidates(run_id)
                 if str(x.get("candidate_id") or "") == str(candidate_id)), None)
    if not cand or str(cand.get("lifecycle_state") or "") != "PRE_TAG_CHECK_PENDING":
        return
    store.transition_candidate(
        candidate_id, "PRE_TAG_CHECK_COMPLETE",
        reason="Qualified GET-only PRE_TAG refresh resolved",
        source="V31_QUALIFIED_CHECK_REFRESH", allowed=CANDIDATE_TRANSITIONS,
    )
    if str(report.get("base_gate") or "") == "PASS" and str(report.get("theme_gate") or "") == "PASS":
        store.transition_candidate(
            candidate_id, "PRE_TAG_CHECK_PASS",
            reason="Qualified refresh resolved live PRE_TAG gates passed",
            source="V31_QUALIFIED_CHECK_REFRESH", allowed=CANDIDATE_TRANSITIONS,
        )


def refresh_qualified_checks(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path, machine_path: Path,
    round_policy_path: Path, project_dir: Path, *, run_id: str,
    preflight: Optional[Mapping[str, Any]] = None, authentication_post_count: int = 0,
    max_candidates: Optional[int] = None, force_new_campaign: bool = False,
) -> Dict[str, Any]:
    """Explicit paused-run GET-only refresh with durable campaign resume."""
    local_preflight = dict(preflight or preflight_qualified_check_refresh(
        store, config, machine_path, round_policy_path, run_id=run_id,
    ))
    if str(local_preflight.get("run_id")) != str(run_id):
        raise ConfigError("QUALIFIED_CHECK_REFRESH_PREFLIGHT_RUN_MISMATCH")
    rr = dict(local_preflight["round"])
    round_id = str(rr["round_id"])
    # Reports must retain the durable run policy; semantic compatibility with
    # the supplied current policy was already proven by preflight.
    policy = dict(local_preflight["stored_policy"])
    current_policy = dict(local_preflight.get("current_policy") or policy)
    hash_result = dict(local_preflight["hash_result"])
    active_campaign = _load_active_qualified_refresh_campaign(store, run_id, round_id)
    if force_new_campaign and active_campaign is not None:
        _close_qualified_refresh_campaign(
            store, round_id, run_id, str(active_campaign["campaign_id"]),
            _QUALIFIED_CAMPAIGN_SUPERSEDED,
            payload={"reason": "EXPLICIT_FORCE_NEW_CAMPAIGN"},
        )
        active_campaign = None
    resumed = active_campaign is not None
    if active_campaign is None:
        selected = _qualified_check_refresh_targets(store, config, alpha_db, run_id, round_id)
        targets = list(selected["targets"])
        skipped = list(selected["skipped"])
        if max_candidates is not None and int(max_candidates) > 0:
            deferred = targets[int(max_candidates):]
            targets = targets[:int(max_candidates)]
            skipped.extend({
                "candidate_id": x["candidate_id"], "alpha_id": x["alpha_id"],
                "reason": "QUALIFIED_REFRESH_CAMPAIGN_SNAPSHOT_LIMIT",
            } for x in deferred)
        campaign = _start_qualified_refresh_campaign(
            store, run_id, round_id, targets, skipped,
        )
    else:
        campaign = active_campaign
        targets = list(campaign["targets"])
        skipped = list(campaign["skipped"])

    campaign_id = str(campaign["campaign_id"])
    resolved_keys = set(campaign.get("resolved_keys") or set())
    durable_deferred_keys = set(campaign.get("deferred_keys") or set())
    durable_interrupted_keys = set(campaign.get("interrupted_keys") or set())
    pending_targets = [
        target for target in targets
        if (str(target.get("candidate_id") or ""), str(target.get("alpha_id") or "")) not in resolved_keys
    ]
    retry_floor = parse_continuous_policy(current_policy).qualified_check_min_retry_after_seconds

    max_http_requests = max(1, int(config.plan.get("budgets", {}).get("max_check_http_requests", 20000)))
    reports: List[Dict[str, Any]] = []
    used_http_requests = 0
    stopped_reason: Optional[str] = None
    progress_total = max(1, len(targets))
    deferred_count = 0
    resolved_this_invocation = 0
    processed_this_invocation = 0
    current_target: Optional[Dict[str, Any]] = None

    mode = "RESUME" if resumed else "START"
    print(
        f"QUALIFIED CHECK REFRESH {mode} campaign={campaign_id} | "
        f"completed={len(resolved_keys)} | remaining={len(pending_targets)} | "
        f"deferred={len(durable_deferred_keys)} | interrupted={len(durable_interrupted_keys)} | total={len(targets)}",
        flush=True,
    )

    try:
        for target in pending_targets:
            current_target = dict(target)
            cid = str(target["candidate_id"]); alpha_id = str(target["alpha_id"])
            target_key = (cid, alpha_id)
            durable_deferred_keys.discard(target_key)
            durable_interrupted_keys.discard(target_key)
            ordinal = targets.index(target) + 1
            if used_http_requests >= max_http_requests:
                stopped_reason = "CHECK_HTTP_BUDGET_EXHAUSTED"
                break
            record_event(
                store, round_id, run_id, "QUALIFIED_CHECK_REFRESH_TARGET_ATTEMPT_STARTED",
                candidate_id=cid, alpha_id=alpha_id,
                payload={"campaign_id": campaign_id, "ordinal": ordinal},
            )
            _check_progress_line(
                "QUALIFIED CHECK REFRESH", len(resolved_keys), progress_total,
                candidate_id=cid, alpha_id=alpha_id,
                state=(f"{mode} ordinal={ordinal}/{len(targets)} | processed={processed_this_invocation} "
                       f"resolved={len(resolved_keys)} pending={len(targets) - len(resolved_keys)} "
                       f"deferred={len(durable_deferred_keys)}"),
            )

            def _qualified_poll_progress(poll: Mapping[str, Any]) -> None:
                _check_progress_line(
                    "QUALIFIED CHECK REFRESH", len(resolved_keys), progress_total,
                    candidate_id=cid, alpha_id=alpha_id, state=_qualified_check_poll_state(poll),
                )

            try:
                report = refresh_one_pretag_check(
                    store, config, machine, session, run_id, cid,
                    source="V31_QUALIFIED_CHECK_REFRESH",
                    evidence_source=_qualified_campaign_evidence_source(campaign_id),
                    poll_observer=_qualified_poll_progress,
                    min_retry_after_seconds=retry_floor,
                )
            except Exception as exc:
                report = {
                    "executed": False, "candidate_id": cid, "alpha_id": alpha_id,
                    "session_status": "TRANSIENT_ERROR",
                    "error_type": type(exc).__name__, "error_nature": "TRANSIENT",
                    "error": f"{type(exc).__name__}: {exc}", "http_request_count": 0,
                }
            report = dict(report)
            report["selection"] = dict(target)
            reports.append(report)
            processed_this_invocation += 1
            used_http_requests += int(report.get("http_request_count") or 0)
            session_status = str(report.get("session_status") or "").upper()
            if session_status == "RESOLVED":
                _sync_qualified_refresh_resolution(store, run_id, cid, alpha_id, report)
                record_event(
                    store, round_id, run_id, _QUALIFIED_CAMPAIGN_RESOLVED,
                    candidate_id=cid, alpha_id=alpha_id,
                    payload={"campaign_id": campaign_id, "ordinal": ordinal},
                    source_event_key=f"qualified_check_campaign_resolved:{campaign_id}:{cid}:{alpha_id}",
                )
                resolved_keys.add((cid, alpha_id))
                durable_deferred_keys.discard(target_key)
                durable_interrupted_keys.discard(target_key)
                resolved_this_invocation += 1
            else:
                deferred_count += 1
                durable_deferred_keys.add(target_key)
                record_event(
                    store, round_id, run_id, _QUALIFIED_CAMPAIGN_DEFERRED,
                    candidate_id=cid, alpha_id=alpha_id,
                    payload={
                        "campaign_id": campaign_id, "ordinal": ordinal,
                        "session_status": session_status,
                        "error_type": report.get("error_type"),
                    },
                )
            _check_progress_line(
                "QUALIFIED CHECK REFRESH", len(resolved_keys), progress_total,
                candidate_id=cid, alpha_id=alpha_id,
                state=(f"{session_status or 'DONE'} | processed={processed_this_invocation} "
                       f"resolved={len(resolved_keys)} pending={len(targets) - len(resolved_keys)} "
                       f"deferred={len(durable_deferred_keys)}"),
            )
            error_type = str(report.get("error_type") or "")
            if error_type in {"HTTP_429_THROTTLE_DEFERRED", "HTTP_429"}:
                stopped_reason = error_type
                break
            current_target = None
    except KeyboardInterrupt:
        if current_target:
            durable_interrupted_keys.add((
                str(current_target.get("candidate_id") or ""),
                str(current_target.get("alpha_id") or ""),
            ))
        record_event(
            store, round_id, run_id, _QUALIFIED_CAMPAIGN_INTERRUPTED,
            candidate_id=str((current_target or {}).get("candidate_id") or "") or None,
            alpha_id=str((current_target or {}).get("alpha_id") or "") or None,
            payload={
                "campaign_id": campaign_id, "resolved_count": len(resolved_keys),
                "pending_count": len(targets) - len(resolved_keys),
                "reason": "KEYBOARD_INTERRUPT",
            },
        )
        print(
            f"QUALIFIED CHECK REFRESH INTERRUPTED campaign={campaign_id} | "
            f"completed={len(resolved_keys)} | remaining={len(targets) - len(resolved_keys)} | "
            f"deferred={len(durable_deferred_keys)} | interrupted={len(durable_interrupted_keys)} | total={len(targets)}",
            flush=True,
        )
        raise

    processed_ids = {str(x.get("candidate_id") or "") for x in reports}
    if stopped_reason:
        for target in pending_targets:
            if str(target["candidate_id"]) not in processed_ids:
                skipped.append({
                    "candidate_id": target["candidate_id"], "alpha_id": target["alpha_id"],
                    "reason": f"NOT_ATTEMPTED_AFTER_STOP:{stopped_reason}",
                })

    campaign_complete = len(resolved_keys) == len(targets)
    if campaign_complete:
        _close_qualified_refresh_campaign(
            store, round_id, run_id, campaign_id, _QUALIFIED_CAMPAIGN_COMPLETED,
            payload={"resolved_count": len(resolved_keys), "total_count": len(targets)},
        )
    record_event(
        store, round_id, run_id, "QUALIFIED_CHECK_REFRESH",
        batch_no=int((get_round(store, round_id=round_id) or {}).get("current_batch") or 0),
        phase=str((get_round(store, round_id=round_id) or {}).get("phase") or "SEARCH"),
        payload={
            "campaign_id": campaign_id, "campaign_resumed": resumed,
            "campaign_status": "COMPLETED" if campaign_complete else "ACTIVE",
            "requested": len(pending_targets), "executed": sum(bool(x.get("executed")) for x in reports),
            "resolved": resolved_this_invocation,
            "durable_resolved": len(resolved_keys), "durable_pending": len(targets) - len(resolved_keys),
            "deferred": deferred_count, "durable_deferred": len(durable_deferred_keys),
            "http_request_count": used_http_requests, "stopped_reason": stopped_reason,
            "skipped": skipped, "simulation_posts": 0, "property_writes": 0,
            "submit_requests": 0, "business_methods": ["GET"],
        },
    )
    files = _write_reports(store, config, alpha_db, run_id, round_id, policy, project_dir)
    after = round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
    return {
        "project_version": "v3.1", "action": "REFRESH_QUALIFIED_CHECKS",
        "run_id": run_id, "round_id": round_id, "round_status": str(rr.get("status") or ""),
        "campaign_id": campaign_id, "campaign_resumed": resumed,
        "campaign_status": "COMPLETED" if campaign_complete else "ACTIVE",
        "target_count": len(targets), "executed_check_count": sum(bool(x.get("executed")) for x in reports),
        "processed_this_invocation": processed_this_invocation,
        "resolved_check_count": resolved_this_invocation,
        "durable_resolved_count": len(resolved_keys),
        "pending_count": len(targets) - len(resolved_keys), "deferred_count": deferred_count,
        "durable_deferred_count": len(durable_deferred_keys),
        "http_request_count": used_http_requests, "stopped_reason": stopped_reason,
        "reports": reports, "skipped": skipped,
        "classification_counts_after_refresh": after.get("ppl_classification", {}).get("counts", {}),
        "ready_after_refresh": after.get("ppl_classification", {}).get("ready_for_manual_finalization"),
        "reports_written": files, "machine_hash": hash_result,
        "network_counters": {
            "authentication_post_count": int(authentication_post_count),
            "simulation_post_count": 0, "delete_count": 0, "submit_count": 0,
            "power_pool_selected_count": 0, "repair_post_count": 0,
            "business_methods": ["GET"],
        },
    }


def _maybe_auto_refresh_manual_finalization(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
    run_id: str, round_id: str, policy: Mapping[str, Any], project_dir: Path, *,
    batch_no: int, fresh_candidate_ids: Sequence[str] = (),
) -> Dict[str, Any]:
    interval = _manual_refresh_interval(policy)
    if batch_no <= 0 or batch_no % interval != 0:
        return {"triggered": False, "interval": interval, "batch_no": batch_no}

    continuous = parse_continuous_policy(policy)
    if continuous.enabled and continuous.check_queue_enabled and continuous.manual_finalization_check_queue_enabled:
        # Continuous auto-refresh is freshness work, not a lifetime-budgeted
        # synchronous check session.  Rebuild targets from durable classification
        # so report/CSV availability cannot block the research loop.
        classified = classify_run(store, config, alpha_db, run_id)
        target_ids = [
            str(x.get("candidate_id")) for x in classified
            if x.get("classification") == "PPL_READY_FOR_MANUAL_FINALIZATION" and x.get("candidate_id")
        ]
        protected_cids, protected_alphas, protected_signals = _protected_manual_queue_sets(store, round_id)
        candidates = {str(x.get("candidate_id")): x for x in store.load_candidates(run_id)}
        filtered: List[str] = []
        skipped: List[Dict[str, Any]] = []
        for cid in target_ids:
            cand = candidates.get(cid) or {}
            alpha_id = str(cand.get("alpha_id") or "")
            signal = str(cand.get("signal_family") or "")
            if cid in protected_cids or alpha_id in protected_alphas or (signal and signal in protected_signals):
                skipped.append({"candidate_id": cid, "alpha_id": alpha_id, "reason": "PROTECTED_OR_SUBMITTED"})
                continue
            filtered.append(cid)
        queued = enqueue_manual_refresh_checks(
            store, run_id, filtered, exclude_candidate_ids=fresh_candidate_ids,
            source="V31_MANUAL_FINALIZATION_REFRESH",
        )
        record_event(
            store, round_id, run_id, "MANUAL_FINALIZATION_QUEUE_REFRESH_QUEUED",
            batch_no=batch_no, phase=str((get_round(store, round_id=round_id) or {}).get("phase") or "SEARCH"),
            payload={
                "trigger": "AUTO_PERIODIC_BATCH_REFRESH", "requested": len(target_ids),
                "queued": int(queued.get("queued_count") or 0),
                "skipped": [*skipped, *(queued.get("skipped") or [])],
                "execution_mode": "V31_DURABLE_CHECK_QUEUE",
                "lifetime_check_budget_enforced": False,
                "simulation_posts": 0, "property_writes": 0, "submit_requests": 0,
            },
        )
        return {
            "triggered": True, "interval": interval, "batch_no": batch_no,
            "requested": len(target_ids), "queued_check_count": int(queued.get("queued_count") or 0),
            "executed_check_count": 0, "resolved_check_count": 0,
            "queue_mode": "V31_DURABLE_RECHECK",
            "skipped": [*skipped, *(queued.get("skipped") or [])],
        }

    targets = _manual_queue_csv_candidates(project_dir, round_id)
    if not targets:
        return {"triggered": True, "interval": interval, "batch_no": batch_no,
                "requested": 0, "executed_check_count": 0, "reason": "MANUAL_QUEUE_EMPTY"}
    out = _refresh_manual_finalization_candidates(
        store, config, machine, session, alpha_db, run_id, round_id, targets,
        trigger="AUTO_PERIODIC_BATCH_REFRESH", batch_no=batch_no, exclude_candidate_ids=fresh_candidate_ids,
    )
    out.update({"triggered": True, "interval": interval})
    return out


def refresh_manual_finalization_queue(
    store: Any, config: Any, machine: Any, session: Any, alpha_db: Path, machine_path: Path,
    round_policy_path: Path, project_dir: Path, *, run_id: str,
    preflight: Optional[Mapping[str, Any]] = None, authentication_post_count: int = 0,
) -> Dict[str, Any]:
    """Explicit user command: refresh every row currently in manual_finalization_queue.csv."""
    local_preflight = dict(preflight or preflight_manual_finalization_refresh(
        store, config, machine_path, round_policy_path, run_id=run_id,
    ))
    if str(local_preflight.get("run_id")) != str(run_id):
        raise ConfigError("MANUAL_FINALIZATION_REFRESH_PREFLIGHT_RUN_MISMATCH")
    rr = dict(local_preflight["round"])
    round_id = str(rr["round_id"])
    policy = dict(local_preflight["current_policy"])
    hash_result = dict(local_preflight["hash_result"])
    # Compatible-patch refresh is deliberately observational: it must not use
    # the command as an opportunity to migrate the durable round policy.
    upgraded = False
    targets = _manual_queue_csv_candidates(project_dir, round_id)
    if not targets:
        # If reports were deleted, rebuild the target set from durable current classification.
        targets = [
            str(x.get("candidate_id")) for x in classify_run(store, config, alpha_db, run_id)
            if x.get("classification") == "PPL_READY_FOR_MANUAL_FINALIZATION" and x.get("candidate_id")
        ]
    refresh = _refresh_manual_finalization_candidates(
        store, config, machine, session, alpha_db, run_id, round_id, targets,
        trigger="MANUAL_COMMAND", batch_no=int((get_round(store, round_id=round_id) or {}).get("current_batch") or 0),
    )
    files = _write_reports(store, config, alpha_db, run_id, round_id, policy, project_dir)
    after = round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
    return {
        "project_version": "v3.0.4o", "action": "REFRESH_MANUAL_FINALIZATION",
        "run_id": run_id, "round_id": round_id, "round_policy_upgraded": upgraded,
        "refresh": refresh, "ready_after_refresh": after.get("ppl_classification", {}).get("ready_for_manual_finalization"),
        "classification_counts_after_refresh": after.get("ppl_classification", {}).get("counts", {}),
        "reports": files,
        "machine_hash": hash_result,
        "network_counters": {
            "authentication_post_count": int(authentication_post_count),
            "simulation_post_count": 0, "delete_count": 0, "submit_count": 0,
            "power_pool_selected_count": 0, "repair_post_count": 0,
            "business_methods": ["GET"],
        },
    }


def _manual_finalization_rows(candidates: Mapping[str, Mapping[str, Any]], classifications: Sequence[Mapping[str, Any]],
                              *, protected_candidate_ids: Sequence[str] = (), protected_alpha_ids: Sequence[str] = (),
                              protected_signal_families: Sequence[str] = (), check_meta_by_candidate: Optional[Mapping[str, Mapping[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Build a local-only queue with copy-ready Power Pool descriptions.

    One row is emitted per unique Alpha ID.  If the same Alpha ID is mapped to
    multiple distinct candidate expressions, the queue refuses to guess which
    expression owns the live Alpha and marks the row for identity review.
    """
    protected_cids = {str(x) for x in protected_candidate_ids}
    protected_alphas = {str(x) for x in protected_alpha_ids}
    protected_signals = {str(x) for x in protected_signal_families}
    check_meta_by_candidate = dict(check_meta_by_candidate or {})
    eligible = [dict(x) for x in classifications if str(x.get("classification") or "") == "PPL_READY_FOR_MANUAL_FINALIZATION"]
    by_alpha: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for cls in eligible:
        cid = str(cls.get("candidate_id") or "")
        cand = dict(candidates.get(cid) or {})
        alpha_id = str(cls.get("alpha_id") or cand.get("alpha_id") or "").strip()
        signal = str(cand.get("signal_family") or "")
        if cid in protected_cids or alpha_id in protected_alphas or (signal and signal in protected_signals):
            continue
        key = alpha_id or f"CANDIDATE::{cid}"
        by_alpha[key].append({"classification": cls, "candidate": cand})

    out: List[Dict[str, Any]] = []
    for key, items in sorted(by_alpha.items()):
        alpha_id = "" if key.startswith("CANDIDATE::") else key
        expressions = {str(x["candidate"].get("expression") or x["candidate"].get("expression_canonical") or "") for x in items}
        identity_ambiguous = len(items) > 1 and len(expressions) > 1
        chosen = sorted(items, key=lambda x: (-(float(x["classification"].get("sharpe") or 0.0)), str(x["candidate"].get("candidate_id") or "")))[0]
        cls = chosen["classification"]
        cand = chosen["candidate"]
        meta = dict(check_meta_by_candidate.get(str(cand.get("candidate_id") or "")) or {})
        if identity_ambiguous:
            draft = {"full_text": "", "validation_status": "BLOCKED_IDENTITY_AMBIGUITY", "description_length": 0}
            manual_action = "VERIFY_ALPHA_CODE_BEFORE_DESCRIPTION"
        else:
            draft = draft_power_pool_description(cand)
            manual_action = "COPY_DESCRIPTION_SAVE_AND_CHECK_SUBMISSION"
        out.append({
            "alpha_id": alpha_id or cls.get("alpha_id"),
            "candidate_id": cand.get("candidate_id"),
            "classification": cls.get("classification"),
            "sharpe": cls.get("sharpe"),
            "fitness": cls.get("fitness"),
            "turnover": cls.get("turnover"),
            "final_theme_outcome": cls.get("final_theme_outcome"),
            "ppc": cls.get("ppc_value"),
            "platform_ppc_outcome": cls.get("platform_ppc_outcome"),
            "ppc_policy_band": cls.get("ppc_policy_band"),
            "ppc_strategy_result": cls.get("ppc_strategy_result"),
            "last_check_at": meta.get("updated_at") or meta.get("created_at"),
            "last_check_session_id": meta.get("check_session_id"),
            "description_pending_checks": cls.get("description_pending_checks"),
            "generated_description": draft.get("full_text"),
            "description_validation_status": draft.get("validation_status"),
            "description_length": draft.get("description_length"),
            "manual_action": manual_action,
            "identity_ambiguous": identity_ambiguous,
            "candidate_mapping_count": len(items),
            "platform_property_write_performed": False,
        })
    return out


def _write_reports(store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
                   policy: Mapping[str, Any], project_dir: Path) -> Dict[str, str]:
    report_dir = project_dir / str(policy.get("report_dir") or "reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    status = round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
    candidates = {x["candidate_id"]: x for x in store.load_candidates(run_id)}
    facts = _alpha_facts(alpha_db, [x["sim_key"] for x in candidates.values() if x.get("sim_key")])
    winners = load_winners(store, round_id)
    variant_counts = Counter(str(x.get("signal_family") or "") for x in candidates.values())
    with store.connect() as conn:
        repair_rows = conn.execute(
            """SELECT child_candidate_id,repair_type,parent_candidate_id,side_effect_verdict
               FROM ppl_repairs WHERE run_id=?""", (run_id,)
        ).fetchall()
    repair_by_child = {str(r[0]): {"repair_strategy": r[1], "repair_parent": r[2], "repair_verdict": r[3]}
                       for r in repair_rows if r[0]}
    winner_rows = []
    for w in winners:
        c = candidates.get(w.get("candidate_id")) or {}
        fact = facts.get(str(c.get("sim_key") or "")) or {}
        checks = _latest_check_metrics(store, run_id, c.get("candidate_id")) if c else {}
        try:
            settings = json.loads(c.get("settings_json") or "{}") if c else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            settings = {}
        sub = checks.get("SUB_UNIVERSE") or checks.get("LOW_SUB_UNIVERSE_SHARPE") or {}
        two_y = checks.get("TWO_YEAR_SHARPE") or checks.get("LOW_2Y_SHARPE") or {}
        repair = repair_by_child.get(str(c.get("candidate_id") or ""), {})
        winner_rows.append({
            "family_id": w.get("family_id"), "alpha_id": w.get("alpha_id"),
            "candidate_id": w.get("candidate_id"), "signal_family": w.get("signal_family"),
            "expression": c.get("expression"), "dataset_id": c.get("dataset_id"), "field_id": c.get("field_id"),
            "operator": c.get("operator"), "region": config.plan["simulation_settings"].get("region"),
            "universe": config.plan["simulation_settings"].get("universe"),
            "neutralization": c.get("neutralization"), "decay": c.get("decay"),
            "truncation": settings.get("truncation"),
            "sharpe": fact.get("sharpe"), "fitness": fact.get("fitness"), "turnover": fact.get("turnover"),
            "returns": fact.get("returns"), "winner_state": w.get("winner_state"), "source": w.get("source"),
            "ht_ratio": (checks.get("HIGH_TURNOVER_RETURNS_RATIO") or {}).get("value"),
            "pp_corr": (checks.get("POWER_POOL_CORRELATION") or {}).get("value"),
            "prod_corr": (checks.get("PROD_CORRELATION") or {}).get("value"),
            "sub_universe": sub.get("value"), "two_year_sharpe": two_y.get("value"),
            "family_variant_count": variant_counts.get(str(w.get("signal_family") or ""), 0),
            "simulation_origin": "REPAIR" if c.get("parent_candidate_id") else "INITIAL_SEARCH",
            "parent_alpha": None,
            "parent_candidate_id": c.get("parent_candidate_id") or repair.get("repair_parent"),
            "repair_strategy": repair.get("repair_strategy"),
            "repair_verdict": repair.get("repair_verdict"),
            "status": w.get("winner_state"),
        })
    base = report_dir / round_id
    summary_json = base.with_name(base.name + "_summary.json")
    summary_md = base.with_name(base.name + "_summary.md")
    winners_csv = base.with_name(base.name + "_ppl_family_winners.csv")
    queue_csv = base.with_name(base.name + "_manual_tag_queue.csv")
    near_csv = base.with_name(base.name + "_near_pass_queue.csv")
    ppl_classification_csv = base.with_name(base.name + "_ppl_classification.csv")
    manual_finalization_csv = base.with_name(base.name + "_manual_finalization_queue.csv")
    budget_csv = base.with_name(base.name + "_budget_audit.csv")
    batches = load_batches(store, round_id)
    batch_totals = {
        "cache_hits": sum(int(x.get("cache_hits") or 0) for x in batches),
        "resume_count": sum(int(x.get("resume_count") or 0) for x in batches),
        "check_count": sum(int(x.get("check_count") or 0) for x in batches),
    }
    tested = [c for c in candidates.values() if str(c.get("simulation_status") or "NONE").upper() not in {"", "NONE"}]
    tested_family_count = len({family_id(c) for c in tested})
    new_winner_ids = {str(w.get("candidate_id")) for w in winners if w.get("candidate_id") and str(w.get("source")) == "V3_LIVE_PRETAG"}

    def grouped_stats(key_name: str) -> List[Dict[str, Any]]:
        grouped: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"simulations": 0, "families": set(), "winners": 0})
        for c in tested:
            key = str(c.get(key_name) or "UNKNOWN")
            grouped[key]["simulations"] += 1
            grouped[key]["families"].add(family_id(c))
            if str(c.get("candidate_id")) in new_winner_ids:
                grouped[key]["winners"] += 1
        out = []
        for key, g in grouped.items():
            sims = int(g["simulations"])
            out.append({
                key_name: key, "simulations": sims, "distinct_families": len(g["families"]),
                "ppl_family_winners": int(g["winners"]),
                "winner_per_simulation": round(int(g["winners"]) / sims, 6) if sims else 0.0,
            })
        return sorted(out, key=lambda x: (-x["ppl_family_winners"], -x["winner_per_simulation"], x[key_name]))

    status["execution_totals"] = {
        **batch_totals,
        "tested_candidates": len(tested),
        "tested_distinct_families": tested_family_count,
        "new_distinct_ppl_family_winners": len(new_winner_ids),
    }
    status["dataset_productivity"] = grouped_stats("dataset_id")  # legacy all-tested view
    status["operator_productivity"] = grouped_stats("operator")  # legacy all-tested view
    status["search_productivity"] = _productivity_snapshot(store, run_id, round_id, policy)
    status["winner_alpha_ids"] = [r.get("alpha_id") for r in winner_rows if r.get("candidate_id") and r.get("source") == "V3_LIVE_PRETAG"]
    summary_json.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_md.write_text(
        "# WorldQuant BRAIN v3 Round Summary\n\n"
        f"- Run: `{run_id}`\n- Round: `{round_id}`\n- Status: `{status['round']['status']}`\n"
        f"- Search budget: {status['budget']['search_consumed']} / {status['budget']['search_budget']}\n"
        f"- Repair budget: {status['budget']['repair_consumed']} / {status['budget']['repair_budget']}\n"
        f"- Protected distinct families: {status['families']['protected']}\n"
        f"- Manual tag queue: {status['families']['manual_tag_queue']}\n"
        f"- Tested distinct families: {tested_family_count}\n"
        f"- New V3 family winners: {len(new_winner_ids)}\n"
        f"- Active datasets: {status.get('dataset_pool', {}).get('active', 0)}\n"
        f"- Dataset refreshes: {status.get('dataset_pool', {}).get('refreshes', 0)}\n"
        f"- Cache hits: {batch_totals['cache_hits']}\n- Resume count: {batch_totals['resume_count']}\n"
        f"- Stop reason: {status['round'].get('stop_reason')}\n",
        encoding="utf-8",
    )
    def write_csv(path: Path, rows: List[Mapping[str, Any]]) -> None:
        fields = sorted({k for row in rows for k in row}) if rows else ["status"]
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields); writer.writeheader()
            if rows:
                writer.writerows(rows)
    write_csv(winners_csv, winner_rows)
    write_csv(queue_csv, [r for r in winner_rows if r.get("candidate_id") and r.get("winner_state") == "PRE_TAG_FINALIST"])
    protected_fids = {str(w.get("family_id")) for w in winners if int(w.get("protected") or 0)}
    near = classify_run(store, config, alpha_db, run_id)
    near_rows = []
    for item in near:
        if item.get("evidence_label") not in {"STRONG_NEAR_PASS", "NEAR_PASS"}:
            continue
        c = candidates.get(str(item.get("candidate_id") or ""))
        if c and family_id(c) in protected_fids:
            continue
        near_rows.append(item)
    write_csv(near_csv, near_rows)
    write_csv(ppl_classification_csv, near)
    protected_cids = {str(w.get("candidate_id") or "") for w in winners if w.get("candidate_id")}
    protected_alphas = {str(w.get("alpha_id") or "") for w in winners if w.get("alpha_id")}
    protected_signals = {str(w.get("signal_family") or "") for w in winners if w.get("signal_family")}
    check_meta = {cid: _latest_check_meta(store, run_id, cid) for cid in candidates}
    manual_finalization_rows = _manual_finalization_rows(
        candidates, near, protected_candidate_ids=protected_cids, protected_alpha_ids=protected_alphas,
        protected_signal_families=protected_signals, check_meta_by_candidate=check_meta,
    )
    write_csv(manual_finalization_csv, manual_finalization_rows)
    budget_rows = [{k: row.get(k) for k in ("batch_no","phase","status","projected_new_posts","logical_posts_consumed","cache_hits","resume_count","check_count","started_at","completed_at")} for row in batches]
    write_csv(budget_csv, budget_rows)

    # Rich, replayable research artifact tree.  Legacy flat files above are
    # retained for compatibility with the first V3 build.
    rich_dir = report_dir / round_id
    rich_dir.mkdir(parents=True, exist_ok=True)
    batches_dir = rich_dir / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    manifest_row = load_manifest(store, round_id)
    manifest_payload = json.loads(manifest_row["manifest_json"]) if manifest_row else _build_manifest_payload(config, policy, project_dir, run_id, round_id)
    if not manifest_row:
        upsert_manifest(store, round_id, run_id, manifest_payload)
    events = load_events(store, round_id)
    decisions = load_decisions(store, round_id)
    ledger = load_ledger(store, round_id)
    snapshots = load_snapshots(store, round_id)
    latest_batch_no = max((int(b.get("batch_no") or 0) for b in batches), default=0) or None
    failures = failure_matrix(store, run_id, round_id=round_id, batch_no=latest_batch_no)
    with store.connect() as conn:
        repairs = [dict(r) for r in conn.execute(
            """SELECT p.repair_plan_id,p.repair_signature,p.parent_candidate_id,p.target_failure,p.repair_type,p.plan_status,
                      p.projected_new_posts,p.committed_posts,p.consumed_posts,p.blocked_reason,
                      r.child_candidate_id,r.side_effect_verdict,r.before_json,r.after_json,r.delta_json,
                      p.created_at,p.updated_at
               FROM ppl_repair_plans p LEFT JOIN ppl_repairs r
                 ON r.run_id=p.run_id AND r.repair_signature=p.repair_signature
               WHERE p.run_id=? ORDER BY p.created_at,p.repair_plan_id""", (run_id,)
        )]

    rich_summary_json = rich_dir / "summary.json"
    rich_summary_md = rich_dir / "summary.md"
    manifest_json = rich_dir / "manifest.json"
    timeline_jsonl = rich_dir / "timeline.jsonl"
    ledger_csv = rich_dir / "simulation_ledger.csv"
    decisions_csv = rich_dir / "candidate_decisions.csv"
    snapshots_jsonl = rich_dir / "batch_snapshots.jsonl"
    rich_winners_csv = rich_dir / "ppl_family_winners.csv"
    rich_queue_csv = rich_dir / "manual_tag_queue.csv"
    rich_near_csv = rich_dir / "near_pass_queue.csv"
    rich_ppl_classification_csv = rich_dir / "ppl_classification.csv"
    rich_manual_finalization_csv = rich_dir / "manual_finalization_queue.csv"
    repair_csv = rich_dir / "repair_history.csv"
    failure_csv = rich_dir / "failure_matrix.csv"
    rich_budget_csv = rich_dir / "budget_audit.csv"
    candidates_csv = rich_dir / "candidates_final.csv"
    dataset_states_csv = rich_dir / "dataset_pool_state.csv"
    dataset_refreshes_csv = rich_dir / "dataset_refresh_history.csv"
    search_productivity_csv = rich_dir / "search_productivity.csv"

    rich_summary_json.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    rich_summary_md.write_text(summary_md.read_text(encoding="utf-8"), encoding="utf-8")
    manifest_json.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with timeline_jsonl.open("w", encoding="utf-8") as fh:
        for event in events:
            row = dict(event)
            try:
                row["payload"] = json.loads(row.pop("payload_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                row["payload"] = {"raw": row.pop("payload_json", None)}
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    with snapshots_jsonl.open("w", encoding="utf-8") as fh:
        for snap in snapshots:
            row = dict(snap)
            try:
                row["snapshot"] = json.loads(row.pop("payload_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                row["snapshot"] = {"raw": row.pop("payload_json", None)}
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    write_csv(ledger_csv, ledger)
    write_csv(decisions_csv, decisions)
    write_csv(rich_winners_csv, winner_rows)
    write_csv(rich_queue_csv, [r for r in winner_rows if r.get("candidate_id") and r.get("winner_state") == "PRE_TAG_FINALIST"])
    write_csv(rich_near_csv, near_rows)
    write_csv(rich_ppl_classification_csv, near)
    write_csv(rich_manual_finalization_csv, manual_finalization_rows)
    write_csv(repair_csv, repairs)
    write_csv(failure_csv, failures)
    write_csv(rich_budget_csv, budget_rows)
    write_csv(candidates_csv, list(candidates.values()))
    write_csv(dataset_states_csv, load_dataset_states(store, round_id))
    write_csv(dataset_refreshes_csv, load_dataset_refreshes(store, round_id))
    write_csv(search_productivity_csv, _search_productivity_rows(store, run_id, round_id, policy))
    decisions_by_batch: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for decision_row in decisions:
        decisions_by_batch[int(decision_row.get("batch_no") or 0)].append(dict(decision_row))
    for batch in batches:
        payload = dict(batch)
        for key in ("selected_candidate_ids_json", "selected_plan_ids_json", "planned_post_sim_keys_json",
                    "planned_resume_sim_keys_json", "report_json"):
            try:
                payload[key[:-5] if key.endswith("_json") else key] = json.loads(payload.get(key) or ("{}" if key == "report_json" else "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        batch_no_value = int(batch.get("batch_no") or 0)
        selected_decisions = []
        for d in decisions_by_batch.get(batch_no_value, []):
            if str(d.get("decision") or "") != "SELECTED":
                continue
            try:
                ctx = json.loads(d.get("context_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                ctx = {}
            selected_decisions.append({
                "selection_rank": d.get("selection_rank"),
                "candidate_id": d.get("candidate_id"),
                "dataset_id": d.get("dataset_id"),
                "field_id": d.get("field_id"),
                "operator": d.get("operator"),
                "selection_mode": d.get("selection_mode"),
                "selection_score": d.get("selection_score"),
                "quality_score": d.get("quality_score"),
                "online_evidence_score": ctx.get("online_evidence_score"),
                "prior_scale": ctx.get("prior_scale"),
                "combo_attempts": ctx.get("combo_attempts"),
                "combo_viable": ctx.get("combo_viable"),
                "exploit_eligible": ctx.get("exploit_eligible"),
                "exploit_gate_reason": ctx.get("exploit_gate_reason"),
                "exploit_score": ctx.get("exploit_score"),
                "explore_score": ctx.get("explore_score"),
                "window": ctx.get("window"),
            })
        payload["selection_decisions"] = sorted(
            selected_decisions, key=lambda x: (x.get("selection_rank") is None, x.get("selection_rank") or 10**9)
        )
        payload["selection_summary"] = dict(Counter(str(x.get("selection_mode") or "NONE") for x in selected_decisions))
        payload["selection_summary_scope"] = "ORIGINAL_SELECTION_DECISIONS"
        effective_selected = list(payload.get("selected_candidate_ids") or [])
        effective_selected_plans = list(payload.get("selected_plan_ids") or [])
        effective_post_intent = list(payload.get("planned_post_sim_keys") or [])
        effective_resume_intent = list(payload.get("planned_resume_sim_keys") or [])
        execution_report = (payload.get("report") or {}).get("execution") if isinstance(payload.get("report"), Mapping) else {}
        execution_report = execution_report if isinstance(execution_report, Mapping) else {}
        payload["effective_execution_scope"] = {
            "original_selected_decision_rows": len(selected_decisions),
            "effective_selected_candidates": len(effective_selected),
            "effective_selected_repair_plans": len(effective_selected_plans),
            "effective_planned_post_intents": len(effective_post_intent),
            "effective_planned_resume_intents": len(effective_resume_intent),
            "released_or_not_effective_candidates": max(0, len(selected_decisions) - len(effective_selected)),
            "logical_posts_consumed": int(batch.get("logical_posts_consumed") or 0),
            "deferred_undispatched_candidates": len(execution_report.get("deferred_candidate_ids") or []),
            "nonterminal_candidates_at_batch_return": len(execution_report.get("nonterminal_candidate_ids") or []),
            "status_semantics": "batch orchestration pass status; candidate Simulation terminality is reported separately",
        }
        (batches_dir / f"batch_{int(batch.get('batch_no') or 0):03d}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )

    return {
        "summary_json": str(summary_json), "summary_md": str(summary_md), "winners_csv": str(winners_csv),
        "manual_tag_queue_csv": str(queue_csv), "near_pass_csv": str(near_csv),
        "ppl_classification_csv": str(ppl_classification_csv),
        "manual_finalization_queue_csv": str(manual_finalization_csv),
        "budget_audit_csv": str(budget_csv),
        "research_dir": str(rich_dir), "manifest_json": str(manifest_json), "timeline_jsonl": str(timeline_jsonl),
        "simulation_ledger_csv": str(ledger_csv), "candidate_decisions_csv": str(decisions_csv),
        "batch_snapshots_jsonl": str(snapshots_jsonl), "repair_history_csv": str(repair_csv),
        "failure_matrix_csv": str(failure_csv), "candidates_final_csv": str(candidates_csv),
        "rich_ppl_classification_csv": str(rich_ppl_classification_csv),
        "rich_manual_finalization_queue_csv": str(rich_manual_finalization_csv),
        "dataset_pool_state_csv": str(dataset_states_csv), "dataset_refresh_history_csv": str(dataset_refreshes_csv),
        "search_productivity_csv": str(search_productivity_csv),
    }


def round_status(store: Any, config: Any, alpha_db: Path, *, run_id: Optional[str] = None,
                 round_id: Optional[str] = None) -> Dict[str, Any]:
    round_row = get_round(store, round_id=round_id, run_id=run_id)
    if not round_row:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    run_id = str(round_row["run_id"])
    run = store.get_run(run_id) or {}
    try:
        stored_round_policy = json.loads(str(round_row.get("config_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        stored_round_policy = {}
    stored_continuous = parse_continuous_policy(stored_round_policy)
    stored_budget_view = budget_view(stored_round_policy, round_row)
    stored_manual_cfg = dict((stored_round_policy.get("ppl_classification") or {}).get("manual_finalization") or {})
    winners = load_winners(store, str(round_row["round_id"]))
    candidates = store.load_candidates(run_id)
    states = Counter(str(x.get("lifecycle_state") or "") for x in candidates)
    sims = Counter(str(x.get("simulation_status") or "NONE").upper() for x in candidates)
    active_policy_states = {}
    if stored_continuous.enabled:
        active_policy_states = {
            ptype: load_policy_state(store, str(round_row["round_id"]), ptype)
            for ptype in ("QUALIFICATION", "SEARCH", "REPAIR")
        }
        if any(active_policy_states.values()):
            restore_policy_bundle_runtime_from_durable_state(
                store, round_id=str(round_row["round_id"]), project_dir=Path(config.project_dir),
                source_path=Path(config.project_dir) / "ppl_round_v31.yaml",
            )
    near = classify_run(store, config, alpha_db, run_id)
    candidate_map = {str(x.get("candidate_id")): x for x in candidates}
    protected_fids = {str(w.get("family_id")) for w in winners if int(w.get("protected") or 0)}
    near = [x for x in near
            if not (candidate_map.get(str(x.get("candidate_id") or ""))
                    and family_id(candidate_map[str(x.get("candidate_id") or "")]) in protected_fids)]
    manual_queue = sum(1 for w in winners if w.get("candidate_id") and str(w.get("winner_state")) == "PRE_TAG_FINALIST")
    dataset_pool = {"active": 0, "cooldown": 0, "seen": 0, "refreshes": 0, "last_refresh_batch": None}
    with store.connect() as conn:
        has_states = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_dataset_states'").fetchone()
        has_refresh = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_dataset_refreshes'").fetchone()
        if has_states:
            ds_rows = conn.execute("SELECT state,COUNT(*) FROM ppl_round_dataset_states WHERE round_id=? GROUP BY state", (str(round_row["round_id"]),)).fetchall()
            ds_counts = {str(r[0]).lower(): int(r[1]) for r in ds_rows}
            dataset_pool.update({"active": ds_counts.get("active", 0), "cooldown": ds_counts.get("cooldown", 0), "seen": sum(ds_counts.values())})
        if has_refresh:
            r = conn.execute("SELECT COUNT(*),MAX(batch_no) FROM ppl_round_dataset_refreshes WHERE round_id=?", (str(round_row["round_id"]),)).fetchone()
            dataset_pool["refreshes"] = int(r[0] or 0); dataset_pool["last_refresh_batch"] = r[1]
    round_status_value = str(round_row.get("status") or "")
    stop_reason = str(round_row.get("stop_reason") or "")
    if stored_continuous.enabled and round_status_value == "RUNNING":
        result = "CONTINUOUS_RUNNING"
    elif stored_continuous.enabled and round_status_value == "PAUSED" and stop_reason == "USER_STOP_REQUESTED":
        result = "CONTINUOUS_STOPPED_BY_USER"
    elif round_status_value == "COMPLETED" and manual_queue > 0:
        result = "ROUND_SUCCESS"
    elif round_status_value == "COMPLETED" and stop_reason == "BUDGET_EXHAUSTED_NORMALLY":
        result = "ROUND_STOPPED_BY_BUDGET"
    elif round_status_value == "COMPLETED" and stop_reason == "ROUND_NO_SAFE_CANDIDATE":
        result = "ROUND_NO_SAFE_CANDIDATE"
    elif round_status_value == "PAUSED" and stop_reason == "MAX_BATCHES_PER_INVOCATION":
        result = "ROUND_PAUSED"
    elif round_status_value == "PAUSED" and ("AUTH" in stop_reason.upper() or "401" in stop_reason or "403" in stop_reason):
        result = "ROUND_STOPPED_BY_AUTH"
    elif round_status_value in {"PAUSED", "FAILED", "STOPPED"} and stop_reason:
        result = "ROUND_STOPPED_BY_GUARD"
    else:
        result = "ROUND_IN_PROGRESS"
    return {
        "project_version": "v3.1" if stored_continuous.enabled else "v3.0.4o",
        "round_result": result,
        "round": {k: round_row.get(k) for k in ("round_id","run_id","objective","status","phase","current_batch","stop_reason","started_at","updated_at","completed_at")},
        "run": {"status": run.get("status"), "current_stage": run.get("current_stage"), "post_attempted": run.get("post_attempted"),
                "post_confirmed": run.get("post_confirmed"), "post_uncertain": run.get("post_uncertain"), "post_consumed": run.get("post_consumed")},
        "research_run": research_run_status(stored_round_policy, run_id=run_id),
        "lifecycle": {
            "mode": stored_continuous.lifecycle_mode.value,
            "continuous": stored_continuous.enabled,
            "global_budget_mode": stored_continuous.global_budget_mode.value,
            "global_budget_enforced": stored_budget_view.enforced,
            "active_policy_bundle": {
                ptype.lower(): {
                    "policy_version": state.get("policy_version") if state else None,
                    "policy_hash": state.get("policy_hash") if state else None,
                    "activated_batch_no": state.get("activated_batch_no") if state else None,
                }
                for ptype, state in active_policy_states.items()
            },
            # compatibility alias retained for existing status consumers
            "active_qualification_policy": {
                "policy_version": (active_policy_states.get("QUALIFICATION") or {}).get("policy_version"),
                "policy_hash": (active_policy_states.get("QUALIFICATION") or {}).get("policy_hash"),
                "activated_batch_no": (active_policy_states.get("QUALIFICATION") or {}).get("activated_batch_no"),
            },
        },
        "budget": {
            "total_budget": stored_budget_view.total_budget,
            "search_budget": stored_budget_view.search_budget,
            "repair_budget": stored_budget_view.repair_budget,
            "search_consumed": stored_budget_view.search_consumed,
            "repair_consumed": stored_budget_view.repair_consumed,
            "remaining_total": stored_budget_view.remaining_total,
            "remaining_search": stored_budget_view.remaining_search,
            "remaining_repair": stored_budget_view.remaining_repair,
            "enforced": stored_budget_view.enforced,
            "semantics": "ENFORCED_LIMIT" if stored_budget_view.enforced else "STATISTICS_ONLY",
        },
        "candidates": {
            "total": len(candidates),
            "lifecycle": dict(states),  # backward-compatible raw workflow state counts
            "workflow_lifecycle": dict(states),
            "workflow_near_pass_count": int(states.get("NEAR_PASS", 0)),
            "workflow_near_threshold_count": int(states.get("NEAR_PASS", 0)),
            "simulation": dict(sims),
        },
        "families": {"protected": len(winners), "manual_tag_queue": manual_queue},
        "dataset_pool": dataset_pool,
        "ppl_classification": {
            "counts": dict(Counter(str(x.get("classification") or "UNKNOWN") for x in near)),
            "repair_priority": dict(Counter(str(x.get("repair_priority") or "NONE") for x in near)),
            "technically_ready": sum(x.get("classification") == "PPL_TECHNICALLY_READY" for x in near),
            "ready_for_manual_finalization": sum(x.get("classification") == "PPL_READY_FOR_MANUAL_FINALIZATION" for x in near),
            "strategy_reject_high_ppc": sum(x.get("classification") == "PPL_STRATEGY_REJECT_HIGH_PPC" for x in near),
            "strategy_reject_mid_ppc_low_sharpe": sum(x.get("classification") == "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE" for x in near),
            "manual_queue_auto_refresh_every_batches": int(stored_manual_cfg.get("auto_refresh_every_batches") or 10),
            "ppc_strategy": dict(stored_manual_cfg.get("ppc_strategy") or {}),
        },
        "ppl_near_pass": {
            "strong": sum(x.get("evidence_label") == "STRONG_NEAR_PASS" for x in near),
            "near": sum(x.get("evidence_label") == "NEAR_PASS" for x in near),
        },
        # Compatibility alias: HIGH/MEDIUM repair-distance bands only.
        "near_pass": {
            "strong": sum(x.get("evidence_label") == "STRONG_NEAR_PASS" for x in near),
            "near": sum(x.get("evidence_label") == "NEAR_PASS" for x in near),
        },
        "status_semantics": {
            "workflow_lifecycle_NEAR_PASS": "legacy raw V2.x workflow state; report label is WORKFLOW_NEAR_THRESHOLD and it is not the PPL repair pool",
            "workflow_near_threshold": "local/workflow near-threshold state only; not equivalent to PPL_NEAR_PASS",
            "ppl_near_pass": "compatibility repair-distance band: HIGH=strong, MEDIUM=near; it is not the primary platform-driven PPL status",
            "ppl_classification": "platform-driven PPL status; Regular/non-PPL diagnostics do not become PPL blockers",
            "manual_finalization": "fixed PPL gates passed + current PPC strategy passed; PRE-TAG Theme warning is deferred while Power Pool Description is pending",
            "ppc_strategy": "PPC<=0.50 allowed; 0.50<PPC<0.65 requires Sharpe>2.00; PPC>=0.65 is a local strategy reject even if platform grants an exemption",
            "manual_queue_refresh": "new candidates use their normal PRE_TAG check; existing queue is GET-only refreshed every 10 completed batches or by --refresh-manual-finalization",
        },
        "batches": len(load_batches(store, str(round_row["round_id"]))),
        "status_query_side_effects": {
            "network_requests": 0, "simulation_posts": 0, "check_requests": 0, "writes": 0,
            "meaning": "effects of this --round-status query only; not cumulative round execution totals",
        },
    }


def rebuild_round_reports(
    store: Any,
    config: Any,
    alpha_db: Path,
    round_policy_path: Path,
    project_dir: Path,
    *,
    run_id: Optional[str] = None,
    round_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Rebuild V3 research telemetry/reports from durable local facts only.

    This command performs no network I/O and no Simulation POST.  It is used
    after telemetry/reporting code upgrades to backfill an already executed
    round without replaying simulations.
    """
    store.initialize()
    ensure_round_schema(store)
    if _integrity(store.path) != "ok" or _integrity(alpha_db) != "ok":
        raise ConfigError("ROUND_DATABASE_INTEGRITY_FAILED")
    row = get_round(store, round_id=round_id, run_id=run_id)
    if not row:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    resolved_round_id = str(row["round_id"])
    resolved_run_id = str(row["run_id"])
    policy = load_round_policy(round_policy_path, config)
    stored_policy = json.loads(row.get("config_json") or "{}")
    policy_upgraded = _migrate_round_policy_if_allowed(
        store, resolved_round_id, resolved_run_id, stored_policy, policy
    )
    if policy_upgraded:
        frozen_extension_source = _extension_source_from_manifest(store, resolved_round_id)
        extension_context = (
            dict(frozen_extension_source["manifest_context"])
            if frozen_extension_source is not None else None
        )
        manifest_config = (
            config_with_extension_execution_identity(config, extension_context)
            if extension_context is not None else config
        )
        upsert_manifest(
            store, resolved_round_id, resolved_run_id,
            _build_manifest_payload(
                manifest_config, policy, project_dir, resolved_run_id, resolved_round_id,
                extension_context=extension_context,
            ),
        )
    dataset_states_bootstrapped = _bootstrap_dataset_states(store, resolved_run_id, resolved_round_id)

    # Refresh the ledger for all durable simulation facts. Existing explicit
    # NEW_POST/CACHE/RESUME origins remain authoritative; legacy UNKNOWN facts
    # are reclassified as HISTORICAL unless a stronger current-round origin is
    # known. Check numeric values are re-read from durable PRE_TAG facts.
    ledger_count = sync_simulation_ledger(
        store, alpha_db, resolved_round_id, resolved_run_id,
        classification_by_candidate={
            str(x.get("candidate_id")): str(x.get("evidence_label") or x.get("classification"))
            for x in classify_run(store, config, alpha_db, resolved_run_id)
            if x.get("candidate_id")
        },
    )
    mirrored = sync_durable_events(store, resolved_round_id, resolved_run_id)

    # Rebuild every existing batch snapshot so scope-aware denominators are
    # available for historical comparison from the first production canary.
    refreshed_snapshots = 0
    for batch in load_batches(store, resolved_round_id):
        bno = int(batch.get("batch_no") or 0)
        if bno <= 0:
            continue
        phase = str(batch.get("phase") or "SEARCH")
        _capture_batch_snapshot(
            store, config, alpha_db, resolved_run_id, resolved_round_id, bno, phase, policy
        )
        refreshed_snapshots += 1

    reports = _write_reports(
        store, config, alpha_db, resolved_run_id, resolved_round_id, policy, project_dir
    )
    return {
        "project_version": "v3.0.4o",
        "action": "REBUILD_ROUND_REPORTS",
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "ledger_rows_synced": ledger_count,
        "durable_events_mirrored": mirrored,
        "batch_snapshots_refreshed": refreshed_snapshots,
        "round_policy_upgraded": policy_upgraded,
        "dataset_states_bootstrapped": dataset_states_bootstrapped,
        "reports": reports,
        "side_effects": {
            "network_requests": 0,
            "simulation_posts": 0,
            "check_requests": 0,
            "submit_requests": 0,
            "power_pool_selected_requests": 0,
            "local_db_writes": "telemetry/report backfill only",
        },
    }


def _prepare_new_round(store: Any, config: Any, policy: Mapping[str, Any], machine: Any, session: Any,
                       alpha_db: Path, external_evidence_path: Path, *, requested_run_id: Optional[str],
                       offline: bool, extension_evidence_run: Optional[str] = None) -> Dict[str, Any]:
    store.initialize(); ensure_round_schema(store)
    if _integrity(store.path) != "ok" or _integrity(alpha_db) != "ok":
        raise ConfigError("ROUND_DATABASE_INTEGRITY_FAILED")
    run_id = requested_run_id or _next_run_id(store)
    try:
        validate_new_research_run(policy, requested_run_id=requested_run_id, resolved_run_id=run_id)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    if store.get_run(run_id):
        raise ConfigError(f"ROUND_RUN_ALREADY_EXISTS:{run_id}")
    extension_source = (
        _extension_source_context(store, config, policy, extension_evidence_run)
        if extension_evidence_run else None
    )
    extension_context = _extension_context_from_source(extension_source)
    extension_config = config_with_extension_execution_identity(config, extension_context)
    # Everything below is pure computation / read-only discovery.  Any failure
    # here leaves no partially-created run or round behind.
    preflight = _candidate_pool_preflight(
        store, extension_config, machine, session, alpha_db, run_id, offline=offline,
        extension_source=extension_source, policy=policy,
    )
    # A concurrent creator must not turn a successful preflight into an
    # accidental overwrite.  The runner lock normally prevents this, but keep
    # the durable check at the write boundary.
    if store.get_run(run_id):
        raise ConfigError(f"ROUND_RUN_ALREADY_EXISTS:{run_id}")
    store.create_run(run_id, extension_config)
    round_id = f"round_{run_id}"
    create_round(store, round_id=round_id, run_id=run_id, policy=policy,
                 total_budget=policy["total_budget"], search_budget=policy["search_budget"], repair_budget=policy["repair_budget"])
    manifest = _build_manifest_payload(
        extension_config, policy, Path(config.project_dir), run_id, round_id,
        extension_context=extension_context,
    )
    manifest_hash = upsert_manifest(store, round_id, run_id, manifest)
    record_event(
        store, round_id, run_id, "ROUND_CREATED", phase="SEARCH",
        payload={"objective": policy["objective"], "total_budget": policy["total_budget"],
                 "search_budget": policy["search_budget"], "repair_budget": policy["repair_budget"],
                 "manifest_hash": manifest_hash, "telemetry_version": TELEMETRY_VERSION,
                 "research_run": research_run_status(policy, run_id=run_id)},
    )
    store.transition_run(run_id, "PLANNED", reason="V3 round created", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
    pool = _persist_candidate_pool(store, preflight, run_id)
    universe_count = record_candidate_universe(store, round_id, run_id, store.load_candidates(run_id))
    dataset_state_count = _bootstrap_dataset_states(store, run_id, round_id)
    record_event(store, round_id, run_id, "DATASET_POOL_INITIALIZED", phase="SEARCH",
                 payload={"active_dataset_count": dataset_state_count, "rolling_discovery_enabled": bool((policy.get("rolling_discovery") or {}).get("enabled"))})
    record_event(store, round_id, run_id, "CANDIDATE_UNIVERSE_CAPTURED", phase="SEARCH",
        payload={"candidate_count": universe_count, "discovery_mode": "OFFLINE" if offline else "ONLINE_READ_ONLY"})
    store.transition_run(run_id, "RECONCILED", reason="V3 candidate pool prepared", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
    store.transition_run(run_id, "READY_FOR_EXECUTION", reason="V3 round preflight complete", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
    seed = _seed_protected_winners(store, round_id, external_evidence_path, run_id)
    update_round(store, round_id, status="READY", phase="SEARCH")
    audit_event(action="ROUND_CREATED", run_id=run_id, round_id=round_id, objective=policy["objective"],
                total_budget=policy["total_budget"], search_budget=policy["search_budget"], repair_budget=policy["repair_budget"],
                protected_family_count=seed)
    sync_durable_events(store, round_id, run_id)
    return {"run_id": run_id, "round_id": round_id, "pool": pool, "protected_seed": seed,
            "manifest_hash": manifest_hash, "telemetry_universe_count": universe_count,
            "execution_config": extension_config}



def authorize_uncertain_repair_retry(
    store: Any, alpha_db: Path, *, run_id: str, batch_no: Optional[int] = None,
    confirm_duplicate_risk: bool = False,
) -> Dict[str, Any]:
    """Locally release one set of REPAIR ``UNCERTAIN_SUBMISSION`` facts for retry.

    This is an explicit operator recovery action.  It performs no network
    request and never deletes the original audit/telemetry evidence.  The
    first uncertain POST remains budget-consumed; only the active HOLD is
    released so the exact same expression/settings/sim_key may be POSTed once
    more by a later normal ``--resume-round`` invocation.

    One retry authorization is allowed per sim_key.  A second uncertain result
    therefore fails closed instead of creating an unbounded POST loop.
    """
    if not confirm_duplicate_risk:
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_REQUIRES_DUPLICATE_RISK_CONFIRMATION")
    if not Path(alpha_db).exists():
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_ALPHA_DB_NOT_FOUND")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    round_id = str(rr["round_id"])
    if str(rr.get("phase") or "").upper() != "REPAIR":
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_REQUIRES_REPAIR_PHASE")
    if str(rr.get("status") or "").upper() not in {"PAUSED", "READY", "RUNNING"}:
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_REQUIRES_ACTIVE_ROUND")

    candidates = [dict(x) for x in store.load_candidates(run_id)]
    if batch_no is not None:
        with store.connect() as conn:
            ledger = conn.execute(
                """SELECT DISTINCT sim_key FROM ppl_round_simulation_ledger
                   WHERE round_id=? AND batch_no=? AND phase='REPAIR'""",
                (round_id, int(batch_no)),
            ).fetchall()
        scoped_keys = {str(x[0]) for x in ledger if x[0]}
    else:
        scoped_keys = {str(x.get("sim_key") or "") for x in candidates}

    candidate_by_key = {
        str(x.get("sim_key") or ""): x for x in candidates
        if str(x.get("sim_key") or "") in scoped_keys
        and str(x.get("simulation_status") or "").upper() == "UNCERTAIN_SUBMISSION"
    }
    if not candidate_by_key:
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_NO_MATCHING_ACTIVE_HOLD")

    facts = _alpha_facts(alpha_db, sorted(candidate_by_key))
    eligible_keys: List[str] = []
    plan_ids_by_key: Dict[str, List[str]] = {}
    with store.connect() as conn:
        for key, candidate in sorted(candidate_by_key.items()):
            fact = facts.get(key) or {}
            if str(fact.get("status") or "").upper() != "UNCERTAIN_SUBMISSION":
                continue
            if fact.get("simulation_url") or fact.get("alpha_id"):
                raise ConfigError(f"UNCERTAIN_REPAIR_RETRY_REMOTE_IDENTITY_PRESENT:{key}")
            prior = conn.execute(
                """SELECT COUNT(*) FROM ppl_round_events
                   WHERE round_id=? AND sim_key=?
                     AND event_type='UNCERTAIN_REPAIR_RETRY_AUTHORIZED'""",
                (round_id, key),
            ).fetchone()[0]
            if int(prior or 0) > 0:
                raise ConfigError(f"UNCERTAIN_REPAIR_RETRY_EXHAUSTED:{key}")
            plans = conn.execute(
                """SELECT DISTINCT p.repair_plan_id,p.plan_status,p.consumed_posts,p.blocked_reason
                   FROM ppl_repair_plans p
                   JOIN ppl_repairs r ON r.run_id=p.run_id AND r.repair_signature=p.repair_signature
                   WHERE p.run_id=? AND r.child_candidate_id=?""",
                (run_id, candidate["candidate_id"]),
            ).fetchall()
            matching = [dict(x) for x in plans if str(x["blocked_reason"] or "") == "UNCERTAIN_SUBMISSION_HOLD"]
            if not matching:
                raise ConfigError(f"UNCERTAIN_REPAIR_RETRY_PLAN_HOLD_NOT_FOUND:{key}")
            if any(str(x["plan_status"] or "").upper() != "READY" for x in matching):
                raise ConfigError(f"UNCERTAIN_REPAIR_RETRY_PLAN_NOT_READY:{key}")
            plan_ids_by_key[key] = sorted(str(x["repair_plan_id"]) for x in matching)
            eligible_keys.append(key)

    if not eligible_keys:
        raise ConfigError("UNCERTAIN_REPAIR_RETRY_NO_MATCHING_CACHE_FACT")

    now = _now()
    # Preserve the uncertain POST as historical evidence while making V2.1's
    # normal retry path eligible.  ERROR is intentional: V2.1 retries ERROR but
    # never retries UNCERTAIN automatically.
    alpha_conn = sqlite3.connect(str(alpha_db))
    try:
        alpha_conn.execute("BEGIN IMMEDIATE")
        for key in eligible_keys:
            alpha_conn.execute(
                """UPDATE alpha_results
                   SET status='ERROR',
                       error=COALESCE(error,'') || ' | UNCERTAIN_RETRY_AUTHORIZED',
                       updated_at=?
                   WHERE sim_key=? AND status='UNCERTAIN_SUBMISSION'
                     AND simulation_url IS NULL AND alpha_id IS NULL""",
                (now, key),
            )
        alpha_conn.commit()
    except Exception:
        alpha_conn.rollback()
        raise
    finally:
        alpha_conn.close()

    released_plan_ids = sorted({p for key in eligible_keys for p in plan_ids_by_key[key]})
    with store.connect() as conn:
        marks = ",".join("?" for _ in eligible_keys)
        conn.execute(
            f"""UPDATE ppl_candidates
                SET simulation_status='ERROR',simulation_freshness='UNKNOWN',
                    live_reconcile_required=0,execution_action='RETRY_PER_V21_POLICY',
                    cache_classification='CACHE_ERROR',updated_at=?
                WHERE run_id=? AND sim_key IN ({marks})
                  AND simulation_status='UNCERTAIN_SUBMISSION'""",
            [now, run_id] + eligible_keys,
        )
        if released_plan_ids:
            plan_marks = ",".join("?" for _ in released_plan_ids)
            # The original batch/round accounting retains the first POST's
            # budget consumption.  Reset only plan-local ownership so the same
            # logical repair strategy can be dispatched once more.
            conn.execute(
                f"""UPDATE ppl_repair_plans
                    SET committed_posts=0,consumed_posts=0,
                        blocked_reason='UNCERTAIN_RETRY_AUTHORIZED',updated_at=?
                    WHERE run_id=? AND repair_plan_id IN ({plan_marks})""",
                [now, run_id] + released_plan_ids,
            )
        row = conn.execute("SELECT post_uncertain FROM ppl_runs WHERE run_id=?", (run_id,)).fetchone()
        active_before = int((row[0] if row else 0) or 0)
        active_after = max(0, active_before - len(eligible_keys))
        conn.execute(
            "UPDATE ppl_runs SET post_uncertain=?,updated_at=? WHERE run_id=?",
            (active_after, now, run_id),
        )

    for key in eligible_keys:
        candidate = candidate_by_key[key]
        record_event(
            store, round_id, run_id, "UNCERTAIN_REPAIR_RETRY_AUTHORIZED",
            batch_no=int(batch_no) if batch_no is not None else None, phase="REPAIR",
            candidate_id=candidate.get("candidate_id"), sim_key=key,
            payload={
                "repair_plan_ids": plan_ids_by_key[key],
                "duplicate_remote_risk_accepted": True,
                "strategy_attempt_consumed": False,
                "original_post_budget_preserved": True,
                "max_retry_authorizations_per_sim_key": 1,
            },
        )
        audit_event(
            action="UNCERTAIN_REPAIR_RETRY_AUTHORIZED", run_id=run_id,
            round_id=round_id, candidate_id=candidate.get("candidate_id"), sim_key=key,
            repair_plan_ids=plan_ids_by_key[key], duplicate_remote_risk_accepted=True,
            strategy_attempt_consumed=False, original_post_budget_preserved=True,
        )

    return {
        "action": "UNCERTAIN_REPAIR_RETRY_AUTHORIZED",
        "run_id": run_id,
        "round_id": round_id,
        "batch_no": batch_no,
        "released_count": len(eligible_keys),
        "released_sim_keys": eligible_keys,
        "released_plan_ids": released_plan_ids,
        "post_uncertain_active_before": active_before,
        "post_uncertain_active_after": active_after,
        "network_requests": 0,
        "simulation_posts": 0,
        "original_post_budget_preserved": True,
        "strategy_attempts_consumed_by_release": 0,
        "retry_limit_per_sim_key": 1,
    }

def reopen_round_after_no_safe_candidate_bugfix(store: Any, *, run_id: str,
                                                  confirm_bugfix_reopen: bool = False) -> Dict[str, Any]:
    """Locally reopen only a falsely completed ROUND_NO_SAFE_CANDIDATE round.

    This is intentionally narrower than a generic COMPLETED -> PAUSED state
    transition.  It exists for selector/orchestration bug recovery only and
    performs no network request, Simulation POST, budget reset, candidate
    mutation, plan mutation, or batch rewrite.
    """
    if not confirm_bugfix_reopen:
        raise ConfigError("ROUND_BUGFIX_REOPEN_REQUIRES_EXPLICIT_CONFIRMATION")
    rr = get_round(store, run_id=run_id)
    if not rr:
        raise ConfigError("V3_ROUND_NOT_FOUND")
    round_id = str(rr["round_id"])
    run = store.get_run(run_id) or {}
    if str(rr.get("status") or "") != "COMPLETED":
        raise ConfigError("ROUND_BUGFIX_REOPEN_REQUIRES_COMPLETED_ROUND")
    if str(rr.get("phase") or "") != "DONE":
        raise ConfigError("ROUND_BUGFIX_REOPEN_REQUIRES_DONE_PHASE")
    if str(rr.get("stop_reason") or "") != "ROUND_NO_SAFE_CANDIDATE":
        raise ConfigError("ROUND_BUGFIX_REOPEN_REASON_NOT_ELIGIBLE")
    if str(run.get("status") or "") != "COMPLETED":
        raise ConfigError("ROUND_BUGFIX_REOPEN_REQUIRES_COMPLETED_RUN")
    if int(run.get("post_uncertain") or 0) != 0:
        raise ConfigError("ROUND_BUGFIX_REOPEN_BLOCKED_BY_UNCERTAIN_POST")

    with store.connect() as conn:
        running_batches = conn.execute(
            "SELECT batch_no,phase FROM ppl_round_batches WHERE round_id=? AND status='RUNNING'",
            (round_id,),
        ).fetchall()
        if running_batches:
            raise ConfigError("ROUND_BUGFIX_REOPEN_BLOCKED_BY_RUNNING_BATCH")
        # A completed round must not contain an intent whose batch never became
        # terminal.  Terminal batches preserve all historical intent events.
        unresolved = conn.execute(
            """SELECT COUNT(*) FROM ppl_round_events e
               JOIN ppl_round_batches b ON b.round_id=e.round_id AND b.batch_no=e.batch_no
               WHERE e.round_id=? AND e.event_type='SIMULATION_POST_INTENT'
                 AND b.status='RUNNING'""",
            (round_id,),
        ).fetchone()[0]
        if int(unresolved or 0):
            raise ConfigError("ROUND_BUGFIX_REOPEN_BLOCKED_BY_UNRESOLVED_POST_INTENT")

        now = _now()
        conn.execute(
            """UPDATE ppl_rounds
               SET status='PAUSED',phase='REPAIR',stop_reason='BUGFIX_REOPEN_REPAIR_SELECTION',
                   completed_at=NULL,updated_at=? WHERE round_id=?""",
            (now, round_id),
        )
        conn.execute(
            "UPDATE ppl_runs SET status='PAUSED',current_stage='PAUSED',updated_at=? WHERE run_id=?",
            (now, run_id),
        )
        conn.execute(
            """INSERT INTO ppl_state_transitions(
                   run_id,candidate_id,entity_type,from_state,to_state,reason,source,metadata_json,created_at
               ) VALUES (?,NULL,'RUN','COMPLETED','PAUSED',?,?,?,?)""",
            (run_id, "Bugfix reopen after false ROUND_NO_SAFE_CANDIDATE", "V3_BUGFIX_RECOVERY",
             _json({"round_id": round_id, "previous_stop_reason": "ROUND_NO_SAFE_CANDIDATE"}), now),
        )

    audit_state_transition(
        "RUN", run_id, run_id=run_id, old_state="COMPLETED", new_state="PAUSED",
        reason="Bugfix reopen after false ROUND_NO_SAFE_CANDIDATE", source="V3_BUGFIX_RECOVERY",
    )
    audit_event(
        action="ROUND_BUGFIX_REOPEN", run_id=run_id, round_id=round_id,
        previous_status="COMPLETED", new_status="PAUSED", new_phase="REPAIR",
        current_batch=int(rr.get("current_batch") or 0),
        search_consumed=int(rr.get("search_consumed") or 0),
        repair_consumed=int(rr.get("repair_consumed") or 0),
        reason="SELECTOR_RECOMMENDATION_CONTRACT_BUGFIX",
        network_requests=0, simulation_posts=0,
    )
    return {
        "action": "ROUND_BUGFIX_REOPEN",
        "run_id": run_id,
        "round_id": round_id,
        "status": "PAUSED",
        "phase": "REPAIR",
        "current_batch": int(rr.get("current_batch") or 0),
        "search_consumed": int(rr.get("search_consumed") or 0),
        "repair_consumed": int(rr.get("repair_consumed") or 0),
        "stop_reason": "BUGFIX_REOPEN_REPAIR_SELECTION",
        "network_requests": 0,
        "simulation_posts": 0,
    }


def _report_retry_due(store: Any, run_id: str) -> bool:
    now = _now()
    with store.connect() as conn:
        row = conn.execute(
            """SELECT 1 FROM ppl_endpoint_waits
               WHERE run_id=? AND endpoint_type='REPORT' AND wait_state='WAIT_REPORT'
                 AND next_retry_at IS NOT NULL AND next_retry_at<=? LIMIT 1""",
            (run_id, now),
        ).fetchone()
    return bool(row)


def _write_reports_resilient(
    store: Any, config: Any, alpha_db: Path, run_id: str, round_id: str,
    policy: Mapping[str, Any], project_dir: Path, *, continuous_enabled: bool,
    retry_seconds: float = 60.0,
) -> Dict[str, Any]:
    """Degrade derived report failures in Continuous mode, never core DB failures.

    SQLite failures still propagate because they may mean durable truth is unsafe.
    Filesystem/rendering failures are control-plane degradation: the research
    loop continues and a durable REPORT wait schedules a later rebuild.
    """
    if not continuous_enabled:
        return _write_reports(store, config, alpha_db, run_id, round_id, policy, project_dir)
    try:
        files = _write_reports(store, config, alpha_db, run_id, round_id, policy, project_dir)
    except sqlite3.Error:
        raise
    except Exception as exc:
        now = _now()
        due = (datetime.now(timezone.utc) + timedelta(seconds=max(1.0, float(retry_seconds)))).isoformat()
        message = f"{type(exc).__name__}: {exc}"
        with store.connect() as conn:
            conn.execute(
                """INSERT INTO ppl_endpoint_waits(
                       run_id,endpoint_type,wait_state,next_retry_at,retry_after_seconds,
                       consecutive_failures,last_error,created_at,updated_at
                   ) VALUES (?, 'REPORT', 'WAIT_REPORT', ?, ?, 1, ?, ?, ?)
                   ON CONFLICT(run_id,endpoint_type) DO UPDATE SET
                       wait_state='WAIT_REPORT',next_retry_at=excluded.next_retry_at,
                       retry_after_seconds=excluded.retry_after_seconds,
                       consecutive_failures=ppl_endpoint_waits.consecutive_failures+1,
                       last_error=excluded.last_error,updated_at=excluded.updated_at""",
                (run_id, due, float(retry_seconds), message, now, now),
            )
        record_event(
            store, round_id, run_id, "REPORT_DEGRADED",
            phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
            payload={"error": message, "next_retry_at": due, "research_continues": True},
        )
        return {"degraded": True, "error": message, "next_retry_at": due}
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_endpoint_waits(
                   run_id,endpoint_type,wait_state,next_retry_at,retry_after_seconds,
                   consecutive_failures,last_error,created_at,updated_at
               ) VALUES (?, 'REPORT', 'READY', NULL, NULL, 0, NULL, ?, ?)
               ON CONFLICT(run_id,endpoint_type) DO UPDATE SET
                   wait_state='READY',next_retry_at=NULL,retry_after_seconds=NULL,
                   consecutive_failures=0,last_error=NULL,updated_at=excluded.updated_at""",
            (run_id, now, now),
        )
    return dict(files)



def _timestamp_age_seconds(value: Any, *, now: Optional[datetime] = None) -> float:
    if not value:
        return 0.0
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (current - parsed.astimezone(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return 0.0


def _oldest_age_seconds(rows: Sequence[Mapping[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    now = datetime.now(timezone.utc)
    return max((_timestamp_age_seconds(row.get(key), now=now) for row in rows), default=0.0)


def _completed_research_streak(store: Any, round_id: str) -> Tuple[Optional[SchedulerActionType], int]:
    batches = [
        dict(row) for row in load_batches(store, round_id)
        if str(row.get("status") or "").upper() in COMPLETED_RESEARCH_BATCH_STATUSES
        and str(row.get("phase") or "").upper() in {"SEARCH", "REPAIR"}
    ]
    if not batches:
        return None, 0
    batches.sort(key=lambda row: int(row.get("batch_no") or 0))
    action = SchedulerActionType(str(batches[-1].get("phase") or "").upper())
    count = 0
    for row in reversed(batches):
        if str(row.get("phase") or "").upper() != action.value:
            break
        count += 1
    return action, count


def _availability_with_slots(*, raw: int, selector: int, safe: int, executable: int,
                             slots_free: int, complete: bool = True,
                             reason: str = "AVAILABLE") -> ResearchAvailabilityFacts:
    executable = max(0, int(executable)) if complete else 0
    return ResearchAvailabilityFacts(
        raw_backlog_count=max(0, int(raw)),
        selector_eligible_count=max(0, int(selector)),
        preview_safe_count=max(0, int(safe)),
        execution_eligible_count=executable,
        evaluation_complete=bool(complete), reason=str(reason),
        remote_slots_free=max(0, int(slots_free)),
        immediately_dispatchable_count=min(executable, max(0, int(slots_free))),
    )


def _search_availability_read_only(store: Any, alpha_db: Path, run_id: str, round_id: str,
                                   policy: Mapping[str, Any], slots_free: int) -> ResearchAvailabilityFacts:
    """Build SEARCH selector evidence without decisions, lifecycle changes, or schema bootstrap."""
    try:
        candidates = [dict(row) for row in store.load_candidates(run_id)]
        raw_rows = [
            row for row in candidates
            if not row.get("parent_candidate_id")
            and str(row.get("lifecycle_state") or "").upper() == "PLANNED"
            and str(row.get("simulation_status") or "NONE").upper() in {"", "NONE"}
        ]
        protected = {w["family_id"] for w in load_winners(store, round_id) if int(w.get("protected") or 0)}
        selector_rows = [
            row for row in raw_rows
            if str(row.get("structure_status") or "ELIGIBLE").upper() == "ELIGIBLE"
            and family_id(row) not in protected
        ]
        preview_safe = 0
        for row in selector_rows:
            action = str(classify_cache_read_only(alpha_db, str(row.get("sim_key") or "")).get("execution_action") or "")
            if action != "HOLD_UNCERTAIN" and (action in {"CACHE_RESTORE", "RESUME_EXISTING"} or requires_extension_new_post(action)):
                preview_safe += 1
        selected = _select_search_batch(
            store, alpha_db, run_id, round_id, policy, 1_000_000_000,
            batch_no=None, skip_uncertain=True,
        )
        executable = len([row for row in selected if not row.get("parent_candidate_id")])
        reason = "AVAILABLE" if executable else "NO_EXECUTABLE_SEARCH_EVIDENCE"
        return _availability_with_slots(
            raw=len(raw_rows), selector=len(selector_rows), safe=preview_safe,
            executable=executable, slots_free=slots_free, reason=reason,
        )
    except Exception as exc:
        return _availability_with_slots(
            raw=0, selector=0, safe=0, executable=0, slots_free=slots_free,
            complete=False, reason=f"SEARCH_AVAILABILITY_INSUFFICIENT:{type(exc).__name__}",
        )


def _repair_availability_read_only(
    store: Any, config: Any, alpha_db: Path, machine: Any, run_id: str,
    round_id: str, policy: Mapping[str, Any], slots_free: int, *,
    external_evidence_path: Optional[Path] = None,
) -> ResearchAvailabilityFacts:
    """Selector-parity Repair availability using the shared read-only core."""
    try:
        rr = get_round(store, round_id=round_id) or {}
        remaining = int(phase_capacity(policy, rr, "REPAIR").capacity) if rr else 0
        preparation = _read_only_repair_preparation(store, config, alpha_db, run_id, round_id)
        evidence_path = Path(external_evidence_path) if external_evidence_path is not None else Path(config.project_dir) / "rescue_evidence.json"
        evaluation = _evaluate_repair_eligibility_core(
            store, config, alpha_db, machine, run_id, round_id, policy, remaining,
            evidence_path, skip_uncertain=True,
            extra_plan_rows=preparation.get("virtual_plan_rows") or (), emit_audit=False,
        )
        durable_raw = [
            dict(row) for row in store.load_repair_plans(run_id)
            if str(row.get("plan_status") or "").upper() in EXECUTABLE_REPAIR_STATUSES
            and not str(row.get("blocked_reason") or "").strip()
        ]
        raw_count = len(durable_raw) + len(preparation.get("virtual_plan_rows") or [])
        selector_count = len(evaluation.get("ranked") or []) + len(evaluation.get("direction_ranked") or [])
        preview_safe_count = len(set(str(x) for x in (evaluation.get("preview_safe_plan_ids") or []) if str(x)))
        executable_count = len(set(str(x) for x in (evaluation.get("eligible_plan_ids") or []) if str(x)))
        complete = bool(preparation.get("evaluation_complete", True)) and bool(evaluation.get("evaluation_complete", True))
        if not complete:
            reasons = [str(x.get("reason") or x) for x in (preparation.get("incomplete_reasons") or [])]
            reason = "REPAIR_SELECTOR_PARITY_INCOMPLETE" + ((":" + ",".join(reasons)) if reasons else "")
        else:
            reason = "AVAILABLE" if executable_count else "NO_EXECUTABLE_REPAIR_EVIDENCE"
        return _availability_with_slots(
            raw=raw_count, selector=selector_count, safe=preview_safe_count,
            executable=executable_count, slots_free=slots_free,
            complete=complete, reason=reason,
        )
    except Exception as exc:
        return _availability_with_slots(
            raw=0, selector=0, safe=0, executable=0, slots_free=slots_free,
            complete=False, reason=f"REPAIR_AVAILABILITY_INSUFFICIENT:{type(exc).__name__}",
        )

def _scheduler_shadow_observation(
    store: Any,
    config: Any,
    run_id: str,
    round_id: str,
    policy: Mapping[str, Any],
    *,
    alpha_db: Optional[Path] = None,
    machine: Any = None,
    batch_no: int,
    actual_action: SchedulerActionType,
    selected_count: int,
    selected_ids: Optional[Sequence[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Build and persist a D1 shadow recommendation.

    This helper is intentionally observational.  Its return value may be added
    to reports/telemetry, but must never replace ``actual_action`` or the
    selected Search/Repair identities in D1.
    """
    raw = dict(policy.get("scheduler_shadow") or {})
    if not bool(raw.get("enabled", False)):
        return None
    try:
        shadow_policy = policy_from_mapping(raw)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    if str(raw.get("mode") or "") != SHADOW_ONLY_MODE:
        raise ConfigError("SCHEDULER_SHADOW_MODE_UNSUPPORTED")

    evidence_raw = dict(policy.get("scheduler_evidence") or {})
    evidence_enabled = bool(evidence_raw.get("enabled", False))
    evidence_policy = None
    if evidence_enabled:
        try:
            evidence_policy = evidence_policy_from_mapping(evidence_raw)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        # Mature any earlier actual-action outcomes before computing the next
        # productivity snapshot.  This is evidence refresh only and cannot
        # select or execute work.
        refresh_scheduler_outcomes(store, round_id, run_id)

    ledger = load_ledger(store, round_id)
    search_productivity = productivity_windows(
        ledger, SchedulerActionType.SEARCH, shadow_policy.productivity_windows,
    )
    repair_productivity = productivity_windows(
        ledger, SchedulerActionType.REPAIR, shadow_policy.productivity_windows,
    )

    candidates = [dict(row) for row in store.load_candidates(run_id)]
    search_rows = [
        row for row in candidates
        if not row.get("parent_candidate_id")
        and str(row.get("lifecycle_state") or "").upper() == "PLANNED"
        and str(row.get("simulation_status") or "NONE").upper() in {"", "NONE"}
    ]
    repair_rows = [
        dict(row) for row in store.load_repair_plans(run_id)
        if str(row.get("plan_status") or "").upper() in EXECUTABLE_REPAIR_STATUSES
        and not str(row.get("blocked_reason") or "").strip()
    ]
    search_backlog = len(search_rows)
    repair_backlog = len(repair_rows)
    if actual_action is SchedulerActionType.SEARCH:
        search_backlog = max(search_backlog, int(selected_count))
    elif actual_action is SchedulerActionType.REPAIR:
        repair_backlog = max(repair_backlog, int(selected_count))

    slot_limit = int(config.plan["runtime"].get(
        "glb_max_concurrency"
        if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
        else "other_max_concurrency",
        config.plan["runtime"].get("concurrency", 1),
    ))
    slots = remote_slot_snapshot(store, run_id, slot_limit)
    availability_alpha_db = Path(alpha_db) if alpha_db is not None else Path(config.project_dir) / "alpha_results.db"
    search_availability = _search_availability_read_only(
        store, availability_alpha_db, run_id, round_id, policy, int(slots.free_slots),
    )
    repair_availability = _repair_availability_read_only(
        store, config, availability_alpha_db, machine, run_id, round_id, policy, int(slots.free_slots),
    )
    search_backlog = max(int(search_availability.raw_backlog_count), int(selected_count) if actual_action is SchedulerActionType.SEARCH else 0)
    repair_backlog = max(int(repair_availability.raw_backlog_count), int(selected_count) if actual_action is SchedulerActionType.REPAIR else 0)
    consecutive_action, consecutive_count = _completed_research_streak(store, round_id)
    snapshot = ShadowSchedulerSnapshot(
        actual_action=actual_action,
        search_queue=QueueFacts(
            backlog=search_backlog,
            oldest_age_seconds=_oldest_age_seconds(search_rows, "created_at"),
        ),
        repair_queue=QueueFacts(
            backlog=repair_backlog,
            oldest_age_seconds=_oldest_age_seconds(repair_rows, "created_at"),
        ),
        search_availability=search_availability,
        repair_availability=repair_availability,
        search_productivity=search_productivity,
        repair_productivity=repair_productivity,
        remote_slot_limit=int(slots.slot_limit),
        remote_slots_reserved=int(slots.reserved_slots),
        consecutive_action=consecutive_action,
        consecutive_count=consecutive_count,
    )
    decision = choose_shadow_action(snapshot, shadow_policy)
    selected_identity = [str(x) for x in (selected_ids or []) if str(x)]
    selection_fingerprint = hashlib.sha256(
        _json(selected_identity).encode("utf-8")
    ).hexdigest()
    decision_timestamp = _now()
    scheduler_hash = shadow_policy_hash(shadow_policy)
    replay = (
        deterministic_replay(snapshot, shadow_policy, repetitions=evidence_policy.replay_repetitions)
        if evidence_enabled and evidence_policy is not None else None
    )
    evidence_record = None
    if evidence_enabled and evidence_policy is not None:
        evidence_record = record_scheduler_evaluation(
            store, round_id=round_id, run_id=run_id, batch_no=int(batch_no),
            snapshot=snapshot, decision=decision, scheduler_policy=shadow_policy,
            evidence_raw=evidence_raw, selected_count=int(selected_count),
            selection_fingerprint=selection_fingerprint, decision_timestamp=decision_timestamp,
            replay_report=replay,
        )
    payload = decision.as_dict()
    payload.update({
        "mode": SHADOW_ONLY_MODE,
        "batch_no": int(batch_no),
        "decision_timestamp": (evidence_record or {}).get("decision_timestamp", decision_timestamp),
        "shadow_policy_version": shadow_policy.policy_version,
        "shadow_policy_hash": scheduler_hash,
        "authoritative_scheduler_policy_version": (policy.get("policy_versions") or {}).get("scheduler"),
        "selected_count": int(selected_count),
        "selection_fingerprint": selection_fingerprint,
        "selection_identity_unchanged": True,
        "remote_slot_assumption": "CONSERVATIVE_RESEARCH_ACTION_MAY_REQUIRE_NEW_SLOT",
        "scheduler_evidence": ({
            **(evidence_record or {}),
            "replay": replay.as_dict() if replay else None,
            "counterfactual_semantics": "UNEXECUTED_ALTERNATIVE_IS_PROXY_ONLY",
            "authoritative": False,
        } if evidence_enabled else None),
    })
    source_key = hashlib.sha256(
        f"{round_id}|{run_id}|{int(batch_no)}|SCHEDULER_SHADOW_DECISION|"
        f"{actual_action.value}|{shadow_policy.policy_version}|{scheduler_hash}|{selection_fingerprint}".encode("utf-8")
    ).hexdigest()
    record_event(
        store, round_id, run_id, "SCHEDULER_SHADOW_DECISION",
        batch_no=int(batch_no), phase=actual_action.value,
        payload=payload, source_event_key=source_key,
    )
    return payload


def execute_round(store: Any, config: Any, machine: Any, session: Any, alpha_db: Path,
                  machine_path: Path, external_evidence_path: Path, round_policy_path: Path,
                  *, run_id: Optional[str] = None, allow_simulation_post: bool = False,
                  resume: bool = False, offline_discovery: bool = False,
                  max_batches: Optional[int] = None,
    extension_evidence_run: Optional[str] = None) -> Dict[str, Any]:
    policy = load_round_policy(round_policy_path, config)
    continuous = parse_continuous_policy(policy)
    strategy_integration = dict(policy.get("strategy_integration") or {})
    strategy_compat_enabled = bool(continuous.enabled and strategy_integration.get("enabled", False))
    if strategy_compat_enabled and str(strategy_integration.get("mode") or "") != SCHEDULER_COMPAT_MODE:
        raise ConfigError("V31_STRATEGY_INTEGRATION_MODE_UNSUPPORTED")
    max_batches = resolve_invocation_batch_limit(policy, max_batches)
    existing = None
    if resume:
        if extension_evidence_run:
            raise ConfigError("EXTENSION_EVIDENCE_RUN_NEW_ROUND_ONLY")
        existing = get_round(store, run_id=run_id) if run_id else get_round(store)
        if not existing:
            raise ConfigError("V3_ROUND_NOT_FOUND")
        round_id = str(existing["round_id"]); run_id = str(existing["run_id"])
    machine_hash_compatibility = validate_machine_lib_hash(
        machine_path,
        operation=(MACHINE_HASH_OPERATION_RESUME if resume else MACHINE_HASH_OPERATION_START),
        config=config, run_id=run_id, round_id=(round_id if resume else None),
    )
    if resume:
        frozen_extension_source = _extension_source_from_manifest(store, round_id)
        extension_context = (
            dict(frozen_extension_source["manifest_context"])
            if frozen_extension_source is not None else None
        )
        if extension_context is not None and str(extension_context.get("extension_policy_version") or "") != EXTENSION_POLICY_VERSION:
            raise ConfigError("EXTENSION_POLICY_VERSION_MISMATCH")
        stored_policy = json.loads(existing["config_json"])
        try:
            validate_durable_research_run_lock(stored_policy, policy, run_id=run_id)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        policy_bundle_reload_deferred = bool(
            continuous.enabled and continuous.safe_checkpoint_policy_reload
            and hot_policy_bundle_only_drift(stored_policy, policy)
            and _json(stored_policy) != _json(policy)
        )
        if policy_bundle_reload_deferred:
            # Q/Search/Repair-only edits are activated by the C6 safe policy
            # bundle checkpoint, never by generic round-policy migration.
            policy_upgraded = False
            policy = dict(stored_policy)
        else:
            policy_upgraded = _migrate_round_policy_if_allowed(store, round_id, run_id, stored_policy, policy)
        if policy_upgraded:
            manifest_config = (
                config_with_extension_execution_identity(config, extension_context)
                if extension_context is not None else config
            )
            upsert_manifest(
                store, round_id, run_id,
                _build_manifest_payload(
                    manifest_config, policy, Path(config.project_dir), run_id, round_id,
                    extension_context=extension_context,
                ),
            )
        compat = execution_hash_status_for_run(
            config, store.get_run(run_id), extension_identity=extension_context,
        )
        if not compat.get("execution_semantics_compatible"):
            raise ConfigError(f"ROUND_EXECUTION_HASH_{compat['status']}")
        if extension_context is not None:
            config = config_with_extension_execution_identity(config, extension_context)
    else:
        prepared = _prepare_new_round(store, config, policy, machine, session, alpha_db,
                                      external_evidence_path, requested_run_id=run_id, offline=offline_discovery,
                                      extension_evidence_run=extension_evidence_run)
        run_id = prepared["run_id"]; round_id = prepared["round_id"]
        config = prepared["execution_config"]

    if continuous.enabled and continuous.safe_checkpoint_policy_reload:
        restored_bundle = initialize_or_restore_policy_bundle(
            store, round_id=round_id, run_id=run_id, policy=policy,
            project_dir=Path(config.project_dir), source_path=round_policy_path,
        )
        policy = dict(restored_bundle.policy)

    if not allow_simulation_post:
        preview = round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
        preview.update({"executed": False, "reason": "ROUND_SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG"})
        return preview
    current = get_round(store, round_id=round_id)
    if str(current.get("status")) in {"COMPLETED", "STOPPED", "FAILED"}:
        return round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
    # Real execution always begins by rebuilding logical budget state from
    # durable facts.  This makes a process restart safe even if the previous
    # process died between a remote Simulation result and the batch commit.
    _reconcile_round_accounting(
        store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True,
        enforce_budget_limits=continuous.global_budget_enforced,
    )
    run = store.get_run(run_id) or {}
    if str(run.get("status")) in {"READY_FOR_EXECUTION", "PAUSED"}:
        store.transition_run(run_id, "EXECUTING", reason="V3 round execution", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
    update_round(store, round_id, status="RUNNING")
    batches_done_this_call = 0
    continuous_startup_reconciled = False
    progress_renderer = ContinuousProgressRenderer() if continuous.enabled else None

    def _emit_continuous_progress(*, checking_count: int = 0) -> None:
        if progress_renderer is None:
            return
        try:
            slot_limit = int(config.plan["runtime"].get(
                "glb_max_concurrency"
                if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
                else "other_max_concurrency",
                config.plan["runtime"].get("concurrency", 1),
            ))
            slot_view = remote_slot_snapshot(store, run_id, slot_limit)
            search_availability = _search_availability_read_only(
                store, alpha_db, run_id, round_id, policy, int(slot_view.free_slots),
            )
            repair_availability = _repair_availability_read_only(
                store, config, alpha_db, machine, run_id, round_id, int(slot_view.free_slots),
            )
            progress_renderer.emit(build_continuous_progress_snapshot(
                store, run_id, round_id, policy,
                search_availability=search_availability,
                repair_availability=repair_availability,
                checking_count=checking_count,
                remote_slot_limit=int(slot_view.slot_limit),
            ))
        except Exception:
            return

    def _emit_reports() -> Dict[str, Any]:
        return _write_reports_resilient(
            store, config, alpha_db, run_id, round_id, policy, Path(config.project_dir),
            continuous_enabled=continuous.enabled,
            retry_seconds=max(30.0, float(continuous.idle_wait_seconds)),
        )

    try:
        while True:
            if continuous.enabled and _report_retry_due(store, run_id):
                _emit_reports()
            if max_batches is not None and batches_done_this_call >= int(max_batches):
                update_round(store, round_id, status="PAUSED", stop_reason="MAX_BATCHES_PER_INVOCATION")
                store.transition_run(run_id, "PAUSED", reason="V3 invocation batch limit", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
                record_event(store, round_id, run_id, "ROUND_PAUSED", phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
                             payload={"reason": "MAX_BATCHES_PER_INVOCATION", "batches_done_this_call": batches_done_this_call})
                sync_durable_events(store, round_id, run_id)
                _emit_reports()
                break
            _reconcile_round_accounting(
                store, alpha_db, run_id, round_id, fail_on_unresolved_intent=True,
                enforce_budget_limits=continuous.global_budget_enforced,
            )
            rr = get_round(store, round_id=round_id) or {}
            search_remaining = phase_capacity(policy, rr, "SEARCH").capacity
            repair_remaining = phase_capacity(policy, rr, "REPAIR").capacity

            # Crash-recovered batches are unfinished batches, not historical
            # completed batches. Finalize the oldest one before allocating any
            # new batch number. A recovered finalization counts toward
            # --max-batches for this invocation.
            recovered_batch = _next_recovered_batch(store, round_id)
            if recovered_batch is not None:
                recovered_phase = str(recovered_batch.get("phase") or "").upper()
                if recovered_phase == "SEARCH":
                    recovered_report = _finalize_recovered_search_batch(
                        store, config, machine, session, alpha_db, run_id, round_id,
                        recovered_batch, policy, Path(config.project_dir),
                    )
                elif recovered_phase == "REPAIR":
                    recovered_report = _finalize_recovered_repair_batch(
                        store, config, machine, session, alpha_db, run_id, round_id,
                        recovered_batch, policy, Path(config.project_dir),
                    )
                else:
                    raise ConfigError(f"ROUND_RECOVERED_PHASE_UNSUPPORTED:{recovered_phase}")
                if recovered_report.get("finalized"):
                    batches_done_this_call += 1
                    continue
                if continuous.enabled and continuous.recoverable_failures_wait:
                    # A recovered SEARCH tail may be waiting for server capacity.
                    # Recovery is intentionally checked before normal new-work
                    # allocation, but it must not starve the Poll Queue that can
                    # release those slots. Service durable remote/auth work once
                    # before sleeping, then retry the same recovered batch.
                    if continuous.poll_remote_without_blocking_worker:
                        sync_remote_work_from_durable_facts(
                            store, alpha_db, run_id, force_due_existing=False,
                        )
                        if continuous.check_queue_enabled:
                            recover_waiting_auth(
                                store, machine, session, run_id,
                                retry_seconds=continuous.auth_retry_seconds,
                            )
                        poll_due_remote_work(
                            store, config, machine, session, alpha_db, run_id,
                            limit=max(1, int(continuous.remote_poll_max_per_cycle)),
                            poll_interval_seconds=continuous.remote_poll_interval_seconds,
                            network_backoff_seconds=continuous.network_backoff_initial_seconds,
                            max_network_backoff_seconds=continuous.network_backoff_max_seconds,
                        )
                    wait_view = due_snapshot(
                        store, run_id, default_wait_seconds=continuous.idle_wait_seconds,
                        max_wait_seconds=continuous.due_sleep_max_seconds,
                    )
                    record_event(
                        store, round_id, run_id, "CONTINUOUS_WAIT", phase=recovered_phase,
                        payload={"reason": str(recovered_report.get("reason") or "RECOVERED_BATCH_STILL_NONTERMINAL"),
                                 "batch_no": int(recovered_batch.get("batch_no") or 0),
                                 "wait_seconds": wait_view.wait_seconds,
                                 "next_due_at": wait_view.next_due_at},
                    )
                    if wait_view.wait_seconds > 0:
                        time.sleep(wait_view.wait_seconds)
                    continue
                update_round(store, round_id, status="PAUSED", stop_reason="RECOVERED_BATCH_STILL_NONTERMINAL")
                store.transition_run(run_id, "PAUSED", reason="Recovered batch still nonterminal", source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
                sync_durable_events(store, round_id, run_id)
                _emit_reports()
                break

            # C6 safe checkpoint: no unfinished batch exists at this point,
            # and no new SEARCH/REPAIR allocation has started. Qualification,
            # Search and Repair policy edits may activate atomically here; all
            # other policy drift is rejected and last-known-good stays active.
            if continuous.enabled and continuous.safe_checkpoint_policy_reload:
                try:
                    candidate_policy = load_round_policy(round_policy_path, config)
                except Exception as exc:
                    error_text = f"{type(exc).__name__}: {exc}"
                    error_digest = hashlib.sha256(error_text.encode("utf-8")).hexdigest()
                    record_event(
                        store, round_id, run_id, "POLICY_BUNDLE_RELOAD_REJECTED",
                        batch_no=int(rr.get("current_batch") or 0),
                        phase=str(rr.get("phase") or "SEARCH"),
                        payload={
                            "reason": "ROUND_POLICY_FILE_INVALID",
                            "error": error_text,
                            "research_continues_with_last_durable_policy": True,
                        },
                        source_event_key=f"policy_bundle_reject:{round_id}:ROUND_POLICY_FILE_INVALID:{error_digest}",
                    )
                else:
                    reload_result = apply_policy_bundle_safe_checkpoint(
                        store, round_id=round_id, run_id=run_id,
                        active_policy=policy, candidate_policy=candidate_policy,
                        project_dir=Path(config.project_dir), source_path=round_policy_path,
                        batch_no=int(rr.get("current_batch") or 0),
                        phase=str(rr.get("phase") or "SEARCH"), checkpoint_safe=True,
                    )
                    policy = dict(reload_result.policy)

            # V3.1 Continuous: durable remote identity is reconciled into a
            # non-blocking Poll Queue.  Legacy V3 keeps the historical
            # resume-first worker behavior unchanged.
            if continuous.enabled and continuous.poll_remote_without_blocking_worker:
                sync_remote_work_from_durable_facts(
                    store, alpha_db, run_id,
                    force_due_existing=(not continuous_startup_reconciled and continuous.startup_reconcile_before_new_post),
                )
                poll_report = poll_due_remote_work(
                    store, config, machine, session, alpha_db, run_id,
                    limit=max(1, int(continuous.remote_poll_max_per_cycle)),
                    poll_interval_seconds=continuous.remote_poll_interval_seconds,
                    network_backoff_seconds=continuous.network_backoff_initial_seconds,
                    max_network_backoff_seconds=continuous.network_backoff_max_seconds,
                )
                continuous_startup_reconciled = True
                if progress_renderer is not None:
                    progress_renderer.emit_completed_results(list(poll_report.get("completed_results") or []))
                completed_from_poll = [str(x) for x in (poll_report.get("completed_candidate_ids") or []) if x]
                # A Continuous cycle performs at most one remote GET per due
                # item and hands PRE_TAG work to a separate durable Check Queue.
                # No candidate may enter the legacy in-function semantic poll loop.
                analyzed_poll = _continuous_analyze_and_enqueue_checks(
                    store, config, alpha_db, run_id, completed_from_poll, repair_remaining
                ) if continuous.check_queue_enabled else _analyze_and_check(
                    store, config, machine, session, alpha_db, run_id, completed_from_poll, repair_remaining
                )
                if completed_from_poll:
                    derive_check_repair_proposals(store, config, alpha_db, run_id, persist=True)
                    reconcile_completed_repair_outcomes(
                        store, config, alpha_db, run_id, completed_from_poll
                    )

                auth_report = recover_waiting_auth(
                    store, machine, session, run_id, retry_seconds=continuous.auth_retry_seconds
                ) if continuous.check_queue_enabled else {"attempted": False}
                check_poll_report = poll_due_checks(
                    store, config, machine, session, run_id,
                    limit=max(1, int(continuous.check_poll_max_per_cycle)),
                    poll_interval_seconds=continuous.check_poll_interval_seconds,
                    network_backoff_seconds=continuous.network_backoff_initial_seconds,
                    max_network_backoff_seconds=continuous.network_backoff_max_seconds,
                ) if continuous.check_queue_enabled else {"polled": 0, "resolved_candidate_ids": []}
                resolved_check_ids = [str(x) for x in (check_poll_report.get("resolved_candidate_ids") or []) if x]
                if resolved_check_ids:
                    derive_check_repair_proposals(store, config, alpha_db, run_id, persist=True)
                discovery_poll_report = poll_due_discovery_work(
                    store, config, machine, session, run_id,
                    limit=max(1, int(continuous.discovery_poll_max_per_cycle)),
                    poll_interval_seconds=continuous.discovery_poll_interval_seconds,
                    network_backoff_seconds=continuous.network_backoff_initial_seconds,
                    max_network_backoff_seconds=continuous.network_backoff_max_seconds,
                    deterministic_failure_cooldown_seconds=continuous.discovery_failure_cooldown_seconds,
                ) if continuous.discovery_queue_enabled else {"polled": 0, "ready_apply_ids": []}
                discovery_apply_reports = _apply_ready_continuous_discovery(
                    store, config, machine, alpha_db, run_id, round_id, policy,
                    limit=max(1, int(continuous.discovery_poll_max_per_cycle)),
                ) if continuous.discovery_queue_enabled else []
                _emit_continuous_progress(checking_count=int(check_poll_report.get("polled") or 0))
                # Refresh the existing research-ledger rows for asynchronous
                # completions/missing remotes.  No new batch number is supplied:
                # sync_simulation_ledger retains the original batch/phase/origin
                # already recorded at dispatch time.
                poll_terminal_ids = sorted({
                    *completed_from_poll,
                    *[str(x) for x in (poll_report.get("remote_missing_candidate_ids") or []) if x],
                })
                if poll_terminal_ids:
                    _sync_research_telemetry(
                        store, config, alpha_db, run_id, round_id,
                        candidate_ids=poll_terminal_ids,
                    )
                    if bool((policy.get("scheduler_evidence") or {}).get("enabled", False)):
                        refresh_scheduler_outcomes(store, round_id, run_id)
                _round_runtime_guard(store, run_id, global_hold=False)
                finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
                slot_limit = int(config.plan["runtime"].get(
                    "glb_max_concurrency" if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
                    else "other_max_concurrency",
                    config.plan["runtime"].get("concurrency", 1),
                ))
                slots = remote_slot_snapshot(store, run_id, slot_limit)
                resume_report = {
                    "mode": "CONTINUOUS_POLL_QUEUE",
                    "polled": int(poll_report.get("polled") or 0),
                    "completed": len(completed_from_poll),
                    "checks": int(analyzed_poll.get("check_count") or 0),
                    "checks_queued": int((analyzed_poll.get("queued_checks") or {}).get("queued_count") or 0),
                    "check_queue_polled": int(check_poll_report.get("polled") or 0),
                    "check_queue_resolved": len(resolved_check_ids),
                    "discovery_queue_polled": int(discovery_poll_report.get("polled") or 0),
                    "discovery_applied": sum(not bool(row.get("suppressed")) for row in discovery_apply_reports),
                    "discovery_suppressed": sum(bool(row.get("suppressed")) for row in discovery_apply_reports),
                    "auth_refresh": auth_report,
                    "candidate_ids": completed_from_poll,
                    "still_nonterminal_candidate_ids": [],
                    "remote_slots_reserved": slots.reserved_slots,
                    "remote_slots_free": slots.free_slots,
                    "uncertain_reserved": slots.uncertain,
                }
                if slots.free_slots <= 0:
                    wait_view = due_snapshot(
                        store, run_id, default_wait_seconds=continuous.idle_wait_seconds,
                        max_wait_seconds=continuous.due_sleep_max_seconds,
                    )
                    record_event(
                        store, round_id, run_id, "CONTINUOUS_WAIT",
                        phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
                        payload={
                            "reason": "WAIT_SERVER_SLOT",
                            "remote_slots_reserved": slots.reserved_slots,
                            "remote_slot_limit": slots.slot_limit,
                            "uncertain_reserved": slots.uncertain,
                            "wait_seconds": wait_view.wait_seconds,
                            "next_due_at": wait_view.next_due_at,
                        },
                    )
                    time.sleep(wait_view.wait_seconds)
                    continue
            else:
                # Resume-first before any NEW batch POST.
                resume_report = _resume_nonterminal(
                    store, config, machine, session, alpha_db, run_id, repair_remaining, round_id=round_id
                )
                if resume_report.get("candidate_ids"):
                    sync_simulation_ledger(
                        store, alpha_db, round_id, run_id,
                        candidate_ids=resume_report.get("candidate_ids"),
                        origin_by_candidate={str(cid): "RESUME" for cid in resume_report.get("candidate_ids", [])},
                        selection_mode_by_candidate={str(cid): "RESUME_FIRST" for cid in resume_report.get("candidate_ids", [])},
                    )
                _round_runtime_guard(store, run_id)
                finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
                still_nonterminal = [str(x) for x in (resume_report.get("still_nonterminal_candidate_ids") or []) if x]
                if still_nonterminal:
                    update_round(store, round_id, status="PAUSED", stop_reason="RESUME_FIRST_STILL_NONTERMINAL")
                    store.transition_run(
                        run_id, "PAUSED", reason="Resume-first simulations still nonterminal; no new POST allocated",
                        source=ROUND_SOURCE, allowed=RUN_TRANSITIONS,
                        metadata={"candidate_ids": still_nonterminal},
                    )
                    record_event(
                        store, round_id, run_id, "ROUND_PAUSED", phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
                        payload={"reason": "RESUME_FIRST_STILL_NONTERMINAL", "candidate_ids": still_nonterminal},
                    )
                    sync_durable_events(store, round_id, run_id)
                    _emit_reports()
                    break
            rr = get_round(store, round_id=round_id) or rr
            phase = str(rr.get("phase") or "SEARCH")
            transient_check_recovery = (
                {"enabled": False, "requested": 0, "executed": 0, "resolved": 0, "reports": [],
                 "reason": "V31_DURABLE_CHECK_QUEUE"}
                if continuous.enabled and continuous.check_queue_enabled
                else _recover_transient_pretag_checks(
                    store, config, machine, session, run_id, round_id, phase=phase,
                )
            )
            batch_no = int(rr.get("current_batch") or 0) + 1
            refresh_report = None
            if phase == "SEARCH" and search_remaining > 0:
                if continuous.enabled and continuous.poll_remote_without_blocking_worker:
                    slot_limit = int(config.plan["runtime"].get(
                        "glb_max_concurrency" if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
                        else "other_max_concurrency",
                        config.plan["runtime"].get("concurrency", 1),
                    ))
                    slots = remote_slot_snapshot(store, run_id, slot_limit)
                    search_remaining = min(int(search_remaining), int(slots.free_slots))
                    if search_remaining <= 0:
                        wait_view = due_snapshot(
                            store, run_id, default_wait_seconds=continuous.idle_wait_seconds,
                            max_wait_seconds=continuous.due_sleep_max_seconds,
                        )
                        record_event(store, round_id, run_id, "CONTINUOUS_WAIT", phase="SEARCH",
                                     payload={"reason": "WAIT_SERVER_SLOT", "wait_seconds": wait_view.wait_seconds,
                                              "next_due_at": wait_view.next_due_at})
                        time.sleep(wait_view.wait_seconds)
                        continue
                _bootstrap_dataset_states(store, run_id, round_id)
                trigger = _refresh_trigger(store, run_id, round_id, policy)
                if trigger:
                    if session is None:
                        raise ConfigError("ROUND_ROLLING_DISCOVERY_REQUIRES_LIVE_SESSION")
                    if continuous.enabled and continuous.discovery_queue_enabled:
                        refresh_report = _enqueue_continuous_rolling_discovery(
                            store, config, run_id, round_id, policy,
                            batch_no=int(rr.get("current_batch") or 0), trigger=trigger,
                        )
                    else:
                        refresh_report = _append_rolling_candidates(
                            store, config, machine, ReadOnlySession(session), alpha_db, run_id, round_id, policy,
                            batch_no=int(rr.get("current_batch") or 0), trigger=trigger,
                        )
                rows = _select_search_batch(
                    store, alpha_db, run_id, round_id, policy, search_remaining, batch_no=batch_no,
                    skip_uncertain=(continuous.enabled and continuous.recoverable_failures_wait),
                )
                if not rows:
                    pending_initial = _pending_selected_initial_posts(store, run_id)
                    if pending_initial:
                        raise ConfigError(f"ROUND_INITIAL_SEARCH_PENDING:{pending_initial}")
                    update_round(store, round_id, phase="REPAIR")
                    record_event(store, round_id, run_id, "ROUND_PHASE_CHANGED", phase="REPAIR",
                                 payload={"from": "SEARCH", "to": "REPAIR", "reason": "NO_MORE_SAFE_SEARCH_CANDIDATES"})
                    continue
                search_strategy_report = None
                if strategy_compat_enabled:
                    search_strategy_decisions = search_decisions_from_selected_rows(run_id, rows, policy)
                    scheduler_decision = choose_compatibility_strategy_action(
                        search_decisions=search_strategy_decisions,
                        wait_reason="SEARCH_COMPATIBILITY_SELECTOR_EMPTY",
                        enforce_remote_slots=False,
                    )
                    if scheduler_decision.action is not SchedulerActionType.SEARCH:
                        raise ConfigError(
                            f"V31_SEARCH_SCHEDULER_COMPAT_INVARIANT:{scheduler_decision.action.value}"
                        )
                    selected_by_id = {str(row.get("candidate_id") or ""): row for row in rows}
                    decision_ids = [str(item.candidate_id) for item in search_strategy_decisions]
                    if len(decision_ids) != len(rows) or set(decision_ids) != set(selected_by_id):
                        raise ConfigError("V31_SEARCH_ADAPTER_SELECTION_MISMATCH")
                    rows = [selected_by_id[cid] for cid in decision_ids]
                    versions = policy_versions_from_policy(policy)
                    search_strategy_report = {
                        "integration_mode": SCHEDULER_COMPAT_MODE,
                        "action": scheduler_decision.action.value,
                        "strategy": SEARCH_COMPAT_STRATEGY,
                        "decision_count": len(search_strategy_decisions),
                        "search_policy_version": versions.search,
                        "search_policy_hash": search_policy_hash(policy),
                        "scheduler_policy_version": versions.scheduler,
                    }
                    record_event(
                        store, round_id, run_id, "STRATEGY_ACTION_SELECTED", batch_no=batch_no, phase="SEARCH",
                        payload=search_strategy_report,
                    )
                search_shadow_report = (
                    _scheduler_shadow_observation(
                        store, config, run_id, round_id, policy, alpha_db=alpha_db, machine=machine,
                        batch_no=batch_no, actual_action=SchedulerActionType.SEARCH,
                        selected_count=len(rows),
                        selected_ids=[str(row.get("candidate_id") or "") for row in rows],
                    )
                    if continuous.enabled else None
                )
                _promote_search_selection(store, run_id, rows)
                projected = sum(1 for r in rows if r.get("round_cache_action") not in {"CACHE_RESTORE", "RESUME_EXISTING"})
                start_batch(store, round_id, batch_no, "SEARCH", candidate_ids=[r["candidate_id"] for r in rows], projected_new_posts=projected)
                record_event(
                    store, round_id, run_id, "BATCH_STARTED", batch_no=batch_no, phase="SEARCH",
                    payload={"candidate_ids": [r["candidate_id"] for r in rows], "projected_new_posts": projected,
                             "selection_modes": Counter(str(r.get("round_selection_mode") or "UNKNOWN") for r in rows)},
                )
                result = _execute_search_rows(store, config, machine, session, alpha_db, run_id, rows,
                                              allow_simulation_post=True, remaining_search_budget=search_remaining,
                                              round_id=round_id, batch_no=batch_no,
                                              extension_new_post_cap=_extension_batch_cap(int(effective_search_allocation(policy)["batch_size"])),
                                              nonblocking_remote=(continuous.enabled and continuous.poll_remote_without_blocking_worker),
                                              global_hold_on_uncertain=not (continuous.enabled and continuous.recoverable_failures_wait))
                analyzed = (
                    _continuous_analyze_and_enqueue_checks(
                        store, config, alpha_db, run_id, result.get("complete_candidate_ids", []), repair_remaining
                    )
                    if continuous.enabled and continuous.check_queue_enabled
                    else _analyze_and_check(
                        store, config, machine, session, alpha_db, run_id,
                        result.get("complete_candidate_ids", []), repair_remaining
                    )
                )
                derive_check_repair_proposals(store, config, alpha_db, run_id, persist=True)
                winners = finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
                consumed = int(result.get("post_consumed") or 0)
                update_round(store, round_id, search_consumed=int(rr["search_consumed"]) + consumed,
                             current_batch=batch_no, stop_reason=None)
                fresh_check_cids = [
                    str(x.get("candidate_id")) for x in analyzed.get("checks", [])
                    if x.get("executed") and x.get("candidate_id")
                ]
                manual_refresh = _maybe_auto_refresh_manual_finalization(
                    store, config, machine, session, alpha_db, run_id, round_id, policy, Path(config.project_dir),
                    batch_no=batch_no, fresh_candidate_ids=fresh_check_cids,
                )
                total_check_count = int(analyzed.get("check_count") or 0) + int(manual_refresh.get("executed_check_count") or 0)
                batch_completion_semantics = (
                    "REMOTE_HANDOFF_COMPLETE"
                    if continuous.enabled and continuous.poll_remote_without_blocking_worker
                    else "SIMULATION_WORKFLOW_COMPLETE"
                )
                report = {"phase": "SEARCH", "resume_first": resume_report, "dataset_refresh": refresh_report,
                          "strategy_integration": search_strategy_report,
                          "scheduler_shadow": search_shadow_report,
                          "execution": result, "analysis": analyzed, "manual_finalization_refresh": manual_refresh,
                          "winners": winners,
                          "batch_completion_semantics": batch_completion_semantics,
                          "remote_nonterminal_count": len(result.get("nonterminal_candidate_ids") or [])}
                finish_batch(store, round_id, batch_no, report, logical_posts_consumed=consumed,
                             cache_hits=int(result.get("cache_hits") or 0), resume_count=int(result.get("resume_count") or 0),
                             check_count=total_check_count)
                origin_map = {}
                mode_map = {}
                for row in rows:
                    cid = str(row.get("candidate_id"))
                    action = str(row.get("round_cache_action") or "NEW_SIMULATION_REQUIRED")
                    origin_map[cid] = "CACHE" if action == "CACHE_RESTORE" else "RESUME" if action == "RESUME_EXISTING" else "NEW_POST"
                    mode_map[cid] = str(row.get("round_selection_mode") or "UNKNOWN")
                record_event(store, round_id, run_id, "BATCH_COMPLETE", batch_no=batch_no, phase="SEARCH",
                             payload={"logical_posts_consumed": consumed, "cache_hits": int(result.get("cache_hits") or 0),
                                      "resume_count": int(result.get("resume_count") or 0),
                                      "check_count": total_check_count,
                                      "manual_queue_refresh_checks": int(manual_refresh.get("executed_check_count") or 0),
                                      "winner_count": int(winners.get("protected_total") or 0),
                                      "completion_semantics": batch_completion_semantics,
                                      "remote_nonterminal_count": len(result.get("nonterminal_candidate_ids") or [])})
                _sync_research_telemetry(
                    store, config, alpha_db, run_id, round_id, batch_no=batch_no, phase="SEARCH",
                    origin_by_candidate=origin_map, selection_mode_by_candidate=mode_map, policy=policy,
                )
                if bool((policy.get("scheduler_evidence") or {}).get("enabled", False)):
                    refresh_scheduler_outcomes(store, round_id, run_id)
                _emit_reports()
                batches_done_this_call += 1
                continue
            if phase == "SEARCH":
                pending_initial = _pending_selected_initial_posts(store, run_id)
                if pending_initial:
                    raise ConfigError(f"ROUND_INITIAL_SEARCH_PENDING:{pending_initial}")
                update_round(store, round_id, phase="REPAIR")
                record_event(store, round_id, run_id, "ROUND_PHASE_CHANGED", phase="REPAIR",
                             payload={"from": "SEARCH", "to": "REPAIR", "reason": "SEARCH_BUDGET_EXHAUSTED"})
                continue
            rr = get_round(store, round_id=round_id) or rr
            repair_remaining = phase_capacity(policy, rr, "REPAIR").capacity
            if repair_remaining > 0:
                plan_ids = _select_repair_batch(store, config, alpha_db, machine, run_id, round_id, policy,
                                                repair_remaining, external_evidence_path, batch_no=batch_no,
                                                session=session,
                                                skip_uncertain=(continuous.enabled and continuous.recoverable_failures_wait))
                if plan_ids:
                    repair_strategy_report = None
                    if strategy_compat_enabled:
                        plan_id_set = {str(x) for x in plan_ids}
                        plan_by_id = {
                            str(item.get("repair_plan_id") or ""): dict(item)
                            for item in store.load_repair_plans(run_id)
                            if str(item.get("repair_plan_id") or "") in plan_id_set
                        }
                        selected_plan_facts = [plan_by_id[str(pid)] for pid in plan_ids if str(pid) in plan_by_id]
                        repair_strategy_decisions = repair_decisions_from_selected_plans(
                            run_id, selected_plan_facts, policy
                        )
                        scheduler_decision = choose_compatibility_strategy_action(
                            repair_decisions=repair_strategy_decisions,
                            wait_reason="REPAIR_COMPATIBILITY_SELECTOR_EMPTY",
                            enforce_remote_slots=False,
                        )
                        if scheduler_decision.action is not SchedulerActionType.REPAIR:
                            raise ConfigError(
                                f"V31_REPAIR_SCHEDULER_COMPAT_INVARIANT:{scheduler_decision.action.value}"
                            )
                        decision_plan_ids = [
                            str(item.metadata.get("repair_plan_id") or "")
                            for item in repair_strategy_decisions
                        ]
                        if len(decision_plan_ids) != len(plan_ids) or decision_plan_ids != [str(x) for x in plan_ids]:
                            raise ConfigError("V31_REPAIR_ADAPTER_SELECTION_MISMATCH")
                        plan_ids = decision_plan_ids
                        versions = policy_versions_from_policy(policy)
                        repair_strategy_report = {
                            "integration_mode": SCHEDULER_COMPAT_MODE,
                            "action": scheduler_decision.action.value,
                            "strategy": REPAIR_COMPAT_STRATEGY,
                            "decision_count": len(repair_strategy_decisions),
                            "repair_policy_version": versions.repair,
                            "repair_policy_hash": repair_policy_hash(policy),
                            "scheduler_policy_version": versions.scheduler,
                        }
                        record_event(
                            store, round_id, run_id, "STRATEGY_ACTION_SELECTED", batch_no=batch_no, phase="REPAIR",
                            payload=repair_strategy_report,
                        )
                    repair_shadow_report = None
                    round_repair_preflight = preflight_round_repair_execution(
                        store, config, machine, alpha_db, machine_path,
                        run_id=run_id, round_id=round_id, batch_no=batch_no,
                        plan_ids=plan_ids, allow_simulation_post=allow_simulation_post,
                        enforce_global_repair_budget=continuous.global_budget_enforced,
                        global_hold_on_uncertain=not (continuous.enabled and continuous.recoverable_failures_wait),
                    )
                    preview = dict(round_repair_preflight["preview"])
                    projected = int(preview.get("projected_new_posts") or 0)
                    if projected > repair_remaining:
                        raise ConfigError("ROUND_REPAIR_BUDGET_EXCEEDED")
                    if continuous.enabled and continuous.poll_remote_without_blocking_worker:
                        slot_limit = int(config.plan["runtime"].get(
                            "glb_max_concurrency" if str(config.plan["simulation_settings"].get("region") or "").upper() == "GLB"
                            else "other_max_concurrency",
                            config.plan["runtime"].get("concurrency", 1),
                        ))
                        slots = remote_slot_snapshot(store, run_id, slot_limit)
                        if projected > slots.free_slots:
                            allowed_post_keys = []
                            for item in preview.get("items", []):
                                if item.get("will_post") and len(allowed_post_keys) < slots.free_slots:
                                    allowed_post_keys.append(str(item.get("repair_sim_key") or ""))
                            allowed_post_keys = {x for x in allowed_post_keys if x}
                            reduced_plan_ids = []
                            for item in preview.get("items", []):
                                if (not item.get("will_post")) or str(item.get("repair_sim_key") or "") in allowed_post_keys:
                                    pid = str(item.get("repair_plan_id") or "")
                                    if pid and pid not in reduced_plan_ids:
                                        reduced_plan_ids.append(pid)
                            if not reduced_plan_ids:
                                wait_view = due_snapshot(
                                    store, run_id, default_wait_seconds=continuous.idle_wait_seconds,
                                    max_wait_seconds=continuous.due_sleep_max_seconds,
                                )
                                record_event(
                                    store, round_id, run_id, "CONTINUOUS_WAIT", phase="REPAIR",
                                    payload={"reason": "WAIT_SERVER_SLOT",
                                             "remote_slots_reserved": slots.reserved_slots,
                                             "remote_slot_limit": slots.slot_limit,
                                             "wait_seconds": wait_view.wait_seconds,
                                             "next_due_at": wait_view.next_due_at},
                                )
                                time.sleep(wait_view.wait_seconds)
                                continue
                            plan_ids = reduced_plan_ids
                            round_repair_preflight = preflight_round_repair_execution(
                                store, config, machine, alpha_db, machine_path,
                                run_id=run_id, round_id=round_id, batch_no=batch_no,
                                plan_ids=plan_ids, allow_simulation_post=allow_simulation_post,
                                enforce_global_repair_budget=continuous.global_budget_enforced,
                                global_hold_on_uncertain=not (continuous.enabled and continuous.recoverable_failures_wait),
                            )
                            preview = dict(round_repair_preflight["preview"])
                            projected = int(preview.get("projected_new_posts") or 0)
                            if projected > slots.free_slots:
                                raise ConfigError("CONTINUOUS_REPAIR_SLOT_CAP_INVARIANT")
                    # D2 records the final compatibility-selected Repair intent
                    # after local preflight/slot trimming, but still before any
                    # execution.  The observation remains non-authoritative.
                    repair_shadow_report = (
                        _scheduler_shadow_observation(
                            store, config, run_id, round_id, policy, alpha_db=alpha_db, machine=machine,
                            batch_no=batch_no, actual_action=SchedulerActionType.REPAIR,
                            selected_count=len(plan_ids),
                            selected_ids=[str(pid) for pid in plan_ids],
                        )
                        if continuous.enabled else None
                    )
                    result = execute_round_repair(
                        store, config, machine, session, alpha_db, machine_path,
                        run_id, round_id, batch_no, plan_ids, True,
                        preflight=round_repair_preflight,
                        nonblocking_remote=(continuous.enabled and continuous.poll_remote_without_blocking_worker),
                    )
                    if result.get("deferred_sim_keys") or result.get("deferred_plan_ids"):
                        _shrink_repair_batch_deferred_intent(
                            store, run_id=run_id, round_id=round_id, batch_no=batch_no,
                            deferred_sim_keys=result.get("deferred_sim_keys") or [],
                            deferred_plan_ids=result.get("deferred_plan_ids") or [],
                        )
                    consumed = int(result.get("post_consumed") or 0)
                    _update_run_post_counters(
                        store, run_id, attempted=int(result.get("post_attempted") or 0),
                        confirmed=int(result.get("post_confirmed") or 0), uncertain=int(result.get("post_uncertain") or 0),
                        consumed=consumed,
                    )
                    update_round(store, round_id, repair_consumed=int(rr["repair_consumed"]) + consumed,
                                 current_batch=batch_no, stop_reason=None)
                    winners = finalize_family_winners(store, alpha_db, run_id, round_id, config=config)
                    fresh_repair_check_cids = [
                        str(x.get("candidate_id")) for x in (result.get("check_reports") or [])
                        if x.get("executed") and x.get("candidate_id")
                    ]
                    manual_refresh = _maybe_auto_refresh_manual_finalization(
                        store, config, machine, session, alpha_db, run_id, round_id, policy, Path(config.project_dir),
                        batch_no=batch_no, fresh_candidate_ids=fresh_repair_check_cids,
                    )
                    total_repair_check_count = len(result.get("check_reports") or []) + int(manual_refresh.get("executed_check_count") or 0)
                    batch_completion_semantics = (
                        "REMOTE_HANDOFF_COMPLETE"
                        if continuous.enabled and continuous.poll_remote_without_blocking_worker
                        else "SIMULATION_WORKFLOW_COMPLETE"
                    )
                    durable_progress = dict(result.get("durable_progress") or {})
                    remote_nonterminal_count = int(durable_progress.get("durable_running") or 0) + int(
                        durable_progress.get("durable_uncertain") or 0
                    )
                    report = {"phase": "REPAIR", "resume_first": resume_report, "preview": preview,
                              "strategy_integration": repair_strategy_report,
                              "scheduler_shadow": repair_shadow_report,
                              "execution": result, "manual_finalization_refresh": manual_refresh, "winners": winners,
                              "batch_completion_semantics": batch_completion_semantics,
                              "remote_nonterminal_count": remote_nonterminal_count}
                    finish_batch(store, round_id, batch_no, report, logical_posts_consumed=consumed,
                                 cache_hits=sum(1 for i in preview.get("items", []) if i.get("required_action") in {"CACHE_COMPLETE", "CACHE_RESTORE"}),
                                 resume_count=sum(1 for i in preview.get("items", []) if i.get("required_action") == "RESUME_EXISTING"),
                                 check_count=total_repair_check_count)
                    current_candidates = {str(c.get("sim_key")): c for c in store.load_candidates(run_id) if c.get("sim_key")}
                    origin_map = {}
                    mode_map = {}
                    plan_map = {}
                    deferred_cids = {str(x) for x in (result.get("deferred_candidate_ids") or []) if x}
                    for item in preview.get("items", []):
                        sk = str(item.get("repair_sim_key") or "")
                        child = current_candidates.get(sk)
                        if not child:
                            continue
                        cid = str(child.get("candidate_id"))
                        if cid in deferred_cids:
                            continue
                        action = str(item.get("required_action") or "")
                        origin_map[cid] = "NEW_POST" if item.get("will_post") else "RESUME" if action == "RESUME_EXISTING" else "CACHE"
                        mode_map[cid] = "REPAIR"
                        plan_map[cid] = str(item.get("repair_plan_id") or "")
                    record_event(store, round_id, run_id, "BATCH_COMPLETE", batch_no=batch_no, phase="REPAIR",
                                 payload={"logical_posts_consumed": consumed, "projected_new_posts": projected,
                                          "check_count": total_repair_check_count,
                                          "manual_queue_refresh_checks": int(manual_refresh.get("executed_check_count") or 0),
                                          "winner_count": int(winners.get("protected_total") or 0),
                                          "completion_semantics": batch_completion_semantics,
                                          "remote_nonterminal_count": remote_nonterminal_count})
                    if origin_map:
                        _sync_research_telemetry(
                            store, config, alpha_db, run_id, round_id, batch_no=batch_no, phase="REPAIR",
                            candidate_ids=sorted(origin_map),
                            origin_by_candidate=origin_map, selection_mode_by_candidate=mode_map,
                            repair_plan_by_candidate=plan_map, policy=policy,
                        )
                    else:
                        # An all-deferred Repair batch has no logical Simulation
                        # ledger rows to sync, but its durable events/snapshot are
                        # still useful for later productivity analysis.
                        sync_durable_events(store, round_id, run_id)
                        _capture_batch_snapshot(store, config, alpha_db, run_id, round_id, batch_no, "REPAIR", policy)
                    if bool((policy.get("scheduler_evidence") or {}).get("enabled", False)):
                        refresh_scheduler_outcomes(store, round_id, run_id)
                    _emit_reports()
                    batches_done_this_call += 1
                    continue
            # No more immediately executable Search/Repair work. Legacy V3
            # completes here. Continuous mode stays alive and returns to SEARCH
            # after an idle wait; later checkpoints replace this coarse wait with
            # queue-aware Discovery/Poll/Check scheduling.
            rr = get_round(store, round_id=round_id) or rr
            if continuous.enabled:
                update_round(store, round_id, status="RUNNING", phase="SEARCH", stop_reason=None)
                wait_view = due_snapshot(
                    store, run_id, default_wait_seconds=continuous.idle_wait_seconds,
                    max_wait_seconds=continuous.due_sleep_max_seconds,
                )
                record_event(
                    store, round_id, run_id, "CONTINUOUS_WAIT", phase="SEARCH",
                    payload={"reason": "NO_IMMEDIATELY_EXECUTABLE_RESEARCH_WORK",
                             "wait_seconds": wait_view.wait_seconds,
                             "next_due_at": wait_view.next_due_at,
                             "remote_due": wait_view.remote_due, "check_due": wait_view.check_due,
                             "search_consumed": int(rr.get("search_consumed") or 0),
                             "repair_consumed": int(rr.get("repair_consumed") or 0)},
                )
                sync_durable_events(store, round_id, run_id)
                time.sleep(wait_view.wait_seconds)
                continue
            total_used = int(rr["search_consumed"]) + int(rr["repair_consumed"])
            if total_used >= int(rr["total_budget"]):
                final_status, reason = "COMPLETED", "BUDGET_EXHAUSTED_NORMALLY"
            else:
                final_status, reason = "COMPLETED", "ROUND_NO_SAFE_CANDIDATE"
            update_round(store, round_id, status=final_status, phase="DONE", stop_reason=reason, completed_at=_now())
            store.transition_run(run_id, "COMPLETED", reason=reason, source=ROUND_SOURCE, allowed=RUN_TRANSITIONS)
            audit_event(action="ROUND_COMPLETE", run_id=run_id, round_id=round_id, reason=reason,
                        search_consumed=rr["search_consumed"], repair_consumed=rr["repair_consumed"])
            record_event(store, round_id, run_id, "ROUND_COMPLETE", phase="DONE",
                         payload={"reason": reason, "search_consumed": rr["search_consumed"],
                                  "repair_consumed": rr["repair_consumed"]})
            _sync_research_telemetry(store, config, alpha_db, run_id, round_id, policy=policy)
            break
    except KeyboardInterrupt:
        if not continuous.enabled:
            raise
        update_round(store, round_id, status="PAUSED", stop_reason="USER_STOP_REQUESTED")
        run_now = store.get_run(run_id) or {}
        if str(run_now.get("status")) == "EXECUTING":
            store.transition_run(
                run_id, "PAUSED", reason="V3.1 graceful user stop", source=ROUND_SOURCE,
                allowed=RUN_TRANSITIONS, metadata={"reason": "USER_STOP_REQUESTED"},
            )
        record_event(
            store, round_id, run_id, "CONTINUOUS_STOPPED_BY_USER",
            phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
            payload={"batches_done_this_call": batches_done_this_call},
        )
        sync_durable_events(store, round_id, run_id)
    except BaseException as exc:
        # Fail closed. Durable candidate/cache facts remain; the round can be
        # inspected and resumed only after the cause is understood.
        reason = f"{type(exc).__name__}: {exc}"
        update_round(store, round_id, status="PAUSED", stop_reason=reason)
        run_now = store.get_run(run_id) or {}
        if str(run_now.get("status")) == "EXECUTING":
            try:
                store.transition_run(run_id, "PAUSED", reason="V3 guard/exception pause", source=ROUND_SOURCE,
                                     allowed=RUN_TRANSITIONS, metadata={"error": reason})
            except ValueError:
                pass
        audit_event(action="ROUND_PAUSED_BY_GUARD", run_id=run_id, round_id=round_id, error_type=type(exc).__name__, reason=str(exc))
        record_event(store, round_id, run_id, "ROUND_PAUSED_BY_GUARD", phase=str((get_round(store, round_id=round_id) or {}).get("phase") or ""),
                     payload={"error_type": type(exc).__name__, "reason": str(exc)})
        try:
            _sync_research_telemetry(store, config, alpha_db, run_id, round_id, policy=policy)
            _emit_reports()
        except Exception:
            pass
        raise
    files = _emit_reports()
    out = round_status(store, config, alpha_db, run_id=run_id, round_id=round_id)
    out["executed"] = True; out["report_files"] = files
    return out
