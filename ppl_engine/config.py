"""Configuration loading, validation, and semantic hashing for Phase 1."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml


class ConfigError(ValueError):
    pass


def simulation_budget_allocation(plan: Mapping[str, Any]) -> Dict[str, int]:
    budgets = plan["budgets"]
    total = int(budgets["max_new_simulation_posts"])
    allocation = budgets.get("simulation_budget_allocation", {})
    initial_fraction = float(allocation.get("initial_search_fraction", 0.60))
    repair_fraction = float(allocation.get("repair_reserve_fraction", 0.40))
    if initial_fraction < 0 or repair_fraction < 0 or initial_fraction + repair_fraction > 1.0 + 1e-12:
        raise ConfigError("Simulation budget fractions must be non-negative and sum to at most 1.0")
    if allocation.get("allow_automatic_borrow") is not False:
        raise ConfigError("Phase 2.1 requires allow_automatic_borrow=false")
    initial = math.floor(total * initial_fraction)
    repair = math.floor(total * repair_fraction)
    if initial + repair > total:
        repair = total - initial
    return {
        "max_new_simulation_posts": total,
        "initial_search_budget": initial,
        "repair_reserve_budget": repair,
        "unallocated_budget": total - initial - repair,
    }


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Execution hash schema compatibility
#
# execution_material has evolved over time.  run_0002 (PRODUCTION_RESEARCH)
# was created under schema V1 (18 fields); the current runner uses schema V2
# (21 fields), which added three fields that are only meaningful for
# LIVE_VALIDATION / Phase-10B runs: validation_phase, source_run_id and
# phase10a_run_id.  For a PRODUCTION_RESEARCH run those three are always None,
# so a stored V1 hash remains semantically equivalent to the current V2 hash.
# ---------------------------------------------------------------------------

EXECUTION_SCHEMA_V1 = "V1"
EXECUTION_SCHEMA_V2 = "V2"
EXTENSION_EXECUTION_IDENTITY_SCHEMA = "CANDIDATE_EXTENSION_V1"
#: Fields added in V2 relative to V1.  They carry no execution semantics for
#: PRODUCTION_RESEARCH runs (always None) but do change the hash.
EXECUTION_SCHEMA_V2_ADDED_FIELDS = ("validation_phase", "source_run_id", "phase10a_run_id")
#: Hash statuses that permit continuing an existing run.
COMPATIBLE_EXECUTION_HASH_STATUSES = (
    "EXACT_MATCH",
    "LEGACY_SCHEMA_MATCH",
    "THEME_POLICY_RELAXATION_MATCH",
    "CONTINUOUS_SIMULATION_SEMANTICS_MATCH",
)
CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA = "V31_SIMULATION_SEMANTICS_V1"
CONTINUOUS_RESEARCH_POLICY_SCHEMA = "V31_RESEARCH_POLICY_V1"


def build_execution_material(
    plan: Mapping[str, Any], rules: Mapping[str, Any], schema: str = EXECUTION_SCHEMA_V2,
    *, extension_identity: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the canonical execution material for a given schema version.

    Schema V1 omits the three V2-added fields; schema V2 includes them.  Every
    other execution field is identical between V1 and V2, so field ordering is
    irrelevant to the hash (canonical JSON sorts keys).
    """
    budgets = plan["budgets"]
    continuous_research = str(plan.get("run_profile") or "").upper() == "CONTINUOUS_RESEARCH"
    global_research_budget_material = {} if continuous_research else {
        key: value
        for key, value in budgets.items()
        if key.startswith("max_new_simulation")
        or key.startswith("max_candidates")
        or key.startswith("max_repair")
        or key == "simulation_budget_allocation"
    }
    material: Dict[str, Any] = {
        "runner_goal": plan["identity"]["runner_goal"],
        "strategy": plan["strategy"],
        "simulation_settings": plan["simulation_settings"],
        "theme": plan["theme"],
        "selection": plan.get("selection", {}),
        # V3.0.x budgets are part of legacy execution identity. V3.1
        # Continuous treats global research budgets as policy/statistics, not
        # Simulation semantics, so changing them must not invalidate durable
        # remote identity or make a long-lived run incompatible.
        "new_simulation_and_repair_budgets": global_research_budget_material,
        "operator_count_policy": rules.get("operator_count_policy", {}),
        "base_presets": rules.get("power_pool_base_presets", {}),
        "theme_rules": rules["current_theme"],
        "discovery": rules.get("discovery", {}),
        "semantic_classification": rules.get("semantic_classification", {}),
        "vector_reducers": rules.get("vector_reducers", {}),
        "candidate_routes": rules.get("candidate_routes", {}),
        "initial_selection": rules.get("initial_selection", {}),
        "heuristics": rules.get("heuristics", {}),
        "repair_budget": rules.get("repair_budget", {}),
        "repair_cycle_control": rules.get("repair_cycle_control", {}),
        "side_effect_tolerance": rules.get("side_effect_tolerance", {}),
    }
    if schema == EXECUTION_SCHEMA_V2:
        material["validation_phase"] = plan.get("validation_phase")
        material["source_run_id"] = plan.get("source_run_id")
        material["phase10a_run_id"] = plan.get("phase10a_run_id")
    # Historical rounds do not carry this field.  Keeping it opt-in preserves
    # their exact execution material and resume compatibility.
    if extension_identity is not None:
        material["candidate_extension_identity"] = dict(extension_identity)
    return material


def build_simulation_semantics_material(
    plan: Mapping[str, Any], rules: Mapping[str, Any], *,
    extension_identity: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Return only material that changes remote Simulation semantics.

    V3.1 Continuous deliberately separates this identity from Search, Repair,
    Qualification, discovery/ranking and reporting policy.  A policy edit may
    change what work the research engine chooses, but it must not make an
    already-submitted {expression, settings} Simulation look incompatible.

    ``candidate_extension_identity`` remains optional because a frozen
    extension evidence context is a provenance/semantic guard for candidate
    generation.  It is intentionally not inferred from mutable Search policy.
    """
    material: Dict[str, Any] = {
        "schema": CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA,
        "runner_goal": (plan.get("identity") or {}).get("runner_goal"),
        "simulation_settings": copy.deepcopy(dict(plan.get("simulation_settings") or {})),
    }
    if extension_identity is not None:
        material["candidate_extension_identity"] = dict(extension_identity)
    return material


def build_research_policy_material(plan: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    """Return research-allocation/generation policy separate from Simulation semantics.

    This hash is an attribution/replay identity, not a no-repost guard.  It may
    change at a safe policy checkpoint without invalidating durable remote work.
    """
    return {
        "schema": CONTINUOUS_RESEARCH_POLICY_SCHEMA,
        "runner_goal": (plan.get("identity") or {}).get("runner_goal"),
        "strategy": copy.deepcopy(dict(plan.get("strategy") or {})),
        "theme": copy.deepcopy(dict(plan.get("theme") or {})),
        "selection": copy.deepcopy(dict(plan.get("selection") or {})),
        "operator_count_policy": copy.deepcopy(dict(rules.get("operator_count_policy") or {})),
        "base_presets": copy.deepcopy(dict(rules.get("power_pool_base_presets") or {})),
        "theme_rules": copy.deepcopy(dict(rules.get("current_theme") or {})),
        "discovery": copy.deepcopy(dict(rules.get("discovery") or {})),
        "semantic_classification": copy.deepcopy(dict(rules.get("semantic_classification") or {})),
        "vector_reducers": copy.deepcopy(dict(rules.get("vector_reducers") or {})),
        "candidate_routes": copy.deepcopy(dict(rules.get("candidate_routes") or {})),
        "initial_selection": copy.deepcopy(dict(rules.get("initial_selection") or {})),
        "heuristics": copy.deepcopy(dict(rules.get("heuristics") or {})),
        "repair_budget": copy.deepcopy(dict(rules.get("repair_budget") or {})),
        "repair_cycle_control": copy.deepcopy(dict(rules.get("repair_cycle_control") or {})),
        "side_effect_tolerance": copy.deepcopy(dict(rules.get("side_effect_tolerance") or {})),
    }


def simulation_semantics_hash(
    plan: Mapping[str, Any], rules: Mapping[str, Any], *,
    extension_identity: Optional[Mapping[str, Any]] = None,
) -> str:
    identity = None
    if extension_identity is not None:
        identity = extension_execution_identity(extension_identity)
    return _hash(build_simulation_semantics_material(plan, rules, extension_identity=identity))


def research_policy_hash(plan: Mapping[str, Any], rules: Mapping[str, Any]) -> str:
    return _hash(build_research_policy_material(plan, rules))


def extension_execution_identity(context: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the bounded semantic material for an extension-enabled round.

    The frozen evidence itself remains in the round manifest for audit/replay;
    only its canonical digest participates in the execution identity.
    """
    required = (
        "extension_policy_version",
        "normalized_source_semantic_identity_digest",
        "canonical_evidence_digest",
    )
    missing = [key for key in required if not str(context.get(key) or "")]
    if missing:
        raise ConfigError("EXTENSION_EXECUTION_IDENTITY_INCOMPLETE:" + ",".join(missing))
    return {
        "schema": EXTENSION_EXECUTION_IDENTITY_SCHEMA,
        "extension_policy_version": str(context["extension_policy_version"]),
        "normalized_source_semantic_identity_digest": str(context["normalized_source_semantic_identity_digest"]),
        "canonical_evidence_digest": str(context["canonical_evidence_digest"]),
    }


def config_with_extension_execution_identity(
    config: "EffectiveConfig", context: Mapping[str, Any],
) -> "EffectiveConfig":
    identity = extension_execution_identity(context)
    execution_hash = _hash(build_execution_material(
        config.plan, config.rules, EXECUTION_SCHEMA_V2, extension_identity=identity,
    ))
    semantic_hash = simulation_semantics_hash(
        config.plan, config.rules, extension_identity=context,
    )
    return replace(config, execution_hash=execution_hash, simulation_semantics_hash=semantic_hash)


def _execution_field_drift(
    stored_plan: Optional[Mapping[str, Any]], stored_rules: Optional[Mapping[str, Any]],
    current_plan: Mapping[str, Any], current_rules: Mapping[str, Any],
):
    """Return changed execution-field paths, [] for no drift, or None if no
    stored snapshot is available to compare against."""
    if stored_plan is None or stored_rules is None:
        return None
    stored_material = build_execution_material(stored_plan, stored_rules, EXECUTION_SCHEMA_V2)
    current_material = build_execution_material(current_plan, current_rules, EXECUTION_SCHEMA_V2)
    return _diff_paths(stored_material, current_material)


def _diff_paths(a: Mapping[str, Any], b: Mapping[str, Any], prefix: str = ""):
    out = []
    for key in sorted(set(a) | set(b)):
        path = f"{prefix}.{key}" if prefix else key
        if key not in a or key not in b:
            out.append(path)
            continue
        va, vb = a[key], b[key]
        if isinstance(va, dict) and isinstance(vb, dict):
            out.extend(_diff_paths(va, vb, path))
        elif va != vb:
            out.append(path)
    return out


def validate_execution_hash_compatibility(
    config: "EffectiveConfig",
    stored_hash: Any,
    stored_plan: Optional[Mapping[str, Any]] = None,
    stored_rules: Optional[Mapping[str, Any]] = None,
    *, extension_identity: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Classify a stored execution hash against the current schema.

    Returns a dict with status in {EXACT_MATCH, LEGACY_SCHEMA_MATCH,
    EXECUTION_DRIFT, UNRESOLVED}.  Only EXACT_MATCH and LEGACY_SCHEMA_MATCH are
    safe to continue; anything else must keep hard-blocking.
    """
    current_hash = (
        _hash(build_execution_material(
            config.plan, config.rules, EXECUTION_SCHEMA_V2,
            extension_identity=extension_execution_identity(extension_identity),
        ))
        if extension_identity is not None else config.execution_hash
    )
    result: Dict[str, Any] = {
        "status": "UNRESOLVED",
        "stored_hash": stored_hash,
        "current_hash": current_hash,
        "matched_schema_version": None,
        "reason": "",
        "execution_semantics_compatible": False,
    }

    if stored_hash == current_hash:
        result.update(
            status="EXTENSION_CONTEXT_MATCH" if extension_identity is not None else "EXACT_MATCH",
            matched_schema_version=EXTENSION_EXECUTION_IDENTITY_SCHEMA if extension_identity is not None else EXECUTION_SCHEMA_V2,
            reason="STORED_HASH_EQUALS_CURRENT_SCHEMA", execution_semantics_compatible=True,
        )
        return result

    # V3.1 Continuous compatibility is based on remote Simulation semantics,
    # not mutable research-allocation policy. The historical broad
    # ``execution_hash`` is retained for audit/artifact compatibility, while
    # Search/Repair/Qualification/discovery drift is classified separately.
    current_continuous = str(config.plan.get("run_profile") or "").upper() == "CONTINUOUS_RESEARCH"
    stored_continuous = bool(
        stored_plan is not None
        and str(stored_plan.get("run_profile") or "").upper() == "CONTINUOUS_RESEARCH"
    )
    if current_continuous and stored_continuous and stored_plan is not None and stored_rules is not None:
        try:
            current_semantic_hash = simulation_semantics_hash(
                config.plan, config.rules, extension_identity=extension_identity,
            )
            stored_semantic_hash = simulation_semantics_hash(
                stored_plan, stored_rules, extension_identity=extension_identity,
            )
        except Exception as exc:
            result.update(
                status="UNRESOLVED",
                reason=f"CONTINUOUS_SIMULATION_SEMANTICS_UNRESOLVED:{type(exc).__name__}:{exc}",
            )
            return result
        result["current_simulation_semantics_hash"] = current_semantic_hash
        result["stored_simulation_semantics_hash"] = stored_semantic_hash
        result["current_research_policy_hash"] = research_policy_hash(config.plan, config.rules)
        result["stored_research_policy_hash"] = research_policy_hash(stored_plan, stored_rules)
        if current_semantic_hash == stored_semantic_hash:
            result.update(
                status="CONTINUOUS_SIMULATION_SEMANTICS_MATCH",
                matched_schema_version=CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA,
                reason="RESEARCH_POLICY_DRIFT_DOES_NOT_CHANGE_REMOTE_SIMULATION_SEMANTICS",
                execution_semantics_compatible=True,
            )
            return result
        semantic_drift = _diff_paths(
            build_simulation_semantics_material(stored_plan, stored_rules),
            build_simulation_semantics_material(config.plan, config.rules),
        )
        result.update(
            status="EXECUTION_DRIFT",
            matched_schema_version=CONTINUOUS_SIMULATION_SEMANTICS_SCHEMA,
            reason="SIMULATION_SEMANTICS_CHANGED: " + ", ".join(semantic_drift),
            execution_semantics_compatible=False,
        )
        return result

    # Extension-enabled legacy runs are intentionally exact-match only. Their
    # extension policy and evidence digests are part of the semantic identity;
    # historical V1/V2 relaxation rules must never bypass that protection.
    if extension_identity is not None:
        result.update(status="EXECUTION_DRIFT", reason="EXTENSION_EXECUTION_IDENTITY_MISMATCH")
        return result

    legacy_hash = _hash(build_execution_material(config.plan, config.rules, EXECUTION_SCHEMA_V1))
    new_fields_all_none = all(
        config.plan.get(key) is None for key in EXECUTION_SCHEMA_V2_ADDED_FIELDS
    )
    if stored_hash == legacy_hash:
        if new_fields_all_none:
            result.update(
                status="LEGACY_SCHEMA_MATCH", matched_schema_version=EXECUTION_SCHEMA_V1,
                reason="HASH_SCHEMA_EVOLUTION_NO_EXECUTION_DRIFT", execution_semantics_compatible=True,
            )
        else:
            result.update(
                status="UNRESOLVED",
                reason="LEGACY_HASH_MATCHES_BUT_NEW_FIELDS_HAVE_SEMANTIC_VALUE",
            )
        return result

    drift = _execution_field_drift(stored_plan, stored_rules, config.plan, config.rules)

    # Official 2026-08-19 GLB Liquid theme simplification compatibility.
    # The active Power Pool theme removed the High-Turnover constraints while
    # keeping this round's GLB/D1/TOPDIV3000 execution settings and dataset
    # exclusion unchanged.  This is an audited relaxation of theme filtering,
    # not a change to Simulation settings, budget, candidate expression, or
    # POST semantics, so the existing round may safely continue.
    if drift and stored_plan is not None and stored_rules is not None:
        allowed = {
            "theme_rules.live_theme_checks.required",
        }
        if set(drift).issubset(allowed):
            old_theme = dict(stored_rules.get("current_theme") or {})
            new_theme = dict(config.rules.get("current_theme") or {})
            old_req = dict(old_theme.get("required_settings") or {})
            new_req = dict(new_theme.get("required_settings") or {})
            old_excl = list(old_theme.get("excluded_datasets") or [])
            new_excl = list(new_theme.get("excluded_datasets") or [])
            old_ht = (((old_theme.get("local_preconditions") or {}).get("high_turnover") or {}).get("turnover") or {}).get("preset_min")
            new_ht = (((new_theme.get("local_preconditions") or {}).get("high_turnover") or {}).get("turnover") or {}).get("preset_min")
            old_live = set(((old_theme.get("live_theme_checks") or {}).get("required") or []))
            new_live = set(((new_theme.get("live_theme_checks") or {}).get("required") or []))
            expected_settings = {"region": "GLB", "delay": 1, "universe": "TOPDIV3000"}
            if (old_req == new_req == expected_settings
                    and old_excl == new_excl
                    and float(old_ht) == float(new_ht) == 0.20
                    and "THEME_MATCH" in new_live
                    and new_live.issubset(old_live)):
                stored_v1 = _hash(build_execution_material(stored_plan, stored_rules, EXECUTION_SCHEMA_V1))
                stored_v2 = _hash(build_execution_material(stored_plan, stored_rules, EXECUTION_SCHEMA_V2))
                matched = EXECUTION_SCHEMA_V1 if stored_hash == stored_v1 else EXECUTION_SCHEMA_V2 if stored_hash == stored_v2 else None
                result.update(
                    status="THEME_POLICY_RELAXATION_MATCH",
                    matched_schema_version=matched,
                    reason="OFFICIAL_GLB_LIQUID_THEME_RELAXATION_NO_POST_SEMANTICS_CHANGE",
                    execution_semantics_compatible=True,
                )
                return result

    if drift:
        result.update(status="EXECUTION_DRIFT", reason="EXECUTION_FIELDS_CHANGED: " + ", ".join(drift))
    elif drift == []:
        result.update(status="UNRESOLVED", reason="STORED_HASH_MATCHES_NO_KNOWN_SCHEMA")
    else:
        result.update(status="UNRESOLVED", reason="STORED_SNAPSHOT_UNAVAILABLE_CANNOT_CLASSIFY")
    return result


def execution_hash_status_for_run(
    config: "EffectiveConfig", run: Optional[Mapping[str, Any]],
    *, extension_identity: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Convenience wrapper: derive stored hash + snapshot from a run row."""
    if not run:
        return {
            "status": "UNRESOLVED", "stored_hash": None, "current_hash": config.execution_hash,
            "matched_schema_version": None, "reason": "RUN_NOT_FOUND",
            "execution_semantics_compatible": False,
        }
    stored_plan = stored_rules = None
    if isinstance(run.get("plan_json"), str):
        try:
            stored_plan = json.loads(run["plan_json"])
        except ValueError:
            stored_plan = None
    if isinstance(run.get("rules_json"), str):
        try:
            stored_rules = json.loads(run["rules_json"])
        except ValueError:
            stored_rules = None
    return validate_execution_hash_compatibility(
        config, run.get("execution_hash"), stored_plan, stored_rules,
        extension_identity=extension_identity,
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise ConfigError(f"YAML root must be a mapping: {path}")
    return loaded


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"Missing {where}.{key}")
    return mapping[key]


def _target_validator(project_dir: Path):
    module = importlib.import_module("machine_lib_V2_1")
    actual = Path(module.__file__).resolve()
    expected = (project_dir / "machine_lib_V2_1.py").resolve()
    if actual != expected:
        raise ConfigError(f"Wrong machine_lib_V2_1 loaded: {actual}; expected {expected}")
    return module.normalize_target_mode


@dataclass(frozen=True)
class EffectiveConfig:
    project_dir: Path
    rules: Dict[str, Any]
    plan: Dict[str, Any]
    target_mode: str
    atom_constraint_active: bool
    execution_hash: str
    simulation_semantics_hash: str
    research_policy_hash: str
    operational_hash: str
    presentation_hash: str
    adjustments: tuple
    # Process-local operational override.  It is intentionally excluded from
    # every durable snapshot and semantic hash.
    machine_hash_policy_override: Optional[str] = None

    def run_snapshot(self) -> Dict[str, Any]:
        return {
            "runner_goal": self.plan["identity"]["runner_goal"],
            "target_mode": self.target_mode,
            "atom_constraint_active": self.atom_constraint_active,
            "theme_preset_id": self.plan["theme"]["preset_id"],
            "run_profile": self.plan["run_profile"],
            "execution_hash": self.execution_hash,
            "simulation_semantics_hash": self.simulation_semantics_hash,
            "research_policy_hash": self.research_policy_hash,
            "operational_hash": self.operational_hash,
            "presentation_hash": self.presentation_hash,
            "adjustments": list(self.adjustments),
        }


def load_effective_config(
    rules_path: Path, plan_path: Path, *, project_dir: Path = None
) -> EffectiveConfig:
    project = Path(project_dir or Path(plan_path).resolve().parent).resolve()
    rules = load_yaml(Path(rules_path))
    plan = copy.deepcopy(load_yaml(Path(plan_path)))

    if rules.get("schema_version") != 1 or plan.get("schema_version") != 1:
        raise ConfigError("Only schema_version=1 is supported")
    rules_goal = _require(_require(rules, "identity", "rules"), "runner_goal", "rules.identity")
    plan_goal = _require(_require(plan, "identity", "plan"), "runner_goal", "plan.identity")
    if rules_goal != "PPL" or plan_goal != "PPL":
        raise ConfigError("identity.runner_goal must be PPL")

    validator = _target_validator(project)
    target_mode = validator(_require(_require(plan, "strategy", "plan"), "target_mode", "plan.strategy"))

    current_theme = _require(rules, "current_theme", "rules")
    plan_theme = _require(_require(plan, "theme", "plan"), "preset_id", "plan.theme")
    if plan_theme != current_theme.get("preset_id"):
        raise ConfigError(
            f"Unknown theme preset {plan_theme!r}; configured preset is {current_theme.get('preset_id')!r}"
        )
    settings = _require(plan, "simulation_settings", "plan")
    mismatches = []
    for key, expected in current_theme.get("required_settings", {}).items():
        actual = settings.get(key)
        equal = str(actual).upper() == str(expected).upper()
        if not equal:
            mismatches.append(f"{key}: expected {expected!r}, got {actual!r}")
    if mismatches:
        raise ConfigError("THEME_SETTINGS_MISMATCH: " + "; ".join(mismatches))

    safety = _require(rules, "safety", "rules")
    prohibited = {
        "auto_submit": safety.get("auto_submit"),
        "auto_power_pool_tag": safety.get("auto_power_pool_tag"),
        "auto_modify_properties": safety.get("auto_modify_properties"),
        "allow_delete_alpha": safety.get("allow_delete_alpha"),
        "allow_delete_database_records": safety.get("allow_delete_database_records"),
    }
    enabled = [name for name, value in prohibited.items() if value is not False]
    if enabled:
        raise ConfigError("Unsafe settings must remain false: " + ", ".join(enabled))

    adjustments = []
    budgets = _require(plan, "budgets", "plan")
    max_posts = int(_require(budgets, "max_new_simulation_posts", "plan.budgets"))
    if max_posts < 0:
        raise ConfigError("max_new_simulation_posts cannot be negative")
    if plan.get("run_profile") == "LIVE_VALIDATION" and max_posts > 10:
        budgets["max_new_simulation_posts"] = 10
        adjustments.append("LIVE_VALIDATION max_new_simulation_posts clamped to 10")
    simulation_budget_allocation(plan)

    runtime = plan.get("runtime", {})
    if not isinstance(runtime, dict):
        raise ConfigError("plan.runtime must be a mapping")
    if "machine_hash_policy" in runtime:
        policy = str(runtime["machine_hash_policy"] or "").strip().upper()
        if policy not in {"STRICT", "WARN", "OFF"}:
            raise ConfigError("MACHINE_HASH_POLICY_INVALID")
        runtime["machine_hash_policy"] = policy

    execution_material = build_execution_material(plan, rules, EXECUTION_SCHEMA_V2)
    operational_material = {
        "runtime": plan.get("runtime", {}),
        "execution_switches": plan.get("execution", {}),
        "check_budgets": {
            key: value for key, value in budgets.items() if key.startswith("max_check") or key.startswith("max_poll")
        },
        "check": rules.get("check", {}),
    }
    presentation_material = {"summary_limits": rules.get("summary_limits", {})}

    atom_active = target_mode in {"ATOM", "POWER_POOL_ATOM"}
    return EffectiveConfig(
        project_dir=project,
        rules=rules,
        plan=plan,
        target_mode=target_mode,
        atom_constraint_active=atom_active,
        execution_hash=_hash(execution_material),
        simulation_semantics_hash=simulation_semantics_hash(plan, rules),
        research_policy_hash=research_policy_hash(plan, rules),
        operational_hash=_hash(operational_material),
        presentation_hash=_hash(presentation_material),
        adjustments=tuple(adjustments),
    )


def config_with_machine_hash_policy_override(
    config: EffectiveConfig, policy: Optional[str],
) -> EffectiveConfig:
    """Attach a process-local machine-hash policy without changing hashes."""
    if policy is None:
        return config
    normalized = str(policy or "").strip().upper()
    if normalized not in {"STRICT", "WARN", "OFF"}:
        raise ConfigError("MACHINE_HASH_POLICY_INVALID")
    return replace(config, machine_hash_policy_override=normalized)
