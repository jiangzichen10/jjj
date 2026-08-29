"""Engine-side safe-checkpoint policy activation for V3.1 Continuous runs.

C3 introduced durable Qualification reload. C6 extends the same durable,
safe-checkpoint semantics to one atomic Qualification/Search/Repair bundle.
Existing durable payloads always win during restart/recovery; edited YAML is a
candidate policy only and cannot silently replace the policy identity of an
unfinished batch.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .qualification_policy import (
    QualificationPolicySnapshot,
    build_qualification_policy_snapshot,
    install_qualification_policy_runtime_snapshot,
)
from .research_telemetry import record_event
from .policy_specs import build_repair_policy_payload, build_search_policy_payload
from .round_store import (
    activate_policy_bundle_atomic, load_policy_state, upsert_policy_state, update_round,
)

QUALIFICATION_POLICY_TYPE = "QUALIFICATION"
QUALIFICATION_RELOAD_CONTROLLER_VERSION = "V31_QUAL_RELOAD_001"


class QualificationReloadStatus(str, Enum):
    INITIALIZED = "INITIALIZED"
    RESTORED = "RESTORED"
    UNCHANGED = "UNCHANGED"
    RELOADED = "RELOADED"
    DEFERRED_UNSAFE_CHECKPOINT = "DEFERRED_UNSAFE_CHECKPOINT"
    REJECTED_VERSION_BUMP_REQUIRED = "REJECTED_VERSION_BUMP_REQUIRED"
    REJECTED_NON_QUALIFICATION_DRIFT = "REJECTED_NON_QUALIFICATION_DRIFT"
    REJECTED_INVALID_POLICY = "REJECTED_INVALID_POLICY"


@dataclass(frozen=True)
class QualificationReloadResult:
    status: QualificationReloadStatus
    policy: Mapping[str, Any]
    active_version: str
    active_hash: str
    previous_version: str = ""
    previous_hash: str = ""
    reason: str = ""

    @property
    def changed(self) -> bool:
        return self.status is QualificationReloadStatus.RELOADED


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _qualification_payload(policy: Mapping[str, Any], *, source_path: str = "") -> Dict[str, Any]:
    versions = dict(policy.get("policy_versions") or {})
    integration = copy.deepcopy(dict(policy.get("qualification_integration") or {}))
    classification = copy.deepcopy(dict(policy.get("ppl_classification") or {}))
    mapped_version = str(versions.get("qualification") or "")
    snapshot = build_qualification_policy_snapshot(
        classification, integration, source_path=source_path, mapped_version=mapped_version,
    )
    return {
        "controller_version": QUALIFICATION_RELOAD_CONTROLLER_VERSION,
        "policy_version": mapped_version,
        "policy_hash": snapshot.policy_hash,
        "qualification_integration": integration,
        "ppl_classification": classification,
    }


def _snapshot_from_payload(payload: Mapping[str, Any], *, source_path: str) -> QualificationPolicySnapshot:
    version = str(payload.get("policy_version") or "")
    snapshot = build_qualification_policy_snapshot(
        dict(payload.get("ppl_classification") or {}),
        dict(payload.get("qualification_integration") or {}),
        source_path=source_path,
        mapped_version=version,
    )
    expected = str(payload.get("policy_hash") or "")
    if expected and snapshot.policy_hash != expected:
        raise ValueError(f"QUALIFICATION_DURABLE_POLICY_HASH_MISMATCH:{expected}:{snapshot.policy_hash}")
    return snapshot


def _merge_qualification_payload(policy: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(dict(policy))
    merged["qualification_integration"] = copy.deepcopy(dict(payload.get("qualification_integration") or {}))
    merged["ppl_classification"] = copy.deepcopy(dict(payload.get("ppl_classification") or {}))
    versions = dict(merged.get("policy_versions") or {})
    versions["qualification"] = str(payload.get("policy_version") or "")
    merged["policy_versions"] = versions
    return merged


def _without_qualification(policy: Mapping[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(dict(policy))
    out.pop("qualification_integration", None)
    out.pop("ppl_classification", None)
    versions = dict(out.get("policy_versions") or {})
    versions.pop("qualification", None)
    out["policy_versions"] = versions
    return out


def qualification_only_drift(active_policy: Mapping[str, Any], candidate_policy: Mapping[str, Any]) -> bool:
    """True when every non-Qualification policy field is identical."""
    return _json(_without_qualification(active_policy)) == _json(_without_qualification(candidate_policy))


def initialize_or_restore_qualification_policy(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    policy: Mapping[str, Any],
    project_dir: Path,
    source_path: Path,
) -> QualificationReloadResult:
    """Install durable active policy before recovery/new work is processed.

    If no durable state exists (new C3 round or migration from C2), the supplied
    round policy becomes the initial active snapshot.  Otherwise the durable
    payload wins over the current YAML file until a later safe checkpoint.
    """
    source = str(Path(source_path).resolve())
    state = load_policy_state(store, round_id, QUALIFICATION_POLICY_TYPE)
    if state is None:
        payload = _qualification_payload(policy, source_path=source)
        upsert_policy_state(
            store, round_id, run_id,
            policy_type=QUALIFICATION_POLICY_TYPE,
            policy_version=str(payload["policy_version"]),
            policy_hash=str(payload["policy_hash"]),
            payload=payload,
            source_path=source,
            activated_batch_no=0,
        )
        snap = _snapshot_from_payload(payload, source_path=source)
        install_qualification_policy_runtime_snapshot(project_dir, snap, Path(source_path).name)
        record_event(
            store, round_id, run_id, "QUALIFICATION_POLICY_INITIALIZED", phase="SEARCH",
            payload={
                "policy_version": payload["policy_version"], "policy_hash": payload["policy_hash"],
                "controller_version": QUALIFICATION_RELOAD_CONTROLLER_VERSION,
            },
            source_event_key=f"qualification_policy_init:{round_id}:{payload['policy_hash']}",
        )
        return QualificationReloadResult(
            QualificationReloadStatus.INITIALIZED, copy.deepcopy(dict(policy)),
            str(payload["policy_version"]), str(payload["policy_hash"]),
        )

    payload = dict(state.get("payload") or {})
    snap = _snapshot_from_payload(payload, source_path=source)
    install_qualification_policy_runtime_snapshot(project_dir, snap, Path(source_path).name)
    restored = _merge_qualification_payload(policy, payload)
    return QualificationReloadResult(
        QualificationReloadStatus.RESTORED, restored,
        str(payload.get("policy_version") or state.get("policy_version") or ""),
        str(payload.get("policy_hash") or state.get("policy_hash") or ""),
    )



def restore_qualification_runtime_from_durable_state(
    store: Any, *, round_id: str, project_dir: Path, source_path: Path,
) -> Optional[QualificationReloadResult]:
    """Read-only runtime restore used by status/report processes.

    Unlike ``initialize_or_restore_qualification_policy`` this never creates or
    mutates durable state. It simply installs the last durable active payload
    into the process-local evaluator cache.
    """
    state = load_policy_state(store, round_id, QUALIFICATION_POLICY_TYPE)
    if state is None:
        return None
    payload = dict(state.get("payload") or {})
    source = str(Path(source_path).resolve())
    snap = _snapshot_from_payload(payload, source_path=source)
    install_qualification_policy_runtime_snapshot(project_dir, snap, Path(source_path).name)
    return QualificationReloadResult(
        QualificationReloadStatus.RESTORED, {},
        str(payload.get("policy_version") or state.get("policy_version") or ""),
        str(payload.get("policy_hash") or state.get("policy_hash") or ""),
    )

def apply_qualification_policy_safe_checkpoint(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    active_policy: Mapping[str, Any],
    candidate_policy: Mapping[str, Any],
    project_dir: Path,
    source_path: Path,
    batch_no: int,
    phase: str,
    checkpoint_safe: bool,
) -> QualificationReloadResult:
    """Validate and activate a Qualification-only policy change at a checkpoint.

    Rejected candidate files never replace the last durable active policy.  This
    keeps a long-running engine operational under its last known-good rules
    while making configuration errors visible through durable events.
    """
    state = load_policy_state(store, round_id, QUALIFICATION_POLICY_TYPE)
    if state is None:
        return initialize_or_restore_qualification_policy(
            store, round_id=round_id, run_id=run_id, policy=active_policy,
            project_dir=project_dir, source_path=source_path,
        )
    old_payload = dict(state.get("payload") or {})
    old_version = str(old_payload.get("policy_version") or state.get("policy_version") or "")
    old_hash = str(old_payload.get("policy_hash") or state.get("policy_hash") or "")

    if not checkpoint_safe:
        return QualificationReloadResult(
            QualificationReloadStatus.DEFERRED_UNSAFE_CHECKPOINT, copy.deepcopy(dict(active_policy)),
            old_version, old_hash, reason="UNFINISHED_BATCH_PRESENT",
        )

    if not qualification_only_drift(active_policy, candidate_policy):
        candidate_digest = __import__("hashlib").sha256(_json(candidate_policy).encode("utf-8")).hexdigest()
        record_event(
            store, round_id, run_id, "QUALIFICATION_POLICY_RELOAD_REJECTED",
            batch_no=int(batch_no), phase=str(phase),
            payload={
                "reason": "NON_QUALIFICATION_POLICY_DRIFT",
                "active_version": old_version, "active_hash": old_hash,
                "controller_version": QUALIFICATION_RELOAD_CONTROLLER_VERSION,
                "candidate_digest": candidate_digest,
            },
            source_event_key=f"qualification_policy_reject:{round_id}:NON_QUALIFICATION_POLICY_DRIFT:{candidate_digest}",
        )
        return QualificationReloadResult(
            QualificationReloadStatus.REJECTED_NON_QUALIFICATION_DRIFT,
            copy.deepcopy(dict(active_policy)), old_version, old_hash,
            reason="NON_QUALIFICATION_POLICY_DRIFT",
        )

    source = str(Path(source_path).resolve())
    try:
        new_payload = _qualification_payload(candidate_policy, source_path=source)
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        error_digest = __import__("hashlib").sha256(error_text.encode("utf-8")).hexdigest()
        record_event(
            store, round_id, run_id, "QUALIFICATION_POLICY_RELOAD_REJECTED",
            batch_no=int(batch_no), phase=str(phase),
            payload={
                "reason": "INVALID_QUALIFICATION_POLICY",
                "error": error_text,
                "active_version": old_version, "active_hash": old_hash,
            },
            source_event_key=f"qualification_policy_reject:{round_id}:INVALID_QUALIFICATION_POLICY:{error_digest}",
        )
        return QualificationReloadResult(
            QualificationReloadStatus.REJECTED_INVALID_POLICY,
            copy.deepcopy(dict(active_policy)), old_version, old_hash,
            reason=f"{type(exc).__name__}: {exc}",
        )

    new_version = str(new_payload.get("policy_version") or "")
    new_hash = str(new_payload.get("policy_hash") or "")
    if new_hash == old_hash:
        return QualificationReloadResult(
            QualificationReloadStatus.UNCHANGED, copy.deepcopy(dict(active_policy)), old_version, old_hash,
        )
    if new_version == old_version:
        record_event(
            store, round_id, run_id, "QUALIFICATION_POLICY_RELOAD_REJECTED",
            batch_no=int(batch_no), phase=str(phase),
            payload={
                "reason": "POLICY_VERSION_BUMP_REQUIRED",
                "active_version": old_version, "active_hash": old_hash,
                "candidate_hash": new_hash,
            },
            source_event_key=f"qualification_policy_reject:{round_id}:POLICY_VERSION_BUMP_REQUIRED:{new_hash}",
        )
        return QualificationReloadResult(
            QualificationReloadStatus.REJECTED_VERSION_BUMP_REQUIRED,
            copy.deepcopy(dict(active_policy)), old_version, old_hash,
            reason="POLICY_VERSION_BUMP_REQUIRED",
        )

    snap = _snapshot_from_payload(new_payload, source_path=source)
    upsert_policy_state(
        store, round_id, run_id,
        policy_type=QUALIFICATION_POLICY_TYPE,
        policy_version=new_version,
        policy_hash=new_hash,
        payload=new_payload,
        source_path=source,
        activated_batch_no=int(batch_no),
    )
    install_qualification_policy_runtime_snapshot(project_dir, snap, Path(source_path).name)
    merged = _merge_qualification_payload(active_policy, new_payload)
    # The round row records the active policy at the safe checkpoint. The
    # immutable creation manifest remains historical evidence of round start.
    update_round(
        store, round_id,
        config_json=_json(merged),
        config_hash=__import__("hashlib").sha256(_json(merged).encode("utf-8")).hexdigest(),
    )
    record_event(
        store, round_id, run_id, "QUALIFICATION_POLICY_RELOADED",
        batch_no=int(batch_no), phase=str(phase),
        payload={
            "from_version": old_version, "to_version": new_version,
            "from_hash": old_hash, "to_hash": new_hash,
            "controller_version": QUALIFICATION_RELOAD_CONTROLLER_VERSION,
            "checkpoint": "NO_UNFINISHED_BATCH_BEFORE_NEW_ALLOCATION",
            "simulation_identity_unchanged": True,
        },
        source_event_key=f"qualification_policy_reload:{round_id}:{new_hash}",
    )
    return QualificationReloadResult(
        QualificationReloadStatus.RELOADED, merged, new_version, new_hash,
        previous_version=old_version, previous_hash=old_hash,
    )

# ---------------------------------------------------------------------------
# C6: atomic Qualification/Search/Repair policy bundle runtime
# ---------------------------------------------------------------------------

SEARCH_POLICY_TYPE = "SEARCH"
REPAIR_POLICY_TYPE = "REPAIR"
HOT_POLICY_TYPES = (QUALIFICATION_POLICY_TYPE, SEARCH_POLICY_TYPE, REPAIR_POLICY_TYPE)
POLICY_BUNDLE_CONTROLLER_VERSION = "V31_POLICY_BUNDLE_001"


class PolicyBundleReloadStatus(str, Enum):
    INITIALIZED = "INITIALIZED"
    RESTORED = "RESTORED"
    UNCHANGED = "UNCHANGED"
    RELOADED = "RELOADED"
    DEFERRED_UNSAFE_CHECKPOINT = "DEFERRED_UNSAFE_CHECKPOINT"
    REJECTED_VERSION_BUMP_REQUIRED = "REJECTED_VERSION_BUMP_REQUIRED"
    REJECTED_NON_HOT_POLICY_DRIFT = "REJECTED_NON_HOT_POLICY_DRIFT"
    REJECTED_INVALID_POLICY = "REJECTED_INVALID_POLICY"


@dataclass(frozen=True)
class PolicyBundleReloadResult:
    status: PolicyBundleReloadStatus
    policy: Mapping[str, Any]
    active_versions: Mapping[str, str]
    active_hashes: Mapping[str, str]
    changed_types: Tuple[str, ...] = ()
    reason: str = ""

    @property
    def changed(self) -> bool:
        return self.status is PolicyBundleReloadStatus.RELOADED


def _search_payload(policy: Mapping[str, Any]) -> Dict[str, Any]:
    payload = build_search_policy_payload(policy)
    if not str(payload.get("policy_version") or ""):
        raise ValueError("SEARCH_POLICY_VERSION_REQUIRED")
    return payload


def _repair_payload(policy: Mapping[str, Any]) -> Dict[str, Any]:
    payload = build_repair_policy_payload(policy)
    if not str(payload.get("policy_version") or ""):
        raise ValueError("REPAIR_POLICY_VERSION_REQUIRED")
    return payload


def _policy_payload_for_type(policy: Mapping[str, Any], *, policy_type: str, source_path: str) -> Dict[str, Any]:
    if policy_type == QUALIFICATION_POLICY_TYPE:
        q = _qualification_payload(policy, source_path=source_path)
        if not str(q.get("policy_version") or ""):
            raise ValueError("QUALIFICATION_POLICY_VERSION_REQUIRED")
        return q
    if policy_type == SEARCH_POLICY_TYPE:
        return _search_payload(policy)
    if policy_type == REPAIR_POLICY_TYPE:
        return _repair_payload(policy)
    raise ValueError(f"UNSUPPORTED_HOT_POLICY_TYPE:{policy_type}")


def _bundle_payloads(policy: Mapping[str, Any], *, source_path: str) -> Dict[str, Dict[str, Any]]:
    return {
        ptype: _policy_payload_for_type(policy, policy_type=ptype, source_path=source_path)
        for ptype in HOT_POLICY_TYPES
    }


def _merge_search_payload(policy: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(dict(policy))
    out["search_policy"] = copy.deepcopy(dict(payload.get("search_policy") or {}))
    versions = dict(out.get("policy_versions") or {})
    versions["search"] = str(payload.get("policy_version") or "")
    out["policy_versions"] = versions
    return out


def _merge_repair_payload(policy: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(dict(policy))
    out["repair_policy"] = copy.deepcopy(dict(payload.get("repair_policy") or {}))
    versions = dict(out.get("policy_versions") or {})
    versions["repair"] = str(payload.get("policy_version") or "")
    out["policy_versions"] = versions
    return out


def _merge_policy_bundle(policy: Mapping[str, Any], payloads: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    out = _merge_qualification_payload(policy, payloads[QUALIFICATION_POLICY_TYPE])
    out = _merge_search_payload(out, payloads[SEARCH_POLICY_TYPE])
    out = _merge_repair_payload(out, payloads[REPAIR_POLICY_TYPE])
    return out


def _without_hot_policy_bundle(policy: Mapping[str, Any]) -> Dict[str, Any]:
    """Strip only fields C6 is authorized to hot-reload."""
    out = copy.deepcopy(dict(policy))
    for key in ("qualification_integration", "ppl_classification", "search_policy", "repair_policy"):
        out.pop(key, None)
    versions = dict(out.get("policy_versions") or {})
    for key in ("qualification", "search", "repair"):
        versions.pop(key, None)
    out["policy_versions"] = versions
    return out


def hot_policy_bundle_only_drift(active_policy: Mapping[str, Any], candidate_policy: Mapping[str, Any]) -> bool:
    """True iff all policy-file drift is confined to Q/Search/Repair hot scopes."""
    return _json(_without_hot_policy_bundle(active_policy)) == _json(_without_hot_policy_bundle(candidate_policy))


def _state_payload(state: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    return dict((state or {}).get("payload") or {})


def _state_versions_hashes(states: Mapping[str, Mapping[str, Any]]) -> Tuple[Dict[str, str], Dict[str, str]]:
    versions: Dict[str, str] = {}
    hashes: Dict[str, str] = {}
    for ptype in HOT_POLICY_TYPES:
        state = dict(states.get(ptype) or {})
        payload = _state_payload(state)
        versions[ptype] = str(payload.get("policy_version") or state.get("policy_version") or "")
        hashes[ptype] = str(payload.get("policy_hash") or state.get("policy_hash") or "")
    return versions, hashes


def _bundle_state_rows(
    *, states: Mapping[str, Mapping[str, Any]], payloads: Mapping[str, Mapping[str, Any]],
    source_path: str, batch_no: int, changed_types: Sequence[str],
) -> list[Dict[str, Any]]:
    changed = set(str(x) for x in changed_types)
    rows = []
    for ptype in HOT_POLICY_TYPES:
        payload = dict(payloads[ptype])
        old = dict(states.get(ptype) or {})
        is_changed = ptype in changed or not old
        rows.append({
            "policy_type": ptype,
            "policy_version": str(payload.get("policy_version") or ""),
            "policy_hash": str(payload.get("policy_hash") or ""),
            "payload": payload,
            "source_path": source_path,
            "activated_batch_no": int(batch_no if is_changed else old.get("activated_batch_no") or 0),
            "activated_at": None if is_changed else old.get("activated_at"),
        })
    return rows


def initialize_or_restore_policy_bundle(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    policy: Mapping[str, Any],
    project_dir: Path,
    source_path: Path,
) -> PolicyBundleReloadResult:
    """Restore the durable Q/S/R bundle before recovery, initializing missing rows atomically.

    This is also the migration path from C3, where only QUALIFICATION had a
    durable policy-state row. Existing durable payloads win over the current
    YAML file; missing Search/Repair rows are initialized from the supplied
    parity policy without rewriting Simulation identity.
    """
    source = str(Path(source_path).resolve())
    states = {ptype: load_policy_state(store, round_id, ptype) for ptype in HOT_POLICY_TYPES}
    payloads: Dict[str, Dict[str, Any]] = {}
    missing = []
    for ptype in HOT_POLICY_TYPES:
        state = states.get(ptype)
        if state is None:
            missing.append(ptype)
            # Parse/validate the current file only for policy types that have no
            # durable state yet (for example C3 -> C6 migration).  When every
            # type is already durable, even a currently broken/half-edited YAML
            # must not prevent restart from restoring last-known-good policy.
            payloads[ptype] = _policy_payload_for_type(
                policy, policy_type=ptype, source_path=source,
            )
        else:
            payload = _state_payload(state)
            if not payload:
                raise ValueError(f"POLICY_DURABLE_PAYLOAD_MISSING:{ptype}")
            payloads[ptype] = payload

    # Validate durable Qualification content before it can become process-local.
    q_snap = _snapshot_from_payload(payloads[QUALIFICATION_POLICY_TYPE], source_path=source)
    merged = _merge_policy_bundle(policy, payloads)

    if missing:
        rows = _bundle_state_rows(
            states={k: v for k, v in states.items() if v}, payloads=payloads,
            source_path=source, batch_no=0, changed_types=missing,
        )
        digest = __import__("hashlib").sha256(
            _json({k: payloads[k].get("policy_hash") for k in HOT_POLICY_TYPES}).encode("utf-8")
        ).hexdigest()
        activate_policy_bundle_atomic(
            store, round_id, run_id, states=rows, active_config=merged, activated_batch_no=0,
            event={
                "event_key": f"policy_bundle_init:{round_id}:{digest}",
                "event_type": "POLICY_BUNDLE_INITIALIZED", "phase": "SEARCH", "batch_no": 0,
                "payload": {
                    "controller_version": POLICY_BUNDLE_CONTROLLER_VERSION,
                    "initialized_types": list(missing),
                    "policy_versions": {k: payloads[k].get("policy_version") for k in HOT_POLICY_TYPES},
                    "policy_hashes": {k: payloads[k].get("policy_hash") for k in HOT_POLICY_TYPES},
                    "simulation_identity_unchanged": True,
                },
            },
        )
        # Reload states so activation metadata returned below reflects the same transaction.
        states = {ptype: load_policy_state(store, round_id, ptype) or {} for ptype in HOT_POLICY_TYPES}
        status = PolicyBundleReloadStatus.INITIALIZED
    else:
        status = PolicyBundleReloadStatus.RESTORED

    install_qualification_policy_runtime_snapshot(project_dir, q_snap, Path(source_path).name)
    versions, hashes = _state_versions_hashes(states)
    return PolicyBundleReloadResult(status, merged, versions, hashes, tuple(missing))


def restore_policy_bundle_runtime_from_durable_state(
    store: Any, *, round_id: str, project_dir: Path, source_path: Path,
) -> Optional[PolicyBundleReloadResult]:
    """Read-only restore for status/report processes; never creates policy rows."""
    states = {ptype: load_policy_state(store, round_id, ptype) for ptype in HOT_POLICY_TYPES}
    if not any(states.values()):
        return None
    q_state = states.get(QUALIFICATION_POLICY_TYPE)
    if q_state:
        source = str(Path(source_path).resolve())
        q_snap = _snapshot_from_payload(_state_payload(q_state), source_path=source)
        install_qualification_policy_runtime_snapshot(project_dir, q_snap, Path(source_path).name)
    versions, hashes = _state_versions_hashes({k: v or {} for k, v in states.items()})
    return PolicyBundleReloadResult(PolicyBundleReloadStatus.RESTORED, {}, versions, hashes)


def _record_bundle_rejection(
    store: Any, *, round_id: str, run_id: str, batch_no: int, phase: str,
    reason: str, active_versions: Mapping[str, str], active_hashes: Mapping[str, str],
    detail: Optional[Mapping[str, Any]] = None,
) -> None:
    detail = dict(detail or {})
    digest = __import__("hashlib").sha256(_json({"reason": reason, **detail}).encode("utf-8")).hexdigest()
    record_event(
        store, round_id, run_id, "POLICY_BUNDLE_RELOAD_REJECTED",
        batch_no=int(batch_no), phase=str(phase),
        payload={
            "reason": reason,
            "active_versions": dict(active_versions),
            "active_hashes": dict(active_hashes),
            "controller_version": POLICY_BUNDLE_CONTROLLER_VERSION,
            "research_continues_with_last_durable_policy": True,
            **detail,
        },
        source_event_key=f"policy_bundle_reject:{round_id}:{reason}:{digest}",
    )


def apply_policy_bundle_safe_checkpoint(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    active_policy: Mapping[str, Any],
    candidate_policy: Mapping[str, Any],
    project_dir: Path,
    source_path: Path,
    batch_no: int,
    phase: str,
    checkpoint_safe: bool,
) -> PolicyBundleReloadResult:
    """Atomically validate/activate Qualification + Search + Repair at a safe checkpoint."""
    source = str(Path(source_path).resolve())
    states = {ptype: load_policy_state(store, round_id, ptype) for ptype in HOT_POLICY_TYPES}
    if any(v is None for v in states.values()):
        return initialize_or_restore_policy_bundle(
            store, round_id=round_id, run_id=run_id, policy=active_policy,
            project_dir=project_dir, source_path=source_path,
        )
    concrete_states = {k: dict(v or {}) for k, v in states.items()}
    old_versions, old_hashes = _state_versions_hashes(concrete_states)

    if not checkpoint_safe:
        return PolicyBundleReloadResult(
            PolicyBundleReloadStatus.DEFERRED_UNSAFE_CHECKPOINT, copy.deepcopy(dict(active_policy)),
            old_versions, old_hashes, reason="UNFINISHED_BATCH_PRESENT",
        )

    if not hot_policy_bundle_only_drift(active_policy, candidate_policy):
        candidate_digest = __import__("hashlib").sha256(_json(candidate_policy).encode("utf-8")).hexdigest()
        _record_bundle_rejection(
            store, round_id=round_id, run_id=run_id, batch_no=batch_no, phase=phase,
            reason="NON_HOT_POLICY_DRIFT", active_versions=old_versions, active_hashes=old_hashes,
            detail={"candidate_digest": candidate_digest},
        )
        return PolicyBundleReloadResult(
            PolicyBundleReloadStatus.REJECTED_NON_HOT_POLICY_DRIFT,
            copy.deepcopy(dict(active_policy)), old_versions, old_hashes,
            reason="NON_HOT_POLICY_DRIFT",
        )

    try:
        new_payloads = _bundle_payloads(candidate_policy, source_path=source)
        # Build now so invalid Qualification cannot be written before runtime install.
        new_q_snap = _snapshot_from_payload(new_payloads[QUALIFICATION_POLICY_TYPE], source_path=source)
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        _record_bundle_rejection(
            store, round_id=round_id, run_id=run_id, batch_no=batch_no, phase=phase,
            reason="INVALID_HOT_POLICY", active_versions=old_versions, active_hashes=old_hashes,
            detail={"error": error_text},
        )
        return PolicyBundleReloadResult(
            PolicyBundleReloadStatus.REJECTED_INVALID_POLICY,
            copy.deepcopy(dict(active_policy)), old_versions, old_hashes,
            reason=error_text,
        )

    changed_types = []
    version_bump_missing = []
    for ptype in HOT_POLICY_TYPES:
        old_version = old_versions.get(ptype, "")
        old_hash = old_hashes.get(ptype, "")
        new_version = str(new_payloads[ptype].get("policy_version") or "")
        new_hash = str(new_payloads[ptype].get("policy_hash") or "")
        if old_hash != new_hash or old_version != new_version:
            changed_types.append(ptype)
        if old_hash != new_hash and old_version == new_version:
            version_bump_missing.append(ptype)

    if version_bump_missing:
        _record_bundle_rejection(
            store, round_id=round_id, run_id=run_id, batch_no=batch_no, phase=phase,
            reason="POLICY_VERSION_BUMP_REQUIRED", active_versions=old_versions, active_hashes=old_hashes,
            detail={"policy_types": version_bump_missing,
                    "candidate_hashes": {k: new_payloads[k].get("policy_hash") for k in version_bump_missing}},
        )
        return PolicyBundleReloadResult(
            PolicyBundleReloadStatus.REJECTED_VERSION_BUMP_REQUIRED,
            copy.deepcopy(dict(active_policy)), old_versions, old_hashes,
            tuple(version_bump_missing), reason="POLICY_VERSION_BUMP_REQUIRED",
        )

    if not changed_types:
        return PolicyBundleReloadResult(
            PolicyBundleReloadStatus.UNCHANGED, copy.deepcopy(dict(active_policy)), old_versions, old_hashes,
        )

    merged = _merge_policy_bundle(active_policy, new_payloads)
    rows = _bundle_state_rows(
        states=concrete_states, payloads=new_payloads, source_path=source,
        batch_no=int(batch_no), changed_types=changed_types,
    )
    transitions = {
        ptype: {
            "from_version": old_versions.get(ptype, ""),
            "to_version": str(new_payloads[ptype].get("policy_version") or ""),
            "from_hash": old_hashes.get(ptype, ""),
            "to_hash": str(new_payloads[ptype].get("policy_hash") or ""),
        }
        for ptype in changed_types
    }
    event_digest = __import__("hashlib").sha256(_json(transitions).encode("utf-8")).hexdigest()
    activate_policy_bundle_atomic(
        store, round_id, run_id, states=rows, active_config=merged,
        activated_batch_no=int(batch_no),
        event={
            "event_key": f"policy_bundle_reload:{round_id}:{event_digest}",
            "event_type": "POLICY_BUNDLE_RELOADED", "batch_no": int(batch_no), "phase": str(phase),
            "payload": {
                "controller_version": POLICY_BUNDLE_CONTROLLER_VERSION,
                "changed_types": list(changed_types), "transitions": transitions,
                "checkpoint": "NO_UNFINISHED_BATCH_BEFORE_NEW_ALLOCATION",
                "atomic_activation": True,
                "simulation_identity_unchanged": True,
            },
        },
    )
    install_qualification_policy_runtime_snapshot(project_dir, new_q_snap, Path(source_path).name)
    new_states = {ptype: load_policy_state(store, round_id, ptype) or {} for ptype in HOT_POLICY_TYPES}
    new_versions, new_hashes = _state_versions_hashes(new_states)
    return PolicyBundleReloadResult(
        PolicyBundleReloadStatus.RELOADED, merged, new_versions, new_hashes, tuple(changed_types),
    )
