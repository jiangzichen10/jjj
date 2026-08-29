"""Controlled branching policy for PP_CORRELATION_FAIL.

This module is intentionally local/offline.  It never performs network I/O.
It reconstructs PP-correlation branch state from durable repair plans/edges,
evaluates completed repair children, and exposes a bounded Best-Node policy.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .check_derived_repair import _alpha_metrics, _load_resolved_check_facts
from .ppl_classifier import (
    classify_ppl_candidate, load_ppl_classification_policy,
    load_ppl_classification_policy_for_config,
)
from .diagnosis import evaluate_local_pre_gate

PPC_TARGET_FAILURE = "PP_CORRELATION_FAIL"
PPC_POLICY_NAME = "PPC_CONTROLLED_BRANCH"
PPC_POLICY_VERSION = "v3.0.4m"

PPC_EVALUATED_OUTCOMES = frozenset({
    "TARGET_PASS",
    "IMPROVED",
    "NO_MEANINGFUL_CHANGE",
    "WORSE",
    "REJECT_SIDE_EFFECT",
})
PPC_BRANCH_CLOSING_OUTCOMES = frozenset({
    "NO_MEANINGFUL_CHANGE",
    "WORSE",
    "REJECT_SIDE_EFFECT",
})

DEFAULT_PPC_BRANCH_CONFIG: Dict[str, Any] = {
    "max_attempts": 3,
    "meaningful_improvement_min": 0.01,
    "meaningful_worsening_min": 0.01,
    "require_no_new_fixed_blockers": True,
    "require_not_strategy_rejected": True,
    "max_sharpe_drop_abs": None,
    "max_fitness_drop_abs": None,
    "same_family_windows": [2, 3, 4, 5],
}


def ppc_branch_config(rules: Mapping[str, Any]) -> Dict[str, Any]:
    near = dict((rules or {}).get("near_pass") or {})
    merged = dict(DEFAULT_PPC_BRANCH_CONFIG)
    merged.update(dict(near.get("ppc_controlled_branch") or {}))
    merged["max_attempts"] = max(1, int(merged.get("max_attempts") or 3))
    merged["meaningful_improvement_min"] = max(0.0, float(merged.get("meaningful_improvement_min") or 0.0))
    merged["meaningful_worsening_min"] = max(0.0, float(merged.get("meaningful_worsening_min") or 0.0))
    windows: List[int] = []
    for value in merged.get("same_family_windows") or []:
        try:
            w = int(value)
        except (TypeError, ValueError):
            continue
        if w > 0 and w not in windows:
            windows.append(w)
    merged["same_family_windows"] = windows or [2, 3, 4, 5]
    return merged


def _float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _strip_outer_wrappers(expression: str) -> str:
    text = str(expression or "").strip()
    changed = True
    while changed:
        changed = False
        if text.startswith("-(") and text.endswith(")"):
            inner = text[2:-1].strip()
            if _balanced(inner):
                text = inner
                changed = True
        elif text.startswith("(") and text.endswith(")"):
            inner = text[1:-1].strip()
            if _balanced(inner):
                text = inner
                changed = True
    return text


def _balanced(text: str) -> bool:
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _outer_call(text: str) -> Tuple[Optional[str], List[str]]:
    expr = _strip_outer_wrappers(text)
    pos = expr.find("(")
    if pos <= 0 or not expr.endswith(")"):
        return None, []
    name = expr[:pos].strip().lower()
    body = expr[pos + 1:-1]
    args: List[str] = []
    start = 0
    depth = 0
    for idx, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return None, []
        elif ch == "," and depth == 0:
            args.append(body[start:idx].strip())
            start = idx + 1
    if depth != 0:
        return None, []
    args.append(body[start:].strip())
    return name, args


def resolve_effective_window(candidate: Mapping[str, Any]) -> Optional[int]:
    """Return the effective ts_mean window, recovering legacy NULL metadata.

    A NULL window is never interpreted as zero.  For a ts_mean candidate the
    canonical expression is parsed; if that cannot be done safely, callers must
    fail closed rather than materialize a no-op repair.
    """
    raw = candidate.get("window")
    if raw is not None:
        try:
            value = int(raw)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    if str(candidate.get("operator") or "").lower() != "ts_mean":
        return None
    name, args = _outer_call(str(candidate.get("expression") or ""))
    if name != "ts_mean" or len(args) != 2:
        return None
    try:
        value = int(args[1])
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _ppc_plan_rows(store: Any, run_id: str) -> List[Dict[str, Any]]:
    with store.connect() as conn:
        rows = conn.execute(
            """SELECT p.repair_plan_id,p.parent_candidate_id,p.root_candidate_id,p.target_failure,
                      p.repair_type,p.repair_signature,p.plan_status,p.consumed_posts,p.blocked_reason,
                      p.candidate_spec_json,p.created_at,p.updated_at,
                      r.child_candidate_id,r.side_effect_verdict,r.before_json,r.after_json,r.delta_json
               FROM ppl_repair_plans p
               LEFT JOIN ppl_repairs r ON r.run_id=p.run_id AND r.repair_signature=p.repair_signature
               WHERE p.run_id=? AND p.target_failure=?
               ORDER BY p.created_at,p.repair_plan_id""",
            (run_id, PPC_TARGET_FAILURE),
        ).fetchall()
    return [dict(r) for r in rows]


def _incoming_ppc_parents(rows: Iterable[Mapping[str, Any]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for row in rows:
        child = str(row.get("child_candidate_id") or "")
        parent = str(row.get("parent_candidate_id") or "")
        if child and parent:
            out.setdefault(child, [])
            if parent not in out[child]:
                out[child].append(parent)
    return out


def infer_ppc_branch_anchor(store: Any, run_id: str, candidate_id: str,
                            *, rows: Optional[Sequence[Mapping[str, Any]]] = None) -> Optional[str]:
    """Trace only PP-correlation repair edges to the branch anchor.

    Non-PPC ancestors (for example a completed HIGH_TURNOVER staged repair) are
    deliberately outside this traversal.  If legacy many-to-one lineage is
    ambiguous, return None and let the selector fail closed for that branch.
    """
    material = list(rows) if rows is not None else _ppc_plan_rows(store, run_id)
    incoming = _incoming_ppc_parents(material)
    current = str(candidate_id or "")
    if not current:
        return None
    seen = set()
    while current in incoming:
        parents = sorted(set(incoming[current]))
        if len(parents) != 1:
            return None
        if current in seen:
            return None
        seen.add(current)
        current = parents[0]
    return current


def _row_anchor(store: Any, run_id: str, row: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Optional[str]:
    spec = _json_dict(row.get("candidate_spec_json"))
    explicit = str(spec.get("ppc_branch_anchor_candidate_id") or "")
    if explicit:
        return explicit
    return infer_ppc_branch_anchor(store, run_id, str(row.get("parent_candidate_id") or ""), rows=rows)


def ppc_branch_state(store: Any, config: Any, run_id: str, anchor_candidate_id: str) -> Dict[str, Any]:
    """Reconstruct bounded PPC branch state from durable outcomes only."""
    cfg = ppc_branch_config(config.rules)
    rows = _ppc_plan_rows(store, run_id)
    anchor = str(anchor_candidate_id or "")
    branch_rows = [r for r in rows if _row_anchor(store, run_id, r, rows) == anchor]
    attempts = [r for r in branch_rows if str(r.get("side_effect_verdict") or "").upper() in PPC_EVALUATED_OUTCOMES]
    best = anchor
    success_child = None
    for row in branch_rows:
        verdict = str(row.get("side_effect_verdict") or "").upper()
        parent = str(row.get("parent_candidate_id") or "")
        child = str(row.get("child_candidate_id") or "")
        if verdict == "TARGET_PASS":
            success_child = child or None
            if child:
                best = child
            break
        if verdict == "IMPROVED" and parent == best and child:
            best = child
    # DISPATCHED means a durable remote identity is still in flight. EXECUTED
    # means the Simulation result exists but PPC evidence may still be pending.
    # In either case the branch must not grow another child until the current
    # execution/evaluation is resolved. READY/PLANNED remain selectable.
    pending_eval = [
        r for r in branch_rows
        if str(r.get("plan_status") or "").upper() in {"DISPATCHED", "EXECUTED"}
        and str(r.get("side_effect_verdict") or "").upper() not in PPC_EVALUATED_OUTCOMES
    ]
    return {
        "policy": PPC_POLICY_NAME,
        "policy_version": PPC_POLICY_VERSION,
        "anchor_candidate_id": anchor,
        "best_candidate_id": best,
        "success": success_child is not None,
        "success_candidate_id": success_child,
        "attempts_used": len(attempts),
        "attempts_remaining": max(0, int(cfg["max_attempts"]) - len(attempts)),
        "max_attempts": int(cfg["max_attempts"]),
        "exhausted": len(attempts) >= int(cfg["max_attempts"]),
        "evaluation_pending": bool(pending_eval),
        "pending_plan_ids": [str(r.get("repair_plan_id") or "") for r in pending_eval],
        "evaluated_plan_ids": [str(r.get("repair_plan_id") or "") for r in attempts],
        "branch_rows": branch_rows,
    }


def _latest_pretag(store: Any, run_id: str, candidate_id: str) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    with store.connect() as conn:
        session = conn.execute(
            """SELECT * FROM ppl_check_sessions
               WHERE run_id=? AND phase='PRE_TAG' AND candidate_id=?
               ORDER BY updated_at DESC,check_session_id DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
        if not session:
            return None, []
        rows = conn.execute(
            """SELECT * FROM ppl_check_results WHERE check_session_id=?
               ORDER BY normalized_name,check_result_id""",
            (session["check_session_id"],),
        ).fetchall()
    return dict(session), [dict(r) for r in rows]


def _candidate_snapshot(store: Any, config: Any, alpha_db: Path, run_id: str,
                        candidate_id: str) -> Dict[str, Any]:
    candidates = {str(c.get("candidate_id")): dict(c) for c in store.load_candidates(run_id)}
    candidate = candidates.get(str(candidate_id))
    if not candidate:
        return {"ready": False, "reason": "CANDIDATE_MISSING", "candidate_id": candidate_id}
    metrics = (_alpha_metrics(alpha_db, [candidate.get("sim_key")]).get(str(candidate.get("sim_key"))) or {})
    local_gate = evaluate_local_pre_gate(candidate, metrics, config.rules)
    base = {
        "candidate_id": candidate_id,
        "candidate": candidate,
        "metrics": metrics,
        "local_gate": local_gate,
        "sharpe": _float(metrics.get("sharpe")),
        "fitness": _float(metrics.get("fitness")),
    }
    session, check_rows = _latest_pretag(store, run_id, str(candidate_id))
    if not session:
        return {**base, "ready": False, "reason": "PRETAG_SESSION_MISSING"}
    if str(session.get("session_status") or "").upper() != "RESOLVED":
        return {
            **base, "ready": False, "reason": "LATEST_PRETAG_NOT_RESOLVED",
            "check_session_id": session.get("check_session_id"), "session_status": session.get("session_status"),
        }
    cls = classify_ppl_candidate(
        candidate, metrics, check_rows, load_ppl_classification_policy_for_config(config),
    )
    return {
        **base,
        "ready": True,
        "classification": cls,
        "ppc": _float(cls.get("ppc_value")),
    }

def _fixed_failure_set(cls: Mapping[str, Any]) -> set:
    return {
        str(x.get("failure") or x.get("check") or "").upper()
        for x in (cls.get("fixed_blockers") or [])
        if str(x.get("failure") or x.get("check") or "").strip()
    }


def evaluate_ppc_repair_outcome(store: Any, config: Any, alpha_db: Path, run_id: str,
                                parent_candidate_id: str, child_candidate_id: str) -> Dict[str, Any]:
    """Evaluate one completed PPC repair from durable local facts only."""
    cfg = ppc_branch_config(config.rules)
    parent = _candidate_snapshot(store, config, alpha_db, run_id, parent_candidate_id)
    child = _candidate_snapshot(store, config, alpha_db, run_id, child_candidate_id)
    if not parent.get("ready"):
        return {
            "verdict": "PENDING_RESULT", "reason": parent.get("reason"),
            "parent": parent, "child": child,
        }
    if not child.get("ready"):
        # Repair children that fail the simulation-level local gate do not get a
        # PRE-TAG request. That is an evaluated negative side effect, not an
        # eternal PPC-evaluation hold.
        local_status = str((child.get("local_gate") or {}).get("status") or "").upper()
        sim_status = str((child.get("metrics") or {}).get("status") or (child.get("candidate") or {}).get("simulation_status") or "").upper()
        if local_status == "FAIL" and sim_status == "COMPLETE":
            return {
                "verdict": "REJECT_SIDE_EFFECT",
                "reason": "CHILD_LOCAL_GATE_FAIL",
                "parent_candidate_id": parent_candidate_id,
                "child_candidate_id": child_candidate_id,
                "parent_ppc": parent.get("ppc"), "child_ppc": None, "delta_ppc": None,
                "parent_sharpe": parent.get("sharpe"), "child_sharpe": child.get("sharpe"),
                "parent_fitness": parent.get("fitness"), "child_fitness": child.get("fitness"),
                "parent_classification": (parent.get("classification") or {}).get("classification"),
                "child_classification": "LOCAL_GATE_FAIL",
                "parent_fixed_failures": sorted(_fixed_failure_set(parent.get("classification") or {})),
                "child_fixed_failures": [], "new_fixed_failures": [],
                "side_effect_reasons": ["CHILD_LOCAL_GATE_FAIL"],
                "meaningful_improvement_min": float(cfg["meaningful_improvement_min"]),
                "meaningful_worsening_min": float(cfg["meaningful_worsening_min"]),
                "ppc_clean_max": float((parent.get("classification") or {}).get("ppc_strategy_clean_max") or 0.50),
            }
        return {
            "verdict": "PENDING_RESULT", "reason": child.get("reason"),
            "parent": parent, "child": child,
        }
    parent_cls = parent["classification"]
    child_cls = child["classification"]
    before_ppc = parent.get("ppc")
    after_ppc = child.get("ppc")
    if before_ppc is None or after_ppc is None:
        return {"verdict": "PENDING_RESULT", "reason": "PPC_VALUE_MISSING", "parent": parent, "child": child}

    parent_fixed = _fixed_failure_set(parent_cls)
    child_fixed = _fixed_failure_set(child_cls)
    new_fixed = sorted(child_fixed - parent_fixed - {PPC_TARGET_FAILURE})
    side_effect_reasons: List[str] = []
    if bool(cfg.get("require_no_new_fixed_blockers", True)) and new_fixed:
        side_effect_reasons.append("NEW_FIXED_BLOCKER:" + ",".join(new_fixed))
    child_status = str(child_cls.get("classification") or "")
    if child_status in {"PPL_TERMINAL_FAIL"}:
        side_effect_reasons.append("CHILD_TERMINAL_FAIL")
    if bool(cfg.get("require_not_strategy_rejected", True)) and child_status == "PPL_STRATEGY_REJECT_MID_PPC_LOW_SHARPE":
        # MID-PPC rejection is driven by insufficient Sharpe and is therefore a
        # quality side effect. HIGH-PPC rejection is the target metric itself;
        # its delta is classified below as WORSE rather than mislabeled as a
        # side effect.
        side_effect_reasons.append("CHILD_MID_PPC_LOW_SHARPE_REJECTED")

    max_sharpe_drop = cfg.get("max_sharpe_drop_abs")
    if max_sharpe_drop is not None and parent.get("sharpe") is not None and child.get("sharpe") is not None:
        if float(parent["sharpe"]) - float(child["sharpe"]) > float(max_sharpe_drop) + 1e-12:
            side_effect_reasons.append("SHARPE_DROP_EXCEEDS_CONFIG")
    max_fitness_drop = cfg.get("max_fitness_drop_abs")
    if max_fitness_drop is not None and parent.get("fitness") is not None and child.get("fitness") is not None:
        if float(parent["fitness"]) - float(child["fitness"]) > float(max_fitness_drop) + 1e-12:
            side_effect_reasons.append("FITNESS_DROP_EXCEEDS_CONFIG")

    delta = float(after_ppc) - float(before_ppc)
    clean_max = float(child_cls.get("ppc_strategy_clean_max") or 0.50)
    child_ppc_still_blocked = PPC_TARGET_FAILURE in child_fixed

    if side_effect_reasons:
        verdict = "REJECT_SIDE_EFFECT"
    elif after_ppc <= clean_max + 1e-12 and not child_ppc_still_blocked:
        verdict = "TARGET_PASS"
    elif delta <= -float(cfg["meaningful_improvement_min"]) - 1e-12:
        verdict = "IMPROVED"
    elif delta >= float(cfg["meaningful_worsening_min"]) - 1e-12:
        verdict = "WORSE"
    else:
        verdict = "NO_MEANINGFUL_CHANGE"

    return {
        "verdict": verdict,
        "parent_candidate_id": parent_candidate_id,
        "child_candidate_id": child_candidate_id,
        "parent_ppc": before_ppc,
        "child_ppc": after_ppc,
        "delta_ppc": delta,
        "parent_sharpe": parent.get("sharpe"),
        "child_sharpe": child.get("sharpe"),
        "parent_fitness": parent.get("fitness"),
        "child_fitness": child.get("fitness"),
        "parent_classification": parent_cls.get("classification"),
        "child_classification": child_cls.get("classification"),
        "parent_fixed_failures": sorted(parent_fixed),
        "child_fixed_failures": sorted(child_fixed),
        "new_fixed_failures": new_fixed,
        "side_effect_reasons": side_effect_reasons,
        "meaningful_improvement_min": float(cfg["meaningful_improvement_min"]),
        "meaningful_worsening_min": float(cfg["meaningful_worsening_min"]),
        "ppc_clean_max": clean_max,
    }


def ppc_outcome_payload(outcome: Mapping[str, Any], *, policy_version: str = PPC_POLICY_VERSION) -> Dict[str, Dict[str, Any]]:
    before = {
        "ppc": outcome.get("parent_ppc"),
        "sharpe": outcome.get("parent_sharpe"),
        "fitness": outcome.get("parent_fitness"),
        "classification": outcome.get("parent_classification"),
        "fixed_failures": outcome.get("parent_fixed_failures"),
    }
    after = {
        "ppc": outcome.get("child_ppc"),
        "sharpe": outcome.get("child_sharpe"),
        "fitness": outcome.get("child_fitness"),
        "classification": outcome.get("child_classification"),
        "fixed_failures": outcome.get("child_fixed_failures"),
    }
    delta = {
        "ppc": outcome.get("delta_ppc"),
        "new_fixed_failures": outcome.get("new_fixed_failures"),
        "side_effect_reasons": outcome.get("side_effect_reasons"),
        "meaningful_improvement_min": outcome.get("meaningful_improvement_min"),
        "meaningful_worsening_min": outcome.get("meaningful_worsening_min"),
        "ppc_clean_max": outcome.get("ppc_clean_max"),
        "repair_policy": PPC_POLICY_NAME,
        "repair_policy_version": policy_version,
    }
    return {"before": before, "after": after, "delta": delta}


def backfill_ppc_repair_outcomes(store: Any, config: Any, alpha_db: Path, run_id: str,
                                 *, confirm: bool = False) -> Dict[str, Any]:
    """Dry-run or explicitly persist missing historical PPC repair outcomes."""
    rows = _ppc_plan_rows(store, run_id)
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        current = str(row.get("side_effect_verdict") or "").upper()
        if current in PPC_EVALUATED_OUTCOMES:
            continue
        child = str(row.get("child_candidate_id") or "")
        parent = str(row.get("parent_candidate_id") or "")
        if not child or not parent:
            continue
        outcome = evaluate_ppc_repair_outcome(store, config, alpha_db, run_id, parent, child)
        candidates.append({
            "repair_plan_id": row.get("repair_plan_id"),
            "repair_signature": row.get("repair_signature"),
            "parent_candidate_id": parent,
            "child_candidate_id": child,
            **outcome,
        })

    evaluable = [x for x in candidates if str(x.get("verdict") or "").upper() in PPC_EVALUATED_OUTCOMES]
    pending = [x for x in candidates if x not in evaluable]
    writes = 0
    if confirm and evaluable:
        with store.connect() as conn:
            for item in evaluable:
                payload = ppc_outcome_payload(item)
                conn.execute(
                    """UPDATE ppl_repairs SET before_json=?,after_json=?,delta_json=?,side_effect_verdict=?
                       WHERE run_id=? AND repair_signature=?""",
                    (
                        json.dumps(payload["before"], ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        json.dumps(payload["after"], ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        json.dumps(payload["delta"], ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        item["verdict"], run_id, item["repair_signature"],
                    ),
                )
                writes += 1
    counts: Dict[str, int] = {}
    for item in evaluable:
        key = str(item.get("verdict") or "")
        counts[key] = counts.get(key, 0) + 1
    return {
        "action": "BACKFILL_PPC_REPAIR_OUTCOMES" if confirm else "PREVIEW_BACKFILL_PPC_REPAIR_OUTCOMES",
        "run_id": run_id,
        "confirmed": bool(confirm),
        "eligible_count": len(evaluable),
        "pending_count": len(pending),
        "verdict_counts": counts,
        "items": evaluable,
        "pending": pending,
        "network_requests": 0,
        "simulation_posts": 0,
        "check_requests": 0,
        "writes": writes,
    }
