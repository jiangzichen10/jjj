"""V3 research telemetry and replayable round artifacts.

This module is deliberately additive.  It does not change the V2.2 workflow
truth tables and never performs network I/O.  Its job is to preserve enough
research context to explain and replay a long production round later:

* what was discovered;
* what was selected or skipped and why;
* which logical simulations were cache/resume/new POST;
* how quality/check/family outcomes evolved batch by batch;
* which policy/config/code version produced the decisions.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from .family import family_id
from .live_execution import _alpha_facts

TELEMETRY_VERSION = "V3_TELEMETRY_003"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _loads(value: Any, fallback: Any) -> Any:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback
    return fallback if parsed is None else parsed


def _event_key(*parts: Any) -> str:
    raw = "|".join("" if x is None else str(x) for x in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def record_event(
    store: Any,
    round_id: str,
    run_id: str,
    event_type: str,
    *,
    batch_no: Optional[int] = None,
    phase: Optional[str] = None,
    candidate_id: Optional[str] = None,
    alpha_id: Optional[str] = None,
    family_id_value: Optional[str] = None,
    sim_key: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
    created_at: Optional[str] = None,
    source_event_key: Optional[str] = None,
) -> None:
    """Insert one idempotent round event.

    `source_event_key` should be supplied when mirroring another durable table.
    Otherwise a deterministic hash of the event identity/payload is used.
    """
    payload = dict(payload or {})
    created_at = created_at or _now()
    key = source_event_key or _event_key(
        round_id, run_id, batch_no, phase, event_type, candidate_id, alpha_id,
        family_id_value, sim_key, created_at, _json(payload),
    )
    with store.connect(stage="RESEARCH_TELEMETRY_EVENT") as conn:
        conn.execute(
            """INSERT OR IGNORE INTO ppl_round_events(
                   event_key,round_id,run_id,batch_no,phase,event_type,candidate_id,
                   alpha_id,family_id,sim_key,payload_json,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key, round_id, run_id, batch_no, phase, event_type, candidate_id,
             alpha_id, family_id_value, sim_key, _json(payload), created_at),
        )


def upsert_candidate_decision(
    store: Any,
    round_id: str,
    run_id: str,
    batch_no: int,
    candidate: Mapping[str, Any],
    *,
    decision: str,
    decision_reason: str,
    selection_rank: Optional[int] = None,
    selection_score: Optional[float] = None,
    quality_score: Optional[float] = None,
    novelty_score: Optional[float] = None,
    family_score: Optional[float] = None,
    dataset_score: Optional[float] = None,
    operator_score: Optional[float] = None,
    repair_risk_score: Optional[float] = None,
    selection_mode: Optional[str] = None,
    context: Optional[Mapping[str, Any]] = None,
) -> None:
    fid = family_id(candidate)
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_candidate_decisions(
                   round_id,run_id,batch_no,candidate_id,family_id,signal_family,
                   dataset_id,field_id,operator,expression,selection_rank,selection_score,
                   quality_score,novelty_score,family_score,dataset_score,operator_score,
                   repair_risk_score,selection_mode,decision,decision_reason,context_json,
                   created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,batch_no,candidate_id) DO UPDATE SET
                   family_id=excluded.family_id,signal_family=excluded.signal_family,
                   dataset_id=excluded.dataset_id,field_id=excluded.field_id,
                   operator=excluded.operator,expression=excluded.expression,
                   selection_rank=excluded.selection_rank,selection_score=excluded.selection_score,
                   quality_score=excluded.quality_score,novelty_score=excluded.novelty_score,
                   family_score=excluded.family_score,dataset_score=excluded.dataset_score,
                   operator_score=excluded.operator_score,repair_risk_score=excluded.repair_risk_score,
                   selection_mode=excluded.selection_mode,decision=excluded.decision,
                   decision_reason=excluded.decision_reason,context_json=excluded.context_json,
                   updated_at=excluded.updated_at""",
            (
                round_id, run_id, int(batch_no), str(candidate.get("candidate_id") or ""), fid,
                str(candidate.get("signal_family") or ""), candidate.get("dataset_id"),
                candidate.get("field_id"), candidate.get("operator"), candidate.get("expression"),
                selection_rank, selection_score, quality_score, novelty_score, family_score,
                dataset_score, operator_score, repair_risk_score, selection_mode, decision,
                decision_reason, _json(dict(context or {})), now, now,
            ),
        )


def record_candidate_universe(store: Any, round_id: str, run_id: str, candidates: Sequence[Mapping[str, Any]]) -> int:
    """Persist batch-0 discovery decisions for the entire initial universe."""
    count = 0
    candidate_ids = [str(c.get("candidate_id") or "") for c in candidates]
    provenance_by_id = {}
    if candidate_ids:
        marks = ",".join("?" for _ in candidate_ids)
        with store.connect() as conn:
            rows = conn.execute(
                f"SELECT candidate_id,provenance_json FROM ppl_candidate_provenance WHERE run_id=? AND candidate_id IN ({marks})",
                [run_id] + candidate_ids,
            ).fetchall()
        for row in rows:
            try:
                parsed = json.loads(row[1] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(parsed, dict) and parsed.get("extension_source"):
                provenance_by_id[str(row[0])] = parsed
    ordered = sorted(
        (dict(c) for c in candidates),
        key=lambda c: (-float(c.get("initial_selection_score") or 0.0), str(c.get("candidate_id") or "")),
    )
    for rank, c in enumerate(ordered, 1):
        eligible = str(c.get("structure_status") or "ELIGIBLE") == "ELIGIBLE"
        upsert_candidate_decision(
            store, round_id, run_id, 0, c,
            decision="DISCOVERED" if eligible else "DISCOVERED_INELIGIBLE",
            decision_reason="INITIAL_DISCOVERY_UNIVERSE" if eligible else str(c.get("structure_status") or "INELIGIBLE"),
            selection_rank=rank,
            selection_score=float(c.get("initial_selection_score") or 0.0),
            quality_score=float(c.get("initial_selection_score") or 0.0),
            selection_mode="DISCOVERY",
            context={
                "semantic_class": c.get("semantic_class"),
                "field_type": c.get("field_type"),
                "window": c.get("window"),
                "vector_reducer": c.get("vector_reducer"),
                "direction": c.get("direction"),
                "transform_family": c.get("transform_family"),
                "sim_key": c.get("sim_key"),
                "cache_classification": c.get("cache_classification"),
                "execution_action": c.get("execution_action"),
                "extension_source": (provenance_by_id.get(str(c.get("candidate_id") or "")) or {}).get("extension_source"),
                "extension_metadata": provenance_by_id.get(str(c.get("candidate_id") or "")),
            },
        )
        count += 1
    return count


def upsert_manifest(store: Any, round_id: str, run_id: str, payload: Mapping[str, Any]) -> str:
    body = _json(dict(payload))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_manifests(round_id,run_id,manifest_json,manifest_hash,created_at,updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(round_id) DO UPDATE SET
                   manifest_json=excluded.manifest_json,manifest_hash=excluded.manifest_hash,
                   updated_at=excluded.updated_at""",
            (round_id, run_id, body, digest, now, now),
        )
    return digest


def upsert_snapshot(
    store: Any,
    round_id: str,
    run_id: str,
    batch_no: int,
    phase: str,
    snapshot_type: str,
    payload: Mapping[str, Any],
) -> None:
    now = _now()
    with store.connect(stage="RESEARCH_TELEMETRY_SNAPSHOT") as conn:
        conn.execute(
            """INSERT INTO ppl_round_snapshots(
                   round_id,run_id,batch_no,phase,snapshot_type,payload_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,batch_no,snapshot_type) DO UPDATE SET
                   phase=excluded.phase,payload_json=excluded.payload_json,updated_at=excluded.updated_at""",
            (round_id, run_id, int(batch_no), phase, snapshot_type, _json(dict(payload)), now, now),
        )


def _json_scalar(value: Any) -> Any:
    """Decode scalar JSON columns used by the V2.2 check parser.

    Live check facts in audited V2.2 intentionally preserve the platform value
    in raw_value_json/raw_limit_json/effective_limit_json.  normalized_value and
    normalized_limit may be NULL even when a valid numeric live fact exists.
    Telemetry must therefore prefer normalized scalars when present and fall
    back to the durable JSON scalar columns without changing check semantics.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return value
    return value


def _latest_check_rows(store: Any, run_id: str, candidate_id: str) -> Dict[str, Dict[str, Any]]:
    with store.connect() as conn:
        session = conn.execute(
            """SELECT * FROM ppl_check_sessions
               WHERE run_id=? AND candidate_id=? AND phase='PRE_TAG'
               ORDER BY updated_at DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
        if not session:
            return {}
        rows = conn.execute(
            """SELECT normalized_name,normalized_value,normalized_limit,eligibility_outcome,
                      normalized_result,status,limit_source,diagnosis_outcome,
                      raw_value_json,raw_limit_json,effective_limit_json
               FROM ppl_check_results WHERE check_session_id=?""",
            (session["check_session_id"],),
        ).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        value = r[1] if r[1] is not None else _json_scalar(r[8])
        limit = r[2]
        if limit is None:
            limit = _json_scalar(r[10])
        if limit is None:
            limit = _json_scalar(r[9])
        out[str(r[0])] = {
            "value": value, "limit": limit, "outcome": r[3], "result": r[4],
            "status": r[5], "limit_source": r[6], "diagnosis_outcome": r[7],
            "value_source": "NORMALIZED" if r[1] is not None else "RAW_VALUE_JSON",
            "limit_value_source": (
                "NORMALIZED" if r[2] is not None else
                "EFFECTIVE_LIMIT_JSON" if _json_scalar(r[10]) is not None else
                "RAW_LIMIT_JSON"
            ),
        }
    return out


def _latest_diagnosis(store: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    with store.connect() as conn:
        row = conn.execute(
            """SELECT * FROM ppl_diagnoses WHERE run_id=? AND candidate_id=?
               ORDER BY created_at DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
    return dict(row) if row else {}


def _repair_info(store: Any, run_id: str, candidate_id: str) -> Dict[str, Any]:
    with store.connect() as conn:
        row = conn.execute(
            """SELECT repair_type,parent_candidate_id,side_effect_verdict,repair_id,repair_signature
               FROM ppl_repairs WHERE run_id=? AND child_candidate_id=?
               ORDER BY created_at DESC LIMIT 1""",
            (run_id, candidate_id),
        ).fetchone()
    return dict(row) if row else {}


def _ledger_sequence(conn: sqlite3.Connection, round_id: str) -> int:
    row = conn.execute(
        "SELECT coalesce(max(logical_sequence_no),0)+1 FROM ppl_round_simulation_ledger WHERE round_id=?",
        (round_id,),
    ).fetchone()
    return int(row[0] or 1)


def sync_simulation_ledger(
    store: Any,
    alpha_db: Path,
    round_id: str,
    run_id: str,
    *,
    batch_no: Optional[int] = None,
    phase: Optional[str] = None,
    candidate_ids: Optional[Iterable[str]] = None,
    origin_by_candidate: Optional[Mapping[str, str]] = None,
    selection_mode_by_candidate: Optional[Mapping[str, str]] = None,
    repair_plan_by_candidate: Optional[Mapping[str, str]] = None,
    classification_by_candidate: Optional[Mapping[str, str]] = None,
) -> int:
    """Refresh one-row-per-logical-simulation research ledger from durable facts."""
    wanted = {str(x) for x in (candidate_ids or []) if x}
    rows = [dict(c) for c in store.load_candidates(run_id) if not wanted or str(c.get("candidate_id")) in wanted]
    if not rows:
        return 0
    candidate_ids = [str(c.get("candidate_id") or "") for c in rows if c.get("candidate_id")]
    provenance_by_id: Dict[str, Dict[str, Any]] = {}
    if candidate_ids:
        marks = ",".join("?" for _ in candidate_ids)
        with store.connect() as conn:
            provenance_rows = conn.execute(
                f"SELECT candidate_id,provenance_json FROM ppl_candidate_provenance WHERE run_id=? AND candidate_id IN ({marks})",
                [run_id] + candidate_ids,
            ).fetchall()
        for row in provenance_rows:
            parsed = _loads(row[1], {})
            if isinstance(parsed, dict) and parsed.get("extension_source"):
                provenance_by_id[str(row[0])] = parsed
    facts = _alpha_facts(alpha_db, [str(c.get("sim_key")) for c in rows if c.get("sim_key")])
    origins = dict(origin_by_candidate or {})
    modes = dict(selection_mode_by_candidate or {})
    plan_map = dict(repair_plan_by_candidate or {})
    class_map = dict(classification_by_candidate or {})
    count = 0
    for c in rows:
        cid = str(c.get("candidate_id") or "")
        sk = str(c.get("sim_key") or "")
        if not sk:
            continue
        fact = facts.get(sk) or {}
        extension_metadata = provenance_by_id.get(cid) or {}
        if (not fact and str(c.get("simulation_status") or "NONE").upper() in {"", "NONE"}
                and cid not in origins):
            continue
        checks = _latest_check_rows(store, run_id, cid)
        diag = _latest_diagnosis(store, run_id, cid)
        repair = _repair_info(store, run_id, cid)
        try:
            settings = _loads(c.get("settings_json") or "{}", {})
        except Exception:
            settings = {}
        with store.connect(stage="RESEARCH_TELEMETRY_LEDGER") as conn:
            existing = conn.execute(
                "SELECT logical_sequence_no,origin,selection_mode,batch_no,phase,post_started_at FROM ppl_round_simulation_ledger WHERE round_id=? AND sim_key=?",
                (round_id, sk),
            ).fetchone()
            seq = int(existing[0]) if existing else _ledger_sequence(conn, round_id)
            origin = origins.get(cid) or (existing[1] if existing else None) or (
                "REPAIR" if c.get("parent_candidate_id") else "HISTORICAL"
            )
            mode = modes.get(cid) or (existing[2] if existing else None)
            effective_batch = int(batch_no) if batch_no is not None else (int(existing[3]) if existing and existing[3] is not None else None)
            effective_phase = phase or (existing[4] if existing else None) or ("REPAIR" if c.get("parent_candidate_id") else "SEARCH")
            post_started = existing[5] if existing else None
            if not post_started and origin == "NEW_POST":
                post_started = fact.get("submitted_at") or fact.get("date_created")
            completed_at = fact.get("updated_at") if str(fact.get("status") or "").upper() == "COMPLETE" else None
            duration_seconds = None
            if post_started and completed_at:
                try:
                    a = datetime.fromisoformat(str(post_started).replace("Z", "+00:00"))
                    b = datetime.fromisoformat(str(completed_at).replace("Z", "+00:00"))
                    duration_seconds = max(0.0, (b - a).total_seconds())
                except (TypeError, ValueError):
                    duration_seconds = None
            local_gate = "UNKNOWN"
            state = str(c.get("lifecycle_state") or "")
            if state in {
                "LOCAL_PRE_GATE_PASS", "PRE_TAG_CHECK_PENDING", "PRE_TAG_CHECK_COMPLETE",
                "PRE_TAG_CHECK_PASS", "FAMILY_DEDUP", "PRE_TAG_FINALIST", "DESCRIPTION_DRAFT",
                "DESCRIPTION_VALIDATED", "AWAITING_MANUAL_PROPERTIES", "PPL_TAGGED",
                "FINAL_CHECK_PENDING", "FINAL_CHECK_COMPLETE", "FINAL_CHECK_PASS",
                "READY_FOR_MANUAL_SUBMIT", "SUBMITTED",
            }:
                local_gate = "PASS"
            elif diag and str(diag.get("source_phase") or "").upper() == "SIMULATION":
                local_gate = "FAIL" if diag.get("primary_failure") else "UNKNOWN"
            pretag_status = None
            with store.connect() as cconn:
                s = cconn.execute(
                    """SELECT session_status,base_gate_result,theme_gate_result FROM ppl_check_sessions
                       WHERE run_id=? AND candidate_id=? AND phase='PRE_TAG' ORDER BY updated_at DESC LIMIT 1""",
                    (run_id, cid),
                ).fetchone()
            if s:
                pretag_status = "PASS" if str(s[0]) == "RESOLVED" and str(s[1]) == "PASS" and str(s[2]) == "PASS" else str(s[0])
            family_result = "WINNER" if state in {"FAMILY_DEDUP", "PRE_TAG_FINALIST", "READY_FOR_MANUAL_SUBMIT", "SUBMITTED"} else None
            rejection_reason = c.get("stop_reason") or diag.get("primary_failure")
            classification = class_map.get(cid)
            check_alias = {
                "ht_ratio": checks.get("HIGH_TURNOVER_RETURNS_RATIO") or checks.get("HT_HIGH_TURNOVER_RETURNS_RATIO") or {},
                "pp_corr": checks.get("POWER_POOL_CORRELATION") or {},
                "prod_corr": checks.get("PROD_CORRELATION") or {},
                "sub_universe": checks.get("SUB_UNIVERSE") or checks.get("LOW_SUB_UNIVERSE_SHARPE") or {},
                "two_year_sharpe": checks.get("TWO_YEAR_SHARPE") or checks.get("LOW_2Y_SHARPE") or {},
            }
            now = _now()
            conn.execute(
                """INSERT INTO ppl_round_simulation_ledger(
                       round_id,run_id,logical_sequence_no,batch_no,phase,candidate_id,parent_candidate_id,
                       repair_plan_id,family_id,signal_family,expression,dataset_id,field_id,operator,
                       region,universe,neutralization,decay,truncation,sim_key,origin,selection_mode,
                       post_started_at,completed_at,duration_seconds,alpha_id,simulation_status,
                       sharpe,fitness,turnover,returns,ht_ratio,pp_corr,prod_corr,sub_universe,
                       two_year_sharpe,local_gate,pretag_status,classification,repair_strategy,
                       repair_verdict,family_result,rejection_reason,details_json,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(round_id,sim_key) DO UPDATE SET
                       batch_no=coalesce(ppl_round_simulation_ledger.batch_no,excluded.batch_no),
                       phase=coalesce(ppl_round_simulation_ledger.phase,excluded.phase),
                       candidate_id=excluded.candidate_id,parent_candidate_id=excluded.parent_candidate_id,
                       repair_plan_id=coalesce(excluded.repair_plan_id,ppl_round_simulation_ledger.repair_plan_id),
                       family_id=excluded.family_id,signal_family=excluded.signal_family,
                       expression=excluded.expression,dataset_id=excluded.dataset_id,field_id=excluded.field_id,
                       operator=excluded.operator,region=excluded.region,universe=excluded.universe,
                       neutralization=excluded.neutralization,decay=excluded.decay,truncation=excluded.truncation,
                       origin=CASE WHEN ppl_round_simulation_ledger.origin IN ('UNKNOWN','HISTORICAL') AND excluded.origin NOT IN ('UNKNOWN','HISTORICAL') THEN excluded.origin ELSE ppl_round_simulation_ledger.origin END,
                       selection_mode=coalesce(ppl_round_simulation_ledger.selection_mode,excluded.selection_mode),
                       post_started_at=coalesce(ppl_round_simulation_ledger.post_started_at,excluded.post_started_at),
                       completed_at=excluded.completed_at,duration_seconds=excluded.duration_seconds,
                       alpha_id=excluded.alpha_id,simulation_status=excluded.simulation_status,
                       sharpe=excluded.sharpe,fitness=excluded.fitness,turnover=excluded.turnover,returns=excluded.returns,
                       ht_ratio=excluded.ht_ratio,pp_corr=excluded.pp_corr,prod_corr=excluded.prod_corr,
                       sub_universe=excluded.sub_universe,two_year_sharpe=excluded.two_year_sharpe,
                       local_gate=excluded.local_gate,pretag_status=excluded.pretag_status,
                       classification=coalesce(excluded.classification,ppl_round_simulation_ledger.classification),
                       repair_strategy=coalesce(excluded.repair_strategy,ppl_round_simulation_ledger.repair_strategy),
                       repair_verdict=coalesce(excluded.repair_verdict,ppl_round_simulation_ledger.repair_verdict),
                       family_result=coalesce(excluded.family_result,ppl_round_simulation_ledger.family_result),
                       rejection_reason=coalesce(excluded.rejection_reason,ppl_round_simulation_ledger.rejection_reason),
                       details_json=excluded.details_json,updated_at=excluded.updated_at""",
                (
                    round_id, run_id, seq, effective_batch, effective_phase, cid,
                    c.get("parent_candidate_id"), plan_map.get(cid), family_id(c), c.get("signal_family"),
                    c.get("expression"), c.get("dataset_id"), c.get("field_id"), c.get("operator"),
                    settings.get("region"), settings.get("universe"), c.get("neutralization") or settings.get("neutralization"),
                    c.get("decay") if c.get("decay") is not None else settings.get("decay"), settings.get("truncation"),
                    sk, origin, mode, post_started, completed_at, duration_seconds,
                    fact.get("alpha_id") or c.get("alpha_id"), fact.get("status") or c.get("simulation_status"),
                    fact.get("sharpe"), fact.get("fitness"), fact.get("turnover"), fact.get("returns"),
                    check_alias["ht_ratio"].get("value"), check_alias["pp_corr"].get("value"),
                    check_alias["prod_corr"].get("value"), check_alias["sub_universe"].get("value"),
                    check_alias["two_year_sharpe"].get("value"), local_gate, pretag_status, classification,
                    repair.get("repair_type"), repair.get("side_effect_verdict"), family_result, rejection_reason,
                    _json({
                        "settings": settings,
                        "check_metrics": check_alias,
                        "diagnosis": {k: diag.get(k) for k in ("primary_failure", "severity", "repairability", "root_cause")},
                        "repair_id": repair.get("repair_id"),
                        "repair_signature": repair.get("repair_signature"),
                        "cache_classification": c.get("cache_classification"),
                        "execution_action": c.get("execution_action"),
                        "extension": ({
                            key: extension_metadata.get(key)
                            for key in ("extension_source", "parent_dataset_id", "parent_field", "trigger_evidence")
                            if key in extension_metadata
                        } if extension_metadata else None),
                    }),
                    now, now,
                ),
            )
        count += 1
    return count


def sync_durable_events(store: Any, round_id: str, run_id: str) -> int:
    """Mirror important V2.2 durable facts into the round timeline idempotently."""
    inserted_before = 0
    with store.connect() as conn:
        inserted_before = int(conn.execute("SELECT count(*) FROM ppl_round_events WHERE round_id=?", (round_id,)).fetchone()[0])
        transitions = conn.execute(
            """SELECT transition_id,candidate_id,from_state,to_state,reason,source,metadata_json,created_at
               FROM ppl_state_transitions WHERE run_id=? ORDER BY transition_id""",
            (run_id,),
        ).fetchall()
    for t in transitions:
        candidate = None
        with store.connect() as conn:
            candidate = conn.execute("SELECT * FROM ppl_candidates WHERE candidate_id=?", (t[1],)).fetchone() if t[1] else None
        c = dict(candidate) if candidate else {}
        record_event(
            store, round_id, run_id, f"STATE_{t[3]}", candidate_id=t[1], alpha_id=c.get("alpha_id"),
            family_id_value=family_id(c) if c else None, sim_key=c.get("sim_key"),
            payload={"from_state": t[2], "to_state": t[3], "reason": t[4], "source": t[5], "metadata": _loads(t[6], {})},
            created_at=t[7], source_event_key=f"state_transition:{run_id}:{t[0]}",
        )
    with store.connect() as conn:
        checks = conn.execute(
            """SELECT check_session_id,candidate_id,alpha_id,phase,session_status,poll_count,http_request_count,
                      base_gate_result,theme_gate_result,error_type,error_nature,created_at,updated_at
               FROM ppl_check_sessions WHERE run_id=? ORDER BY created_at""",
            (run_id,),
        ).fetchall()
    for s in checks:
        with store.connect() as conn:
            candidate = conn.execute("SELECT * FROM ppl_candidates WHERE candidate_id=?", (s[1],)).fetchone() if s[1] else None
        c = dict(candidate) if candidate else {}
        record_event(
            store, round_id, run_id, "PRETAG_CHECK_SESSION" if str(s[3]) == "PRE_TAG" else "CHECK_SESSION",
            phase=s[3], candidate_id=s[1], alpha_id=s[2], family_id_value=family_id(c) if c else None,
            sim_key=c.get("sim_key"),
            payload={"session_status": s[4], "poll_count": s[5], "http_request_count": s[6],
                     "base_gate": s[7], "theme_gate": s[8], "error_type": s[9], "error_nature": s[10]},
            created_at=s[12] or s[11], source_event_key=f"check_session:{run_id}:{s[0]}:{s[4]}:{s[12]}",
        )
    with store.connect() as conn:
        repairs = conn.execute(
            """SELECT repair_id,parent_candidate_id,child_candidate_id,repair_type,side_effect_verdict,
                      before_json,after_json,delta_json,created_at
               FROM ppl_repairs WHERE run_id=? ORDER BY created_at""",
            (run_id,),
        ).fetchall()
    for r in repairs:
        with store.connect() as conn:
            child = conn.execute("SELECT * FROM ppl_candidates WHERE candidate_id=?", (r[2],)).fetchone() if r[2] else None
        c = dict(child) if child else {}
        record_event(
            store, round_id, run_id, "REPAIR_OUTCOME", candidate_id=r[2], alpha_id=c.get("alpha_id"),
            family_id_value=family_id(c) if c else None, sim_key=c.get("sim_key"),
            payload={"repair_id": r[0], "parent_candidate_id": r[1], "repair_type": r[3],
                     "verdict": r[4], "before": _loads(r[5], {}), "after": _loads(r[6], {}), "delta": _loads(r[7], {})},
            created_at=r[8], source_event_key=f"repair:{run_id}:{r[0]}",
        )
    with store.connect() as conn:
        after = int(conn.execute("SELECT count(*) FROM ppl_round_events WHERE round_id=?", (round_id,)).fetchone()[0])
    return after - inserted_before


def load_events(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_events WHERE round_id=? ORDER BY created_at,event_id", (round_id,)
        )]


def load_decisions(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_candidate_decisions WHERE round_id=? ORDER BY batch_no,coalesce(selection_rank,999999),candidate_id",
            (round_id,),
        )]


def load_ledger(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_simulation_ledger WHERE round_id=? ORDER BY logical_sequence_no,sim_key",
            (round_id,),
        )]


def load_snapshots(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_snapshots WHERE round_id=? ORDER BY batch_no,snapshot_type", (round_id,)
        )]


def load_manifest(store: Any, round_id: str) -> Optional[Dict[str, Any]]:
    with store.connect() as conn:
        row = conn.execute("SELECT * FROM ppl_round_manifests WHERE round_id=?", (round_id,)).fetchone()
    return dict(row) if row else None


def failure_matrix(
    store: Any,
    run_id: str,
    *,
    round_id: Optional[str] = None,
    batch_no: Optional[int] = None,
) -> list[Dict[str, Any]]:
    """Return denominator-aware failures with explicit research scopes.

    V3.0 mixed current-round NEW_POST/CACHE facts with inherited historical
    facts in one denominator.  That is useful for cumulative knowledge but
    misleading for batch evaluation.  V3.0.1 therefore exports explicit
    scopes while preserving all durable facts:

    * BATCH_NEW_POST   - logical NEW_POST facts attributed to ``batch_no``;
    * BATCH_CACHE      - cache facts attributed to ``batch_no``;
    * ROUND_CUMULATIVE - all non-historical facts consumed by this round;
    * HISTORICAL_BASELINE - prior facts present in the run but not bought by
      the current round.

    When ``round_id`` is omitted the function falls back to the legacy
    ROUND_CUMULATIVE view for compatibility.
    """
    candidates = [dict(c) for c in store.load_candidates(run_id)]
    by_id = {str(c.get("candidate_id")): c for c in candidates}
    with store.connect() as conn:
        diagnoses = [dict(r) for r in conn.execute(
            "SELECT candidate_id,primary_failure,secondary_failures_json,source_phase FROM ppl_diagnoses WHERE run_id=?",
            (run_id,),
        )]
    failures_by_candidate: Dict[str, set[str]] = defaultdict(set)
    for d in diagnoses:
        cid = str(d.get("candidate_id") or "")
        if d.get("primary_failure"):
            failures_by_candidate[cid].add(str(d["primary_failure"]))
        for f in _loads(d.get("secondary_failures_json"), []):
            if f:
                failures_by_candidate[cid].add(str(f))

    dimensions = {
        "ALL": lambda c: "ALL",
        "DATASET": lambda c: str(c.get("dataset_id") or "UNKNOWN"),
        "FIELD": lambda c: str(c.get("field_id") or "UNKNOWN"),
        "OPERATOR": lambda c: str(c.get("operator") or "UNKNOWN"),
        "SEMANTIC_CLASS": lambda c: str(c.get("semantic_class") or "UNKNOWN"),
        "WINDOW": lambda c: str(c.get("window") if c.get("window") is not None else "NONE"),
        "NEUTRALIZATION": lambda c: str(c.get("neutralization") or "UNKNOWN"),
    }
    tested_all = {
        str(c.get("candidate_id")) for c in candidates
        if str(c.get("simulation_status") or "NONE").upper() not in {"", "NONE"}
    }

    scopes: list[tuple[str, Optional[int], set[str]]] = []
    if round_id:
        ledger = load_ledger(store, round_id)
        # Facts explicitly attributed to this round.  REPAIR may be a new POST
        # or a cache/resume child; it remains part of ROUND_CUMULATIVE.
        current_round_ids = {
            str(r.get("candidate_id")) for r in ledger
            if str(r.get("origin") or "").upper() not in {"", "UNKNOWN", "HISTORICAL"}
            and r.get("candidate_id")
        }
        historical_ids = set(tested_all) - current_round_ids
        historical_ids.update(
            str(r.get("candidate_id")) for r in ledger
            if str(r.get("origin") or "").upper() in {"UNKNOWN", "HISTORICAL"}
            and r.get("candidate_id")
        )
        if batch_no is not None:
            b = int(batch_no)
            scopes.append((
                "BATCH_NEW_POST", b,
                {str(r.get("candidate_id")) for r in ledger
                 if int(r.get("batch_no") or -1) == b and str(r.get("origin") or "").upper() == "NEW_POST" and r.get("candidate_id")},
            ))
            scopes.append((
                "BATCH_CACHE", b,
                {str(r.get("candidate_id")) for r in ledger
                 if int(r.get("batch_no") or -1) == b and str(r.get("origin") or "").upper() == "CACHE" and r.get("candidate_id")},
            ))
        scopes.append(("ROUND_CUMULATIVE", None, current_round_ids))
        scopes.append(("HISTORICAL_BASELINE", None, historical_ids))
    else:
        scopes.append(("ROUND_CUMULATIVE", None, set(tested_all)))

    def emit_scope(scope: str, scope_batch: Optional[int], member_ids: set[str]) -> list[Dict[str, Any]]:
        members_all = [by_id[cid] for cid in sorted(member_ids) if cid in by_id]
        result: list[Dict[str, Any]] = []
        for dim, fn in dimensions.items():
            groups: Dict[str, list[Dict[str, Any]]] = defaultdict(list)
            for c in members_all:
                groups[fn(c)].append(c)
            # Keep the ALL denominator visible even for an empty batch scope.
            if dim == "ALL" and not groups:
                groups["ALL"] = []
            for value, members in groups.items():
                denominator = len(members)
                counts = Counter()
                for c in members:
                    for failure in failures_by_candidate.get(str(c.get("candidate_id")), set()):
                        counts[failure] += 1
                base = {"scope": scope, "batch_no": scope_batch, "dimension": dim, "value": value, "tested": denominator}
                if not counts:
                    result.append({**base, "failure": "NONE_RECORDED", "count": 0, "rate": 0.0})
                else:
                    for failure, count in counts.items():
                        result.append({
                            **base, "failure": failure, "count": int(count),
                            "rate": round(count / denominator, 6) if denominator else 0.0,
                        })
        return result

    out: list[Dict[str, Any]] = []
    for scope, scope_batch, ids in scopes:
        out.extend(emit_scope(scope, scope_batch, ids))
    return sorted(out, key=lambda r: (
        str(r.get("scope")), -1 if r.get("batch_no") is None else int(r.get("batch_no")),
        str(r.get("dimension")), str(r.get("value")), -int(r.get("count") or 0), str(r.get("failure")),
    ))
