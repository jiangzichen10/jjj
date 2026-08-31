"""V3 durable round orchestration state.

This module deliberately lives beside the V2.2 workflow store instead of
changing its core schema contract.  The V2.2 tables remain the source of truth
for candidates/checks/repairs; these tables only persist long-running V3 round
coordination and reports.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional


COMPLETED_RESEARCH_BATCH_STATUSES = frozenset({
    "COMPLETED", "RECOVERED", "RECOVERED_PRE_DISPATCH",
})

ROUND_SCHEMA_VERSION = 4


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def config_hash(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(dict(config)).encode("utf-8")).hexdigest()


def ensure_round_schema(store: Any) -> None:
    now = _now()
    with store.connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ppl_round_meta(
                schema_version INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ppl_rounds(
                round_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL UNIQUE REFERENCES ppl_runs(run_id),
                objective TEXT NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                config_json TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                total_budget INTEGER NOT NULL,
                search_budget INTEGER NOT NULL,
                repair_budget INTEGER NOT NULL,
                search_consumed INTEGER NOT NULL DEFAULT 0,
                repair_consumed INTEGER NOT NULL DEFAULT 0,
                batch_size INTEGER NOT NULL,
                current_batch INTEGER NOT NULL DEFAULT 0,
                protected_family_count INTEGER NOT NULL DEFAULT 0,
                winner_count INTEGER NOT NULL DEFAULT 0,
                stop_reason TEXT,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS ppl_round_batches(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                batch_no INTEGER NOT NULL,
                phase TEXT NOT NULL,
                status TEXT NOT NULL,
                selected_candidate_ids_json TEXT NOT NULL,
                selected_plan_ids_json TEXT NOT NULL,
                planned_post_sim_keys_json TEXT NOT NULL DEFAULT '[]',
                planned_resume_sim_keys_json TEXT NOT NULL DEFAULT '[]',
                projected_new_posts INTEGER NOT NULL DEFAULT 0,
                logical_posts_consumed INTEGER NOT NULL DEFAULT 0,
                cache_hits INTEGER NOT NULL DEFAULT 0,
                resume_count INTEGER NOT NULL DEFAULT 0,
                check_count INTEGER NOT NULL DEFAULT 0,
                report_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                PRIMARY KEY(round_id,batch_no)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_family_winners(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                family_id TEXT NOT NULL,
                signal_family TEXT NOT NULL,
                candidate_id TEXT,
                alpha_id TEXT,
                winner_state TEXT NOT NULL,
                source TEXT NOT NULL,
                score_json TEXT NOT NULL,
                protected INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,family_id)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_events(
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                batch_no INTEGER,
                phase TEXT,
                event_type TEXT NOT NULL,
                candidate_id TEXT,
                alpha_id TEXT,
                family_id TEXT,
                sim_key TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ppl_round_candidate_decisions(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                batch_no INTEGER NOT NULL,
                candidate_id TEXT NOT NULL,
                family_id TEXT NOT NULL,
                signal_family TEXT,
                dataset_id TEXT,
                field_id TEXT,
                operator TEXT,
                expression TEXT,
                selection_rank INTEGER,
                selection_score REAL,
                quality_score REAL,
                novelty_score REAL,
                family_score REAL,
                dataset_score REAL,
                operator_score REAL,
                repair_risk_score REAL,
                selection_mode TEXT,
                decision TEXT NOT NULL,
                decision_reason TEXT NOT NULL,
                context_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,batch_no,candidate_id)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_simulation_ledger(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                logical_sequence_no INTEGER NOT NULL,
                batch_no INTEGER,
                phase TEXT,
                candidate_id TEXT,
                parent_candidate_id TEXT,
                repair_plan_id TEXT,
                family_id TEXT,
                signal_family TEXT,
                expression TEXT,
                dataset_id TEXT,
                field_id TEXT,
                operator TEXT,
                region TEXT,
                universe TEXT,
                neutralization TEXT,
                decay INTEGER,
                truncation REAL,
                sim_key TEXT NOT NULL,
                origin TEXT NOT NULL,
                selection_mode TEXT,
                post_started_at TEXT,
                completed_at TEXT,
                duration_seconds REAL,
                alpha_id TEXT,
                simulation_status TEXT,
                sharpe REAL,
                fitness REAL,
                turnover REAL,
                returns REAL,
                ht_ratio REAL,
                pp_corr REAL,
                prod_corr REAL,
                sub_universe REAL,
                two_year_sharpe REAL,
                local_gate TEXT,
                pretag_status TEXT,
                classification TEXT,
                repair_strategy TEXT,
                repair_verdict TEXT,
                family_result TEXT,
                rejection_reason TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,sim_key),
                UNIQUE(round_id,logical_sequence_no)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_snapshots(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                batch_no INTEGER NOT NULL,
                phase TEXT NOT NULL,
                snapshot_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,batch_no,snapshot_type)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_manifests(
                round_id TEXT PRIMARY KEY REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ppl_round_policy_state(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL,
                policy_type TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                source_path TEXT,
                activated_batch_no INTEGER NOT NULL DEFAULT 0,
                activated_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,policy_type)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_scheduler_authority_state(
                round_id TEXT PRIMARY KEY REFERENCES ppl_rounds(round_id),
                run_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                active_authority TEXT NOT NULL,
                shadow_authority TEXT NOT NULL,
                pending_authority TEXT,
                authority_epoch INTEGER NOT NULL DEFAULT 0,
                scheduler_policy_version TEXT NOT NULL,
                scheduler_policy_hash TEXT NOT NULL,
                activation_gate_report_key TEXT,
                preflight_json TEXT NOT NULL DEFAULT '{}',
                activated_batch_no INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ppl_round_dataset_states(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                dataset_id TEXT NOT NULL,
                state TEXT NOT NULL,
                admitted_batch INTEGER NOT NULL DEFAULT 0,
                last_refresh_batch INTEGER NOT NULL DEFAULT 0,
                source_snapshot_id TEXT,
                productivity_score REAL,
                attempts INTEGER NOT NULL DEFAULT 0,
                checked INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(round_id,dataset_id)
            );
            CREATE TABLE IF NOT EXISTS ppl_round_dataset_refreshes(
                round_id TEXT NOT NULL REFERENCES ppl_rounds(round_id),
                refresh_no INTEGER NOT NULL,
                batch_no INTEGER NOT NULL,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL,
                source_snapshot_id TEXT,
                probed_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                admitted_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                cooled_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                retained_dataset_ids_json TEXT NOT NULL DEFAULT '[]',
                new_candidate_count INTEGER NOT NULL DEFAULT 0,
                network_get_count INTEGER NOT NULL DEFAULT 0,
                stats_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                PRIMARY KEY(round_id,refresh_no)
            );
            CREATE INDEX IF NOT EXISTS idx_round_batches_round ON ppl_round_batches(round_id,batch_no);
            CREATE INDEX IF NOT EXISTS idx_round_winners_round ON ppl_round_family_winners(round_id,protected);
            CREATE INDEX IF NOT EXISTS idx_round_events_round_time ON ppl_round_events(round_id,created_at,event_id);
            CREATE INDEX IF NOT EXISTS idx_round_events_candidate ON ppl_round_events(round_id,candidate_id,event_type);
            CREATE INDEX IF NOT EXISTS idx_round_dataset_states_active ON ppl_round_dataset_states(round_id,state,dataset_id);
            CREATE INDEX IF NOT EXISTS idx_round_dataset_refreshes_round ON ppl_round_dataset_refreshes(round_id,refresh_no);
            CREATE INDEX IF NOT EXISTS idx_round_decisions_round_batch ON ppl_round_candidate_decisions(round_id,batch_no,decision);
            CREATE INDEX IF NOT EXISTS idx_round_ledger_round_batch ON ppl_round_simulation_ledger(round_id,batch_no,logical_sequence_no);
            CREATE INDEX IF NOT EXISTS idx_round_ledger_family ON ppl_round_simulation_ledger(round_id,family_id);
            """
        )
        # Additive migration for early V3 development snapshots.  This module
        # owns only V3 coordination tables, so the V2.2 core schema contract
        # (ppl_schema_meta / SCHEMA_VERSION=13) stays untouched.
        columns = {str(r[1]) for r in conn.execute("PRAGMA table_info(ppl_round_batches)")}
        if "planned_post_sim_keys_json" not in columns:
            conn.execute(
                "ALTER TABLE ppl_round_batches ADD COLUMN planned_post_sim_keys_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "planned_resume_sim_keys_json" not in columns:
            conn.execute(
                "ALTER TABLE ppl_round_batches ADD COLUMN planned_resume_sim_keys_json TEXT NOT NULL DEFAULT '[]'"
            )
        conn.execute(
            """INSERT INTO ppl_round_meta(schema_version,created_at,updated_at)
               VALUES (?,?,?) ON CONFLICT(schema_version) DO UPDATE SET updated_at=excluded.updated_at""",
            (ROUND_SCHEMA_VERSION, now, now),
        )


def create_round(store: Any, *, round_id: str, run_id: str, policy: Mapping[str, Any],
                 total_budget: int, search_budget: int, repair_budget: int) -> None:
    ensure_round_schema(store)
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_rounds(
                   round_id,run_id,objective,status,phase,config_json,config_hash,total_budget,
                   search_budget,repair_budget,batch_size,started_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (round_id,run_id,str(policy["objective"]),"CREATED","SEARCH",_json(policy),config_hash(policy),
             int(total_budget),int(search_budget),int(repair_budget),int(policy["batch_size"]),now,now),
        )
        # D3-A persists an ARMED authority identity with no active executor.
        # It is deliberately created in the same transaction as the round so a
        # crash cannot leave an Adaptive canary without its authority lock.
        from .research_run_mode import ADAPTIVE_CANARY_MODE, parse_research_run_policy
        research = parse_research_run_policy(policy)
        if research.mode == ADAPTIVE_CANARY_MODE:
            scheduler_raw = dict(policy.get("scheduler_shadow") or {})
            scheduler_body = _json(scheduler_raw)
            scheduler_digest = hashlib.sha256(scheduler_body.encode("utf-8")).hexdigest()
            conn.execute(
                """INSERT INTO ppl_round_scheduler_authority_state(
                       round_id,run_id,state,active_authority,shadow_authority,pending_authority,
                       authority_epoch,scheduler_policy_version,scheduler_policy_hash,
                       activation_gate_report_key,preflight_json,activated_batch_no,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    round_id, run_id, "ARMED", "NONE", research.scheduler_shadow,
                    research.scheduler_authority, 0,
                    str(scheduler_raw.get("policy_version") or ""), scheduler_digest,
                    research.d2_gate_report_key, "{}", None, now, now,
                ),
            )


def get_round(store: Any, *, round_id: Optional[str] = None, run_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with store.connect() as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_rounds'").fetchone()
        if not exists:
            return None
        if round_id:
            row = conn.execute("SELECT * FROM ppl_rounds WHERE round_id=?", (round_id,)).fetchone()
        elif run_id:
            row = conn.execute("SELECT * FROM ppl_rounds WHERE run_id=?", (run_id,)).fetchone()
        else:
            row = conn.execute("SELECT * FROM ppl_rounds ORDER BY started_at DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def load_scheduler_authority_state(
    store: Any, *, round_id: Optional[str] = None, run_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Load the durable D3 authority identity without synthesizing defaults."""
    if not round_id and not run_id:
        raise ValueError("SCHEDULER_AUTHORITY_LOOKUP_KEY_REQUIRED")
    with store.connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_scheduler_authority_state'"
        ).fetchone()
        if not exists:
            return None
        if round_id:
            row = conn.execute(
                "SELECT * FROM ppl_round_scheduler_authority_state WHERE round_id=?", (round_id,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM ppl_round_scheduler_authority_state WHERE run_id=?", (run_id,),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["preflight"] = json.loads(str(result.pop("preflight_json") or "{}"))
        return result


def checkpoint_scheduler_authority_preflight(
    store: Any,
    *,
    round_id: str,
    run_id: str,
    expected_epoch: int,
    preflight: Mapping[str, Any],
) -> Dict[str, Any]:
    """Durably checkpoint a read-only D3 preflight without activating authority."""
    ensure_round_schema(store)
    now = _now()
    payload = _json(dict(preflight or {}))
    with store.connect(stage="D3_AUTHORITY_PREFLIGHT_CHECKPOINT") as conn:
        row = conn.execute(
            "SELECT * FROM ppl_round_scheduler_authority_state WHERE round_id=? AND run_id=?",
            (round_id, run_id),
        ).fetchone()
        if not row:
            raise RuntimeError("D3_AUTHORITY_STATE_NOT_FOUND")
        if str(row["state"] or "") != "ARMED" or str(row["active_authority"] or "") != "NONE":
            raise RuntimeError("D3_AUTHORITY_PREFLIGHT_REQUIRES_ARMED_INACTIVE_STATE")
        if int(row["authority_epoch"] or 0) != int(expected_epoch):
            raise RuntimeError("D3_AUTHORITY_EPOCH_CONFLICT")
        conn.execute(
            """UPDATE ppl_round_scheduler_authority_state
               SET preflight_json=?,updated_at=? WHERE round_id=? AND run_id=? AND authority_epoch=?""",
            (payload, now, round_id, run_id, int(expected_epoch)),
        )
    state = load_scheduler_authority_state(store, round_id=round_id)
    if not state:
        raise RuntimeError("D3_AUTHORITY_STATE_CHECKPOINT_LOST")
    return state


def update_round(store: Any, round_id: str, **fields: Any) -> None:
    if not fields:
        return
    fields = dict(fields)
    fields["updated_at"] = _now()
    names = sorted(fields)
    with store.connect() as conn:
        conn.execute(
            "UPDATE ppl_rounds SET " + ",".join(f"{name}=?" for name in names) + " WHERE round_id=?",
            [fields[name] for name in names] + [round_id],
        )


def start_batch(store: Any, round_id: str, batch_no: int, phase: str,
                candidate_ids: Iterable[str] = (), plan_ids: Iterable[str] = (),
                projected_new_posts: int = 0,
                planned_post_sim_keys: Iterable[str] = (),
                planned_resume_sim_keys: Iterable[str] = ()) -> None:
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_batches(
                   round_id,batch_no,phase,status,selected_candidate_ids_json,selected_plan_ids_json,
                   planned_post_sim_keys_json,planned_resume_sim_keys_json,
                   projected_new_posts,report_json,started_at,completed_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)""",
            (round_id,int(batch_no),phase,"RUNNING",_json(list(candidate_ids)),_json(list(plan_ids)),
             _json(sorted(set(str(x) for x in planned_post_sim_keys))),
             _json(sorted(set(str(x) for x in planned_resume_sim_keys))),
             int(projected_new_posts),"{}",now),
        )


def set_batch_intent(store: Any, round_id: str, batch_no: int, *,
                     planned_post_sim_keys: Iterable[str],
                     planned_resume_sim_keys: Iterable[str]) -> None:
    """Persist the exact network intent immediately before V2.1 is invoked.

    This is the crash-recovery anchor for a long V3 round.  If the process is
    terminated after a remote Simulation was created but before the batch
    report/counters are committed, resume can reconstruct logical budget use
    from these sim_keys plus alpha_results.db without re-POSTing.
    """
    with store.connect() as conn:
        cur = conn.execute(
            """UPDATE ppl_round_batches
               SET planned_post_sim_keys_json=?,planned_resume_sim_keys_json=?
               WHERE round_id=? AND batch_no=?""",
            (_json(sorted(set(str(x) for x in planned_post_sim_keys))),
             _json(sorted(set(str(x) for x in planned_resume_sim_keys))),
             round_id,int(batch_no)),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"ROUND_BATCH_INTENT_NOT_DURABLE:{round_id}:{batch_no}")


def finish_batch(store: Any, round_id: str, batch_no: int, report: Mapping[str, Any], *,
                 logical_posts_consumed: int, cache_hits: int = 0, resume_count: int = 0,
                 check_count: int = 0, status: str = "COMPLETED") -> None:
    with store.connect() as conn:
        conn.execute(
            """UPDATE ppl_round_batches SET status=?,logical_posts_consumed=?,cache_hits=?,resume_count=?,
                   check_count=?,report_json=?,completed_at=? WHERE round_id=? AND batch_no=?""",
            (status,int(logical_posts_consumed),int(cache_hits),int(resume_count),int(check_count),
             _json(dict(report)),_now(),round_id,int(batch_no)),
        )


def upsert_winner(store: Any, round_id: str, *, family_id: str, signal_family: str,
                  candidate_id: Optional[str], alpha_id: Optional[str], winner_state: str,
                  source: str, score: Mapping[str, Any], protected: bool = True) -> None:
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_family_winners(
                   round_id,family_id,signal_family,candidate_id,alpha_id,winner_state,source,
                   score_json,protected,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,family_id) DO UPDATE SET
                   candidate_id=excluded.candidate_id,alpha_id=excluded.alpha_id,
                   winner_state=excluded.winner_state,source=excluded.source,
                   score_json=excluded.score_json,protected=excluded.protected,updated_at=excluded.updated_at""",
            (round_id,family_id,signal_family,candidate_id,alpha_id,winner_state,source,
             _json(dict(score)),int(bool(protected)),now,now),
        )


def load_winners(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_family_winners'").fetchone()
        if not exists:
            return []
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_family_winners WHERE round_id=? ORDER BY created_at,family_id", (round_id,)
        )]


def load_batches(store: Any, round_id: str) -> list[Dict[str, Any]]:
    with store.connect() as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_batches'").fetchone()
        if not exists:
            return []
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_batches WHERE round_id=? ORDER BY batch_no", (round_id,)
        )]


def load_dataset_states(store: Any, round_id: str) -> list[Dict[str, Any]]:
    ensure_round_schema(store)
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_dataset_states WHERE round_id=? ORDER BY dataset_id", (round_id,)
        )]


def upsert_dataset_state(
    store: Any, round_id: str, dataset_id: str, *, state: str, admitted_batch: int = 0,
    last_refresh_batch: int = 0, source_snapshot_id: Optional[str] = None,
    productivity_score: Optional[float] = None, attempts: int = 0, checked: int = 0,
    success: int = 0, reason: Optional[str] = None, metadata: Optional[Mapping[str, Any]] = None,
) -> None:
    ensure_round_schema(store)
    now = _now()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_dataset_states(
                   round_id,dataset_id,state,admitted_batch,last_refresh_batch,source_snapshot_id,
                   productivity_score,attempts,checked,success,reason,metadata_json,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,dataset_id) DO UPDATE SET
                   state=excluded.state,last_refresh_batch=excluded.last_refresh_batch,
                   source_snapshot_id=coalesce(excluded.source_snapshot_id,ppl_round_dataset_states.source_snapshot_id),
                   productivity_score=excluded.productivity_score,attempts=excluded.attempts,
                   checked=excluded.checked,success=excluded.success,reason=excluded.reason,
                   metadata_json=excluded.metadata_json,updated_at=excluded.updated_at""",
            (round_id, str(dataset_id), str(state), int(admitted_batch), int(last_refresh_batch),
             source_snapshot_id, productivity_score, int(attempts), int(checked), int(success), reason,
             _json(dict(metadata or {})), now, now),
        )


def record_dataset_refresh(
    store: Any, round_id: str, *, refresh_no: int, batch_no: int, trigger: str, status: str,
    source_snapshot_id: Optional[str] = None, probed_dataset_ids: Iterable[str] = (),
    admitted_dataset_ids: Iterable[str] = (), cooled_dataset_ids: Iterable[str] = (),
    retained_dataset_ids: Iterable[str] = (), new_candidate_count: int = 0,
    network_get_count: int = 0, stats: Optional[Mapping[str, Any]] = None,
) -> None:
    ensure_round_schema(store)
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_dataset_refreshes(
                   round_id,refresh_no,batch_no,trigger,status,source_snapshot_id,
                   probed_dataset_ids_json,admitted_dataset_ids_json,cooled_dataset_ids_json,
                   retained_dataset_ids_json,new_candidate_count,network_get_count,stats_json,created_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,refresh_no) DO NOTHING""",
            (round_id, int(refresh_no), int(batch_no), str(trigger), str(status), source_snapshot_id,
             _json(list(probed_dataset_ids)), _json(list(admitted_dataset_ids)), _json(list(cooled_dataset_ids)),
             _json(list(retained_dataset_ids)), int(new_candidate_count), int(network_get_count),
             _json(dict(stats or {})), _now()),
        )


def load_dataset_refreshes(store: Any, round_id: str) -> list[Dict[str, Any]]:
    ensure_round_schema(store)
    with store.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ppl_round_dataset_refreshes WHERE round_id=? ORDER BY refresh_no", (round_id,)
        )]

def upsert_policy_state(
    store: Any, round_id: str, run_id: str, *, policy_type: str, policy_version: str,
    policy_hash: str, payload: Mapping[str, Any], source_path: Optional[str] = None,
    activated_batch_no: int = 0, activated_at: Optional[str] = None,
) -> None:
    """Persist the active policy identity/payload used by a long-running round."""
    ensure_round_schema(store)
    now = _now()
    activated = str(activated_at or now)
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_round_policy_state(
                   round_id,run_id,policy_type,policy_version,policy_hash,payload_json,source_path,
                   activated_batch_no,activated_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(round_id,policy_type) DO UPDATE SET
                   policy_version=excluded.policy_version,policy_hash=excluded.policy_hash,
                   payload_json=excluded.payload_json,source_path=excluded.source_path,
                   activated_batch_no=excluded.activated_batch_no,
                   activated_at=excluded.activated_at,updated_at=excluded.updated_at""",
            (round_id, run_id, str(policy_type), str(policy_version), str(policy_hash),
             _json(dict(payload or {})), source_path, int(activated_batch_no), activated, now),
        )


def load_policy_state(store: Any, round_id: str, policy_type: str) -> Optional[Dict[str, Any]]:
    # Read-only callers (notably --round-status) must not create/migrate tables.
    with store.connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_policy_state'"
        ).fetchone()
        if not exists:
            return None
        row = conn.execute(
            "SELECT * FROM ppl_round_policy_state WHERE round_id=? AND policy_type=?",
            (round_id, str(policy_type)),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    try:
        item["payload"] = json.loads(str(item.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        item["payload"] = {}
    return item


def load_policy_states(store: Any, round_id: str) -> list[Dict[str, Any]]:
    """Return all durable active policy states for one round without mutating schema."""
    with store.connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ppl_round_policy_state'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(
            "SELECT * FROM ppl_round_policy_state WHERE round_id=? ORDER BY policy_type", (round_id,)
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(str(item.get("payload_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            item["payload"] = {}
        out.append(item)
    return out


def activate_policy_bundle_atomic(
    store: Any,
    round_id: str,
    run_id: str,
    *,
    states: Iterable[Mapping[str, Any]],
    active_config: Mapping[str, Any],
    activated_batch_no: int,
    event: Optional[Mapping[str, Any]] = None,
) -> None:
    """Atomically activate a Q/S/R policy bundle and round active config.

    The policy-state rows and ``ppl_rounds.config_json`` are one durable truth.
    An optional round event is inserted in the same SQLite transaction so a
    crash cannot leave a half-activated policy bundle with misleading audit
    attribution.
    """
    ensure_round_schema(store)
    now = _now()
    config_body = _json(dict(active_config or {}))
    config_digest = __import__("hashlib").sha256(config_body.encode("utf-8")).hexdigest()
    state_rows = [dict(x) for x in states]
    with store.connect(stage="POLICY_BUNDLE_ACTIVATION") as conn:
        for item in state_rows:
            activated = str(item.get("activated_at") or now)
            conn.execute(
                """INSERT INTO ppl_round_policy_state(
                       round_id,run_id,policy_type,policy_version,policy_hash,payload_json,source_path,
                       activated_batch_no,activated_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(round_id,policy_type) DO UPDATE SET
                       policy_version=excluded.policy_version,policy_hash=excluded.policy_hash,
                       payload_json=excluded.payload_json,source_path=excluded.source_path,
                       activated_batch_no=excluded.activated_batch_no,
                       activated_at=excluded.activated_at,updated_at=excluded.updated_at""",
                (
                    round_id, run_id, str(item.get("policy_type") or ""),
                    str(item.get("policy_version") or ""), str(item.get("policy_hash") or ""),
                    _json(dict(item.get("payload") or {})), item.get("source_path"),
                    int(item.get("activated_batch_no", activated_batch_no)), activated, now,
                ),
            )
        cur = conn.execute(
            "UPDATE ppl_rounds SET config_json=?,config_hash=?,updated_at=? WHERE round_id=? AND run_id=?",
            (config_body, config_digest, now, round_id, run_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"POLICY_BUNDLE_ROUND_UPDATE_FAILED:{round_id}:{run_id}")
        if event:
            event = dict(event)
            event_key = str(event.get("event_key") or "")
            if not event_key:
                raise ValueError("POLICY_BUNDLE_EVENT_KEY_REQUIRED")
            conn.execute(
                """INSERT OR IGNORE INTO ppl_round_events(
                       event_key,round_id,run_id,batch_no,phase,event_type,candidate_id,
                       alpha_id,family_id,sim_key,payload_json,created_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_key, round_id, run_id,
                    event.get("batch_no"), event.get("phase"), str(event.get("event_type") or "POLICY_BUNDLE_RELOADED"),
                    event.get("candidate_id"), event.get("alpha_id"), event.get("family_id"), event.get("sim_key"),
                    _json(dict(event.get("payload") or {})), str(event.get("created_at") or now),
                ),
            )
