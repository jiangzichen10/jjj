"""Phase 10B: four-post bounded initial-search validation."""
from __future__ import annotations

import hashlib, json, time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .config import (
    COMPATIBLE_EXECUTION_HASH_STATUSES,
    ConfigError,
    execution_hash_status_for_run,
    simulation_budget_allocation,
)
from .live_execution import (
    MACHINE_HASH_OPERATION_PHASE10B, MACHINE_HASH_OPERATION_PHASE10B_REPAIR,
    _alpha_facts, _instrument_v21, _json,
    _now, _sync_candidate_fact, _v21_candidate, run_local_analysis,
    run_one_pretag_check, validate_machine_lib_hash,
)
from .simulation_adapter import execute_with_v21
from .state_machine import CANDIDATE_TRANSITIONS, RUN_TRANSITIONS
from .repair_engine import is_retired_auto_repair_plan, materialize_repair_candidate
from .settings_contract import validate_full_simulation_settings

PHASE = "PHASE_10B_INITIAL_VALIDATION"
CANARY_A_PHASE = "PHASE_10A_CANARY"
TOTAL_CAP, INITIAL_CAP, PHASE10B_CAP, REPAIR_RESERVE = 10, 6, 4, 4


def validate_phase10b_config(store: Any, config: Any) -> Dict[str, int]:
    plan=config.plan
    if plan.get("run_profile")!="LIVE_VALIDATION":raise ConfigError("PHASE10B_REQUIRES_LIVE_VALIDATION_PROFILE")
    if plan.get("validation_phase")!=PHASE:raise ConfigError("PHASE10B_VALIDATION_PHASE_MISMATCH")
    if not plan.get("source_run_id") or not plan.get("phase10a_run_id"):raise ConfigError("PHASE10B_RUN_LINEAGE_REQUIRED")
    allocation=simulation_budget_allocation(plan);total=int(plan["budgets"]["max_new_simulation_posts"])
    if total>TOTAL_CAP or allocation["initial_search_budget"]>INITIAL_CAP:raise ConfigError("PHASE10_BUDGET_HARD_CAP_EXCEEDED")
    if allocation["repair_reserve_budget"]<REPAIR_RESERVE:raise ConfigError("PHASE10_REPAIR_RESERVE_NOT_PRESERVED")
    requested=int(plan["runtime"]["concurrency"]);region=str(plan["simulation_settings"]["region"]).upper()
    regional=int(plan["runtime"]["glb_max_concurrency"] if region=="GLB" else plan["runtime"]["other_max_concurrency"])
    effective=min(max(1,requested),regional)
    phase_a=store.get_run(str(plan["phase10a_run_id"]))
    if not phase_a or phase_a.get("validation_phase")!=CANARY_A_PHASE:raise ConfigError("PHASE10A_VALIDATION_RUN_MISSING")
    phase_a_consumed=int(phase_a.get("post_consumed") or 0)
    if phase_a_consumed!=2:raise ConfigError("PHASE10A_CONSUMPTION_MUST_EQUAL_TWO")
    remaining=allocation["initial_search_budget"]-phase_a_consumed
    if remaining!=PHASE10B_CAP:raise ConfigError("PHASE10B_INITIAL_REMAINING_MISMATCH")
    return {"total":total,"initial":allocation["initial_search_budget"],"phase10a_consumed":phase_a_consumed,
            "phase10b":remaining,"repair":allocation["repair_reserve_budget"],"requested_concurrency":requested,"effective_concurrency":effective}


def _next_run_id(store):
    with store.connect() as c:values=[str(r[0]) for r in c.execute("select run_id from ppl_runs")]
    nums=[int(x[4:]) for x in values if x.startswith("run_") and x[4:].isdigit()]
    return f"run_{max(nums,default=0)+1:04d}"


def select_phase10b(source:List[Mapping[str,Any]],facts:Mapping[str,Mapping[str,Any]],excluded:set)->List[Dict[str,Any]]:
    eligible=[]
    for raw in source:
        r=dict(raw);fact=facts.get(str(r.get("sim_key")))
        if not r.get("selected_for_initial_search") or r.get("sim_key") in excluded:continue
        if r.get("execution_action")!="NEW_SIMULATION_REQUIRED" or r.get("cache_classification")!="CACHE_MISS":continue
        if fact:continue
        eligible.append(r)
    eligible.sort(key=lambda r:(int(r.get("selection_rank") or 10**9),-float(r.get("initial_selection_score") or 0),str(r["candidate_id"])))
    if len(eligible)<PHASE10B_CAP:raise ConfigError("PHASE10B_INSUFFICIENT_NEW_CANDIDATES")
    chosen=[]
    while len(chosen)<PHASE10B_CAP:
        remaining=[r for r in eligible if r["candidate_id"] not in {x["candidate_id"] for x in chosen}]
        if not chosen:pick=remaining[0]
        else:
            def key(r):
                return (-int(r.get("dataset_id") not in {x.get("dataset_id") for x in chosen}),
                        -int(r.get("field_id") not in {x.get("field_id") for x in chosen}),
                        -int(r.get("signal_family") not in {x.get("signal_family") for x in chosen}),
                        -int(r.get("semantic_class") not in {x.get("semantic_class") for x in chosen}),
                        int(r.get("selection_rank") or 10**9),str(r["candidate_id"]))
            remaining.sort(key=key);pick=remaining[0]
        if pick.get("signal_family") in {x.get("signal_family") for x in chosen}:raise ConfigError("PHASE10B_DUPLICATE_FAMILY_SELECTION")
        chosen.append(pick)
    return chosen


def _audit(store,run_id,event,payload):
    aid="live_"+hashlib.sha256(f"{run_id}|{event}|{time.time_ns()}".encode()).hexdigest()[:24]
    with store.connect() as c:c.execute("insert into ppl_live_execution_audits(audit_id,run_id,validation_phase,event_type,payload_json,created_at) values (?,?,?,?,?,?)",(aid,run_id,PHASE,event,_json(payload),_now()))


def create_phase10b_run(store,config,alpha_db:Path,run_id:Optional[str]=None):
    store.initialize();caps=validate_phase10b_config(store,config);source_id=str(config.plan["source_run_id"])
    source=store.get_run(source_id)
    if not source or source.get("status")!="READY_FOR_EXECUTION":raise ConfigError("PHASE10B_SOURCE_RUN_NOT_READY")
    phase_a_rows=store.load_candidates(str(config.plan["phase10a_run_id"]));excluded={x["sim_key"] for x in phase_a_rows}
    rows=store.load_candidates(source_id);facts=_alpha_facts(alpha_db,[x["sim_key"] for x in rows]);selected=select_phase10b(rows,facts,excluded)
    rid=run_id or _next_run_id(store)
    if store.get_run(rid):raise ConfigError(f"Validation run already exists: {rid}")
    store.create_run(rid,config)
    with store.connect() as c:
        c.execute("update ppl_runs set source_run_id=?,validation_phase=? where run_id=?",(source_id,PHASE,rid));cols=[r[1] for r in c.execute("pragma table_info(ppl_candidates)")]
        for rank,row in enumerate(selected,1):
            clone=dict(row);sid=row["candidate_id"];cid="phase10b_"+hashlib.sha256(f"{rid}|{sid}".encode()).hexdigest()[:24]
            clone.update(candidate_id=cid,run_id=rid,source_candidate_id=sid,lifecycle_state="PLANNED",simulation_status="NONE",simulation_freshness="UNKNOWN",cache_classification="CACHE_MISS",execution_action="NEW_SIMULATION_REQUIRED",selection_rank=rank,selected_for_initial_search=1,new_post_budget_consumed=0,alpha_id=None,result_reference_json=None,created_at=_now(),updated_at=_now())
            use=[x for x in cols if x in clone];c.execute(f"insert into ppl_candidates({','.join(use)}) values ({','.join('?' for _ in use)})",[clone[x] for x in use])
            p=c.execute("select * from ppl_candidate_provenance where candidate_id=?",(sid,)).fetchone()
            if not p:raise ConfigError("PHASE10B_SOURCE_PROVENANCE_MISSING")
            payload=json.loads(p["provenance_json"]);payload.update(source_run_id=source_id,source_candidate_id=sid,validation_phase=PHASE)
            pid="prov_"+hashlib.sha256(f"{rid}|{sid}".encode()).hexdigest()[:24]
            c.execute("insert into ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?)",(pid,cid,rid,row["sim_key"],row["context_fingerprint"],row["discovery_snapshot_id"],row["dry_run_snapshot_id"],_json(payload),_now(),_now()))
    for state in ("PLANNED","RECONCILED","READY_FOR_EXECUTION"):store.transition_run(rid,state,reason="Phase 10B validation preparation",source="PHASE10B",allowed=RUN_TRANSITIONS)
    out={"validation_run_id":rid,"source_run_id":source_id,"phase10a_run_id":config.plan["phase10a_run_id"],"phase":PHASE,"caps":caps}
    _audit(store,rid,"VALIDATION_RUN_CREATED",out);return out


def preview_phase10b(store,config,alpha_db:Path,run_id:str):
    caps=validate_phase10b_config(store,config);run=store.get_run(run_id)
    if not run or run.get("validation_phase")!=PHASE:raise ConfigError("PHASE10B_RUN_IDENTITY_OR_HASH_MISMATCH")
    compat=execution_hash_status_for_run(config,run)
    if compat["status"] not in COMPATIBLE_EXECUTION_HASH_STATUSES:raise ConfigError(f"EXECUTION_HASH_{compat['status']}: {compat['reason']}")
    rows=sorted(
        (x for x in store.load_candidates(run_id) if x.get("selected_for_initial_search")),
        key=lambda x:x.get("selection_rank") or 0,
    )
    if len(rows)!=PHASE10B_CAP or len({x["sim_key"] for x in rows})!=PHASE10B_CAP:raise ConfigError("PHASE10B_REQUIRES_FOUR_UNIQUE_CANDIDATES")
    phase_a={x["sim_key"] for x in store.load_candidates(str(config.plan["phase10a_run_id"]))}
    if phase_a & {x["sim_key"] for x in rows}:raise ConfigError("PHASE10A_SIM_KEY_REUSE_RISK")
    facts=_alpha_facts(alpha_db,[x["sim_key"] for x in rows]);counts={"CACHE_RESTORE":0,"RESUME":0,"NEW_SIMULATION":0,"HOLD":0,"RETRY":0};items=[]
    for r in rows:
        f=facts.get(r["sim_key"]);status=str((f or {}).get("status") or "NONE").upper()
        if not f:action="NEW_SIMULATION"
        elif status=="COMPLETE":action="CACHE_RESTORE"
        elif status in {"RUNNING","SUBMITTED"} and f.get("simulation_url"):action="RESUME"
        elif status=="UNCERTAIN_SUBMISSION":action="HOLD"
        else:action="RETRY"
        counts[action]+=1;items.append({"candidate_id":r["candidate_id"],"source_candidate_id":r.get("source_candidate_id"),"dataset":r.get("dataset_id"),"field":r.get("field_id"),"semantic_class":r.get("semantic_class"),"family":r.get("signal_family"),"expression":r["expression"],"sim_key":r["sim_key"],"action":action})
    out={"validation_run_id":run_id,"source_run_id":run["source_run_id"],"phase":PHASE,"candidate_count":len(rows),**counts,"estimated_new_posts":counts["NEW_SIMULATION"]+counts["RETRY"],"remaining_initial_budget":PHASE10B_CAP-int(run.get("post_consumed") or 0),"remaining_phase10_total_budget":TOTAL_CAP-int(store.get_run(str(config.plan["phase10a_run_id"])).get("post_consumed") or 0)-int(run.get("post_consumed") or 0),"caps":caps,"candidates":items}
    _audit(store,run_id,"FINAL_PREVIEW",out);return out


def execute_phase10b(store,config,machine,session,alpha_db:Path,machine_path:Path,run_id:str,allow:bool):
    if not allow:
        out=preview_phase10b(store,config,alpha_db,run_id);out.update(executed=False,reason="SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG");return out
    caps=validate_phase10b_config(store,config)
    validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_PHASE10B,
        config=config, run_id=run_id,
    )
    preview=preview_phase10b(store,config,alpha_db,run_id)
    if preview["estimated_new_posts"]!=PHASE10B_CAP:raise ConfigError("PHASE10B_FINAL_TOCTOU_REQUIRES_FOUR_CACHE_MISSES")
    rows=sorted(store.load_candidates(run_id),key=lambda x:x.get("selection_rank") or 0)
    if len(rows)>PHASE10B_CAP or caps["effective_concurrency"]>4:raise ConfigError("PHASE10B_LOW_LEVEL_HARD_CLAMP")
    source_before=store.get_run(str(config.plan["source_run_id"]));store.transition_run(run_id,"EXECUTING",reason="Explicit Phase 10B authorization",source="PHASE10B",allowed=RUN_TRANSITIONS)
    for r in rows:store.transition_candidate(r["candidate_id"],"SIMULATION_PENDING",reason="Scheduled by Phase 10B",source="PHASE10B",allowed=CANDIDATE_TRANSITIONS)
    candidates=[{"execution_action":"NEW_SIMULATION_REQUIRED","v21_candidate":_v21_candidate(r,config.target_mode)} for r in rows];by_key={r["sim_key"]:r["candidate_id"] for r in rows};stats={};started=time.time()
    with _instrument_v21(machine,store,by_key) as methods:
        orig=machine.simulate_candidates
        def delegated(*a,**kw):kw["_runtime_stats"]=stats;return orig(*a,**kw)
        machine.simulate_candidates=delegated
        try:frame=execute_with_v21(candidates,config,machine,session=session,cache_db=str(alpha_db),allow_simulation_post=True,remaining_initial_budget=PHASE10B_CAP)
        finally:machine.simulate_candidates=orig
    facts=_alpha_facts(alpha_db,by_key)
    for key,cid in by_key.items():_sync_candidate_fact(store,cid,facts.get(key) or {"sim_key":key,"status":"UNKNOWN"},source="PHASE10B_RESULT_RECONCILE")
    posts=[x for x in methods if x["method"]=="POST" and x["url"].rstrip('/').endswith('/simulations')];attempted=len(posts);uncertain=sum(str((facts.get(k) or {}).get('status') or '').upper()=="UNCERTAIN_SUBMISSION" for k in by_key);confirmed=sum(bool((facts.get(k) or {}).get('simulation_url')) and str((facts.get(k) or {}).get('status') or '').upper()!="UNCERTAIN_SUBMISSION" for k in by_key);consumed=confirmed+uncertain
    if attempted>PHASE10B_CAP or consumed>PHASE10B_CAP:raise RuntimeError("PHASE10B_POST_BUDGET_INVARIANT_BREACH")
    with store.connect() as c:c.execute("update ppl_runs set post_attempted=?,post_confirmed=?,post_uncertain=?,post_consumed=?,updated_at=? where run_id=?",(attempted,confirmed,uncertain,consumed,_now(),run_id))
    local=run_local_analysis(store,config,alpha_db,run_id,audit_source="PHASE10B");pretag=run_one_pretag_check(store,config,machine,session,run_id,local["local_pass_candidates"])
    final=store.load_candidates(run_id);target="PAUSED" if any(x.get("simulation_status") in {"RUNNING","SUBMITTED","UNCERTAIN_SUBMISSION","UNKNOWN"} for x in final) else "COMPLETED";store.transition_run(run_id,target,reason="Phase 10B execution ended",source="PHASE10B",allowed=RUN_TRANSITIONS)
    after=store.get_run(str(config.plan["source_run_id"]));
    if (source_before["status"],source_before["current_stage"],source_before["updated_at"])!=(after["status"],after["current_stage"],after["updated_at"]):raise RuntimeError("SOURCE_RUN_WAS_MODIFIED")
    results=[]
    for r in final:
        f=facts.get(r["sim_key"]) or {};results.append({"candidate_id":r["candidate_id"],"source_candidate_id":r.get("source_candidate_id"),"sim_key":r["sim_key"],"status":f.get("status"),"alpha_id":f.get("alpha_id"),"simulation_url":f.get("simulation_url"),"sharpe":f.get("sharpe"),"fitness":f.get("fitness"),"turnover":f.get("turnover"),"returns":f.get("returns"),"margin":f.get("margin"),"positions":((f.get("long_count") or 0)+(f.get("short_count") or 0)) if f.get("long_count") is not None and f.get("short_count") is not None else None,"retry_count":f.get("retry_count"),"lifecycle":r.get("lifecycle_state")})
    out={"validation_run_id":run_id,"phase":PHASE,"elapsed_seconds":time.time()-started,"post_attempted":attempted,"post_confirmed":confirmed,"post_uncertain":uncertain,"post_consumed":consumed,"repair_posts":0,"runtime_stats":stats,"http_audit":methods,"http_methods":sorted({x['method'] for x in methods}),"results":results,"dataframe_rows":len(frame),"run_status":target,"local_analysis":local,"pre_tag_check":pretag}
    _audit(store,run_id,"EXECUTION_COMPLETE",out);return out


def prepare_repair_batch(store,config,machine,alpha_db:Path,run_id:str,*,persist:bool=False)->Dict[str,Any]:
    """Preview naturally READY repairs; persist children only for explicit execution."""
    validate_phase10b_config(store,config)
    with store.connect() as c:
        consumed=int(c.execute(
            "select coalesce(sum(consumed_posts),0) from ppl_repair_plans where run_id=?",
            (run_id,),
        ).fetchone()[0])
        plans=[dict(r) for r in c.execute("select * from ppl_repair_plans where run_id=? and plan_status='READY' order by created_at,repair_plan_id",(run_id,))]
        plans=[p for p in plans if not is_retired_auto_repair_plan(p)]
    remaining=max(0,REPAIR_RESERVE-consumed)
    plans=plans[:remaining];created=[]
    for plan in plans:
        parent=next(x for x in store.load_candidates(run_id) if x["candidate_id"]==plan["parent_candidate_id"])
        spec=json.loads(plan["candidate_spec_json"]);v21=materialize_repair_candidate(parent,spec,config,machine);key=v21["sim_key"];settings=validate_full_simulation_settings(v21["settings"],context="PHASE10B_REPAIR_CHILD")
        existing=next((x for x in store.load_candidates(run_id) if x.get("sim_key")==key),None)
        if existing:
            created.append({"candidate_id":existing["candidate_id"],"repair_plan_id":plan["repair_plan_id"],"sim_key":key,"created":False,"would_create":False});continue
        cid="repair_"+hashlib.sha256(f"{run_id}|{plan['repair_signature']}".encode()).hexdigest()[:24]
        row=dict(parent);row.update(candidate_id=cid,source_candidate_id=parent.get("source_candidate_id"),expression=v21["expr"],expression_raw=v21["expr"],expression_canonical=v21["expr"],expression_hash=hashlib.sha256(v21["expr"].encode()).hexdigest(),sim_key=key,settings_json=_json(settings),settings_hash=hashlib.sha256(_json(settings).encode()).hexdigest(),context_fingerprint=hashlib.sha256(f"{parent['context_fingerprint']}|{plan['repair_signature']}".encode()).hexdigest(),direction=v21.get("direction") or spec.get("direction_override") or parent.get("direction"),transform_family=str(v21.get("operator") or parent.get("transform_family")).upper(),operator=v21.get("operator"),window=v21.get("window"),decay=v21.get("decay"),root_candidate_id=parent.get("root_candidate_id") or parent["candidate_id"],parent_candidate_id=parent["candidate_id"],parent_sim_key=parent["sim_key"],repair_path_json=_json(spec.get("repair_path",[])),repair_depth=spec.get("repair_depth",1),lifecycle_state="PLANNED",simulation_status="NONE",simulation_freshness="UNKNOWN",cache_classification="CACHE_MISS",execution_action="NEW_SIMULATION_REQUIRED",selected_for_initial_search=0,selection_rank=None,new_post_budget_consumed=0,alpha_id=None,result_reference_json=None,created_at=_now(),updated_at=_now())
        if persist:
            with store.connect() as c:
                cols=[r[1] for r in c.execute("pragma table_info(ppl_candidates)")];use=[x for x in cols if x in row];c.execute(f"insert into ppl_candidates({','.join(use)}) values ({','.join('?' for _ in use)})",[row[x] for x in use])
                pid="prov_"+hashlib.sha256(f"{run_id}|{cid}".encode()).hexdigest()[:24];payload={"candidate_stage":"REPAIR","parent_candidate_id":parent["candidate_id"],"parent_sim_key":parent["sim_key"],"repair_plan_id":plan["repair_plan_id"],"repair_signature":plan["repair_signature"],"validation_phase":PHASE}
                c.execute("insert into ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?)",(pid,cid,run_id,key,row["context_fingerprint"],row["discovery_snapshot_id"],row["dry_run_snapshot_id"],_json(payload),_now(),_now()))
        created.append({"candidate_id":cid,"repair_plan_id":plan["repair_plan_id"],"sim_key":key,"created":persist,"would_create":True})
    facts=_alpha_facts(alpha_db,[x["sim_key"] for x in created]);items=[]
    for x in created:
        f=facts.get(x["sim_key"]);status=str((f or {}).get("status") or "NONE").upper();action="NEW_SIMULATION" if not f else "CACHE_RESTORE" if status=="COMPLETE" else "RESUME" if status in {"RUNNING","SUBMITTED"} and f.get("simulation_url") else "HOLD" if status=="UNCERTAIN_SUBMISSION" else "RETRY"
        items.append({**x,"action":action})
    out={"run_id":run_id,"ready_plans":len(plans),"repair_reserve":REPAIR_RESERVE,
         "repair_consumed_before":consumed,"repair_reserve_remaining":remaining,
         "items":items,"estimated_new_posts":sum(x["action"] in {"NEW_SIMULATION","RETRY"} for x in items)};_audit(store,run_id,"REPAIR_PREVIEW",out);return out


def execute_repair_batch(store,config,machine,session,alpha_db:Path,machine_path:Path,run_id:str,allow:bool):
    preview=prepare_repair_batch(store,config,machine,alpha_db,run_id,persist=allow)
    if not allow:return {**preview,"executed":False,"reason":"SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG"}
    validate_machine_lib_hash(
        machine_path, operation=MACHINE_HASH_OPERATION_PHASE10B_REPAIR,
        config=config, run_id=run_id,
    )
    if preview["estimated_new_posts"]>preview["repair_reserve_remaining"]:raise ConfigError("PHASE10_REPAIR_BUDGET_EXCEEDED")
    ids=[x["candidate_id"] for x in preview["items"]];rows=[x for x in store.load_candidates(run_id) if x["candidate_id"] in ids];facts=_alpha_facts(alpha_db,[x["sim_key"] for x in rows])
    action_by_id={x["candidate_id"]:x["action"] for x in preview["items"]}
    runnable=[r for r in rows if action_by_id.get(r["candidate_id"]) in {"NEW_SIMULATION","RETRY"}]
    for r in runnable:store.transition_candidate(r["candidate_id"],"SIMULATION_PENDING",reason="Scheduled from READY repair plan",source="PHASE10B_REPAIR",allowed=CANDIDATE_TRANSITIONS)
    candidates=[{"execution_action":"NEW_SIMULATION_REQUIRED","v21_candidate":_v21_candidate(r,config.target_mode)} for r in runnable];by_key={r["sim_key"]:r["candidate_id"] for r in runnable};stats={};started=time.time()
    methods=[];frame=[]
    if candidates:
        with _instrument_v21(machine,store,by_key) as methods:
            orig=machine.simulate_candidates
            def delegated(*a,**kw):kw["_runtime_stats"]=stats;return orig(*a,**kw)
            machine.simulate_candidates=delegated
            try:frame=execute_with_v21(candidates,config,machine,session=session,cache_db=str(alpha_db),allow_simulation_post=True,remaining_initial_budget=preview["repair_reserve_remaining"])
            finally:machine.simulate_candidates=orig
    facts=_alpha_facts(alpha_db,[x["sim_key"] for x in rows])
    for r in rows:_sync_candidate_fact(store,r["candidate_id"],facts.get(r["sim_key"]) or {"sim_key":r["sim_key"],"status":"UNKNOWN"},source="PHASE10B_REPAIR_RECONCILE")
    posts=[x for x in methods if x["method"]=="POST" and x["url"].rstrip('/').endswith('/simulations')];uncertain=sum(str((facts.get(k) or {}).get('status') or '').upper()=="UNCERTAIN_SUBMISSION" for k in by_key);confirmed=sum(bool((facts.get(k) or {}).get('simulation_url')) and str((facts.get(k) or {}).get('status') or '').upper()!="UNCERTAIN_SUBMISSION" for k in by_key);consumed=confirmed+uncertain
    if len(posts)>REPAIR_RESERVE or consumed>REPAIR_RESERVE:raise RuntimeError("PHASE10_REPAIR_BUDGET_INVARIANT_BREACH")
    with store.connect() as c:
        for item in preview["items"]:
            f=facts.get(item["sim_key"]) or {};used=int(str(f.get("status") or "").upper() in {"COMPLETE","RUNNING","SUBMITTED","UNCERTAIN_SUBMISSION"});c.execute("update ppl_repair_plans set plan_status=?,committed_posts=?,consumed_posts=?,updated_at=? where repair_plan_id=?",("EXECUTED" if used else "READY",used,used,_now(),item["repair_plan_id"]))
    local=run_local_analysis(store,config,alpha_db,run_id,audit_source="PHASE10B_REPAIR",candidate_ids=ids)
    results=[]
    for r in [x for x in store.load_candidates(run_id) if x["candidate_id"] in ids]:
        f=facts.get(r["sim_key"]) or {};results.append({"candidate_id":r["candidate_id"],"parent_candidate_id":r.get("parent_candidate_id"),"sim_key":r["sim_key"],"status":f.get("status"),"alpha_id":f.get("alpha_id"),"simulation_url":f.get("simulation_url"),"sharpe":f.get("sharpe"),"fitness":f.get("fitness"),"turnover":f.get("turnover"),"returns":f.get("returns"),"margin":f.get("margin"),"positions":((f.get("long_count") or 0)+(f.get("short_count") or 0)) if f.get("long_count") is not None and f.get("short_count") is not None else None,"lifecycle":r.get("lifecycle_state")})
    out={"run_id":run_id,"phase":PHASE,"kind":"REPAIR","elapsed_seconds":time.time()-started,"post_attempted":len(posts),"post_confirmed":confirmed,"post_uncertain":uncertain,"post_consumed":consumed,"runtime_stats":stats,"http_audit":methods,"results":results,"local_analysis":local};_audit(store,run_id,"REPAIR_EXECUTION_COMPLETE",out);return out
