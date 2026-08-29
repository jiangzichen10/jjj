"""Phase 7 low-token, offline summary and rebuildable derived analytics."""
from __future__ import annotations
import json
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
from typing import Any,Dict,Optional
from .atomic import atomic_write_json
from .family import FAMILY_SIMILARITY_VERSION,build_family_index,build_reference_pool,evidence_level
from .priority import PRIORITY_SCORE_VERSION,diversity_rerank,score_candidate
SUMMARY_VERSION=1

def _now():return datetime.now(timezone.utc).isoformat()
def _metric(c):
    x=c.get("available_result_json") or {}
    if isinstance(x,str):
        try:x=json.loads(x) if x else {}
        except:x={}
    return x or {}
def build_analytics(store,run_id,config,persist=False):
    candidates=store.load_candidates(run_id); refs=build_reference_pool(candidates,store.load_reference_pool(),store.load_manual_evidence())
    families,members=build_family_index(candidates,refs); fby={x["family_id"]:x for x in families};mby={x["candidate_id"]:x for x in members};cby={x["candidate_id"]:x for x in candidates}
    scores=[score_candidate(c,mby[c["candidate_id"]],fby[mby[c["candidate_id"]]["family_id"]]) for c in candidates]
    limits=config.rules["summary_limits"]; ranked=diversity_rerank(scores,cby,fby,mby,limits["diversity"],int(limits["top_candidates"]))
    if persist:store.save_derived(run_id,families,members,scores)
    return {"candidates":candidates,"families":families,"members":members,"references":refs,"scores":scores,"ranked":ranked,"maps":(cby,fby,mby)}
def _card(cid,a):
    cby,fby,mby=a["maps"];c=cby[cid];m=_metric(c);s=next(x for x in a["scores"] if x["candidate_id"]==cid);mem=mby[cid]
    return {"candidate_id":cid,"alpha_id":c.get("alpha_id"),"family_id":mem["family_id"],"dataset":c.get("dataset_id"),"field":c.get("field_id"),"semantic_class":c.get("semantic_class"),"transform":c.get("transform_family"),"window":c.get("window"),"direction":c.get("direction"),"lifecycle_state":c.get("lifecycle_state"),"evidence_level":s["evidence_level"],"sharpe":m.get("sharpe"),"fitness":m.get("fitness"),"turnover":m.get("turnover"),"margin":m.get("margin"),"priority_score":s["research_priority_score"],"priority_confidence":s["priority_confidence"],"primary_failure":c.get("primary_failure"),"repair_depth":c.get("repair_depth"),"family_role":mem["family_role"],"similarity_risk":fby[mem["family_id"]]["local_similarity_risk"]}
def build_phase7_summary(store,config,run_id=None,persist_derived=False):
    run=(store.status(run_id).get("runs") or [None])[0];rid=run.get("run_id") if run else run_id
    if not rid:return build_foundation_summary(store,config,run_id)
    if not store.load_candidates(rid):
        value=build_foundation_summary(store,config,rid);value.update({"run_status":run.get("status"),"current_stage":run.get("current_stage"),"execution_hash":config.execution_hash});return value
    a=build_analytics(store,rid,config,persist_derived); candidates=a["candidates"]; limits=config.rules["summary_limits"]
    top_ids=[x["candidate_id"] for x in a["ranked"]]
    score_by={x["candidate_id"]:x for x in a["scores"]}
    near_ids=[c["candidate_id"] for c in sorted(candidates,key=lambda x:(-score_by[x["candidate_id"]]["research_priority_score"],int(x.get("repair_depth") or 0),x["candidate_id"])) if c.get("primary_failure")=="SHARPE_NEAR_PASS" or c.get("lifecycle_state")=="NEAR_PASS"][:int(limits["near_pass"])]
    lifecycle_sections={name:[c["candidate_id"] for c in candidates if c.get("lifecycle_state")==state][:int(limits.get(name,10))] for name,state in {"pre_tag_finalists":"PRE_TAG_FINALIST","awaiting_manual_properties":"AWAITING_MANUAL_PROPERTIES","ppl_tagged":"PPL_TAGGED","final_check_pass":"FINAL_CHECK_PASS","ready_for_submit":"READY_FOR_MANUAL_SUBMIT","submitted":"SUBMITTED"}.items()}
    structural_ids=[c["candidate_id"] for c in candidates if c.get("primary_failure") in {"STRUCTURAL_CORRELATION_FAIL","STRUCTURAL_CORRELATION_RISK"} or c.get("lifecycle_state")=="STRUCTURAL_FAIL"][:int(limits["stopped"])]
    card_ids=list(dict.fromkeys(top_ids+near_ids+structural_ids+sum(lifecycle_sections.values(),[])))
    cards={cid:_card(cid,a) for cid in card_ids}
    failures=Counter(str(c.get("primary_failure")) for c in candidates if c.get("primary_failure")); fam_by_fail=defaultdict(set); reps=defaultdict(list)
    for c in candidates:
        if c.get("primary_failure"): fam_by_fail[c["primary_failure"]].add(a["maps"][2][c["candidate_id"]]["family_id"]);reps[c["primary_failure"]].append(c["candidate_id"])
    important=[{"failure_type":k,"count":v,"affected_families":len(fam_by_fail[k]),"representative_candidates":reps[k][:3]} for k,v in failures.most_common(int(limits["important_failures"]))]
    op=[]
    with store.connect() as conn:
        op=[{"operator":r[0],"status":r[1],"blocked_repair_count":r[2]} for r in conn.execute("SELECT operator_name,status,0 FROM ppl_operator_capabilities WHERE status IN ('UNVERIFIED','NEEDS_REVALIDATION') ORDER BY operator_name LIMIT ?",(int(limits["operator_validation_needed"]),))]
        repairs=dict(conn.execute("SELECT plan_status,count(*) FROM ppl_repair_plans WHERE run_id=? GROUP BY plan_status",(rid,)).fetchall())
        last_check=conn.execute("SELECT max(updated_at) FROM ppl_check_sessions WHERE run_id=?",(rid,)).fetchone()[0]
        last_manual=conn.execute("SELECT max(created_at) FROM ppl_manual_evidence").fetchone()[0]
        discovery=conn.execute("SELECT max(created_at) FROM ppl_discovery_snapshots").fetchone()[0]
        manual_types=dict(conn.execute("SELECT evidence_type,count(*) FROM ppl_manual_evidence GROUP BY evidence_type").fetchall());manual_conflicts=conn.execute("SELECT count(*) FROM ppl_manual_evidence WHERE conflict_state IS NOT NULL AND conflict_state NOT IN ('','NONE')").fetchone()[0]
        interpretation_conflicts=[{"evidence_id":r[0],"alpha_id":r[1],"conflict_state":r[2]} for r in conn.execute("SELECT evidence_id,alpha_id,conflict_state FROM ppl_manual_evidence WHERE conflict_state='INTERPRETATION_CONFLICT' ORDER BY created_at LIMIT 5")]
        desc_stats={"drafted":conn.execute("SELECT count(*) FROM ppl_descriptions").fetchone()[0],"validated":conn.execute("SELECT count(*) FROM ppl_descriptions WHERE validation_status='VALID'").fetchone()[0],"needs_manual":conn.execute("SELECT count(*) FROM ppl_descriptions WHERE validation_status='NEEDS_MANUAL_DESCRIPTION'").fetchone()[0],"awaiting_properties":conn.execute("SELECT count(*) FROM ppl_candidates WHERE run_id=? AND lifecycle_state='AWAITING_MANUAL_PROPERTIES'",(rid,)).fetchone()[0]}
        prop_stats={"awaiting_description":conn.execute("SELECT count(*) FROM ppl_property_snapshots WHERE description_present=0").fetchone()[0],"awaiting_tag":conn.execute("SELECT count(*) FROM ppl_property_snapshots WHERE power_pool_selected_status='ABSENT'").fetchone()[0],"ready":conn.execute("SELECT count(*) FROM ppl_property_snapshots WHERE power_pool_selected_status='PRESENT' AND description_present=1").fetchone()[0],"conflicts":manual_conflicts}
    selected=[c for c in candidates if c.get("selected_for_initial_search")]; actions=Counter(c.get("execution_action") for c in selected); levels=Counter(evidence_level(c) for c in candidates)
    top_fams=sorted(a["families"],key=lambda f:(-f["complete_count"],-f["candidate_count"],f["family_id"]))[:int(limits["top_families"])]
    fam_cards=[{k:f.get(k) for k in ("family_id","dataset_id","field_id","semantic_class","transform_family","direction","candidate_count","complete_count","representative_candidate_id","best_sharpe","best_turnover","family_status","local_similarity_risk","reference_overlap")} for f in top_fams]
    summary={"schema_version":1,"summary_version":SUMMARY_VERSION,"priority_score_version":PRIORITY_SCORE_VERSION,"family_similarity_version":FAMILY_SIMILARITY_VERSION,"next_plan_schema_version":1,"generated_at":_now(),"run_id":rid,"run_status":run.get("status"),"current_stage":run.get("current_stage"),"runner_goal":"PPL","target_mode":config.target_mode,"atom_constraint_active":config.atom_constraint_active,"execution_hash":config.execution_hash,"theme":{"preset_id":config.rules["current_theme"]["preset_id"],"display_name":config.rules["current_theme"]["display_name"],"live_resolved":bool(last_check)},"settings":config.plan["simulation_settings"],"budget":config.plan["budgets"],"stats":{"candidates":len(candidates),"selected_initial":len(selected),"evidence_levels":dict(levels),"cache_complete":actions.get("CACHE_RESTORE",0),"new_simulation_required":actions.get("NEW_SIMULATION_REQUIRED",0)},"family_stats":{"total":len(a["families"]),"by_dataset":dict(Counter(f["dataset_id"] for f in a["families"])),"by_semantic_class":dict(Counter(f["semantic_class"] for f in a["families"])),"by_transform_family":dict(Counter(f["transform_family"] for f in a["families"])),"by_direction":dict(Counter(f["direction"] for f in a["families"])),"by_vector_reducer":dict(Counter(f["vector_reducer"] for f in a["families"]))},"top_candidates_evidence_coverage":{"complete_evidence_candidates":sum(v for k,v in levels.items() if k!="PLANNED_ONLY"),"planned_only_candidates":levels.get("PLANNED_ONLY",0)},"candidate_cards":cards,"top_candidates":[{"candidate_id":x,"reason":"DIVERSITY_RERANK"} for x in top_ids],"top_families":fam_cards,"near_pass":[{"candidate_id":x,"reason":"SHARPE_NEAR_PASS"} for x in near_ids],**{k:[{"candidate_id":x} for x in v] for k,v in lifecycle_sections.items()},"structural_stops":[{"candidate_id":x,"reason":"STRUCTURAL_EVIDENCE"} for x in structural_ids],"important_failures":important,"unknown_checks":store.check_summary(rid),"repair_summary":{"planned":repairs.get("PLANNED",0),"blocked_operator_validation":repairs.get("BLOCKED_OPERATOR_VALIDATION",0),"blocked_budget":repairs.get("BLOCKED_BUDGET",0),"dispatched":repairs.get("DISPATCHED",0),"executed":repairs.get("EXECUTED",0),"accepted":repairs.get("EVALUATED_ACCEPT",0),"rejected":repairs.get("EVALUATED_REJECT",0)},"operator_validation_needed":op,"reference_pool":{"count":len(a["references"]),"by_source":dict(Counter(r.get("source") for r in a["references"]))},"performance_comparison":{"available":False,"source":None,"before":None,"after":None,"change":None,"captured_at":None},"manual_evidence":{"count":sum(manual_types.values()),"latest_at":last_manual,"types":manual_types,"conflicts":manual_conflicts},"data_freshness":{"simulation_cache_as_of":Path(config.project_dir/"alpha_results.db").stat().st_mtime_ns,"last_discovery_at":discovery,"last_check_at":last_check,"last_manual_evidence_at":last_manual},"live_check_resolved":bool(last_check),"next_action_context":{"run_state":run.get("status"),"selected_initial":len(selected),"cache_complete":actions.get("CACHE_RESTORE",0),"new_simulation_required":actions.get("NEW_SIMULATION_REQUIRED",0),"initial_post_budget_consumed":0,"repair_reserve":48,"blocking_issue":"NONE","recommended_system_action":"EXECUTE_INITIAL_SEARCH_WHEN_LIVE_EXECUTION_IS_APPROVED"}}
    summary["description_stats"]=desc_stats;summary["manual_properties_stats"]=prop_stats
    summary["evidence_interpretation_conflict_count"]=len(interpretation_conflicts);summary["evidence_interpretation_conflicts"]=interpretation_conflicts
    return summary
def build_foundation_summary(store,config,run_id=None):
    manual=store.load_manual_evidence() if store.path.exists() else []
    conflicts=[{"evidence_id":x.get("evidence_id"),"alpha_id":x.get("alpha_id"),"conflict_state":x.get("conflict_state")} for x in manual if x.get("conflict_state")=="INTERPRETATION_CONFLICT"][:5]
    return {"schema_version":1,"summary_version":SUMMARY_VERSION,"priority_score_version":PRIORITY_SCORE_VERSION,"family_similarity_version":FAMILY_SIMILARITY_VERSION,"next_plan_schema_version":1,"run_id":run_id,"runner_goal":"PPL","target_mode":config.target_mode,"atom_constraint_active":config.atom_constraint_active,"stats":{"planned_candidates":0},"repair_summary":{"planned":0},"performance_comparison":{"available":False,"source":None},"manual_evidence":{"count":len(manual)},"evidence_interpretation_conflict_count":len(conflicts),"evidence_interpretation_conflicts":conflicts,"description_stats":{"drafted":0,"validated":0,"needs_manual":0,"awaiting_properties":0},"manual_properties_stats":{"awaiting_description":0,"awaiting_tag":0,"ready":0,"conflicts":0},"live_check_resolved":False,"next_action_context":{"phase":"FOUNDATION","implementation_status":"PHASE_1_ONLY"}}
def write_foundation_summary(path,store,config,run_id=None,persist_derived=False):
    value=build_phase7_summary(store,config,run_id,persist_derived);atomic_write_json(path,value);return value
