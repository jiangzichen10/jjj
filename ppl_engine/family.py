"""Stable signal-family indexing and explainable local similarity (never PP correlation)."""
from __future__ import annotations
import hashlib, json
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

FAMILY_SIMILARITY_VERSION=1
ROLES={"REPRESENTATIVE","STRONG_VARIANT","REDUNDANT_VARIANT","HISTORICAL_DUPLICATE_RISK","STRUCTURAL_STOP","STRUCTURAL_RISK_STOP","UNASSESSED"}

def family_components(c: Mapping[str,Any])->Dict[str,str]:
    return {k:str(c.get(k) or "UNKNOWN").upper() for k in ("dataset_id","field_id","vector_reducer","direction","transform_family")}
def family_fingerprint(c: Mapping[str,Any])->str:
    return hashlib.sha256(json.dumps(family_components(c),sort_keys=True,separators=(",",":")).encode()).hexdigest()
def family_id(c: Mapping[str,Any])->str: return "fam_"+family_fingerprint(c)[:24]

def evidence_level(c: Mapping[str,Any])->str:
    state=str(c.get("lifecycle_state") or "PLANNED")
    if state=="SUBMITTED": return "SUBMITTED"
    if state=="FINAL_CHECK_PASS": return "FINAL_CHECK_PASS"
    if state=="FINAL_CHECK_COMPLETE": return "FINAL_CHECK_COMPLETE"
    if state=="PPL_TAGGED": return "PPL_TAGGED"
    if state=="PRE_TAG_CHECK_PASS": return "PRE_TAG_CHECK_PASS"
    if state=="PRE_TAG_CHECK_COMPLETE": return "PRE_TAG_CHECK_COMPLETE"
    if state=="LOCAL_PRE_GATE_PASS": return "LOCAL_GATE_EVALUATED"
    if str(c.get("simulation_status")).upper()=="COMPLETE" or state=="SIMULATION_COMPLETE": return "SIMULATION_COMPLETE"
    return "PLANNED_ONLY"

EVIDENCE_RANK={"SUBMITTED":9,"FINAL_CHECK_PASS":8,"FINAL_CHECK_COMPLETE":7,"PPL_TAGGED":6,"PRE_TAG_CHECK_PASS":5,"PRE_TAG_CHECK_COMPLETE":4,"LOCAL_GATE_EVALUATED":3,"SIMULATION_COMPLETE":2,"PLANNED_ONLY":1}

def local_family_similarity(a: Mapping[str,Any],b: Mapping[str,Any],reference_source=None)->Dict[str,Any]:
    ca,cb=family_components(a),family_components(b); same={"same_dataset":ca["dataset_id"]==cb["dataset_id"],"same_field":ca["field_id"]==cb["field_id"],"same_vector_reducer":ca["vector_reducer"]==cb["vector_reducer"],"same_direction":ca["direction"]==cb["direction"],"same_transform_family":ca["transform_family"]==cb["transform_family"]}
    wa,wb=a.get("window"),b.get("window"); distance=abs(int(wa)-int(wb)) if wa is not None and wb is not None else None
    points=sum((20,30,10,15,20)[i] for i,v in enumerate(same.values()) if v)+(5 if distance is not None and distance<=2 else 0)+(10 if reference_source else 0)
    risk="VERY_HIGH" if points>=95 else "HIGH" if points>=80 else "MEDIUM" if points>=50 else "LOW"
    return {"kind":"local_family_similarity","version":FAMILY_SIMILARITY_VERSION,"similarity_risk":risk,"heuristic_score":points,"similarity_components":{**same,"window_distance":distance,"reference_source":reference_source}}

def build_reference_pool(candidates:Sequence[Mapping[str,Any]],stored:Sequence[Mapping[str,Any]]=(),manual:Sequence[Mapping[str,Any]]=())->List[Dict[str,Any]]:
    allowed={"PRE_TAG_FINALIST":"CURRENT_FINALIST","PPL_TAGGED":"PPL_TAGGED","SUBMITTED":"SUBMITTED_PPL"}; out=[]
    for c in candidates:
        state=str(c.get("lifecycle_state")); source=allowed.get(state)
        if source: out.append({"reference_id":"ref_"+family_fingerprint(c)[:16]+str(c.get("candidate_id"))[-6:],"family_fingerprint":family_fingerprint(c),"candidate_id":c.get("candidate_id"),"alpha_id":c.get("alpha_id"),"source":source,"evidence_source":"PPL_RUNNER_DB","run_id":c.get("run_id"),"confidence":"CONFIRMED"})
    for row in stored:
        if row.get("source") in {"CURRENT_FINALIST","HISTORICAL_FINALIST","PPL_TAGGED","SUBMITTED_PPL","USER_REFERENCE","MANUAL_EVIDENCE"}:
            item=dict(row)
            if not item.get("family_fingerprint") and item.get("signal_family"):
                parts=str(item["signal_family"]).split("/")
                if len(parts)==5: item["family_fingerprint"]=family_fingerprint(dict(zip(("dataset_id","field_id","vector_reducer","direction","transform_family"),parts)))
            out.append(item)
    for row in manual:
        if row.get("evidence_type") in {"REFERENCE_ALPHA","PPL_REFERENCE"}: out.append({**row,"source":"MANUAL_EVIDENCE","confidence":"MANUAL"})
    return out

def _metrics(c):
    raw=c.get("available_result") or c.get("available_result_json") or {}
    if isinstance(raw,str):
        try: raw=json.loads(raw) if raw else {}
        except json.JSONDecodeError: raw={}
    return raw or {}

def family_status(items):
    states={str(x.get("lifecycle_state")) for x in items}; failures={str(x.get("primary_failure")) for x in items}
    if "FINAL_CHECK_PASS" in states:return "FINAL_PASS"
    if "PPL_TAGGED" in states:return "PPL_TAGGED"
    if "PRE_TAG_FINALIST" in states:return "PRE_TAG_FINALIST"
    if "STRUCTURAL_CORRELATION_FAIL" in failures and all(x in {"STRUCTURAL_CORRELATION_FAIL","None",""} for x in failures):return "STRUCTURAL_STOP"
    if "STRUCTURAL_CORRELATION_RISK" in failures and all(x in {"STRUCTURAL_CORRELATION_RISK","None",""} for x in failures):return "STRUCTURAL_RISK_STOP"
    if "NEAR_PASS" in states or "SHARPE_NEAR_PASS" in failures:return "NEAR_PASS"
    if any(evidence_level(x)!="PLANNED_ONLY" for x in items):return "ACTIVE"
    return "UNTESTED"

def build_family_index(candidates:Sequence[Mapping[str,Any]],references=())->Tuple[List[Dict[str,Any]],List[Dict[str,Any]]]:
    groups=defaultdict(list)
    for c in candidates: groups[family_id(c)].append(dict(c))
    families=[]; members=[]; ref_fps={r.get("family_fingerprint") for r in references}
    for fid,items in sorted(groups.items()):
        ordered=sorted(items,key=lambda x:(-EVIDENCE_RANK[evidence_level(x)],int(x.get("repair_depth") or 0),str(x.get("candidate_id"))))
        rep=ordered[0]; complete=[x for x in items if evidence_level(x)!="PLANNED_ONLY"]
        metrics=[_metrics(x) for x in complete]; windows=sorted({x.get("window") for x in items if x.get("window") is not None})
        adjacent=any(b-a<=2 for a,b in zip(windows,windows[1:])); overlap=family_fingerprint(rep) in ref_fps
        risk="VERY_HIGH" if overlap and adjacent else "HIGH" if overlap or adjacent else "LOW"
        comp=family_components(rep)
        fam={"family_id":fid,"family_fingerprint":family_fingerprint(rep),**{k.lower():v for k,v in comp.items()},"semantic_class":rep.get("semantic_class"),"candidate_count":len(items),"complete_count":len(complete),"representative_candidate_id":rep.get("candidate_id"),"representative_type":"RESULT_REPRESENTATIVE" if complete else "PLANNING_REPRESENTATIVE","representative_evidence_level":evidence_level(rep),"best_sharpe":max((m.get("sharpe") for m in metrics if m.get("sharpe") is not None),default=None),"best_turnover":max((m.get("turnover") for m in metrics if m.get("turnover") is not None),default=None),"family_status":family_status(items),"local_similarity_risk":risk,"reference_overlap":overlap,"adjacent_windows":adjacent}
        families.append(fam)
        for i,x in enumerate(ordered):
            role="REPRESENTATIVE" if i==0 else "HISTORICAL_DUPLICATE_RISK" if overlap else "REDUNDANT_VARIANT" if adjacent else "STRONG_VARIANT"
            if x.get("primary_failure")=="STRUCTURAL_CORRELATION_FAIL": role="STRUCTURAL_STOP"
            if x.get("primary_failure")=="STRUCTURAL_CORRELATION_RISK": role="STRUCTURAL_RISK_STOP"
            members.append({"family_id":fid,"candidate_id":x.get("candidate_id"),"family_role":role,"evidence_level":evidence_level(x)})
    return families,members
