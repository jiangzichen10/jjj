"""Versioned, transparent research priority and diversity reranking."""
from __future__ import annotations
import json
from collections import Counter
from typing import Any,Dict,Mapping,Sequence
from .family import evidence_level
PRIORITY_SCORE_VERSION=1

def _metrics(c):
    x=c.get("available_result") or c.get("available_result_json") or {}
    if isinstance(x,str):
        try:x=json.loads(x) if x else {}
        except:x={}
    return x or {}
def score_candidate(c:Mapping[str,Any],member:Mapping[str,Any],family:Mapping[str,Any])->Dict[str,Any]:
    level=evidence_level(c); m=_metrics(c); available=level!="PLANNED_ONLY"
    components={}; status={}
    if available:
        sharpe=m.get("sharpe"); turnover=m.get("turnover")
        components["gate_performance"]=min(30,max(0,(float(sharpe or 0)/1.0)*20 + (10 if turnover is not None and .2<=float(turnover)<=.7 else 0)))
        components["simulation_quality"]=min(15,max(0,float(m.get("fitness") or 0)*8+float(m.get("margin") or 0)*1000))
        status.update({"gate_performance":"AVAILABLE","simulation_quality":"AVAILABLE"})
    else:
        components.update({"gate_performance":None,"simulation_quality":None});status.update({"gate_performance":"UNAVAILABLE","simulation_quality":"UNAVAILABLE"})
    state=str(c.get("lifecycle_state")); check=20 if state in {"PRE_TAG_CHECK_PASS","FINAL_CHECK_PASS"} else 10 if state in {"PRE_TAG_CHECK_COMPLETE","FINAL_CHECK_COMPLETE"} else None
    components["check_health"]=check;status["check_health"]="AVAILABLE" if check is not None else "UNAVAILABLE"
    components["novelty"]=20-(10 if family.get("reference_overlap") else 0)-(5 if family.get("local_similarity_risk") in {"HIGH","VERY_HIGH"} else 0);status["novelty"]="AVAILABLE"
    conf={"HIGH":5,"MEDIUM":3,"LOW":0}.get(str(c.get("classification_confidence")),0);components["classification_confidence"]=conf;status["classification_confidence"]="AVAILABLE"
    repair=max(0,10-2*int(c.get("repair_depth") or 0));components["repair_efficiency"]=repair;status["repair_efficiency"]="AVAILABLE"
    penalties={"structural_stop":35 if c.get("primary_failure") in {"STRUCTURAL_CORRELATION_FAIL","STRUCTURAL_CORRELATION_RISK"} or member.get("family_role") in {"STRUCTURAL_STOP","STRUCTURAL_RISK_STOP"} else 0,"reference_overlap":10 if family.get("reference_overlap") else 0,"repair_depth":2*int(c.get("repair_depth") or 0),"planned_missing_evidence":20 if not available else 0,"low_confidence":5 if str(c.get("classification_confidence"))=="LOW" else 0}
    score=sum(v for v in components.values() if v is not None)-sum(penalties.values());score=max(0,min(100,round(score,2)))
    confidence="HIGH" if level in {"PRE_TAG_CHECK_PASS","PPL_TAGGED","FINAL_CHECK_COMPLETE","FINAL_CHECK_PASS","SUBMITTED"} else "MEDIUM" if available else "LOW"
    return {"candidate_id":c.get("candidate_id"),"research_priority_score":score,"priority_confidence":confidence,"priority_score_version":PRIORITY_SCORE_VERSION,"components":components,"score_component_status":status,"penalties":penalties,"evidence_level":level}

def diversity_rerank(scored:Sequence[Mapping[str,Any]],candidates:Mapping[str,Mapping[str,Any]],families:Mapping[str,Mapping[str,Any]],members:Mapping[str,Mapping[str,Any]],limits:Mapping[str,int],top_n:int):
    out=[]; counts={k:Counter() for k in ("dataset","field","family","semantic")}
    for s in sorted(scored,key=lambda x:(-x["research_priority_score"],str(x["candidate_id"]))):
        c=candidates[s["candidate_id"]]; mem=members[s["candidate_id"]]; keys={"dataset":c.get("dataset_id"),"field":c.get("field_id"),"family":mem.get("family_id"),"semantic":c.get("semantic_class")}
        if any(counts[k][v]>=int(limits.get("max_top_per_"+k,top_n)) for k,v in keys.items()):continue
        out.append(dict(s));[counts[k].update([v]) for k,v in keys.items()]
        if len(out)>=top_n:break
    return out
