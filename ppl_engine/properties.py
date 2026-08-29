"""Conservative versioned Alpha property parser and manual property gate."""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from typing import Any,Dict,Mapping
PROPERTY_PARSER_VERSION=2;TAG_ALIAS_VERSION=2
def parse_alpha_properties(payload:Mapping[str,Any],*,evidence_source="SYNTHETIC_TEST")->Dict[str,Any]:
    props=payload.get("properties");regular=payload.get("regular");top_tags=payload.get("tags")
    if isinstance(top_tags,list):known=True;tags=top_tags
    elif isinstance(props,Mapping) and isinstance(props.get("tags"),list):known=True;tags=props.get("tags")
    else:known=False;tags=[]
    if isinstance(regular,Mapping) and "description" in regular:desc=regular.get("description")
    elif "description" in payload:desc=payload.get("description")
    else:desc=props.get("description") if isinstance(props,Mapping) else None
    normalized=[]
    if known:
        for x in tags:
            if isinstance(x,Mapping) and set(x).intersection({"name","id"}):normalized.append(str(x.get("name") or x.get("id")))
            elif isinstance(x,str):normalized.append(x)
    tag="PRESENT" if known and "PowerPoolSelected" in normalized else "ABSENT" if known else "UNKNOWN"
    submission="SUBMITTED" if isinstance(props,Mapping) and props.get("submissionStatus")=="SUBMITTED" else "UNKNOWN"
    raw=json.dumps(payload,ensure_ascii=False,sort_keys=True);sid="prop_"+hashlib.sha256(raw.encode()).hexdigest()[:24]
    return {"snapshot_id":sid,"description_present":bool(str(desc or '').strip()),"description_text":desc,"description_length":len(str(desc or '')),"tags_raw":tags if known else None,"normalized_tags":normalized,"power_pool_selected_status":tag,"submission_status":submission,"raw_payload":payload,"property_parser_version":PROPERTY_PARSER_VERSION,"tag_alias_version":TAG_ALIAS_VERSION,"evidence_source":evidence_source,"captured_at":datetime.now(timezone.utc).isoformat()}
def evaluate_manual_properties_gate(candidate:Mapping[str,Any],description_validation:Mapping[str,Any],snapshot:Mapping[str,Any])->Dict[str,Any]:
    reasons=[];status=snapshot.get("power_pool_selected_status")
    if description_validation.get("validation_status")!="VALID" or not snapshot.get("description_present"):reasons.append("DESCRIPTION_REQUIRED")
    elif snapshot.get("description_length") is not None and int(snapshot.get("description_length"))<int(description_validation.get("minimum_length",100)):reasons.append("DESCRIPTION_TOO_SHORT")
    if status=="ABSENT":reasons.append("ADD_POWERPOOLSELECTED")
    elif status=="UNKNOWN":reasons.append("TAG_STRUCTURE_NEEDS_CONFIRMATION")
    conflict=bool(description_validation.get("property_description_conflict"));
    if conflict:reasons.append("PROPERTY_DESCRIPTION_CONFLICT")
    gate="PASS" if not reasons else "UNKNOWN" if status=="UNKNOWN" else "PENDING"
    return {"gate":"MANUAL_PROPERTIES_GATE","status":gate,"reasons":reasons,"next_manual_action":None if gate=="PASS" else reasons[0],"would_transition":"PPL_TAGGED" if gate=="PASS" else None,"would_schedule":"FINAL_CHECK_PENDING" if gate=="PASS" else None,"final_check_required":gate=="PASS"}
def refresh_preview(candidate,validation,snapshot,manual_evidence=None):
    gate=evaluate_manual_properties_gate(candidate,validation,snapshot);manual_claim=any(x.get("evidence_type")=="POWERPOOL_TAG" and json.loads(x.get("payload_json","{}") or "{}").get("status")=="PRESENT" for x in (manual_evidence or []));conflict=manual_claim and snapshot.get("power_pool_selected_status")=="ABSENT"
    if conflict:gate={**gate,"status":"PENDING","would_transition":None,"would_schedule":None,"final_check_required":False,"reasons":gate["reasons"]+["MANUAL_API_CONFLICT"]}
    return {"current_lifecycle":candidate.get("lifecycle_state"),"property_snapshot":snapshot,"property_gate":gate,"manual_api_conflict":conflict,"refresh_status":"NEEDS_CONFIRMATION" if snapshot.get("power_pool_selected_status")=="UNKNOWN" else "READY" if gate["status"]=="PASS" else "INCOMPLETE_MANUAL_PROPERTIES","network_requests":0,"check_requests":0,"writes":0}
def resolve_refresh_candidate(candidates,alpha_id,run_id=None):
    found=[x for x in candidates if x.get("alpha_id")==alpha_id and (run_id is None or x.get("run_id")==run_id)]
    if not found:raise ValueError("REFRESH_CANDIDATE_NOT_FOUND")
    if len(found)>1:raise ValueError("REFRESH_CANDIDATE_AMBIGUOUS_REQUIRE_RUN_ID")
    return found[0]
def final_check_schedule_allowed(candidate):return str(candidate.get("lifecycle_state"))=="PPL_TAGGED"
