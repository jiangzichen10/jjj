"""Deterministic, evidence-bounded description drafting and validation."""
from __future__ import annotations
import re,hashlib,json
from datetime import datetime,timezone
from typing import Any,Dict,Mapping,Optional
DESCRIPTION_TEMPLATE_VERSION=1
ALLOWED_STATES={"PRE_TAG_FINALIST","DESCRIPTION_DRAFT","NEEDS_MANUAL_DESCRIPTION"}
CALL_RE=re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")

def _operators(expr):return list(dict.fromkeys(CALL_RE.findall(str(expr))))
def _now():return datetime.now(timezone.utc).isoformat()
def draft_description(candidate:Mapping[str,Any],field_metadata:Mapping[str,Any],rules:Mapping[str,Any],*,formal=False)->Dict[str,Any]:
    state=str(candidate.get("lifecycle_state"))
    if formal and state not in ALLOWED_STATES:raise ValueError("DESCRIPTION_NOT_ALLOWED_FOR_STATE")
    field=str(candidate.get("field_id") or "");desc=str(field_metadata.get("description") or "").strip();semantic=str(candidate.get("semantic_class") or "UNKNOWN")
    expr=str(candidate.get("expression") or candidate.get("expression_canonical") or "");ops=_operators(expr);direction=str(candidate.get("direction") or "NORMAL");window=candidate.get("window");reducer=str(candidate.get("vector_reducer") or "IDENTITY")
    idea=f"This alpha uses the observable field {field}"
    if direction=="REVERSE":idea+=" in the reverse direction"
    idea+=f" and applies {candidate.get('transform_family') or 'the recorded transform'}"
    if window is not None:idea+=f" over {window} periods"
    idea+=" to form the signal."
    if desc and semantic!="UNKNOWN":data=f"The data rationale is limited to the supplied metadata: {desc} The recorded semantic class is {semantic}."
    else:data="The available field metadata is insufficient to state a reliable economic interpretation. Manual description is required."
    parts=[]
    if direction=="REVERSE":parts.append("The unary negative sign reverses the recorded signal direction.")
    rationales={"ts_mean":"uses a time-series mean to reduce period-to-period noise","rank":"converts values to a cross-sectional relative ranking","zscore":"standardizes values cross-sectionally","ts_delta":"focuses on the recorded change over the specified lag","ts_rank":"ranks the value within its time-series window","ts_zscore":"standardizes the value within its time-series window","vec_avg":"aggregates the vector field to a scalar average","vec_sum":"aggregates the vector field to a scalar sum"}
    for op in ops:parts.append(f"{op} {rationales.get(op,'is used exactly as shown in the recorded expression')}.")
    if reducer!="IDENTITY" and reducer.lower() not in ops:parts.append(f"{reducer.lower()} aggregates the vector field before the scalar transform.")
    operator=" ".join(parts) or "No function operator is present; the raw recorded field is used directly."
    full=f"Idea: {idea}\nData rationale: {data}\nOperator rationale: {operator}"
    did="desc_"+hashlib.sha256(json.dumps({"candidate":candidate.get("candidate_id"),"expression":expr,"template":1},sort_keys=True).encode()).hexdigest()[:24]
    draft={"description_id":did,"candidate_id":candidate.get("candidate_id"),"version":int(candidate.get("description_version") or 1),"source":"AUTO_DRAFT","idea":idea,"data_rationale":data,"operator_rationale":operator,"full_text":full,"field_metadata_source":field_metadata.get("source","FIXTURE_METADATA"),"expression_snapshot":expr,"operator_snapshot":ops,"field_snapshot":[field],"description_template_version":DESCRIPTION_TEMPLATE_VERSION,"created_at":_now(),"updated_at":_now()}
    draft.update(validate_description(candidate,draft,field_metadata,rules));return draft

def validate_description(candidate:Mapping[str,Any],draft:Mapping[str,Any],field_metadata:Mapping[str,Any],rules:Mapping[str,Any])->Dict[str,Any]:
    warnings=[];sections=[draft.get("idea"),draft.get("data_rationale"),draft.get("operator_rationale")]
    if not all(str(x or '').strip() for x in sections):return {"validation_status":"MISSING_SECTION","validation_warnings":["MISSING_SECTION"]}
    actual_fields=set(candidate.get("data_fields_used") or [candidate.get("field_id")]);mentioned=set(draft.get("field_snapshot") or actual_fields)
    if not mentioned<=actual_fields:return {"validation_status":"INVALID_FIELD_REFERENCE","validation_warnings":["DESCRIPTION_FIELD_MISMATCH"]}
    actual_ops=set(_operators(candidate.get("expression") or candidate.get("expression_canonical") or ""));mentioned_ops=set(draft.get("operator_snapshot") or actual_ops)
    if not mentioned_ops<=actual_ops:return {"validation_status":"INVALID_OPERATOR_REFERENCE","validation_warnings":["DESCRIPTION_OPERATOR_MISMATCH"]}
    if not str(field_metadata.get("description") or "").strip() or str(candidate.get("semantic_class"))=="UNKNOWN":return {"validation_status":"NEEDS_MANUAL_DESCRIPTION","validation_warnings":["INSUFFICIENT_FIELD_METADATA"]}
    minimum=int(rules.get("description_validation",{}).get("minimum_length",100))
    if len(str(draft.get("full_text") or ""))<minimum:return {"validation_status":"TOO_SHORT","validation_warnings":["DESCRIPTION_TOO_SHORT"]}
    return {"validation_status":"VALID","validation_warnings":warnings}

def manual_checklist(candidate,draft):
    first="Copy the validated Description." if draft.get("validation_status")=="VALID" else "Complete and validate the manual Description."
    return {"candidate_id":candidate.get("candidate_id"),"alpha_id":candidate.get("alpha_id"),"steps":[first,"Paste it into the BRAIN Description field.","Add PowerPoolSelected manually.","Run the read-only refresh after manual changes."],"description_id":draft.get("description_id")}
def build_manual_actions(candidate):
    return [{"manual_action_id":f"manual_{candidate.get('candidate_id')}_{kind.lower()}","candidate_id":candidate.get("candidate_id"),"alpha_id":candidate.get("alpha_id"),"action_type":kind,"status":"PENDING"} for kind in ("DESCRIPTION_EXPECTED","POWERPOOL_TAG_EXPECTED")]


POWER_POOL_DESCRIPTION_TEMPLATE_VERSION = 1
POWER_POOL_TEMPLATE_HEADINGS = (
    "Idea:",
    "Rationale for data used:",
    "Rationale for operators used:",
)


def _operator_rationale_sentence(op: str) -> str:
    rationale = {
        "ts_mean": "smooths the input across the recorded time window to reduce short-lived noise while retaining persistent information",
        "rank": "converts the input into a cross-sectional relative ordering so the signal depends on relative rather than absolute levels",
        "zscore": "standardizes the input cross-sectionally so unusually high and low values are expressed on a comparable scale",
        "ts_delta": "focuses on the change in the input over the recorded lag rather than its absolute level",
        "ts_rank": "measures the input's relative position within its own recorded time-series window",
        "ts_zscore": "standardizes the input relative to its own recent time-series history",
        "ts_std_dev": "measures recent variability in the input over the recorded time-series window",
        "vec_avg": "reduces a vector-valued field to a scalar average before the remaining signal transformation",
        "vec_sum": "reduces a vector-valued field to a scalar sum before the remaining signal transformation",
    }
    return rationale.get(op, "is used exactly as recorded in the alpha expression")


def draft_power_pool_description(candidate: Mapping[str, Any], field_metadata: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Create a deterministic, copy-ready Power Pool description.

    This helper is intentionally local-only: it never PATCHes/PUTs Alpha
    properties.  It uses the exact recorded field/operator names and avoids
    inventing an economic interpretation when field metadata is unavailable.
    """
    meta = dict(field_metadata or {})
    field = str(candidate.get("field_id") or "").strip() or "the recorded input field"
    dataset = str(candidate.get("dataset_id") or "").strip() or "the recorded dataset"
    expr = str(candidate.get("expression") or candidate.get("expression_canonical") or "").strip()
    ops = _operators(expr)
    direction = str(candidate.get("direction") or "NORMAL").upper()
    window = candidate.get("window")
    transform = str(candidate.get("transform_family") or candidate.get("operator") or "the recorded transform")

    idea = (
        f"Use {field} as the quantitative input and apply {transform} to capture a stable cross-sectional signal. "
        "The alpha is designed to preserve persistent information in the recorded input while limiting sensitivity to short-lived fluctuations."
    )
    if window not in (None, "", "None"):
        idea += f" The recorded time-series window is {window} periods."
    if direction == "REVERSE":
        idea += " The recorded signal direction is reversed."

    supplied_desc = str(meta.get("description") or "").strip()
    if supplied_desc:
        data = (
            f"The alpha uses {field} from {dataset}. The supplied field metadata describes this input as: {supplied_desc} "
            "The description stays within the supplied metadata and does not add unsupported field semantics."
        )
    else:
        data = (
            f"The alpha uses {field} from {dataset} as its recorded quantitative input. "
            "No additional field interpretation is assumed here; the rationale intentionally stays close to the exact data field used by the expression."
        )

    if ops:
        parts = [f"{op} {_operator_rationale_sentence(op)}." for op in ops]
        operator = " ".join(parts)
    else:
        operator = "The raw recorded field is used directly without a function operator."
    if direction == "REVERSE":
        operator += " The unary negative sign reverses the recorded signal direction."

    full = (
        f"Idea:\n{idea}\n\n"
        f"Rationale for data used:\n{data}\n\n"
        f"Rationale for operators used:\n{operator}"
    )
    valid_headings = all(h in full for h in POWER_POOL_TEMPLATE_HEADINGS)
    validation_status = "VALID" if valid_headings and len(full) >= 100 else "INVALID"
    warnings = []
    if not valid_headings:
        warnings.append("POWER_POOL_TEMPLATE_HEADING_MISSING")
    if len(full) < 100:
        warnings.append("POWER_POOL_DESCRIPTION_TOO_SHORT")
    return {
        "candidate_id": candidate.get("candidate_id"),
        "alpha_id": candidate.get("alpha_id"),
        "source": "AUTO_POWER_POOL_DRAFT",
        "description_template_version": POWER_POOL_DESCRIPTION_TEMPLATE_VERSION,
        "idea": idea,
        "data_rationale": data,
        "operator_rationale": operator,
        "full_text": full,
        "validation_status": validation_status,
        "validation_warnings": warnings,
        "description_length": len(full),
        "expression_snapshot": expr,
        "field_snapshot": [field],
        "operator_snapshot": ops,
        "manual_only": True,
        "platform_write_performed": False,
    }
