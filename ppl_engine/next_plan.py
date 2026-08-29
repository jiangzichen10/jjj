"""Strict allowlisted next_plan.json validation; never executes code or endpoints."""
from typing import Any,Dict,Mapping
from .config import ConfigError
NEXT_PLAN_SCHEMA_VERSION=1
FIELDS={
"explore_dataset":{"dataset_ids","max_new_simulation_posts"},"explore_fields":{"dataset_id","field_ids"},"explore_family":{"family_ids","max_new_simulation_posts"},"repair_alpha":{"candidate_id","allowed_repair_types","max_new_simulation_posts"},"resume_run":set(),"export_summary":set(),"stop_family":{"family_id","reason"}}
def validate_next_plan(value:Mapping[str,Any],*,run_id:str,execution_hash:str,initial_budget_remaining:int,repair_budget_remaining:int)->Dict[str,Any]:
    required={"schema_version","run_id","execution_hash","action"}; unknown=set(value)-required-FIELDS.get(value.get("action"),set())
    if value.get("schema_version")!=1:raise ConfigError("NEXT_PLAN_SCHEMA_INVALID")
    if value.get("run_id")!=run_id or value.get("execution_hash")!=execution_hash:raise ConfigError("NEXT_PLAN_CONTEXT_MISMATCH")
    action=value.get("action")
    if action not in FIELDS:raise ConfigError("NEXT_PLAN_ACTION_FORBIDDEN")
    if unknown:raise ConfigError("NEXT_PLAN_UNKNOWN_FIELDS: "+",".join(sorted(unknown)))
    requested=int(value.get("max_new_simulation_posts",0)); remaining=repair_budget_remaining if action=="repair_alpha" else initial_budget_remaining
    out=dict(value);out["effective_max_new_simulation_posts"]=min(requested,remaining);out["budget_clamped"]=requested>remaining
    return out
