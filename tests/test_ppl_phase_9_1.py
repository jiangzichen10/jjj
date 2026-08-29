import json
from pathlib import Path

from ppl_engine.check_parser import parse_check_payload, parse_individual_check
from ppl_engine.check_transport import CheckBudget, CheckResponse, semantic_poll_check
from ppl_engine.config import load_effective_config
from ppl_engine.diagnosis import compare_check_interpretations, diagnose_evidence
from ppl_engine.repair_engine import plan_repairs
from ppl_engine.store import RunnerStore

ROOT=Path(__file__).resolve().parents[1]
LIVE=ROOT/'tests/fixtures/live_sanitized/check_WjAxWeoG_sanitized.json'

def cfg():return load_effective_config(ROOT/'ppl_rules.yaml',ROOT/'ppl_plan.yaml',project_dir=ROOT)
def parsed():return parse_check_payload(json.loads(LIVE.read_text(encoding='utf-8')),phase='FINAL',rules=cfg().rules,evidence_source='LIVE_VALIDATION_FIXTURE')
def by_raw():return {x['raw_name']:x for x in parsed()['results']}

def test_real_pp_corr_warning_raw_and_eligibility_preserved():
 x=by_raw()['POWER_POOL_CORRELATION'];assert x['raw_result']==x['normalized_result']==x['eligibility_outcome']=='WARNING'
 assert x['raw_value']==.8797 and x['raw_limit']==.5 and x['threshold_exceeded'] is True
 assert x['eligibility_reason']=='PP_CORRELATION_LIMIT_EXCEEDED_ADVANTAGE_STATUS_UNRESOLVED'
 assert x['diagnosis_outcome']=='STRUCTURAL_CORRELATION_RISK'

def test_pp_corr_warning_neither_pass_nor_fail():
 x=by_raw()['POWER_POOL_CORRELATION'];assert x['eligibility_outcome'] not in {'PASS','FAIL'}
 assert parsed()['base_gate']['status']=='WARNING' and not parsed()['final_check_pass']

def test_theme_warning_fail_closed_without_false_causality():
 x=by_raw()['MATCHES_THEMES'];assert x['raw_result']=='WARNING' and x['eligibility_outcome']=='WARNING'
 assert parsed()['theme_gate']['status']=='WARNING' and not parsed()['final_check_pass']
 assert x['diagnosis_outcome']=='MANUAL_REVIEW' and x['diagnosis_reason']=='THEME_ROOT_CAUSE_UNKNOWN'

def test_wjax_live_diagnosis_is_structural_risk_not_fail():
 d=diagnose_evidence({'phase':'FINAL','candidate':{'data_field_count_estimate':1,'pp_total_operator_count_estimate':2},'metrics':{'sharpe':1.17,'turnover':.608},'parsed':parsed(),'evidence_source':'LIVE_VALIDATION_FIXTURE'},cfg().rules)
 assert d['primary_failure']=='STRUCTURAL_CORRELATION_RISK'
 assert d['local_pre_gate']['checks']['simulation_sharpe'] is True and d['local_pre_gate']['checks']['simulation_turnover_base'] is True
 assert 'STRUCTURAL_CORRELATION_FAIL' not in [d['primary_failure'],*d['secondary_failures']]
 assert d['root_cause']=='STRUCTURAL_CORRELATION_RISK'
 assert d['recommended_research_actions']==['STOP_LOCAL_VARIANTS','SWITCH_FIELD','SWITCH_FAMILY','SWITCH_DATASET']
 p=plan_repairs({'candidate_id':'c','field_id':'f','field_type':'MATRIX','dataset_id':'d','expression':'x','signal_family':'d/f/IDENTITY/NORMAL/raw','operator':'raw'},d,cfg().rules)
 assert p['stop_reason']=='STOP_LOCAL_VARIANTS' and p['plans']==[]

def test_real_turnover_names_are_structurally_distinct():
 x=by_raw();assert x['HIGH_TURNOVER']['normalized_name']=='TURNOVER' and x['HIGH_TURNOVER']['category']=='PPL_BASE'
 assert x['HT_TURNOVER']['normalized_name']=='HIGH_TURNOVER' and x['HT_TURNOVER']['category']=='PPL_THEME'
 assert x['HT_HIGH_TURNOVER_RETURNS_RATIO']['normalized_name']=='HIGH_TURNOVER_RETURNS_RATIO'

def test_classification_requires_explicit_high_turnover_value():
 good=by_raw()['MATCHES_CLASSIFICATION'];assert good['eligibility_outcome']=='PASS'
 other=parse_individual_check({'name':'MATCHES_CLASSIFICATION','result':'PASS','value':['Other']},'FINAL',cfg().rules,'TEST')
 assert other['normalized_name']=='CLASSIFICATION_HIGH_TURNOVER' and other['eligibility_outcome']=='UNKNOWN'

def test_three_correlation_concepts_remain_separate():
 x=by_raw();assert x['SELF_CORRELATION']['normalized_name']=='SELF_CORRELATION'
 assert x['PROD_CORRELATION']['normalized_name']=='PROD_CORRELATION'
 assert x['POWER_POOL_CORRELATION']['normalized_name']=='POWER_POOL_CORRELATION'

def test_generic_warning_does_not_change_ppl_gate():
 payload=json.loads(LIVE.read_text(encoding='utf-8'));base=parse_check_payload(payload,phase='FINAL',rules=cfg().rules,evidence_source='TEST')
 payload['is']['checks'].append({'name':'LOW_2Y_SHARPE','result':'WARNING','value':0.1,'limit':9})
 changed=parse_check_payload(payload,phase='FINAL',rules=cfg().rules,evidence_source='TEST')
 assert changed['base_gate']==base['base_gate'] and changed['theme_gate']==base['theme_gate']

def test_live_evidence_wins_over_manual_interpretation():
 conflict=compare_check_interpretations({'raw_result':'FAIL','raw_value':.8797,'raw_limit':.5},{'raw_result':'WARNING','raw_value':.8797,'raw_limit':.5})
 assert conflict=={'conflict_type':'INTERPRETATION_CONFLICT','manual_result':'FAIL','live_result':'WARNING','raw_value_consistent':True,'raw_limit_consistent':True,'authoritative_source':'LIVE_API'}

def test_theme_warning_diagnosis_keeps_unknown_root_cause():
 item=by_raw()['MATCHES_THEMES'];d=diagnose_evidence({'phase':'FINAL','candidate':{'data_field_count_estimate':1,'pp_total_operator_count_estimate':1},'metrics':{'sharpe':1.2,'turnover':.4},'check_results':[item]},cfg().rules)
 assert d['primary_failure']=='THEME_MATCH_WARNING' and d['root_cause']=='UNKNOWN' and d['recommended_research_actions']==['MANUAL_REVIEW']

class T:
 def __init__(self,text):self.text=text
 def fetch_check(self,_):return CheckResponse(200,self.text,1)

def test_eligibility_fields_persist_without_rewriting_raw(tmp_path):
 s=RunnerStore(tmp_path/'r.db');s.initialize();text=LIVE.read_text(encoding='utf-8')
 semantic_poll_check(T(text),alpha_id='WjAxWeoG',phase='FINAL',rules=cfg().rules,budget=CheckBudget(1,2,2,1),store=s,evidence_source='LIVE_VALIDATION_FIXTURE')
 with s.connect() as c:
  row=c.execute("select raw_result,normalized_result,eligibility_outcome,eligibility_reason,threshold_exceeded,diagnosis_outcome from ppl_check_results where raw_name='POWER_POOL_CORRELATION'").fetchone()
 assert tuple(row)==('WARNING','WARNING','WARNING','PP_CORRELATION_LIMIT_EXCEEDED_ADVANTAGE_STATUS_UNRESOLVED',1,'STRUCTURAL_CORRELATION_RISK')
