import json,sqlite3,subprocess,sys
from pathlib import Path
import pytest
from ppl_engine.config import load_effective_config
from ppl_engine.description import build_manual_actions,draft_description,manual_checklist,validate_description
from ppl_engine.properties import evaluate_manual_properties_gate,final_check_schedule_allowed,parse_alpha_properties,refresh_preview,resolve_refresh_candidate
from ppl_engine.store import RunnerStore,SCHEMA_VERSION
from ppl_engine.summary_writer import build_phase7_summary
ROOT=Path(__file__).resolve().parents[1];FIX=ROOT/'tests'/'fixtures'
def cfg():return load_effective_config(ROOT/'ppl_rules.yaml',ROOT/'ppl_plan.yaml',project_dir=ROOT)
def load(n):return json.loads((FIX/n).read_text())
def cand(**kw):
 x=dict(candidate_id='c',alpha_id='a',lifecycle_state='PRE_TAG_FINALIST',field_id='field_a',data_fields_used=['field_a'],semantic_class='RETURN',expression='ts_mean(-field_a, 4)',direction='REVERSE',transform_family='TS_MEAN',window=4,vector_reducer='IDENTITY');x.update(kw);return x
def meta(**kw):x={'description':'Return associated with a supplied price field.','source':'SYNTHETIC_TEST'};x.update(kw);return x
def test_only_finalist_formal():
 assert draft_description(cand(),meta(),cfg().rules,formal=True)['validation_status']=='VALID'
@pytest.mark.parametrize('state',['PLANNED','SIMULATION_COMPLETE','PRE_TAG_CHECK_PENDING'])
def test_disallowed_states(state):
 with pytest.raises(ValueError,match='DESCRIPTION_NOT_ALLOWED'):draft_description(cand(lifecycle_state=state),meta(),cfg().rules,formal=True)
def test_sections_and_minimum():
 d=draft_description(cand(),meta(),cfg().rules);assert d['idea'] and d['data_rationale'] and d['operator_rationale'] and len(d['full_text'])>=cfg().rules['description_validation']['minimum_length']
def test_no_repetitive_padding():
 d=draft_description(cand(),meta(),cfg().rules);assert d['full_text'].count(d['idea'])==1
def test_unknown_metadata_needs_manual_no_hallucination():
 e=load('description_unknown_field.json');d=draft_description(e['candidate'],e['field_metadata'],cfg().rules);assert d['validation_status']=='NEEDS_MANUAL_DESCRIPTION' and 'sentiment' not in d['full_text'].lower()
def test_field_mismatch():
 d=draft_description(cand(),meta(),cfg().rules);d['field_snapshot']=['field_b'];assert validate_description(cand(),d,meta(),cfg().rules)['validation_status']=='INVALID_FIELD_REFERENCE'
def test_operator_mismatch():
 d=draft_description(cand(),meta(),cfg().rules);d['operator_snapshot']=['ts_rank'];assert validate_description(cand(),d,meta(),cfg().rules)['validation_status']=='INVALID_OPERATOR_REFERENCE'
def test_vector_and_reverse_rationale():
 c=cand(expression='vec_avg(field_a)',field_type='VECTOR',vector_reducer='VEC_AVG',direction='NORMAL',transform_family='RAW',window=None);d=draft_description(c,meta(),cfg().rules);assert 'vec_avg' in d['operator_rationale']
 assert 'reverses' in draft_description(cand(),meta(),cfg().rules)['operator_rationale']
def test_description_version_and_sources(tmp_path):
 s=RunnerStore(tmp_path/'r.db');s.initialize()
 with sqlite3.connect(s.path) as db:
  for v,source in [(1,'AUTO_DRAFT'),(2,'MANUAL'),(3,'API_SNAPSHOT')]:db.execute("insert into ppl_descriptions(candidate_id,version,source,created_at,description_template_version) values('c',?,?,?,1)",(v,source,str(v)))
  assert db.execute("select source from ppl_descriptions order by version").fetchall()==[('AUTO_DRAFT',),('MANUAL',),('API_SNAPSHOT',)]
def test_manual_actions_pending_not_confirmed():assert all(x['status']=='PENDING' for x in build_manual_actions(cand()))
def test_property_present_absent_unknown():
 assert parse_alpha_properties(load('alpha_details_tag_present.json')['alpha_details'])['power_pool_selected_status']=='PRESENT'
 assert parse_alpha_properties(load('alpha_details_tag_absent.json')['alpha_details'])['power_pool_selected_status']=='ABSENT'
 assert parse_alpha_properties(load('alpha_details_unknown_tag_structure.json')['alpha_details'])['power_pool_selected_status']=='UNKNOWN'
def test_no_string_contains_false_positive():assert parse_alpha_properties({'description':'PowerPoolSelected','availableTags':['PowerPoolSelected']})['power_pool_selected_status']=='UNKNOWN'
def test_submission_not_guessed():assert parse_alpha_properties({'properties':{'status':'ACTIVE','tags':[]}})['submission_status']=='UNKNOWN'
def test_property_parser_versions_and_stable_snapshot():
 p={'properties':{'description':'x','tags':[]}};a=parse_alpha_properties(p);b=parse_alpha_properties(p);assert a['property_parser_version']==a['tag_alias_version']==2 and a['snapshot_id']==b['snapshot_id']
@pytest.mark.parametrize('desc,tag,expected',[('VALID','PRESENT','PASS'),('NEEDS_MANUAL_DESCRIPTION','PRESENT','PENDING'),('VALID','ABSENT','PENDING'),('VALID','UNKNOWN','UNKNOWN')])
def test_manual_gate(desc,tag,expected):
 snap={'description_present':desc=='VALID','power_pool_selected_status':tag};assert evaluate_manual_properties_gate(cand(),{'validation_status':desc},snap)['status']==expected
def test_description_conflict_blocks():
 x=evaluate_manual_properties_gate(cand(),{'validation_status':'VALID','property_description_conflict':True},{'description_present':True,'power_pool_selected_status':'PRESENT'});assert x['status']!='PASS'
def test_ready_would_transition_separately():
 x=evaluate_manual_properties_gate(cand(),{'validation_status':'VALID'},{'description_present':True,'power_pool_selected_status':'PRESENT'});assert x['would_transition']=='PPL_TAGGED' and x['would_schedule']=='FINAL_CHECK_PENDING'
def test_manual_api_conflict_api_wins():
 snap={'description_present':True,'power_pool_selected_status':'ABSENT'};m=[{'evidence_type':'POWERPOOL_TAG','payload_json':'{"status":"PRESENT"}'}];x=refresh_preview(cand(lifecycle_state='AWAITING_MANUAL_PROPERTIES'),{'validation_status':'VALID'},snap,m);assert x['manual_api_conflict'] and x['property_gate']['would_transition'] is None
def test_refresh_idempotent_computation():
 e=load('alpha_details_tag_present.json');snap=parse_alpha_properties(e['alpha_details']);assert refresh_preview(e['candidate'],e['description_validation'],snap)==refresh_preview(e['candidate'],e['description_validation'],snap)
def test_snapshot_change():
 a=parse_alpha_properties(load('alpha_details_tag_absent.json')['alpha_details']);b=parse_alpha_properties(load('alpha_details_tag_present.json')['alpha_details']);assert a['snapshot_id']!=b['snapshot_id']
def test_alpha_ambiguity():
 with pytest.raises(ValueError,match='AMBIGUOUS'):resolve_refresh_candidate([{'alpha_id':'a','run_id':'r1'},{'alpha_id':'a','run_id':'r2'}],'a')
 assert resolve_refresh_candidate([{'alpha_id':'a','run_id':'r1'},{'alpha_id':'a','run_id':'r2'}],'a','r2')['run_id']=='r2'
def test_final_check_only_tagged():assert final_check_schedule_allowed({'lifecycle_state':'PPL_TAGGED'}) and not final_check_schedule_allowed({'lifecycle_state':'PRE_TAG_FINALIST'})
def test_export_and_checklist():
 d=draft_description(cand(),meta(),cfg().rules);assert manual_checklist(cand(),d)['steps'] and d['full_text']
@pytest.mark.parametrize('flag,fixture',[('--description-preview','description_valid_vwap.json'),('--refresh-preview','alpha_details_tag_present.json'),('--export-description','description_valid_vwap.json')])
def test_cli_zero_network_writes(flag,fixture):
 p=subprocess.run([sys.executable,str(ROOT/'ppl_runner.py'),flag,'--fixture',str(FIX/fixture)],cwd=ROOT,capture_output=True,text=True);assert p.returncode==0 and '"network_requests": 0' in p.stdout and '"writes": 0' in p.stdout
def test_schema_and_summary_zero(tmp_path):
    s=RunnerStore(tmp_path/'r.db');s.initialize();s.create_run('r',cfg());summary=build_phase7_summary(s,cfg(),'r');assert SCHEMA_VERSION==13 and summary['description_stats']=={'drafted':0,'validated':0,'needs_manual':0,'awaiting_properties':0} and summary['manual_properties_stats']['ready']==0


def test_power_pool_description_draft_is_copy_ready_and_local_only():
    from ppl_engine.description import draft_power_pool_description
    c = cand(expression='ts_mean(field_a,3)', field_id='field_a', transform_family='TS_MEAN', window=3)
    d = draft_power_pool_description(c)
    assert d['validation_status'] == 'VALID'
    assert d['description_length'] >= 100
    assert 'Idea:' in d['full_text']
    assert 'Rationale for data used:' in d['full_text']
    assert 'Rationale for operators used:' in d['full_text']
    assert 'ts_mean' in d['full_text']
    assert d['manual_only'] is True
    assert d['platform_write_performed'] is False
