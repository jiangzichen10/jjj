import json,sqlite3,subprocess,sys
from pathlib import Path
import pytest
from ppl_engine.config import ConfigError,load_effective_config
from ppl_engine.family import build_family_index,build_reference_pool,evidence_level,family_fingerprint,local_family_similarity
from ppl_engine.next_plan import validate_next_plan
from ppl_engine.priority import diversity_rerank,score_candidate
from ppl_engine.store import RunnerStore,SCHEMA_VERSION
from ppl_engine.summary_writer import build_phase7_summary
ROOT=Path(__file__).resolve().parents[1]
def cfg():return load_effective_config(ROOT/'ppl_rules.yaml',ROOT/'ppl_plan.yaml',project_dir=ROOT)
def c(cid='c',**kw):
 x=dict(candidate_id=cid,run_id='r',dataset_id='ds',field_id='f',vector_reducer='IDENTITY',direction='NORMAL',transform_family='TS_MEAN',window=4,semantic_class='RETURN',lifecycle_state='PLANNED',simulation_status='NONE',repair_depth=0,classification_confidence='HIGH',available_result_json=None,primary_failure=None);x.update(kw);return x
def mkstore(tmp):
 s=RunnerStore(tmp/'r.db');s.initialize();s.create_run('r',cfg())
 return s
@pytest.mark.parametrize('changed',[
 {'window':5},{'run_id':'other'}])
def test_fingerprint_stable_ignores_window_run(changed):assert family_fingerprint(c())==family_fingerprint(c(**changed))
@pytest.mark.parametrize('changed',[
 {'transform_family':'RANK'},{'vector_reducer':'VEC_SUM'},{'direction':'REVERSE'}])
def test_fingerprint_components_change(changed):assert family_fingerprint(c())!=family_fingerprint(c(**changed))
def test_adjacent_similarity_and_name():
 x=local_family_similarity(c(window=4),c(window=5));assert x['similarity_risk'] in {'HIGH','VERY_HIGH'} and x['kind']=='local_family_similarity' and 'CORRELATION' not in json.dumps(x).upper()
def test_different_transform_not_same_family():assert family_fingerprint(c())!=family_fingerprint(c(transform_family='RAW'))
def test_reference_evidence_only():
 vals=[c('a',lifecycle_state='SIMULATION_COMPLETE'),c('b',lifecycle_state='PRE_TAG_FINALIST'),c('d',lifecycle_state='PPL_TAGGED'),c('e',lifecycle_state='SUBMITTED')]
 refs=build_reference_pool(vals);assert {x['candidate_id'] for x in refs}=={'b','d','e'}
def test_historical_reference():
 refs=build_reference_pool([],stored=[{'source':'HISTORICAL_FINALIST','signal_family':'ds/f/IDENTITY/NORMAL/TS_MEAN'}]);assert refs[0]['family_fingerprint']
def test_representative_result_over_planned():
 fam,mem=build_family_index([c('p'),c('r',lifecycle_state='SIMULATION_COMPLETE',simulation_status='COMPLETE')]);assert fam[0]['representative_candidate_id']=='r' and fam[0]['representative_type']=='RESULT_REPRESENTATIVE'
def test_planning_representative():assert build_family_index([c()])[0][0]['representative_type']=='PLANNING_REPRESENTATIVE'
@pytest.mark.parametrize('state,sim,expected',[('PLANNED','NONE','PLANNED_ONLY'),('SIMULATION_COMPLETE','COMPLETE','SIMULATION_COMPLETE'),('PRE_TAG_CHECK_PASS','COMPLETE','PRE_TAG_CHECK_PASS'),('FINAL_CHECK_PASS','COMPLETE','FINAL_CHECK_PASS')])
def test_evidence_levels(state,sim,expected):assert evidence_level(c(lifecycle_state=state,simulation_status=sim))==expected
def test_planned_metrics_unavailable_not_zero():
 fam,mem=build_family_index([c()]);s=score_candidate(c(),mem[0],fam[0]);assert s['components']['gate_performance'] is None and s['score_component_status']['gate_performance']=='UNAVAILABLE' and s['priority_confidence']=='LOW'
def test_priority_components_and_confidence():
 x=c(lifecycle_state='SIMULATION_COMPLETE',simulation_status='COMPLETE',available_result_json=json.dumps({'sharpe':1.2,'turnover':.4,'fitness':.5,'margin':.001}));fam,mem=build_family_index([x]);s=score_candidate(x,mem[0],fam[0]);assert s['priority_score_version']==1 and s['priority_confidence']=='MEDIUM' and s['components']['gate_performance']>0
def test_structural_and_depth_penalty():
 x=c(primary_failure='STRUCTURAL_CORRELATION_FAIL',repair_depth=3,lifecycle_state='SIMULATION_COMPLETE',simulation_status='COMPLETE');fam,mem=build_family_index([x]);s=score_candidate(x,mem[0],fam[0]);assert s['penalties']['structural_stop']==35 and s['penalties']['repair_depth']==6
def test_reference_overlap_penalty():
 x=c();fp=family_fingerprint(x);fam,mem=build_family_index([x],[{'family_fingerprint':fp}]);s=score_candidate(x,mem[0],fam[0]);assert s['penalties']['reference_overlap']==10
@pytest.mark.parametrize('confidence,value',[('HIGH',5),('MEDIUM',3),('LOW',0)])
def test_classification_component(confidence,value):
 x=c(classification_confidence=confidence);fam,mem=build_family_index([x]);assert score_candidate(x,mem[0],fam[0])['components']['classification_confidence']==value
def test_priority_does_not_mutate():
 x=c();before=dict(x);fam,mem=build_family_index([x]);score_candidate(x,mem[0],fam[0]);assert x==before
@pytest.mark.parametrize('items,expected',[([c()],'UNTESTED'),([c(lifecycle_state='NEAR_PASS')],'NEAR_PASS'),([c(lifecycle_state='PRE_TAG_FINALIST')],'PRE_TAG_FINALIST'),([c(lifecycle_state='FINAL_CHECK_PASS')],'FINAL_PASS')])
def test_family_status(items,expected):assert build_family_index(items)[0][0]['family_status']==expected
def test_structural_stop_explicit_only():
 assert build_family_index([c(primary_failure='STRUCTURAL_CORRELATION_FAIL')])[0][0]['family_status']=='STRUCTURAL_STOP'
 assert build_family_index([c(primary_failure='SHARPE_NEAR_PASS')])[0][0]['family_status']!='STRUCTURAL_STOP'
@pytest.mark.parametrize('limitkey,change',[('max_top_per_dataset',lambda i:{}),('max_top_per_field',lambda i:{'dataset_id':f'd{i}'}),('max_top_per_family',lambda i:{}),('max_top_per_semantic',lambda i:{'dataset_id':f'd{i}','field_id':f'f{i}','transform_family':f'T{i}'})])
def test_diversity_caps(limitkey,change):
 cs=[c(str(i),**change(i)) for i in range(3)];fams,mem=build_family_index(cs);fb={x['family_id']:x for x in fams};mb={x['candidate_id']:x for x in mem};cb={x['candidate_id']:x for x in cs};scores=[{'candidate_id':x['candidate_id'],'research_priority_score':100-i} for i,x in enumerate(cs)];limits={'max_top_per_dataset':9,'max_top_per_field':9,'max_top_per_family':9,'max_top_per_semantic':9};limits[limitkey]=1;assert len(diversity_rerank(scores,cb,fb,mb,limits,3))==1
def test_next_plan_valid_and_clamp():
 x={'schema_version':1,'run_id':'r','execution_hash':'h','action':'explore_family','family_ids':['f'],'max_new_simulation_posts':20};out=validate_next_plan(x,run_id='r',execution_hash='h',initial_budget_remaining=5,repair_budget_remaining=2);assert out['effective_max_new_simulation_posts']==5 and out['budget_clamped']
@pytest.mark.parametrize('value,error',[({'schema_version':1,'run_id':'r','execution_hash':'h','action':'submit_alpha'},'FORBIDDEN'),({'schema_version':1,'run_id':'x','execution_hash':'h','action':'resume_run'},'MISMATCH'),({'schema_version':1,'run_id':'r','execution_hash':'x','action':'resume_run'},'MISMATCH'),({'schema_version':1,'run_id':'r','execution_hash':'h','action':'resume_run','command':'x'},'UNKNOWN'),({'schema_version':1,'run_id':'r','execution_hash':'h','action':'repair_alpha','candidate_id':'c','expression':'x'},'UNKNOWN'),({'schema_version':1,'run_id':'r','execution_hash':'h','action':'repair_alpha','candidate_id':'c','settings':{}},'UNKNOWN')])
def test_next_plan_rejects(value,error):
 with pytest.raises(ConfigError,match=error):validate_next_plan(value,run_id='r',execution_hash='h',initial_budget_remaining=5,repair_budget_remaining=2)
def test_repair_budget_separate():
 x={'schema_version':1,'run_id':'r','execution_hash':'h','action':'repair_alpha','candidate_id':'c','allowed_repair_types':['SHARPE'],'max_new_simulation_posts':20};assert validate_next_plan(x,run_id='r',execution_hash='h',initial_budget_remaining=50,repair_budget_remaining=3)['effective_max_new_simulation_posts']==3
def test_advanced_operator_field_forbidden():
 x={'schema_version':1,'run_id':'r','execution_hash':'h','action':'repair_alpha','candidate_id':'c','operator':'trade_when'}
 with pytest.raises(ConfigError):validate_next_plan(x,run_id='r',execution_hash='h',initial_budget_remaining=5,repair_budget_remaining=2)
def test_schema_tables_and_rebuild(tmp_path):
 s=mkstore(tmp_path);cands=[c('a'),c('b',window=5)];
 # insert compact formal candidates through existing writer is cumbersome; derived persistence itself is verified directly.
 fam,mem=build_family_index(cands);scores=[score_candidate(x,{m['candidate_id']:m for m in mem}[x['candidate_id']],fam[0]) for x in cands];s.save_derived('r',fam,mem,scores)
 with s.connect() as db:
  assert db.execute('select count(*) from ppl_families').fetchone()[0]==1 and db.execute('select count(*) from ppl_priority_scores').fetchone()[0]==2
 s.save_derived('r',fam,mem,scores)
 with s.connect() as db:assert db.execute('select count(*) from ppl_priority_scores').fetchone()[0]==2
def test_summary_empty_repairs_and_versions(tmp_path):
 s=mkstore(tmp_path);summary=build_phase7_summary(s,cfg(),'r');assert summary['summary_version']==summary['priority_score_version']==summary['family_similarity_version']==1 and summary['repair_summary']['planned']==0 and summary['performance_comparison']['available'] is False and summary['live_check_resolved'] is False
def test_manual_evidence_summary(tmp_path):
 s=mkstore(tmp_path)
 with s.connect() as db:db.execute("insert into ppl_manual_evidence values('e',null,'REFERENCE_ALPHA','{}','MANUAL',null,'2026-01-01')")
 assert build_phase7_summary(s,cfg(),'r')['manual_evidence']['count']==1
def test_cli_previews_zero_network(tmp_path):
 db=tmp_path/'runner.db'
 s=RunnerStore(db);s.initialize();s.create_run('run_preview',cfg())
 for flag in ('--family-preview','--priority-preview'):
  p=subprocess.run([sys.executable,str(ROOT/'ppl_runner.py'),flag,'--run-id','run_preview','--db',str(db)],cwd=ROOT,capture_output=True,text=True)
  assert p.returncode==0, p.stderr
  assert '"network_requests": 0' in p.stdout
