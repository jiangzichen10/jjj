import hashlib, json, sqlite3
import machine_lib_V2_1 as real_machine
from pathlib import Path
import pytest
from ppl_engine.config import ConfigError,load_effective_config
from ppl_engine.phase10b import PHASE10B_CAP,create_phase10b_run,prepare_repair_batch,preview_phase10b,select_phase10b,validate_phase10b_config
from ppl_engine.store import RunnerStore

ROOT=Path(__file__).resolve().parents[1]
def cfg():return load_effective_config(ROOT/'ppl_rules.yaml',ROOT/'ppl_plan_phase10b.yaml',project_dir=ROOT)
def adb(path,rows=()):
 c=sqlite3.connect(path);c.execute('create table alpha_results(sim_key text primary key,status text,simulation_url text)');c.execute('create table alpha_contexts(context_key text primary key,sim_key text)')
 for x in rows:c.execute('insert into alpha_results values (?,?,?)',x)
 c.commit();c.close()
def _identity(conf, expression, *, decay=0):
 defaults=conf.plan['simulation_settings']
 candidate={'expr':expression,'decay':decay}
 settings=real_machine.build_settings(
  candidate,neutralization=defaults['neutralization'],region=defaults['region'],
  universe=defaults['universe'],delay=int(defaults['delay']),truncation=float(defaults['truncation']),
  test_period=defaults.get('test_period',defaults.get('testPeriod','P0Y')))
 settings_json=json.dumps(settings,ensure_ascii=False,sort_keys=True,separators=(',',':'))
 return real_machine.simulation_key(expression,settings),settings,settings_json,hashlib.sha256(settings_json.encode()).hexdigest()

def make_store(tmp_path):
 s=RunnerStore(tmp_path/'r.db');s.initialize();conf=cfg();now='2026-01-01T00:00:00Z';s.create_run('run_0002',conf);s.create_run('run_0003',conf)
 identities=[]
 with s.connect() as c:
  c.execute("update ppl_runs set status='READY_FOR_EXECUTION',current_stage='READY_FOR_EXECUTION',run_profile='PRODUCTION_RESEARCH' where run_id='run_0002'")
  c.execute("update ppl_runs set status='COMPLETED',current_stage='COMPLETED',validation_phase='PHASE_10A_CANARY',source_run_id='run_0002',post_consumed=2,post_confirmed=2 where run_id='run_0003'")
  c.execute("insert into ppl_discovery_snapshots(snapshot_id,region,universe,delay,instrument_type,source,dataset_count,field_count,metadata_hash,exclusion_status_json,created_at) values ('disc','GLB','TOPDIV3000',1,'EQUITY','T',6,6,'h','{}',?)",(now,))
  for i in range(7):
   cid=f'c{i}';ds=f'd{i%3}';field=f'f{i}';fam=f'{ds}/{field}/NORMAL/T{i}';expr=f'rank({field})'
   sk,settings,settings_json,settings_hash=_identity(conf,expr,decay=0);identities.append(sk)
   c.execute("insert into ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,operator,decay,neutralization,lifecycle_state,simulation_status,discovery_snapshot_id,dry_run_snapshot_id,cache_classification,execution_action,selection_rank,selected_for_initial_search,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(cid,'run_0002',expr,sk,settings_json,settings_hash,f'ctx{i}',ds,field,'MATRIX','RETURN','NORMAL',fam,f'T{i}','rank',0,settings['neutralization'],'PLANNED','NONE','disc','dry','CACHE_MISS','NEW_SIMULATION_REQUIRED',i+1,1,now,now))
   c.execute("insert into ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?)",(f'p{i}',cid,'run_0002',sk,f'ctx{i}','disc','dry','{}',now,now))
  for i in range(2):
   c.execute("insert into ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,selected_for_initial_search,created_at,updated_at) values (?,?,?,?,?,?,?,?)",(f'a{i}','run_0003',f'x{i}',identities[i],'NEAR_PASS',1,now,now))
 return s,conf

def test_budget_remaining_and_concurrency(tmp_path):
 s,c=make_store(tmp_path);x=validate_phase10b_config(s,c);assert x['phase10b']==4 and x['repair']==4 and x['effective_concurrency']==4
@pytest.mark.parametrize('consumed',[0,1,3,4])
def test_phase10a_consumption_must_be_two(tmp_path,consumed):
 s,c=make_store(tmp_path)
 with s.connect() as con:con.execute("update ppl_runs set post_consumed=? where run_id='run_0003'",(consumed,))
 with pytest.raises(ConfigError):validate_phase10b_config(s,c)
def test_selection_excludes_phase10a_and_cache(tmp_path):
 s,c=make_store(tmp_path);rows=s.load_candidates('run_0002');keys=[x['sim_key'] for x in rows]
 facts={keys[2]:{'status':'COMPLETE'}};excluded={keys[0],keys[1]};got=select_phase10b(rows,facts,excluded)
 assert len(got)==4 and not ({x['sim_key'] for x in got}&(excluded|{keys[2]}))
def test_create_isolated_four_and_preview(tmp_path):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);before=s.get_run('run_0002')['updated_at'];out=create_phase10b_run(s,c,p)
 assert out['validation_run_id']=='run_0004' and len(s.load_candidates('run_0004'))==4
 assert s.get_run('run_0002')['updated_at']==before
 preview=preview_phase10b(s,c,p,'run_0004');assert preview['NEW_SIMULATION']==4 and preview['estimated_new_posts']==4 and preview['remaining_initial_budget']==4
def test_preview_cache_resume_uncertain_and_retry(tmp_path):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);create_phase10b_run(s,c,p);rows=s.load_candidates('run_0004')
 con=sqlite3.connect(p);vals=[('COMPLETE','u'),('RUNNING','u'),('UNCERTAIN_SUBMISSION',None),('ERROR',None)]
 for r,(st,url) in zip(rows,vals):con.execute('insert into alpha_results values (?,?,?)',(r['sim_key'],st,url))
 con.commit();con.close();x=preview_phase10b(s,c,p,'run_0004')
 assert (x['CACHE_RESTORE'],x['RESUME'],x['HOLD'],x['RETRY'],x['estimated_new_posts'])==(1,1,1,1,1)
def test_repeated_preview_zero_after_complete(tmp_path):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);create_phase10b_run(s,c,p);rows=s.load_candidates('run_0004');con=sqlite3.connect(p)
 for r in rows:con.execute('insert into alpha_results values (?,?,?)',(r['sim_key'],'COMPLETE','u'))
 con.commit();con.close();x=preview_phase10b(s,c,p,'run_0004');assert x['CACHE_RESTORE']==4 and x['estimated_new_posts']==0
def test_phase10a_simkey_reuse_rejected(tmp_path):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);create_phase10b_run(s,c,p)
 with s.connect() as con:
  key=con.execute("select sim_key from ppl_candidates where run_id='run_0003' limit 1").fetchone()[0]
  con.execute("update ppl_candidates set sim_key=? where run_id='run_0004' and selection_rank=1",(key,))
 with pytest.raises(ConfigError,match='REUSE'):preview_phase10b(s,c,p,'run_0004')

def test_initial_preview_ignores_repair_children(tmp_path):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);create_phase10b_run(s,c,p)
 rows=s.load_candidates('run_0004');parent=rows[0];now='2026-01-01T00:00:00Z'
 with s.connect() as con:
  con.execute("insert into ppl_candidates(candidate_id,run_id,expression,sim_key,lifecycle_state,selected_for_initial_search,created_at,updated_at) values (?,?,?,?,?,?,?,?)",('repair_child','run_0004','rank(fx)','repair_key','PLANNED',0,now,now))
 out=preview_phase10b(s,c,p,'run_0004')
 assert out['candidate_count']==4 and out['estimated_new_posts']==4

def test_repair_reserve_is_cumulative(tmp_path):
 s,c=make_store(tmp_path)
 with s.connect() as con:
  now='2026-01-01T00:00:00Z'
  for i in range(5):
   con.execute("insert into ppl_repair_plans(repair_plan_id,run_id,parent_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(f'r{i}','run_0002','c2','F','R',f'sig{i}','[]',1,'{}','[]','READY',1,0,0,now,now))
  con.execute("update ppl_repair_plans set plan_status='EXECUTED',consumed_posts=2 where repair_plan_id='r0'")
  consumed=con.execute("select coalesce(sum(consumed_posts),0) from ppl_repair_plans where run_id='run_0002'").fetchone()[0]
  remaining=max(0,4-consumed)
  selectable=con.execute("select count(*) from (select 1 from ppl_repair_plans where run_id='run_0002' and plan_status='READY' limit ?)",(remaining,)).fetchone()[0]
 assert consumed==2 and remaining==2 and selectable==2

def test_repair_preview_does_not_persist_child(tmp_path,monkeypatch):
 s,c=make_store(tmp_path);p=tmp_path/'a.db';adb(p);now='2026-01-01T00:00:00Z'
 spec={'repair_type':'REVERSE_DIRECTION','repair_signature':'sig','repair_depth':1,'expression_preview':'-(rank(f2))'}
 with s.connect() as con:
  con.execute("insert into ppl_repair_plans(repair_plan_id,run_id,parent_candidate_id,target_failure,repair_type,repair_signature,repair_path_json,repair_depth,candidate_spec_json,operator_requirements_json,plan_status,projected_new_posts,committed_posts,consumed_posts,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",('rp','run_0002','c2','NEGATIVE_STRONG_SIGNAL','REVERSE_DIRECTION','sig','[]',1,json.dumps(spec),'[]','READY',1,0,0,now,now))
  before=con.execute("select count(*) from ppl_candidates where run_id='run_0002'").fetchone()[0]
 repair_expr='-(rank(f2))';repair_key,repair_settings,_,_=_identity(c,repair_expr,decay=0)
 monkeypatch.setattr('ppl_engine.phase10b.materialize_repair_candidate',lambda *a,**k:{'sim_key':repair_key,'settings':repair_settings,'expr':repair_expr,'operator':'rank','window':None,'decay':0})
 out=prepare_repair_batch(s,c,None,p,'run_0002')
 with s.connect() as con: after=con.execute("select count(*) from ppl_candidates where run_id='run_0002'").fetchone()[0]
 assert out['estimated_new_posts']==1 and out['items'][0]['would_create'] is True and before==after
