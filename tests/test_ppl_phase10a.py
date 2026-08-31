import hashlib
import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest
import requests
import machine_lib_V2_1 as real_machine

from ppl_engine.config import ConfigError, load_effective_config
from ppl_engine.live_execution import (
    CANARY_CAP, EXPECTED_MACHINE_HASH, PHASE, alpha_schema_snapshot,
    create_validation_run, execute_phase10a, preview_phase10a,
    select_canaries, validate_phase10a_config,
)
from ppl_engine.store import RunnerStore


ROOT = Path(__file__).resolve().parents[1]


def cfg():
    return load_effective_config(ROOT / "ppl_rules.yaml", ROOT / "ppl_plan_phase10a.yaml", project_dir=ROOT)


def alpha_db(path: Path, rows=()):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE alpha_results(sim_key TEXT PRIMARY KEY,status TEXT,simulation_url TEXT,alpha_id TEXT,sharpe REAL,fitness REAL,turnover REAL,margin REAL,returns REAL,long_count INTEGER,short_count INTEGER,retry_count INTEGER,last_http_status INTEGER,updated_at TEXT)")
    con.execute("CREATE TABLE alpha_contexts(sim_key TEXT PRIMARY KEY,context_json TEXT)")
    for row in rows:
        con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url) VALUES (?,?,?)", row)
    con.commit(); con.close()


def _identity(config, expression: str, *, decay: int = 0):
    defaults = config.plan["simulation_settings"]
    candidate = {"expr": expression, "decay": decay}
    settings = real_machine.build_settings(
        candidate,
        neutralization=defaults["neutralization"],
        region=defaults["region"],
        universe=defaults["universe"],
        delay=int(defaults["delay"]),
        truncation=float(defaults["truncation"]),
        test_period=defaults.get("test_period", defaults.get("testPeriod", "P0Y")),
    )
    settings_json = json.dumps(settings, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    sim_key = real_machine.simulation_key(expression, settings)
    settings_hash = hashlib.sha256(settings_json.encode("utf-8")).hexdigest()
    return sim_key, settings, settings_json, settings_hash


def source_store(tmp_path: Path, candidates=3):
    store = RunnerStore(tmp_path / "runner.db"); store.initialize(); config = cfg()
    store.create_run("run_0002", config)
    now = "2026-01-01T00:00:00+00:00"
    with store.connect() as con:
        con.execute("UPDATE ppl_runs SET status='READY_FOR_EXECUTION',current_stage='READY_FOR_EXECUTION',run_profile='PRODUCTION_RESEARCH' WHERE run_id='run_0002'")
        con.execute("INSERT INTO ppl_discovery_snapshots(snapshot_id,region,universe,delay,instrument_type,source,dataset_count,field_count,metadata_hash,exclusion_status_json,created_at) VALUES ('disc','GLB','TOPDIV3000',1,'EQUITY','TEST',2,3,'h','{}',?)", (now,))
        specs = [
            ("c1", "d1", "f1", "RETURN", "d1/f1/NORMAL/RAW", 1),
            ("c2", "d1", "f2", "RETURN", "d1/f2/NORMAL/RANK", 2),
            ("c3", "d2", "f3", "FLOW", "d2/f3/NORMAL/TS_DELTA", 3),
        ][:candidates]
        for cid, ds, field, semantic, family, rank in specs:
            expression = f'rank({field})'
            sk, settings, settings_json, settings_hash = _identity(config, expression, decay=0)
            con.execute("""INSERT INTO ppl_candidates(candidate_id,run_id,expression,sim_key,settings_json,settings_hash,context_fingerprint,dataset_id,field_id,field_type,semantic_class,direction,signal_family,transform_family,operator,window,decay,neutralization,lifecycle_state,simulation_status,discovery_snapshot_id,dry_run_snapshot_id,cache_classification,execution_action,selection_rank,selected_for_initial_search,created_at,updated_at)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (cid,'run_0002',expression,sk,settings_json,settings_hash,f'ctx{rank}',ds,field,'MATRIX',semantic,'NORMAL',family,'RANK','rank',None,0,settings['neutralization'],'PLANNED','NONE','disc','dry','CACHE_MISS','NEW_SIMULATION_REQUIRED',rank,1,now,now))
            con.execute("INSERT INTO ppl_candidate_provenance(provenance_id,candidate_id,run_id,sim_key,context_fingerprint,discovery_snapshot_id,dry_run_snapshot_id,provenance_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (f'p{rank}',cid,'run_0002',sk,f'ctx{rank}','disc','dry',json.dumps({'candidate_stage':'PPL_INITIAL'}),now,now))
    return store, config


def test_phase10a_config_and_caps():
    assert validate_phase10a_config(cfg()) == {"total": 10, "initial": 6, "canary": 2, "repair": 4}


@pytest.mark.parametrize("field,value,error", [
    ("run_profile", "PRODUCTION_RESEARCH", "PROFILE"),
    ("validation_phase", "WRONG", "PHASE"),
    ("source_run_id", None, "SOURCE"),
])
def test_identity_rejected(field, value, error):
    c = cfg(); c.plan[field] = value
    with pytest.raises(ConfigError, match=error): validate_phase10a_config(c)


@pytest.mark.parametrize("total,concurrency,rounds,error", [
    (11, 2, 0, "TOTAL"), (10, 3, 0, "CONCURRENCY"), (10, 2, 1, "REPAIR")
])
def test_low_level_caps(total, concurrency, rounds, error):
    c=cfg(); c.plan['budgets']['max_new_simulation_posts']=total;c.plan['runtime']['concurrency']=concurrency;c.plan['budgets']['max_repair_rounds']=rounds
    with pytest.raises(ConfigError, match=error): validate_phase10a_config(c)


def test_selection_requires_miss_and_diversity():
    rows=[
        {'candidate_id':'a','sim_key':'a','selected_for_initial_search':1,'execution_action':'NEW_SIMULATION_REQUIRED','cache_classification':'CACHE_MISS','selection_rank':1,'signal_family':'x','dataset_id':'d1','field_id':'f1','semantic_class':'RETURN'},
        {'candidate_id':'b','sim_key':'b','selected_for_initial_search':1,'execution_action':'NEW_SIMULATION_REQUIRED','cache_classification':'CACHE_MISS','selection_rank':2,'signal_family':'x','dataset_id':'d1','field_id':'f1','semantic_class':'RETURN'},
        {'candidate_id':'c','sim_key':'c','selected_for_initial_search':1,'execution_action':'NEW_SIMULATION_REQUIRED','cache_classification':'CACHE_MISS','selection_rank':3,'signal_family':'y','dataset_id':'d2','field_id':'f2','semantic_class':'FLOW'},
    ]
    assert [x['candidate_id'] for x in select_canaries(rows,{})] == ['a','c']
    assert [x['candidate_id'] for x in select_canaries(rows,{'a':{'status':'COMPLETE'}})] == ['b','c']


@pytest.mark.parametrize("action,cache,selected", [
    ('CACHE_RESTORE','CACHE_COMPLETE',1),('NEW_SIMULATION_REQUIRED','CACHE_MISS',0),('RESUME_EXISTING','CACHE_MISS',1)
])
def test_selection_rejects_ineligible(action,cache,selected):
    base=[]
    for i in range(3):base.append({'candidate_id':str(i),'sim_key':str(i),'selected_for_initial_search':1,'execution_action':'NEW_SIMULATION_REQUIRED','cache_classification':'CACHE_MISS','selection_rank':i,'signal_family':str(i),'dataset_id':str(i),'field_id':str(i),'semantic_class':'RETURN'})
    base[0].update(execution_action=action,cache_classification=cache,selected_for_initial_search=selected)
    got=select_canaries(base,{})
    assert base[0] not in got


def test_create_run_isolated_and_preview(tmp_path):
    store,config=source_store(tmp_path); adb=tmp_path/'alpha.db';alpha_db(adb)
    before=store.get_run('run_0002').copy()
    out=create_validation_run(store,config,adb)
    assert out['validation_run_id']=='run_0003'; assert out['source_run_id']=='run_0002'
    assert len(store.load_candidates('run_0003'))==2
    after=store.get_run('run_0002')
    assert (before['status'],before['updated_at'])==(after['status'],after['updated_at'])
    preview=preview_phase10a(store,config,adb,'run_0003')
    assert preview['estimated_new_posts']==2 and preview['repair_posts']==0
    assert {x['source_candidate_id'] for x in preview['candidates']}=={'c1','c3'}


@pytest.mark.parametrize("status,url,action", [
    ('COMPLETE','u','CACHE_RESTORE'),('RUNNING','u','RESUME_EXISTING'),('SUBMITTED','u','RESUME_EXISTING'),('UNCERTAIN_SUBMISSION',None,'HOLD_UNCERTAIN')
])
def test_toctou_preview_never_claims_new(status,url,action,tmp_path):
    store,config=source_store(tmp_path);adb=tmp_path/'a.db';alpha_db(adb)
    create_validation_run(store,config,adb)
    first_key = store.load_candidates('run_0003')[0]['sim_key']
    with sqlite3.connect(adb) as con:con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url) VALUES (?,?,?)",(first_key,status,url));con.commit()
    p=preview_phase10a(store,config,adb,'run_0003')
    row=next(x for x in p['candidates'] if x['sim_key']==first_key)
    assert row['execution_action']==action and p['estimated_new_posts']==1


def test_no_flag_is_preview_only(tmp_path):
    store,config=source_store(tmp_path);adb=tmp_path/'a.db';alpha_db(adb);create_validation_run(store,config,adb)
    out=execute_phase10a(store,config,None,None,adb,tmp_path/'missing.py','run_0003',allow_simulation_post=False)
    assert out['executed'] is False and out['estimated_new_posts']==2


def test_schema_snapshot(tmp_path):
    adb=tmp_path/'a.db';alpha_db(adb);snap=alpha_schema_snapshot(adb)
    assert snap['tables']==['alpha_contexts','alpha_results'] and snap['row_counts']['alpha_results']==0


class FakeMachine:
    requests=requests
    build_settings = staticmethod(real_machine.build_settings)
    simulation_key = staticmethod(real_machine.simulation_key)

    def __init__(self, mapping):
        self.mapping=mapping; self.cache={}; self.settings_seen=[]

    def cache_put(self,db,key,candidate,settings,result):
        self.cache[key]=dict(result)

    def cache_get(self,db,key):
        return self.cache.get(key)

    def simulate_candidates(self,candidates,**kwargs):
        stats=kwargs['_runtime_stats'];stats.update(max_workers=2,max_futures=2,processed=2,submitted_futures=2,interrupted=False)
        for c in candidates:
            settings = self.build_settings(
                c, neutralization=kwargs['neutralization'], region=kwargs['region'],
                universe=kwargs['universe'], delay=kwargs['delay'],
                truncation=kwargs['truncation'], test_period=kwargs['test_period'],
            )
            self.settings_seen.append(settings)
            key=self.simulation_key(c['expr'], settings)
            assert key == self.mapping[c['expr']]
            self.cache_put(None,key,c,settings, {'sim_key':key,'status':'SUBMITTED','simulation_url':'https://x/simulations/'+key})
            self.cache_put(None,key,c,settings, {'sim_key':key,'status':'RUNNING','simulation_url':'https://x/simulations/'+key})
            self.cache_put(None,key,c,settings, {'sim_key':key,'status':'COMPLETE','simulation_url':'https://x/simulations/'+key,'alpha_id':'a'+key,'sharpe':1.0})
        return pd.DataFrame([{'status':'COMPLETE'} for _ in candidates])


def test_execution_lifecycle_and_repeat_preview(monkeypatch,tmp_path):
    store,config=source_store(tmp_path);adb=tmp_path/'a.db';alpha_db(adb);create_validation_run(store,config,adb)
    rows=store.load_candidates('run_0003');mapping={x['expression']:x['sim_key'] for x in rows};fake=FakeMachine(mapping)
    # Make the fake cache durable in the temporary alpha DB.
    def put(db,key,candidate,settings,result):
        fake.cache[key]=dict(result)
        with sqlite3.connect(adb) as con:
            con.execute("INSERT INTO alpha_results(sim_key,status,simulation_url,alpha_id,sharpe) VALUES (?,?,?,?,?) ON CONFLICT(sim_key) DO UPDATE SET status=excluded.status,simulation_url=excluded.simulation_url,alpha_id=excluded.alpha_id,sharpe=excluded.sharpe",(key,result.get('status'),result.get('simulation_url'),result.get('alpha_id'),result.get('sharpe')));con.commit()
    fake.cache_put=put
    monkeypatch.setattr('ppl_engine.live_execution._hash_file',lambda p:EXPECTED_MACHINE_HASH)
    out=execute_phase10a(store,config,fake,object(),adb,tmp_path/'m.py','run_0003',allow_simulation_post=True)
    assert out['repair_posts']==0 and out['runtime_stats']['max_workers']==2
    assert fake.settings_seen and all(set(real_machine.build_settings({'expr':'x','decay':0}, neutralization='SUBINDUSTRY', region='GLB', universe='TOPDIV3000', delay=1, truncation=0.08, test_period='P0Y')) <= set(x) for x in fake.settings_seen)
    assert {x['lifecycle_state'] for x in store.load_candidates('run_0003')}=={'NEAR_PASS'}
    again=preview_phase10a(store,config,adb,'run_0003')
    assert again['estimated_new_posts']==0 and {x['execution_action'] for x in again['candidates']}=={'CACHE_RESTORE'}


def test_machine_hash_constant_matches_baseline():
    assert EXPECTED_MACHINE_HASH == "58634F1EB01880EDC88B7D9904EDF3716335C35C17D57AAA0215985D82FA34E4"
