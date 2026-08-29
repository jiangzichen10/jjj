import json,sqlite3
from pathlib import Path
import pytest
from ppl_engine.config import load_effective_config
from ppl_engine.live_validation import GetOnlySession,sanitize,schema_fingerprint
from ppl_engine.check_parser import parse_check_payload
from ppl_engine.properties import parse_alpha_properties
from ppl_engine.store import RunnerStore,SCHEMA_VERSION
ROOT=Path(__file__).resolve().parents[1]
class Resp:
 status_code=200
class Fake:
 def __init__(self):self.calls=[]
 def request(self,m,u,*a,**k):self.calls.append((m,u));return Resp()
def test_firewall_get_only():assert GetOnlySession(Fake()).request('GET','https://x/alphas/a').status_code==200
@pytest.mark.parametrize('method',['POST','PATCH','PUT','DELETE'])
def test_firewall_blocks_writes(method):
 with pytest.raises(RuntimeError,match='READ_ONLY_VIOLATION'):GetOnlySession(Fake()).request(method,'https://x/alphas/a')
def test_firewall_blocks_simulation_even_get():
 with pytest.raises(RuntimeError):GetOnlySession(Fake()).request('GET','https://x/simulations')
def test_sanitize_secrets():
 x=sanitize({'token':'x','cookie':'y','email':'a@b.com','author':'acct-1','userCount':9,'nested':{'Authorization':'z'}});assert all('x' not in json.dumps(x) for _ in [0]) and '[REDACTED]' in json.dumps(x) and x['author']=='[REDACTED]' and x['userCount']==9
def test_fingerprint_stable_values_ignored():
 assert schema_fingerprint({'a':1,'b':['x']})==schema_fingerprint({'a':9,'b':['y']})
 assert schema_fingerprint({'a':1})!=schema_fingerprint({'a':'1'})
def test_top_level_tags_and_regular_description():
 p=parse_alpha_properties({'tags':['PowerPoolSelected'],'regular':{'description':'d'}});assert p['power_pool_selected_status']=='PRESENT' and p['description_text']=='d' and p['property_parser_version']==2
def test_nested_v1_compatible():assert parse_alpha_properties({'properties':{'tags':[{'name':'PowerPoolSelected'}],'description':'d'}})['power_pool_selected_status']=='PRESENT'
def test_unknown_description_and_tag_fail_closed():
 p=parse_alpha_properties({'availableTags':['PowerPoolSelected'],'metadata':{'description':'d'}});assert p['power_pool_selected_status']=='UNKNOWN' and not p['description_present']
def test_active_not_submitted():assert parse_alpha_properties({'status':'ACTIVE','tags':[]})['submission_status']=='UNKNOWN'
def test_available_tag_and_description_string_not_present():assert parse_alpha_properties({'description':'PowerPoolSelected','availableTags':['PowerPoolSelected']})['power_pool_selected_status']=='UNKNOWN'
def test_validation_schema_tables(tmp_path):
 s=RunnerStore(tmp_path/'r.db');s.initialize();assert SCHEMA_VERSION==13
 with s.connect() as c:assert c.execute("select count(*) from ppl_live_validation_evidence").fetchone()[0]==0
def test_validation_evidence_separate(tmp_path):
 s=RunnerStore(tmp_path/'r.db');s.initialize();s.save_live_validation('v','t',[{'target_type':'ALPHA_DETAILS','target_id':'a','endpoint':'/alphas/a','http_method':'GET','http_status':200,'raw_payload_sanitized':{},'schema_fingerprint':'f','parser_version':2,'observations':{}}],{})
 with s.connect() as c:
  assert c.execute('select count(*) from ppl_live_validation_evidence').fetchone()[0]==1
  assert c.execute('select count(*) from ppl_property_snapshots').fetchone()[0]==0 and c.execute('select count(*) from ppl_check_sessions').fetchone()[0]==0

def test_real_live_check_fixture_exact_aliases_and_resolves():
 payload=json.loads((ROOT/'tests/fixtures/live_sanitized/check_WjAxWeoG_sanitized.json').read_text(encoding='utf-8'))
 parsed=parse_check_payload(payload,phase='FINAL',rules=load_effective_config(ROOT/'ppl_rules.yaml',ROOT/'ppl_plan.yaml',project_dir=ROOT).rules,evidence_source='LIVE_VALIDATION_FIXTURE')
 by_raw={x['raw_name']:x for x in parsed['results']}
 assert by_raw['LOW_SHARPE']['normalized_name']=='SHARPE'
 assert by_raw['HIGH_TURNOVER']['normalized_name']=='TURNOVER'
 assert by_raw['HT_TURNOVER']['normalized_name']=='HIGH_TURNOVER'
 assert by_raw['HT_HIGH_TURNOVER_RETURNS_RATIO']['normalized_name']=='HIGH_TURNOVER_RETURNS_RATIO'
 assert by_raw['LOW_SUB_UNIVERSE_SHARPE']['normalized_name']=='SUB_UNIVERSE'
 assert by_raw['POWER_POOL_CORRELATION']['normalized_name']=='POWER_POOL_CORRELATION'
 assert parsed['session_semantic_status']=='RESOLVED'
 assert parsed['base_gate']['status']=='WARNING' and parsed['theme_gate']['status']=='WARNING'
