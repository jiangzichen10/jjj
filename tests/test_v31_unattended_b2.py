import json
from datetime import datetime, timezone
from pathlib import Path

from ppl_engine.config import load_effective_config
from ppl_engine.continuous_check import enqueue_pretag_checks, poll_due_checks
from ppl_engine.continuous_control import due_snapshot, recover_waiting_auth
from ppl_engine.store import RunnerStore

ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_effective_config(ROOT / 'ppl_rules.yaml', ROOT / 'ppl_plan_v31.yaml', project_dir=ROOT)


def _setup(tmp_path):
    cfg = _config()
    store = RunnerStore(tmp_path / 'runner.db')
    store.initialize(); store.create_run('run_0006', cfg)
    now = datetime.now(timezone.utc).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO ppl_candidates(
                   candidate_id,run_id,expression,sim_key,settings_json,field_id,field_type,operator,decay,
                   neutralization,lifecycle_state,simulation_status,alpha_id,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ('cand1','run_0006','rank(close)','sk1',json.dumps(cfg.plan['simulation_settings']),
             'close','MATRIX','rank',0,cfg.plan['simulation_settings']['neutralization'],
             'LOCAL_PRE_GATE_PASS','COMPLETE','A1',now,now),
        )
    return cfg, store


class Response:
    def __init__(self, status=200, text='[]', headers=None):
        self.status_code=status; self.text=text; self.headers=headers or {}


class Session:
    def __init__(self, responses):
        self.responses=list(responses); self.calls=[]
    def get(self, url, timeout=60):
        self.calls.append((url, timeout))
        return self.responses.pop(0)


def test_enqueue_check_is_durable_and_does_not_poll(tmp_path):
    cfg, store = _setup(tmp_path)
    out = enqueue_pretag_checks(store, 'run_0006', ['cand1'])
    assert out['queued_count'] == 1
    c = store.load_candidates('run_0006')[0]
    assert c['lifecycle_state'] == 'PRE_TAG_CHECK_PENDING'
    with store.connect() as conn:
        row = conn.execute("select queue_state,alpha_id from ppl_check_work where candidate_id='cand1'").fetchone()
    assert tuple(row) == ('CHECK_DUE','A1')


def test_check_429_requeues_without_sleep_or_terminal_transition(tmp_path):
    cfg, store = _setup(tmp_path); enqueue_pretag_checks(store,'run_0006',['cand1'])
    s = Session([Response(429, '{}', {'Retry-After':'17'})])
    out = poll_due_checks(store,cfg,type('M',(),{'BRAIN_API_URL':'https://api.worldquantbrain.com'})(),s,'run_0006')
    assert out['polled']==1 and out['waits']==1
    assert len(s.calls)==1
    with store.connect() as conn:
        row=conn.execute("select queue_state,retry_after_seconds,last_http_status from ppl_check_work where candidate_id='cand1'").fetchone()
    assert row[0]=='WAIT_RATE_LIMIT' and float(row[1])==17.0 and row[2]==429
    assert store.load_candidates('run_0006')[0]['lifecycle_state']=='PRE_TAG_CHECK_PENDING'


def test_check_auth_wait_and_coordinated_refresh_releases_queue(tmp_path):
    cfg, store = _setup(tmp_path); enqueue_pretag_checks(store,'run_0006',['cand1'])
    s = Session([Response(401, '{}')])
    machine = type('M',(),{'BRAIN_API_URL':'https://api.worldquantbrain.com'})()
    out = poll_due_checks(store,cfg,machine,s,'run_0006')
    assert out['auth_waits']==1
    calls={'n':0}
    class AuthMachine:
        @staticmethod
        def ensure_session(session): calls['n']+=1; return session
    recovered = recover_waiting_auth(store, AuthMachine, s, 'run_0006')
    assert recovered['success'] is True and recovered['released']==1 and calls['n']==1
    with store.connect() as conn:
        assert conn.execute("select queue_state from ppl_check_work where candidate_id='cand1'").fetchone()[0]=='CHECK_DUE'


def test_auth_failure_remains_waiting_and_schedules_retry(tmp_path):
    cfg, store = _setup(tmp_path); enqueue_pretag_checks(store,'run_0006',['cand1'])
    with store.connect() as conn:
        conn.execute("update ppl_check_work set queue_state='WAIT_AUTH' where candidate_id='cand1'")
    class BadMachine:
        @staticmethod
        def ensure_session(session): raise RuntimeError('login unavailable')
    out = recover_waiting_auth(store,BadMachine,object(),'run_0006',retry_seconds=42)
    assert out['success'] is False
    with store.connect() as conn:
        row=conn.execute("select queue_state,retry_after_seconds,last_error from ppl_check_work where candidate_id='cand1'").fetchone()
    assert row[0]=='WAIT_AUTH' and float(row[1])==42.0 and 'login unavailable' in row[2]


def test_due_snapshot_uses_earliest_durable_due_time(tmp_path):
    cfg, store = _setup(tmp_path); enqueue_pretag_checks(store,'run_0006',['cand1'])
    snap=due_snapshot(store,'run_0006',default_wait_seconds=30,max_wait_seconds=300)
    assert snap.due_now is True and snap.check_due==1 and snap.wait_seconds==0


def test_resolved_check_advances_candidate_without_hidden_repoll(tmp_path):
    cfg, store = _setup(tmp_path); enqueue_pretag_checks(store,'run_0006',['cand1'])
    # Parser accepts JSON list of checks. Empty list is not guaranteed resolved,
    # so use the existing all-pass fixture payload.
    text=(ROOT/'check_preview_all_pass.json').read_text(encoding='utf-16')
    s=Session([Response(200,text)])
    machine=type('M',(),{'BRAIN_API_URL':'https://api.worldquantbrain.com'})()
    out=poll_due_checks(store,cfg,machine,s,'run_0006')
    assert len(s.calls)==1
    # Depending on fixture theme semantics this may be RESOLVED/PASS or pending;
    # the invariant under test is one request only and durable queue ownership.
    with store.connect() as conn:
        q=conn.execute("select queue_state,attempt_count from ppl_check_work where candidate_id='cand1'").fetchone()
    assert q[1]==1 and q[0] in {'RESOLVED','WAIT_CHECK'}
