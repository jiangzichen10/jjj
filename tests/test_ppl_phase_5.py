import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from ppl_engine.check_parser import (
    CHECK_ALIAS_VERSION, CHECK_PARSER_VERSION, build_check_summary,
    evaluate_live_base_gate, evaluate_live_theme_gate, normalize_check_name,
    normalize_result, parse_check_payload, parse_response_text,
)
from ppl_engine.check_transport import CheckBudget, CheckResponse, MeteredSession, semantic_poll_check
from ppl_engine.config import load_effective_config
from ppl_engine.contracts import CandidateLifecycle
from ppl_engine.store import RunnerStore


ROOT=Path(__file__).resolve().parents[1]; FIX=ROOT/"tests"/"fixtures"


def _file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


@pytest.fixture(scope="module", autouse=True)
def _project_runner_db_read_only_guard():
    """Phase-5 tests must never create or mutate the project production DB."""
    path = ROOT / "ppl_runner.db"
    existed = path.exists()
    before = _file_hash(path) if existed else None
    yield {"existed": existed, "hash": before}
    assert path.exists() == existed, "Phase-5 tests changed ppl_runner.db presence"
    if existed:
        assert _file_hash(path) == before, "Phase-5 tests modified ppl_runner.db"


def config(): return load_effective_config(ROOT/"ppl_rules.yaml",ROOT/"ppl_plan.yaml",project_dir=ROOT)
def load(name): return json.loads((FIX/name).read_text(encoding="utf-8"))
def parse(name, phase=None):
    data=load(name); return parse_check_payload(data,phase=phase or data.get("phase","PRE_TAG"),rules=config().rules,evidence_source=data["evidence_source"])


class FakeTransport:
    def __init__(self,responses): self.responses=list(responses); self.calls=0
    def fetch_check(self,alpha_id):
        self.calls+=1
        return self.responses.pop(0) if self.responses else CheckResponse(200,'{"checks":[]}')


def budget(**kw):
    values=dict(max_check_candidates=30,max_check_http_requests=100,max_poll_requests_per_candidate=5,max_check_sessions_per_candidate=3)
    values.update(kw); return CheckBudget(**values)


def test_session_model_resolved():
    text=json.dumps(load("check_all_pass.json")); t=FakeTransport([CheckResponse(200,text)])
    out=semantic_poll_check(t,alpha_id="a",phase="FINAL",rules=config().rules,budget=budget())
    assert out["session_status"]=="RESOLVED" and out["poll_count"]==out["http_request_count"]==1


def test_poll_and_individual_results_persist(tmp_path):
    store=RunnerStore(tmp_path/"r.db"); store.initialize()
    text=json.dumps(load("check_all_pass.json")); out=semantic_poll_check(FakeTransport([CheckResponse(200,text)]),alpha_id="a",phase="FINAL",rules=config().rules,budget=budget(),store=store)
    with store.connect() as c:
        assert c.execute("select count(*) from ppl_check_sessions").fetchone()[0]==1
        assert c.execute("select count(*) from ppl_check_polls").fetchone()[0]==1
        assert c.execute("select count(*) from ppl_check_results").fetchone()[0]==10


@pytest.mark.parametrize("phase",["PRE_TAG","RECHECK","FINAL"])
def test_all_check_phases(phase):
    out=parse("check_all_pass.json",phase)
    assert out["phase"]==phase


@pytest.mark.parametrize("raw,canonical",[
    ("POWER_POOL_CORRELATION","POWER_POOL_CORRELATION"),
    ("Power Pool Correlation","POWER_POOL_CORRELATION"),
    ("SUBUNIVERSE","SUB_UNIVERSE"),
])
def test_exact_and_normalized_aliases(raw,canonical): assert normalize_check_name(raw)["normalized_name"]==canonical


def test_unknown_name_is_conservative():
    x=normalize_check_name("POWER POOL CORR MAYBE")
    assert x["normalized_name"]==x["category"]=="UNKNOWN" and x["mapping_suggestion"]


def test_prod_corr_is_not_pp_corr(): assert normalize_check_name("PROD_CORRELATION")["normalized_name"]=="PROD_CORRELATION"


def test_pp_corr_and_self_corr_remain_distinct():
    assert normalize_check_name("POWER_POOL_CORRELATION")["normalized_name"] != normalize_check_name("POWER_POOL_SELF_CORRELATION")["normalized_name"]


@pytest.mark.parametrize("raw,expected",[("PASS","PASS"),("FAIL","FAIL"),("WARNING","WARNING"),("PENDING","PENDING"),("x","UNKNOWN"),("N/A","NOT_APPLICABLE")])
def test_result_normalization(raw,expected): assert normalize_result(raw)==expected


def test_warning_is_not_pass():
    out=parse("check_warning_only.json"); assert out["pre_tag_check_pass"] is False and out["session_semantic_status"]=="RESOLVED"


def test_pending_and_empty_never_pass():
    assert parse("check_pending.json")["session_semantic_status"]=="PENDING"
    assert parse("check_empty.json")["session_semantic_status"]=="PENDING"


def test_missing_required_pre_tag_pending_but_pp_corr_deferred():
    out=parse("check_missing_required_pre_tag.json")
    assert out["base_gate"]["checks"]["POWER_POOL_CORRELATION"]=="DEFERRED"
    assert out["base_gate"]["status"]=="PENDING"


def test_pre_tag_base_provisional_when_only_corr_missing():
    data=load("check_all_pass.json"); data["is"]["checks"]=[x for x in data["is"]["checks"] if x["name"]!="POWER_POOL_CORRELATION"]
    out=parse_check_payload(data,phase="PRE_TAG",rules=config().rules,evidence_source="SYNTHETIC_TEST")
    assert out["base_gate"]["status"]=="PROVISIONAL_PASS" and out["pre_tag_check_pass"]


def test_missing_pp_corr_final_cannot_pass():
    out=parse("check_missing_required_final.json")
    assert out["base_gate"]["status"]=="UNKNOWN" and not out["final_check_pass"]
    assert "MISSING_REQUIRED_FINAL_CHECK" in out["base_gate"]["errors"]


def test_final_all_pass():
    out=parse("check_all_pass.json"); assert out["base_gate"]["status"]==out["theme_gate"]["status"]=="PASS" and out["final_check_pass"]


def test_theme_fail_is_separate():
    data=load("check_all_pass.json"); next(x for x in data["is"]["checks"] if x["name"]=="THEME_MATCH")["result"]="FAIL"
    out=parse_check_payload(data,phase="FINAL",rules=config().rules,evidence_source="SYNTHETIC_TEST")
    assert out["base_gate"]["status"]=="PASS" and out["theme_gate"]["status"]=="FAIL"


def test_generic_fail_does_not_fail_ppl_gate():
    data=load("check_all_pass.json"); data["is"]["checks"].append({"name":"FITNESS","result":"FAIL"})
    out=parse_check_payload(data,phase="FINAL",rules=config().rules,evidence_source="SYNTHETIC_TEST")
    assert out["base_gate"]["status"]==out["theme_gate"]["status"]=="PASS"


def test_wjax_parallel_facts_no_false_causality():
    data=load("check_high_corr_WjAxWeoG.json"); out=parse_check_payload(data,phase="FINAL",rules=config().rules,evidence_source=data["evidence_source"])
    by={x["normalized_name"]:x for x in out["results"] if x["category"] in {"PPL_BASE","PPL_THEME"}}
    assert by["HIGH_TURNOVER"]["normalized_result"]=="PASS"
    assert by["HIGH_TURNOVER_RETURNS_RATIO"]["raw_value"]==.8224 and by["HIGH_TURNOVER_RETURNS_RATIO"]["normalized_result"]=="PASS"
    assert by["POWER_POOL_CORRELATION"]["raw_value"]==.8797 and by["POWER_POOL_CORRELATION"]["raw_limit"]==.5 and by["POWER_POOL_CORRELATION"]["normalized_result"]=="FAIL"
    assert by["THEME_MATCH"]["normalized_result"]=="FAIL"
    assert data["facts"]["theme_root_cause"]=="UNKNOWN"
    assert out["session_semantic_status"]=="RESOLVED"  # no explicit PENDING; gate completeness is tracked separately


def test_live_limit_precedence_and_preset_fallback():
    live=parse("check_live_limit.json")["results"][0]
    assert live["effective_limit"]==.77 and live["limit_source"]=="LIVE_CHECK"
    raw={"name":"HIGH_TURNOVER_RETURNS_RATIO","result":"PASS"}
    from ppl_engine.check_parser import parse_individual_check
    fallback=parse_individual_check(raw,"PRE_TAG",config().rules,"SYNTHETIC_TEST")
    assert fallback["effective_limit"]==.75 and fallback["limit_source"]=="PRESET"


def test_unknown_unit_keeps_raw_and_does_not_normalize():
    item=parse("check_unit_unknown.json")["results"][0]
    assert item["raw_value"]==60.8 and item["normalized_value"] is None and item["unit_confidence"]=="UNKNOWN"


def test_json_decode_error_is_transient():
    out=parse_response_text((FIX/"check_non_json.txt").read_text(),phase="PRE_TAG",rules=config().rules,evidence_source="SYNTHETIC_TEST")
    assert out["error_type"]=="JSON_DECODE_ERROR" and out["error_nature"]=="TRANSIENT"


@pytest.mark.parametrize("status,error",[(429,"HTTP_429"),(503,"HTTP_5XX")])
def test_http_transient_errors(status,error):
    out=semantic_poll_check(FakeTransport([CheckResponse(status,"x")]),alpha_id="a",phase="PRE_TAG",rules=config().rules,budget=budget(max_poll_requests_per_candidate=1))
    assert out["session_status"]=="BUDGET_EXHAUSTED" and out["polls"][0]["parsed"]["error_type"]==error


def test_429_throttle_event_cap_defers_instead_of_raising():
    t=FakeTransport([CheckResponse(429,"x"),CheckResponse(429,"x"),CheckResponse(200,json.dumps(load("check_all_pass.json")))])
    out=semantic_poll_check(
        t,alpha_id="a",phase="PRE_TAG",rules={**config().rules,"check":{"timeout_seconds":300,"poll_seconds":0}},
        budget=budget(max_poll_requests_per_candidate=5),throttle_max_events=2,
    )
    assert out["session_status"]=="BUDGET_EXHAUSTED"
    assert out["error_type"]=="HTTP_429_THROTTLE_DEFERRED"
    assert out["error_nature"]=="TRANSIENT"
    assert out["throttle_events"]==2
    assert t.calls==2


def test_throttle_gate_wait_is_excluded_from_semantic_timeout():
    values=iter([0,0,100,100])
    clock=lambda: next(values)
    text=json.dumps(load("check_all_pass.json"))
    t=FakeTransport([CheckResponse(200,text,1,100.0)])
    out=semantic_poll_check(
        t,alpha_id="a",phase="PRE_TAG",rules={**config().rules,"check":{"timeout_seconds":1,"poll_seconds":0}},
        budget=budget(),clock=clock,
    )
    assert out["session_status"]=="RESOLVED"
    assert out["throttle_wait_seconds"]==pytest.approx(100.0)


def test_semantic_polling_repeats_once_per_fetch():
    pending=json.dumps(load("check_pending.json")); passed=json.dumps(load("check_all_pass.json"))
    t=FakeTransport([CheckResponse(200,pending,2),CheckResponse(200,passed,1)]); b=budget()
    out=semantic_poll_check(t,alpha_id="a",phase="FINAL",rules=config().rules,budget=b)
    assert t.calls==2 and out["poll_count"]==2 and out["http_request_count"]==3 and b.pending_poll_requests==1


def test_candidate_poll_budget_exhausted():
    t=FakeTransport([CheckResponse(200,json.dumps(load("check_pending.json")))])
    out=semantic_poll_check(t,alpha_id="a",phase="PRE_TAG",rules=config().rules,budget=budget(max_poll_requests_per_candidate=1))
    assert out["session_status"]=="BUDGET_EXHAUSTED"


def test_total_http_budget_exhausted_before_fetch():
    b=budget(max_check_http_requests=0); t=FakeTransport([])
    out=semantic_poll_check(t,alpha_id="a",phase="PRE_TAG",rules=config().rules,budget=b)
    assert out["session_status"]=="BUDGET_EXHAUSTED" and t.calls==0


def test_candidate_and_session_budgets_are_counted_separately():
    b=budget(max_check_sessions_per_candidate=2); text=json.dumps(load("check_all_pass.json"))
    for phase in ("FINAL","FINAL"):
        assert semantic_poll_check(FakeTransport([CheckResponse(200,text)]),alpha_id="a",phase=phase,rules=config().rules,budget=b)["session_status"]=="RESOLVED"
    third=semantic_poll_check(FakeTransport([CheckResponse(200,text)]),alpha_id="a",phase="FINAL",rules=config().rules,budget=b)
    assert b.check_candidates==1 and b.check_sessions==2 and third["session_status"]=="BUDGET_EXHAUSTED"


def test_timeout_keeps_pending():
    values=iter([0,0,10]); clock=lambda: next(values)
    out=semantic_poll_check(FakeTransport([CheckResponse(200,json.dumps(load("check_pending.json")))]),alpha_id="a",phase="PRE_TAG",rules={**config().rules,"check":{"timeout_seconds":1,"poll_seconds":0}},budget=budget(),clock=clock)
    assert out["session_status"]=="TIMEOUT" and out["error_type"]=="POLL_TIMEOUT"


def test_parser_and_alias_versions_saved(tmp_path):
    store=RunnerStore(tmp_path/"r.db"); store.initialize(); text=json.dumps(load("check_all_pass.json"))
    semantic_poll_check(FakeTransport([CheckResponse(200,text)]),alpha_id="a",phase="FINAL",rules=config().rules,budget=budget(),store=store)
    with store.connect() as c:
        assert tuple(c.execute("select distinct parser_version,alias_version from ppl_check_polls").fetchone())==(CHECK_PARSER_VERSION,CHECK_ALIAS_VERSION)
        assert tuple(c.execute("select distinct parser_version,alias_version from ppl_check_results").fetchone())==(CHECK_PARSER_VERSION,CHECK_ALIAS_VERSION)


def test_raw_evidence_is_immutable_across_sessions(tmp_path):
    store=RunnerStore(tmp_path/"r.db"); store.initialize(); text=json.dumps(load("check_all_pass.json"),sort_keys=True)
    for _ in range(2): semantic_poll_check(FakeTransport([CheckResponse(200,text)]),alpha_id="a",phase="FINAL",rules=config().rules,budget=budget(),store=store)
    with store.connect() as c:
        rows=c.execute("select raw_response_text from ppl_check_polls order by poll_id").fetchall()
    assert len(rows)==2 and rows[0][0]==rows[1][0]==text


def test_unknown_checks_enter_low_token_summary():
    out=parse("check_unknown_name.json"); summary=build_check_summary(out)
    assert summary["unknown_check_count"]==1 and summary["unknown_check_names"]==["MYSTERY_NEW_CHECK"]
    assert "results" not in summary


def test_metered_session_counts_request_calls():
    class S:
        def request(self,*a,**k): return "ok"
    s=MeteredSession(S()); assert s.request("GET","u")=="ok" and s.request_count==1


def test_check_complete_lifecycle_states_exist():
    assert CandidateLifecycle.PRE_TAG_CHECK_COMPLETE.value=="PRE_TAG_CHECK_COMPLETE"
    assert CandidateLifecycle.FINAL_CHECK_COMPLETE.value=="FINAL_CHECK_COMPLETE"


def test_phase5_project_db_guard_is_snapshot_independent(_project_runner_db_read_only_guard):
    # The module-level guard works whether a clean package has no project DB or
    # a developer checkout happens to contain one. No historical row counts are
    # required for this unit-test suite.
    assert set(_project_runner_db_read_only_guard) == {"existed", "hash"}


def test_metered_live_check_transport_converts_final_429_to_response():
    from ppl_engine import live_validation as lv

    class Safe:
        def __init__(self): self.request_count=0

    class Failure(RuntimeError):
        status_code=429
        category="HTTP_ERROR"
        body='{"detail":"THROTTLED"}'
        retry_after=None

    class Machine:
        BRAIN_API_URL="https://example.test"
        @staticmethod
        def _request_with_retry(safe,method,url,**kwargs):
            safe.request_count+=1
            assert kwargs.get("max_retries")==1
            raise Failure("throttled")

    lv._CHECK_THROTTLE_GATE.reset()
    safe=Safe()
    t=lv.MeteredLiveCheckTransport(
        safe,Machine(),runtime={
            "check_http_retries_per_poll":1,
            "check_429_initial_cooldown_seconds":0,
            "check_429_max_cooldown_seconds":0,
        },
    )
    out=t.fetch_check("A")
    assert out.http_status==429
    assert out.http_request_count==1
    assert "THROTTLED" in out.text
    lv._CHECK_THROTTLE_GATE.reset()
