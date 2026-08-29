"""Strict Phase-9 read-only live validation boundary and evidence utilities."""
from __future__ import annotations
import hashlib,json,re,time,uuid,threading
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
from typing import Any,Dict,Mapping
from .atomic import atomic_write_json
from .check_transport import CheckBudget,CheckResponse,semantic_poll_check
from .properties import parse_alpha_properties

SENSITIVE=re.compile(r"authorization|cookie|token|password|credential|email|username|^(author|team|account|account_?id|user_?id)$",re.I)
def sanitize(value,key=""):
    if SENSITIVE.search(str(key)):return "[REDACTED]"
    if isinstance(value,Mapping):return {str(k):sanitize(v,str(k)) for k,v in value.items()}
    if isinstance(value,list):return [sanitize(x,key) for x in value]
    if isinstance(value,str) and re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}",value):return "[REDACTED_EMAIL]"
    return value
def _shape(x):
    if isinstance(x,Mapping):return {str(k):_shape(v) for k,v in sorted(x.items())}
    if isinstance(x,list):return [(_shape(x[0]) if x else "EMPTY")] 
    return type(x).__name__
def schema_fingerprint(payload):return hashlib.sha256(json.dumps(_shape(payload),sort_keys=True,separators=(",",":")).encode()).hexdigest()

class GetOnlySession:
    def __init__(self,session,max_requests=39):self.session=session;self.methods=[];self.urls=[];self.statuses=Counter();self.request_count=0;self.max_requests=max_requests
    def request(self,method,url,*a,**kw):
        method=str(method).upper()
        if method!="GET":raise RuntimeError(f"PHASE_9_READ_ONLY_VIOLATION: {method} {url}")
        if "/simulations" in str(url):raise RuntimeError("PHASE_9_READ_ONLY_VIOLATION: simulation endpoint")
        if self.request_count >= self.max_requests:raise RuntimeError("PHASE_9_LIVE_HTTP_BUDGET_EXHAUSTED")
        self.methods.append(method);self.urls.append(str(url));self.request_count+=1
        r=self.session.request(method,url,*a,**kw);self.statuses[str(r.status_code)]+=1;return r
    def __getattr__(self,n):return getattr(self.session,n)
class _SharedCheckThrottleGate:
    """Process-wide adaptive cooldown for GET /check 429 responses.

    BRAIN throttling is account/server wide, so letting every candidate run its
    own eight-request retry loop only creates a thundering herd and can stop a
    long unattended round.  The gate serializes cooldown state across all
    PRE-TAG/manual-refresh check transports in the current process.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._cooldown_until = 0.0
        self._streak = 0

    def wait(self) -> float:
        waited = 0.0
        announced = False
        while True:
            with self._lock:
                remaining = max(0.0, self._cooldown_until - time.monotonic())
            if remaining <= 0:
                return waited
            if not announced:
                print(f"[CHECK 429] global cooldown {remaining:.1f}s", flush=True)
                announced = True
            started = time.monotonic()
            time.sleep(remaining)
            waited += max(0.0, time.monotonic() - started)

    def note_429(self, *, retry_after=None, initial=60.0, maximum=600.0, multiplier=2.0) -> float:
        with self._lock:
            self._streak += 1
            try:
                header_wait = max(0.0, float(retry_after)) if retry_after is not None else 0.0
            except (TypeError, ValueError):
                header_wait = 0.0
            try:
                adaptive = float(initial) * (float(multiplier) ** max(0, self._streak - 1))
            except (TypeError, ValueError, OverflowError):
                adaptive = float(initial)
            cooldown = min(max(0.0, float(maximum)), max(header_wait, adaptive))
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + cooldown)
            return cooldown

    def note_success(self) -> None:
        with self._lock:
            self._streak = 0
            self._cooldown_until = 0.0

    def reset(self) -> None:
        self.note_success()


_CHECK_THROTTLE_GATE = _SharedCheckThrottleGate()


class MeteredLiveCheckTransport:
    def __init__(self,safe,machine,*,runtime=None):
        self.safe=safe;self.machine=machine;self.runtime=dict(runtime or {})

    def _runtime_float(self,key,default):
        try:return float(self.runtime.get(key,default))
        except (TypeError,ValueError):return float(default)

    def _runtime_int(self,key,default):
        try:return max(1,int(self.runtime.get(key,default)))
        except (TypeError,ValueError):return int(default)

    def fetch_check(self,alpha_id):
        waited=_CHECK_THROTTLE_GATE.wait()
        before=self.safe.request_count
        url=f"{self.machine.BRAIN_API_URL}/alphas/{alpha_id}/check"
        # One low-level attempt per semantic poll.  The semantic poller and the
        # shared throttle gate own retry pacing; nesting the machine-lib's
        # eight-attempt retry loop here is what previously turned one 429 into
        # minutes of hammering and an uncaught RequestFailure.
        retries=self._runtime_int("check_http_retries_per_poll",1)
        try:
            r=self.machine._request_with_retry(self.safe,"GET",url,max_retries=retries)
            status=int(r.status_code)
            if 200 <= status < 300:
                _CHECK_THROTTLE_GATE.note_success()
            elif status == 429:
                _CHECK_THROTTLE_GATE.note_429(
                    retry_after=r.headers.get("Retry-After") if hasattr(r,"headers") else None,
                    initial=self._runtime_float("check_429_initial_cooldown_seconds",60),
                    maximum=self._runtime_float("check_429_max_cooldown_seconds",600),
                    multiplier=self._runtime_float("check_429_backoff_multiplier",2.0),
                )
            return CheckResponse(status,r.text,max(1,self.safe.request_count-before),waited)
        except Exception as exc:
            status=getattr(exc,"status_code",None)
            category=str(getattr(exc,"category","") or "")
            body=str(getattr(exc,"body","") or str(exc))
            retry_after=getattr(exc,"retry_after",None)
            # Structured transient GET failures are converted into semantic
            # check responses.  They remain durable/unresolved evidence but no
            # longer abort the entire round guard.
            if status is not None:
                status=int(status)
                if status == 429:
                    cooldown=_CHECK_THROTTLE_GATE.note_429(
                        retry_after=retry_after,
                        initial=self._runtime_float("check_429_initial_cooldown_seconds",60),
                        maximum=self._runtime_float("check_429_max_cooldown_seconds",600),
                        multiplier=self._runtime_float("check_429_backoff_multiplier",2.0),
                    )
                    print(f"[CHECK 429] throttled; next GET cooldown {cooldown:.1f}s",flush=True)
                return CheckResponse(status,body,max(1,self.safe.request_count-before),waited)
            if category == "NETWORK_ERROR":
                return CheckResponse(599,body,max(1,self.safe.request_count-before),waited)
            raise

def inspect_paths(payload):
    paths={}
    for p in (("tags",),("properties","tags"),("regular","tags"),("description",),("properties","description"),("regular","description"),("submissionStatus",),("properties","submissionStatus")):
        cur=payload;ok=True
        for k in p:
            if not isinstance(cur,Mapping) or k not in cur:ok=False;break
            cur=cur[k]
        paths[".".join(p)]={"present":ok,"type":type(cur).__name__ if ok else None,"empty":not bool(cur) if ok else None}
    return paths
def run_live_validation(machine,base_session,store,config,output_path,fixture_dir,targets,*,detail_targets=None,check_targets=None,initial_details=None):
    safe=GetOnlySession(base_session);sid="liveval_"+uuid.uuid4().hex;started=datetime.now(timezone.utc).isoformat();evidence=[];details=list(initial_details or []);checks=[]
    fixture_dir.mkdir(parents=True,exist_ok=True)
    selected_details=list(detail_targets if detail_targets is not None else targets[:3])
    selected_checks=list(check_targets if check_targets is not None else targets[:2])
    for alpha_id in selected_details:
        try:
            r=machine._request_with_retry(safe,"GET",f"{machine.BRAIN_API_URL}/alphas/{alpha_id}");payload=r.json();clean=sanitize(payload);prop=parse_alpha_properties(clean,evidence_source="LIVE_VALIDATION")
            obs={"top_level_keys":sorted(clean),"paths":inspect_paths(clean),"power_pool_selected_status":prop["power_pool_selected_status"],"submission_status":prop["submission_status"]}
            fp=schema_fingerprint(clean);details.append({"alpha_id":alpha_id,"http_status":r.status_code,"schema_fingerprint":fp,"observations":obs});atomic_write_json(fixture_dir/f"alpha_details_{alpha_id}_sanitized.json",clean)
            evidence.append({"target_type":"ALPHA_DETAILS","target_id":alpha_id,"endpoint":f"/alphas/{alpha_id}","http_method":"GET","http_status":r.status_code,"raw_payload_sanitized":clean,"schema_fingerprint":fp,"parser_version":prop["property_parser_version"],"observations":obs})
        except Exception as exc:details.append({"alpha_id":alpha_id,"error":f"{type(exc).__name__}: {exc}"})
    transport=MeteredLiveCheckTransport(safe,machine);budget=CheckBudget(2,20,8,1)
    for alpha_id in selected_checks:
        out=semantic_poll_check(transport,alpha_id=alpha_id,phase="FINAL",rules=config.rules,budget=budget,evidence_source="LIVE_VALIDATION",wait=time.sleep,store=None)
        raw=(out.get("polls") or [{}])[-1].get("raw_response_text","{}");
        try:payload=json.loads(raw)
        except:payload={"raw_text":raw[:1000]}
        clean=sanitize(payload);fp=schema_fingerprint(clean);atomic_write_json(fixture_dir/f"check_{alpha_id}_sanitized.json",clean)
        names=[x.get("raw_name") for x in (out.get("final") or {}).get("results",[])];unknown=[x.get("raw_name") for x in (out.get("final") or {}).get("results",[]) if x.get("normalized_name")=="UNKNOWN"]
        item={"alpha_id":alpha_id,"session_status":out.get("session_status"),"semantic_poll_count":out.get("poll_count"),"actual_http_request_count":out.get("http_request_count"),"raw_check_names":names,"unknown_check_names":unknown,"schema_fingerprint":fp,"parsed":out.get("final")};checks.append(item);evidence.append({"target_type":"CHECK","target_id":alpha_id,"endpoint":f"/alphas/{alpha_id}/check","http_method":"GET","http_status":(out.get("polls") or [{}])[-1].get("http_status"),"raw_payload_sanitized":clean,"schema_fingerprint":fp,"parser_version":(out.get("final") or {}).get("check_parser_version"),"observations":item})
    dataset_observation={"status":"NOT_ATTEMPTED"}
    try:
        settings=config.plan["simulation_settings"]
        frame=machine.get_datasets(safe,instrument_type=settings["instrument_type"],region=settings["region"],delay=settings["delay"],universe=settings["universe"])
        records=json.loads(frame.to_json(orient="records"))
        clean=sanitize(records);fp=schema_fingerprint(clean)
        exact=any(str(row.get("id"))=="model110" for row in records)
        dataset_observation={"status":"MODEL110_EXACT_CONFIRMED" if exact else "EXCLUSION_UNMATCHED_WARNING","dataset_count":len(records),"model110_exact":exact,"schema_fingerprint":fp}
        atomic_write_json(fixture_dir/"datasets_glb_d1_topdiv3000_sanitized.json",clean)
        evidence.append({"target_type":"DATASET","target_id":"GLB_D1_TOPDIV3000","endpoint":"/data-sets","http_method":"GET","http_status":200,"raw_payload_sanitized":clean,"schema_fingerprint":fp,"parser_version":None,"observations":dataset_observation})
    except Exception as exc:
        dataset_observation={"status":"DATASET_VALIDATION_ERROR","error":f"{type(exc).__name__}: {exc}"}
    store.save_live_validation(sid,started,evidence,{"authentication_requests":1,"details":details,"checks":checks,"dataset":dataset_observation})
    report={"validation_session_id":sid,"mode":"VALIDATION_RESULT_ONLY","authentication_requests":1,"http_methods":{"POST_AUTHENTICATION":1,"GET":safe.request_count},"status_codes":dict(safe.statuses),"alpha_details":details,"checks":checks,"dataset":dataset_observation,"operators":{"available":False,"reason":"NO_CONFIRMED_ENDPOINT"},"theme_endpoint":"THEME_ENDPOINT_UNAVAILABLE","total_live_http_requests":1+safe.request_count,"writes_to_brain":0,"simulation_posts":0,"run_lifecycle_changes":0}
    atomic_write_json(output_path,report);return report
