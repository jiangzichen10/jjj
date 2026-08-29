"""
WorldQuant BRAIN Alpha Search Library - V2.1

V2.1 goals:
1) Keep the durable V2 simulation/cache/resume engine unchanged in identity semantics.
2) Filter Data Fields before Stage-1 using a configurable Data Coverage threshold.
3) Add POWER_POOL / ATOM / POWER_POOL_ATOM / REGULAR research modes without hard-coding Power Pool regions.
4) Track candidate provenance and strategy metadata separately from simulation identity.
5) Use 5/22/66 as the core search and move 120 + arg/quantile operators to targeted extension.
6) Make Stage-2 target-aware so single-dataset modes avoid cross-dataset helper fields.
7) Add submission-check-aware repair while preserving the legacy turnover repair fallback.
8) Enforce configurable regional concurrency protection (GLB default max 4, other regions default max 8).
"""

from __future__ import annotations

import os
import re
import time
import json
import math
import copy
import hashlib
import sqlite3
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from http.client import RemoteDisconnected
from pathlib import Path
from time import sleep
from itertools import product
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests


BRAIN_API_URL = "https://api.worldquantbrain.com"

SUBMIT_STAGGER_SECONDS = 0.4

# Local polling timeout protection:
# A simulation that remains RUNNING after repeated polling failures for this
# duration is marked STALE_RUNNING instead of occupying the scheduler forever.
# The saved simulation_url is preserved and can be resumed later.
STALE_RUNNING_AFTER_SECONDS = 21600

_AUTH_REFRESH_LOCK = threading.Lock()
_CACHE_INIT_LOCK = threading.Lock()
_CACHE_WRITE_LOCK = threading.RLock()
_PRINT_LOCK = threading.Lock()

# ---------- Search-space defaults ----------

TARGET_REGULAR = "REGULAR"
TARGET_ATOM = "ATOM"
TARGET_POWER_POOL = "POWER_POOL"
TARGET_POWER_POOL_ATOM = "POWER_POOL_ATOM"
TARGET_MODES = {
    TARGET_REGULAR,
    TARGET_ATOM,
    TARGET_POWER_POOL,
    TARGET_POWER_POOL_ATOM,
}

# These grouping fields are treated as support fields by the local structural
# classifier. Power Pool region eligibility is intentionally NOT hard-coded;
# current platform checks remain the source of truth because supported regions
# can change.
SUPPORT_GROUP_FIELDS = {
    "country",
    "exchange",
    "market",
    "sector",
    "industry",
    "subindustry",
    "currency",
}

CORE_TS_OPS = ["ts_rank", "ts_zscore", "ts_delta", "ts_mean", "ts_std_dev"]

# Core Stage-1: deliberately no 120-day sweep.
OP_WINDOWS = {
    "ts_delta": [5, 22, 66],
    "ts_rank": [22, 66],
    "ts_zscore": [22, 66],
    "ts_mean": [5, 22, 66],
    "ts_std_dev": [22, 66],
    # Legacy / explicitly requested operators still use compact defaults.
    "ts_sum": [5, 22, 66],
    "ts_delay": [5, 22, 66],
    "ts_scale": [22, 66],
}

# Extended Stage-1 is only generated for fields that show promise in Core.
EXTENDED_OP_WINDOWS = {
    "ts_rank": [120],
    "ts_zscore": [120],
    "ts_arg_min": [22, 66],
    "ts_arg_max": [22, 66],
    "ts_quantile": [22, 66, 120],
}

basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
ts_ops = CORE_TS_OPS[:]
ops_set = basic_ops + ts_ops


def _now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat()


def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _json_safe(value: Any) -> Any:
    """Convert common pandas/numpy values into JSON-safe Python values."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _nested_value(value: Any, key: str, default: Any = None) -> Any:
    return value.get(key, default) if isinstance(value, dict) else default


def normalize_target_mode(target_mode: Optional[str]) -> str:
    mode = str(target_mode or TARGET_REGULAR).strip().upper()
    aliases = {
        "PPL": TARGET_POWER_POOL,
        "PP": TARGET_POWER_POOL,
        "POWERPOOL": TARGET_POWER_POOL,
        "PP_ATOM": TARGET_POWER_POOL_ATOM,
        "PPL_ATOM": TARGET_POWER_POOL_ATOM,
        "POWERPOOL_ATOM": TARGET_POWER_POOL_ATOM,
    }
    mode = aliases.get(mode, mode)
    if mode not in TARGET_MODES:
        raise ValueError(
            f"Unknown TARGET_MODE={target_mode!r}. Choose one of {sorted(TARGET_MODES)}"
        )
    return mode


def resolve_concurrency(
    region: str,
    requested: int,
    *,
    glb_max: int = 4,
    other_max: int = 8,
    announce: bool = True,
) -> int:
    """Apply configurable regional concurrency protection.

    Defaults reflect the user's current BRAIN limits: GLB<=4, other regions<=8.
    Both caps remain arguments so future platform changes require configuration,
    not source edits.
    """
    requested = max(1, int(requested))
    glb_max = max(1, int(glb_max))
    other_max = max(1, int(other_max))
    limit = glb_max if str(region).upper() == "GLB" else other_max
    effective = min(requested, limit)
    if announce:
        _safe_print(
            f"Requested concurrency: {requested}",
            f"Region concurrency limit: {limit}",
            f"Effective concurrency: {effective}",
        )
    return effective


# ---------- Authentication & HTTP ----------

_PREFERRED_CREDENTIAL_FILES = (
    "credentials.txt",
    "account.txt",
    "brain_credentials.txt",
    "账号密码.txt",
    "账户密码.txt",
)


def _read_credentials_file(credentials_path: Path) -> Tuple[str, str]:
    """Read a supported two-line or labeled credentials text file."""
    text = None
    decode_errors = []
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            text = credentials_path.read_text(encoding=encoding)
            break
        except UnicodeDecodeError as exc:
            decode_errors.append(f"{encoding}: {exc}")

    if text is None:
        raise RuntimeError(
            f"Cannot decode credentials file {credentials_path}. "
            f"Tried UTF-8 and GB18030 ({'; '.join(decode_errors)})."
        )

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"Credentials file is empty: {credentials_path}")

    labels = {
        "username": "username",
        "user": "username",
        "account": "username",
        "账号": "username",
        "账户": "username",
        "password": "password",
        "密码": "password",
    }
    labeled_values: Dict[str, str] = {}
    labeled_line_seen = False
    label_pattern = re.compile(r"^([^=:\uFF1A]+?)\s*[=:\uFF1A]\s*(.*)$")

    for line in lines:
        match = label_pattern.match(line)
        if not match:
            continue
        label = match.group(1).strip().lower()
        canonical_label = labels.get(label)
        if canonical_label is None:
            continue
        labeled_line_seen = True
        # Split only at the first label separator. Special characters in the
        # remaining password are preserved.
        labeled_values[canonical_label] = match.group(2).strip()

    if labeled_line_seen:
        username = labeled_values.get("username", "")
        password = labeled_values.get("password", "")
        if not username or not password:
            raise RuntimeError(
                f"Credentials file {credentials_path} must contain both username and password."
            )
        return username, password

    if len(lines) < 2:
        raise RuntimeError(
            f"Credentials file {credentials_path} must contain username on line 1 "
            "and password on line 2."
        )
    return lines[0], lines[1]


def _find_credentials_file(
    credentials_file: Optional[str] = None,
) -> Optional[Path]:
    """Resolve an explicit file or discover one beside this module."""
    project_dir = Path(__file__).resolve().parent

    if credentials_file:
        requested = Path(credentials_file).expanduser()
        if not requested.is_absolute():
            cwd_candidate = (Path.cwd() / requested).resolve()
            project_candidate = (project_dir / requested).resolve()
            requested = cwd_candidate if cwd_candidate.is_file() else project_candidate
        else:
            requested = requested.resolve()
        if not requested.is_file():
            raise RuntimeError(f"Credentials file not found: {requested}")
        return requested

    for filename in _PREFERRED_CREDENTIAL_FILES:
        candidate = project_dir / filename
        if candidate.is_file():
            return candidate

    txt_candidates = sorted(
        (path for path in project_dir.glob("*.txt") if path.is_file()),
        key=lambda path: path.name.casefold(),
    )
    if len(txt_candidates) == 1:
        return txt_candidates[0]
    if len(txt_candidates) > 1:
        print("Found multiple credential text-file candidates:")
        for path in txt_candidates:
            print(f"  - {path}")
        raise RuntimeError(
            "Multiple .txt files found. Specify one explicitly, for example: "
            "login(credentials_file='账号密码.txt')"
        )
    return None


def login(credentials_file: Optional[str] = None) -> requests.Session:
    """
    Login to BRAIN.

    Credentials are loaded from a text file beside this module. Preferred
    filenames are checked in order; otherwise the only .txt file is used.
    BRAIN_USERNAME/BRAIN_PASSWORD are used only when no text file exists.
    """
    credentials_path = _find_credentials_file(credentials_file)
    if credentials_path is not None:
        username, password = _read_credentials_file(credentials_path)
        print(f"Loading BRAIN credentials from: {credentials_path}")
    else:
        username = (os.getenv("BRAIN_USERNAME") or "").strip()
        password = (os.getenv("BRAIN_PASSWORD") or "").strip()

    if not username or not password:
        raise RuntimeError(
            "Missing credentials. Add a supported credentials .txt file beside "
            "machine_lib.py, or set BRAIN_USERNAME and BRAIN_PASSWORD."
        )

    s = requests.Session()
    # Windows may expose a local HTTP CONNECT proxy as an HTTPS proxy URL,
    # which makes requests start a TLS handshake with the proxy itself and
    # fail with SSLEOFError. BRAIN is reachable directly, so keep this
    # session independent from system/environment proxy settings.
    s.trust_env = False
    s.auth = (username, password)
    response = s.post(f"{BRAIN_API_URL}/authentication", timeout=60)

    if response.status_code == requests.codes.unauthorized:
        if response.headers.get("WWW-Authenticate") == "persona":
            print(
                "Biometrics authentication required. Open:\n"
                + urljoin(response.url, response.headers["Location"])
            )
            input("Complete biometrics, then press Enter.")
            biometrics_response = s.post(
                urljoin(response.url, response.headers["Location"]), timeout=60
            )
            while biometrics_response.status_code != 201:
                input("Biometrics not complete. Complete it and press Enter.")
                biometrics_response = s.post(
                    urljoin(response.url, response.headers["Location"]), timeout=60
                )
        else:
            raise RuntimeError("BRAIN login failed: incorrect username/password.")
    elif not response.ok:
        raise RuntimeError(
            f"BRAIN login failed: HTTP {response.status_code}: {response.text[:300]}"
        )

    print("Logged in successfully.")
    return s


def login_hk():
    """Backward-compatible alias."""
    return login()


class RequestFailure(RuntimeError):
    """Structured HTTP/network failure with enough context for cache logging."""

    def __init__(
        self,
        *,
        category: str,
        method: str,
        url: str,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
        body: str = "",
        last_exception: Optional[BaseException] = None,
    ) -> None:
        self.category = category
        self.method = method.upper()
        self.url = url
        self.status_code = status_code
        self.retry_after = retry_after
        self.body = body
        self.last_exception = last_exception
        parts = [f"category={category}", f"method={self.method}", f"url={url}"]
        if status_code is not None:
            parts.append(f"status={status_code}")
        if retry_after is not None:
            parts.append(f"retry_after={retry_after}")
        if body:
            parts.append(f"body={body[:1000]}")
        if last_exception is not None:
            parts.append(
                f"last_exception={type(last_exception).__name__}({last_exception})"
            )
        super().__init__(" | ".join(parts))


def _response_body(response: Optional[requests.Response], limit: int = 1000) -> str:
    if response is None:
        return ""
    try:
        return (response.text or "")[:limit].replace("\x00", "")
    except Exception as exc:
        return f"<unable to read response body: {type(exc).__name__}: {exc}>"


def _retry_after_seconds(
    response: requests.Response, fallback: float
) -> float:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return float(fallback)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return float(fallback)


class SimulationInterrupted(RuntimeError):
    """Cooperative stop signal that must not be persisted as a simulation error."""


def _safe_print(*lines: Any) -> None:
    """Print one complete log block without interleaving worker output."""
    with _PRINT_LOCK:
        for line in lines:
            print(line)


def _check_stop(
    stop_event: Optional[threading.Event], context: str = "operation"
) -> None:
    if stop_event is not None and stop_event.is_set():
        raise SimulationInterrupted(f"stop requested before {context}")


def _interruptible_wait(
    stop_event: Optional[threading.Event], seconds: float, context: str
) -> None:
    seconds = max(0.0, float(seconds))
    if stop_event is None:
        time.sleep(seconds)
        return
    if stop_event.wait(seconds):
        raise SimulationInterrupted(f"stop requested during {context}")
    _check_stop(stop_event, f"{context} completion")


def _copy_session_state(
    source: requests.Session, target: requests.Session
) -> requests.Session:
    """Copy authenticated state without sharing mutable headers or CookieJar."""
    target.auth = copy.copy(source.auth)
    target.headers.clear()
    target.headers.update(dict(source.headers))
    target.cookies.clear()
    for cookie in source.cookies:
        target.cookies.set_cookie(copy.copy(cookie))
    target.trust_env = source.trust_env
    target.verify = source.verify
    target.cert = source.cert
    target.max_redirects = source.max_redirects
    target.params = dict(source.params)
    target.proxies = dict(source.proxies)
    return target


def _clone_session(base_session: requests.Session) -> requests.Session:
    """Create a worker-owned Session with an independent CookieJar."""
    return _copy_session_state(base_session, requests.Session())


class _PostGate:
    """Coordinate POST staggering and a shared 429 cooldown for one run."""

    def __init__(self, stagger_seconds: float = SUBMIT_STAGGER_SECONDS) -> None:
        self._lock = threading.Lock()
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._stagger_seconds = max(0.0, float(stagger_seconds))

    def update_cooldown(self, retry_after: Optional[float]) -> None:
        # Some BRAIN 429 responses do not include Retry-After.
        # Never let a missing/malformed cooldown value crash the worker.
        try:
            seconds = max(0.0, float(retry_after)) if retry_after is not None else 0.0
        except (TypeError, ValueError):
            seconds = 0.0
        deadline = time.monotonic() + seconds
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, deadline)

    def wait_for_turn(
        self,
        stop_event: Optional[threading.Event],
        progress_label: str = "",
    ) -> None:
        cooldown_logged = False
        while True:
            _check_stop(stop_event, "POST gate")
            with self._lock:
                now = time.monotonic()
                allowed_at = max(self._next_start, self._cooldown_until)
                wait_seconds = max(0.0, allowed_at - now)
                in_cooldown = self._cooldown_until > now
                if wait_seconds <= 0:
                    self._next_start = now + self._stagger_seconds
                    return
            if in_cooldown and not cooldown_logged:
                _safe_print(
                    f"{progress_label} [GLOBAL 429] POST paused {wait_seconds:.1f}s"
                )
                cooldown_logged = True
            _interruptible_wait(stop_event, wait_seconds, "POST gate wait")


def ensure_session(
    s: requests.Session,
    *,
    stop_event: Optional[threading.Event] = None,
) -> requests.Session:
    """Re-authenticate and copy the fresh authenticated state into ``s``."""
    _check_stop(stop_event, "authentication refresh")
    while not _AUTH_REFRESH_LOCK.acquire(timeout=0.1):
        _check_stop(stop_event, "authentication refresh lock")
    try:
        _check_stop(stop_event, "authentication login")
        _safe_print("[AUTH] session expired; logging in again")
        fresh = login()
        try:
            _check_stop(stop_event, "authentication state copy")
            return _copy_session_state(fresh, s)
        finally:
            fresh.close()
    finally:
        _AUTH_REFRESH_LOCK.release()


def _request_with_retry(
    s: requests.Session,
    method: str,
    url: str,
    *,
    json_body: Optional[Any] = None,
    max_retries: int = 8,
    timeout: int = 60,
    stop_event: Optional[threading.Event] = None,
) -> requests.Response:
    """HTTP helper for idempotent requests with status-aware retry rules.

    Simulation POSTs intentionally use ``submit_simulation`` instead.
    """
    method = method.upper()
    max_retries = max(1, int(max_retries))
    delay = 2.0
    last_exc: Optional[BaseException] = None
    last_response: Optional[requests.Response] = None
    auth_refreshed = False
    network_errors = (
        requests.ConnectionError,
        requests.Timeout,
        requests.RequestException,
        ConnectionResetError,
        RemoteDisconnected,
    )

    # One additional iteration is reserved only for a 401/403 re-login retry;
    # rate-limit, server, and network branches still enforce max_retries below.
    for attempt in range(1, max_retries + 2):
        _check_stop(stop_event, f"{method} request")
        try:
            kwargs = {"timeout": timeout}
            if json_body is not None:
                kwargs["json"] = json_body
            response = s.request(method, url, **kwargs)
            last_response = response
            last_exc = None
            status = int(response.status_code)

            # A successful response is final even when it includes Retry-After.
            if 200 <= status < 300:
                return response

            if status in (400, 422):
                raise RequestFailure(
                    category="INVALID",
                    method=method,
                    url=url,
                    status_code=status,
                    retry_after=_retry_after_seconds(response, delay)
                    if "Retry-After" in response.headers
                    else None,
                    body=_response_body(response),
                )

            if status in (401, 403):
                if auth_refreshed:
                    raise RequestFailure(
                        category="AUTH_ERROR",
                        method=method,
                        url=url,
                        status_code=status,
                        body=_response_body(response),
                    )
                try:
                    ensure_session(s, stop_event=stop_event)
                except Exception as exc:
                    raise RequestFailure(
                        category="AUTH_ERROR",
                        method=method,
                        url=url,
                        status_code=status,
                        body=_response_body(response),
                        last_exception=exc,
                    ) from exc
                auth_refreshed = True
                continue

            if status == 429:
                wait = _retry_after_seconds(response, delay)
                if attempt >= max_retries:
                    break
                _safe_print(
                    f"[429] retry {attempt + 1}/{max_retries} | wait {wait:.1f}s"
                )
                _interruptible_wait(
                    stop_event, max(0.5, wait), f"{method} 429 backoff"
                )
                delay = min(delay * 2, 30.0)
                continue

            if status in (500, 502, 503, 504):
                if attempt >= max_retries:
                    break
                wait = min(delay, 30.0)
                _safe_print(
                    f"[HTTP {status}] retry {attempt + 1}/{max_retries} | "
                    f"wait {wait:.1f}s"
                )
                _interruptible_wait(stop_event, wait, f"{method} HTTP backoff")
                delay = min(delay * 2, 30.0)
                continue

            raise RequestFailure(
                category="HTTP_ERROR",
                method=method,
                url=url,
                status_code=status,
                retry_after=_retry_after_seconds(response, delay)
                if "Retry-After" in response.headers
                else None,
                body=_response_body(response),
            )
        except RequestFailure:
            raise
        except network_errors as exc:
            last_exc = exc
            last_response = None
            if attempt >= max_retries:
                break
            wait = min(delay, 30.0)
            _safe_print(
                f"[NETWORK] retry {attempt + 1}/{max_retries} | wait {wait:.1f}s | "
                f"{type(exc).__name__}: {exc}"
            )
            _interruptible_wait(stop_event, wait, f"{method} network backoff")
            delay = min(delay * 2, 30.0)

    final_category = "NETWORK_ERROR" if last_exc is not None else "HTTP_ERROR"
    if last_response is not None and last_response.status_code in (401, 403):
        final_category = "AUTH_ERROR"
    raise RequestFailure(
        category=final_category,
        method=method,
        url=url,
        status_code=last_response.status_code if last_response is not None else None,
        retry_after=_retry_after_seconds(last_response, delay)
        if last_response is not None and "Retry-After" in last_response.headers
        else None,
        body=_response_body(last_response),
        last_exception=last_exc,
    )


def _get_json(
    s: requests.Session,
    url: str,
    required_key: Optional[str] = None,
    max_retries: int = 8,
) -> Dict[str, Any]:
    for _ in range(max_retries):
        response = _request_with_retry(s, "GET", url)
        try:
            payload = response.json()
        except ValueError:
            sleep(1)
            continue
        if required_key is None or required_key in payload:
            return payload
        sleep(1)
    raise RuntimeError(f"Could not obtain JSON key={required_key!r} from {url}")


# ---------- Datasets & fields ----------

def get_datasets(
    s: requests.Session,
    instrument_type: str = "EQUITY",
    region: str = "USA",
    delay: int = 1,
    universe: str = "TOP3000",
) -> pd.DataFrame:
    url = (
        f"{BRAIN_API_URL}/data-sets?"
        f"instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}"
    )
    payload = _get_json(s, url, "results")
    return pd.DataFrame(payload["results"])


def get_datafields(
    s: requests.Session,
    instrument_type: str = "EQUITY",
    region: str = "USA",
    delay: int = 1,
    universe: str = "TOP3000",
    dataset_id: str = "",
    search: str = "",
) -> pd.DataFrame:
    if not search:
        url_template = (
            f"{BRAIN_API_URL}/data-fields?"
            f"instrumentType={instrument_type}&region={region}&delay={delay}"
            f"&universe={universe}&dataset.id={dataset_id}&limit=50&offset={{x}}"
        )
        count = int(_get_json(s, url_template.format(x=0), "count")["count"])
    else:
        url_template = (
            f"{BRAIN_API_URL}/data-fields?"
            f"instrumentType={instrument_type}&region={region}&delay={delay}"
            f"&universe={universe}&limit=50&search={search}&offset={{x}}"
        )
        first = _get_json(s, url_template.format(x=0), "results")
        count = int(first.get("count", 100))

    rows = []
    for offset in range(0, max(count, 1), 50):
        payload = _get_json(s, url_template.format(x=offset), "results")
        batch = payload.get("results", [])
        rows.extend(batch)
        if len(batch) < 50:
            break
    return pd.DataFrame(rows)



def resolve_data_coverage_column(
    df: pd.DataFrame,
    preferred: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Resolve the API column that actually carries the displayed Data Coverage.

    BRAIN responses seen in the project expose both ``dateCoverage`` and
    ``coverage``; in some datasets ``coverage`` is all zero while
    ``dateCoverage`` contains the meaningful 0..1 values shown on the UI.
    This resolver refuses to silently choose a degenerate all-zero column when
    a more informative coverage-like column exists.
    """
    if df.empty:
        raise ValueError("Cannot resolve Data Coverage from an empty DataFrame.")

    if preferred is not None:
        if preferred not in df.columns:
            raise KeyError(
                f"Requested coverage column {preferred!r} not found. "
                f"Available columns: {list(df.columns)}"
            )
        candidates = [preferred]
    else:
        candidates = [
            c for c in ("dataCoverage", "dateCoverage", "coverage")
            if c in df.columns
        ]
        if not candidates:
            raise KeyError(
                "No Data Coverage column found. Expected one of: "
                "dataCoverage, dateCoverage, coverage."
            )

    diagnostics: Dict[str, Dict[str, Any]] = {}
    ranked = []
    for priority, column in enumerate(candidates):
        values = pd.to_numeric(df[column], errors="coerce")
        valid = values.dropna()
        in_range = valid[(valid >= 0) & (valid <= 1)]
        nonzero = int((in_range > 0).sum())
        unique = int(in_range.nunique()) if not in_range.empty else 0
        max_value = float(in_range.max()) if not in_range.empty else None
        diagnostics[column] = {
            "valid": int(valid.size),
            "in_range": int(in_range.size),
            "nonzero": nonzero,
            "unique": unique,
            "max": max_value,
        }
        # Prefer meaningful non-zero 0..1 coverage, then variability, then the
        # documented/API naming order above.
        score = (
            1 if in_range.size else 0,
            1 if nonzero else 0,
            nonzero,
            unique,
            -priority,
        )
        ranked.append((score, column))

    _, chosen = max(ranked)
    chosen_diag = diagnostics[chosen]
    if chosen_diag["in_range"] == 0:
        raise ValueError(
            f"Coverage column {chosen!r} has no numeric values in [0, 1]. "
            f"Diagnostics: {diagnostics}"
        )
    if chosen_diag["nonzero"] == 0 and preferred is None:
        raise ValueError(
            "Coverage-like API columns are all zero/empty; refusing to filter "
            f"silently. Diagnostics: {diagnostics}"
        )

    report = {"chosen": chosen, "diagnostics": diagnostics}
    return chosen, report


def filter_datafields(
    df: pd.DataFrame,
    *,
    min_data_coverage: float,
    coverage_column: Optional[str] = None,
    allowed_types: Sequence[str] = ("MATRIX", "VECTOR"),
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Filter fields by configurable Data Coverage before Stage-1.

    ``min_data_coverage`` is intentionally required by callers so the threshold
    lives in notebook/configuration rather than being hidden in the library.
    MAX_FIELDS should be applied *after* this function (and after any manual
    exclusions) so low-coverage rows never consume the field cap.
    """
    threshold = float(min_data_coverage)
    if not 0 <= threshold <= 1:
        raise ValueError("min_data_coverage must be between 0 and 1.")
    if df.empty:
        return df.copy(), {
            "coverage_column": None,
            "threshold": threshold,
            "before": 0,
            "after": 0,
            "filtered_out": 0,
        }

    chosen, resolver_report = resolve_data_coverage_column(df, coverage_column)
    x = df.copy()
    x["_data_coverage"] = pd.to_numeric(x[chosen], errors="coerce")
    allowed = {str(v).upper() for v in allowed_types}
    type_mask = x.get("type", pd.Series(index=x.index, dtype=object)).astype(str).str.upper().isin(allowed)
    coverage_mask = x["_data_coverage"].ge(threshold)
    kept = x[type_mask & coverage_mask].copy().reset_index(drop=True)

    report = {
        "coverage_column": chosen,
        "threshold": threshold,
        "before": int(len(x)),
        "after": int(len(kept)),
        "filtered_out": int(len(x) - len(kept)),
        "resolver": resolver_report,
    }
    return kept, report

def get_vec_fields(fields: Sequence[str], vec_ops: Sequence[str] = ("vec_avg", "vec_sum")) -> List[str]:
    out = []
    for field in fields:
        for op in vec_ops:
            if op == "vec_choose":
                out.extend([f"{op}({field}, nth=-1)", f"{op}({field}, nth=0)"])
            else:
                out.append(f"{op}({field})")
    return out


def prepare_fields(
    df: pd.DataFrame,
    backfill_days: int = 120,
    winsor_std: int = 4,
    vec_ops: Sequence[str] = ("vec_avg", "vec_sum"),
) -> List[Dict[str, Any]]:
    """Convert the filtered BRAIN field table into metadata-rich base records."""
    records: List[Dict[str, Any]] = []
    if df.empty:
        return records

    for _, row in df.iterrows():
        field_id = row.get("id")
        field_type = row.get("type")
        if not field_id or field_type not in ("MATRIX", "VECTOR"):
            continue

        dataset_obj = row.get("dataset")
        category_obj = row.get("category")
        subcategory_obj = row.get("subcategory")
        dataset_id = _nested_value(dataset_obj, "id")
        data_coverage = row.get("_data_coverage")
        if data_coverage is None:
            # Do not guess between API coverage columns here; the notebook should
            # run filter_datafields first. This fallback is metadata-only.
            data_coverage = row.get("dataCoverage", row.get("dateCoverage"))

        base_exprs: List[Tuple[str, Optional[str]]] = []
        if field_type == "MATRIX":
            base_exprs.append((str(field_id), None))
        else:
            for vec_op in vec_ops:
                if vec_op == "vec_choose":
                    base_exprs.extend([
                        (f"vec_choose({field_id}, nth=-1)", "vec_choose_-1"),
                        (f"vec_choose({field_id}, nth=0)", "vec_choose_0"),
                    ])
                else:
                    base_exprs.append((f"{vec_op}({field_id})", vec_op))

        for base_expr, vec_op in base_exprs:
            expr = f"winsorize(ts_backfill({base_expr}, {backfill_days}), std={winsor_std})"
            records.append(
                {
                    "field": str(field_id),
                    "field_type": str(field_type),
                    "vector_op": vec_op,
                    "base_expr": base_expr,
                    "expr": expr,
                    "stage": 0,
                    "search_tier": "base",
                    "operator": "preprocess",
                    "window": backfill_days,
                    "parent": None,
                    "decay": 6,
                    # Candidate provenance / field metadata.
                    "dataset_id": _json_safe(dataset_id),
                    "dataset_name": _json_safe(_nested_value(dataset_obj, "name")),
                    "dataset_ids": [str(dataset_id)] if dataset_id else [],
                    "category_id": _json_safe(_nested_value(category_obj, "id")),
                    "category_name": _json_safe(_nested_value(category_obj, "name")),
                    "subcategory_id": _json_safe(_nested_value(subcategory_obj, "id")),
                    "region": _json_safe(row.get("region")),
                    "delay": _json_safe(row.get("delay")),
                    "universe": _json_safe(row.get("universe")),
                    "data_coverage": _json_safe(data_coverage),
                    "date_coverage": _json_safe(row.get("dateCoverage")),
                    "api_coverage": _json_safe(row.get("coverage")),
                    "pyramid_multiplier": _json_safe(row.get("pyramidMultiplier")),
                    "themes": _json_safe(row.get("themes")) or [],
                    # Structural accounting. Support group fields are tracked
                    # separately and do not increase data_field_count.
                    "data_fields": [str(field_id)],
                    "support_group_fields": [],
                }
            )
    return records

def process_datafields(df: pd.DataFrame) -> List[str]:
    """Legacy-compatible output used by old notebooks."""
    return [r["expr"] for r in prepare_fields(df)]


# ---------- Candidate factories ----------

def _candidate_key(candidate: Dict[str, Any]) -> str:
    return hashlib.sha256(candidate["expr"].strip().encode("utf-8")).hexdigest()


def _copy_candidate(base_candidate: Dict[str, Any], **updates: Any) -> Dict[str, Any]:
    child = dict(base_candidate)
    child.update(updates)
    return child


_OPERATOR_CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def count_unique_operators(expr: str) -> Tuple[int, List[str]]:
    operators = sorted(set(_OPERATOR_CALL_RE.findall(str(expr))))
    return len(operators), operators


def _merge_unique_strings(*values: Any) -> List[str]:
    out: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            seq = [value]
        else:
            try:
                seq = list(value)
            except TypeError:
                seq = [value]
        for item in seq:
            if item is None:
                continue
            item = str(item)
            if item and item not in out:
                out.append(item)
    return out


def annotate_candidate_strategy(
    candidate: Dict[str, Any],
    target_mode: Optional[str],
) -> Dict[str, Any]:
    """Attach local structural metadata without changing simulation identity."""
    c = dict(candidate)
    mode = normalize_target_mode(target_mode)
    operator_count, operators = count_unique_operators(c.get("expr", ""))
    data_fields = [
        f for f in _merge_unique_strings(c.get("data_fields"), c.get("field"))
        if f not in SUPPORT_GROUP_FIELDS
    ]
    dataset_ids = _merge_unique_strings(c.get("dataset_ids"), c.get("dataset_id"))
    support_fields = _merge_unique_strings(c.get("support_group_fields"))

    c.update(
        {
            "target_mode": mode,
            "operators": operators,
            "operator_count": operator_count,
            "data_fields": data_fields,
            "data_field_count": len(data_fields),
            "dataset_ids": dataset_ids,
            "dataset_count": len(dataset_ids),
            "support_group_fields": support_fields,
            "is_single_dataset": len(dataset_ids) == 1,
        }
    )
    c["atom_structure_ok"] = bool(c["is_single_dataset"])
    c["pp_structure_ok"] = bool(
        operator_count <= 8 and len(data_fields) <= 3
    )
    c["pp_atom_structure_ok"] = bool(
        c["atom_structure_ok"] and c["pp_structure_ok"]
    )
    return c


def annotate_candidates(
    candidates: Sequence[Dict[str, Any]],
    target_mode: Optional[str],
) -> List[Dict[str, Any]]:
    return [annotate_candidate_strategy(c, target_mode) for c in candidates]


def validate_candidate_context(
    candidates: Sequence[Dict[str, Any]],
    *,
    dataset_id: Optional[str] = None,
    target_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Hard guard against stale Notebook variables leaking across datasets."""
    expected_dataset = str(dataset_id) if dataset_id else None
    expected_mode = normalize_target_mode(target_mode) if target_mode else None
    bad_dataset = []
    bad_mode = []
    for idx, candidate in enumerate(candidates):
        c_dataset = candidate.get("dataset_id")
        c_mode = candidate.get("target_mode")
        if expected_dataset and str(c_dataset) != expected_dataset:
            bad_dataset.append((idx, c_dataset, candidate.get("field")))
        if expected_mode and c_mode and normalize_target_mode(c_mode) != expected_mode:
            bad_mode.append((idx, c_mode, candidate.get("field")))
    if bad_dataset:
        raise RuntimeError(
            "Dataset mismatch detected. Refusing to continue with stale/mixed candidates: "
            + repr(bad_dataset[:10])
        )
    if bad_mode:
        raise RuntimeError(
            "TARGET_MODE mismatch detected. Refusing to continue: "
            + repr(bad_mode[:10])
        )
    return {
        "count": len(candidates),
        "dataset_id": expected_dataset,
        "target_mode": expected_mode,
        "ok": True,
    }


def first_order_candidates(
    field_records: Sequence[Dict[str, Any]],
    ts_operators: Sequence[str] = CORE_TS_OPS,
    cross_ops: Sequence[str] = ("rank", "zscore"),
    op_windows: Optional[Dict[str, Sequence[int]]] = None,
    init_decay: int = 6,
) -> List[Dict[str, Any]]:
    """
    Compact Stage-1 exploration.
    Uses operator-specific windows instead of applying every window to every operator.
    """
    op_windows = op_windows or OP_WINDOWS
    out: List[Dict[str, Any]] = []

    for base in field_records:
        base_expr = base["expr"]

        # Keep the preprocessed raw signal once.
        out.append(
            _copy_candidate(
                base,
                stage=1,
                search_tier="core",
                operator="raw",
                window=None,
                parent=_candidate_key(base),
                decay=init_decay,
            )
        )

        for op in cross_ops:
            out.append(
                _copy_candidate(
                    base,
                    expr=f"{op}({base_expr})",
                    stage=1,
                    search_tier="core",
                    operator=op,
                    window=None,
                    parent=_candidate_key(base),
                    decay=init_decay,
                )
            )

        for op in ts_operators:
            windows = list(op_windows.get(op, [5, 22, 66]))
            for day in windows:
                out.append(
                    _copy_candidate(
                        base,
                        expr=f"{op}({base_expr}, {int(day)})",
                        stage=1,
                        search_tier="core",
                        operator=op,
                        window=int(day),
                        parent=_candidate_key(base),
                        decay=init_decay,
                    )
                )

    # Exact-expression de-duplication.
    dedup: Dict[str, Dict[str, Any]] = {}
    for c in out:
        dedup.setdefault(c["expr"], c)
    return list(dedup.values())



def extended_first_order_candidates(
    field_records: Sequence[Dict[str, Any]],
    *,
    active_fields: Optional[Sequence[str]] = None,
    op_windows: Optional[Dict[str, Sequence[int]]] = None,
    init_decay: int = 6,
) -> List[Dict[str, Any]]:
    """Generate the targeted Stage-1 extension (120 + arg/quantile family)."""
    windows_map = op_windows or EXTENDED_OP_WINDOWS
    allowed_fields = set(map(str, active_fields)) if active_fields is not None else None
    out: List[Dict[str, Any]] = []
    for base in field_records:
        if allowed_fields is not None and str(base.get("field")) not in allowed_fields:
            continue
        base_expr = base["expr"]
        for op, windows in windows_map.items():
            for day in windows:
                out.append(
                    _copy_candidate(
                        base,
                        expr=f"{op}({base_expr}, {int(day)})",
                        stage=1,
                        search_tier="extended",
                        operator=op,
                        window=int(day),
                        parent=_candidate_key(base),
                        decay=init_decay,
                    )
                )
    dedup: Dict[str, Dict[str, Any]] = {}
    for c in out:
        dedup.setdefault(c["expr"], c)
    return list(dedup.values())

def ts_factory(op: str, field: str, days: Optional[Sequence[int]] = None) -> List[str]:
    """Legacy helper; now uses operator-specific defaults."""
    days = list(days or OP_WINDOWS.get(op, [5, 22, 66]))
    return [f"{op}({field}, {int(day)})" for day in days]


def first_order_factory(fields: Sequence[str], operators: Sequence[str]) -> List[str]:
    """Legacy helper with compact operator-specific windows."""
    out = []
    for field in fields:
        out.append(field)
        for op in operators:
            if op.startswith("ts_"):
                out.extend(ts_factory(op, field))
            else:
                out.append(f"{op}({field})")
    return _dedupe_keep_order(out)


def core_groups(
    region: str,
    extended: bool = False,
    *,
    target_mode: Optional[str] = None,
) -> List[str]:
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    liquidity_group = "bucket(rank(close*volume), range='0.1, 1, 0.1')"
    mode = normalize_target_mode(target_mode) if target_mode else None

    # ATOM / PP+ATOM must stay single-dataset; support groups do not introduce
    # cap/price-volume fields. Pure Power Pool also starts compact for cost and
    # structure efficiency. Regular keeps the historical five-group search.
    if mode in (TARGET_ATOM, TARGET_POWER_POOL, TARGET_POWER_POOL_ATOM):
        groups = ["sector", "industry", "subindustry"]
    else:
        groups = ["sector", "industry", "subindustry", cap_group, liquidity_group]

    if extended and mode in (None, TARGET_REGULAR) and region.upper() == "USA":
        groups.extend([
            "pv13_h_min2_3000_sector",
            "pv13_r2_min20_3000_sector",
            "pv13_r2_min2_3000_sector",
            "pv13_h_min2_focused_pureplay_3000_sector",
        ])
    return _dedupe_keep_order(groups)

def group_factory(
    op: str,
    field: str,
    region: str,
    *,
    extended: bool = False,
    target_mode: Optional[str] = None,
) -> List[str]:
    out = []
    for group in core_groups(region, extended=extended, target_mode=target_mode):
        g = f"densify({group})"
        if op.startswith("group_vector"):
            out.append(f"{op}({field}, cap, {g})")
        elif op.startswith("group_percentage"):
            out.append(f"{op}({field}, {g}, percentage=0.5)")
        else:
            out.append(f"{op}({field}, {g})")
    return _dedupe_keep_order(out)

def second_order_candidates(
    promoted: Sequence[Dict[str, Any]],
    region: str,
    group_ops: Sequence[str] = ("group_neutralize", "group_rank"),
    extended_groups: bool = False,
    *,
    target_mode: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Target-aware Stage-2 group search with structural metadata tracking."""
    mode = normalize_target_mode(target_mode) if target_mode else None
    out: List[Dict[str, Any]] = []
    for parent in promoted:
        for op in group_ops:
            groups = core_groups(
                region,
                extended=extended_groups,
                target_mode=mode,
            )
            for group in groups:
                g = f"densify({group})"
                if op.startswith("group_vector"):
                    expr = f"{op}({parent['expr']}, cap, {g})"
                elif op.startswith("group_percentage"):
                    expr = f"{op}({parent['expr']}, {g}, percentage=0.5)"
                else:
                    expr = f"{op}({parent['expr']}, {g})"

                data_fields = _merge_unique_strings(parent.get("data_fields"), parent.get("field"))
                support_fields = _merge_unique_strings(parent.get("support_group_fields"))
                if group in SUPPORT_GROUP_FIELDS:
                    support_fields = _merge_unique_strings(support_fields, group)
                elif "close*volume" in group:
                    data_fields = _merge_unique_strings(data_fields, "close", "volume")
                elif "rank(cap)" in group or op.startswith("group_vector"):
                    data_fields = _merge_unique_strings(data_fields, "cap")
                else:
                    # Extended custom groups are conservatively counted as an
                    # additional data field unless they are known support fields.
                    data_fields = _merge_unique_strings(data_fields, group)

                child = _copy_candidate(
                    parent,
                    expr=expr,
                    stage=2,
                    search_tier="group",
                    parent=_candidate_key(parent),
                    group_operator=op,
                    group=group,
                    data_fields=data_fields,
                    support_group_fields=support_fields,
                )
                out.append(
                    annotate_candidate_strategy(child, mode)
                    if mode else child
                )

    dedup: Dict[str, Dict[str, Any]] = {}
    for c in out:
        dedup.setdefault(c["expr"], c)
    return list(dedup.values())

def get_group_second_order_factory(first_order: Sequence[str], group_ops: Sequence[str], region: str) -> List[str]:
    """Legacy-compatible wrapper."""
    out = []
    for expr in first_order:
        for op in group_ops:
            out.extend(group_factory(op, expr, region))
    return _dedupe_keep_order(out)


def region_events(region: str) -> List[str]:
    """
    Region-specific event library.
    Unlike the original code, these events are actually appended to the generic events.
    """
    region = region.upper()
    mapping = {
        "USA": [
            "rank(rp_css_business) > 0.8",
            "ts_rank(rp_css_business, 22) > 0.8",
            "rank(vec_avg(mws82_sentiment)) > 0.8",
            "ts_rank(vec_avg(nws48_ssc), 22) > 0.8",
        ],
        "ASI": [
            "rank(vec_avg(mws38_score)) > 0.8",
            "ts_rank(vec_avg(mws38_score), 22) > 0.8",
        ],
        "EUR": [
            "rank(rp_css_business) > 0.8",
            "ts_rank(rp_css_business, 22) > 0.8",
            "rank(mdl110_analyst_sentiment) > 0.8",
            "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
        ],
        "GLB": [
            "rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
            "ts_rank(vec_avg(mdl109_news_sent_1m), 22) > 0.8",
            "rank(vec_avg(nws20_ssc)) > 0.8",
            "ts_rank(vec_avg(nws20_ssc), 22) > 0.8",
        ],
        "CHN": [
            "rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
            "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform), 22) > 0.8",
        ],
        "KOR": [
            "rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
            "ts_rank(vec_avg(mws38_score), 22) > 0.8",
        ],
        "TWN": [
            "rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
            "ts_rank(rp_ess_business, 22) > 0.8",
        ],
    }
    return mapping.get(region, [])


def generic_trade_events() -> List[str]:
    # Intentionally compact. These are repair tools, not a full Cartesian search.
    return [
        "ts_arg_max(volume, 5) == 0",
        "ts_mean(volume, 10) > ts_mean(volume, 60)",
        "group_rank(ts_std_dev(returns, 60), sector) > 0.7",
        "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
        "ts_arg_max(close, 20) == 0",
        "ts_corr(close, volume, 20) > 0.3",
    ]


def trade_when_factory(
    op: str,
    field: str,
    region: str,
    *,
    max_events: Optional[int] = None,
    include_region_events: bool = True,
) -> List[str]:
    generic = generic_trade_events()
    regional = region_events(region) if include_region_events else []

    if max_events is None:
        events = _dedupe_keep_order(generic + regional)
    else:
        # Balanced sampling: keep generic events, but reserve one slot for a
        # region-specific event when available. This fixes the original code
        # where regional event lists were defined but never used.
        max_events = max(0, int(max_events))
        if max_events == 0:
            events = []
        elif regional:
            events = generic[: max(0, max_events - 1)] + regional[:1]
        else:
            events = generic[:max_events]
        events = _dedupe_keep_order(events)

    # Only one default exit rule to avoid an unnecessary x2 explosion.
    return [f"{op}({event}, {field}, -1)" for event in events]



def _first_top_level_argument(call_expr: str) -> str:
    """Return the first argument from a function-call expression.

    Used to recover the signed Stage1 signal from a Stage2 outer
    group_neutralize/group_rank wrapper without relying on stale parent metadata.
    """
    expr = str(call_expr or "").strip()
    open_pos = expr.find("(")
    if open_pos < 0 or not expr.endswith(")"):
        return expr

    inner = expr[open_pos + 1:-1]
    depth = 0
    quote: Optional[str] = None
    escape = False

    for i, ch in enumerate(inner):
        if quote is not None:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                quote = None
            continue

        if ch in ("'", '"'):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            return inner[:i].strip()

    return inner.strip()


def _regional_repair_base_signal(parent: Dict[str, Any]) -> str:
    """Recover the signal before the outer Stage2 grouping operator."""
    expr = str(parent.get("expr", "") or "").strip()
    outer_op = str(parent.get("group_operator", "") or "").strip()

    if outer_op in {
        "group_neutralize",
        "group_rank",
        "group_zscore",
    } and expr.startswith(outer_op + "("):
        return _first_top_level_argument(expr)

    return expr


def regional_repair_candidates(
    stage2_selected: pd.DataFrame,
    *,
    target_mode: str,
    max_parents: int = 3,
    humps: Sequence[float] = (0.01, 0.02),
) -> List[Dict[str, Any]]:
    """Generate compact region-robustness repair variants.

    Parent selection:
    - choose at most one strongest Stage2 Alpha per field;
    - rank fields by Stage2 score, then Sharpe;
    - default max 3 parents.

    Variants per parent:
    1) country neutralize
    2) country rank
    3) industry -> country double neutralize
    4) country neutralize + hump(0.01)
    5) country neutralize + hump(0.02)

    The goal is to test whether country-level cross-sectional control improves
    weak EMEA/APAC and sub-universe behavior. This function only generates
    candidates; BRAIN live /check remains the final judge.
    """
    if stage2_selected is None or stage2_selected.empty:
        return []

    mode = normalize_target_mode(target_mode)
    work = stage2_selected.copy()

    if "field" not in work.columns:
        work["field"] = work["candidate"].map(
            lambda c: c.get("field") if isinstance(c, dict) else None
        )

    if "score" not in work.columns:
        work["score"] = 0.0
    if "sharpe" not in work.columns:
        work["sharpe"] = 0.0

    work["_score_num"] = pd.to_numeric(work["score"], errors="coerce").fillna(0.0)
    work["_sharpe_num"] = pd.to_numeric(work["sharpe"], errors="coerce").fillna(0.0)

    # One strongest Stage2 parent per field.
    parents_df = (
        work.sort_values(
            ["_score_num", "_sharpe_num"],
            ascending=False,
        )
        .dropna(subset=["field"])
        .drop_duplicates(subset=["field"], keep="first")
        .head(max(1, int(max_parents)))
        .copy()
    )

    out: List[Dict[str, Any]] = []

    for _, row in parents_df.iterrows():
        parent = row.get("candidate")
        if not isinstance(parent, dict):
            continue

        base_signal = _regional_repair_base_signal(parent)
        if not base_signal:
            continue

        parent_alpha_id = str(row.get("alpha_id") or "")
        field = str(row.get("field") or parent.get("field") or "")

        variants: List[Tuple[str, str, str, Sequence[str]]] = [
            (
                f"group_neutralize({base_signal}, densify(country))",
                "regional_country_neutralize",
                "country",
                ("country",),
            ),
            (
                f"group_rank({base_signal}, densify(country))",
                "regional_country_rank",
                "country",
                ("country",),
            ),
            (
                "group_neutralize("
                f"group_neutralize({base_signal}, densify(industry)), "
                "densify(country))",
                "regional_industry_then_country",
                "industry+country",
                ("industry", "country"),
            ),
        ]

        country_neutral = f"group_neutralize({base_signal}, densify(country))"
        for hump_value in humps:
            hv = float(hump_value)
            variants.append(
                (
                    f"hump({country_neutral}, hump={hv:g})",
                    f"regional_country_hump_{hv:g}",
                    "country",
                    ("country",),
                )
            )

        for expr, repair_name, group_name, support_groups in variants:
            child = _copy_candidate(
                parent,
                expr=expr,
                stage=3,
                search_tier="regional_repair",
                parent=_candidate_key(parent),
                repair=repair_name,
                regional_parent_alpha_id=parent_alpha_id,
                regional_parent_field=field,
                group_operator=(
                    "group_rank"
                    if repair_name == "regional_country_rank"
                    else "group_neutralize"
                ),
                group=group_name,
                support_group_fields=_merge_unique_strings(
                    parent.get("support_group_fields"),
                    *support_groups,
                ),
            )

            annotated = annotate_candidate_strategy(child, mode)

            # Keep local structural target intact.
            if mode == TARGET_POWER_POOL and not annotated["pp_structure_ok"]:
                continue
            if mode == TARGET_ATOM and not annotated["atom_structure_ok"]:
                continue
            if mode == TARGET_POWER_POOL_ATOM and not annotated["pp_atom_structure_ok"]:
                continue

            out.append(annotated)

    # Expression-level dedupe, preserving parent-score order.
    dedup: Dict[str, Dict[str, Any]] = {}
    for candidate in out:
        dedup.setdefault(candidate["expr"], candidate)

    return list(dedup.values())


def targeted_repair_candidates(
    promoted_results: pd.DataFrame,
    region: str,
    *,
    max_variants_per_parent: int = 6,
    turnover_trigger: float = 0.35,
) -> List[Dict[str, Any]]:
    """
    Stage-3 targeted repair.

    Rules:
    - Strong + already low-turnover signals are not modified.
    - High-turnover signals get a few decay variants, hump, and a small event subset.
    - This replaces the old fixed 40 trade_when variants for every parent.
    """
    out: List[Dict[str, Any]] = []
    if promoted_results.empty:
        return out

    for _, row in promoted_results.iterrows():
        parent = row["candidate"]
        sharpe = float(row.get("sharpe", 0) or 0)
        fitness = float(row.get("fitness", 0) or 0)
        turnover = float(row.get("turnover", 0) or 0)
        variants: List[Dict[str, Any]] = []

        # Good and already efficient -> do not over-process.
        if sharpe >= 1.2 and fitness >= 0.8 and turnover <= turnover_trigger:
            continue

        if turnover > turnover_trigger:
            base_decay = int(parent.get("decay", 6) or 6)
            decay_values = _dedupe_keep_order([
                str(min(base_decay + 2, 20)),
                str(min(base_decay + 4, 20)),
                str(min(max(base_decay * 2, base_decay + 2), 20)),
            ])
            for decay_str in decay_values:
                variants.append(
                    _copy_candidate(
                        parent,
                        stage=3,
                        parent=_candidate_key(parent),
                        decay=int(decay_str),
                        repair="decay",
                    )
                )

            variants.append(
                _copy_candidate(
                    parent,
                    expr=f"hump({parent['expr']}, hump=0.01)",
                    stage=3,
                    parent=_candidate_key(parent),
                    repair="hump_0.01",
                )
            )

            # At most two trade events for the first repair pass.
            for tw in trade_when_factory(
                "trade_when",
                parent["expr"],
                region,
                max_events=2,
                include_region_events=True,
            ):
                variants.append(
                    _copy_candidate(
                        parent,
                        expr=tw,
                        stage=3,
                        parent=_candidate_key(parent),
                        repair="trade_when",
                    )
                )

        # Deduplicate and cap per parent.
        seen = set()
        kept = []
        for v in variants:
            key = (v["expr"], int(v.get("decay", 6)))
            if key in seen:
                continue
            seen.add(key)
            kept.append(v)
            if len(kept) >= max_variants_per_parent:
                break
        out.extend(kept)

    return out


def _failed_check_names(row: pd.Series) -> List[str]:
    names = row.get("failed_check_names")
    if isinstance(names, str):
        return [n.strip().upper() for n in names.split(",") if n.strip()]
    if isinstance(names, (list, tuple, set)):
        return [str(n).upper() for n in names]
    checks = row.get("failed_checks")
    if isinstance(checks, list):
        return [str(c.get("name")).upper() for c in checks if isinstance(c, dict) and c.get("name")]
    return []


def submission_aware_repair_candidates(
    checked_results: pd.DataFrame,
    region: str,
    *,
    target_mode: str,
    max_variants_per_parent: int = 6,
    turnover_trigger: float = 0.35,
) -> List[Dict[str, Any]]:
    """Generate a small repair set from live submission-check failure reasons.

    This does not claim any repair guarantees a platform pass. It only maps
    observed failures to a compact, explainable family of variants. ATOM and
    PP+ATOM avoid external event fields so single-dataset structure is preserved.
    """
    if checked_results is None or checked_results.empty:
        return []
    mode = normalize_target_mode(target_mode)
    out: List[Dict[str, Any]] = []

    for _, row in checked_results.iterrows():
        parent = row.get("candidate")
        if not isinstance(parent, dict):
            continue
        failed = set(_failed_check_names(row))
        turnover = float(row.get("turnover", 0) or 0)
        variants: List[Dict[str, Any]] = []

        def add_expr(expr: str, repair: str, **updates: Any) -> None:
            variants.append(
                _copy_candidate(
                    parent,
                    expr=expr,
                    stage=3,
                    search_tier="repair",
                    parent=_candidate_key(parent),
                    repair=repair,
                    **updates,
                )
            )

        if "LOW_SHARPE" in failed:
            add_expr(f"rank({parent['expr']})", "low_sharpe_rank")
            add_expr(f"ts_mean({parent['expr']}, 5)", "low_sharpe_ts_mean_5")

        if "LOW_2Y_SHARPE" in failed:
            add_expr(f"ts_mean({parent['expr']}, 22)", "low_2y_ts_mean_22")
            add_expr(f"ts_rank({parent['expr']}, 22)", "low_2y_ts_rank_22")

        if "LOW_SUB_UNIVERSE_SHARPE" in failed:
            add_expr(f"rank({parent['expr']})", "sub_universe_rank")
            if parent.get("group") != "industry":
                add_expr(
                    f"group_neutralize({parent['expr']}, densify(industry))",
                    "sub_universe_industry_neutralize",
                    group_operator="group_neutralize",
                    group="industry",
                    support_group_fields=_merge_unique_strings(
                        parent.get("support_group_fields"), "industry"
                    ),
                )
            base_decay = int(parent.get("decay", 6) or 6)
            variants.append(
                _copy_candidate(
                    parent,
                    stage=3,
                    search_tier="repair",
                    parent=_candidate_key(parent),
                    decay=min(base_decay + 2, 20),
                    repair="sub_universe_decay",
                )
            )

        weight_fail = any(
            name in failed
            for name in (
                "CONCENTRATED_WEIGHT",
                "WEIGHT_CONCENTRATION",
                "WEIGHT_DISTRIBUTION",
            )
        )
        if weight_fail:
            add_expr(f"rank({parent['expr']})", "weight_rank")
            add_expr(f"zscore({parent['expr']})", "weight_zscore")
            add_expr(f"hump({parent['expr']}, hump=0.01)", "weight_hump_0.01")

        if turnover > turnover_trigger or "HIGH_TURNOVER" in failed:
            base_decay = int(parent.get("decay", 6) or 6)
            for decay in _dedupe_keep_order([
                str(min(base_decay + 2, 20)),
                str(min(base_decay + 4, 20)),
            ]):
                variants.append(
                    _copy_candidate(
                        parent,
                        stage=3,
                        search_tier="repair",
                        parent=_candidate_key(parent),
                        decay=int(decay),
                        repair="turnover_decay",
                    )
                )
            add_expr(f"hump({parent['expr']}, hump=0.01)", "turnover_hump_0.01")

        # For Regular only, retain a tiny trade_when option. Single-dataset
        # modes intentionally avoid region event fields from other datasets.
        if mode == TARGET_REGULAR and ("LOW_SUB_UNIVERSE_SHARPE" in failed or turnover > turnover_trigger):
            for tw in trade_when_factory(
                "trade_when",
                parent["expr"],
                region,
                max_events=1,
                include_region_events=True,
            ):
                add_expr(tw, "trade_when")

        seen = set()
        kept: List[Dict[str, Any]] = []
        for variant in variants:
            annotated = annotate_candidate_strategy(variant, mode)
            # Preserve the target structure locally. Platform checks still make
            # the final eligibility decision.
            if mode == TARGET_POWER_POOL and not annotated["pp_structure_ok"]:
                continue
            if mode == TARGET_ATOM and not annotated["atom_structure_ok"]:
                continue
            if mode == TARGET_POWER_POOL_ATOM and not annotated["pp_atom_structure_ok"]:
                continue
            key = (annotated["expr"], int(annotated.get("decay", 6)))
            if key in seen:
                continue
            seen.add(key)
            kept.append(annotated)
            if len(kept) >= max(1, int(max_variants_per_parent)):
                break
        out.extend(kept)

    dedup: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for c in out:
        dedup.setdefault((c["expr"], int(c.get("decay", 6))), c)
    return list(dedup.values())


# ---------- SQLite experiment cache ----------

_CACHE_MIGRATIONS = {
    "simulation_url": "TEXT",
    "submitted_at": "TEXT",
    "retry_count": "INTEGER DEFAULT 0",
    "last_http_status": "INTEGER",
    "last_retry_after": "REAL",
    "warning": "TEXT",
}
_INITIALIZED_CACHE_PATHS = set()


def _connect_cache(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_cache(db_path: str = "alpha_results.db") -> None:
    cache_identity = str(Path(db_path).expanduser().resolve())
    if cache_identity in _INITIALIZED_CACHE_PATHS:
        return
    with _CACHE_INIT_LOCK:
        if cache_identity in _INITIALIZED_CACHE_PATHS:
            return
        conn = _connect_cache(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alpha_results (
                    sim_key TEXT PRIMARY KEY,
                    expr TEXT NOT NULL,
                    settings_json TEXT NOT NULL,
                    candidate_json TEXT,
                    alpha_id TEXT,
                    status TEXT,
                    sharpe REAL,
                    fitness REAL,
                    turnover REAL,
                    margin REAL,
                    returns REAL,
                    long_count INTEGER,
                    short_count INTEGER,
                    date_created TEXT,
                    error TEXT,
                    simulation_url TEXT,
                    submitted_at TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_http_status INTEGER,
                    last_retry_after REAL,
                    warning TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            existing_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(alpha_results)"
                )
            }
            for column, definition in _CACHE_MIGRATIONS.items():
                if column not in existing_columns:
                    conn.execute(
                        f"ALTER TABLE alpha_results ADD COLUMN {column} {definition}"
                    )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alpha_id ON alpha_results(alpha_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_status ON alpha_results(status)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alpha_contexts (
                    context_key TEXT PRIMARY KEY,
                    sim_key TEXT NOT NULL,
                    dataset_id TEXT,
                    target_mode TEXT,
                    stage INTEGER,
                    candidate_json TEXT,
                    strategy_json TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_context_sim_key ON alpha_contexts(sim_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_context_dataset ON alpha_contexts(dataset_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_context_target ON alpha_contexts(target_mode)"
            )
            conn.commit()
            _INITIALIZED_CACHE_PATHS.add(cache_identity)
        finally:
            conn.close()


def simulation_key(expr: str, settings: Dict[str, Any]) -> str:
    """Stable SHA-256 key from expression and canonical request settings."""
    payload = json.dumps(
        {"expr": expr.strip(), "settings": settings},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def cache_get(db_path: str, sim_key: str) -> Optional[Dict[str, Any]]:
    init_cache(db_path)
    conn = _connect_cache(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM alpha_results WHERE sim_key = ?", (sim_key,)
        ).fetchone()
        if not row:
            return None
        rec = dict(row)
        if rec.get("candidate_json"):
            rec["candidate"] = json.loads(rec["candidate_json"])
        if rec.get("settings_json"):
            rec["settings"] = json.loads(rec["settings_json"])
        return rec
    finally:
        conn.close()



def _candidate_metadata_view(candidate: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "dataset_id", "dataset_name", "field", "field_type", "vector_op",
        "stage", "search_tier", "operator", "window", "group_operator",
        "group", "repair", "parent", "decay", "data_coverage",
        "date_coverage", "api_coverage", "category_id", "category_name",
        "pyramid_multiplier", "themes",
    )
    return {k: _json_safe(candidate.get(k)) for k in keys if k in candidate}


def _strategy_metadata_view(candidate: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "target_mode", "operator_count", "operators", "data_field_count",
        "data_fields", "dataset_count", "dataset_ids", "is_single_dataset",
        "pp_structure_ok", "atom_structure_ok", "pp_atom_structure_ok",
        "support_group_fields", "direction_flipped",
    )
    return {k: _json_safe(candidate.get(k)) for k in keys if k in candidate}


def register_candidate_context(
    db_path: str,
    sim_key: str,
    candidate: Dict[str, Any],
    *,
    strategy_updates: Optional[Dict[str, Any]] = None,
) -> str:
    """Persist research context separately from the simulation cache row."""
    init_cache(db_path)
    candidate_meta = _candidate_metadata_view(candidate)
    strategy_meta = _strategy_metadata_view(candidate)
    if strategy_updates:
        strategy_meta.update(_json_safe(strategy_updates))
    context_payload = {
        "sim_key": sim_key,
        "dataset_id": candidate.get("dataset_id"),
        "target_mode": candidate.get("target_mode"),
        "stage": candidate.get("stage"),
        "field": candidate.get("field"),
        "operator": candidate.get("operator"),
        "group_operator": candidate.get("group_operator"),
        "group": candidate.get("group"),
        "repair": candidate.get("repair"),
    }
    context_key = hashlib.sha256(
        json.dumps(context_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    with _CACHE_WRITE_LOCK:
        conn = _connect_cache(db_path)
        try:
            conn.execute(
                """
                INSERT INTO alpha_contexts (
                    context_key, sim_key, dataset_id, target_mode, stage,
                    candidate_json, strategy_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(context_key) DO UPDATE SET
                    candidate_json=excluded.candidate_json,
                    strategy_json=excluded.strategy_json,
                    updated_at=excluded.updated_at
                """,
                (
                    context_key,
                    sim_key,
                    candidate.get("dataset_id"),
                    candidate.get("target_mode"),
                    candidate.get("stage"),
                    json.dumps(candidate_meta, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    json.dumps(strategy_meta, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    _now_iso(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    return context_key


def context_summary(
    db_path: str,
    *,
    dataset_id: Optional[str] = None,
    target_mode: Optional[str] = None,
) -> pd.DataFrame:
    init_cache(db_path)
    sql = "SELECT dataset_id, target_mode, stage, COUNT(*) AS contexts FROM alpha_contexts"
    clauses = []
    params: List[Any] = []
    if dataset_id is not None:
        clauses.append("dataset_id = ?")
        params.append(str(dataset_id))
    if target_mode is not None:
        clauses.append("target_mode = ?")
        params.append(normalize_target_mode(target_mode))
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " GROUP BY dataset_id, target_mode, stage ORDER BY dataset_id, target_mode, stage"
    conn = _connect_cache(db_path)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

def cache_put(
    db_path: str,
    sim_key: str,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    result: Dict[str, Any],
) -> None:
    init_cache(db_path)
    with _CACHE_WRITE_LOCK:
        conn = _connect_cache(db_path)
        try:
            conn.execute(
            """
            INSERT INTO alpha_results (
                sim_key, expr, settings_json, candidate_json, alpha_id, status,
                sharpe, fitness, turnover, margin, returns, long_count, short_count,
                date_created, error, simulation_url, submitted_at, retry_count,
                last_http_status, last_retry_after, warning, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sim_key) DO UPDATE SET
                expr=excluded.expr,
                settings_json=excluded.settings_json,
                candidate_json=excluded.candidate_json,
                alpha_id=excluded.alpha_id,
                status=excluded.status,
                sharpe=excluded.sharpe,
                fitness=excluded.fitness,
                turnover=excluded.turnover,
                margin=excluded.margin,
                returns=excluded.returns,
                long_count=excluded.long_count,
                short_count=excluded.short_count,
                date_created=excluded.date_created,
                error=excluded.error,
                simulation_url=COALESCE(excluded.simulation_url, alpha_results.simulation_url),
                submitted_at=COALESCE(excluded.submitted_at, alpha_results.submitted_at),
                retry_count=COALESCE(excluded.retry_count, alpha_results.retry_count, 0),
                last_http_status=COALESCE(excluded.last_http_status, alpha_results.last_http_status),
                last_retry_after=COALESCE(excluded.last_retry_after, alpha_results.last_retry_after),
                warning=excluded.warning,
                updated_at=excluded.updated_at
            """,
            (
                sim_key,
                candidate["expr"],
                json.dumps(
                    settings,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                json.dumps(
                    candidate,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                result.get("alpha_id"),
                result.get("status"),
                result.get("sharpe"),
                result.get("fitness"),
                result.get("turnover"),
                result.get("margin"),
                result.get("returns"),
                result.get("long_count"),
                result.get("short_count"),
                result.get("date_created"),
                result.get("error"),
                result.get("simulation_url"),
                result.get("submitted_at"),
                result.get("retry_count"),
                result.get("last_http_status"),
                result.get("last_retry_after"),
                result.get("warning"),
                _now_iso(),
            ),
        )
            conn.commit()
        finally:
            conn.close()


def cache_summary(
    db_path: str = "alpha_results.db", *, print_output: bool = True
) -> Dict[str, int]:
    """Return and optionally print the durable cache status distribution."""
    init_cache(db_path)
    statuses = (
        "COMPLETE",
        "SUBMITTED",
        "RUNNING",
        "ERROR",
        "INVALID",
        "AUTH_ERROR",
        "UNCERTAIN_SUBMISSION",
        "STALE_RUNNING",
    )
    conn = _connect_cache(db_path)
    try:
        counts = {
            str(status or "NULL").upper(): int(count)
            for status, count in conn.execute(
                "SELECT status, COUNT(*) FROM alpha_results GROUP BY status"
            )
        }
        result = {"TOTAL": int(sum(counts.values()))}
        result.update({status: counts.get(status, 0) for status in statuses})
        other = sum(
            count for status, count in counts.items() if status not in statuses
        )
        if other:
            result["OTHER"] = int(other)
    finally:
        conn.close()

    if print_output:
        print("Cache summary")
        print("-------------")
        for name, count in result.items():
            print(f"{name}: {count}")
    return result


def build_settings(
    candidate: Dict[str, Any],
    *,
    neutralization: str,
    region: str,
    universe: str,
    delay: int = 1,
    truncation: float = 0.08,
    test_period: str = "P0Y",
) -> Dict[str, Any]:
    return {
        "instrumentType": "EQUITY",
        "region": region,
        "universe": universe,
        "delay": delay,
        "decay": int(candidate.get("decay", 6)),
        "neutralization": neutralization,
        "truncation": truncation,
        "pasteurization": "ON",
        "testPeriod": test_period,
        "unitHandling": "VERIFY",
        "nanHandling": "ON",
        "language": "FASTEXPR",
        "visualization": False,
    }


# ---------- Simulation engine ----------

def _prepare_simulation_items(
    candidates: Sequence[Dict[str, Any]],
    *,
    neutralization: str,
    region: str,
    universe: str,
    delay: int,
    truncation: float,
    test_period: str,
) -> List[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    unique: Dict[str, Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for candidate in candidates:
        settings = build_settings(
            candidate,
            neutralization=neutralization,
            region=region,
            universe=universe,
            delay=delay,
            truncation=truncation,
            test_period=test_period,
        )
        key = simulation_key(candidate["expr"], settings)
        unique.setdefault(key, (candidate, settings))
    return [(key, candidate, settings) for key, (candidate, settings) in unique.items()]


def _resume_counts(
    items: Sequence[Tuple[str, Dict[str, Any], Dict[str, Any]]], db_path: str
) -> Dict[str, int]:
    counts = {
        "completed": 0,
        "submitted_running": 0,
        "errors": 0,
        "invalid": 0,
        "auth_error": 0,
        "uncertain": 0,
        "new": 0,
    }
    for key, _, _ in items:
        cached = cache_get(db_path, key)
        if not cached:
            counts["new"] += 1
            continue
        status = str(cached.get("status") or "").upper()
        if status == "COMPLETE":
            counts["completed"] += 1
        elif status in ("SUBMITTED", "RUNNING"):
            if cached.get("simulation_url"):
                counts["submitted_running"] += 1
            else:
                counts["uncertain"] += 1
        elif status == "INVALID":
            counts["invalid"] += 1
        elif status == "UNCERTAIN_SUBMISSION":
            counts["uncertain"] += 1
        elif status == "AUTH_ERROR":
            counts["auth_error"] += 1
        else:
            counts["errors"] += 1
    counts["remaining"] = (
        counts["submitted_running"]
        + counts["errors"]
        + counts["auth_error"]
        + counts["new"]
    )
    return counts


def _print_resume_summary(
    candidate_count: int,
    unique_count: int,
    counts: Dict[str, int],
) -> None:
    print("=" * 40)
    print("Resume summary")
    print("=" * 40)
    print(f"Candidates: {candidate_count}")
    print(f"Unique: {unique_count}")
    print("")
    print(f"Completed cache: {counts['completed']}")
    print(f"Submitted / running: {counts['submitted_running']}")
    print(f"Errors to retry: {counts['errors']}")
    print(f"Auth errors to retry: {counts['auth_error']}")
    print(f"Invalid: {counts['invalid']}")
    print(f"Uncertain submission (not auto-retried): {counts['uncertain']}")
    print(f"New: {counts['new']}")
    print("")
    print(f"Remaining simulations: {counts['remaining']}")
    print("=" * 40)


def resume_summary(
    candidates: Sequence[Dict[str, Any]],
    *,
    neutralization: str,
    region: str,
    universe: str,
    cache_db: str = "alpha_results.db",
    delay: int = 1,
    truncation: float = 0.08,
    test_period: str = "P0Y",
) -> Dict[str, int]:
    """Show how a candidate set maps to durable cache states before running."""
    init_cache(cache_db)
    items = _prepare_simulation_items(
        candidates,
        neutralization=neutralization,
        region=region,
        universe=universe,
        delay=delay,
        truncation=truncation,
        test_period=test_period,
    )
    for key, candidate, _settings in items:
        if candidate.get("target_mode"):
            register_candidate_context(cache_db, key, candidate)

    counts = _resume_counts(items, cache_db)
    _print_resume_summary(len(candidates), len(items), counts)
    return counts


def _submission_failure(
    *,
    category: str,
    response: Optional[requests.Response] = None,
    last_exception: Optional[BaseException] = None,
    retry_after: Optional[float] = None,
) -> RequestFailure:
    return RequestFailure(
        category=category,
        method="POST",
        url=f"{BRAIN_API_URL}/simulations",
        status_code=response.status_code if response is not None else None,
        retry_after=retry_after,
        body=_response_body(response),
        last_exception=last_exception,
    )


def submit_simulation(
    s: requests.Session,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    cache_db: str,
    sim_key: str,
    max_retries: int = 3,
    timeout: int = 90,
    progress_label: str = "",
    stop_event: Optional[threading.Event] = None,
    post_gate: Optional[_PostGate] = None,
    submission_meta: Optional[Dict[str, Any]] = None,
) -> str:
    """POST one simulation and persist SUBMITTED before any polling.

    A network exception leaves the server-side outcome unknown, so it is
    recorded as UNCERTAIN_SUBMISSION and is never automatically re-POSTed.
    """
    url = f"{BRAIN_API_URL}/simulations"
    payload = {"type": "REGULAR", "settings": settings, "regular": candidate["expr"]}
    existing = cache_get(cache_db, sim_key) or {}
    retry_count = int(existing.get("retry_count") or 0)
    max_retries = max(1, int(max_retries))
    delay_seconds = 2.0
    auth_refreshed = False

    # As above, the extra slot is used only when authentication is refreshed.
    for attempt in range(1, max_retries + 2):
        _check_stop(stop_event, "simulation POST")
        if post_gate is not None:
            post_gate.wait_for_turn(stop_event, progress_label)
        _check_stop(stop_event, "simulation POST start")
        try:
            response = s.post(url, json=payload, timeout=timeout)
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.RequestException,
            ConnectionResetError,
            RemoteDisconnected,
            TimeoutError,
        ) as exc:
            retry_count += 1
            failure = _submission_failure(
                category="UNCERTAIN_SUBMISSION", last_exception=exc
            )
            cache_put(
                cache_db,
                sim_key,
                candidate,
                settings,
                {
                    "status": "UNCERTAIN_SUBMISSION",
                    "error": str(failure),
                    "retry_count": retry_count,
                },
            )
            _safe_print(
                f"{progress_label} NETWORK outcome uncertain | "
                f"{type(exc).__name__}: {exc}",
                f"{progress_label} not re-POSTing automatically; review this record",
            )
            raise failure from exc

        status = int(response.status_code)
        # For HTTP 429 we need a usable cooldown even when the server omits
        # Retry-After. In that case fall back to the current exponential backoff.
        if status == 429:
            retry_after = _retry_after_seconds(response, delay_seconds)
        else:
            retry_after = (
                _retry_after_seconds(response, delay_seconds)
                if "Retry-After" in response.headers
                else None
            )

        if 200 <= status < 300:
            location = response.headers.get("Location")
            if not location:
                failure = _submission_failure(
                    category="UNCERTAIN_SUBMISSION",
                    response=response,
                    retry_after=retry_after,
                )
                cache_put(
                    cache_db,
                    sim_key,
                    candidate,
                    settings,
                    {
                        "status": "UNCERTAIN_SUBMISSION",
                        "error": str(failure) + " | missing Location header",
                        "retry_count": retry_count,
                        "last_http_status": status,
                        "last_retry_after": retry_after,
                    },
                )
                raise failure
            simulation_url = urljoin(response.url or url, location)
            submitted_at = _now_iso()
            cache_put(
                cache_db,
                sim_key,
                candidate,
                settings,
                {
                    "status": "SUBMITTED",
                    "simulation_url": simulation_url,
                    "submitted_at": submitted_at,
                    "retry_count": retry_count,
                    "last_http_status": status,
                    "last_retry_after": retry_after,
                    "error": None,
                },
            )
            _safe_print(
                f"{progress_label} POST submitted",
                f"           Location={simulation_url}",
            )
            if submission_meta is not None:
                submission_meta["retry_after"] = retry_after
                submission_meta["status_code"] = status
                submission_meta["simulation_url"] = simulation_url
            return simulation_url

        if status in (400, 422):
            failure = _submission_failure(
                category="INVALID", response=response, retry_after=retry_after
            )
            cache_put(
                cache_db,
                sim_key,
                candidate,
                settings,
                {
                    "status": "INVALID",
                    "error": str(failure),
                    "retry_count": retry_count,
                    "last_http_status": status,
                    "last_retry_after": retry_after,
                },
            )
            raise failure

        if status in (401, 403):
            if auth_refreshed:
                failure = _submission_failure(category="AUTH_ERROR", response=response)
                cache_put(
                    cache_db,
                    sim_key,
                    candidate,
                    settings,
                    {
                        "status": "AUTH_ERROR",
                        "error": str(failure),
                        "retry_count": retry_count,
                        "last_http_status": status,
                    },
                )
                raise failure
            try:
                ensure_session(s, stop_event=stop_event)
            except Exception as exc:
                failure = _submission_failure(
                    category="AUTH_ERROR", response=response, last_exception=exc
                )
                cache_put(
                    cache_db,
                    sim_key,
                    candidate,
                    settings,
                    {
                        "status": "AUTH_ERROR",
                        "error": str(failure),
                        "retry_count": retry_count,
                        "last_http_status": status,
                    },
                )
                raise failure from exc
            auth_refreshed = True
            retry_count += 1
            continue

        if status == 429 or status in (500, 502, 503, 504):
            retry_count += 1
            wait = (
                retry_after
                if status == 429 and retry_after is not None
                else min(delay_seconds, 30.0)
            )
            category = "RATE_LIMIT" if status == 429 else "SERVER_ERROR"
            if status == 429 and post_gate is not None:
                # Publish the cooldown before SQLite/log work so no other worker
                # can slip another POST through the gate after this response.
                post_gate.update_cooldown(wait)
            failure = _submission_failure(
                category=category,
                response=response,
                retry_after=wait,
            )
            cache_put(
                cache_db,
                sim_key,
                candidate,
                settings,
                {
                    "status": "ERROR",
                    "error": str(failure),
                    "retry_count": retry_count,
                    "last_http_status": status,
                    "last_retry_after": wait,
                },
            )
            if attempt >= max_retries:
                raise failure
            label = "429" if status == 429 else f"HTTP {status}"
            _safe_print(
                f"{progress_label} [{label}] retry {attempt + 1}/{max_retries} | "
                f"wait {wait:.1f}s"
            )
            if status != 429 or post_gate is None:
                _interruptible_wait(
                    stop_event,
                    max(0.5, wait),
                    f"simulation POST {label} backoff",
                )
            else:
                _check_stop(stop_event, "simulation POST 429 retry")
            delay_seconds = min(delay_seconds * 2, 30.0)
            continue

        failure = _submission_failure(category="HTTP_ERROR", response=response)
        cache_put(
            cache_db,
            sim_key,
            candidate,
            settings,
            {
                "status": "ERROR",
                "error": str(failure),
                "retry_count": retry_count,
                "last_http_status": status,
                "last_retry_after": retry_after,
            },
        )
        raise failure

    failure = _submission_failure(category="ERROR")
    cache_put(
        cache_db,
        sim_key,
        candidate,
        settings,
        {"status": "ERROR", "error": str(failure), "retry_count": retry_count},
    )
    raise failure


def _post_single_simulation(
    s: requests.Session,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    cache_db: str = "alpha_results.db",
    sim_key: Optional[str] = None,
) -> str:
    """Backward-compatible wrapper around the durable submit function."""
    key = sim_key or simulation_key(candidate["expr"], settings)
    return submit_simulation(
        s, candidate, settings, cache_db=cache_db, sim_key=key
    )


def _is_stale_running_record(
    record: Optional[Dict[str, Any]],
    *,
    stale_after_seconds: int = STALE_RUNNING_AFTER_SECONDS,
) -> bool:
    """Return True when a RUNNING record has exceeded local recovery window."""
    if not record:
        return False
    status = str(record.get("status") or "").upper()
    if status not in ("RUNNING", "SUBMITTED"):
        return False

    updated_at = record.get("updated_at") or record.get("submitted_at")
    if not updated_at:
        return False

    try:
        updated_ts = pd.Timestamp(updated_at)
        if updated_ts.tzinfo is None:
            updated_ts = updated_ts.tz_localize("UTC")
        age = (pd.Timestamp.utcnow() - updated_ts).total_seconds()
    except Exception:
        return False

    return age >= max(1, int(stale_after_seconds))


def mark_stale_running(
    cache_db: str,
    sim_key: str,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    reason: str,
) -> None:
    """Convert abandoned local RUNNING state without deleting resume metadata."""
    current = cache_get(cache_db, sim_key) or {}
    cache_put(
        cache_db,
        sim_key,
        candidate,
        settings,
        {
            "status": "STALE_RUNNING",
            "simulation_url": current.get("simulation_url"),
            "alpha_id": current.get("alpha_id"),
            "error": reason,
            "last_http_status": current.get("last_http_status"),
            "retry_count": current.get("retry_count"),
        },
    )


def wait_simulation(
    s: requests.Session,
    progress_url: str,
    *,
    cache_db: Optional[str] = None,
    sim_key: Optional[str] = None,
    candidate: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    poll_interval: float = 1.0,
    max_wait_seconds: int = 3600,
    progress_label: str = "",
    stop_event: Optional[threading.Event] = None,
) -> Dict[str, Any]:
    """Poll an existing simulation URL; this function never submits a POST."""
    start = time.time()

    def save_poll_state(result: Dict[str, Any]) -> None:
        if cache_db and sim_key and candidate is not None and settings is not None:
            cache_put(cache_db, sim_key, candidate, settings, result)

    while True:
        _check_stop(stop_event, "simulation poll")
        if time.time() - start > max_wait_seconds:
            error = f"Polling timeout after {max_wait_seconds}s: {progress_url}"
            save_poll_state(
                {
                    "status": "RUNNING",
                    "simulation_url": progress_url,
                    "error": error,
                }
            )
            raise TimeoutError(error)

        try:
            response = _request_with_retry(
                s,
                "GET",
                progress_url,
                stop_event=stop_event,
            )
        except SimulationInterrupted:
            _safe_print(
                f"{progress_label} interrupted; saved simulation URL will be resumed"
            )
            raise
        except KeyboardInterrupt:
            _safe_print(
                f"{progress_label} interrupted; saved simulation URL will be resumed"
            )
            raise
        except Exception as exc:
            save_poll_state(
                {
                    "status": "RUNNING",
                    "simulation_url": progress_url,
                    "error": str(exc),
                }
            )
            raise

        retry_after = _retry_after_seconds(response, poll_interval)
        try:
            payload = response.json()
        except ValueError as exc:
            error = (
                f"Invalid polling JSON | status={response.status_code} | "
                f"body={_response_body(response)} | {type(exc).__name__}: {exc}"
            )
            save_poll_state(
                {
                    "status": "RUNNING",
                    "simulation_url": progress_url,
                    "last_http_status": response.status_code,
                    "last_retry_after": retry_after,
                    "error": error,
                }
            )
            _safe_print(
                f"{progress_label} POLL invalid JSON; wait {retry_after:.1f}s"
            )
            _interruptible_wait(
                stop_event,
                max(0.5, retry_after),
                "invalid poll response wait",
            )
            continue

        api_status = str(payload.get("status") or "").upper()
        if api_status in ("COMPLETE", "WARNING"):
            # Persist the latest poll response before fetching metrics. If the
            # metrics request is interrupted, restart will poll this URL again.
            save_poll_state(
                {
                    "alpha_id": payload.get("alpha"),
                    "status": "RUNNING",
                    "simulation_url": progress_url,
                    "last_http_status": response.status_code,
                    "last_retry_after": retry_after,
                    "error": None,
                }
            )
            return payload
        if payload.get("alpha") and api_status not in (
            "PENDING",
            "RUNNING",
            "QUEUED",
        ):
            save_poll_state(
                {
                    "alpha_id": payload.get("alpha"),
                    "status": "RUNNING",
                    "simulation_url": progress_url,
                    "last_http_status": response.status_code,
                    "last_retry_after": retry_after,
                    "error": None,
                }
            )
            return payload
        if api_status in ("ERROR", "FAILED"):
            save_poll_state(
                {
                    "alpha_id": payload.get("alpha"),
                    "status": "ERROR",
                    "simulation_url": progress_url,
                    "last_http_status": response.status_code,
                    "last_retry_after": retry_after,
                    "error": json.dumps(payload, ensure_ascii=False)[:2000],
                }
            )
            return payload

        durable_status = "RUNNING"
        save_poll_state(
            {
                "alpha_id": payload.get("alpha"),
                "status": durable_status,
                "simulation_url": progress_url,
                "last_http_status": response.status_code,
                "last_retry_after": retry_after,
                "error": None,
            }
        )
        shown_status = api_status or "PENDING"
        _safe_print(
            f"{progress_label} POLL {shown_status} | waiting {retry_after:.1f}s"
        )
        _interruptible_wait(
            stop_event,
            max(0.5, retry_after),
            "simulation poll wait",
        )


def _wait_simulation(
    s: requests.Session,
    progress_url: str,
    *,
    poll_interval: float = 1.0,
    max_wait_seconds: int = 3600,
) -> Dict[str, Any]:
    """Backward-compatible polling wrapper."""
    return wait_simulation(
        s,
        progress_url,
        poll_interval=poll_interval,
        max_wait_seconds=max_wait_seconds,
    )


def locate_alpha(s: requests.Session, alpha_id: str) -> List[Any]:
    response = _request_with_retry(s, "GET", f"{BRAIN_API_URL}/alphas/{alpha_id}")
    metrics = response.json()
    is_metrics = metrics.get("is", {})
    return [
        alpha_id,
        metrics.get("regular", {}).get("code"),
        is_metrics.get("sharpe"),
        is_metrics.get("turnover"),
        is_metrics.get("fitness"),
        is_metrics.get("margin"),
        metrics.get("dateCreated"),
        metrics.get("settings", {}).get("decay"),
    ]


def fetch_alpha_result(
    s: requests.Session,
    alpha_id: str,
    *,
    stop_event: Optional[threading.Event] = None,
) -> Dict[str, Any]:
    _check_stop(stop_event, "metrics fetch")
    response = _request_with_retry(
        s,
        "GET",
        f"{BRAIN_API_URL}/alphas/{alpha_id}",
        stop_event=stop_event,
    )
    metrics = response.json()
    is_metrics = metrics.get("is", {})
    return {
        "alpha_id": alpha_id,
        "status": "COMPLETE",
        "sharpe": is_metrics.get("sharpe"),
        "fitness": is_metrics.get("fitness"),
        "turnover": is_metrics.get("turnover"),
        "margin": is_metrics.get("margin"),
        "returns": is_metrics.get("returns"),
        "long_count": is_metrics.get("longCount"),
        "short_count": is_metrics.get("shortCount"),
        "date_created": metrics.get("dateCreated"),
        "decay": metrics.get("settings", {}).get("decay"),
        "expr": metrics.get("regular", {}).get("code"),
    }


def _candidate_result_row(
    record: Optional[Dict[str, Any]],
    key: str,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    cached: bool,
) -> Dict[str, Any]:
    row = dict(record or {})
    # Simulation facts come from the cache record, but research metadata must
    # come from the *current* candidate/settings. This prevents an older run's
    # Dataset/TARGET_MODE metadata from leaking into a new Notebook run.
    row.update(
        {
            "candidate": candidate,
            "settings": settings,
            "sim_key": key,
            "cached": cached,
        }
    )
    return row


def _run_candidate_worker(
    position: int,
    total: int,
    width: int,
    key: str,
    candidate: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    base_session: requests.Session,
    cache_db: str,
    stop_event: threading.Event,
    post_gate: _PostGate,
    submit_max_retries: int,
    poll_timeout_seconds: int,
) -> Dict[str, Any]:
    """Run one complete candidate lifecycle without touching shared counters."""
    label = f"[{position:0{width}d}/{total}]"
    worker_session = _clone_session(base_session)
    action = "UNKNOWN"
    cached_result = False
    try:
        _check_stop(stop_event, "worker start")
        current = cache_get(cache_db, key)
        status = str((current or {}).get("status") or "NEW").upper()
        simulation_url: Optional[str] = None

        if status == "COMPLETE":
            action = "CACHE_COMPLETE"
            cached_result = True
            _safe_print(f"{label} CACHE COMPLETE")
            return {
                "position": position,
                "sim_key": key,
                "record": _candidate_result_row(
                    current, key, candidate, settings, cached=True
                ),
                "action": action,
                "interrupted": False,
            }

        if status == "INVALID":
            action = "CACHE_INVALID"
            cached_result = True
            _safe_print(f"{label} CACHE INVALID (not retried)")
            return {
                "position": position,
                "sim_key": key,
                "record": _candidate_result_row(
                    current, key, candidate, settings, cached=True
                ),
                "action": action,
                "interrupted": False,
            }

        if status == "UNCERTAIN_SUBMISSION":
            action = "CACHE_UNCERTAIN"
            cached_result = True
            _safe_print(f"{label} CACHE UNCERTAIN_SUBMISSION (not auto-retried)")
            return {
                "position": position,
                "sim_key": key,
                "record": _candidate_result_row(
                    current, key, candidate, settings, cached=True
                ),
                "action": action,
                "interrupted": False,
            }

        if status in ("SUBMITTED", "RUNNING"):
            if _is_stale_running_record(current):
                mark_stale_running(
                    cache_db,
                    key,
                    candidate,
                    settings,
                    reason=(
                        "Local polling exceeded stale timeout; simulation_url "
                        "preserved for manual resume."
                    ),
                )
                current = cache_get(cache_db, key)
                action = "STALE_RUNNING"
                _safe_print(
                    f"{label} STALE_RUNNING | existing simulation URL preserved"
                )
                return {
                    "position": position,
                    "sim_key": key,
                    "record": _candidate_result_row(
                        current, key, candidate, settings, cached=True
                    ),
                    "action": action,
                    "interrupted": False,
                }

            simulation_url = (current or {}).get("simulation_url")
            if not simulation_url:
                error = (
                    f"status={status} but simulation_url is missing; refusing to re-POST"
                )
                cache_put(
                    cache_db,
                    key,
                    candidate,
                    settings,
                    {
                        "status": "UNCERTAIN_SUBMISSION",
                        "error": error,
                        "retry_count": (current or {}).get("retry_count"),
                    },
                )
                action = "MISSING_URL"
                _safe_print(f"{label} UNCERTAIN_SUBMISSION | {error}")
                updated = cache_get(cache_db, key)
                return {
                    "position": position,
                    "sim_key": key,
                    "record": _candidate_result_row(
                        updated, key, candidate, settings, cached=True
                    ),
                    "action": action,
                    "interrupted": False,
                }
            action = "RESUME"
            _safe_print(
                f"{label} RESUME polling existing simulation",
                f"           Location={simulation_url}",
            )
        else:
            action = "POST"
            _check_stop(stop_event, "candidate POST")
            submission_meta: Dict[str, Any] = {}
            simulation_url = submit_simulation(
                worker_session,
                candidate,
                settings,
                cache_db=cache_db,
                sim_key=key,
                max_retries=submit_max_retries,
                progress_label=label,
                stop_event=stop_event,
                post_gate=post_gate,
                submission_meta=submission_meta,
            )
            _check_stop(stop_event, "post-submit transition")
            initial_poll_delay = float(submission_meta.get("retry_after") or 0.0)
            if initial_poll_delay > 0:
                _safe_print(
                    f"{label} INITIAL POLL waiting {initial_poll_delay:.1f}s"
                )
                _interruptible_wait(
                    stop_event,
                    initial_poll_delay,
                    "initial simulation poll delay",
                )

        if not simulation_url:
            raise RuntimeError(f"{label} no simulation_url after action={action}")

        _check_stop(stop_event, "simulation polling")
        progress = wait_simulation(
            worker_session,
            simulation_url,
            cache_db=cache_db,
            sim_key=key,
            candidate=candidate,
            settings=settings,
            progress_label=label,
            stop_event=stop_event,
            max_wait_seconds=max(60, int(poll_timeout_seconds)),
        )
        api_status = str(progress.get("status") or "").upper()
        alpha_id = progress.get("alpha")

        if api_status in ("COMPLETE", "WARNING") and alpha_id:
            _check_stop(stop_event, "metrics fetch")
            try:
                result = fetch_alpha_result(
                    worker_session,
                    alpha_id,
                    stop_event=stop_event,
                )
            except SimulationInterrupted:
                raise
            except Exception as exc:
                result = {
                    "alpha_id": alpha_id,
                    "status": "RUNNING",
                    "simulation_url": simulation_url,
                    "error": (
                        "Simulation completed but metrics fetch failed; "
                        f"will resume from simulation_url | {exc}"
                    ),
                }
                cache_put(cache_db, key, candidate, settings, result)
                raise
            result.update(
                {
                    "status": "COMPLETE",
                    "simulation_url": simulation_url,
                    "error": None,
                    "warning": (
                        json.dumps(progress, ensure_ascii=False)[:2000]
                        if api_status == "WARNING"
                        else None
                    ),
                }
            )
            cache_put(cache_db, key, candidate, settings, result)
            _safe_print(
                f"{label} COMPLETE",
                "           "
                f"Sharpe={result.get('sharpe')} | "
                f"Fitness={result.get('fitness')} | "
                f"Turnover={result.get('turnover')} | "
                f"Margin={result.get('margin')}",
            )
        else:
            result = {
                "alpha_id": alpha_id,
                "status": "ERROR",
                "simulation_url": simulation_url,
                "error": json.dumps(progress, ensure_ascii=False)[:2000],
            }
            cache_put(cache_db, key, candidate, settings, result)
            _safe_print(
                f"{label} ERROR | simulation returned {api_status or 'UNKNOWN'}"
            )

    except SimulationInterrupted as exc:
        action = f"{action}_INTERRUPTED"
        current = cache_get(cache_db, key)
        _safe_print(f"{label} INTERRUPTED | {exc}")
        return {
            "position": position,
            "sim_key": key,
            "record": _candidate_result_row(
                current, key, candidate, settings, cached=cached_result
            ),
            "action": action,
            "interrupted": True,
        }
    except Exception as exc:
        current = cache_get(cache_db, key)
        if current is None:
            failure_status = (
                exc.category
                if isinstance(exc, RequestFailure)
                and exc.category
                in ("INVALID", "AUTH_ERROR", "UNCERTAIN_SUBMISSION")
                else "ERROR"
            )
            cache_put(
                cache_db,
                key,
                candidate,
                settings,
                {"status": failure_status, "error": str(exc)},
            )
            current = cache_get(cache_db, key)
        _safe_print(f"{label} {(current or {}).get('status', 'ERROR')} | {exc}")

    finally:
        worker_session.close()

    current = cache_get(cache_db, key)
    return {
        "position": position,
        "sim_key": key,
        "record": _candidate_result_row(
            current, key, candidate, settings, cached=cached_result
        ),
        "action": action,
        "interrupted": False,
    }


def simulate_candidates(
    candidates: Sequence[Dict[str, Any]],
    *,
    neutralization: str,
    region: str,
    universe: str,
    session: Optional[requests.Session] = None,
    cache_db: str = "alpha_results.db",
    concurrency: int = 1,
    delay: int = 1,
    truncation: float = 0.08,
    test_period: str = "P0Y",
    reuse_cache: bool = True,
    progress_every: int = 10,
    submit_max_retries: int = 3,
    stop_event: Optional[threading.Event] = None,
    submit_stagger_seconds: float = SUBMIT_STAGGER_SECONDS,
    poll_timeout_seconds: int = 900,
    glb_max_concurrency: int = 4,
    other_max_concurrency: int = 8,
    _runtime_stats: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Bounded, durable and resumable simulation scheduler.

    At most ``concurrency`` futures are submitted at once. COMPLETE records are
    cache hits, SUBMITTED/RUNNING records resume their saved URL, ERROR and
    AUTH_ERROR records retry, while INVALID and UNCERTAIN_SUBMISSION remain
    terminal until explicitly changed by the user.
    """
    init_cache(cache_db)
    base_session = session or login()
    items = _prepare_simulation_items(
        candidates,
        neutralization=neutralization,
        region=region,
        universe=universe,
        delay=delay,
        truncation=truncation,
        test_period=test_period,
    )
    for key, candidate, _settings in items:
        if candidate.get("target_mode"):
            register_candidate_context(cache_db, key, candidate)

    counts = _resume_counts(items, cache_db)
    _print_resume_summary(len(candidates), len(items), counts)

    if not reuse_cache:
        _safe_print(
            "[SAFETY] reuse_cache=False is ignored: COMPLETE results are never re-submitted."
        )

    max_workers = resolve_concurrency(
        region,
        concurrency,
        glb_max=glb_max_concurrency,
        other_max=other_max_concurrency,
        announce=True,
    )
    _safe_print(f"Polling timeout per worker: {max(60, int(poll_timeout_seconds))}s")

    run_stop_event = stop_event or threading.Event()
    post_gate = _PostGate(submit_stagger_seconds)
    total = len(items)
    width = max(3, len(str(max(total, 1))))
    rows_by_key: Dict[str, Dict[str, Any]] = {}
    # task tuple:
    # (position, sim_key, candidate, settings, initial_cache_status)
    tasks: List[
        Tuple[int, str, Dict[str, Any], Dict[str, Any], str]
    ] = []
    scheduled_keys = set()

    for position, (key, candidate, settings) in enumerate(items, start=1):
        label = f"[{position:0{width}d}/{total}]"
        cached = cache_get(cache_db, key)
        status = str((cached or {}).get("status") or "NEW").upper()
        if status == "COMPLETE":
            _safe_print(f"{label} CACHE COMPLETE")
            rows_by_key[key] = _candidate_result_row(
                cached, key, candidate, settings, cached=True
            )
        elif status == "INVALID":
            _safe_print(f"{label} CACHE INVALID (not retried)")
            rows_by_key[key] = _candidate_result_row(
                cached, key, candidate, settings, cached=True
            )
        elif status == "UNCERTAIN_SUBMISSION":
            _safe_print(f"{label} CACHE UNCERTAIN_SUBMISSION (not auto-retried)")
            rows_by_key[key] = _candidate_result_row(
                cached, key, candidate, settings, cached=True
            )
        elif status in ("SUBMITTED", "RUNNING") and not cached.get(
            "simulation_url"
        ):
            error = f"status={status} but simulation_url is missing; refusing to re-POST"
            cache_put(
                cache_db,
                key,
                candidate,
                settings,
                {
                    "status": "UNCERTAIN_SUBMISSION",
                    "error": error,
                    "retry_count": cached.get("retry_count"),
                },
            )
            updated = cache_get(cache_db, key)
            _safe_print(f"{label} UNCERTAIN_SUBMISSION | {error}")
            rows_by_key[key] = _candidate_result_row(
                updated, key, candidate, settings, cached=True
            )
        else:
            if key not in scheduled_keys:
                scheduled_keys.add(key)
                tasks.append((position, key, candidate, settings, status))

    # Existing server-side simulations must be resumed first.  Otherwise a
    # cached RUNNING job can still occupy a BRAIN concurrency slot while this
    # scheduler incorrectly POSTs a replacement into the same slot.
    resume_statuses = {"SUBMITTED", "RUNNING"}
    tasks.sort(
        key=lambda item: (
            0 if item[4] in resume_statuses else 1,
            item[0],
        )
    )

    initial_resume_count = sum(
        1 for item in tasks if item[4] in resume_statuses
    )
    if initial_resume_count:
        _safe_print(
            f"Resume-first guard: {initial_resume_count} existing server-side "
            "simulation(s) will be polled before retry/new POSTs."
        )

    started = time.time()
    processed = total - len(tasks)
    progress_every = max(1, int(progress_every))
    runtime_stats = _runtime_stats if _runtime_stats is not None else {}
    runtime_stats.update(
        {
            "max_workers": max_workers,
            "max_futures": 0,
            "submitted_futures": 0,
            "processed": processed,
            "interrupted": False,
        }
    )

    def print_progress(active: int) -> None:
        elapsed_seconds = int(time.time() - started)
        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        status_counts: Dict[str, int] = {}
        for row in rows_by_key.values():
            status = str(row.get("status") or "UNKNOWN").upper()
            status_counts[status] = status_counts.get(status, 0) + 1
        _safe_print(
            "Progress",
            "--------",
            f"Processed: {processed} / {total}",
            f"Active: {active} / {max_workers}",
            f"Complete: {status_counts.get('COMPLETE', 0)}",
            f"Error: {status_counts.get('ERROR', 0)}",
            f"Invalid: {status_counts.get('INVALID', 0)}",
            f"Uncertain: {status_counts.get('UNCERTAIN_SUBMISSION', 0)}",
            f"Remaining: {max(0, total - processed)}",
            f"Elapsed: {hours:02d}:{minutes:02d}:{seconds:02d}",
        )

    if not tasks:
        runtime_stats["processed"] = processed
        ordered_rows = [
            rows_by_key[key] for key, _, _ in items if key in rows_by_key
        ]
        return pd.DataFrame(ordered_rows)

    executor = ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="brain-simulation",
    )
    task_iterator = iter(tasks)
    running: Dict[
        Future,
        Tuple[int, str, Dict[str, Any], Dict[str, Any], str],
    ] = {}
    normal_shutdown = False

    # If a worker returns with durable RUNNING (usually poll timeout), the
    # server-side simulation still consumes a BRAIN concurrency slot.  From
    # that point onward we do not POST replacement work during this invocation.
    # A later rerun will resume the saved simulation_url first.
    defer_new_posts = False
    deferred_task = None

    def fill_slots() -> None:
        nonlocal deferred_task
        while len(running) < max_workers and not run_stop_event.is_set():
            if deferred_task is not None:
                task = deferred_task
                deferred_task = None
            else:
                try:
                    task = next(task_iterator)
                except StopIteration:
                    return

            position, key, candidate, settings, initial_status = task

            if defer_new_posts and initial_status not in resume_statuses:
                # Keep this task and all following retry/new work for the next
                # invocation.  Do not consume a server slot that is still
                # occupied by a timed-out RUNNING simulation.
                deferred_task = task
                return
            future = executor.submit(
                _run_candidate_worker,
                position,
                total,
                width,
                key,
                candidate,
                settings,
                base_session=base_session,
                cache_db=cache_db,
                stop_event=run_stop_event,
                post_gate=post_gate,
                submit_max_retries=submit_max_retries,
                poll_timeout_seconds=poll_timeout_seconds,
            )
            running[future] = task
            runtime_stats["submitted_futures"] += 1
            runtime_stats["max_futures"] = max(
                runtime_stats["max_futures"], len(running)
            )

    try:
        fill_slots()
        while running:
            if run_stop_event.is_set():
                break
            completed, _ = wait(
                tuple(running),
                timeout=0.25,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                continue
            for future in completed:
                task = running.pop(future)
                position, key, candidate, settings, initial_status = task
                try:
                    worker_result = future.result()
                except Exception as exc:
                    current = cache_get(cache_db, key)
                    worker_result = {
                        "position": position,
                        "sim_key": key,
                        "record": _candidate_result_row(
                            current, key, candidate, settings, cached=False
                        ),
                        "action": "WORKER_EXCEPTION",
                        "interrupted": False,
                    }
                    _safe_print(
                        f"[{position:0{width}d}/{total}] WORKER_EXCEPTION | {exc}"
                    )
                rows_by_key[key] = worker_result["record"]
                processed += 1
                runtime_stats["processed"] = processed

                durable_status = str(
                    (worker_result.get("record") or {}).get("status") or ""
                ).upper()
                if durable_status in resume_statuses:
                    if not defer_new_posts:
                        _safe_print(
                            "[SERVER SLOT GUARD] A simulation is still RUNNING "
                            "server-side after local polling ended.",
                            "[SERVER SLOT GUARD] New/retry POSTs are deferred; "
                            "rerun this Stage later to resume the saved URL first.",
                        )
                    defer_new_posts = True

            fill_slots()
            if processed % progress_every == 0 or processed == total:
                print_progress(len(running))

        if run_stop_event.is_set():
            runtime_stats["interrupted"] = True
            for future in running:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            normal_shutdown = True
            _safe_print(
                "[INTERRUPT] stop requested",
                "[INTERRUPT] no new simulation will be submitted",
                "[INTERRUPT] existing simulation URLs are preserved",
                "[INTERRUPT] in-flight HTTP calls may take until their timeout to return",
                "[INTERRUPT] rerun this Stage cell to resume",
            )
        else:
            executor.shutdown(wait=True, cancel_futures=False)
            normal_shutdown = True
    except KeyboardInterrupt:
        runtime_stats["interrupted"] = True
        run_stop_event.set()
        for future in running:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        normal_shutdown = True
        _safe_print(
            "[INTERRUPT] stop requested",
            "[INTERRUPT] no new simulation will be submitted",
            "[INTERRUPT] existing simulation URLs are preserved",
            "[INTERRUPT] in-flight HTTP calls may take until their timeout to return",
            "[INTERRUPT] rerun this Stage cell to resume",
        )
        raise
    finally:
        if not normal_shutdown:
            run_stop_event.set()
            for future in running:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)

    # If server-slot protection deferred retry/new work, keep those candidates
    # visible in the returned DataFrame without POSTing them.  Their durable
    # cache state remains unchanged, so the next Stage rerun can continue.
    if defer_new_posts:
        deferred_count = 0
        for key, candidate, settings in items:
            if key in rows_by_key:
                continue
            cached = cache_get(cache_db, key)
            if cached is not None:
                rows_by_key[key] = _candidate_result_row(
                    cached, key, candidate, settings, cached=True
                )
            else:
                rows_by_key[key] = _candidate_result_row(
                    {
                        "status": "NEW",
                        "error": (
                            "Deferred: an existing server-side simulation is "
                            "still RUNNING and occupies a concurrency slot."
                        ),
                    },
                    key,
                    candidate,
                    settings,
                    cached=False,
                )
            deferred_count += 1
        if deferred_count:
            _safe_print(
                f"[SERVER SLOT GUARD] Deferred {deferred_count} candidate(s) "
                "without POSTing. Rerun the Stage to continue."
            )

    ordered_rows = [
        rows_by_key[key] for key, _, _ in items if key in rows_by_key
    ]
    return pd.DataFrame(ordered_rows)


# ---------- Scoring / promotion ----------

def _safe_numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def score_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-batch percentile score:
      35% Sharpe, 25% Fitness, 15% Margin,
      10% position coverage, 15% turnover quality.
    """
    if df.empty:
        return df.copy()

    x = df.copy()
    for col in ["sharpe", "fitness", "margin", "turnover", "long_count", "short_count"]:
        if col not in x:
            x[col] = 0.0

    x["sharpe"] = _safe_numeric(x["sharpe"])
    x["fitness"] = _safe_numeric(x["fitness"])
    x["margin"] = _safe_numeric(x["margin"])
    x["turnover"] = _safe_numeric(x["turnover"])
    x["long_count"] = _safe_numeric(x["long_count"])
    x["short_count"] = _safe_numeric(x["short_count"])

    x["abs_sharpe"] = x["sharpe"].abs()
    x["abs_fitness"] = x["fitness"].abs()
    x["abs_margin"] = x["margin"].abs()
    x["positions"] = x["long_count"] + x["short_count"]

    # Turnover quality: ideal broad band 1%-35%, softly penalize 35%-70%, hard penalize >70%.
    def turnover_quality(v: float) -> float:
        if v <= 0:
            return 0.0
        if 0.01 <= v <= 0.35:
            return 1.0
        if 0.35 < v <= 0.70:
            return max(0.2, 1.0 - (v - 0.35) / 0.35 * 0.8)
        return 0.0

    x["turnover_quality"] = x["turnover"].map(turnover_quality)
    x["coverage_raw"] = x["positions"].clip(lower=0)

    def pct_rank(s: pd.Series) -> pd.Series:
        if len(s) <= 1:
            return pd.Series([1.0] * len(s), index=s.index)
        return s.rank(pct=True, method="average")

    x["score"] = (
        0.35 * pct_rank(x["abs_sharpe"])
        + 0.25 * pct_rank(x["abs_fitness"])
        + 0.15 * pct_rank(x["abs_margin"])
        + 0.10 * pct_rank(x["coverage_raw"])
        + 0.15 * x["turnover_quality"]
    )
    return x


def _canonical_candidate(row: pd.Series) -> Dict[str, Any]:
    candidate = dict(row["candidate"])
    sharpe = float(row.get("sharpe", 0) or 0)
    if sharpe < 0:
        candidate["expr"] = f"-({candidate['expr']})"
        candidate["direction_flipped"] = True
    return candidate



def classify_stage1_results(
    results: pd.DataFrame,
    *,
    target_mode: Optional[str] = None,
    strict_abs_sharpe: float = 0.80,
    strict_abs_fitness: float = 0.45,
    min_positions: int = 100,
    max_turnover: float = 0.70,
    exploration_abs_sharpe: float = 0.55,
    exploration_abs_fitness: float = 0.25,
) -> pd.DataFrame:
    """Add explicit Stage-1 funnel labels instead of a binary pass/fail only.

    POWER_POOL / POWER_POOL_ATOM intentionally do not use Fitness as a Stage-1
    extension gate. ATOM / REGULAR keep the Fitness gate.
    """
    if results.empty:
        return results.copy()

    x = score_results(results)
    mode = normalize_target_mode(target_mode) if target_mode is not None else None
    pp_mode = mode in {TARGET_POWER_POOL, TARGET_POWER_POOL_ATOM}

    status = x.get("status", pd.Series(index=x.index, dtype=object)).astype(str).str.upper()
    complete = status.eq("COMPLETE")
    eligible_base = complete & x["positions"].ge(min_positions) & x["turnover"].le(max_turnover)

    if pp_mode:
        strict_fitness_ok = pd.Series(True, index=x.index)
        exploration_fitness_ok = pd.Series(True, index=x.index)
    else:
        strict_fitness_ok = x["abs_fitness"].ge(strict_abs_fitness)
        exploration_fitness_ok = x["abs_fitness"].ge(exploration_abs_fitness)

    x["fitness_gate_applied"] = not pp_mode
    x["is_strict_pass"] = (
        eligible_base
        & x["abs_sharpe"].ge(strict_abs_sharpe)
        & strict_fitness_ok
    )
    x["is_positive"] = (
        eligible_base
        & x["sharpe"].ge(exploration_abs_sharpe)
        & exploration_fitness_ok
    )
    x["is_flippable_negative"] = (
        eligible_base
        & x["sharpe"].le(-exploration_abs_sharpe)
        & exploration_fitness_ok
    )
    x["is_exploration"] = (
        eligible_base
        & x["abs_sharpe"].ge(exploration_abs_sharpe)
        & exploration_fitness_ok
        & ~x["is_strict_pass"]
    )

    x["signal_class"] = "REJECT"
    x.loc[x["is_exploration"], "signal_class"] = "EXPLORATION"
    x.loc[x["is_positive"], "signal_class"] = "POSITIVE"
    x.loc[x["is_flippable_negative"], "signal_class"] = "FLIPPABLE_NEGATIVE"
    x.loc[x["is_strict_pass"], "signal_class"] = "STRICT_PASS"
    return x

def stage1_classification_summary(classified: pd.DataFrame) -> Dict[str, Any]:
    if classified.empty:
        return {
            "complete": 0,
            "strict": 0,
            "positive": 0,
            "flippable_negative": 0,
            "exploration": 0,
            "strict_rate": 0.0,
            "eligible_strict_rate": 0.0,
        }
    status = classified.get("status", pd.Series(index=classified.index, dtype=object)).astype(str).str.upper()
    complete = int(status.eq("COMPLETE").sum())
    strict = int(classified.get("is_strict_pass", False).sum())
    eligible = classified[
        classified.get("is_positive", False)
        | classified.get("is_flippable_negative", False)
        | classified.get("is_exploration", False)
        | classified.get("is_strict_pass", False)
    ]
    return {
        "complete": complete,
        "strict": strict,
        "positive": int(classified.get("is_positive", False).sum()),
        "flippable_negative": int(classified.get("is_flippable_negative", False).sum()),
        "exploration": int(classified.get("is_exploration", False).sum()),
        "strict_rate": strict / complete if complete else 0.0,
        "eligible_strict_rate": strict / len(eligible) if len(eligible) else 0.0,
    }


def _result_with_strategy_columns(results: pd.DataFrame, target_mode: str) -> pd.DataFrame:
    x = score_results(results)
    mode = normalize_target_mode(target_mode)
    if "candidate" not in x.columns:
        return x
    annotated = x["candidate"].map(
        lambda c: annotate_candidate_strategy(c, mode) if isinstance(c, dict) else {}
    )
    x["candidate"] = annotated
    for col in (
        "target_mode", "operator_count", "data_field_count", "dataset_count",
        "is_single_dataset", "pp_structure_ok", "atom_structure_ok",
        "pp_atom_structure_ok", "data_coverage", "dataset_id", "field",
        "operator", "window", "vector_op", "group_operator", "group", "search_tier",
    ):
        x[col] = annotated.map(lambda c, key=col: c.get(key))
    return x


def _operator_family_name(operator: Any) -> str:
    """Map closely related operators into a coarse family for Stage1 diversity."""
    op = str(operator or "").strip().lower()
    if op in {"rank", "ts_rank"}:
        return "rank"
    if op in {"zscore", "ts_zscore"}:
        return "zscore"
    if op in {"ts_arg_min", "ts_arg_max"}:
        return "arg_extreme"
    if op == "ts_mean":
        return "mean"
    if op == "ts_delta":
        return "delta"
    if op == "ts_std_dev":
        return "volatility"
    if op == "ts_quantile":
        return "quantile"
    if op == "raw":
        return "raw"
    return op or "unknown"


def _select_stage1_diverse_candidates(
    eligible: pd.DataFrame,
    *,
    keep_per_field: int = 3,
    min_quality_ratio: float = 0.80,
) -> pd.DataFrame:
    """Keep strong Stage1 parents while avoiding near-duplicate structures.

    Rules per field:
    - First keep the strongest candidate.
    - Then prefer a new operator family/operator/window/vector reducer.
    - A candidate must still have abs(Sharpe) >= best abs(Sharpe) * ratio,
      so diversity never forces a clearly weak parent into Stage2.
    """
    if eligible is None or eligible.empty:
        return eligible.copy()

    keep_n = max(1, int(keep_per_field))
    ratio = max(0.0, min(1.0, float(min_quality_ratio)))
    selected_parts = []

    for _, group_df in eligible.groupby("field", sort=False):
        g = group_df.sort_values(
            ["score", "abs_sharpe"],
            ascending=False,
        ).copy()
        if g.empty:
            continue

        if "vector_op" not in g.columns:
            g["vector_op"] = g["candidate"].map(
                lambda c: c.get("vector_op") if isinstance(c, dict) else None
            )
        g["_op_family"] = g["operator"].map(_operator_family_name)

        best = g.iloc[0]
        best_abs_sharpe = float(best.get("abs_sharpe") or 0.0)
        quality_floor = best_abs_sharpe * ratio

        # The best always stays. Additional choices must clear the relative
        # quality floor; eligible already cleared the absolute Stage1 gate.
        quality_pool = g[
            pd.to_numeric(g["abs_sharpe"], errors="coerce").fillna(0.0)
            >= quality_floor
        ].copy()

        chosen_indices = [best.name]

        while len(chosen_indices) < keep_n:
            remaining = quality_pool.loc[
                ~quality_pool.index.isin(chosen_indices)
            ].copy()
            if remaining.empty:
                break

            chosen = g.loc[chosen_indices]
            used_families = set(chosen["_op_family"].astype(str))
            used_ops = set(chosen["operator"].astype(str))
            used_windows = set(chosen["window"].astype(str))
            used_vecs = set(chosen["vector_op"].astype(str))

            # Larger novelty wins first; score and Sharpe break ties.
            remaining["_novelty"] = (
                (~remaining["_op_family"].astype(str).isin(used_families)).astype(int) * 8
                + (~remaining["operator"].astype(str).isin(used_ops)).astype(int) * 4
                + (~remaining["window"].astype(str).isin(used_windows)).astype(int) * 2
                + (~remaining["vector_op"].astype(str).isin(used_vecs)).astype(int)
            )

            pick = remaining.sort_values(
                ["_novelty", "score", "abs_sharpe"],
                ascending=False,
            ).iloc[0]
            chosen_indices.append(pick.name)

        part = g.loc[chosen_indices].drop(
            columns=["_op_family"],
            errors="ignore",
        )
        selected_parts.append(part)

    if not selected_parts:
        return eligible.iloc[0:0].copy()

    return pd.concat(selected_parts, axis=0).sort_values(
        ["score", "abs_sharpe"],
        ascending=False,
    )


def promote_candidates_for_target(
    results: pd.DataFrame,
    *,
    target_mode: str,
    min_abs_sharpe: float = 0.80,
    min_abs_fitness: float = 0.45,
    min_positions: int = 100,
    keep_per_field: int = 2,
    max_total: int = 60,
    allow_negative_flip: bool = True,
    power_pool_min_sharpe: float = 1.0,
    stage1_diversity_mode: Optional[bool] = None,
    stage1_keep_per_field: int = 3,
    stage1_min_quality_ratio: float = 0.80,
) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    """Research-stage promotion tailored to the requested Alpha type.

    Platform eligibility/submission checks remain authoritative. This function
    only avoids spending Stage-2/Repair simulations on candidates that already
    violate known local structure or broad quality constraints.
    """
    if results.empty:
        return [], results.copy()
    mode = normalize_target_mode(target_mode)
    scored = _result_with_strategy_columns(results, mode)
    if "status" not in scored.columns:
        return [], scored.iloc[0:0].copy()
    scored = scored[scored["status"].astype(str).str.upper().eq("COMPLETE")].copy()
    if scored.empty:
        return [], scored

    if allow_negative_flip:
        sharpe_mask = scored["abs_sharpe"].ge(min_abs_sharpe)
    else:
        sharpe_mask = scored["sharpe"].ge(min_abs_sharpe)

    mask = sharpe_mask & scored["positions"].ge(min_positions) & scored["turnover"].le(0.70)

    if mode == TARGET_REGULAR:
        mask &= scored["abs_fitness"].ge(min_abs_fitness)
    elif mode == TARGET_ATOM:
        mask &= scored["atom_structure_ok"].fillna(False)
        mask &= scored["abs_fitness"].ge(min_abs_fitness)
    elif mode == TARGET_POWER_POOL:
        # Power Pool local prefilter: official live checks make the final call.
        mask &= scored["pp_structure_ok"].fillna(False)
        mask &= scored["turnover"].ge(0.01)
        pp_min = max(0.0, float(power_pool_min_sharpe))
        mask &= scored["abs_sharpe"].ge(pp_min) if allow_negative_flip else scored["sharpe"].ge(pp_min)
    elif mode == TARGET_POWER_POOL_ATOM:
        mask &= scored["pp_atom_structure_ok"].fillna(False)
        mask &= scored["turnover"].ge(0.01)
        pp_min = max(0.0, float(power_pool_min_sharpe))
        mask &= scored["abs_sharpe"].ge(pp_min) if allow_negative_flip else scored["sharpe"].ge(pp_min)

    eligible = scored[mask].copy()
    if eligible.empty:
        return [], eligible
    eligible = eligible.sort_values(["score", "abs_sharpe"], ascending=False)

    # Backward-compatible Stage1 detection:
    # the current Stage1 funnel is the only one that enables negative flipping.
    # This lets the existing Notebook keep its old keep_per_field=2 argument
    # while Stage1 transparently upgrades to diversity-aware max-3 selection.
    use_stage1_diversity = (
        bool(allow_negative_flip)
        if stage1_diversity_mode is None
        else bool(stage1_diversity_mode)
    )

    if use_stage1_diversity:
        effective_keep = max(
            int(stage1_keep_per_field),
            int(keep_per_field),
            1,
        )
        diverse = _select_stage1_diverse_candidates(
            eligible,
            keep_per_field=effective_keep,
            min_quality_ratio=stage1_min_quality_ratio,
        )
        diverse = diverse.head(max(1, int(max_total))).copy()
    else:
        diverse = (
            eligible.groupby("field", group_keys=False)
            .head(max(1, int(keep_per_field)))
            .sort_values(["score", "abs_sharpe"], ascending=False)
            .head(max(1, int(max_total)))
            .copy()
        )
    promoted = [_canonical_candidate(row) for _, row in diverse.iterrows()]
    promoted = [annotate_candidate_strategy(c, mode) for c in promoted]
    return promoted, diverse


def restore_cached_results_for_candidates(
    candidates: Sequence[Dict[str, Any]],
    *,
    cache_db: str,
    neutralization: str,
    region: str,
    universe: str,
    delay: int = 1,
    truncation: float = 0.08,
    test_period: str = "P0Y",
    label: str = "Stage1",
) -> pd.DataFrame:
    """Restore exact simulation rows from alpha_results.db without POSTing."""
    rows: List[Dict[str, Any]] = []
    seen_keys = set()
    missing_keys: List[str] = []

    for candidate in candidates:
        settings = build_settings(
            candidate,
            neutralization=neutralization,
            region=region,
            universe=universe,
            delay=delay,
            truncation=truncation,
            test_period=test_period,
        )
        key = simulation_key(candidate["expr"], settings)

        if key in seen_keys:
            continue
        seen_keys.add(key)

        cached = cache_get(cache_db, key)
        if cached is None:
            missing_keys.append(key)
            continue

        row = dict(cached)
        row.update(
            {
                "candidate": candidate,
                "settings": settings,
                "sim_key": key,
                "cached": True,
            }
        )
        rows.append(row)

    result = pd.DataFrame(rows)

    print(f"{label} cache recovery: {len(rows)} / {len(seen_keys)}")
    print(f"{label} cache missing: {len(missing_keys)}")
    if not result.empty and "status" in result.columns:
        print(
            f"{label} status:",
            result["status"]
            .fillna("NONE")
            .astype(str)
            .str.upper()
            .value_counts()
            .to_dict(),
        )

    return result


def restore_stage1_cache_and_promote(
    *,
    session: Optional[requests.Session],
    cache_db: str,
    target_mode: str,
    dataset_id: str,
    region: str,
    universe: str,
    neutralization: str,
    delay: int = 1,
    truncation: float = 0.08,
    test_period: str = "P0Y",
    init_decay: int = 4,
    min_data_coverage: float = 0.90,
    data_coverage_column: Optional[str] = None,
    max_fields: Optional[int] = None,
    manual_excluded_fields_by_dataset: Optional[Dict[str, Sequence[str]]] = None,
    field_records: Optional[Sequence[Dict[str, Any]]] = None,
    core_ts_ops: Sequence[str] = CORE_TS_OPS,
    stage1_min_abs_sharpe: float = 0.80,
    stage1_min_abs_fitness: float = 0.45,
    stage1_exploration_sharpe: float = 0.55,
    stage1_exploration_fitness: float = 0.25,
    stage1_min_positions: int = 100,
    stage1_pp_sharpe_gate: float = 0.80,
    stage1_keep_per_field: int = 3,
    stage1_min_quality_ratio: float = 0.80,
    max_total: int = 60,
) -> Dict[str, Any]:
    """Cache-only Stage1 restore + promotion for Notebook 1 -> 2 -> 3 -> 6.

    This helper never calls simulate_candidates and therefore never creates a
    new simulation POST.

    If field_records is missing, Data Fields are rebuilt from BRAIN using the
    current Dataset/Region/Universe/Coverage configuration.
    """
    mode = normalize_target_mode(target_mode)

    # 1) Resolve / rebuild field_records
    df_fields = pd.DataFrame()
    coverage_report: Optional[Dict[str, Any]] = None

    if field_records is not None and len(field_records) > 0:
        resolved_field_records = [dict(x) for x in field_records]
        field_source = "memory"
    else:
        field_source = "BRAIN data-fields"
        if session is None:
            raise RuntimeError(
                "field_records is missing and no BRAIN session was supplied. "
                "Run the login cell first."
            )

        df_raw = get_datafields(
            session,
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
        )

        df_cov, coverage_report = filter_datafields(
            df_raw,
            min_data_coverage=min_data_coverage,
            coverage_column=data_coverage_column,
        )

        excluded_map = manual_excluded_fields_by_dataset or {}
        excluded = {
            str(x)
            for x in excluded_map.get(str(dataset_id), ())
        }

        if excluded:
            df_fields = df_cov[
                ~df_cov["id"].astype(str).isin(excluded)
            ].copy()
        else:
            df_fields = df_cov.copy()

        if max_fields is not None:
            df_fields = df_fields.head(max(0, int(max_fields))).copy()

        df_fields = df_fields.reset_index(drop=True)
        resolved_field_records = prepare_fields(df_fields)

    if not resolved_field_records:
        raise RuntimeError(
            f"No usable field_records for dataset {dataset_id!r}. "
            "Check Data Coverage filtering and Dataset settings."
        )

    print("Stage1 field source:", field_source)
    print("Stage1 prepared field expressions:", len(resolved_field_records))

    # 2) Core candidates -> DB cache only
    core_candidates = first_order_candidates(
        resolved_field_records,
        ts_operators=core_ts_ops,
        cross_ops=("rank", "zscore"),
        init_decay=init_decay,
    )
    core_candidates = annotate_candidates(core_candidates, mode)
    validate_candidate_context(
        core_candidates,
        dataset_id=dataset_id,
        target_mode=mode,
    )

    core_results = restore_cached_results_for_candidates(
        core_candidates,
        cache_db=cache_db,
        neutralization=neutralization,
        region=region,
        universe=universe,
        delay=delay,
        truncation=truncation,
        test_period=test_period,
        label="Stage1 Core",
    )

    # 3) Core funnel -> Extended fields
    extension_fields: List[str] = []
    core_classified = pd.DataFrame()

    if not core_results.empty:
        core_classified = classify_stage1_results(
            core_results,
            target_mode=mode,
            strict_abs_sharpe=stage1_min_abs_sharpe,
            strict_abs_fitness=stage1_min_abs_fitness,
            min_positions=stage1_min_positions,
            exploration_abs_sharpe=stage1_exploration_sharpe,
            exploration_abs_fitness=stage1_exploration_fitness,
        )

        core_classified["field"] = core_classified["candidate"].map(
            lambda c: c.get("field") if isinstance(c, dict) else None
        )

        active_mask = (
            core_classified["is_strict_pass"]
            | core_classified["is_positive"]
            | core_classified["is_flippable_negative"]
            | core_classified["is_exploration"]
        )

        extension_fields = sorted(
            core_classified.loc[active_mask, "field"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

    print("Fields eligible for Extended Stage1:", len(extension_fields))
    print(extension_fields)

    # 4) Extended candidates -> DB cache only
    extended_candidates = extended_first_order_candidates(
        resolved_field_records,
        active_fields=extension_fields,
        init_decay=init_decay,
    )
    extended_candidates = annotate_candidates(extended_candidates, mode)

    if extended_candidates:
        validate_candidate_context(
            extended_candidates,
            dataset_id=dataset_id,
            target_mode=mode,
        )
        extended_results = restore_cached_results_for_candidates(
            extended_candidates,
            cache_db=cache_db,
            neutralization=neutralization,
            region=region,
            universe=universe,
            delay=delay,
            truncation=truncation,
            test_period=test_period,
            label="Stage1 Extended",
        )
    else:
        extended_results = pd.DataFrame()
        print("Stage1 Extended candidates: 0")

    # 5) Merge + sim_key dedupe
    frames = [
        frame
        for frame in (core_results, extended_results)
        if isinstance(frame, pd.DataFrame) and not frame.empty
    ]

    if frames:
        stage1_results = pd.concat(frames, ignore_index=True, sort=False)
        if "sim_key" in stage1_results.columns:
            stage1_results = stage1_results.drop_duplicates(
                subset=["sim_key"],
                keep="last",
            )
    else:
        stage1_results = pd.DataFrame()

    # 6) Current Stage1 diversity-aware promotion
    promoted, selected = promote_candidates_for_target(
        stage1_results,
        target_mode=mode,
        min_abs_sharpe=stage1_min_abs_sharpe,
        min_abs_fitness=stage1_min_abs_fitness,
        min_positions=stage1_min_positions,
        keep_per_field=stage1_keep_per_field,
        max_total=max_total,
        allow_negative_flip=True,
        power_pool_min_sharpe=stage1_pp_sharpe_gate,
        stage1_diversity_mode=True,
        stage1_keep_per_field=stage1_keep_per_field,
        stage1_min_quality_ratio=stage1_min_quality_ratio,
    )

    validate_candidate_context(
        promoted,
        dataset_id=dataset_id,
        target_mode=mode,
    )

    status_counts = {}
    if not stage1_results.empty and "status" in stage1_results.columns:
        status_counts = (
            stage1_results["status"]
            .fillna("NONE")
            .astype(str)
            .str.upper()
            .value_counts()
            .to_dict()
        )

    print("\n========================================")
    print("Stage1 cache restore / promotion summary")
    print("========================================")
    print("Stage1 total results:", len(stage1_results))
    print("Stage1 status:", status_counts)
    print("Stage1 promoted:", len(promoted))

    return {
        "field_records": resolved_field_records,
        "df_fields": df_fields,
        "coverage_report": coverage_report,
        "core_candidates": core_candidates,
        "core_results": core_results,
        "core_classified": core_classified,
        "extension_fields": extension_fields,
        "extended_candidates": extended_candidates,
        "extended_results": extended_results,
        "stage1_results": stage1_results,
        "stage1_promoted": promoted,
        "stage1_selected": selected,
    }


def promote_candidates(
    results: pd.DataFrame,
    *,
    min_abs_sharpe: float = 0.8,
    min_abs_fitness: float = 0.45,
    min_positions: int = 100,
    keep_per_field: int = 3,
    max_total: int = 80,
) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    """
    Promote high-quality candidates while preserving field diversity.
    """
    if results.empty:
        return [], results.copy()

    scored = score_results(results)
    if "status" not in scored.columns:
        return [], scored.iloc[0:0].copy()
    scored = scored[scored["status"] == "COMPLETE"].copy()
    if scored.empty:
        return [], scored

    mask = (
        (scored["abs_sharpe"] >= min_abs_sharpe)
        & (scored["abs_fitness"] >= min_abs_fitness)
        & (scored["positions"] >= min_positions)
        & (scored["turnover"] <= 0.70)
    )
    eligible = scored[mask].copy()
    if eligible.empty:
        return [], eligible

    eligible["field"] = eligible["candidate"].map(lambda c: c.get("field", "UNKNOWN"))
    eligible = eligible.sort_values(["score", "abs_sharpe"], ascending=False)

    # Keep a limited number per field first.
    diverse = (
        eligible.groupby("field", group_keys=False)
        .head(max(1, int(keep_per_field)))
        .sort_values(["score", "abs_sharpe"], ascending=False)
        .head(max_total)
        .copy()
    )

    promoted = [_canonical_candidate(row) for _, row in diverse.iterrows()]
    return promoted, diverse


def prune(next_alpha_recs, prefix=None, keep_num=5):
    """
    Legacy wrapper.
    If records contain metadata, prefer promote_candidates instead.
    """
    output = []
    count = {}
    for rec in next_alpha_recs:
        try:
            exp = rec[1]
            decay = rec[-1]
        except Exception:
            continue
        field = exp
        if prefix:
            try:
                field = exp.split(prefix)[-1].split(",")[0]
            except Exception:
                field = exp
        count[field] = count.get(field, 0)
        if count[field] < keep_num:
            count[field] += 1
            output.append([exp, decay])
    return output


# ---------- Power Pool properties ----------

POWER_POOL_SELECTED_TAG = "PowerPoolSelected"


def get_alpha_details(s: requests.Session, alpha_id: str) -> Dict[str, Any]:
    """Fetch the current Alpha object before changing user-visible properties."""
    response = _request_with_retry(
        s, "GET", f"{BRAIN_API_URL}/alphas/{alpha_id}"
    )
    return response.json()


def build_power_pool_description_draft(candidate: Dict[str, Any]) -> str:
    """Build a conservative English draft from known metadata only.

    The draft does not invent the economic meaning of the field. Review/edit it
    before applying it to BRAIN.
    """
    c = candidate if isinstance(candidate, dict) else {}
    fields = _merge_unique_strings(c.get("data_fields"), c.get("field"))
    datasets = _merge_unique_strings(c.get("dataset_ids"), c.get("dataset_id"))
    operator = c.get("operator") or "the selected transformation"
    window = c.get("window")
    group_operator = c.get("group_operator")
    group = c.get("group")

    field_text = ", ".join(fields) if fields else "the selected data field"
    dataset_text = ", ".join(datasets) if datasets else "the selected dataset"
    op_text = str(operator)
    if window not in (None, "", "None"):
        op_text += f" with a {window}-day window"

    group_text = ""
    if group_operator and group:
        group_text = (
            f" The expression also uses {group_operator} with {group} "
            "to reduce broad group-level exposure."
        )

    return (
        f"Idea: Use the recent cross-sectional behavior of {field_text} as a compact "
        f"signal and transform it with {op_text} to identify relative differences "
        "across securities. "
        f"Rationale for data used: The expression uses {field_text} from {dataset_text}, "
        "keeping the research input focused on a small and traceable set of fields. "
        f"Rationale for operators used: The preprocessing handles missing/extreme values "
        f"and the selected transformation summarizes or standardizes recent behavior."
        f"{group_text}"
    )


def prepare_power_pool_alpha_properties(
    s: requests.Session,
    alpha_id: str,
    *,
    description: str,
    tag: str = POWER_POOL_SELECTED_TAG,
    min_description_chars: int = 100,
) -> Dict[str, Any]:
    """Add PowerPoolSelected and a reviewed description to one Alpha.

    This is a WRITE operation (PATCH) and is never called automatically by the
    simulation engine.
    """
    desc = str(description or "").strip()
    min_chars = max(1, int(min_description_chars))
    if len(desc) < min_chars:
        raise ValueError(
            f"Power Pool description is too short for {alpha_id}: "
            f"{len(desc)} < {min_chars} characters"
        )

    details = get_alpha_details(s, str(alpha_id))
    existing_tags = details.get("tags") or []
    tag_names: List[str] = []
    for item in existing_tags:
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict):
            name = item.get("name") or item.get("id")
        else:
            name = None
        if name:
            name = str(name)
            if name not in tag_names:
                tag_names.append(name)

    if tag not in tag_names:
        tag_names.append(tag)

    payload = {
        "tags": tag_names,
        "regular": {"description": desc},
    }
    response = _request_with_retry(
        s,
        "PATCH",
        f"{BRAIN_API_URL}/alphas/{alpha_id}",
        json_body=payload,
    )
    return {
        "alpha_id": str(alpha_id),
        "tag": tag,
        "tags_after": tag_names,
        "description_chars": len(desc),
        "properties_applied": True,
        "http_status": response.status_code,
    }


def prepare_power_pool_candidates(
    s: requests.Session,
    results: pd.DataFrame,
    *,
    descriptions: Optional[Dict[str, str]] = None,
    tag: str = POWER_POOL_SELECTED_TAG,
    min_description_chars: int = 100,
    apply: bool = False,
) -> pd.DataFrame:
    """Validate/dry-run or apply Power Pool properties for Final candidates."""
    if results is None or results.empty:
        return pd.DataFrame()

    descriptions = descriptions or {}
    rows: List[Dict[str, Any]] = []

    for _, row in results.iterrows():
        alpha_id = row.get("alpha_id")
        candidate = row.get("candidate")
        if not alpha_id:
            continue

        alpha_key = str(alpha_id)
        manual_desc = descriptions.get(alpha_key)
        draft = build_power_pool_description_draft(
            candidate if isinstance(candidate, dict) else {}
        )
        desc = str(manual_desc or "").strip()
        enough = len(desc) >= max(1, int(min_description_chars))

        out = {
            "alpha_id": alpha_key,
            "description_chars": len(desc),
            "description_ready": bool(enough),
            "tag": tag,
            "properties_applied": False,
            "property_status": "READY" if enough else "NEEDS_DESCRIPTION",
            "description_draft": draft,
            "property_error": None,
        }

        if apply:
            if not enough:
                out["property_status"] = "SKIPPED_SHORT_DESCRIPTION"
            else:
                try:
                    applied = prepare_power_pool_alpha_properties(
                        s,
                        alpha_key,
                        description=desc,
                        tag=tag,
                        min_description_chars=min_description_chars,
                    )
                    out.update(applied)
                    out["property_status"] = "APPLIED"
                except Exception as exc:
                    out["property_status"] = "ERROR"
                    out["property_error"] = f"{type(exc).__name__}: {exc}"

        rows.append(out)

    return pd.DataFrame(rows)


# ---------- Submission checks ----------

def get_check_submission_detailed(s: requests.Session, alpha_id: str) -> Dict[str, Any]:
    response = _request_with_retry(
        s, "GET", f"{BRAIN_API_URL}/alphas/{alpha_id}/check"
    )
    payload = response.json()
    checks = payload.get("is", {}).get("checks", [])
    failed = [c for c in checks if c.get("result") == "FAIL"]
    self_corr = None
    for c in checks:
        if c.get("name") == "SELF_CORRELATION":
            self_corr = c.get("value")
            break
    return {
        "alpha_id": alpha_id,
        "passed": len(failed) == 0,
        "self_correlation": self_corr,
        "failed_checks": failed,
        "checks": checks,
    }



def check_submission_candidates(
    s: requests.Session,
    results: pd.DataFrame,
    *,
    limit: Optional[int] = 30,
    cache_db: Optional[str] = None,
) -> pd.DataFrame:
    """Run live /check for result rows and keep failure reasons with candidates."""
    if results is None or results.empty:
        return pd.DataFrame()
    rows = []
    source = results.copy()
    if limit is not None:
        source = source.head(max(0, int(limit)))
    for _, row in source.iterrows():
        alpha_id = row.get("alpha_id")
        if not alpha_id:
            continue
        base = row.to_dict()
        try:
            detail = get_check_submission_detailed(s, str(alpha_id))
            failed_names = [
                str(c.get("name")) for c in detail.get("failed_checks", []) if c.get("name")
            ]
            base.update(
                {
                    "check_pass": bool(detail.get("passed")),
                    "self_correlation": detail.get("self_correlation"),
                    "failed_checks": detail.get("failed_checks", []),
                    "failed_check_names": failed_names,
                    "checks": detail.get("checks", []),
                    "check_error": None,
                }
            )
            candidate = base.get("candidate")
            sim_key = base.get("sim_key")
            if cache_db and isinstance(candidate, dict) and sim_key:
                register_candidate_context(
                    cache_db,
                    str(sim_key),
                    candidate,
                    strategy_updates={
                        "check_pass": bool(detail.get("passed")),
                        "self_correlation": detail.get("self_correlation"),
                        "failed_check_names": failed_names,
                    },
                )
        except Exception as exc:
            base.update(
                {
                    "check_pass": False,
                    "failed_checks": [],
                    "failed_check_names": [],
                    "checks": [],
                    "check_error": f"{type(exc).__name__}: {exc}",
                }
            )
        rows.append(base)
    return pd.DataFrame(rows)

def get_check_submission(s: requests.Session, alpha_id: str):
    try:
        detail = get_check_submission_detailed(s, alpha_id)
        if detail["passed"]:
            return detail["self_correlation"]
        return "fail"
    except Exception:
        return "error"


def check_submission(alpha_bag, gold_bag=None, start=0):
    gold_bag = gold_bag if gold_bag is not None else []
    s = login()
    for idx, alpha_id in enumerate(alpha_bag):
        if idx < start:
            continue
        detail = get_check_submission_detailed(s, alpha_id)
        if detail["passed"]:
            print(alpha_id, "PASS", detail["self_correlation"])
            gold_bag.append((alpha_id, detail["self_correlation"]))
        else:
            names = [c.get("name") for c in detail["failed_checks"]]
            print(alpha_id, "FAIL", names)
    return gold_bag


def view_alphas(gold_bag):
    s = login()
    rows = []
    for alpha_id, pc in gold_bag:
        triple = locate_alpha(s, alpha_id)
        rows.append(
            [
                triple[0],
                triple[2],
                triple[3],
                triple[4],
                triple[5],
                triple[6],
                triple[1],
                pc,
            ]
        )
    rows.sort(reverse=True, key=lambda x: (x[1] is not None, x[1]),)
    for row in rows:
        print(row)


# ---------- Legacy simulation wrappers ----------

def load_task_pool_single(alpha_list, limit_of_single_simulations):
    return [
        alpha_list[i : i + limit_of_single_simulations]
        for i in range(0, len(alpha_list), limit_of_single_simulations)
    ]


def single_simulate(alpha_pool, neut, region, universe, start=0):
    """
    Legacy compatibility wrapper.
    New notebooks should use simulate_candidates() because it captures metrics + cache.
    """
    flattened = []
    for idx, task in enumerate(alpha_pool):
        if idx < start:
            continue
        flattened.extend(task)

    candidates = [
        {"expr": expr, "decay": decay, "stage": -1, "field": "LEGACY"}
        for expr, decay in flattened
    ]
    return simulate_candidates(
        candidates,
        neutralization=neut,
        region=region,
        universe=universe,
        concurrency=3,
    )


def generate_sim_data(alpha_list, region, uni, neut):
    out = []
    for alpha, decay in alpha_list:
        candidate = {"expr": alpha, "decay": decay}
        out.append(
            {
                "type": "REGULAR",
                "settings": build_settings(
                    candidate,
                    neutralization=neut,
                    region=region,
                    universe=uni,
                    test_period="P2Y",
                ),
                "regular": alpha,
            }
        )
    return out


def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    tasks = [
        alpha_list[i : i + limit_of_children_simulations]
        for i in range(0, len(alpha_list), limit_of_children_simulations)
    ]
    return [
        tasks[i : i + limit_of_multi_simulations]
        for i in range(0, len(tasks), limit_of_multi_simulations)
    ]


def multi_simulate(alpha_pools, neut, region, universe, start=0):
    """
    Retained for compatibility. V2 notebook intentionally uses the cache-aware
    single engine until multi-simulation child/result mapping is verified on your account.
    """
    flattened = []
    for p_idx, pool in enumerate(alpha_pools):
        if p_idx < start:
            continue
        for task in pool:
            flattened.extend(task)
    candidates = [
        {"expr": expr, "decay": decay, "stage": -1, "field": "LEGACY"}
        for expr, decay in flattened
    ]
    return simulate_candidates(
        candidates,
        neutralization=neut,
        region=region,
        universe=universe,
        concurrency=3,
    )


# ---------- Misc legacy helpers ----------

def set_alpha_properties(
    s,
    alpha_id,
    name=None,
    color=None,
    selection_desc="None",
    combo_desc="None",
    tags=None,
):
    tags = tags or ["ace_tag"]
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    return _request_with_retry(
        s,
        "PATCH",
        f"{BRAIN_API_URL}/alphas/{alpha_id}",
        json_body=params,
    )


def get_alphas(
    start_date,
    end_date,
    sharpe_th,
    fitness_th,
    region,
    alpha_num,
    usage,
):
    """
    Legacy date-query method kept only for compatibility.
    V2 workflow does not depend on this function.
    """
    s = login()
    output = []
    for i in range(0, alpha_num, 100):
        url = (
            f"{BRAIN_API_URL}/users/self/alphas?limit=100&offset={i}"
            f"&status=UNSUBMITTED%1FIS_FAIL"
            f"&dateCreated%3E=2026-{start_date}T00:00:00-04:00"
            f"&dateCreated%3C2026-{end_date}T00:00:00-04:00"
            f"&is.fitness%3E{fitness_th}&is.sharpe%3E{sharpe_th}"
            f"&settings.region={region}&order=-is.sharpe&hidden=false&type!=SUPER"
        )
        payload = _get_json(s, url, "results")
        for alpha in payload.get("results", []):
            is_m = alpha.get("is", {})
            long_count = is_m.get("longCount", 0) or 0
            short_count = is_m.get("shortCount", 0) or 0
            if long_count + short_count <= 100:
                continue
            rec = [
                alpha.get("id"),
                alpha.get("regular", {}).get("code"),
                is_m.get("sharpe"),
                is_m.get("turnover"),
                is_m.get("fitness"),
                is_m.get("margin"),
                alpha.get("dateCreated"),
                alpha.get("settings", {}).get("decay"),
            ]
            output.append(rec)
    return output


def vector_factory(op, field):
    return [f"{op}({field}, cap)"]


def ts_comp_factory(op, field, factor, paras):
    out = []
    for day, para in product([5, 22, 66, 240], paras):
        if isinstance(para, float):
            out.append(f"{op}({field}, {day}, {factor}={para:.1f})")
        else:
            out.append(f"{op}({field}, {day}, {factor}={int(para)})")
    return out


def twin_field_factory(op, field, fields):
    out = []
    for day in [5, 22, 66, 240]:
        for counterpart in set(fields) - {field}:
            out.append(f"{op}({field}, {counterpart}, {day})")
    return out
