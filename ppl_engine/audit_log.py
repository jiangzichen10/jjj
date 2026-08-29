"""V2.2 Production Logging / Execution Audit Trail.

This module is the single entry point for the Execution Audit Trail. It writes
JSON Lines (one JSON object per line) to ``logs/ppl_v2_2.log`` via a
``RotatingFileHandler``.

Design invariants (do not relax these):

1. **Audit trail is NOT workflow truth.** ``alpha_results.db`` is Simulation /
   Alpha Fact truth and ``ppl_runner.db`` is Workflow truth. The log is only a
   best-effort execution audit trail; no business decision may depend on it.

2. **Best-effort.** A logging failure (unwritable directory, rotate failure,
   encoding error) must never crash Simulation / Repair / Resume / DB state
   transitions. It degrades to a single stderr warning.

3. **No secrets.** Authorization / Cookie / Set-Cookie / API key / password /
   token / session / secret values are redacted before any line is written.
   URLs are stripped to ``scheme://host/path`` (query and fragment dropped).

4. **Opt-in per process.** ``audit_event`` is a silent no-op until
   ``configure_audit_log`` has been called (the CLI calls it at startup). This
   keeps library-level calls (e.g. tests) from scattering log files.
"""

from __future__ import annotations

import json
from collections import deque
import logging
import os
import re
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 10


@dataclass
class AuditLogConfig:
    enabled: bool = True
    directory: str = "logs"
    filename: str = "ppl_v2_2.log"
    max_bytes: int = DEFAULT_MAX_BYTES
    backup_count: int = DEFAULT_BACKUP_COUNT
    level: str = "INFO"
    extra: Dict[str, Any] = field(default_factory=dict)


_ENV_KEYS = {
    "AUDIT_LOG_ENABLED": "enabled",
    "AUDIT_LOG_DIR": "directory",
    "AUDIT_LOG_FILENAME": "filename",
    "AUDIT_LOG_MAX_BYTES": "max_bytes",
    "AUDIT_LOG_BACKUP_COUNT": "backup_count",
    "AUDIT_LOG_LEVEL": "level",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def resolve_audit_config(project_dir: Optional[Path] = None) -> AuditLogConfig:
    """Resolve audit configuration from environment (with safe defaults).

    Defaults live here, not in the YAML files, so enabling logging never
    touches the run execution hash material. Overrides come from ``AUDIT_LOG_*``
    environment variables.
    """
    root = Path(project_dir).resolve() if project_dir else Path.cwd().resolve()
    return AuditLogConfig(
        enabled=_env_bool("AUDIT_LOG_ENABLED", True),
        directory=os.environ.get("AUDIT_LOG_DIR", "logs"),
        filename=os.environ.get("AUDIT_LOG_FILENAME", "ppl_v2_2.log"),
        max_bytes=int(os.environ.get("AUDIT_LOG_MAX_BYTES", str(DEFAULT_MAX_BYTES))),
        backup_count=int(os.environ.get("AUDIT_LOG_BACKUP_COUNT", str(DEFAULT_BACKUP_COUNT))),
        level=os.environ.get("AUDIT_LOG_LEVEL", "INFO").upper(),
        extra={"project_dir": str(root)},
    )


# ---------------------------------------------------------------------------
# Sensitive data redaction
# ---------------------------------------------------------------------------

_SENSITIVE_WORDS = (
    "authorization", "auth", "cookie", "set-cookie", "setcookie", "api_key",
    "apikey", "password", "passwd", "token", "secret", "credential", "credentials",
    "bearer", "session",
)

# Workflow identifiers that happen to contain a sensitive word but are NOT
# credentials and are safe to log verbatim.
_SAFE_KEYS = {
    "session_status", "check_session_id", "check_session_status",
    "session_state", "session_id_count",
}


def _norm_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_").replace(" ", "_")


def _is_sensitive_key(key: Any) -> bool:
    k = _norm_key(key)
    if k in _SAFE_KEYS:
        return False
    if k in {"authorization", "cookie", "set_cookie", "api_key", "apikey",
             "password", "passwd", "token", "secret", "credentials", "credential",
             "bearer", "session", "session_id", "session_token", "session_key",
             "session_secret", "access_token", "refresh_token", "auth_token",
             "id_token", "csrf_token"}:
        return True
    for word in _SENSITIVE_WORDS:
        if word in k:
            # "session" alone is too broad (would hit session_status), so only
            # treat it as sensitive when paired with token/key/secret.
            if word == "session":
                if any(s in k for s in ("token", "key", "secret", "id")):
                    return True
                continue
            if word == "auth":
                continue  # handled by exact match above
            return True
    return False


_CREDENTIAL_PATTERN = re.compile(
    r"(?i)(bearer\s+\S+|basic\s+\S+|"
    r"password\s*[=:]\s*\S+|passwd\s*[=:]\s*\S+|"
    r"token\s*[=:]\s*\S+|secret\s*[=:]\s*\S+|"
    r"api[_-]?key\s*[=:]\s*\S+|"
    r"authorization\s*[=:]\s*\S+)"
)


def _sanitize_string(value: str) -> str:
    if _CREDENTIAL_PATTERN.search(value):
        return "[REDACTED]"
    return value


def sanitize(obj: Any) -> Any:
    """Recursively redact sensitive keys and credential-bearing strings."""
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if _is_sensitive_key(k):
                out[str(k)] = "[REDACTED]"
            else:
                out[str(k)] = sanitize(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [sanitize(x) for x in obj]
    if isinstance(obj, str):
        return _sanitize_string(obj)
    return obj


def sanitize_url(url: Any) -> Any:
    """Return scheme://host/path, dropping query and fragment (token-safe)."""
    if url is None:
        return None
    if not isinstance(url, str):
        return url
    try:
        parts = urlsplit(url)
        if parts.scheme or parts.netloc:
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        return parts.path
    except (ValueError, TypeError):
        return "[REDACTED]"


def redact_headers(headers: Any) -> Any:
    """Redact sensitive header values; safe headers pass through untouched."""
    if headers is None:
        return None
    if isinstance(headers, dict):
        return {k: ("[REDACTED]" if _is_sensitive_key(k) else v) for k, v in headers.items()}
    if isinstance(headers, (list, tuple)):
        out = []
        for item in headers:
            if isinstance(item, dict):
                out.append(redact_headers(item))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((item[0], "[REDACTED]" if _is_sensitive_key(item[0]) else item[1]))
            else:
                out.append(item)
        return out
    return headers


def truncate_text(value: Any, limit: int = 1000) -> Any:
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + f"...[truncated:{len(value)}]"
    return value


# ---------------------------------------------------------------------------
# Logger lifecycle (singleton, idempotent)
# ---------------------------------------------------------------------------

_LOGGER_NAME = "ppl.audit"
_LOGGER: Optional[logging.Logger] = None
_CONFIGURED = False
_CONFIGURED_PATH: Optional[str] = None
_LOCK = threading.Lock()
_WARNED = False

_LEVELS = {
    "DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
    "WARN": logging.WARNING, "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL,
}


class _JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


def get_audit_logger() -> logging.Logger:
    """Return the (lazy) module logger. Does not add file handlers by itself."""
    global _LOGGER
    if _LOGGER is None:
        logger = logging.getLogger(_LOGGER_NAME)
        logger.propagate = False
        logger.setLevel(logging.INFO)
        # A NullHandler keeps the logger silent until configured; it is never
        # duplicated because we add file handlers in configure_audit_log only.
        if not any(isinstance(h, logging.NullHandler) for h in logger.handlers):
            logger.addHandler(logging.NullHandler())
        _LOGGER = logger
    return _LOGGER


def _reset_handlers(logger: logging.Logger) -> None:
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass


def configure_audit_log(
    project_dir: Optional[Path] = None,
    config: Optional[AuditLogConfig] = None,
) -> logging.Logger:
    """Configure the file handler. Idempotent per target path.

    Returns the configured logger. Raises on hard configuration errors so tests
    can detect them, but the *write* path (``audit_event``) remains best-effort.
    """
    global _CONFIGURED, _CONFIGURED_PATH
    cfg = config or resolve_audit_config(project_dir)
    logger = get_audit_logger()

    with _LOCK:
        if not cfg.enabled:
            _reset_handlers(logger)
            logger.addHandler(logging.NullHandler())
            _CONFIGURED = False
            _CONFIGURED_PATH = None
            return logger

        root = Path(project_dir).resolve() if project_dir else Path.cwd().resolve()
        log_dir = Path(cfg.directory)
        if not log_dir.is_absolute():
            log_dir = root / log_dir
        log_path = log_dir / cfg.filename
        log_dir.mkdir(parents=True, exist_ok=True)

        target = str(log_path.resolve())
        if _CONFIGURED and _CONFIGURED_PATH == target:
            return logger  # already configured for this exact file

        handler = RotatingFileHandler(
            str(log_path), maxBytes=int(cfg.max_bytes),
            backupCount=int(cfg.backup_count), encoding="utf-8",
        )
        handler.setFormatter(_JsonLineFormatter())
        handler.setLevel(_LEVELS.get(cfg.level.upper(), logging.INFO))

        _reset_handlers(logger)
        logger.setLevel(_LEVELS.get(cfg.level.upper(), logging.INFO))
        logger.addHandler(handler)
        _CONFIGURED = True
        _CONFIGURED_PATH = target
        return logger


def is_configured() -> bool:
    return _CONFIGURED


def _warn_once(message: str) -> None:
    global _WARNED
    if _WARNED:
        return
    _WARNED = True
    try:
        print(f"[audit-log] {message}", file=sys.stderr)
    except Exception:
        pass


def _local_now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _level_int(level: str) -> int:
    return _LEVELS.get(str(level).upper(), logging.INFO)


# ---------------------------------------------------------------------------
# Event emission
# ---------------------------------------------------------------------------

def audit_event(action: str = "EVENT", level: str = "INFO", **fields: Any) -> None:
    """Write a single JSONL audit record. No-op until configured; best-effort."""
    if not _CONFIGURED:
        return
    try:
        record: Dict[str, Any] = {
            "timestamp": _local_now_iso(),
            "level": str(level).upper(),
            "action": action,
        }
        record.update(fields)
        record = {k: v for k, v in sanitize(record).items() if v is not None}
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)
        get_audit_logger().log(_level_int(level), line)
    except Exception as exc:  # best-effort: never crash the caller
        _warn_once(f"audit event write failed: {exc}")


def audit_state_transition(
    entity_type: str, entity_id: Any, *, run_id: Optional[str] = None,
    old_state: Optional[str] = None, new_state: Optional[str] = None,
    reason: Optional[str] = None, source: Optional[str] = None, **extra: Any,
) -> None:
    audit_event(
        action="STATE_TRANSITION", run_id=run_id,
        entity_type=entity_type, entity_id=str(entity_id) if entity_id is not None else None,
        old_state=old_state, new_state=new_state, reason=reason, source=source, **extra,
    )


def audit_http(
    action: str = "HTTP", *, method: Optional[str] = None,
    endpoint_type: Optional[str] = None, status: Optional[int] = None,
    retry_count: Optional[int] = None, elapsed_ms: Optional[int] = None,
    url: Optional[str] = None, headers: Any = None, run_id: Optional[str] = None,
    **extra: Any,
) -> None:
    payload: Dict[str, Any] = {
        "http_method": method, "endpoint_type": endpoint_type,
        "http_status": status, "retry_count": retry_count, "elapsed_ms": elapsed_ms,
        "run_id": run_id,
    }
    if url is not None:
        payload["url"] = sanitize_url(url)
    if headers is not None:
        payload["headers"] = redact_headers(headers)
    payload.update(extra)
    audit_event(action=action, **payload)


def audit_error(
    error_type: Optional[str] = None, error_message: Optional[str] = None, **extra: Any,
) -> None:
    audit_event(
        action="ERROR", level="ERROR", error_type=error_type,
        error_message=truncate_text(error_message, 1000), **extra,
    )


# ---------------------------------------------------------------------------
# Read / query (streaming, for CLI)
# ---------------------------------------------------------------------------

def read_audit_log(
    path: Path, *, run_id: Optional[str] = None, action: Optional[str] = None,
    candidate_id: Optional[str] = None, alpha_id: Optional[str] = None,
    repair_plan_id: Optional[str] = None, level: Optional[str] = None,
    limit: int = 50,
) -> Iterator[Dict[str, Any]]:
    """Stream JSONL and yield matching records (newest-ish first via reverse scan).

    Reads line by line; never loads the whole file into memory.
    """
    p = Path(path)
    if not p.exists():
        return
    if limit <= 0:
        return
    level_upper = level.upper() if level else None
    # Keep only the latest matching records while streaming the file. This is
    # bounded by ``limit`` and therefore does not load a large audit log into
    # memory. The previous implementation stopped after the first N matches and
    # accidentally returned the oldest records in reverse order.
    collected = deque(maxlen=limit)
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except (ValueError, TypeError):
                continue
            if not isinstance(rec, dict):
                continue
            if run_id and rec.get("run_id") != run_id:
                continue
            if action and rec.get("action") != action:
                continue
            if candidate_id and rec.get("candidate_id") != candidate_id:
                continue
            if alpha_id and rec.get("alpha_id") != alpha_id:
                continue
            if repair_plan_id and rec.get("repair_plan_id") != repair_plan_id:
                continue
            if level_upper and rec.get("level") != level_upper:
                continue
            collected.append(rec)
    for rec in reversed(collected):
        yield rec


def audit_log_path(project_dir: Optional[Path] = None, config: Optional[AuditLogConfig] = None) -> Path:
    cfg = config or resolve_audit_config(project_dir)
    root = Path(project_dir).resolve() if project_dir else Path.cwd().resolve()
    log_dir = Path(cfg.directory)
    if not log_dir.is_absolute():
        log_dir = root / log_dir
    return log_dir / cfg.filename
