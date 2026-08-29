"""Build local operator capability evidence from COMPLETE V2.1 cache rows."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
ADVANCED_OPERATORS = {
    "hump",
    "ts_decay_linear",
    "trade_when",
    "ts_target_tvr_delta_limit",
    "ts_target_tvr_decay",
}


def build_project_operator_evidence(
    alpha_db: Path,
    *,
    advanced_operators: Iterable[str] = ADVANCED_OPERATORS,
    example_limit: int = 5,
) -> List[Dict[str, Any]]:
    advanced = set(advanced_operators)
    counts = defaultdict(int)
    examples = defaultdict(list)
    connection = sqlite3.connect(f"file:{Path(alpha_db).resolve().as_posix()}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT sim_key, expr FROM alpha_results WHERE status='COMPLETE' AND expr IS NOT NULL"
        )
        for sim_key, expression in rows:
            for operator in set(CALL_RE.findall(str(expression))):
                counts[operator] += 1
                if len(examples[operator]) < example_limit:
                    examples[operator].append(sim_key)
    finally:
        connection.close()

    names = sorted(set(counts) | advanced)
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for name in names:
        count = counts.get(name, 0)
        operator_class = "ADVANCED_OPERATOR" if name in advanced else "CORE_VERIFIED_OPERATOR"
        status = "VERIFIED_PROJECT" if count else "UNVERIFIED"
        signature = "UNKNOWN"
        signature_hash = hashlib.sha256(f"{name}:{signature}".encode("utf-8")).hexdigest()
        records.append({
            "operator_name": name,
            "operator_class": operator_class,
            "status": status,
            "source": "PROJECT_COMPLETE_CACHE" if count else "NO_CURRENT_PROJECT_EVIDENCE",
            "complete_expression_count": count,
            "example_sim_keys": examples.get(name, []),
            "signature": signature,
            "signature_hash": signature_hash,
            "operator_metadata_hash": None,
            "validated_at": now if count else None,
            "last_seen_at": now if count else None,
            "validation_error": None,
            "evidence": {"basis": "distinct COMPLETE expressions containing operator"},
            "evidence_note": (
                "Verified only as project execution evidence; API signature remains unknown."
                if count else "No COMPLETE expression or Operators API evidence in Phase 2."
            ),
        })
    return records
