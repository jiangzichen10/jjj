"""Stable V3.1 Continuous Research entry point.

A1 deliberately reuses the audited V3 execution primitives while changing the
lifecycle semantics inside the orchestrator.  The public Continuous entry point
is separate so later queue/WAIT/POLL implementations can replace the internal
adapter without changing CLI or strategy contracts.

Important: this function invokes the orchestrator exactly once.  It is not an
"auto-resume while True execute_round()" shim.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .config import ConfigError
from .continuous_policy import parse_continuous_policy


def execute_continuous(
    store: Any,
    config: Any,
    machine: Any,
    session: Any,
    alpha_db: Path,
    machine_path: Path,
    external_evidence_path: Path,
    continuous_policy_path: Path,
    *,
    run_id: Optional[str] = None,
    allow_simulation_post: bool = False,
    resume: bool = False,
    offline_discovery: bool = False,
    max_batches: Optional[int] = None,
    extension_evidence_run: Optional[str] = None,
) -> Dict[str, Any]:
    # Local import avoids a circular dependency: round_orchestrator owns the
    # audited execution primitives and imports Continuous lifecycle helpers.
    from .round_orchestrator import execute_round, load_round_policy

    policy = load_round_policy(continuous_policy_path, config)
    parsed = parse_continuous_policy(policy)
    if not parsed.enabled:
        raise ConfigError("CONTINUOUS_ACTION_REQUIRES_CONTINUOUS_POLICY")
    if parsed.global_budget_enforced:
        raise ConfigError("CONTINUOUS_ACTION_REQUIRES_STATISTICS_ONLY_GLOBAL_BUDGET")

    return execute_round(
        store, config, machine, session, alpha_db, machine_path,
        external_evidence_path, continuous_policy_path,
        run_id=run_id,
        allow_simulation_post=allow_simulation_post,
        resume=resume,
        offline_discovery=offline_discovery,
        max_batches=max_batches,
        extension_evidence_run=extension_evidence_run,
    )
