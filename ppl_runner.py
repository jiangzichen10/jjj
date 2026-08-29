"""WorldQuant BRAIN v3 CLI. V2.2 commands remain compatible; V3 adds reusable round orchestration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from ppl_engine.config import (
    ConfigError, config_with_machine_hash_policy_override, load_effective_config,
)
from ppl_engine.atomic import atomic_write_json
from ppl_engine.discovery import DiscoveryResult, ReadOnlySession, discover_online
from ppl_engine.dry_run import estimate_candidate_plan
from ppl_engine.operator_registry import build_project_operator_evidence
from ppl_engine.runner_lock import SingleRunnerLock
from ppl_engine.state_machine import write_initial_state
from ppl_engine.store import RunnerStore
from ppl_engine.summary_writer import build_analytics, write_foundation_summary
from ppl_engine.candidate_factory import generate_candidate_preview
from ppl_engine.reconcile import (
    apply_offline_reconcile,
    plan_offline_reconcile,
    write_reconciled_outputs,
)
from ppl_engine.check_parser import build_check_summary, parse_response_text
from ppl_engine.diagnosis import diagnose_evidence
from ppl_engine.repair_engine import plan_repairs
from ppl_engine.description import draft_description, manual_checklist
from ppl_engine.properties import parse_alpha_properties, refresh_preview
from ppl_engine.live_validation import run_live_validation
from ppl_engine.live_execution import (
    MACHINE_HASH_OPERATION_LIVE_VALIDATION,
    create_validation_run,
    execute_phase10a,
    preview_phase10a,
    validate_machine_lib_hash,
)
from ppl_engine.phase10b import (
    create_phase10b_run,
    execute_phase10b,
    execute_repair_batch,
    prepare_repair_batch,
    preview_phase10b,
)
from ppl_engine.production_repair import (
    execute_production_repair,
    list_repair_plans,
    preview_production_repair,
)
from ppl_engine.check_derived_repair import derive_check_repair_proposals


PROJECT_DIR = Path(__file__).resolve().parent


def _login_with_authentication_meter(machine_lib):
    """Login while counting only authentication POSTs; reject any other POST."""
    original_request = machine_lib.requests.sessions.Session.request
    counter = {"authentication_post_count": 0}

    def metered_request(session, method, url, *args, **kwargs):
        if str(method or "").upper() == "POST":
            path = urlsplit(str(url)).path.rstrip("/")
            if path not in {"/authentication", "/authentication/biometrics"}:
                raise ConfigError("MANUAL_REFRESH_LOGIN_NON_AUTHENTICATION_POST_BLOCKED")
            counter["authentication_post_count"] += 1
        return original_request(session, method, url, *args, **kwargs)

    machine_lib.requests.sessions.Session.request = metered_request
    try:
        session = machine_lib.login()
    finally:
        machine_lib.requests.sessions.Session.request = original_request
    return session, int(counter["authentication_post_count"])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WorldQuant BRAIN v3 runner (V2.2-compatible + resumable round orchestration)")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--initialize", action="store_true", help="create the independent runner DB")
    action.add_argument("--status", action="store_true", help="show local runner status")
    action.add_argument("--summary", action="store_true", help="write a local JSON summary")
    action.add_argument("--dry-run", action="store_true", help="read-only discovery and candidate-plan estimate")
    action.add_argument("--candidate-preview", action="store_true", help="offline formal candidate and cache preview")
    action.add_argument("--reconcile", action="store_true", help="offline workflow/cache reconciliation")
    action.add_argument("--check-preview", action="store_true", help="offline parse of a check fixture")
    action.add_argument("--diagnose-preview", action="store_true", help="offline diagnosis of a fixture")
    action.add_argument("--family-preview", action="store_true", help="offline family analytics preview")
    action.add_argument("--priority-preview", action="store_true", help="offline priority and diversity preview")
    action.add_argument("--description-preview", action="store_true", help="offline deterministic description preview")
    action.add_argument("--refresh-preview", action="store_true", help="offline property refresh preview")
    action.add_argument("--export-description", action="store_true", help="offline description/checklist export from fixture")
    action.add_argument("--live-validate", action="store_true", help="strict Phase-9 read-only live validation")
    action.add_argument("--prepare-live-validation", action="store_true", help="create an isolated Phase-10A canary run")
    action.add_argument("--prepare-phase10b", action="store_true", help="create an isolated Phase-10B validation run")
    action.add_argument("--execute-live-validation", action="store_true", help="preview or explicitly execute a Phase-10A canary run")
    action.add_argument("--execute-phase10b-repair", action="store_true", help="preview or explicitly execute naturally READY Phase-10B repair plans")
    action.add_argument("--production-repair", action="store_true", help="preview or explicitly execute selected Production Repair plans")
    action.add_argument("--list-repair-plans", action="store_true", help="list deferred and check-derived repair plans (read-only)")
    action.add_argument("--plan-check-derived", action="store_true", help="generate Check-derived Repair Proposals (planning only, no POST)")
    action.add_argument("--list-near-pass", action="store_true", help="list Near-Pass rescue candidates (read-only)")
    action.add_argument("--list-manual-review", action="store_true", help="list Manual Review P1/P2/P3 escalation (read-only)")
    action.add_argument("--preview-rescue", action="store_true", help="preview a single candidate's rescue classification + recommendation (read-only)")
    action.add_argument("--show-audit-log", action="store_true", help="stream and filter the execution audit log (read-only)")
    action.add_argument("--start-round", action="store_true", help="create and optionally execute a new V3 Production Research round")
    action.add_argument("--resume-round", action="store_true", help="resume an existing V3 round from durable state")
    action.add_argument("--continuous", action="store_true", help="start or resume a V3.1 Continuous Research run; defaults to v3.1 plan/policy")
    action.add_argument("--reopen-round-after-bugfix", action="store_true", help="locally reopen only a COMPLETED ROUND_NO_SAFE_CANDIDATE round after a confirmed selector/orchestration bugfix; no network/POST")
    action.add_argument("--retry-uncertain-repair", action="store_true", help="locally authorize one retry of REPAIR UNCERTAIN_SUBMISSION rows; no network/POST")
    action.add_argument("--round-status", action="store_true", help="show V3 round status (read-only)")
    action.add_argument("--scheduler-evidence-report", action="store_true", help="read-only D2 Scheduler evidence calibration report; never changes thresholds or execution")
    action.add_argument("--rebuild-round-reports", action="store_true", help="rebuild V3 telemetry/reports from durable local facts only; no network/POST")
    action.add_argument("--backfill-ppc-repair-outcomes", action="store_true", help="preview or locally backfill missing durable PP_CORRELATION_FAIL repair outcomes; no network/POST")
    action.add_argument("--protect-alpha", action="store_true", help="locally protect a user-confirmed submitted alpha family from future repair selection")
    action.add_argument("--refresh-manual-finalization", action="store_true", help="GET-only refresh of every row currently in manual_finalization_queue.csv, then rebuild classification/reports")
    action.add_argument("--recover-interrupted-batch", action="store_true", help="locally release a human-confirmed never-dispatched tail from one interrupted or SERVER-SLOT-deferred V3 SEARCH batch; no network/POST")
    action.add_argument("--recover-interrupted-repair-batch", action="store_true", help="locally release a fully-proven never-dispatched V3 REPAIR batch intent; no network/POST")
    action.add_argument("--repair-interrupted-batch-ledger", action="store_true", help="locally repair research-ledger batch attribution for durable candidates of one interrupted SEARCH batch; no network/POST")
    action.add_argument("--cancel-simulation", action="store_true", help="resolve one existing remote Simulation; DELETE requires explicit confirmation")
    parser.add_argument("--rules", type=Path, default=PROJECT_DIR / "ppl_rules.yaml")
    parser.add_argument("--plan", type=Path, default=PROJECT_DIR / "ppl_plan.yaml")
    parser.add_argument("--round-plan", type=Path, default=PROJECT_DIR / "ppl_plan_v3.yaml", help="V3 round execution plan")
    parser.add_argument("--round-policy", type=Path, default=PROJECT_DIR / "ppl_round_v3.yaml", help="V3 round orchestration policy")
    parser.add_argument("--continuous-plan", type=Path, default=PROJECT_DIR / "ppl_plan_v31.yaml", help="V3.1 Continuous execution plan")
    parser.add_argument("--continuous-policy", type=Path, default=PROJECT_DIR / "ppl_round_v31.yaml", help="V3.1 Continuous orchestration policy")
    parser.add_argument("--db", type=Path, default=PROJECT_DIR / "ppl_runner.db")
    parser.add_argument("--output", type=Path, default=PROJECT_DIR / "ppl_summary.json")
    parser.add_argument("--dry-run-output", type=Path, default=PROJECT_DIR / "ppl_dry_run.json")
    parser.add_argument("--candidate-preview-output", type=Path, default=PROJECT_DIR / "ppl_candidate_preview.json")
    parser.add_argument("--execution-plan-output", type=Path, default=PROJECT_DIR / "ppl_execution_plan.json")
    parser.add_argument("--state", type=Path, default=PROJECT_DIR / "ppl_state.json")
    parser.add_argument("--lock", type=Path, default=PROJECT_DIR / "ppl_runner.lock")
    parser.add_argument("--run-id")
    parser.add_argument("--alpha-id", help="alpha id for --protect-alpha")
    parser.add_argument("--phase", choices=("10A", "10B"), help="live validation phase")
    parser.add_argument("--plan-id", action="append", default=[], help="explicit repair plan id (repeatable for a small explicit list)")
    parser.add_argument("--plan-source", choices=("all", "deferred", "check-derived"), default="all", help="filter for --list-repair-plans")
    parser.add_argument("--candidate-id", help="candidate id for --preview-rescue")
    parser.add_argument("--simulation-id", help="remote simulation id for --cancel-simulation")
    parser.add_argument("--audit-limit", type=int, default=50, help="max records for --show-audit-log")
    parser.add_argument("--audit-action", help="filter --show-audit-log by action")
    parser.add_argument("--audit-candidate-id", help="filter --show-audit-log by candidate_id")
    parser.add_argument("--audit-alpha-id", help="filter --show-audit-log by alpha_id")
    parser.add_argument("--audit-plan-id", help="filter --show-audit-log by repair_plan_id")
    parser.add_argument("--audit-level", help="filter --show-audit-log by level (e.g. ERROR)")
    parser.add_argument("--offline", action="store_true", help="use the latest matching discovery snapshot")
    parser.add_argument("--preview", action="store_true", help="calculate reconcile changes without writing")
    parser.add_argument("--fixture", type=Path, help="offline check fixture for --check-preview")
    parser.add_argument("--repair-preview", action="store_true", help="include bounded offline repair planning")
    parser.add_argument("--rebuild-derived", action="store_true", help="rebuild family/priority derived tables with --summary")
    parser.add_argument("--live", action="store_true", help="explicitly authorize Phase-9 read-only network access")
    parser.add_argument("--allow-simulation-post", action="store_true", help="explicitly authorize Simulation POST for supported execution actions")
    parser.add_argument("--max-batches", type=int, help="optional V3 invocation batch limit; durable round can resume later")
    parser.add_argument("--batch-no", type=int, help="batch number for undispatched-tail recovery / interrupted-batch ledger repair actions")
    parser.add_argument("--confirm-bugfix-reopen", action="store_true", help="explicitly confirm --reopen-round-after-bugfix local state recovery")
    parser.add_argument("--confirm-duplicate-risk", action="store_true", help="explicitly accept possible duplicate remote Simulation risk for --retry-uncertain-repair")
    parser.add_argument("--confirm-undispatched-tail", action="store_true", help="explicitly confirm the missing planned tail of --recover-interrupted-batch was never POST-dispatched (including SERVER SLOT GUARD deferred tails)")
    parser.add_argument("--confirm-undispatched-repair-tail", action="store_true", help="explicitly confirm every planned key of --recover-interrupted-repair-batch must prove NEVER_DISPATCHED")
    parser.add_argument("--confirm-ledger-reattribution", action="store_true", help="explicitly confirm --repair-interrupted-batch-ledger may rewrite local telemetry batch/origin attribution only")
    parser.add_argument("--confirm-ppc-outcome-backfill", action="store_true", help="explicitly confirm local durable outcome writes for --backfill-ppc-repair-outcomes")
    parser.add_argument("--confirm-cancel-simulation", action="store_true", help="explicitly confirm the remote DELETE resolution protocol for --cancel-simulation")
    parser.add_argument("--offline-discovery", action="store_true", help="V3 start-round: reuse latest matching discovery snapshot instead of live GET discovery")
    parser.add_argument("--extension-evidence-run", help="new V3 round only: read compatible durable evidence from this source run for targeted operator extensions")
    parser.add_argument(
        "--machine-hash-policy", type=lambda value: str(value).strip().upper(),
        choices=("STRICT", "WARN", "OFF"),
        help="process-local machine source hash policy; forced-strict operations ignore this override",
    )
    return parser


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.extension_evidence_run and not (args.start_round or args.continuous):
            raise ConfigError("--extension-evidence-run is only valid with --start-round or a new --continuous run")
        selected_plan = args.continuous_plan if (args.continuous or args.scheduler_evidence_report) else args.round_plan if (
    args.start_round
    or args.resume_round
    or args.reopen_round_after_bugfix
    or args.retry_uncertain_repair
    or args.round_status
    or args.rebuild_round_reports
    or args.backfill_ppc_repair_outcomes
    or args.protect_alpha
    or args.refresh_manual_finalization
    or args.recover_interrupted_batch
    or args.recover_interrupted_repair_batch
    or args.repair_interrupted_batch_ledger
    or args.cancel_simulation
    or args.reconcile
) else args.plan
        config = load_effective_config(args.rules, selected_plan, project_dir=PROJECT_DIR)
        config = config_with_machine_hash_policy_override(config, args.machine_hash_policy)
        store = RunnerStore(args.db)
        from ppl_engine.audit_log import audit_event, configure_audit_log, is_configured
        # --show-audit-log must be observationally read-only: querying the audit
        # trail must not append LOGGING_INITIALIZED/RUN_START to that same trail.
        if not (args.show_audit_log or args.round_status or args.scheduler_evidence_report):
            configure_audit_log(PROJECT_DIR)
            if is_configured():
                audit_event(action="LOGGING_INITIALIZED", run_id=args.run_id, source="PRODUCTION_LOGGING_ENABLEMENT")
                audit_event(action="RUN_START", run_id=args.run_id, source="CLI", component="ppl_runner")
        if args.allow_simulation_post and not (args.execute_live_validation or args.execute_phase10b_repair or args.production_repair or args.start_round or args.resume_round or args.continuous):
            raise ConfigError("SIMULATION_POST_FLAG_REQUIRES_EXECUTION_ACTION")
        if args.confirm_bugfix_reopen and not args.reopen_round_after_bugfix:
            raise ConfigError("--confirm-bugfix-reopen requires --reopen-round-after-bugfix")
        if args.confirm_duplicate_risk and not args.retry_uncertain_repair:
            raise ConfigError("--confirm-duplicate-risk requires --retry-uncertain-repair")
        if args.confirm_cancel_simulation and not args.cancel_simulation:
            raise ConfigError("--confirm-cancel-simulation requires --cancel-simulation")
        if args.confirm_ppc_outcome_backfill and not args.backfill_ppc_repair_outcomes:
            raise ConfigError("--confirm-ppc-outcome-backfill requires --backfill-ppc-repair-outcomes")
        if args.confirm_undispatched_repair_tail and not args.recover_interrupted_repair_batch:
            raise ConfigError("--confirm-undispatched-repair-tail requires --recover-interrupted-repair-batch")
        if args.live and not args.live_validate: raise ConfigError("--live is only valid with --live-validate")
        if args.retry_uncertain_repair:
            from ppl_engine.round_orchestrator import authorize_uncertain_repair_retry
            if not args.run_id:
                raise ConfigError("--retry-uncertain-repair requires --run-id")
            if not args.confirm_duplicate_risk:
                raise ConfigError("--retry-uncertain-repair requires --confirm-duplicate-risk")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = authorize_uncertain_repair_retry(
                    store, PROJECT_DIR / "alpha_results.db", run_id=args.run_id,
                    batch_no=args.batch_no, confirm_duplicate_risk=True,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.reopen_round_after_bugfix:
            from ppl_engine.round_orchestrator import reopen_round_after_no_safe_candidate_bugfix
            if not args.run_id:
                raise ConfigError("--reopen-round-after-bugfix requires --run-id")
            if not args.confirm_bugfix_reopen:
                raise ConfigError("--reopen-round-after-bugfix requires --confirm-bugfix-reopen")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = reopen_round_after_no_safe_candidate_bugfix(
                    store, run_id=args.run_id, confirm_bugfix_reopen=True,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.protect_alpha:
            from ppl_engine.round_orchestrator import protect_submitted_alpha
            if not args.run_id:
                raise ConfigError("--protect-alpha requires --run-id")
            if not args.alpha_id:
                raise ConfigError("--protect-alpha requires --alpha-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = protect_submitted_alpha(store, run_id=args.run_id, alpha_id=args.alpha_id)
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.refresh_manual_finalization:
            from ppl_engine.round_orchestrator import (
                preflight_manual_finalization_refresh, refresh_manual_finalization_queue,
            )
            if not args.run_id:
                raise ConfigError("--refresh-manual-finalization requires --run-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            import machine_lib_V2_1 as machine_lib
            with SingleRunnerLock(args.lock):
                local_preflight = preflight_manual_finalization_refresh(
                    store, config, PROJECT_DIR / "machine_lib_V2_1.py", args.round_policy,
                    run_id=args.run_id,
                )
                session, authentication_post_count = _login_with_authentication_meter(machine_lib)
                try:
                    report = refresh_manual_finalization_queue(
                        store, config, machine_lib, session, PROJECT_DIR / "alpha_results.db",
                        PROJECT_DIR / "machine_lib_V2_1.py", args.round_policy, PROJECT_DIR,
                        run_id=args.run_id, preflight=local_preflight,
                        authentication_post_count=authentication_post_count,
                    )
                finally:
                    session.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.recover_interrupted_batch:
            from ppl_engine.round_orchestrator import recover_interrupted_batch_undispatched_tail
            if not args.run_id or args.batch_no is None:
                raise ConfigError("--recover-interrupted-batch requires --run-id and --batch-no")
            if not args.confirm_undispatched_tail:
                raise ConfigError("--recover-interrupted-batch requires --confirm-undispatched-tail")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = recover_interrupted_batch_undispatched_tail(
                    store, config, PROJECT_DIR / "alpha_results.db",
                    run_id=args.run_id, batch_no=args.batch_no,
                    confirm_undispatched_tail=True,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.recover_interrupted_repair_batch:
            from ppl_engine.round_orchestrator import recover_interrupted_repair_batch
            if not args.run_id or args.batch_no is None:
                raise ConfigError("--recover-interrupted-repair-batch requires --run-id and --batch-no")
            if not args.confirm_undispatched_repair_tail:
                raise ConfigError("--recover-interrupted-repair-batch requires --confirm-undispatched-repair-tail")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = recover_interrupted_repair_batch(
                    store, config, PROJECT_DIR / "alpha_results.db", PROJECT_DIR,
                    run_id=args.run_id, batch_no=args.batch_no,
                    confirm_undispatched_repair_tail=True,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.repair_interrupted_batch_ledger:
            from ppl_engine.round_orchestrator import repair_interrupted_batch_ledger_attribution
            if not args.run_id or args.batch_no is None:
                raise ConfigError("--repair-interrupted-batch-ledger requires --run-id and --batch-no")
            if not args.confirm_ledger_reattribution:
                raise ConfigError("--repair-interrupted-batch-ledger requires --confirm-ledger-reattribution")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = repair_interrupted_batch_ledger_attribution(
                    store, config, PROJECT_DIR / "alpha_results.db",
                    run_id=args.run_id, batch_no=args.batch_no,
                    confirm_ledger_reattribution=True,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.cancel_simulation:
            from ppl_engine.round_orchestrator import cancel_remote_simulation
            if not args.run_id or not args.simulation_id:
                raise ConfigError("--cancel-simulation requires --run-id and --simulation-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            if not args.confirm_cancel_simulation:
                report = cancel_remote_simulation(
                    store, config, None, None, PROJECT_DIR / "alpha_results.db",
                    run_id=args.run_id, simulation_id=args.simulation_id, confirmed=False,
                )
            else:
                import machine_lib_V2_1 as machine_lib
                session = machine_lib.login()
                try:
                    with SingleRunnerLock(args.lock):
                        report = cancel_remote_simulation(
                            store, config, machine_lib, session, PROJECT_DIR / "alpha_results.db",
                            run_id=args.run_id, simulation_id=args.simulation_id, confirmed=True,
                        )
                finally:
                    session.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.backfill_ppc_repair_outcomes:
            from ppl_engine.ppc_controlled_branch import backfill_ppc_repair_outcomes
            if not args.run_id:
                raise ConfigError("--backfill-ppc-repair-outcomes requires --run-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            if args.confirm_ppc_outcome_backfill:
                with SingleRunnerLock(args.lock):
                    report = backfill_ppc_repair_outcomes(
                        store, config, PROJECT_DIR / "alpha_results.db", args.run_id, confirm=True,
                    )
            else:
                report = backfill_ppc_repair_outcomes(
                    store, config, PROJECT_DIR / "alpha_results.db", args.run_id, confirm=False,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.scheduler_evidence_report:
            if not args.run_id:
                raise ConfigError("--scheduler-evidence-report requires --run-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            from ppl_engine.scheduler_evidence_report import build_scheduler_evidence_report
            report = build_scheduler_evidence_report(store.path, run_id=args.run_id)
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.round_status:
            from ppl_engine.round_orchestrator import round_status
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            report = round_status(store, config, PROJECT_DIR / "alpha_results.db", run_id=args.run_id)
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.rebuild_round_reports:
            from ppl_engine.round_orchestrator import rebuild_round_reports
            if not args.run_id:
                raise ConfigError("--rebuild-round-reports requires --run-id")
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            with SingleRunnerLock(args.lock):
                report = rebuild_round_reports(
                    store, config, PROJECT_DIR / "alpha_results.db", args.round_policy, PROJECT_DIR,
                    run_id=args.run_id,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.start_round or args.resume_round or args.continuous:
            from ppl_engine.continuous_engine import execute_continuous
            from ppl_engine.continuous_policy import parse_continuous_policy
            from ppl_engine.round_orchestrator import execute_round, get_round, load_round_policy
            store.initialize()
            round_policy_path = args.continuous_policy if args.continuous else args.round_policy
            if args.continuous:
                loaded_continuous_policy = load_round_policy(round_policy_path, config)
                if not parse_continuous_policy(loaded_continuous_policy).enabled:
                    raise ConfigError("CONTINUOUS_ACTION_REQUIRES_CONTINUOUS_POLICY")
            resume_mode = bool(args.resume_round)
            if args.continuous and args.run_id:
                existing_round = get_round(store, run_id=args.run_id)
                if existing_round:
                    try:
                        stored_policy = json.loads(str(existing_round.get("config_json") or "{}"))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        stored_policy = {}
                    if not parse_continuous_policy(stored_policy).enabled:
                        raise ConfigError("CONTINUOUS_CANNOT_RESUME_LEGACY_RUN")
                    resume_mode = True
            import machine_lib_V2_1 as machine_lib
            session = None
            starting_new = not resume_mode
            if args.allow_simulation_post or (starting_new and not args.offline_discovery):
                session = machine_lib.login()
                # Local-only fail-fast check before any new remote Simulation POST.
                ReadOnlySession(session).assert_auth_refresh_compatible()
            try:
                with SingleRunnerLock(args.lock):
                    executor = execute_continuous if args.continuous else execute_round
                    report = executor(
                        store, config, machine_lib, session, PROJECT_DIR / "alpha_results.db",
                        PROJECT_DIR / "machine_lib_V2_1.py", PROJECT_DIR / "rescue_evidence.json",
                        round_policy_path, run_id=args.run_id, allow_simulation_post=args.allow_simulation_post,
                        resume=resume_mode, offline_discovery=args.offline_discovery, max_batches=args.max_batches,
                        extension_evidence_run=(args.extension_evidence_run if starting_new else None),
                    )
            finally:
                if session is not None:
                    session.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.live_validate:
            if not args.live: raise ConfigError("LIVE_VALIDATION_REQUIRES_EXPLICIT_LIVE")
            store.initialize()
            import machine_lib_V2_1 as machine_lib
            validate_machine_lib_hash(
                PROJECT_DIR / "machine_lib_V2_1.py",
                operation=MACHINE_HASH_OPERATION_LIVE_VALIDATION,
                config=config, run_id=args.run_id,
            )
            base=machine_lib.login()
            try:
                report=run_live_validation(machine_lib,base,store,config,PROJECT_DIR/"ppl_live_validation.json",PROJECT_DIR/"tests"/"fixtures"/"live_sanitized",["78z2XJWZ","WjAxWeoG","ZYEMVYpx"])
            finally: base.close()
            print(json.dumps(report,ensure_ascii=False,indent=2));return 0
        if args.prepare_live_validation:
            if args.allow_simulation_post:
                raise ConfigError("PREPARATION_NEVER_ALLOWS_SIMULATION_POST")
            with SingleRunnerLock(args.lock):
                report = create_validation_run(store, config, PROJECT_DIR / "alpha_results.db", run_id=args.run_id)
                report["preview"] = preview_phase10a(store, config, PROJECT_DIR / "alpha_results.db", report["validation_run_id"])
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.prepare_phase10b:
            if args.allow_simulation_post:
                raise ConfigError("PREPARATION_NEVER_ALLOWS_SIMULATION_POST")
            with SingleRunnerLock(args.lock):
                report = create_phase10b_run(store, config, PROJECT_DIR / "alpha_results.db", run_id=args.run_id)
                report["preview"] = preview_phase10b(store, config, PROJECT_DIR / "alpha_results.db", report["validation_run_id"])
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.execute_live_validation:
            if args.phase not in {"10A", "10B"} or not args.run_id:
                raise ConfigError("--execute-live-validation requires --phase and --run-id")
            with SingleRunnerLock(args.lock):
                if not args.allow_simulation_post:
                    if args.phase == "10A":
                        report = execute_phase10a(store, config, None, None, PROJECT_DIR / "alpha_results.db",
                                                  PROJECT_DIR / "machine_lib_V2_1.py", args.run_id,
                                                  allow_simulation_post=False)
                    else:
                        report = execute_phase10b(store, config, None, None, PROJECT_DIR / "alpha_results.db",
                                                  PROJECT_DIR / "machine_lib_V2_1.py", args.run_id, False)
                else:
                    import machine_lib_V2_1 as machine_lib
                    base = machine_lib.login()
                    try:
                        if args.phase == "10A":
                            report = execute_phase10a(store, config, machine_lib, base, PROJECT_DIR / "alpha_results.db",
                                                      PROJECT_DIR / "machine_lib_V2_1.py", args.run_id,
                                                      allow_simulation_post=True)
                        else:
                            report = execute_phase10b(store, config, machine_lib, base, PROJECT_DIR / "alpha_results.db",
                                                      PROJECT_DIR / "machine_lib_V2_1.py", args.run_id, True)
                    finally:
                        base.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.execute_phase10b_repair:
            if args.phase not in {None, "10B"} or not args.run_id:
                raise ConfigError("--execute-phase10b-repair requires --run-id and only supports --phase 10B")
            with SingleRunnerLock(args.lock):
                import machine_lib_V2_1 as machine_lib
                if not args.allow_simulation_post:
                    report = prepare_repair_batch(
                        store, config, machine_lib, PROJECT_DIR / "alpha_results.db", args.run_id
                    )
                    report.update(executed=False, reason="SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
                else:
                    base = machine_lib.login()
                    try:
                        report = execute_repair_batch(
                            store, config, machine_lib, base, PROJECT_DIR / "alpha_results.db",
                            PROJECT_DIR / "machine_lib_V2_1.py", args.run_id, True,
                        )
                    finally:
                        base.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.production_repair:
            if not args.run_id:
                raise ConfigError("--production-repair requires --run-id")
            if not args.plan_id:
                raise ConfigError("--production-repair requires at least one explicit --plan-id (never auto-selects all plans)")
            import machine_lib_V2_1 as machine_lib
            if not args.allow_simulation_post:
                with SingleRunnerLock(args.lock):
                    report = preview_production_repair(
                        store, config, PROJECT_DIR / "alpha_results.db", args.run_id, args.plan_id, machine_lib
                    )
                    report.update(executed=False, reason="SIMULATION_POST_REQUIRES_EXPLICIT_ALLOW_FLAG")
            else:
                with SingleRunnerLock(args.lock):
                    base = machine_lib.login()
                    try:
                        report = execute_production_repair(
                            store, config, machine_lib, base, PROJECT_DIR / "alpha_results.db",
                            PROJECT_DIR / "machine_lib_V2_1.py", args.run_id, args.plan_id, True,
                        )
                    finally:
                        base.close()
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.list_repair_plans:
            if not args.run_id:
                raise ConfigError("--list-repair-plans requires --run-id")
            report = list_repair_plans(store, args.run_id, source=args.plan_source)
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.plan_check_derived:
            if not args.run_id:
                raise ConfigError("--plan-check-derived requires --run-id")
            with SingleRunnerLock(args.lock):
                report = derive_check_repair_proposals(
                    store, config, PROJECT_DIR / "alpha_results.db", args.run_id, persist=True
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.list_near_pass or args.list_manual_review or args.preview_rescue:
            if not args.run_id:
                raise ConfigError("--list-near-pass / --list-manual-review / --preview-rescue requires --run-id")
            from ppl_engine.near_pass import (
                list_manual_review, list_near_pass, load_external_evidence, preview_rescue,
            )
            if args.list_near_pass:
                report = list_near_pass(store, config, PROJECT_DIR / "alpha_results.db", args.run_id)
            elif args.list_manual_review:
                report = list_manual_review(store, config, PROJECT_DIR / "alpha_results.db", args.run_id)
            else:
                if not args.candidate_id:
                    raise ConfigError("--preview-rescue requires --candidate-id")
                ext = load_external_evidence(PROJECT_DIR / "rescue_evidence.json")
                report = preview_rescue(
                    store, config, PROJECT_DIR / "alpha_results.db", args.run_id, args.candidate_id, ext,
                )
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.show_audit_log:
            from ppl_engine.audit_log import audit_log_path, read_audit_log, resolve_audit_config
            cfg = resolve_audit_config(PROJECT_DIR)
            log_path = audit_log_path(PROJECT_DIR, cfg)
            records = list(read_audit_log(
                log_path, run_id=args.run_id, action=args.audit_action,
                candidate_id=args.audit_candidate_id, alpha_id=args.audit_alpha_id,
                repair_plan_id=args.audit_plan_id, level=args.audit_level,
                limit=max(0, int(args.audit_limit or 50)),
            ))
            report = {
                "mode": "SHOW_AUDIT_LOG", "log_path": str(log_path),
                "count": len(records), "records": records,
            }
            print(json.dumps(report, ensure_ascii=False, indent=2)); return 0
        if args.offline and not args.dry_run:
            raise ConfigError("--offline is only valid with --dry-run")
        if args.preview and not args.reconcile:
            raise ConfigError("--preview is only valid with --reconcile")
        if args.repair_preview and not args.diagnose_preview:
            raise ConfigError("--repair-preview is only valid with --diagnose-preview")
        if args.rebuild_derived and not args.summary:
            raise ConfigError("--rebuild-derived is only valid with --summary")
        if args.diagnose_preview:
            if not args.fixture:
                raise ConfigError("--diagnose-preview requires --fixture")
            envelope = json.loads(args.fixture.read_text(encoding="utf-8"))
            if envelope.get("checks") is not None:
                parsed = parse_response_text(
                    json.dumps(envelope, ensure_ascii=False), phase=str(envelope.get("phase") or "PRE_TAG"),
                    rules=config.rules, evidence_source=str(envelope.get("evidence_source") or "OFFLINE_FIXTURE"),
                )
                envelope["parsed"] = parsed
            diagnosis = diagnose_evidence(envelope, config.rules)
            output_value = {"fixture": str(args.fixture.resolve()), "diagnosis": diagnosis}
            if args.repair_preview:
                registry = {}
                if store.path.exists():
                    with store.connect() as conn:
                        registry = {row[0]: row[1] for row in conn.execute(
                            "SELECT operator_name,status FROM ppl_operator_capabilities"
                        )}
                output_value["repair"] = plan_repairs(
                    envelope.get("candidate") or {}, diagnosis, config.rules, registry=registry,
                    repair_reserve_remaining=48,
                )
            output_value.update({"network_requests": 0, "simulation_posts": 0, "check_requests": 0, "writes": 0})
            print(json.dumps(output_value, ensure_ascii=False, indent=2))
            return 0
        if args.description_preview or args.export_description:
            if not args.fixture: raise ConfigError("description preview/export requires --fixture")
            envelope=json.loads(args.fixture.read_text(encoding="utf-8"));candidate=envelope["candidate"]
            draft=draft_description(candidate,envelope.get("field_metadata",{}),config.rules,formal=False)
            output_value={"fixture":str(args.fixture.resolve()),"draft":draft,"manual_action_checklist":manual_checklist(candidate,draft),"network_requests":0,"writes":0,"patch_requests":0}
            print(json.dumps(output_value,ensure_ascii=False,indent=2));return 0
        if args.refresh_preview:
            if not args.fixture: raise ConfigError("--refresh-preview requires --fixture")
            envelope=json.loads(args.fixture.read_text(encoding="utf-8"));snapshot=parse_alpha_properties(envelope["alpha_details"],evidence_source=envelope.get("evidence_source","SYNTHETIC_TEST"))
            output_value=refresh_preview(envelope["candidate"],envelope["description_validation"],snapshot,envelope.get("manual_evidence"));output_value["fixture"]=str(args.fixture.resolve())
            print(json.dumps(output_value,ensure_ascii=False,indent=2));return 0
        if args.check_preview:
            if not args.fixture:
                raise ConfigError("--check-preview requires --fixture")
            text_value = args.fixture.read_text(encoding="utf-8")
            try:
                envelope = json.loads(text_value)
            except json.JSONDecodeError:
                envelope = {}
            phase = str(envelope.get("phase") or "PRE_TAG")
            evidence = str(envelope.get("evidence_source") or "SYNTHETIC_TEST")
            parsed = parse_response_text(text_value, phase=phase, rules=config.rules, evidence_source=evidence)
            output_value = {
                "fixture": str(args.fixture.resolve()), "alpha_id": envelope.get("alpha_id"),
                "evidence_source": evidence, "phase": phase, "parsed": parsed,
                "summary": build_check_summary(parsed), "facts": envelope.get("facts", {}),
                "network_requests": 0, "check_requests": 0, "writes": 0,
            }
            print(json.dumps(output_value, ensure_ascii=False, indent=2))
            return 0
        if args.status:
            print(json.dumps(store.status(args.run_id), ensure_ascii=False, indent=2))
            return 0
        if args.summary:
            summary = write_foundation_summary(args.output, store, config, args.run_id, args.rebuild_derived)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0
        if args.family_preview or args.priority_preview:
            selected_run=args.run_id or ((store.latest_run() or {}).get("run_id"))
            if not selected_run: raise ConfigError("No run exists")
            analytics=build_analytics(store,selected_run,config,False)
            if args.family_preview:
                families=analytics["families"]
                output_value={"run_id":selected_run,"families_total":len(families),"families_with_complete":sum(f["complete_count"]>0 for f in families),"largest_families":sorted(families,key=lambda f:(-f["candidate_count"],f["family_id"]))[:10],"similarity_risks":dict(__import__('collections').Counter(f["local_similarity_risk"] for f in families)),"dataset_distribution":dict(__import__('collections').Counter(f["dataset_id"] for f in families)),"transform_distribution":dict(__import__('collections').Counter(f["transform_family"] for f in families)),"network_requests":0,"writes":0}
            else:
                cby,fby,mby=analytics["maps"]
                output_value={"run_id":selected_run,"top_candidates":analytics["ranked"],"evidence_levels":dict(__import__('collections').Counter(x["evidence_level"] for x in analytics["scores"])),"top_families":sorted(analytics["families"],key=lambda f:(-f["complete_count"],-f["candidate_count"],f["family_id"]))[:10],"network_requests":0,"writes":0}
            print(json.dumps(output_value,ensure_ascii=False,indent=2));return 0
        if args.reconcile and args.preview:
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist")
            selected_run = args.run_id or ((store.latest_run() or {}).get("run_id"))
            if not selected_run:
                raise ConfigError("No run exists")
            plan = plan_offline_reconcile(store, selected_run, config, PROJECT_DIR / "alpha_results.db")
            output = {k: v for k, v in plan.items() if k != "changes"}
            output["preview"] = True
            output["writes"] = 0
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0
        with SingleRunnerLock(args.lock):
            if args.initialize:
                store.initialize()
                if args.run_id:
                    store.create_run(args.run_id, config)
                    write_initial_state(args.state, args.run_id, config)
                print(json.dumps({"initialized": str(store.path), **config.run_snapshot()}, ensure_ascii=False, indent=2))
                return 0
            if not store.path.exists():
                raise ConfigError("ppl_runner.db does not exist; run --initialize first")
            if args.dry_run:
                store.initialize()
                evidence = build_project_operator_evidence(PROJECT_DIR / "alpha_results.db")
                store.upsert_operator_evidence(evidence)
                if args.offline:
                    cached = store.load_latest_discovery(config.plan["simulation_settings"])
                    if cached is None:
                        raise ConfigError("OFFLINE_DISCOVERY_CACHE_MISS")
                    discovery = DiscoveryResult(**cached)
                    methods = []
                    status_counts = {}
                else:
                    import machine_lib_V2_1 as machine_lib

                    base_session = machine_lib.login()
                    read_only = ReadOnlySession(base_session)
                    try:
                        discovery = discover_online(read_only, config, machine_lib)
                    finally:
                        base_session.close()
                    store.save_discovery_snapshot(
                        discovery.snapshot, discovery.datasets, discovery.fields
                    )
                    methods = sorted(set(read_only.methods))
                    status_counts = dict(sorted(read_only.status_counts.items()))
                report = estimate_candidate_plan(
                    discovery, config, store.operator_registry_summary()
                )
                report["discovery_http_methods"] = methods
                report["discovery_http_status_counts"] = status_counts
                report["discovery_429_count"] = int(status_counts.get("429", 0))
                report["network_mode"] = "OFFLINE" if args.offline else "ONLINE_READ_ONLY"
                report["authentication_note"] = (
                    "BRAIN authentication occurs before the post-authentication GET-only firewall."
                    if not args.offline else None
                )
                store.save_dry_run(
                    report["dry_run_id"], discovery.snapshot["snapshot_id"],
                    config.execution_hash, report["source"], report,
                )
                atomic_write_json(args.dry_run_output, report)
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 0
            if args.candidate_preview:
                store.initialize()
                cached = store.load_latest_discovery(config.plan["simulation_settings"])
                if cached is None:
                    raise ConfigError("DISCOVERY_SNAPSHOT_REQUIRED")
                discovery = DiscoveryResult(**cached)
                dry_snapshot = store.load_latest_dry_run(discovery.snapshot["snapshot_id"], config.execution_hash)
                if dry_snapshot is None:
                    raise ConfigError("DISCOVERY_SNAPSHOT_MISMATCH: matching Dry Run snapshot required")
                import machine_lib_V2_1 as machine_lib

                run_id = store.get_or_create_run(config, args.run_id)
                candidates, report = generate_candidate_preview(
                    discovery, dry_snapshot, config, run_id=run_id,
                    alpha_db=PROJECT_DIR / "alpha_results.db", machine_lib=machine_lib,
                )
                store.upsert_candidates(candidates)
                atomic_write_json(args.candidate_preview_output, report)
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 0
            if args.reconcile:
                store.initialize()
                selected_run = args.run_id or ((store.latest_run() or {}).get("run_id"))
                if not selected_run:
                    raise ConfigError("No run exists")
                plan = plan_offline_reconcile(store, selected_run, config, PROJECT_DIR / "alpha_results.db")
                result = apply_offline_reconcile(store, plan, config)
                files = write_reconciled_outputs(
                    args.state, args.execution_plan_output, store, selected_run, config, result
                )
                result.update(files)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            raise ConfigError("No action selected")
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
