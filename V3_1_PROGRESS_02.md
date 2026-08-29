# V3.1 Progress 02 — A1 Continuous Lifecycle Integration

Baseline: v3.0.4o  
Date: 2026-08-27  
Checkpoint: V3.1-A1  
Status: DEVELOPMENT CHECKPOINT COMPLETE — NOT PRODUCTION READY

## 1. Scope completed

A1 moves V3.1 from contracts-only foundation into the first real lifecycle integration while preserving V3.0.x compatibility.

Implemented:

1. Added a dedicated `--continuous` public entry path.
2. Added `ppl_engine/continuous_engine.py` as the stable Continuous execution boundary.
   - It invokes the orchestrator once.
   - It is intentionally not `while True: execute_round()`.
3. Added `ppl_engine/continuous_runtime.py` for lifecycle/budget interpretation without DB or HTTP side effects.
4. Continuous global research budgets are now statistics-only at lifecycle level.
   - Historical counts remain available.
   - Legacy V3 budget enforcement remains unchanged.
   - Local safety bounds remain bounded.
5. V3.1 global budget edits no longer participate in Continuous execution identity material.
   - Simulation settings still do.
   - Therefore a policy-only/global-budget edit cannot silently create a new Simulation identity.
6. Added Continuous invocation semantics:
   - default `max_batches = null`;
   - explicit `--max-batches` remains an optional canary/diagnostic guard.
7. Continuous accounting reconciliation can exceed historical 2000/1600/400 counters without declaring budget exhaustion.
8. Continuous no-work / recoverable-nonterminal lifecycle paths no longer complete or pause solely because legacy budget/no-safe-work conditions were reached.
9. Continuous KeyboardInterrupt receives a graceful durable stop path (`USER_STOP_REQUESTED`).
10. `round_status()` can report Continuous lifecycle semantics and marks budget as not enforced.
11. Rolling-discovery lifetime refresh cap is nullable for V3.1; the legacy V3 config remains unchanged.
12. `run_0005` cannot be accidentally resumed through the V3.1 Continuous entry point; a Continuous run must have Continuous stored policy semantics.
13. Policy-version identifiers are present in the V3.1 policy for future attribution/checkpoint work.
14. The hypothetical example `2-year Sharpe >= 1.58` is not a V3.1 business rule and is not present in the architecture/config. Only the generic capability to add future declarative qualification rules is retained.

## 2. Files added in A1

- `ppl_engine/continuous_engine.py`
- `ppl_engine/continuous_runtime.py`
- `tests/test_v31_lifecycle_a1.py`
- `V3_1_PROGRESS_02.md`
- `V3_1_CHANGESET_02.json`

## 3. Files changed in A1

- `V3_1_ARCHITECTURE_CHANGE_MAP.md`
- `ppl_engine/config.py`
- `ppl_engine/continuous_policy.py`
- `ppl_engine/round_orchestrator.py`
- `ppl_runner.py`
- `ppl_plan_v31.yaml`
- `ppl_round_v31.yaml`

## 4. Safety properties intentionally preserved

A1 does not remove or weaken:

- `sim_key` identity;
- resume-first;
- durable `simulation_url`;
- post attempted/confirmed/uncertain accounting;
- Simulation ledger;
- UNCERTAIN no-auto-repost semantics;
- REMOTE_NOT_FOUND no-repost semantics;
- cache durable-first ordering;
- manual `protect-alpha`;
- single-runner lock;
- V3.0.x historical audit/read compatibility;
- v3.0.4o SQLite diagnostics.

No production DB, credentials, `run_0005`, or WorldQuant remote state was modified.

## 5. Tests executed for this checkpoint

Static compile:

- `python -m compileall -q ppl_engine tests ppl_runner.py machine_lib_V2_1.py` → PASS

V3.1 tests:

- `tests/test_v31_foundation.py`
- `tests/test_v31_lifecycle_a1.py`
- Result: `22 passed`

Targeted compatibility/safety suite:

- machine hash policy
- V3.1 foundation/lifecycle
- v3.0.4o SQLite diagnostics
- remote simulation
- production repair
- V3 round orchestrator
- Result: `214 passed`

Execution-hash compatibility tests that do not require the excluded production DB fixture:

- Result: `14 passed, 1 deselected`
- The deselected historical integration test requires root-level production `alpha_results.db`, which is intentionally absent from the sanitized source baseline.

The original frozen v3.0.4o production baseline remains the previously verified `808 passed / 0 failed` before sanitization.

## 6. Important limitations — not implemented yet

A1 must not be described as a fully continuous unattended engine yet.

Still missing:

1. Remote polling is still based on the legacy blocking resume/poll implementation; a single remote RUNNING task can still occupy a worker until timeout.
2. No durable `next_poll_at` Poll Queue exists yet.
3. Server-slot accounting is not yet scheduler-driven per known RUNNING/UNCERTAIN remote task.
4. `_round_runtime_guard()` still contains legacy global-stop behavior for some states that should become scoped quarantine/wait in V3.1.
5. 429/5xx/network endpoint WAIT controllers are not yet fully generalized.
6. Coordinated 401/auth recovery is not yet the V3.1 Auth Coordinator design.
7. Safe-checkpoint policy hot reload is declared/configured but not yet fully implemented.
8. Qualification/Search/Repair remain compatibility implementations; the strategy contracts exist, but current production algorithms have not yet been extracted behind them.
9. Dataset revisit/stagnation scheduling is not yet adaptive; making refresh count unlimited does not by itself create a productive continuous discovery loop.
10. Telemetry is not yet optimized for very long-lived runs.

## 7. Next checkpoint: V3.1-B

The next implementation step should focus on continuity under real remote/runtime conditions:

1. Separate POST from long-running poll.
2. Introduce durable/due Poll Queue semantics (`next_poll_at`).
3. Account for known remote RUNNING slots without globally stopping unrelated free slots.
4. Reconcile all durable RUNNING/UNCERTAIN work before opening new POST capacity on process startup.
5. Convert endpoint 429/5xx/network failures into scoped WAIT/backoff where safe.
6. Add coordinated auth recovery.
7. Convert appropriate runtime-guard cases from global pause to local quarantine or wait.
8. Preserve global HALT for core DB/durable-identity/invariant failures.

A1 is a development checkpoint only. Do not use this package as a production replacement for v3.0.4o yet.
