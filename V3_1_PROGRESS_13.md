# V3.1 Progress 13 — D2E Compatibility Evidence Run Infrastructure

Date: 2026-08-28
Status: `D2E INFRASTRUCTURE READY / REAL PLATFORM RUN NOT STARTED`

## 1. Roadmap correction accepted

The prior sequence contained a real dependency conflict: D2 activation thresholds require real matured Shadow outcomes, while the older roadmap deferred the first real WorldQuant run until after D3/D4/E. That would force D3 thresholds to be guessed or calibrated post hoc.

The formal sequence is now:

`D2 offline evidence infrastructure -> D2E run_0006 Compatibility Evidence -> Calibration -> D2 Safety Gate Review -> D3 run_0007 Adaptive Canary -> D4 -> E -> Production`.

D2E is not D3 and does not grant Adaptive authority.

## 2. run_0006 identity is now explicit and locked

Added `ppl_round_v31_d2e.yaml` with durable research-run identity:

- mode: `COMPATIBILITY_EVIDENCE`;
- expected run: `run_0006`;
- Scheduler authority: `PHASE_COMPATIBILITY`;
- Shadow mode: `SHADOW_ONLY`;
- Adaptive control: `DISABLED`;
- authority transition allowed: `false`;
- automatic evidence stop: `false`.

Added `ppl_engine/research_run_mode.py`.

Creation safety:

- `run_0006` is reserved for D2E;
- generic/default Continuous is rejected if it would explicitly or automatically allocate `run_0006`;
- D2E requires explicit `--run-id run_0006`;
- D2E policy cannot be used to create `run_0007` or another run.

Resume safety:

- the durable `ppl_rounds.config_json` research-run lock is checked before policy migration/reload;
- any D2E authority-lock drift is rejected;
- a pre-existing `run_0006` without D2E durable identity is rejected rather than silently repurposed.

## 3. Maturation semantics are pre-registered before real evidence

D2E uses state-based maturation:

- terminal failed/missing Simulation -> mature zero-yield;
- Search COMPLETE waits for resolved durable classification;
- Repair COMPLETE waits for durable repair verdict;
- RUNNING/SUBMITTED/UNCERTAIN/unresolved COMPLETE -> right-censored;
- `minimum_observation_age_seconds` remains unset.

This prevents selecting a convenient elapsed-time threshold after seeing run_0006 results. A future time fallback, if needed for very long censoring, must be proposed from observed latency evidence and explicitly reviewed.

## 4. Calibration report extended without changing authority

The read-only Scheduler Evidence report now also exposes:

- durable research-run identity;
- pre-registered maturation protocol;
- Search/Repair durable observation-latency proxy distributions;
- productivity 100/500 score series summaries and absolute step-change distributions;
- qualitative coverage flags for matured Search, matured Repair, disagreement, both-backlog fairness and zero-slot pressure.

No numeric stability threshold or evidence-sufficiency threshold is invented. The monitor cannot auto-stop, auto-pause or auto-declare READY_FOR_CALIBRATION.

## 5. Execution boundary

Still unchanged:

- `strategy_integration.mode = PHASE_COMPATIBILITY`;
- `scheduler_shadow.mode = SHADOW_ONLY`;
- Scheduler authoritative = `false`;
- `.shadow_action` execution references in `round_orchestrator.py` = `0`;
- D3 not started;
- no live WorldQuant requests made during D2E implementation.

## 6. Validation

Collected tests: `963`.

Sanitized-source runnable: `948`.

Non-overlapping regression matrix:

- Compatibility: `247 passed`; 1 known production-DB-bound test unavailable because sanitized source lacks root `alpha_results.db`.
- Phase 2.1–10B: `320 passed`; 2 known production-DB-bound tests unavailable because sanitized source lacks the historical root runner/alpha DB snapshot.
- Production/Remote/Repair: `149 passed`.
- V3.1 A/B/C/D1/D2/Calibration/D2E: `149 passed`.
- V3 Round Orchestrator: `83 passed`.
- Total runnable: `948 passed / 0 runnable failures`.

Historical production-DB-bound unavailable: `15`, unchanged in nature:

- 12 `test_ppl_foundation.py` tests require root `alpha_results.db`;
- 1 historical execution-hash integration requires root production DBs;
- 1 Phase-5 production snapshot assertion requires root `ppl_runner.db`;
- 1 Phase-7 CLI preview requires root production runner DB.

Focused D1+D2+Calibration+D2E: `39 passed`.
New D2E focused: `7 passed`.

`compileall`: PASS.

`ROUND_SCHEMA_VERSION`: `4`.

`machine_lib_V2_1.py` SHA256 unchanged:
`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Active source/config contains no `2Y Sharpe >= 1.58` production rule.

## 7. Important implementation correction found during D2E

Initial run reservation logic checked only the operator-supplied `requested_run_id`. That was insufficient because a default Continuous start with no explicit run ID could auto-allocate the next ID as `run_0006` and bypass the reservation.

Correction: validation now checks the resolved run ID as well. If `_next_run_id()` resolves to `run_0006` under a non-D2E policy, creation fails closed.

Prevention rule: experimental run identity locks must be enforced against both requested identity and resolved durable identity, not only CLI input.

## 8. Next action

Do not start D3.

The next real-platform action is `run_0006` under `ppl_round_v31_d2e.yaml`. It remains a compatibility-authoritative control/evidence run for its entire lifetime. Evidence collection should be inspected periodically with:

`python ppl_runner.py --scheduler-evidence-report --run-id run_0006`

The run is paused only by explicit operator decision when evidence appears adequate for calibration review; there is no automatic N-sample lifecycle stop.
