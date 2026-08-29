# WorldQuant_BRAIN_v3 v3.0.4n — /check 429 unattended-run hardening

## Why this patch exists

`run_0005` Batch 47 completed all 18 Repair simulations, but the GET-only PRE-TAG/manual-finalization `/check` tail hit sustained HTTP 429 (`THROTTLED`). The legacy check path nested the machine-lib 8-attempt retry loop inside the semantic check loop, and one final 429 RequestFailure could escape the manual queue refresh and pause the whole Round. This is especially harmful for unattended overnight runs: durable Simulation work is safe, but the process stops after only a small amount of progress.

## Project version

- Previous: `v3.0.4m`
- New: `v3.0.4n`
- `PPC_CONTROLLED_BRANCH` policy version remains `v3.0.4m`; its repair semantics were not changed in this patch.

## Fixes

### 1. Shared adaptive `/check` 429 cooldown

All live PRE-TAG/manual-refresh checks now share one process-wide throttle gate.

Default round runtime settings:

- one low-level GET attempt per semantic poll (`check_http_retries_per_poll: 1`)
- initial 429 cooldown: 60 seconds
- exponential growth: x2
- maximum cooldown: 300 seconds
- 4 throttled polls per check session before that candidate is deferred

A 2xx response resets the shared 429 streak.

This removes the old nested `8 x semantic-poll` retry storm.

### 2. 429 no longer aborts the whole Round

Structured final 429 responses are converted into durable transient check evidence. A repeatedly throttled candidate becomes unresolved/deferred instead of raising a guard-stopping RequestFailure.

Manual-finalization GET-only refresh is also isolated: an observational refresh failure cannot abort durable SEARCH/REPAIR work.

### 3. Throttle wait does not consume semantic `/check` timeout

Time deliberately spent behind the shared 429 gate is tracked separately and excluded from the normal semantic polling timeout. A healthy cooldown is no longer mistaken for `POLL_TIMEOUT`.

### 4. Configured check poll budget is actually used

`run_one_pretag_check()` and `refresh_one_pretag_check()` previously hard-coded an 8-request local budget even though the production round plan configures `max_poll_requests_per_candidate: 40`.

v3.0.4n uses the configured per-candidate poll budget.

### 5. Automatic recovery of transient unresolved PRE-TAG checks

During REPAIR, before allocating the next batch, the Round retries a small aged slice of unresolved PRE-TAG checks using GET only.

Defaults:

- enabled
- 2 candidates per cycle
- minimum unresolved age: 300 seconds

This lets an unattended run gradually clear checks that were deferred during a throttle window, without re-POSTing simulations.

### 6. Recovered REPAIR-batch finalization

A second crash window was found while auditing Batch 47:

1. `execute_round_repair()` can finish and persist all durable Simulation/Repair facts;
2. outer Round updates Repair budget/current batch;
3. a later GET-only manual refresh can fail before `finish_batch()`;
4. on restart, accounting marks that batch `RECOVERED`;
5. the old recovery finalizer only supported SEARCH batches.

v3.0.4n adds a REPAIR recovery finalizer. It completes the batch from durable local facts, refreshes/reporting/telemetry as possible, and performs **zero Simulation POSTs** for the recovered work.

This is directly relevant to the interrupted `run_0005` Batch 47.

## Safety invariants preserved

- No automatic Submit.
- No automatic PowerPoolSelected/property PATCH.
- No DELETE behavior changed.
- No `machine_lib_V2_1.py` changes.
- Simulation POST identity/resume rules are unchanged.
- Repair budgets are unchanged.
- PPC Controlled Branch semantics are unchanged.
- HIGH_TURNOVER staged Repair semantics are unchanged.

## Hash semantics

The v3.0.4n check-throttle configuration lives in operational runtime/check-budget material.

Verified locally:

- execution hash: unchanged from v3.0.4m
- operational hash: changed, as expected
- presentation hash: unchanged

Therefore this patch does not redefine the frozen alpha-generation execution semantics.

## Offline validation

Split full-suite result:

- group 1: 228 passed
- group 2: 342 passed
- group 3a: 109 passed
- group 3b: 31 passed
- v3 round orchestrator: 83 passed
- total: **793 passed, 0 failed**

`python -m compileall -q ppl_engine ppl_runner.py` also passed.

New regression coverage includes:

- final 429 converted to semantic response instead of escaping
- bounded 429 deferral semantics
- throttle wait excluded from semantic timeout
- recovered REPAIR batch finalizes without re-POSTing
- existing Production Repair/PPC/turnover/state-machine tests remain green

## Recommended run_0005 recovery sequence

After overlaying the patch:

```powershell
python -m pytest -q -p no:cacheprovider --disable-warnings `
  .\tests\test_ppl_phase_5.py `
  .\tests\test_production_logging_phase2.py `
  .\tests\test_v3_round_orchestrator.py
```

Then inspect:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

Run a one-batch recovery canary:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 1
```

If Batch 47 was left RUNNING, v3.0.4n should reconcile it as a recovered REPAIR batch and finalize it from durable facts without re-POSTing those 18 simulations.

After the canary is clean, continue the requested unattended run:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 10
```

Expected 429 behavior is now a visible sequence such as:

```text
[CHECK 429] throttled; next GET cooldown 60.0s
[CHECK 429] global cooldown 60.0s
...
```

Persistent throttling should defer affected checks and allow the Round to continue/recover later instead of terminating with `ROUND_STOPPED_BY_GUARD` solely because `/check` returned 429.
