# V3.1 Progress 03 — B1 Remote Queue / Slot Runtime

Baseline: `V3.1-A1` on frozen `v3.0.4o` source  
Date: 2026-08-27  
Checkpoint: `V3.1-B1`  
Status: DEVELOPMENT CHECKPOINT COMPLETE — NOT PRODUCTION READY

## 1. Scope completed

B1 removes the most important blocking-remote behavior from the Continuous path while preserving legacy V3.0.x semantics.

Implemented:

1. Added durable `ppl_remote_work` queue state.
2. Continuous startup reconciles RUNNING/SUBMITTED/UNCERTAIN durable identities before opening new POST capacity.
3. New or resumed remote Simulations are handed off after durable `simulation_url` persistence; the Continuous path does not enter the legacy long `wait_simulation()` loop.
4. Remote work is polled once per due cycle using `next_poll_at`; a RUNNING job is requeued instead of occupying a Python worker.
5. Remote slot accounting is derived from durable queue state.
   - current GLB limit remains configuration-driven (`4` in the active plan);
   - one known RUNNING/UNCERTAIN remote identity reserves one slot;
   - unrelated free slots remain usable.
6. `UNCERTAIN_SUBMISSION` stays no-auto-repost and conservatively reserves capacity.
7. Remote polling scope handling:
   - `429` -> durable `WAIT_RATE_LIMIT` + Retry-After;
   - network/5xx -> durable `WAIT_NETWORK` + bounded backoff;
   - `401/403` -> durable `WAIT_AUTH` (coordinated re-login remains B2);
   - `404/410` -> two-observation confirmation before `REMOTE_NOT_FOUND`;
   - `REMOTE_NOT_FOUND` releases its slot and remains no-repost.
8. Search execution uses non-blocking remote handoff in Continuous mode.
9. Round Repair execution uses the same non-blocking remote handoff in Continuous mode.
10. The legacy global 400-Repair reserve is statistics-only in Continuous mode, including the inner `production_repair.py` enforcement path.
11. Local Repair safety remains bounded through `local_repair_planning_cap_per_cycle`.
12. Added `DISPATCHED` Repair-plan state for asynchronous remote execution.
    - a RUNNING/SUBMITTED Repair child no longer masquerades as fully EXECUTED;
    - the plan is non-executable while the remote job is in flight;
    - Poll completion advances the plan to `EXECUTED`.
13. PPC branch locking now treats both `DISPATCHED` and `EXECUTED-without-outcome` as pending, preventing a second child from being launched while the current PPC repair is still running/evaluating.
14. Async Poll completion refreshes the existing research ledger without changing the original dispatch batch/phase/origin attribution.
15. Continuous batch reports explicitly record `batch_completion_semantics=REMOTE_HANDOFF_COMPLETE` when remote work is still nonterminal. The legacy path remains `SIMULATION_WORKFLOW_COMPLETE`.
16. The v3.0.4o SQLite diagnostics remain intact; core durable-write failures are still fail-closed.

## 2. Important safety invariants retained

B1 does not weaken:

- `sim_key` identity;
- durable-first `alpha_results.db` Simulation identity;
- resume/no-repost behavior;
- post attempted/confirmed/uncertain accounting;
- Simulation ledger;
- `UNCERTAIN_SUBMISSION` no-auto-repost;
- `REMOTE_NOT_FOUND` no-repost;
- manual `protect-alpha`;
- single-runner lock;
- legacy V3.0.x Budget enforcement for legacy runs;
- core DB write failure as global halt/fail-closed behavior.

`machine_lib_V2_1.py` was not modified. Its SHA-256 remains:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## 3. B1 files added

- `ppl_engine/continuous_remote.py`
- `tests/test_v31_remote_queue_b1.py`
- `V3_1_PROGRESS_03.md`
- `V3_1_CHANGESET_03.json`
- `V3_1_B1_SHA256SUMS.txt`

## 4. B1 files changed from A1

Core/config:

- `ppl_engine/continuous_policy.py`
- `ppl_engine/contracts.py`
- `ppl_engine/live_execution.py`
- `ppl_engine/ppc_controlled_branch.py`
- `ppl_engine/production_repair.py`
- `ppl_engine/round_orchestrator.py`
- `ppl_engine/state_machine.py`
- `ppl_engine/store.py`
- `ppl_engine/summary_writer.py`
- `ppl_plan_v31.yaml`
- `ppl_round_v31.yaml`

Tests:

- `tests/test_ppc_controlled_branch_v1.py`
- `tests/test_production_repair.py`

## 5. Test evidence

Static compile:

- `python -m compileall -q ppl_engine tests ppl_runner.py machine_lib_V2_1.py` -> PASS

Focused B1 / repair / PPC / lifecycle tests:

- `94 passed`

SQLite + remote resolver + V3 round compatibility:

- `129 passed`

Additional compatibility groups:

- audit/candidate/check/concurrency/near-pass/PPC/classifier group: `233 passed`
- Phase 2.1/3/4/6/7/8/9/9.1/10A/10B group: `276 passed, 1 deselected`
- production logging/repair/reconcile/remote/selector/rescue/staged-repair group: `139 passed`
- execution-hash sanitized subset: `14 passed, 1 deselected`
- Phase-5 sanitized subset: `44 passed, 1 deselected`

Unique sanitized-source compatibility total represented by those non-overlapping groups:

- `835 passed`
- `0 failed`
- `3 production-DB-bound tests deselected`
- `12 Foundation tests not runnable from the sanitized source package because root-level production `alpha_results.db` is intentionally excluded`

The frozen v3.0.4o production baseline remains the prior `808 passed / 0 failed` full offline run performed before source sanitization.

## 6. Bugs found and corrected during B1

### B1-01 — inner Repair budget remained hard-enforced

Even after the outer Continuous lifecycle made 400 Repair posts statistics-only, `production_repair.py` still had an independent global reserve check.

Fix:

- legacy V3 keeps strict reserve enforcement;
- Continuous preflight/execution accepts `enforce_global_repair_budget=False`;
- local per-cycle Repair planning remains bounded.

### B1-02 — Continuous non-enforced Repair invariant compared `consumed > None`

The async Continuous execution path could reach a legacy invariant check with `remaining=None`.

Fix:

- the invariant is evaluated only when the global Repair budget is actually enforced.

### B1-03 — async Repair was marked EXECUTED before remote completion

A SUBMITTED/RUNNING Repair child was previously able to make the selected plan look terminal.

Fix:

- new `DISPATCHED` state;
- Poll completion advances it to `EXECUTED`;
- PPC branch selection treats DISPATCHED as pending.

### B1-04 — async terminal facts could leave research ledger stale

A ledger row created at dispatch could remain RUNNING/SUBMITTED after Poll completion because later batch telemetry is batch-scoped.

Fix:

- Poll completion performs candidate-scoped ledger refresh with no new batch number;
- existing batch/phase/origin/selection attribution is retained.

## 7. What B1 does NOT claim

B1 is not yet a fully unattended production engine.

Still missing / deferred to B2 or later:

1. coordinated automatic auth recovery (`WAIT_AUTH` exists, Auth Coordinator does not yet);
2. dedicated due Check Queue and endpoint-level `/check` scheduling;
3. generalized Discovery endpoint cooldown/retry controller;
4. queue-aware sleeping/wakeup using earliest `next_due_at` rather than coarse `idle_wait_seconds`;
5. full runtime guard scope conversion for every recoverable state;
6. report failure degradation/retry separation;
7. policy hot reload at safe checkpoints;
8. Search/Repair/Qualification plugin extraction behind the already-defined strategy contracts;
9. adaptive Search-vs-Repair fairness/productivity scheduler;
10. long-run telemetry incrementalization/retention.

## 8. Next checkpoint — V3.1-B2

B2 should focus on unattended recovery/control-plane behavior:

1. Auth Coordinator: one re-login owner, other remote work waits safely.
2. Check Queue: GET-only due work, durable Retry-After/backoff, no global stop on one throttled candidate.
3. Discovery endpoint wait state/backoff.
4. A shared due-time controller that sleeps until the earliest safe work rather than fixed-loop sleeping.
5. Convert remaining recoverable global guards to scoped WAIT/quarantine while preserving core DB/invariant HALT.

B1 is a development checkpoint only. Do not replace the frozen production v3.0.4o runner with this package yet.
