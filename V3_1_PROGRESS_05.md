# V3.1 Progress 05 — B2 Unattended Runtime Closeout

Baseline: `V3.1-B2A` on frozen `v3.0.4o` source  
Date: 2026-08-27  
Checkpoint: `V3.1-B2`  
Status: **OFFLINE DEVELOPMENT CHECKPOINT COMPLETE — V3.1-B UNATTENDED RUNTIME COMPLETE; V3.1 OVERALL NOT PRODUCTION READY**

## 1. Scope completed in B2 closeout

B2 closes the remaining unattended-runtime gaps identified after B2A.

1. Durable non-blocking Rolling Discovery
   - added `ppl_discovery_work` control-plane queue;
   - at most one Discovery GET per due work item/cycle;
   - Dataset and DataField pagination progress is durable;
   - 429 -> `WAIT_RATE_LIMIT`;
   - 401/403 -> `WAIT_AUTH`;
   - 5xx/network -> `WAIT_NETWORK`;
   - deterministic refresh failure -> terminal failure for that refresh number plus durable cooldown, so a later refresh number may proceed;
   - DataField 404/410 is Dataset-local skip/quarantine, not whole-refresh failure.

2. Manual Finalization recheck is queue-driven in Continuous mode
   - periodic manual-finalization refresh no longer performs a synchronous `/check` loop;
   - candidates are enqueued as durable RECHECK work;
   - Continuous automatic refresh no longer stops permanently on the legacy lifetime `max_check_candidates` / `max_check_http_requests` counters;
   - per-candidate/session/rate safety limits remain available.

3. Recovery path V3.1 semantics
   - recovered SEARCH uses non-blocking Remote handoff;
   - recovered REPAIR can close local Batch intent as `REMOTE_HANDOFF_COMPLETE` while durable remote work continues;
   - recovered UNCERTAIN is locally quarantined and never re-POSTed;
   - recovered NEW_POST tails that do not fit current server slots remain deferred, never silently dropped;
   - Recovery report generation uses the resilient report path.

4. UNCERTAIN scope hardening
   - Continuous Search and Repair preflight/TOCTOU paths use local quarantine rather than global fail-closed;
   - `UNCERTAIN_SUBMISSION` remains no-auto-repost and conservatively reserves one potential remote slot;
   - Legacy V3.0.x keeps historical global fail-closed semantics.

5. Report degradation boundary
   - derived CSV/JSON/report rendering failures -> `REPORT_DEGRADED` + durable endpoint wait/retry;
   - research execution continues;
   - `sqlite3.Error` / core durable-write failures are not swallowed and remain fail-closed.

6. Due-time control-plane wakeup
   - Remote, Check, Discovery, Auth and Report endpoint waits contribute to `due_snapshot()`;
   - Continuous idle/server-slot waits sleep using the nearest durable due-time rather than an unrelated fixed poll loop.

7. Mixed unattended synthetic soak
   - Remote sequence: 500 -> 429 -> 401 -> RUNNING -> COMPLETE;
   - Check sequence: 429 -> 500 -> 401 -> RESOLVED;
   - Discovery: 429 -> Dataset success -> DataField 404 Dataset-local isolation;
   - Report: derived write failure -> DEGRADED -> later retry success;
   - process restart simulated mid-backoff;
   - UNCERTAIN identity preserved throughout;
   - no Simulation POST was issued by recovery/control-plane work.

## 2. Safety invariants verified

B2 retains the V3 safety assets:

- `sim_key` identity unchanged;
- durable `simulation_url` retained across restart;
- no-repost for existing/resumed Simulation identities;
- `UNCERTAIN_SUBMISSION` no-auto-repost;
- `REMOTE_NOT_FOUND` no-repost;
- dynamic server-slot accounting from durable remote state;
- Repair async `DISPATCHED` lifecycle remains intact;
- Simulation ledger attribution remains tied to original dispatch batch;
- core DB failures remain fail-closed;
- manual `protect-alpha` boundary unchanged;
- Legacy V3.0.x semantics remain available when Continuous is disabled;
- `machine_lib_V2_1.py` unchanged.

`machine_lib_V2_1.py` SHA-256 remains:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## 3. Static closeout scan

### Sleep / blocking paths

The remaining `time.sleep()` calls fall into two classes:

- Continuous orchestrator waits use `wait_view.wait_seconds` from `due_snapshot()`;
- legacy-only resume preflight / legacy remote polling retains historical short/blocking sleeps.

Continuous Remote and Check work do not enter the legacy long polling loop.

### UNCERTAIN global-hold scan

Continuous call sites explicitly request local scope:

- `_round_runtime_guard(..., global_hold=False)`;
- Search execution uses `global_hold_on_uncertain=False` when Continuous recoverable-wait mode is enabled;
- Repair preflight/execution receives the same Continuous local-scope semantics;
- recovered Search/Repair paths preserve local quarantine.

Legacy defaults remain fail-closed.

### Discovery blocking scan

The Continuous main path queues Discovery and materializes the already-fetched durable metadata through the existing ranking/admission implementation. Legacy synchronous `machine_lib.get_datasets/get_datafields` code remains for Legacy/manual paths but is not the V3.1 Continuous queue path.

## 4. Test evidence

Current source collects:

- `868 tests`

Sanitized-source runnable tests validated in non-overlapping groups:

- audit/candidate/check/concurrency/hash/near-pass/PPC/classifier: `247 passed, 1 production-DB test deselected`;
- phase 2.1/3/4/5/6/7/8/9/9.1/10A/10B: `320 passed, 2 production-DB tests deselected`;
- production logging/repair/reconcile/remote/selector/rescue/staged-repair/SQLite: `149 passed`;
- V3.1 Foundation/A1/B1/B2/B2B/B2C/soak: `54 passed`;
- V3 Round Orchestrator: `83 passed`.

Unique runnable sanitized-source total:

- **853 passed**
- **0 failed**
- **15 tests not runnable from this sanitized package because root production DB fixtures are intentionally absent**

The 15 environment-bound tests are:

- 12 `test_ppl_foundation.py` tests requiring root `alpha_results.db`;
- 1 historical execution-hash integration test requiring root `ppl_runner.db` + `alpha_results.db`;
- 1 Phase-5 production snapshot assertion requiring root `ppl_runner.db`;
- 1 Phase-7 CLI preview test requiring the root production runner DB.

Those DBs are intentionally excluded from sanitized source packages. The frozen v3.0.4o baseline still has the earlier full pre-sanitization evidence: `808 passed / 0 failed`.

Static compile:

- `python -m compileall -q ppl_engine tests ppl_runner.py machine_lib_V2_1.py` -> PASS

Mixed unattended soak:

- `tests/test_v31_unattended_soak.py` -> PASS

## 5. What B2 completion means

`V3.1-B Unattended Runtime = COMPLETE` means the development source now has offline-validated durable queues and scoped recovery for the major external interruption classes that repeatedly stopped V3.0.x production:

- remote RUNNING;
- server-slot saturation;
- 429 throttling;
- auth expiry;
- network/5xx;
- Check pending/throttle;
- rolling Discovery throttle/failure;
- UNCERTAIN identity ambiguity;
- restart/recovery;
- derived report failure.

It does **not** mean the whole V3.1 project is production-ready.

## 6. Still not completed — V3.1-C/D and live validation

The following remain intentionally deferred:

1. real Scheduler takeover of Search vs Repair selection;
2. SearchStrategy production compatibility adapter/plugin extraction;
3. RepairStrategy registry/production compatibility adapter;
4. declaration-driven Qualification rule evaluator;
5. policy-version attribution on every strategy decision and safe-checkpoint hot reload;
6. productivity windows / fairness / starvation prevention in the live orchestrator;
7. long-run telemetry incrementalization/retention;
8. live `run_0006` canary against WorldQuant after C/D integration and final review.

Do not replace frozen v3.0.4o production with this checkpoint yet.

## 7. Recommended next checkpoint

Proceed to **V3.1-C1 — Strategy/Scheduler Integration**, compatibility-first:

1. make the existing Scheduler choose between declarative Search/Repair decisions in the real orchestrator;
2. wrap current Search selector behind a SearchStrategy compatibility adapter without changing ranking behavior;
3. wrap current Repair selector behind a RepairStrategy compatibility adapter without changing repair behavior;
4. add policy-version attribution;
5. only after compatibility tests pass, extract declaration-driven Qualification rules.

The objective is architectural separation without changing the current Alpha research strategy by accident.
