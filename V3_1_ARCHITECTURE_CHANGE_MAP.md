# WorldQuant BRAIN V3.1 Architecture Change Map

Baseline: `v3.0.4o`
Source: `WorldQuant_BRAIN_v3.0.4o_source.zip`
Date: 2026-08-27
Target: `Continuous Alpha Research Engine`

## 0. Non-negotiable architecture rules

1. Continuous is the top-level lifecycle; V3.1 must not be implemented as `while True: execute_round()`.
2. Global research budget is unlimited/statistics-only in Continuous mode; local safety bounds remain bounded.
3. Recoverable external failures become WAIT/DEFER/RETRY; unsafe durable truth remains a global HALT.
4. Failure blast radius must match failure scope: candidate, endpoint, dataset, remote slot, report, or core storage.
5. On process startup, reconcile existing RUNNING/SUBMITTED/UNCERTAIN remote identities before any new POST.
6. Runtime remote polling must be queue-based and must not occupy a Simulation worker for 30 minutes.
7. Qualification, Search Strategy, Repair Strategy, Scheduler, Execution and Durable layers are separate.
8. Platform hard rules, local strategy rules and diagnostics/warnings are separate rule classes.
9. Policy changes are versioned and apply at safe checkpoints; pure policy changes must not mutate `sim_key` or remote Simulation identity.
10. Scheduler uses value + age/fairness + stagnation; removing 1600/400 must not create Search/Repair starvation.
11. Strategy code must not own DB transactions, HTTP sessions, state transitions or POST/DELETE execution.
12. Strategy changes must not require execution-engine changes.

## 1. Primary coupling hotspot

`ppl_engine/round_orchestrator.py`

- ~6760 lines
- 119 top-level functions
- owns round lifecycle, global budgets, search ranking/selection, repair selection, recovery, check recovery, dataset rolling discovery, telemetry/reporting, machine/execution compatibility and completion.

Decision: `DECOMPOSE GRADUALLY`, not rewrite in one step.

## 2. Change matrix

| Current mechanism | Current source | V3.1 classification | Target behavior |
|---|---|---|---|
| 2000 total budget | `ppl_plan_v3.yaml`, `config.py:21`, `round_store.py:create_round`, `round_orchestrator.py:6470+` | `STATISTICS_ONLY` | Counters remain; no normal stop condition |
| 1600 Search / 400 Repair | same | `STATISTICS_ONLY` | Scheduler chooses Search/Repair dynamically |
| execution hash includes research/budget policy | `config.py:72 build_execution_material()` | `SPLIT_IDENTITY` | Simulation semantics identity separate from Qualification/Search/Repair policy identity |
| `--max-batches` | `ppl_runner.py`, `execute_round():6476` | `KEEP_OPTIONAL_GUARD` | explicit canary/debug boundary only; no default production stop |
| SEARCH -> REPAIR one-way phase | `execute_round():6549-6630` | `REPLACE_WITH_SCHEDULER` | SEARCH and REPAIR become work types |
| `ROUND_NO_SAFE_CANDIDATE` completion | `execute_round():6723-6733` | `REMOVE_AS_NORMAL_COMPLETION` | Discovery/Repair/Wait refresh fallback |
| `BUDGET_EXHAUSTED_NORMALLY` completion | `execute_round():6723-6733` | `REMOVE_AS_NORMAL_COMPLETION` | no global budget stop in Continuous mode |
| recovered batch still nonterminal -> PAUSE | `execute_round():6490-6516` | `CONVERT_TO_QUEUE/WAIT` | retain durable batch audit but do not globally stop if other safe work exists |
| any resumed nonterminal -> PAUSE | `execute_round():6517-6542` | `CONVERT_TO_REMOTE_QUEUE` | reserve remote slot; due polling; continue safe work |
| one UNCERTAIN -> global exception | `_round_runtime_guard():3980` | `LOCAL_QUARANTINE + SLOT_RESERVATION` | no repost for identity; global WAIT only when capacity/safety exhausted |
| one AUTH_ERROR -> global exception | `_round_runtime_guard():3980` | `AUTO_RECOVER / WAIT_AUTH` | coordinated re-auth, per-candidate retry; global wait only if credentials unavailable |
| REMOTE_NOT_FOUND | resume/quarantine functions | `KEEP_LOCAL_TERMINAL` | release slot; never repost; engine continues |
| long blocking poll | `machine_lib_V2_1.py simulate_candidates()` + V2.1 polling | `REPLACE_WITH_DUE_POLL_QUEUE` | POST returns after durable URL; GET later by `next_poll_at` |
| server slot guard defers remaining invocation | `simulation_adapter.py` / V2.1 + orchestrator | `REPLACE_WITH_SLOT_ACCOUNTING` | available slots continue; wait only if all slots reserved |
| `/check` candidate/request totals | `_analyze_and_check():3557`, `CheckBudget` | `LOCAL_SAFETY/THROTTLE`, not research stop | due Check queue + endpoint cooldown; backlog control |
| 429 | `check_transport.py`, `live_validation.py`, `live_execution.py` | `ENDPOINT_WAIT` | endpoint-specific cooldown/backoff, automatic resume |
| 5xx/network timeout | simulation/check HTTP helpers | `ENDPOINT_WAIT/REQUEUE` | transient backoff; not terminal Candidate failure |
| SQLite/core DB write failure | `store.py` diagnostics | `KEEP_GLOBAL_HALT` | stop new remote effects immediately |
| report/CSV failure | `_write_reports()` | `REPORT_DEGRADED` | durable engine can continue; report retry separately |
| machine hash warning/block | `live_execution.py:validate_machine_lib_hash`, `config.py` | `SPLIT_ATTESTATION_COMPATIBILITY` | source attestation separate from Simulation semantics compatibility |
| Ctrl+C | current BaseException path | `GRACEFUL_STOP` | stop new POST, checkpoint queues, preserve URLs, restart reconciliation |

## 3. Global budget coupling details

### Current coupling

- `ppl_plan_v3.yaml`:
  - `max_new_simulation_posts: 2000`
  - `initial_search_fraction: 0.8`
  - `repair_reserve_fraction: 0.2`
- `ppl_engine/config.py:21 simulation_budget_allocation()` materializes 2000/1600/400.
- `ppl_engine/config.py:72 build_execution_material()` includes budget and repair policy values in `execution_hash`.
- `ppl_engine/round_store.py:create_round()` requires integer `total_budget/search_budget/repair_budget` columns.
- `_select_search_batch(... remaining)` returns `[]` when remaining <= 0.
- `_execute_search_rows()` enforces `consumed <= remaining_search_budget`.
- `_select_repair_batch(... remaining)` is bounded by remaining Repair budget.
- `execute_round()` uses remaining budgets for phase transition and final completion.

### V3.1 migration

Do not delete legacy DB columns. For Continuous runs:

- preserve counters for audit;
- add lifecycle/budget semantics metadata (`CONTINUOUS`, `STATISTICS_ONLY`);
- budget values may remain compatibility placeholders initially, but must not govern scheduler termination;
- `run_0005` remains interpreted under legacy V3.0.x semantics;
- new V3.1 run starts fresh (recommended `run_0006`).

## 4. Recovery and remote execution

### Current issue

`_resume_nonterminal()` selects every `RUNNING/SUBMITTED/STALE_RUNNING` candidate and calls `_execute_search_rows()`; V2.1 can poll a candidate up to ~1800 seconds. `execute_round()` then pauses globally if any resumed candidate is still nonterminal.

### Target

Introduce durable remote work scheduling:

- `remote_state`
- `simulation_url`
- `submitted_at`
- `next_poll_at`
- `last_polled_at`
- `poll_attempts`
- `slot_reservation_state`
- `remote_resolution_state`

At startup:

1. reconcile RUNNING/SUBMITTED/UNCERTAIN identities;
2. compute conservative slot reservations;
3. perform due GETs;
4. only then enable new POST for genuinely free slots.

At runtime, one long remote Simulation must not block unrelated free slots.

## 5. Qualification architecture

### Current facts

`ppl_classifier.py:classify_ppl_candidate()` centralizes PPL classification but mixes:

- local metric gates (`sharpe`, turnover),
- platform `/check` facts,
- theme-specific interpretation,
- local PPC portfolio strategy,
- diagnostics,
- manual-finalization classification,
- repairability/priority.

YAML holds many values, but the decision structure is still hard-coded.

### Required V3.1 split

Three distinct rule roles:

1. `PLATFORM_HARD_RULE`
2. `LOCAL_QUALIFICATION_RULE` / `LOCAL_STRATEGY_RULE`
3. `DIAGNOSTIC_WARNING`

Missing metric behavior must be explicit, e.g. `on_missing: UNRESOLVED`, not silently FAIL.

Target API:

`QualificationPolicy.evaluate(context) -> QualificationResult`

Result carries:

- `qualified`
- `classification`
- `blockers`
- `unresolved`
- `diagnostics`
- `repairable_failure_codes`
- `platform_facts`
- `local_strategy_results`
- `policy_version`

Future qualification changes must be declarative and must not alter `sim_key`.
Do not pre-populate hypothetical business rules or thresholds. Only rules explicitly
approved for the active policy may appear in the qualification configuration.

## 6. Policy identity and hot changes

Current execution hash includes research policy and budgets, causing non-Simulation strategy edits to require compatibility handling.

V3.1 needs separate identities:

- `simulation_semantics_version/hash`
- `qualification_policy_version/hash`
- `search_policy_version/hash`
- `repair_policy_version/hash`
- `scheduler_policy_version/hash`
- `source_attestation/hash`

Rules:

- already submitted remote Simulation keeps its original Simulation identity;
- Qualification/Search/Repair policy may change at a safe checkpoint;
- new decisions record current policy versions;
- old decisions retain their original attribution;
- a policy-only change must not force duplicate Simulation or invalidate resumable URL.

## 7. Search Strategy extraction

Current `_select_search_batch()` combines evidence collection, cache classification, uncertainty guard, breadth/novelty ranking, exploit/explore, active Dataset filtering, diversity caps, extension caps and selection persistence.

V3.1 separation:

- Engine/Context builder supplies read-only facts.
- `SearchStrategy.propose(context)` returns ranked `SearchDecision` objects.
- Scheduler chooses how many to execute.
- Execution validates cache/no-repost/slot capacity immediately before POST.
- Strategy must not access HTTP or mutable DB connection.

## 8. Repair Strategy extraction

Current repair selection is partly modular but orchestrator still knows concrete PPC/turnover/direction/staged behavior.

Target registry protocol:

- `supports(context) -> bool`
- `score(context) -> float`
- `propose(context) -> RepairDecision[]`
- `evaluate(parent, child, context) -> RepairOutcome`

Concrete strategies may include PPC, turnover, sub-universe, region, neutralization, etc.

Local per-candidate/family bounds remain mandatory even though global research budget is unlimited.

## 9. Scheduler fairness and stagnation

Removing fixed 1600/400 allocation creates starvation risk.

Scheduler score must include at least:

- expected research value
- priority
- queue age / aging
- safe slot availability
- queue backlog pressure
- recent Search/Repair productivity
- stagnation/zero-yield streaks

Do not replace 1600/400 with another rigid percentage budget.

High-value Repair may jump ahead, but Search must not be starved indefinitely; similarly, Search cannot starve Repair/Check recovery queues.

## 10. Dataset/Search productivity

Continuous mode needs explicit productivity windows, at minimum:

- last 100 Simulation attempts
- last 500 Simulation attempts
- new READY families
- Near-Pass yield
- distinct family yield
- Repair improvement/success rate
- Dataset/operator/family yield

Dataset lifecycle remains configurable and should support ACTIVE / COOLDOWN / EXHAUSTED / REVISITABLE semantics.

Existing user constraints retained:

- Data Coverage default threshold >= 0.90 and configurable;
- do not add automatic Dataset Historical Coverage Preflight;
- supported PPL regions must not be hard-coded;
- concurrency limits remain configurable (current GLB=4, others=8).

## 11. Manual finalization boundary

Continuous research != automatic final submission.

Keep:

- manual finalization queue;
- fresh platform Check before submission;
- human-confirmed `protect-alpha` after submission;
- separation between `protect-alpha` and Simulation no-repost identity.

## 12. Telemetry/reporting

Continuous operation makes full-history repeated telemetry/report rebuild increasingly expensive.

V3.1 target:

- core durable facts remain authoritative;
- telemetry becomes incremental where possible;
- report failure => `REPORT_DEGRADED` + retry, not global halt;
- core DB write/integrity failure => global HALT.

Do not claim telemetry refactor fixes the historical SQLite readonly incident; root cause remains unknown.

## 13. V3 safety assets that must not regress

- `sim_key` identity
- resume-first on process startup
- durable `simulation_url`
- post attempted/confirmed/uncertain accounting
- Simulation ledger
- UNCERTAIN no-auto-repost
- REMOTE_NOT_FOUND no-repost
- cache durable-first behavior
- manual `protect-alpha`
- single-runner lock
- Batch/round audit history
- v3.0.4o SQLite diagnostics

## 14. Build order

### V3.1-A — compatibility + policy/lifecycle foundation

Status: `A1 DEVELOPMENT CHECKPOINT IMPLEMENTED` (not production-ready).

Implemented in A1:

- explicit Continuous lifecycle profile;
- legacy V3 mode remains the compatibility default;
- global research budget is statistics-only in Continuous mode while legacy enforcement remains intact;
- separate policy-version fields/contracts are present;
- `--continuous` is a dedicated public entry point and does not implement `while True: execute_round()`;
- explicit `--max-batches` remains available only as an invocation/canary guard;
- Continuous startup/resume keeps resume-first reconciliation ahead of new POST work;
- Continuous no-work/nonterminal paths wait and retry instead of completing/pausing solely because of legacy budget/no-safe-work conditions;
- Continuous Ctrl+C has a graceful durable stop path;
- tests cover budget/statistics semantics, legacy enforcement, execution identity, CLI entry, and no hypothetical qualification threshold being pre-populated.

Still deferred beyond A1:

- non-blocking Remote Poll Queue and slot-aware execution;
- endpoint/auth WAIT controllers;
- scoped runtime-guard quarantine;
- full safe-checkpoint policy hot reload;
- declaration-driven Qualification evaluator and Search/Repair compatibility adapters.

### V3.1-B — remote queue / WAIT / recovery

- separate POST from long poll;
- due Poll queue and slot accounting;
- startup reconciliation;
- endpoint wait controllers for 429/5xx/network;
- coordinated auth recovery;
- UNCERTAIN local quarantine and conservative slot reservation;
- graceful Ctrl+C.

### V3.1-C — Qualification/Search/Repair extraction

- declarative Qualification evaluator;
- platform/local/diagnostic rule layers;
- SearchStrategy interface and first compatibility implementation wrapping current behavior;
- RepairStrategy registry and compatibility adapters;
- policy-version attribution.

### V3.1-D — adaptive scheduler/productivity

- expected value + fairness/aging;
- Search/Repair productivity windows;
- queue high-water marks;
- Dataset stagnation/revisit policies;
- discovery refresh scheduling.

## 15. Acceptance rule

V3.1 is not considered successful merely because the process stays alive.

Success requires:

- no artificial global budget stop;
- no single remote RUNNING blocking unrelated free slots;
- no duplicate POST regression;
- recoverable failures self-recover;
- core durable failures still halt safely;
- Search/Repair/Qualification policy can change without execution-engine edits;
- policy-only changes do not change Simulation identity;
- Search/Repair queues do not starve;
- historical V3.0.x state remains readable/auditable.

## 15. Implementation checkpoint — V3.1-B1 (2026-08-27)

Implemented in the development branch/package:

- durable `ppl_remote_work` due-poll queue;
- non-blocking Search and Repair Simulation handoff;
- startup durable remote reconciliation;
- conservative remote slot accounting for RUNNING/SUBMITTED/UNCERTAIN;
- scoped remote 429/network/5xx/auth wait states;
- two-observation 404/410 remote-missing quarantine;
- legacy 400 Repair reserve removed from Continuous control semantics while preserved for legacy V3;
- async Repair `DISPATCHED` state and completion reconciliation;
- async completion research-ledger refresh retaining original batch attribution;
- explicit batch report semantics distinguishing remote handoff completion from full Simulation/workflow completion.

Not yet implemented: coordinated auth recovery, Check Queue, Discovery wait controller, due-time wake scheduler, full policy plugin extraction and adaptive Search/Repair scheduler.

## 16. Implementation checkpoint — V3.1-B2 (2026-08-27)

Status: `B UNATTENDED RUNTIME OFFLINE COMPLETE` — overall V3.1 remains not production-ready.

B2 now implements and offline-validates:

- durable non-blocking Remote Poll Queue and slot accounting;
- durable PRE-TAG / Manual Finalization Check Queue;
- coordinated Auth recovery for Remote/Check/Discovery WAIT_AUTH;
- durable Rolling Discovery queue with endpoint cooldown/backoff;
- Dataset-local DataField 404/410 isolation;
- deterministic failed-refresh cooldown and later refresh-number progression;
- Continuous UNCERTAIN local quarantine across Search, Repair and recovered batches;
- recovered Search/Repair remote-handoff semantics without re-POST;
- due-time wake control across Remote, Check, Discovery, Auth and Report waits;
- derived report degradation/retry while preserving core DB fail-closed behavior;
- mixed synthetic restart/throttle/auth/network/UNCERTAIN/report soak.

Current sanitized-source validation: `853 passed / 0 failed`; 15 historical production-DB-bound tests are intentionally not runnable because sanitized packages exclude root production databases.

Next: V3.1-C should make the already-defined Scheduler/SearchStrategy/RepairStrategy/Qualification contracts control the real orchestrator while preserving current PPL strategy behavior first. V3.1-D then adds productivity/fairness policy. No live run_0006 production canary should start before C/D integration and final review.

## 17. Implementation checkpoint — V3.1-C1 Strategy/Scheduler Compatibility Integration (2026-08-27)

Status: `C1 OFFLINE COMPLETE` — compatibility-first strategy boundary is wired into the real Continuous orchestrator; adaptive cross-queue arbitration and declaration-driven Qualification remain later work.

Implemented:

- Added pure `ppl_engine/strategy_compat.py`; adapters receive immutable facts only and have no DB/HTTP/session execution primitives.
- Existing proven Search selector output is projected into `SearchDecision[]` using `LEGACY_SEARCH_SELECTOR_COMPAT`.
- Existing proven Repair selector output is projected into `RepairDecision[]` using `LEGACY_REPAIR_SELECTOR_COMPAT`.
- Real Continuous orchestrator now routes selected Search/Repair work through `choose_next_action()` before execution; execution consumes IDs returned through the declarative decisions.
- C1 uses explicit `PHASE_COMPATIBILITY` mode so selector behavior/order is preserved. Remote slot enforcement stays in the already-validated B runtime path; C1 does not introduce new Search/Repair allocation behavior.
- `ppl_round_v31.yaml` now declares the strategy integration bridge explicitly.
- Search/Repair candidate-decision telemetry records strategy adapter plus Search/Repair/Scheduler policy versions.
- Each real strategy gate emits `STRATEGY_ACTION_SELECTED` and batch reports retain strategy integration attribution.
- Policy-only strategy metadata does not alter SimulationSpec or sim_key.
- The planning-only example `2Y Sharpe >= 1.58` is not present as a rule and remains intentionally absent.

Compatibility boundary:

- C1 does **not** yet move the internals of `_select_search_batch()` / `_select_repair_batch()` into pure plugins. Those selectors remain engine-owned compatibility implementations so their ranking/planning behavior is unchanged.
- C1 does **not** yet perform value/fairness arbitration between simultaneously populated Search and Repair queues. V3.1-D will remove the phase-compatibility gate after productivity/fairness evidence is available.
- C1 does **not** alter PPL Qualification rules.

Sanitized-source offline validation after C1:

- collected: `874` tests;
- runnable without production DB snapshots: `859 passed / 0 failed`;
- production-DB-bound historical tests: `15` not runnable from sanitized package;
- V3 Round Orchestrator: `83 passed`;
- V3.1 A/B/C1 group: `60 passed`;
- Production/Remote/Repair group: `149 passed`;
- Phase group: `320 passed / 2 production-DB-bound deselected`;
- other compatibility group: `247 passed / 1 production-DB-bound deselected`;
- `compileall`: PASS;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Next recommended checkpoint: **V3.1-C2 — declaration-driven Qualification compatibility extraction**, preserving current PPL classification exactly before any new business rule is considered. After C2, proceed to policy checkpoint/version reload and then V3.1-D adaptive Search/Repair scheduler.

## 18. Implementation checkpoint — V3.1-C2 Qualification Compatibility Extraction (2026-08-27)

Status: `C2 OFFLINE COMPLETE` — current PPL Qualification behavior is now projected through a declaration-driven compatibility evaluator while the proven V3 classifier remains the authoritative composer. No business threshold was changed.

Implemented:

- Added pure `ppl_engine/qualification_policy.py` with immutable rule declarations/evaluations and `QualificationResult` projection.
- `ppl_round_v31.yaml` now declares only current approved rules under `qualification_integration`; values are referenced from the existing `ppl_classification` section rather than duplicated.
- Rule roles are explicit: `PLATFORM_HARD_RULE`, `LOCAL_QUALIFICATION_RULE`, `LOCAL_STRATEGY_RULE`, `DIAGNOSTIC_WARNING`.
- Missing facts use explicit `UNRESOLVED` / `NOT_APPLICABLE` behavior; absence is not silently converted into FAIL.
- Platform `POWER_POOL_CORRELATION` outcome remains authoritative platform fact while PPC portfolio cutoffs remain a separate local strategy layer.
- Existing non-PPL checks, including `LOW_2Y_SHARPE` / `TWO_YEAR_SHARPE`, remain diagnostics only. No hypothetical two-year-Sharpe threshold was introduced.
- Continuous `classify_run()` attaches Qualification policy/evaluator version, policy hash, blockers, unresolved facts, diagnostics, platform facts and local strategy results as sidecar attribution without rewriting the public `classify_ppl_candidate()` dictionary.
- Continuous policy loading now uses `ppl_round_v31.yaml`; Legacy continues to use `ppl_round_v3.yaml`.
- A process-local immutable Qualification snapshot prevents uncontrolled YAML edits from changing semantics mid-batch. `force_reload` exists only as a future safe-checkpoint hook; automatic hot reload is not enabled in C2.
- Qualification policy identity is separate from Simulation identity; pure Qualification edits change the Qualification policy hash but do not enter `simulation_key(expression, settings)`.

Compatibility boundary:

- The V3 classifier decision tree still composes the final PPL classification in C2. The declarative evaluator independently evaluates and attributes current rules, then projects the proven legacy classification into the stable `QualificationResult` contract.
- C2 intentionally does not activate safe-checkpoint policy reload; that requires explicit version/hash transition validation and durable reload events.
- C2 does not alter Search/Repair ranking, Scheduler phase compatibility, SimulationSpec, remote execution or no-repost behavior.

Sanitized-source offline validation after C2:

- collected: `887` tests;
- runnable without production DB snapshots: `872 passed / 0 failed`;
- production-DB-bound historical tests: `15` unavailable by design;
- other compatibility group: `247 passed / 1 production-DB-bound deselected`;
- phase group: `320 passed / 2 production-DB-bound deselected`;
- Production/Remote/Repair group: `149 passed`;
- V3.1 A/B/C1/C2 group: `73 passed`;
- V3 Round Orchestrator: `83 passed`;
- `compileall`: PASS;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Next recommended checkpoint: **V3.1-C3 — safe-checkpoint Qualification policy reload and durable policy attribution**, followed by **V3.1-D** adaptive Search/Repair arbitration and productivity/fairness scheduling.

## 19. Implementation checkpoint — V3.1-C3 Safe-Checkpoint Qualification Policy Reload (2026-08-28)

Status: `C3 OFFLINE COMPLETE` — Qualification policy can now change during a Continuous run at a durable safe checkpoint without changing Simulation identity or applying mixed rule versions inside an unfinished batch. Overall V3.1 remains not production-ready until the D scheduler/productivity stage and final production canary are complete.

Implemented:

- Added engine-side `ppl_engine/policy_runtime.py`; Qualification evaluation remains pure/read-only while DB/event activation lives in the execution/control layer.
- Added durable `ppl_round_policy_state` coordination state for the active Qualification policy version/hash/payload and activation batch.
- New/restarted Continuous runs install the durable active Qualification snapshot before recovery/classification work.
- If YAML changes while an unfinished recovered batch exists, the durable old policy remains active; reload is only considered after no unfinished batch remains and before any new Search/Repair allocation.
- At the safe checkpoint, only Qualification-scope drift is eligible: `qualification_integration`, `ppl_classification`, and `policy_versions.qualification`.
- Any non-Qualification drift is rejected by the C3 controller and the last durable active policy continues running.
- A semantic Qualification change with the same policy version is rejected with `POLICY_VERSION_BUMP_REQUIRED`.
- A valid new version/hash is durably activated, process-local evaluator snapshot is replaced, round active config is updated, and `QUALIFICATION_POLICY_RELOADED` is recorded with from/to version/hash plus `simulation_identity_unchanged=true`.
- Repeated identical rejected candidate policies use deterministic event keys so a bad file cannot spam the durable event table every scheduler cycle.
- Status/report processes restore the durable active Qualification snapshot rather than blindly interpreting the latest edited YAML.
- Batch-end snapshots now include the active Qualification policy version/hash/activation batch for later replay/audit.
- The immutable round-creation manifest remains historical evidence of round start; later Qualification activations are tracked through durable policy state/events rather than rewriting creation history.
- Qualification changes still do not enter `simulation_key(expression, settings)` or mutate already-dispatched SimulationSpec/remote identity.
- No new business rule or threshold was introduced. `LOW_2Y_SHARPE` / `TWO_YEAR_SHARPE` remain diagnostics only and the earlier hypothetical `2Y Sharpe >= 1.58` example remains absent.

Crash/restart semantics:

- Durable policy state is the active-policy source of truth.
- If a crash occurs after durable policy-state activation but before a derived event/report update, restart restores the durable active payload and continues under the same version/hash.
- An edited YAML file becomes only a candidate policy until a later safe checkpoint validates it.

Compatibility boundary:

- C3 hot reload is Qualification-only. Search/Repair/Scheduler policy hot reload is intentionally not enabled yet.
- C3 does not alter Search ranking, Repair planning, phase-compatible Scheduler behavior, Simulation execution, no-repost invariants, or platform submission behavior.
- Legacy V3.0.x policy drift behavior remains unchanged; C3 activation is only for Continuous mode with `safe_checkpoint_policy_reload=true`.

Sanitized-source offline validation after C3:

- collected: `897` tests;
- runnable without production DB snapshots: `882 passed / 0 failed`;
- historical production-DB-bound tests: `15` unavailable by design;
- compatibility group: `247 passed / 1 production-DB-bound unavailable`;
- phase group: `320 passed / 2 production-DB-bound unavailable`;
- Production/Remote/Repair group: `149 passed`;
- V3.1 A/B/C1/C2/C3 group: `83 passed`;
- V3 Round Orchestrator: `83 passed`;
- `compileall`: PASS;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Next recommended checkpoint: **V3.1-D1 — adaptive Search/Repair scheduler compatibility rollout**, using observed productivity + fairness/aging while preserving no-starvation and all B/C safety invariants. Search/Repair strategy hot reload should follow only after D1 establishes durable scheduler attribution and replay semantics.

## 20. Implementation checkpoint — V3.1-D1 Adaptive Scheduler Shadow (2026-08-28)

Status: `D1 OFFLINE COMPLETE` — adaptive Search/Repair arbitration is now observable and replay-attributed, but it is deliberately non-authoritative. Overall V3.1 remains not production-ready.

Implemented:

- Added pure `ppl_engine/scheduler_shadow.py`; it receives immutable ledger/queue/slot facts and has no DB/HTTP/session/state-transition primitives.
- Added explicit `scheduler_shadow.mode=SHADOW_ONLY` and independent `V31_SCHED_SHADOW_001` policy identity.
- Existing `policy_versions.scheduler=V31_SCHED_001` and C1 `PHASE_COMPATIBILITY` remain authoritative.
- Search productivity is computed from latest 100/500 paid `NEW_POST` Search simulations using completion, READY/PPL-success, PRE-TAG PASS, local PASS, family diversity and an explicitly labelled Near-Pass proxy.
- Repair productivity is computed from latest 100/500 paid `NEW_POST` Repair simulations using completion, resolved verdicts, `TARGET_PASS`, `IMPROVED`, `ACCEPT`, `WORSE`, `NO_IMPROVEMENT` and family diversity.
- Shadow value combines observed productivity, backlog pressure, queue aging, cold-start exploration and consecutive-action fairness.
- Remote-slot reservation from the B runtime is included as an observational input.
- Every new Search/Repair allocation records `SCHEDULER_SHADOW_DECISION` with actual action, shadow recommendation, scores, agreement, queue/productivity/slot facts, and explicit `authoritative=false` / `execution_action_unchanged=true`.
- Shadow event identity includes a selected-ID fingerprint so retry/recovery cannot silently reuse a decision for a different selected set.
- Search/Repair batch reports include `scheduler_shadow` attribution.
- A static regression guard verifies `round_orchestrator.py` does not branch on `.shadow_action`.

Compatibility boundary:

- D1 does not alter `_select_search_batch()` or `_select_repair_batch()`.
- D1 does not change actual Search/Repair order, allocation, ranking, Repair planning, Qualification, SimulationSpec, `sim_key`, no-repost, or B-stage endpoint recovery.
- A shadow `WAIT`, `SEARCH`, or `REPAIR` is telemetry only.
- The immutable ledger does not preserve every historical `evidence_label`; therefore Near-Pass is explicitly a proxy in D1 and cannot be treated as exact historical Near-Pass yield.
- No Search/Repair/Scheduler hot reload is enabled by D1.

Sanitized-source offline validation after D1:

- collected: `903` tests;
- runnable without production DB snapshots: `888 passed / 0 failed`;
- historical production-DB-bound tests: `15` unavailable by design;
- compatibility: `247 passed`;
- phase: `320 passed`;
- Production/Remote/Repair: `149 passed`;
- V3.1 A/B/C1/C2/C3/D1: `89 passed`;
- V3 Round Orchestrator: `83 passed`;
- D1-specific: `6 passed`;
- `compileall`: PASS;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Next checkpoint: **V3.1-D2 — scheduler evidence gate / replay / fallback design before any authoritative adaptive Search↔Repair arbitration.** D2 must not be activated merely because a shadow scorer exists. It must first prove deterministic replay, starvation safety, acceptable disagreement behavior, sufficient productivity sample size and an explicit fallback to `PHASE_COMPATIBILITY`.

## 21. Implementation checkpoint — V3.1-C Policy / Strategy Layer Finalization (2026-08-28)

Status: `V3.1-C OFFLINE COMPLETE` — the C-layer architecture debt identified after the D1 review is now closed. D1 remains Shadow-only; no adaptive scheduler recommendation controls execution yet.

Closed C-layer items:

- Execution identity split: historical `execution_hash` retained, while Continuous resume safety uses `simulation_semantics_hash` and mutable research attribution uses `research_policy_hash`.
- Search policy drift and Repair policy drift no longer masquerade as remote Simulation semantic drift.
- Pure SearchStrategy extracted from the orchestrator; runtime Search selection uses immutable facts -> pure strategy -> decisions -> Engine.
- Pure RepairStrategy extracted for Repair value/ranking; durable plan creation, no-repost, preview and remote execution remain Engine responsibilities.
- Explicit `search_policy` and `repair_policy` sections own V3.1 allocation/ranking/planning values. Historical aliases remain compatibility fallbacks only.
- Search and Repair cycle capacity now independently follow their dedicated policy allocation.
- Durable policy state now covers Qualification/Search/Repair.
- Q/S/R hot-policy changes activate atomically at a safe checkpoint with version/hash validation and rollback on transaction failure.
- Restart restores durable last-known-good Q/S/R before parsing mutable candidate-policy content for replacement.
- Non-hot drift cannot enter through the Q/S/R reload controller.
- Search/Repair decisions and reports carry active policy version/hash attribution.
- Batch reports explicitly distinguish `legacy_aliases` from `effective_search_policy` and `effective_repair_policy` so compatibility fields cannot be mistaken for active policy.
- Pure Q/S/R policy changes do not alter `sim_key`, SimulationSpec or existing remote identity.
- No new Qualification rule was introduced; the earlier conversational `2Y Sharpe >= 1.58` example remains absent from active policy.

Static boundary audit:

- Runtime Search/Repair selection after policy loading/migration contains no direct `adaptive_ranking` access.
- Remaining legacy adaptive references are loader/migration/backward-compatibility logic only.
- SearchStrategy and RepairStrategy expose no SQLite/RunnerStore/HTTP/session/machine_lib/POST/state-transition primitive.
- Continuous execution compatibility compares remote Simulation semantics independently from mutable research-policy drift.

Final sanitized-source validation for C closure:

- collected: `930` tests;
- runnable: `915 passed / 0 failed`;
- historical production-DB-bound: `15 unavailable by design`;
- compatibility: `247 passed`;
- phase: `320 passed`;
- Production/Remote/Repair: `149 passed`;
- V3.1 A/B/C/D1: `116 passed`;
- V3 Round Orchestrator: `83 passed`;
- C4/C5/C6 focused after final report-attribution regression: `27 passed`;
- `compileall`: PASS;
- live WorldQuant requests: `0`;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Revised next sequence:

1. D2 — scheduler evidence gate, deterministic replay, matured productivity cohorts, starvation/fairness validation, explicit fallback. Remains non-authoritative.
2. D3 — constrained adaptive Search-vs-Repair canary.
3. D4 — authoritative adaptive allocation only after D2/D3 evidence passes.
4. E — long-run telemetry/report hardening plus final attestation cleanup and extended synthetic soak.
5. F — real `run_0006` WorldQuant canary, then production hardening from observed failures.

## 22. Implementation checkpoint — V3.1-D2 Scheduler Evidence Gate (2026-08-28)

Status: `D2 OFFLINE COMPLETE` — Scheduler evidence, replay, matured productivity, starvation safety and activation eligibility are implemented, but adaptive Search/Repair control remains strictly non-authoritative. `PHASE_COMPATIBILITY` still owns real execution.

Implemented:

- Added `ppl_engine/scheduler_evidence.py` as an evidence-only layer with no HTTP POST, workflow-transition or execution primitive.
- Added additive durable tables for Scheduler evaluations, actual-action outcomes and gate reports without changing `ROUND_SCHEMA_VERSION=4`.
- Evaluation Ledger freezes `actual_action`, `shadow_action`, agreement, decision timestamp/batch, Scheduler version/hash, Evidence-policy version/hash, Search/Repair backlog and queue age, matured 100/500 productivity, slot state, fairness/consecutive state, selected-intent fingerprint and scores.
- Scheduler identity and Evidence-policy identity are separate durable identities. `V31_SCHED_SHADOW_002` identifies the evaluated Scheduler; `V31_SCHED_EVIDENCE_001` identifies D2 evidence/gate semantics.
- D1 productivity semantics were version-bumped to `V31_SCHED_SHADOW_002` because right-censor handling changes Scheduler input semantics.
- Paid productivity now uses the latest matured `NEW_POST` cohort rather than treating recent RUNNING/SUBMITTED/UNCERTAIN observations as zero-yield. Terminal failures are mature zero-yield; Search COMPLETE waits for a resolved classification and Repair COMPLETE waits for a durable repair verdict.
- Actual-action outcome attribution records COMPLETE, READY/PPL success, Near-Pass, distinct families, family winner facts, Repair verdict outcomes and an explicitly defined effective-Simulation ratio.
- Unexecuted alternatives are stored only as `COUNTERFACTUAL_PROXY` with `observed_outcome=false`; they are never represented as real counterfactual results.
- Deterministic replay can rebuild the pure Scheduler snapshot from frozen durable facts and requires the same Scheduler policy hash before replay.
- A hard starvation guard now forces the opposite research side after the configured consecutive-action bound when both Search and Repair backlogs exist and a research slot is available. This replaces the previous assumption that a soft score penalty alone proves no-starvation.
- Stress validation covers Search-dominant, Repair-dominant, slot=1/full-slot Remote pressure, and explicitly preserves 429/Check/Discovery/Auth as runtime obligations outside the research expected-value model.
- Scheduler Safety Gate checks observations, Search samples, Repair samples, deterministic replay, starvation, slot safety, no-repost attestation, recovery-safety attestation and policy identity.
- Production sample thresholds are intentionally unset (`null`) in D2. Therefore current gate status is structurally `INELIGIBLE_THRESHOLDS_UNSET`; D2 cannot activate a canary by itself.
- Safe-fallback classification is formalized: scheduler exception/invalid decision/policy mismatch/slot conflict/missing identity -> future `PHASE_COMPATIBILITY`; duplicate-POST risk/durable identity conflict/DB corruption/core invariant failure -> fail closed/global halt.
- Repair Shadow observation was moved to after local preflight/slot trimming but still before execution so its selected-intent fingerprint reflects the final compatibility-selected Repair intent.

Compatibility and safety boundary:

- `strategy_integration.mode` remains `PHASE_COMPATIBILITY`.
- `scheduler_shadow.mode` remains `SHADOW_ONLY`.
- Every Scheduler decision and gate report remains `authoritative=false` and has no activation side effect.
- `round_orchestrator.py` still does not branch on `.shadow_action`.
- Runtime obligations remain due-time/safety obligations and are not scored against Search/Repair productivity.
- D2 does not change `sim_key`, SimulationSpec, durable `simulation_url`, no-repost, UNCERTAIN quarantine, Q/S/R atomic policy activation or run lifecycle semantics.
- D2 adds no Qualification rule; `2Y Sharpe >= 1.58` remains absent from active source/config.
- `run_0005` remains frozen; no real `run_0006` production canary was executed.

Final sanitized-source validation after D2:

- collected: `948` tests;
- runnable without production DB snapshots: `933 passed / 0 failed`;
- historical production-DB-bound: `15 unavailable by design`;
- Compatibility: `247 passed` (`1` production-DB-bound deselected);
- Phase 2.1–10B: `320 passed` (`2` production-DB-bound deselected);
- Production/Remote/Repair: `149 passed`;
- V3.1 A/B/C/D1/D2: `134 passed`;
- V3 Round Orchestrator: `83 passed`;
- D1+D2 focused: `24 passed`;
- `compileall`: PASS;
- live WorldQuant requests: `0`;
- `ROUND_SCHEMA_VERSION`: remains `4`;
- `machine_lib_V2_1.py` SHA256 unchanged: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Next checkpoint: `V3.1-D3 ADAPTIVE_CANARY`, but it must not start until Shadow evidence accumulates enough real observations to justify explicit sample thresholds and the user authorizes moving beyond D2.

## 23. Post-D2 checkpoint — Scheduler Evidence Calibration Readiness (2026-08-28)

Status: `CALIBRATION TOOLING COMPLETE / REAL EVIDENCE ACCUMULATION PENDING`.

This checkpoint does not start D3. The clean D2 source package intentionally contains no production `ppl_runner.db`, therefore there is no real `run_0006` Scheduler evidence from which activation sample thresholds can yet be justified.

Added a read-only evidence-review path before any threshold selection:

- `ppl_engine/scheduler_evidence_report.py` reads the durable D2 Evaluation/Outcome Ledger through SQLite `mode=ro` + `PRAGMA query_only=ON`;
- `python ppl_runner.py --scheduler-evidence-report --run-id <run_id>` prints the calibration report;
- the report never calls D2 schema creation/migration helpers, never writes SQLite, never mutates `activation_thresholds`, and never changes Search/Repair execution;
- current Scheduler/Evidence policy identity is resolved from the round's durable stored `config_json`, not from an edited candidate YAML;
- historical evidence with different Scheduler/Evidence hashes is not pooled into the current identity cohort; same-version/different-hash conflicts are surfaced explicitly;
- the report summarizes actual/shadow action distribution, agreement/disagreement matrix, replay consistency, matured/censored NEW_POST counts, Search READY/Near-Pass yield, Repair success yield, decision-margin distribution, both-backlog fairness coverage and zero-slot safety coverage;
- Bernoulli rate precision is reported with 95% Wilson score intervals;
- no automatic activation sample threshold is generated. `recommended_thresholds` stays `null` until real D2 observations plus an explicit risk/precision choice justify a threshold.

Safety boundary remains unchanged:

- `strategy_integration.mode = PHASE_COMPATIBILITY`;
- `scheduler_shadow.mode = SHADOW_ONLY`;
- Scheduler `authoritative=false`;
- D3 `NOT STARTED`;
- `ROUND_SCHEMA_VERSION=4`;
- `sim_key`, SimulationSpec, durable URL/no-repost and Q/S/R policy identities unchanged;
- Runtime obligations remain outside the research expected-value model.

Operational dependency discovered: D2 requires real Shadow observations before D3, while the earlier roadmap placed the first real `run_0006` canary in F after D3/D4/E. Those two statements cannot both be literal. Before any live execution, the roadmap must distinguish a pre-D3 compatibility-mode evidence run from the later adaptive/production canary. The safe evidence run keeps `PHASE_COMPATIBILITY` authoritative and `SHADOW_ONLY` non-authoritative; it is not D3. The run identity to use for that evidence collection must be confirmed before execution so a live run is not silently repurposed. Periodically inspect the read-only calibration report; only after confidence/coverage is adequate should explicit sample thresholds be proposed and D3 considered.

## 24. Route correction — V3.1-D2E Compatibility Evidence Run (2026-08-28)

Status: `D2E INFRASTRUCTURE READY / REAL PLATFORM RUN NOT STARTED`.

The post-D2 audit found a real roadmap dependency conflict: D2 activation thresholds must be calibrated from real matured Shadow outcomes, but the older roadmap placed the first real WorldQuant run after D3/D4/E. D3 cannot be justified from real D2 evidence if no real compatibility-mode evidence run exists first.

Formal route now supersedes the earlier D2 -> D3 -> ... -> F/run_0006 sequence:

1. D2 — Scheduler Evidence Infrastructure: offline complete; threshold values remain unset.
2. D2E — `run_0006` Compatibility Evidence Run: real WorldQuant, `PHASE_COMPATIBILITY` authoritative, Scheduler `SHADOW_ONLY`, Adaptive control zero.
3. Evidence Calibration Report: review matured Search/Repair yield, disagreement, queue aging, fairness/slot pressure, productivity-window volatility and censoring.
4. D2 Safety Gate Review: only after real evidence supports explicit threshold values.
5. D3 — new `run_0007` Adaptive Canary.
6. D4 — Adaptive Authoritative only after canary evidence.
7. E — Long-run Hardening.
8. Production candidate on a later clean run (`run_0008` if sequencing remains unchanged).

### D2E run identity and authority lock

A dedicated policy file `ppl_round_v31_d2e.yaml` defines the run identity before any live request:

- `research_run.mode = COMPATIBILITY_EVIDENCE`;
- `expected_run_id = run_0006`;
- `scheduler_authority = PHASE_COMPATIBILITY`;
- `scheduler_shadow = SHADOW_ONLY`;
- `adaptive_control = DISABLED`;
- `authority_transition_allowed = false`;
- `automatic_evidence_stop = false`.

The lock is persisted in the durable `ppl_rounds.config_json` snapshot. Creation requires an explicit `run_0006`; generic Continuous execution is not allowed to create or auto-allocate `run_0006`. Resume validates the durable lock before policy migration/reload and rejects any authority-mode drift. If an existing `run_0006` does not carry the D2E durable identity, resume fails closed rather than silently repurposing that run.

This is intentionally stricter than merely remembering not to enable Adaptive control. The first real V3.1 run becomes a clean compatibility control group and can never be converted into D3 within the same run.

### Pre-registered maturation semantics

D2E fixes the maturation rule type before real evidence is observed:

- primary maturation is state-based, not elapsed-time based;
- terminal failed/missing Simulation facts are mature zero-yield;
- Search `COMPLETE` waits for durable resolved classification;
- Repair `COMPLETE` waits for durable repair verdict;
- RUNNING/SUBMITTED/UNCERTAIN and unresolved COMPLETE work remain right-censored;
- no post-hoc minute threshold may be introduced merely because it makes the observed results look better.

`minimum_observation_age_seconds` remains unset. If real long-running censoring later requires a time fallback, it must be proposed from the D2E latency distribution and explicitly reviewed before becoming policy.

### Evidence sufficiency is observational, never lifecycle control

The read-only Scheduler Evidence Calibration Report now additionally exposes:

- the durable D2E research-run identity;
- the pre-registered maturation protocol;
- Search/Repair durable maturation-latency proxies;
- 100/500 productivity score series and absolute step-change summaries;
- qualitative evidence coverage flags for Search/Repair matured evidence, disagreement, both-backlog fairness and zero-slot pressure.

No numeric stability threshold or evidence-sufficiency threshold is automatically generated. The monitor cannot auto-stop or auto-pause Continuous execution. A D2E pause remains an explicit operator evidence checkpoint, not a reintroduced global research budget.

### Run lifecycle decision

- `run_0005`: V3.0.x historical production, frozen.
- `run_0006`: V3.1 D2E Compatibility Evidence / control run, permanently compatibility-authoritative.
- `run_0007`: D3 Adaptive Canary, only after D2 Safety Gate review passes.
- later run (`run_0008` if required): D4 / production candidate.

No live WorldQuant request was made while implementing this D2E infrastructure.
