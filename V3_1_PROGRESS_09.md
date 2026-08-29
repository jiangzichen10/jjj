# V3.1 Progress 09 — D1 Adaptive Scheduler Shadow

Date: 2026-08-28

Status: `V3.1-D1 ADAPTIVE SCHEDULER SHADOW — OFFLINE COMPLETE`

Overall V3.1 production status: `NOT PRODUCTION READY`

Baseline: `V3.1-C3 Safe-Checkpoint Qualification Policy Reload`

## Goal

D1 introduces an adaptive Search-vs-Repair scheduler as an observational shadow only. The new scheduler may recommend a different action, but the proven C1 `PHASE_COMPATIBILITY` scheduler remains authoritative and the D1 recommendation cannot alter selected candidates/plans, Simulation identity, POST behavior, remote slot enforcement, or batch phase execution.

## Implemented

### 1. Pure shadow scheduler

Added `ppl_engine/scheduler_shadow.py`.

The module is pure/read-only and accepts immutable ledger/queue/slot facts. It has no RunnerStore, sqlite connection, HTTP session, machine_lib or state-transition primitives.

D1 computes:

- Search productivity over latest 100 and 500 paid `NEW_POST` Search simulations;
- Repair productivity over latest 100 and 500 paid `NEW_POST` Repair simulations;
- Search and Repair queue backlog;
- oldest queue age;
- cold-start exploration bonus;
- consecutive-action fairness penalty;
- durable remote-slot capacity.

### 2. Search productivity

Search window metrics include:

- attempts;
- completed;
- READY/PPL-success classifications;
- PRE-TAG PASS;
- local gate PASS;
- distinct families;
- Near-Pass proxy.

Important limitation: the immutable simulation ledger does not preserve every historical `evidence_label`, so D1 explicitly labels Near-Pass as `PROXY_FROM_LEDGER_CLASSIFICATION`. It is not presented as exact historical Near-Pass telemetry.

CACHE/RESUME rows do not count as paid productivity attempts.

### 3. Repair productivity

Repair window metrics include:

- attempts;
- completed;
- resolved repair verdicts;
- `TARGET_PASS`;
- `IMPROVED`;
- `ACCEPT`;
- `WORSE`;
- `NO_IMPROVEMENT`;
- distinct families.

### 4. Fairness and aging

The shadow score is based on:

- observed productivity;
- backlog pressure;
- oldest queue age;
- cold-start exploration;
- consecutive same-action fairness penalty.

This does not introduce a new fixed `Search 80% / Repair 20%` allocation. Queue aging/fairness is dynamic and observational in D1.

### 5. Remote-slot awareness

The shadow snapshot uses the already-audited B runtime `remote_slot_snapshot`.

If all conservative remote slots are reserved, the shadow recommendation may be `WAIT`, but D1 does not make that WAIT authoritative. The B runtime remains the real server-slot safety controller.

### 6. Durable shadow telemetry

Every D1 observation emits an idempotent:

`SCHEDULER_SHADOW_DECISION`

Payload contains:

- actual action;
- shadow action;
- agreement/disagreement;
- Search/Repair shadow scores;
- Search/Repair 100/500 productivity metrics;
- backlog and queue ages;
- remote slot limit/reserved/free;
- consecutive action/count;
- shadow policy version;
- authoritative scheduler policy version;
- `authoritative=false`;
- `execution_action_unchanged=true`;
- `selection_identity_unchanged=true`.

The event identity is bound to a deterministic selection fingerprint so a crash/retry does not silently attach a stale shadow observation to a different selected candidate/plan set.

### 7. Batch report attribution

Search and Repair batch `report_json` now include a `scheduler_shadow` section.

### 8. Policy identity separation

`ppl_round_v31.yaml` now includes:

- `scheduler_shadow.mode = SHADOW_ONLY`;
- `scheduler_shadow.policy_version = V31_SCHED_SHADOW_001`;
- productivity windows `[100, 500]`;
- fairness/aging/backlog/cold-start parameters.

The real execution scheduler remains:

`policy_versions.scheduler = V31_SCHED_001`

Therefore D1 observation policy identity is not falsely presented as the authoritative execution scheduler identity.

## Explicit non-changes

- C1 `PHASE_COMPATIBILITY` remains authoritative.
- D1 does not change Search/Repair order or allocation.
- D1 does not replace `_select_search_batch()` or `_select_repair_batch()`.
- D1 does not change Search ranking.
- D1 does not change Repair planning.
- D1 does not change Qualification rules.
- No new PPL business threshold was added.
- No two-year Sharpe Qualification rule was added.
- D1 does not change SimulationSpec or `sim_key`.
- D1 does not alter no-repost behavior.
- D1 does not alter B-stage remote/check/discovery/auth/report recovery semantics.
- No `machine_lib_V2_1.py` change.
- No live WorldQuant request.

## Safety proof in tests/static checks

D1 has a regression guard asserting `round_orchestrator.py` never branches on `.shadow_action`. The shadow recommendation is serialized to telemetry/reporting only.

A test deliberately produces:

- actual action = `SEARCH`;
- shadow action = `REPAIR`;

and verifies:

- `authoritative=false`;
- `execution_action_unchanged=true`.

A separate full-slot case allows shadow `WAIT` while the actual action remains unchanged.

## Validation

Collected: `903` tests.

Sanitized-source runnable:

- Compatibility: `247 passed`; 1 production-DB-bound unavailable.
- Phase: `320 passed`; 2 production-DB-bound unavailable.
- Production/Remote/Repair: `149 passed`.
- V3.1 A/B/C1/C2/C3/D1: `89 passed`.
- V3 Round Orchestrator: `83 passed`.

Total runnable: `888 passed / 0 failed`.

Historical production-DB-bound unavailable by design: `15`.

Static compile: `PASS`.

`machine_lib_V2_1.py` SHA256 remains:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## D1 issues caught during development

### Selection-unbound shadow event identity

Initial D1 event identity used only round/batch/action/policy. A crash before the batch intent became durable could theoretically allow the same batch number to be reselected with different identities while retaining the first shadow event.

Correction:

- bind `SCHEDULER_SHADOW_DECISION` to a deterministic selected-ID fingerprint;
- persist `selection_fingerprint` and `selection_identity_unchanged=true`.

Prevention rule:

> Durable decision telemetry created before a side-effect intent is committed must be bound to the selected decision identity, not only to a batch number.

### Non-finite unavailable queue score

The first pure scoring draft represented unavailable work with `-Infinity`. Python can serialize this, but it is not strict JSON and is undesirable durable telemetry.

Correction:

- use a finite unavailable score sentinel;
- determine `NO_RESEARCH_WORK` from queue facts rather than a non-finite numeric value.

Prevention rule:

> Durable telemetry must contain finite JSON-safe numeric values.

## D2 gate

D1 is intentionally not sufficient to activate adaptive arbitration.

Before D2 can make the scheduler authoritative, we need an evidence gate over shadow observations, including at minimum:

- enough Search and Repair paid attempts for meaningful productivity estimates;
- shadow/actual agreement rate;
- disagreement outcome analysis;
- starvation/fairness simulation;
- slot-pressure behavior;
- replay test proving the same durable facts produce the same scheduler decision;
- explicit fallback to `PHASE_COMPATIBILITY` if the adaptive policy is disabled or invalid.

No live `run_0006` production canary should be started solely because D1 is complete.
