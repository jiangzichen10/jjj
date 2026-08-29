# V3.1 Progress 06 — C1 Strategy/Scheduler Compatibility Integration

Date: 2026-08-27
Baseline: `WorldQuant_BRAIN_v3.1_B2_complete`
Status: `V3.1-C1 OFFLINE COMPLETE`
Production readiness: `NO`
Live WorldQuant requests in this checkpoint: `0`

## 1. Objective

C1 is an architectural compatibility checkpoint. It must put the stable Strategy/Scheduler contracts on the real Continuous execution path without changing the current Alpha research strategy.

The acceptance rule is deliberately conservative:

> same current selector inputs/configuration -> same selected Search candidates / Repair plan IDs and order -> same existing execution preflight/no-repost path.

No new PPL business threshold is introduced.

## 2. Implemented

### 2.1 Pure compatibility strategy adapters

Added `ppl_engine/strategy_compat.py`.

It provides:

- `LegacySearchStrategyAdapter`
- `LegacyRepairStrategyAdapter`
- `search_decisions_from_selected_rows()`
- `repair_decisions_from_selected_plans()`
- `choose_compatibility_strategy_action()`
- policy-version projection via `PolicyVersions`

The adapter module has no SQLite connection, RunnerStore, HTTP session or machine execution dependency. It consumes read-only mappings and returns immutable `SearchDecision` / `RepairDecision` values.

### 2.2 Real orchestrator strategy gate

For V3.1 Continuous with `strategy_integration.enabled=true`:

Search path:

`legacy Search selector -> SearchDecision[] -> Scheduler contract -> selected candidate IDs -> existing Search execution`

Repair path:

`legacy Repair selector -> RepairDecision[] -> Scheduler contract -> selected repair_plan IDs -> existing Repair preflight/execution`

The adapter output is checked against the legacy selector output. Any missing/duplicated/reordered identity mismatch is a fail-closed C1 invariant.

### 2.3 Compatibility scheduling mode

`ppl_round_v31.yaml` now declares:

```yaml
strategy_integration:
  enabled: true
  mode: PHASE_COMPATIBILITY
  search_adapter: LEGACY_SEARCH_SELECTOR_COMPAT
  repair_adapter: LEGACY_REPAIR_SELECTOR_COMPAT
  scheduler_gate: V31_SCHEDULER_CONTRACT
```

C1 intentionally does not change Search-vs-Repair allocation. It keeps the proven phase order and uses Scheduler as the common execution gate. V3.1-D will later enable cross-queue value/fairness arbitration.

### 2.4 Policy attribution

Search/Repair decision telemetry now records, when present:

- `strategy_adapter`
- `search_policy_version` or `repair_policy_version`
- `scheduler_policy_version`

Continuous batches also emit `STRATEGY_ACTION_SELECTED`, and the batch report retains a `strategy_integration` block.

### 2.5 Explicit non-change

The earlier conversational example `2-year Sharpe >= 1.58` is not a real rule and is not present in the C1 configuration or code.

C1 does not modify Qualification behavior.

## 3. What C1 intentionally does not do

- does not rewrite `_select_search_batch()` internals;
- does not rewrite `_select_repair_batch()` internals;
- does not let strategy plugins access mutable DB/HTTP primitives;
- does not change current ranking/repair strategy;
- does not enable adaptive Search-vs-Repair fairness/productivity scheduling;
- does not add/modify PPL qualification thresholds;
- does not start `run_0006` live.

This is a bridge checkpoint: real execution now depends on declarative Strategy/Scheduler contracts, while the old proven selectors remain the compatibility source of decisions.

## 4. Validation

New C1 focused tests: `6 passed`.

Full sanitized validation matrix after C1:

- total collected: `874`
- sanitized runnable: `859 passed / 0 failed`
- historical production-DB-bound: `15` unavailable by design
- V3 Round Orchestrator: `83 passed`
- V3.1 Foundation/A1/B/C1: `60 passed`
- Production/Remote/Repair: `149 passed`
- Phase suite: `320 passed`, `2` production-DB-bound deselected
- other compatibility suite: `247 passed`, `1` production-DB-bound deselected
- 12 additional `test_ppl_foundation.py` tests require the excluded production `alpha_results.db`
- `compileall`: PASS

`machine_lib_V2_1.py` unchanged:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## 5. Next

Proceed to V3.1-C2:

1. extract a declaration-driven Qualification compatibility evaluator from current PPL rules;
2. preserve current classifier output exactly first;
3. separate Platform Hard Rules / Local Qualification Rules / Local Strategy Rules / Diagnostic Warnings;
4. define missing-fact handling (`UNRESOLVED`, not silent fail);
5. prove pure Qualification-policy edits do not change Simulation identity/sim_key;
6. only after compatibility parity, add safe-checkpoint policy version reload;
7. V3.1-D then activates adaptive Search/Repair scheduling using productivity + fairness/aging.

No live production canary before C/D integration and final review.
