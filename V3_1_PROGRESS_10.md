# V3.1 Progress 10 — C Layer Finalization

Date: 2026-08-28
Status: `V3.1-C POLICY / STRATEGY LAYER OFFLINE COMPLETE`
Production status: `NOT PRODUCTION READY` — D2/D3/D4, long-run telemetry hardening, and real `run_0006` canary remain.

## 1. Scope completed

This checkpoint closes the C-layer debt identified after reviewing the V3.1 architecture map. D1 Shadow remains non-authoritative and was not expanded here.

Completed C sub-stages:

- C1 — Strategy compatibility bridge: COMPLETE.
- C2 — Qualification compatibility extraction: COMPLETE.
- C3 — Qualification safe-checkpoint reload: COMPLETE.
- C4 — Execution identity split: COMPLETE.
- C4 — Pure SearchStrategy extraction: COMPLETE.
- C4 — Pure RepairStrategy extraction: COMPLETE.
- C5 — Dedicated SearchPolicy: COMPLETE.
- C5 — Dedicated RepairPolicy: COMPLETE.
- C5 — Per-phase Search/Repair allocation separation: COMPLETE.
- C6 — Atomic Qualification/Search/Repair policy bundle: COMPLETE.
- C6 — Safe-checkpoint reload/version/hash enforcement: COMPLETE.
- C6 — Restart durable last-known-good restore: COMPLETE.
- C6 — Atomic rollback and attribution/replay metadata: COMPLETE.

## 2. Execution identity boundary

V3.1 Continuous now distinguishes three identities:

1. `execution_hash` — retained as the historical broad V3 artifact for legacy compatibility/audit.
2. `simulation_semantics_hash` — protects actual remote Simulation semantics and resume safety.
3. `research_policy_hash` — attributes mutable research policy independently.

Verified behavior:

- Search policy change -> `research_policy_hash` changes; `simulation_semantics_hash` does not.
- Repair policy change -> `research_policy_hash` changes; `simulation_semantics_hash` does not.
- Simulation settings / extension execution identity change -> `simulation_semantics_hash` changes and resume fails closed.
- Pure Q/S/R policy reload does not mutate `sim_key`, existing SimulationSpec, durable `simulation_url`, or already-dispatched remote identity.

## 3. SearchStrategy extraction

Added `ppl_engine/search_strategy.py` as a pure/read-only strategy module.

Moved into the strategy boundary:

- adaptive Search scoring;
- exploit/explore/backfill selection;
- dataset/semantic/field diversity caps;
- unproven-dataset controls;
- extension batch-cap aware selection;
- deterministic selection-mode assignment.

The strategy module has no SQLite, RunnerStore, HTTP/session, machine_lib, workflow transition, cache write, or remote POST primitive.

The orchestrator remains responsible for durable facts, no-repost, Simulation identity, ledger writes, workflow transitions, and remote execution.

## 4. RepairStrategy extraction

Added `ppl_engine/repair_strategy.py` as a pure/read-only prioritization layer.

Moved into the strategy boundary:

- ordinary/good/elite Repair value bands;
- turnover-proximity value;
- negative-direction Repair value bands;
- deterministic Repair ranking.

Durable Repair plan creation, plan preview, sim_key/no-repost, remote POST, cache/resume and outcome persistence remain in the Engine.

## 5. Dedicated Search / Repair policy

Added `ppl_engine/policy_specs.py` and explicit V3.1 policy sections:

- `search_policy` — allocation, ranking, diversity;
- `repair_policy` — allocation, ranking, planning.

Legacy V3 aliases (`batch_size`, `exploration_fraction`, `adaptive_ranking`, family caps) remain only for old snapshot compatibility/fallback.

Continuous execution paths use effective dedicated policies. Search and Repair can now have independent cycle capacities.

A final static audit found one reporting ambiguity: batch snapshots still exposed legacy `batch_size` / `exploration_fraction` as if they were the active policy. This was corrected before C completion. Reports now label those fields under `legacy_aliases` and expose `effective_search_policy` / `effective_repair_policy` with version, hash and active values.

## 6. Atomic Q/S/R policy bundle

`ppl_round_policy_state` now durably tracks:

- QUALIFICATION;
- SEARCH;
- REPAIR.

Safe policy reload rules:

- YAML is a candidate policy; durable DB state is active truth.
- Reload is considered only at a safe checkpoint with no unfinished batch before new allocation.
- A semantic change requires a policy-version bump for each changed policy type.
- Q/S/R changes can activate together in one SQLite transaction.
- Non-hot drift (scheduler/runtime/execution/discovery/etc.) is rejected by the C6 controller.
- A late transaction failure rolls back policy rows and round active config together.
- Invalid/partially edited YAML cannot replace last-known-good durable Q/S/R state during restart.
- Rejected identical candidates use deterministic event identity and do not spam durable telemetry.

## 7. Business-rule integrity

No new PPL qualification rule or threshold was introduced.

In particular, the earlier conversational example `2Y Sharpe >= 1.58` was not added to `ppl_round_v31.yaml` or the declarative Qualification rule set. Existing `LOW_2Y_SHARPE` / `TWO_YEAR_SHARPE` remain diagnostics according to the current approved policy.

## 8. Final static audit

PASS:

- no direct `adaptive_ranking` / legacy allocation access remains in runtime Search/Repair selection paths after the loader/migration compatibility zone;
- Search runtime uses `effective_search_*` + pure `select_search_candidates()`;
- Repair runtime uses `effective_repair_*` + pure Repair ranking/value helpers;
- legacy adaptive references in `round_orchestrator.py` are confined to policy validation/migration/compatibility fallbacks;
- SearchStrategy/RepairStrategy contain no DB/HTTP/execution side effects;
- active batch report now distinguishes legacy aliases from effective Q/S/R policy;
- Continuous execution compatibility is based on `simulation_semantics_hash`, not mutable research policy;
- `machine_lib_V2_1.py` remains unchanged.

## 9. Offline validation

Collected tests: `930`.

Sanitized-source runnable: `915 passed / 0 failed`.

Historical production-DB-bound tests: `15 unavailable by design` because clean source packages exclude root `ppl_runner.db` / `alpha_results.db`.

Non-overlapping validation groups:

- Compatibility: `247 passed`.
- Phase: `320 passed` (`2` production-DB-bound tests excluded).
- Production / Remote / Repair: `149 passed`.
- V3.1 A/B/C/D1: `116 passed`.
- V3 Round Orchestrator: `83 passed`.

C4/C5/C6 focused tests: `27 passed` after the final report-attribution regression test.

Static compile: `compileall PASS`.

Live WorldQuant requests: `0`.

`machine_lib_V2_1.py` SHA256:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## 10. Mistakes / lessons recorded

### A. C was entered incompletely before D1

D1 Shadow was implemented before Search/Repair extraction and policy identity were fully separated. Because D1 was strictly `SHADOW_ONLY`, this did not change authoritative execution, but the architecture review correctly identified the ordering drift.

Prevention: no D2/D3 authoritative scheduler work until C is formally closed.

### B. Continuous phase capacity still used legacy global `batch_size`

Dedicated Search/Repair policy existed, but `phase_capacity()` initially still consulted the historical global batch size.

Correction: Search and Repair capacities now come from their own dedicated policy allocation.

Prevention: every policy extraction must audit both selector internals and outer lifecycle/resource caps.

### C. Durable restore initially parsed edited YAML too early

The first C6 restore path could parse a broken currently-edited YAML before restoring existing durable Q/S/R state.

Correction: existing durable policy payload wins first; disk is parsed only for missing policy types or later safe-checkpoint candidate evaluation.

Prevention: on restart, durable active truth must be restored before evaluating mutable configuration files.

### D. Batch report could mislabel legacy aliases as active policy

After C5, reports still emitted legacy `batch_size` / `exploration_fraction` at the top of the policy section, which could disagree with active dedicated Search/Repair policy.

Correction: legacy values are explicitly labelled `legacy_aliases`; effective Search/Repair policy, version and hash are now reported separately.

Prevention: compatibility aliases must never be presented as authoritative active configuration.

## 11. What is intentionally not done

- D1 remains `SHADOW_ONLY` and non-authoritative.
- D2 scheduler evidence/replay/safety gate is not yet implemented.
- D3/D4 adaptive scheduler canary/authoritative control is not implemented.
- Scheduler hot reload is not enabled by C6.
- Long-run incremental telemetry/report hardening is not yet complete.
- Full Machine Source Attestation vs Execution Compatibility redesign is not yet fully closed.
- No live `run_0006` WorldQuant canary has been started.

## 12. Next checkpoint

Return to D after the C-layer closure:

`V3.1-D2 — Scheduler Evidence Gate / deterministic replay / matured productivity / starvation safety / fallback qualification`.

D2 remains non-authoritative. Only after D2 proves safety should D3 begin a constrained adaptive Search-vs-Repair canary.
