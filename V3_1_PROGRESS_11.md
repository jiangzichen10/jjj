# V3.1 Progress 11 — D2 Scheduler Evidence Gate

Date: 2026-08-28
Status: `V3.1-D2 OFFLINE COMPLETE`
Production status: `NOT PRODUCTION READY`
Authoritative Scheduler: `PHASE_COMPATIBILITY`
Adaptive Scheduler: `SHADOW_ONLY / authoritative=false`
Next stage: `D3 ADAPTIVE_CANARY` only after evidence thresholds are justified and explicitly approved.

## 1. Baseline and audit result

Development baseline was exclusively `WorldQuant_BRAIN_v3.1_C_COMPLETE.zip` (SHA256 `2398830f92ec19c4d6647ba8a457711ba8d44b6cda79df63af9ecb1529d0c2b4`).

Baseline facts re-verified before modification:

- internal C_COMPLETE SHA manifest: 252/252 matched;
- collected tests: 930;
- C1-C6 complete in source/tests;
- D1 `SHADOW_ONLY`, `authoritative=false`;
- real Search/Repair execution still `PHASE_COMPATIBILITY`;
- `ROUND_SCHEMA_VERSION=4`;
- `machine_lib_V2_1.py` SHA256 `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

Two D1 evidence limitations were confirmed from source rather than assumed from the handover:

1. latest 100/500 productivity used all recent NEW_POST rows in the denominator, so RUNNING/UNCERTAIN observations were right-censored but effectively scored as low yield;
2. fairness was a soft score penalty only, which did not mathematically guarantee no-starvation under persistent score dominance.

A third implementation-level issue was found during D2 integration: Repair Shadow attribution originally occurred before local slot trimming, so the selection fingerprint could describe a pre-trim plan set rather than the final compatibility-selected Repair intent. D2 moves the observation after local preflight/slot trimming and still before execution.

## 2. D2 data model

Added additive Scheduler evidence tables through `ensure_scheduler_evidence_schema()`:

- `ppl_round_scheduler_evaluations`;
- `ppl_round_scheduler_outcomes`;
- `ppl_round_scheduler_gate_reports`.

They do not modify the V2.2 core schema and do not bump the V3 round schema version.

Evaluation Ledger freezes:

- actual/shadow action and agreement;
- decision timestamp and batch;
- Scheduler policy version/hash;
- D2 Evidence-policy version/hash;
- Search/Repair backlog and oldest queue age;
- Search/Repair matured productivity 100/500;
- remote slot limit/reserved/free;
- fairness state and consecutive-action state;
- selected count/fingerprint;
- Search/Repair/shadow scores and decision reason;
- immutable facts JSON and decision JSON;
- replay PASS/hash/repetition count.

## 3. Policy identity

The earlier potential identity ambiguity is explicitly prevented.

Evaluated Scheduler identity:

- `scheduler_policy_version = V31_SCHED_SHADOW_002`;
- `scheduler_policy_hash = hash(effective Shadow Scheduler semantics)`.

D2 evidence/gate identity:

- `evidence_policy_version = V31_SCHED_EVIDENCE_001`;
- `evidence_policy_hash = hash(effective evidence/gate semantics)`.

The two version/hash pairs never point at different objects under one field name.

`V31_SCHED_SHADOW_001` was version-bumped because matured-cohort and hard-starvation semantics change Scheduler behavior even though it remains non-authoritative.

## 4. Matured productivity / right censoring

Search/Repair scores now use latest matured paid NEW_POST cohorts.

Right-censored and excluded from productivity denominator:

- RUNNING;
- SUBMITTED/STALE_RUNNING while unresolved;
- UNCERTAIN_SUBMISSION;
- COMPLETE Search without resolved classification;
- COMPLETE Repair without durable repair verdict.

Mature zero-yield terminal observations include deterministic failed/missing outcomes such as REMOTE_NOT_FOUND/FAILED/INVALID/ERROR.

This prevents a burst of newly posted remote RUNNING simulations from artificially depressing Search or Repair productivity.

## 5. Actual outcome evaluation

Only the action actually executed receives observed outcome attribution.

Available durable outcomes include:

- COMPLETE;
- READY/PPL success;
- Near-Pass / repairable evidence labels;
- distinct mature families;
- family winner facts;
- Repair resolved verdicts;
- TARGET_PASS;
- IMPROVED;
- ACCEPT;
- effective Simulation ratio, explicitly defined as `matured NEW_POST with simulation_status=COMPLETE / matured NEW_POST`.

If the Shadow recommendation differs from the actual action, the unexecuted side is written only as:

`COUNTERFACTUAL_PROXY`, `observed_outcome=false`.

No D2 code claims a true counterfactual result.

## 6. Deterministic replay

D2 can reconstruct `ShadowSchedulerSnapshot` from immutable `facts_json` and replay the pure scheduler N times.

Replay requires the same Scheduler policy hash. A hash mismatch fails with `SCHEDULER_REPLAY_POLICY_IDENTITY_MISMATCH` rather than silently replaying under changed semantics.

Every Evaluation Ledger row stores replay PASS, decision hash and repetition count.

## 7. Starvation/fairness and runtime boundary

D1 soft fairness penalty was insufficient as a proof of no-starvation.

D2 adds a hard Shadow-only rule:

- if Search and Repair both have backlog;
- a research slot is available;
- current completed research streak reaches configured `max_consecutive_same_action`;
- Shadow recommendation must switch to the opposite research side.

Stress harness covers:

- Search permanently dominant while Repair backlog exists;
- Repair permanently dominant while Search backlog exists;
- slot limit = 1;
- all slot capacity reserved by Remote work;
- 429/WAIT boundary;
- Check backlog boundary;
- Discovery wait boundary.

429, Check, Discovery and Auth remain Runtime obligations. They are not expected-value inputs and do not compete with Search/Repair yield.

## 8. Scheduler Safety Gate

Formal activation-eligibility checks exist for:

- minimum observations;
- minimum Search samples;
- minimum Repair samples;
- deterministic replay;
- starvation;
- slot safety;
- no-repost attestation;
- recovery-safety attestation;
- policy identity.

D2 intentionally leaves all three production sample thresholds `null` in `ppl_round_v31.yaml`.

Therefore even when all safety tests pass, current activation status remains:

`INELIGIBLE_THRESHOLDS_UNSET`.

This is intentional. D2 does not manufacture an evidence threshold from guesswork and cannot activate D3.

## 9. Safe fallback model

Future adaptive-scheduler errors classified for `PHASE_COMPATIBILITY` fallback:

- SCHEDULER_EXCEPTION;
- INVALID_DECISION;
- POLICY_MISMATCH;
- SLOT_CONFLICT;
- MISSING_IDENTITY.

Still fail closed/global halt:

- DUPLICATE_POST_RISK;
- DURABLE_IDENTITY_CONFLICT;
- SIM_KEY_IDENTITY_CONFLICT;
- DB_CORRUPTION;
- CORE_INVARIANT_FAILURE.

D2 only defines/tests this fallback classification. It does not activate adaptive execution.

## 10. Safety invariants re-verified

- duplicate Simulation POST path unchanged;
- UNCERTAIN no-repost path unchanged;
- durable `simulation_url` path unchanged;
- `sim_key` generation unchanged;
- Q/S/R policy does not enter Simulation identity;
- Search/Repair/Qualification strategy still has no direct HTTP POST;
- D2 Evidence layer has no HTTP POST or workflow-transition primitive;
- DB durable-truth failures remain fail closed;
- recoverable endpoint/runtime failures remain B-layer obligations;
- run_0005 not opened/resumed;
- no live run_0006 WorldQuant canary executed;
- `2Y Sharpe >= 1.58` absent from active source/config.

## 11. Validation

Collected after D2: `948` tests.

Sanitized-source runnable: `933 passed / 0 failed`.

Historical production-DB-bound unavailable: `15`, unchanged from C_COMPLETE:

- 12 tests in `test_ppl_foundation.py` requiring root `alpha_results.db` / production snapshots;
- 1 execution-hash historical integration requiring root production DBs;
- 1 Phase-5 real production snapshot assertion;
- 1 Phase-7 CLI preview requiring root production runner DB.

Non-overlapping regression groups:

- Compatibility: `247 passed`, 1 production-DB-bound deselected;
- Phase 2.1–10B: `320 passed`, 2 production-DB-bound deselected;
- Production/Remote/Repair: `149 passed`;
- V3.1 A/B/C/D1/D2: `134 passed`;
- V3 Round Orchestrator: `83 passed`;
- total runnable: `933 passed`.

Focused D1+D2: `24 passed`.

`compileall`: PASS.

`machine_lib_V2_1.py` SHA256 unchanged:
`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

`ROUND_SCHEMA_VERSION`: `4`, unchanged.

Live WorldQuant requests during validation: `0`.

## 12. Development mistake / prevention record

- Do not report planned/predicted implementation as completed code before tool/file verification. This happened in the interrupted chat before the fresh C_COMPLETE audit; it was a communication error, not a source-package bug.
- Do not overload Scheduler policy identity with Evidence-policy identity. Always persist the two version/hash pairs separately.
- Do not bump `ROUND_SCHEMA_VERSION` merely because an independent additive D2 evidence table is added. Core round-schema semantics remain version 4.
- Do not treat a soft fairness score penalty as proof of no-starvation; use an explicit bounded guard and stress validation.
- Do not fingerprint Repair intent before slot trimming when the actual executable plan set can still shrink.

## 13. Next stage

Do not start D3 automatically.

First accumulate real D2 Shadow evidence under `run_0006`-era operation, inspect disagreement/matured productivity/fairness data, and then choose explicit activation sample thresholds from observed evidence. Only after that and explicit approval may D3 run a constrained adaptive canary.
