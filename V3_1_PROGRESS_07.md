# V3.1 Progress 07 — C2 Qualification Compatibility Extraction

Date: 2026-08-27
Checkpoint: V3.1-C2
Baseline: V3.1-C1 Strategy/Scheduler Compatibility
Status: OFFLINE COMPLETE
Production ready: NO
Live WorldQuant requests in this checkpoint: 0

## 1. Objective

Extract a stable, declaration-driven Qualification boundary from the current PPL classifier without changing current Alpha qualification behavior.

C2 acceptance rule:

> same candidate + same metrics + same `/check` facts + same approved current policy -> same existing PPL classification; the new evaluator may add attribution, but must not silently invent or change a business rule.

## 2. Implemented

### 2.1 Declaration-driven Qualification evaluator

Added `ppl_engine/qualification_policy.py`.

It provides:

- immutable `QualificationRuleDeclaration`;
- immutable `QualificationRuleEvaluation`;
- `QualificationEvaluationBundle`;
- `QualificationPolicySnapshot`;
- declaration compiler/validator;
- current-rule evaluator;
- stable `QualificationResult` projection;
- Qualification-only policy hashing.

The module owns no SQLite connection, RunnerStore, HTTP session, state transition, Simulation POST or DELETE.

### 2.2 Explicit rule roles

`ppl_round_v31.yaml` now separates current rules into:

- `PLATFORM_HARD_RULE`;
- `LOCAL_QUALIFICATION_RULE`;
- `LOCAL_STRATEGY_RULE`;
- `DIAGNOSTIC_WARNING`.

Rule declarations reference values under the existing `ppl_classification` section using `threshold_from`/`names_from`. Threshold values are not duplicated.

### 2.3 Current approved rules only

C2 does not add a new business rule.

In particular, the earlier conversational `2-year Sharpe >= 1.58` example is not present in `qualification_integration` and is not an active rule.

`LOW_2Y_SHARPE` and `TWO_YEAR_SHARPE` retain their existing `non_ppl_diagnostics` role.

### 2.4 Missing-fact semantics

Rule declarations explicitly choose `on_missing` behavior.

Examples:

- Sharpe missing after Simulation completion -> `UNRESOLVED`, not an invented Sharpe FAIL;
- optional insurance diagnostics -> `NOT_APPLICABLE` when missing;
- platform check absence -> `UNRESOLVED` when the current policy requires evidence.

### 2.5 Platform fact vs local strategy separation

Platform `POWER_POOL_CORRELATION` PASS/WARNING/FAIL remains a platform fact.

The local PPC strategy (`clean_max`, `mid_max`, `mid_min_sharpe`) is evaluated independently as `LOCAL_STRATEGY_RULE`; it never rewrites platform PASS into platform FAIL.

### 2.6 Real Continuous path integration

For `run_profile=CONTINUOUS_RESEARCH`, `classify_run()` now:

1. loads the V3.1 Qualification snapshot;
2. runs the unchanged public V3 PPL classifier;
3. evaluates the declaration-driven rule set independently;
4. projects the result through the stable Qualification contract;
5. attaches policy version/hash, evaluator version, platform facts, local strategy facts, blockers, unresolved facts and diagnostics.

The public `classify_ppl_candidate()` output itself remains unchanged for compatibility.

`preview_rescue()` and PPC controlled-branch evaluation now use the config-aware classification policy loader, so Continuous does not silently fall back to `ppl_round_v3.yaml`.

### 2.7 Qualification runtime snapshot

C2 deliberately prevents uncontrolled hot reload.

The first Continuous Qualification load creates one process-local immutable snapshot. Editing YAML afterward does not change current process semantics until an explicit `force_reload=True` call is made.

This hook is reserved for C3 safe-checkpoint reload. C2 does not call it automatically.

Malformed declarations or mismatch between:

- `qualification_integration.policy_version`, and
- `policy_versions.qualification`

fail closed before a result is attributed to an ambiguous policy identity.

### 2.8 Qualification identity does not mutate Simulation identity

A Qualification-only policy hash is computed independently.

Tests prove a Qualification policy change can change the Qualification hash while the same expression/settings continue to produce the same `simulation_key`.

No SimulationSpec field was added or changed in C2.

## 3. Explicit non-changes

- no PPL threshold change;
- no two-year Sharpe qualification rule;
- no Search ranking change;
- no Repair ranking/planning change;
- no adaptive Search-vs-Repair scheduling yet;
- no automatic policy hot reload yet;
- no `machine_lib_V2_1.py` change;
- no production DB modification;
- no live WorldQuant request.

## 4. Validation

Current source collects `887 tests`.

Sanitized-source non-overlapping validation:

- other compatibility group: `247 passed`, `1` production-DB test deselected;
- Phase 2.1/3/4/5/6/7/8/9/9.1/10A/10B: `320 passed`, `2` production-DB tests deselected;
- Production/Remote/Repair group: `149 passed`;
- V3.1 Foundation/A1/B/C1/C2 group: `73 passed`;
- V3 Round Orchestrator: `83 passed`.

Unique sanitized runnable total:

- `872 passed`;
- `0 failed`;
- `15` historical production-DB-bound tests unavailable by design.

Static compile:

- `python -m compileall -q ppl_engine tests ppl_runner.py machine_lib_V2_1.py` -> PASS.

`machine_lib_V2_1.py` SHA256 remains:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## 5. What C2 completion means

The project now has a real Qualification contract and declaration-driven current-rule evaluator on the Continuous classification path, while retaining the proven V3 classifier as the final compatibility composer.

This is sufficient to start controlled policy versioning/reload work without coupling policy edits to Simulation execution.

It does not yet mean arbitrary YAML rule changes can be applied to a running process automatically.

## 6. Next

Proceed to V3.1-C3:

1. reload Qualification policy only at an explicit safe checkpoint;
2. validate old/new policy version and hash before activation;
3. reject unversioned or malformed changes;
4. persist a durable `QUALIFICATION_POLICY_RELOADED` audit event;
5. ensure already-submitted Simulations keep their original Simulation identity;
6. ensure new classifications/decisions carry the new Qualification version;
7. preserve restart/recovery semantics.

After C3, proceed to V3.1-D adaptive Search/Repair scheduler/productivity work.
