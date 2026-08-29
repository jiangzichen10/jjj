# V3.1 Progress 08 — C3 Safe-Checkpoint Qualification Policy Reload

Date: 2026-08-28
Baseline: V3.1-C2 Qualification Compatibility Extraction
Checkpoint: V3.1-C3
Status: OFFLINE COMPLETE
Production ready: NO
Live WorldQuant requests during C3: 0

## Goal

Make Qualification rules safely changeable during a long-lived Continuous run without:

- changing Simulation identity;
- mixing old/new Qualification semantics inside one unfinished batch;
- requiring restart for every rule edit;
- allowing an invalid edited YAML file to destroy the last known-good policy;
- changing Legacy V3.0.x behavior.

## Implemented

### 1. Durable active Qualification policy

Added `ppl_round_policy_state` in the additive V3 round coordination schema. It stores:

- `policy_type=QUALIFICATION`;
- active policy version;
- active policy hash;
- full Qualification payload (`qualification_integration` + `ppl_classification`);
- source path;
- activation batch number/time.

The durable payload, not the current filesystem YAML, is the active-policy source of truth after a restart.

### 2. Engine-side policy controller

Added `ppl_engine/policy_runtime.py`.

The Qualification evaluator remains DB/HTTP free. The runtime controller owns:

- initialization/restoration;
- safe-checkpoint validation;
- durable activation;
- runtime snapshot installation;
- reload/rejection events.

### 3. Safe checkpoint semantics

Reload is considered only when:

1. no crash-recovered unfinished batch remains;
2. no new Search/Repair batch has been allocated yet.

Therefore an unfinished batch always finishes/reconciles under the durable policy that was active when it existed.

### 4. Qualification-only scope

C3 may change only:

- `qualification_integration`;
- `ppl_classification`;
- `policy_versions.qualification`.

Any Search/Repair/Scheduler/lifecycle/discovery/batch-size/etc. drift is rejected by this controller and the previous active Qualification policy remains in force.

### 5. Version/hash rules

- unchanged policy hash -> no-op;
- changed Qualification hash + unchanged policy version -> reject (`POLICY_VERSION_BUMP_REQUIRED`);
- valid new version/hash -> activate;
- malformed rule declaration or version mismatch -> reject and continue with last durable policy.

Rejected candidate policies use deterministic event keys to avoid event-table spam on every Continuous scheduler cycle.

### 6. Restart behavior

At process start/resume, Continuous mode installs the durable active Qualification payload before classification/recovery work.

If YAML was edited while the process was down:

- unfinished recovered batch -> old durable policy stays active;
- after recovery reaches safe checkpoint -> candidate YAML may be validated/activated.

### 7. Attribution

A successful reload records `QUALIFICATION_POLICY_RELOADED` with:

- from/to version;
- from/to hash;
- checkpoint identity;
- controller version;
- `simulation_identity_unchanged=true`.

Batch-end snapshots also include active Qualification version/hash/activation batch.

### 8. Status/report interpretation

Status/report processes restore the durable active Qualification snapshot before calling Continuous classification, so reports do not accidentally use an unactivated edited YAML file.

## Explicit non-changes

- No PPL business threshold changed.
- No two-year Sharpe Qualification gate added.
- The earlier `2Y Sharpe >= 1.58` example is not a real rule and is not present.
- No Search ranking behavior change.
- No Repair planning behavior change.
- No adaptive Search/Repair arbitration yet.
- No Search/Repair/Scheduler hot reload yet.
- No SimulationSpec/sim_key change.
- No `machine_lib_V2_1.py` change.
- No live WorldQuant request.

## Validation

Collected: 897 tests.

Sanitized-source runnable:

- Compatibility: 247 passed; 1 production-DB-bound unavailable.
- Phase: 320 passed; 2 production-DB-bound unavailable.
- Production/Remote/Repair: 149 passed.
- V3.1 A/B/C1/C2/C3: 83 passed.
- V3 Round Orchestrator: 83 passed.

Total runnable: 882 passed / 0 failed.

Historical production-DB-bound unavailable by design: 15.

Static compile: PASS.

`machine_lib_V2_1.py` SHA256 remains:

`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## Bugs found during C3

### Round coordination schema version regression

Initial C3 implementation bumped `ROUND_SCHEMA_VERSION` from 4 to 5 solely for an additive coordination table. Existing V3 regression tests correctly caught this as a compatibility regression.

Correction:

- keep round coordination schema version at 4;
- create `ppl_round_policy_state` additively;
- preserve the existing V3 schema-version contract.

Prevention rule:

> Additive V3 coordination tables do not automatically justify changing the frozen round schema-version contract. Run `test_v3_round_orchestrator.py` immediately after any round-store schema edit.

### Rejected-policy event spam risk

A persistent invalid YAML candidate could have emitted one rejection event per scheduler cycle.

Correction:

- deterministic rejection event keys keyed by rejection reason + candidate/error digest.

Prevention rule:

> Any recoverable Continuous condition that can repeat every cycle must use idempotent/coalesced durable telemetry.

## Next

V3.1-D1 should make the scheduler arbitrate Search vs Repair using productivity + fairness/aging, but begin in compatibility/shadow mode before changing allocation behavior. No live production run_0006 canary yet.
