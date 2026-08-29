# V3.1-D2E Compatibility Evidence Run Infrastructure Package Manifest

Date: 2026-08-28
Status: `D2E INFRASTRUCTURE READY / REAL PLATFORM RUN NOT STARTED / NOT PRODUCTION READY`

## Baseline

`WorldQuant_BRAIN_v3.1_D2_CALIBRATION_READY.zip`

Baseline SHA256:
`8d8077ffe48c938f359047f9861d396eb0642b75cfd33de015d79df1f4dbce69`

## Formal route decision

The roadmap is now:

`D2 offline evidence infrastructure`
→ `D2E run_0006 Compatibility Evidence`
→ `Evidence Calibration Report`
→ `D2 Safety Gate Review`
→ `D3 run_0007 Adaptive Canary`
→ `D4`
→ `E`
→ `Production Candidate`.

D2E is a real-platform evidence/control run, not an Adaptive Canary.

## run_0006 durable role

`run_0006` is permanently defined for this experiment as:

- research mode: `COMPATIBILITY_EVIDENCE`;
- Scheduler authority: `PHASE_COMPATIBILITY`;
- Shadow Scheduler: `SHADOW_ONLY`;
- Adaptive control: `DISABLED`;
- authority transition allowed: `false`;
- automatic evidence stop: `false`.

The code rejects:

- generic/default Continuous creation if the resolved next run would be `run_0006`;
- D2E creation without explicit `--run-id run_0006`;
- D2E creation using another run ID;
- resume of `run_0006` whose durable identity is not D2E;
- resume-time D2E authority-lock drift.

## Added

- `ppl_engine/research_run_mode.py`
- `ppl_round_v31_d2e.yaml`
- `tests/test_v31_d2e_compatibility_evidence.py`
- `V3_1_PROGRESS_13.md`
- `V3_1_D2E_TEST_MATRIX.txt`
- `WorldQuant_BRAIN_V3.1_HANDOVER_2026-08-28_D2E.md`
- `V3_1_CHANGESET_13.json`
- `V3_1_D2E_PACKAGE_MANIFEST.md`
- final D2E source-file list and SHA256 manifest generated during packaging

## Modified

- `ppl_engine/round_orchestrator.py`
- `ppl_engine/scheduler_evidence_report.py`
- `V3_1_ARCHITECTURE_CHANGE_MAP.md`

## Explicitly unchanged

- `ROUND_SCHEMA_VERSION = 4`;
- Simulation identity / `sim_key` semantics;
- durable `simulation_url` semantics;
- UNCERTAIN no-repost behavior;
- Qualification policy business rules;
- Search/Repair policy business semantics;
- Q/S/R safe-checkpoint atomic bundle semantics;
- Runtime obligation vs research-allocation boundary;
- `machine_lib_V2_1.py`;
- Adaptive Scheduler remains non-authoritative.

## Pre-registered maturation semantics

Before any real run_0006 evidence is observed:

- terminal failed/missing Simulation → mature zero-yield;
- Search COMPLETE waits for resolved durable classification;
- Repair COMPLETE waits for durable repair verdict;
- RUNNING / SUBMITTED / UNCERTAIN / unresolved COMPLETE remain right-censored;
- no arbitrary elapsed-minute maturation threshold is preloaded.

A future time-based censoring fallback, if ever needed, requires evidence and explicit review.

## Evidence monitor semantics

The read-only calibration report may summarize:

- matured/censored Search and Repair samples;
- actual/shadow agreement and disagreement;
- queue/fairness/slot-pressure coverage;
- Search/Repair outcome yield;
- durable maturation-latency proxy distributions;
- 100/500 productivity-window stability statistics.

It does not:

- choose activation thresholds;
- mutate policy;
- stop or pause the run automatically;
- declare READY_FOR_CALIBRATION automatically;
- activate Adaptive scheduling.

## Validation

- Collected: `963` tests;
- sanitized-source runnable: `948`;
- runnable result: `948 passed / 0 failures` using complete non-overlapping groups;
- historical production-DB-bound unavailable: `15`, unchanged in nature;
- D1+D2+Calibration+D2E focused: `39 passed`;
- new D2E focused: `7 passed`;
- `compileall`: PASS;
- live WorldQuant requests: `0`;
- `ROUND_SCHEMA_VERSION`: `4`;
- `.shadow_action` execution references in `round_orchestrator.py`: `0`;
- `machine_lib_V2_1.py` SHA256 unchanged:
  `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

The 948 runnable figure is the sum of complete non-overlapping regression groups. A single combined command is not claimed because the available execution window timed out before that combined invocation finished.

## Next real action

No live run is started by this package.

When explicitly authorized, the next real-platform run is `run_0006` under `ppl_round_v31_d2e.yaml`. It must remain a compatibility-authoritative control/evidence run for its entire lifetime. D3 is not started by, or inside, run_0006.
