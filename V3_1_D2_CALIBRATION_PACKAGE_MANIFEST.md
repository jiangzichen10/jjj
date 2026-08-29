# V3.1 D2 Scheduler Evidence Calibration Readiness Package Manifest

Date: 2026-08-28
Status: `CALIBRATION TOOLING COMPLETE / REAL EVIDENCE PENDING / NOT PRODUCTION READY`

## Baseline

`WorldQuant_BRAIN_v3.1_D2_COMPLETE.zip`
SHA256: `bd4ddfd2a9386378586d514620e358645a69f20085197a6bed9a5a58d547e9e7`

## Added

- `ppl_engine/scheduler_evidence_report.py`
- `tests/test_v31_scheduler_evidence_report.py`
- `V3_1_PROGRESS_12.md`
- `V3_1_CHANGESET_12.json`
- `V3_1_D2_CALIBRATION_TEST_MATRIX.txt`
- `V3_1_D2_CALIBRATION_PACKAGE_MANIFEST.md`
- final source-file list and SHA256 manifest generated during packaging

## Modified

- `ppl_runner.py`
- `V3_1_ARCHITECTURE_CHANGE_MAP.md`

## Explicitly unchanged

- `ppl_round_v31.yaml` Scheduler/Evidence thresholds and policy versions;
- `ppl_engine/scheduler_shadow.py`;
- `ppl_engine/scheduler_evidence.py`;
- `ppl_engine/round_orchestrator.py`;
- `ROUND_SCHEMA_VERSION=4`;
- Simulation identity / `sim_key`;
- Q/S/R policy bundle semantics;
- `machine_lib_V2_1.py`;
- `PHASE_COMPATIBILITY` authority.

## CLI

Read-only evidence review:

`python ppl_runner.py --scheduler-evidence-report --run-id run_0006`

The command is intentionally observational and has no network/POST/threshold/activation side effect.

## Validation

- 956 collected;
- 941 sanitized runnable passed in non-overlapping groups;
- 15 historical production-DB-bound unavailable by design;
- 32 D1/D2/calibration focused passed;
- 8 new calibration focused passed;
- compileall PASS;
- no live WorldQuant requests;
- machine_lib hash unchanged;
- D3 not started.

## Live-run sequencing note

No live run is started by this package. D2 needs real Shadow evidence before D3, but the earlier roadmap also described F as the first real `run_0006` canary. That dependency must be resolved explicitly before live execution. Any pre-D3 evidence run must remain `PHASE_COMPATIBILITY` / `SHADOW_ONLY` and is not an adaptive canary.
