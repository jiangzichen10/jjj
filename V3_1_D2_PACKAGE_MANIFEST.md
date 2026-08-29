# V3.1-D2 Package Manifest

Date: 2026-08-28
Checkpoint: `V3.1-D2 Scheduler Evidence Gate`
Status: `OFFLINE COMPLETE / NOT PRODUCTION READY`

## Baseline

Only baseline used for development:
`WorldQuant_BRAIN_v3.1_C_COMPLETE.zip`
SHA256: `2398830f92ec19c4d6647ba8a457711ba8d44b6cda79df63af9ecb1529d0c2b4`

## New D2 files

- `ppl_engine/scheduler_evidence.py`
- `tests/test_v31_scheduler_evidence_d2.py`
- `V3_1_PROGRESS_11.md`
- `V3_1_CHANGESET_11.json`
- `V3_1_D2_TEST_MATRIX.txt`
- `V3_1_D2_PACKAGE_MANIFEST.md`
- `V3_1_D2_SOURCE_FILE_LIST.txt`
- `V3_1_D2_SHA256SUMS.txt`

## Modified active files

- `ppl_engine/scheduler_shadow.py`
- `ppl_engine/round_orchestrator.py`
- `ppl_round_v31.yaml`
- `tests/test_v31_scheduler_shadow_d1.py`
- `V3_1_ARCHITECTURE_CHANGE_MAP.md`

## Safety boundary

- real Scheduler remains `PHASE_COMPATIBILITY`;
- adaptive Scheduler remains `SHADOW_ONLY`;
- all D2 gate/evaluation outputs are `authoritative=false`;
- activation sample thresholds are intentionally unset;
- no D3 canary is activated;
- no live WorldQuant request is required or executed by this checkpoint;
- no production DB/runtime state is included in the clean package.

## Validation

- 948 collected;
- 933 sanitized runnable passed;
- 15 historical production-DB-bound unavailable by design;
- focused D1+D2 24 passed;
- compileall PASS;
- machine_lib hash unchanged;
- round schema version remains 4.
