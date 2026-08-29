# V3.1-C Complete Package Manifest

Date: 2026-08-28
Checkpoint: `V3.1-C POLICY / STRATEGY LAYER OFFLINE COMPLETE`
Baseline: V3.1-D1 Shadow checkpoint; D1 remains non-authoritative.

## Package purpose

This is the clean source checkpoint after closing the V3.1 C layer: execution-identity separation, pure Search/Repair strategy extraction, dedicated Search/Repair policy, and atomic Qualification/Search/Repair safe-checkpoint reload.

## Validation

- Collected tests: 930
- Sanitized runnable: 915 passed / 0 failed
- Historical production-DB-bound: 15 unavailable by design
- Compatibility: 247 passed
- Phase: 320 passed
- Production/Remote/Repair: 149 passed
- V3.1 A/B/C/D1: 116 passed
- V3 Round Orchestrator: 83 passed
- C4/C5/C6 focused: 27 passed
- compileall: PASS
- live WorldQuant requests: 0
- machine_lib_V2_1.py SHA256: `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## File inventory before SHA manifest

- Total files before SHA manifest: 252
- `.ipynb`: 3
- `.json`: 52
- `.md`: 42
- `.py`: 108
- `.txt`: 38
- `.yaml`: 8
- `<noext>`: 1
- Final ZIP will add `V3_1_C_COMPLETE_SHA256SUMS.txt` as one additional file.

## Explicit exclusions

The package must not contain production/runtime state: `credentials.txt`, root production `*.db`, `*.db-wal`, `*.db-shm`, `.git`, `.pytest_cache`, `__pycache__`, `*.pyc`, logs, reports, or backup runtime trees.

## Active development boundary

- C layer: offline complete.
- D1: Shadow-only, non-authoritative.
- D2/D3/D4: not yet complete.
- Long-run telemetry hardening: not yet complete.
- Real `run_0006` production canary: not started.
