# V3.1-C1 Package Manifest

Checkpoint: V3.1-C1 Strategy/Scheduler Compatibility Integration
Date: 2026-08-27
Baseline: V3.1-B2 Complete
Production ready: NO
Live WorldQuant requests during C1: 0

Core status:
- C1 compatibility strategy adapters wired to real Continuous Search/Repair execution paths.
- Scheduler contract is the execution gate in PHASE_COMPATIBILITY mode.
- Existing Search/Repair selector behavior is intentionally preserved.
- No Qualification business rule change.
- No hypothetical 2Y Sharpe >= 1.58 rule.
- machine_lib_V2_1.py unchanged.

Validation:
- 874 tests collected.
- 859 sanitized-source runnable tests passed / 0 failed.
- 15 historical production-DB-bound tests unavailable by design.
- compileall PASS.

Sanitization:
- no production SQLite databases/WAL/SHM;
- no credentials.txt/key.txt;
- no .git/.pytest_cache/__pycache__/pyc runtime artifacts.

Hashed source/package files (excluding this manifest, SHA file, and source-list file): 212
