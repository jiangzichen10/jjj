# V3.1-D1 Package Manifest

Checkpoint: V3.1-D1 Adaptive Scheduler Shadow
Date: 2026-08-28
Baseline: V3.1-C3 Safe-Checkpoint Qualification Policy Reload
Status: OFFLINE COMPLETE
Production ready: NO
Live WorldQuant requests during D1: 0

Core status:
- adaptive Search-vs-Repair scheduler exists in explicit `SHADOW_ONLY` mode;
- current `PHASE_COMPATIBILITY` scheduler remains authoritative;
- D1 cannot alter actual Search/Repair selection or execution;
- Search/Repair productivity uses latest 100/500 paid NEW_POST ledger observations;
- queue aging, fairness, backlog and remote-slot capacity are observational inputs;
- `SCHEDULER_SHADOW_DECISION` is durable/idempotent and bound to selected identity fingerprint;
- batch reports preserve shadow attribution;
- no Search/Repair/Qualification business rule was changed;
- no hypothetical two-year Sharpe rule was added;
- SimulationSpec/sim_key/no-repost semantics are unchanged;
- `machine_lib_V2_1.py` unchanged.

Validation:
- 903 tests collected;
- 888 sanitized-source runnable tests passed / 0 failed;
- 15 historical production-DB-bound tests unavailable by design;
- D1-specific: 6 passed;
- V3.1 group: 89 passed;
- V3 Round Orchestrator: 83 passed;
- compileall PASS.

Sanitization:
- no production SQLite databases/WAL/SHM;
- no credentials.txt/key.txt;
- no .git/.pytest_cache/__pycache__/pyc runtime artifacts;
- no logs/reports/backup runtime directories in the package.

Hashed source/package files (excluding this manifest, the D1 SHA file, and D1 source-list file): 236
