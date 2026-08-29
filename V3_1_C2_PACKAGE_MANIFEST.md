# V3.1-C2 Package Manifest

Checkpoint: V3.1-C2 Qualification Compatibility Extraction
Date: 2026-08-27
Baseline: V3.1-C1 Strategy/Scheduler Compatibility
Production ready: NO
Live WorldQuant requests during C2: 0

Core status:
- declaration-driven Qualification compatibility evaluator is on the real Continuous classification path;
- current V3 PPL classifier remains the final compatibility composer;
- Platform Hard / Local Qualification / Local Strategy / Diagnostic roles are explicit;
- missing-fact behavior is explicit;
- Qualification policy identity/hash is separate from Simulation identity;
- process-local policy snapshot prevents uncontrolled mid-batch hot reload;
- no new PPL business threshold was introduced;
- no hypothetical 2Y Sharpe >= 1.58 rule exists;
- `machine_lib_V2_1.py` unchanged.

Validation:
- 887 tests collected;
- 872 sanitized-source runnable tests passed / 0 failed;
- 15 historical production-DB-bound tests unavailable by design;
- compileall PASS.

Sanitization:
- no production SQLite databases/WAL/SHM;
- no credentials.txt/key.txt;
- no .git/.pytest_cache/__pycache__/pyc runtime artifacts.

Hashed source/package files (excluding this manifest, the C2 SHA file, and C2 source-list file): 220
