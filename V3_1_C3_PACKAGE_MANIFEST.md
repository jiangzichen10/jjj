# V3.1-C3 Package Manifest

Checkpoint: V3.1-C3 Safe-Checkpoint Qualification Policy Reload
Date: 2026-08-28
Baseline: V3.1-C2 Qualification Compatibility Extraction
Production ready: NO
Live WorldQuant requests during C3: 0

Core status:
- durable active Qualification version/hash/payload is persisted for Continuous runs;
- restart restores the durable active Qualification policy before recovery/classification;
- Qualification-only edits activate only at a safe checkpoint with no unfinished batch and before new allocation;
- changed Qualification semantics require an explicit policy-version bump;
- non-Qualification policy drift is rejected by the C3 controller and does not replace the last known-good policy;
- successful reload emits durable from/to version/hash attribution and leaves Simulation identity unchanged;
- repeated identical rejected candidates are telemetry-idempotent;
- no new PPL business threshold was introduced;
- the hypothetical 2Y Sharpe >= 1.58 example remains absent;
- `machine_lib_V2_1.py` unchanged.

Validation:
- 897 tests collected;
- 882 sanitized-source runnable tests passed / 0 failed;
- 15 historical production-DB-bound tests unavailable by design;
- compileall PASS.

Sanitization:
- no production SQLite databases/WAL/SHM;
- no credentials.txt/key.txt;
- no .git/.pytest_cache/__pycache__/pyc runtime artifacts;
- no logs/reports/backup runtime directories in the package.

Hashed source/package files (excluding this manifest, the C3 SHA file, and C3 source-list file): 228
