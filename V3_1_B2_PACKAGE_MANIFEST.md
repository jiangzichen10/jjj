# V3.1-B2 Package Manifest

Checkpoint: `V3.1-B2`  
Date: 2026-08-27  
Baseline: frozen `v3.0.4o` source + V3.1 A/B development changes  
Status: V3.1-B unattended runtime offline-complete; overall V3.1 not production-ready.

Package rules:

- excludes production credentials;
- excludes root/runtime SQLite databases and WAL/SHM files;
- excludes logs/reports/cache/`__pycache__`/`.pytest_cache`;
- contains source, tests, configuration, historical documentation and V3.1 checkpoint records;
- `machine_lib_V2_1.py` remains unchanged from the audited v3.0.4o baseline;
- no live WorldQuant request was used during B2 closeout validation.

Validation summary:

- 868 tests collected;
- 853 runnable sanitized-source tests passed;
- 0 runnable-test failures;
- 15 production-DB-bound tests intentionally not runnable from the sanitized package;
- mixed unattended synthetic soak PASS;
- static compile PASS.

See:

- `V3_1_PROGRESS_05.md`
- `V3_1_B2_TEST_MATRIX.txt`
- `V3_1_CHANGESET_05.json`
- `V3_1_ARCHITECTURE_CHANGE_MAP.md`
- `V3_1_B2_SHA256SUMS.txt`
