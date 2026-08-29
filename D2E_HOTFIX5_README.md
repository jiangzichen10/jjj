# WorldQuant BRAIN V3.1 D2E HOTFIX5

## Purpose

HOTFIX5 is a cumulative replacement for HOTFIX1-4. It keeps the D2E run identity and databases unchanged and closes the Simulation Settings identity gap discovered during the first real `run_0006` execution.

The design follows Route B: production execution remains strict; old tests/fixtures are upgraded to the real full-settings contract instead of weakening production code to support incomplete fake settings.

## Production contract

Every executable V3.1 candidate must carry the complete canonical BRAIN Simulation settings object:

- `instrumentType`
- `region`
- `universe`
- `delay`
- `decay`
- `neutralization`
- `truncation`
- `pasteurization`
- `testPeriod`
- `unitHandling`
- `nanHandling`
- `language`
- `visualization`

The complete object is the durable Simulation identity used by candidate persistence, `sim_key`, cache identity and the actual HTTP POST. The compact V2.1 execution scope is only a transient projection for legacy delegation and is never a second settings identity.

Before execution, the adapter re-materializes the V2.1 payload and requires it to equal the durable full settings. Before direct POST, the existing identity guard requires the recomputed Simulation key to equal the durable candidate `sim_key`. Any mismatch fails closed before network submission.

## Included fixes

1. HOTFIX1: historical COMPLETE cache facts reconcile forward from `PLANNED` instead of attempting illegal workflow jumps.
2. HOTFIX2: local analysis is idempotent and does not regress `PRE_TAG_CHECK_PENDING` or later workflow states.
3. HOTFIX3: Continuous direct POST uses complete BRAIN Simulation settings; the four known HTTP 400 client-payload rejects are eligible for safe retry only when no remote Simulation could have been created.
4. HOTFIX4/5 settings architecture:
   - complete canonical settings are the single durable identity;
   - `_execution_scope()` is only a projection;
   - V2.1 execution groups are keyed by complete settings identity, so differences such as `decay` cannot collapse into one group;
   - candidate creation and Repair child materialization validate the full settings contract;
   - Production Repair and Phase10B revalidate materialized Repair children;
   - test fakes and fixtures now implement/use the complete settings contract instead of `{}` or partial payloads where a real executable candidate is expected.
5. Legacy tests that incorrectly depended on project-root production SQLite files now use isolated temporary database fixtures where applicable.

## Files to replace/add

Production:

- `ppl_engine/candidate_factory.py`
- `ppl_engine/continuous_remote.py`
- `ppl_engine/live_execution.py`
- `ppl_engine/phase10b.py`
- `ppl_engine/production_repair.py`
- `ppl_engine/repair_engine.py`
- `ppl_engine/settings_contract.py` (new)
- `ppl_engine/simulation_adapter.py`

Tests:

- all files under this package's `tests/` directory.

No database, credential, YAML policy, or `machine_lib_V2_1.py` file is included.

## Verification performed

- `python -m compileall -q ppl_engine ppl_runner.py tests`: PASS
- Full repository test suite collected: 976 tests
- Split execution to avoid wall-time timeout:
  - chunk 1: 359 passed
  - chunk 2: 381 passed
  - chunk 3: 236 passed
  - total: 976 passed, 0 failed
- `machine_lib_V2_1.py` remains unchanged:
  - SHA256 `58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

## Install

Copy the package contents over the existing `WorldQuant_BRAIN_v3.1_D2E_READY` project, preserving the directory structure. Do not restore or replace `ppl_runner.db` or `alpha_results.db`.

Then run:

```powershell
pytest -q tests/test_production_logging_phase2.py tests/test_ppl_phase10a.py tests/test_ppl_phase10b.py tests/test_v31_remote_queue_b1.py tests/test_v31_d2e_compatibility_evidence.py
```

After tests pass, query the existing run before allowing new POSTs:

```powershell
python ppl_runner.py --round-status --run-id run_0006
```

Do not create a new `run_0006` and do not restore the pre-hotfix database backup unless a separate recovery decision is made from durable state.
