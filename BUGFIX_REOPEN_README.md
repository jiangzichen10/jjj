# run_0005 Repair Selector P0 + Controlled Reopen Fix

## Included fixes

1. `ppl_engine/round_orchestrator.py`
   - Fixes the `preview_rescue()` contract mismatch: the selector now accepts the current `recommended_strategy` output and the future `recommendation` mapping.
   - Adds `REPAIR_PREVIEW_BLOCKED` audit records instead of silently swallowing `ConfigError` / `ValueError` during repair preview.
   - Adds a narrow local-only recovery helper for rounds falsely completed as `ROUND_NO_SAFE_CANDIDATE`.

2. `ppl_runner.py`
   - Adds explicit recovery CLI:

```powershell
python ppl_runner.py --reopen-round-after-bugfix --run-id run_0005 --confirm-bugfix-reopen
```

The command is local-only and does not log in, make network requests, POST simulations, reset budgets, rewrite batches, delete candidates, or delete repair plans.

## Recovery guard

The reopen command only accepts a round/run satisfying all of these:

- Round status = `COMPLETED`
- Round phase = `DONE`
- Round stop reason = `ROUND_NO_SAFE_CANDIDATE`
- Run status = `COMPLETED`
- `post_uncertain = 0`
- No `RUNNING` round batch
- Explicit `--confirm-bugfix-reopen`

It changes only the execution state required to continue Repair:

- Round: `COMPLETED/DONE` -> `PAUSED/REPAIR`
- Round stop reason -> `BUGFIX_REOPEN_REPAIR_SELECTION`
- Round `completed_at` -> NULL
- Run: `COMPLETED` -> `PAUSED`

It preserves `current_batch`, `search_consumed`, `repair_consumed`, candidates, plans, sim_keys and existing batches.

For the uploaded snapshot, a temporary-copy validation confirmed:

- current_batch: 43 -> 43
- search_consumed: 743 -> 743
- repair_consumed: 16 -> 16
- candidates: unchanged
- repair plans: unchanged
- batches: unchanged
- network requests: 0
- simulation posts: 0

## Tests performed

- `py_compile`: PASS
- Dedicated selector regression: `1 passed`
- Repair/Near-Pass/Round targeted suite: `163 passed`
- Full offline suite was started and reached about 65% with no failures before the sandbox command timeout; it did not finish, so this is not claimed as a full-suite PASS.

## Recommended production sequence

After backing up the project and database, overwrite the included files, then:

```powershell
python -m py_compile .\ppl_runner.py .\ppl_engine\round_orchestrator.py
```

```powershell
python -m pytest -q -p no:cacheprovider --disable-warnings .\tests\test_repair_selector_neutralization_regression.py
```

Then reopen locally:

```powershell
python ppl_runner.py --reopen-round-after-bugfix --run-id run_0005 --confirm-bugfix-reopen
```

Verify:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

Expected core state:

- status = `PAUSED`
- phase = `REPAIR`
- current_batch = `43`
- search_consumed = `743`
- repair_consumed = `16`
- stop_reason = `BUGFIX_REOPEN_REPAIR_SELECTION`

Only after that, run one controlled Repair batch:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 1
```

That next batch should be Batch 44.
