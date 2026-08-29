# WorldQuant BRAIN V3.0.4g

Upgrade target: V3.0.4f -> V3.0.4g.

This hotfix keeps V3_RANK_003, the 2000/1600/400 budgets, concurrency=4, cache/resume behavior, the stale 404/410 guard, and the terminal FAIL resume guard unchanged. `machine_lib_V2_1.py` is not included and must remain unchanged.

## 1. Manual-finalization queue refresh

New Alphas do NOT receive a duplicate extra Check. Their normal PRE_TAG `/check` remains the admission evidence.

Existing rows already published in `manual_finalization_queue.csv` are refreshed automatically every 10 completed batches (batch 10, 20, 30, ...). The periodic refresh is GET-only and excludes candidates already freshly checked in that same batch.

Manual refresh command:

`python ppl_runner.py --refresh-manual-finalization --run-id run_0005`

The command reads the currently published `manual_finalization_queue.csv`, deduplicates by Alpha ID, skips protected/submitted families, performs GET-only PRE_TAG `/check`, persists the new Check facts, reclassifies, and rewrites the reports. It performs no Simulation POST, PATCH, Submit, PowerPoolSelected, or DELETE.

If the newest refresh is unresolved/PENDING/error, an older resolved Check is no longer allowed to keep the Alpha in READY state; classification becomes `PPL_CHECK_UNRESOLVED` until a later resolved Check is obtained.

## 2. PPC strategy policy

Platform `POWER_POOL_CORRELATION` PASS/WARNING/FAIL remains preserved as the platform fact. V3.0.4g adds a separate local strategy filter:

- PPC <= 0.50: strategy PASS.
- 0.50 < PPC < 0.65: Sharpe must be strictly > 2.00 to enter `PPL_READY_FOR_MANUAL_FINALIZATION` / `PPL_TECHNICALLY_READY`.
- PPC >= 0.65: `PPL_STRATEGY_REJECT_HIGH_PPC`, even if the platform grants a Sharpe-based correlation exemption.
- Missing numeric PPC when a candidate would otherwise be READY: `PPL_CHECK_UNRESOLVED`.

The thresholds are configurable in `ppl_round_v3.yaml` under `ppl_classification.manual_finalization.ppc_strategy`.

## 3. Manual queue hygiene

Protected/submitted candidates, Alpha IDs, and signal families are excluded from `manual_finalization_queue.csv` while their historical classification facts remain available elsewhere.

The queue now includes PPC audit fields such as `ppc`, `platform_ppc_outcome`, `ppc_policy_band`, `ppc_strategy_result`, `last_check_at`, and `last_check_session_id`.

## 4. Recommended first command after overlay

Because the existing V3.0.4f queue may contain stale Checks, do NOT rebuild reports first if you want to refresh every row currently visible in that queue. After overlay, run directly:

`python ppl_runner.py --refresh-manual-finalization --run-id run_0005`

Then run:

`python ppl_runner.py --round-status --run-id run_0005`

## 5. Safety / verification

- No automatic final Submit.
- No automatic PowerPoolSelected.
- No PATCH/PUT/DELETE property writes.
- Manual refresh performs GET-only `/check` after login.
- Simulation POST budgets are untouched by manual queue refresh.
- `machine_lib_V2_1.py` expected SHA256 remains:
  `0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762`
- Regression suite run in three groups: 169 + 267 + 208 = 644 passed.
- Final targeted V3.0.4g classifier/orchestrator tests: 72 passed.
- `compileall`: PASS.
