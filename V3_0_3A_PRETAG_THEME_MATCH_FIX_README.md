# V3.0.3a — PRE_TAG Theme Match Classification Fix

Purpose: correct PPL Near-Pass classification before manual PowerPoolSelected tagging.

## Bug fixed

V3.0.3 treated `THEME_MATCH=WARNING` with no numeric value/limit as an unquantified PPL hard blocker during PRE_TAG. This could demote a valid PPL Near-Pass to NORMAL even though the user had not yet performed the manual PowerPoolSelected/tagging step.

## New phase-aware rule

- PRE_TAG:
  - `THEME_MATCH`, `MATCHES_THEMES`, `PURE_POWER_POOL_THEME` are deferred when not PASS.
  - They are recorded in `deferred_ppl_checks` with reason `PENDING_MANUAL_POWER_POOL_TAG`.
  - They do not increase `blocker_count` and do not demote PPL Near-Pass.
- POST_TAG:
  - The same theme-match checks become hard checks again.
  - WARNING/FAIL/UNKNOWN can block final PPL readiness.

Unchanged:
- `POWER_POOL_CORRELATION` remains a PPL hard check.
- `PROD_CORRELATION`, `SELF_CORRELATION`, `FITNESS`, `TWO_YEAR_SHARPE` remain diagnostic-only for PPL Near-Pass classification.
- No automatic Submit, PowerPoolSelected, PATCH or DELETE.
- No Simulation POST is performed by report rebuild.
- Round budgets, ranking policy, allocation policy, rolling discovery and repair budgets are unchanged.
- `machine_lib_V2_1.py` is not included or modified.

## Regression case

For `ak7jNMk2` facts:
- HT Ratio 0.681 / 0.75 WARNING -> one quantifiable blocker, gap 9.2%
- Power Pool Correlation 0.4868 / 0.50 PASS
- Sub-universe PASS
- Theme Match WARNING/null/null -> deferred at PRE_TAG
- Prod Correlation 0.7077 / 0.70 FAIL -> diagnostic-only

Expected classification: `NEAR_PASS`, blocker_count=1.

## Install

Copy the hotfix files over the existing V3 project. Then run:

    python ppl_runner.py --rebuild-round-reports --run-id run_0005
    python ppl_runner.py --round-status --run-id run_0005

Expected for the current Batch-5 state:
- project_version: v3.0.3a
- current_batch: 5
- post_consumed: 200
- search_consumed: 200
- repair_consumed: 0
- remaining_total: 1800
- ppl_near_pass.near: 1 (assuming no newer live facts changed the candidate)

Do not resume Batch 6 until this verification passes.
