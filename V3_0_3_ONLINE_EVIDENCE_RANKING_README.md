# WorldQuant BRAIN V3.0.3 — Online Evidence Ranking Hotfix

Purpose: improve SEARCH productivity for the in-progress V3 round without rebuilding the project or touching V2.2 execution semantics.

## What changes

1. V3_RANK_002 / V3_ALLOC_003
   - EXPLOIT and EXPLORE use separate scores.
   - EXPLOIT contains no exploration/novelty bonus.
   - EXPLORE alone rewards under-tested Dataset / Operator / Window directions.

2. Online evidence progressively overrides static discovery priors.
   - Only current-round paid SEARCH `NEW_POST` + `COMPLETE` facts drive the adaptive evidence layer.
   - Evidence hierarchy:
     - Dataset
     - Operator
     - Dataset × Operator
     - Operator × Window
     - Dataset × Operator × Window
   - Evidence stages, from shallow to deep:
     - Local Gate PASS
     - PRE-TAG resolved
     - PPL Near-Pass
     - PPL Strong Near-Pass
     - PPL Success / protected winner
   - Small samples are shrinkage-weighted; a 2/2 or 3/3 result cannot immediately dominate the whole round.

3. Window learning is no longer static.
   - Windows are learned from real round outcomes; no hard-coded preference for 3/4/5/etc.
   - A new Dataset can borrow operator/window evidence until its own Dataset × Operator × Window sample becomes informative.

4. PPL Near-Pass semantics are now PPL-specific.
   - `PROD_CORRELATION`, `SELF_CORRELATION`, `POWER_POOL_SELF_CORRELATION`, `FITNESS`, `2Y Sharpe`, regional quality checks are diagnostic-only for PPL Near-Pass classification.
   - `POWER_POOL_CORRELATION` remains a true PPL hard check.
   - Theme hard checks with no quantifiable gap (for example unresolved/failed THEME_MATCH) do not get incorrectly labeled Near-Pass.
   - Diagnostic failures remain recorded for research and quality ranking.

5. Reporting
   - New `reports/<round_id>/search_productivity.csv`.
   - Explicit rates per paid completed SEARCH POST:
     - `local_pass_per_post`
     - `pretag_per_post`
     - `ppl_near_per_post`
     - `ppl_strong_near_per_post`
     - `ppl_success_per_post`
   - Dimensions: Dataset, Operator, Dataset×Operator, Operator×Window, Dataset×Operator×Window.
   - `workflow NEAR_PASS` remains backward-compatible in the DB, but status semantics now call it `WORKFLOW_NEAR_THRESHOLD`; it is not the PPL repair pool.

## What does NOT change

- `machine_lib_V2_1.py` is not included and must not be modified.
- `ppl_rules.yaml` is not changed.
- `ppl_plan_v3.yaml` is not changed.
- 2000 total logical NEW Simulation POST budget is unchanged.
- SEARCH 1600 / REPAIR 400 is unchanged.
- Batch size 40 is unchanged.
- 70% exploit / 30% explore is unchanged.
- Rolling Dataset Discovery remains enabled.
- Family dedup, Cache-first, Resume-first and PostGate remain unchanged.
- No automatic Submit / PowerPoolSelected / PATCH / DELETE is added.

## In-progress run upgrade

V3.0.3 explicitly allows the audited additive V3.0.2 -> V3.0.3 round-policy migration. It rejects unrelated policy drift.

After copying the hotfix files over the existing V3.0.2 project, first run:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
```

Expected safety properties:

- `network_requests = 0`
- `simulation_posts = 0`
- `check_requests = 0`
- `submit_requests = 0`
- `power_pool_selected_requests = 0`
- local writes are telemetry/report/policy-version backfill only

Then run:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

Confirm the existing budget is unchanged before any new execution.

For the production canary, run only one new batch:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 1
```

For the current run this should start with Resume-first, then the first periodic Rolling Dataset Discovery (because five SEARCH batches already completed), and then Batch 6 under V3_RANK_002.

## Validation performed

- 53 focused V3/Near-Pass tests pass after the final changes.
- 152 affected Phase-5/Phase-9/Production-Repair/V3/Near-Pass tests pass.
- The broader suite was run in split groups during development; 600 tests were passing before the final narrow semantic cleanup, and all directly affected groups were rerun afterwards.
- V3.0.2 -> V3.0.3 migration was executed locally against a copy of the supplied run_0005 database snapshot:
  - DB integrity: `ok`
  - round policy upgraded: `true`
  - Simulation POSTs: `0`
  - network requests: `0`
  - budget unchanged in the copied round
  - `search_productivity.csv` generated successfully
- `machine_lib_V2_1.py` SHA-256 remains:
  `0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762`

## Production evidence used to design V3_RANK_002

From the supplied Batch 1-5 report bundle (paid completed NEW_POST scope):

- `techindi_model × ts_mean`: 5 attempts, 5 Local Gate PASS, 1 true PPL Near-Pass.
- `techindi_model × raw`: 6 attempts, 0 Local Gate PASS.
- `ts_mean window=3`: 10 attempts, 9 Local Gate PASS, 1 PPL Near-Pass.
- `ts_mean window=4`: 7 attempts, 6 Local Gate PASS.

With the new scoring on that evidence, a representative `techindi_model × ts_mean(window=3)` candidate receives a materially higher EXPLOIT score than `techindi_model × raw`, while the 30% EXPLORE lane still preserves new directions.
