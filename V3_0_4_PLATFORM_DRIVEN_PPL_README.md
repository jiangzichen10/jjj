# V3.0.4 — Platform-Driven PPL Classification & Repair

## Scope

V3.0.4 changes only derived PPL interpretation, repair ranking, parser aliases, and telemetry/reporting. It does not change the V2.1 simulation engine or safety boundary.

Unchanged:
- `machine_lib_V2_1.py`
- `ppl_rules.yaml` and the existing execution hash contract
- Total logical NEW Simulation POST budget = 2000
- SEARCH = 1600 / REPAIR = 400
- Batch size = 40, concurrency = 4, 70/30 exploit/explore
- Resume-first, cache-first, family dedup, PostGate, rolling dataset discovery
- No automatic Submit / PowerPoolSelected / PATCH / DELETE

## Classification model

The classifier separates:

1. PPL fixed gates
   - Sharpe >= 1.0. Sharpe < 1.0 => `PPL_TERMINAL_FAIL`.
   - Turnover 1%-70%. Turnover failure is repairable.
   - Sub-universe uses platform `/check` outcome and is repairable.
   - Power Pool correlation uses platform `/check` outcome and is repairable. A platform PASS is authoritative even when numeric value appears beyond the displayed cutoff because platform exemptions (for example Sharpe advantage) may already be applied.
   - Operator/data-field counts are retained as insurance diagnostics only in this release; no automatic repair is generated for them.

2. Current Theme-specific repair signals (configuration only)
   - `HT_TURNOVER`, fallback limit 0.20
   - `HT_HIGH_TURNOVER_RETURNS_RATIO`, fallback limit 0.75
   - Any new `HT_*` check is preserved as `UNMAPPED_THEME_SIGNAL` but is not automatically a blocker or repair target until added to the theme config.

3. Non-PPL diagnostics
   - Regional GLB Sharpe checks, PROD_CORRELATION, ordinary SELF_CORRELATION, FITNESS, 2Y Sharpe, Cluster, Osmosis, Competition, Pyramid, Regular submission, Concentrated Weight, Data Diversity, etc.
   - FAIL/WARNING is recorded but does not count as a PPL blocker and does not spend Repair Budget.

4. Final Theme outcome
   - `MATCHES_THEMES` is a final platform outcome, not an early-discard rule.
   - PASS + fixed gates satisfied => `PPL_TECHNICALLY_READY`.
   - WARNING/FAIL => inspect fixed/theme repair signals; do not discard merely because the final theme is not matched yet.

Automation ignores Description checks and `PURE_POWER_POOL_THEME`; those do not affect automatic PPL research/repair classification.

## PPL statuses

- `PPL_TERMINAL_FAIL`
- `PPL_FIXED_REPAIRABLE`
- `PPL_THEME_REPAIRABLE`
- `PPL_FIXED_AND_THEME_REPAIRABLE`
- `PPL_THEME_UNRESOLVED`
- `PPL_CHECK_UNRESOLVED`
- `PPL_TECHNICALLY_READY`

Repair priority:
- HIGH: max repair gap <= 5%
- MEDIUM: 5%-10%
- LOW: > 10% or unquantified blocker

Only HIGH/MEDIUM repairable candidates are eligible for automatic Repair-Budget ranking. LOW is retained for research but is not selected automatically.

## Compatibility evidence labels

For V3 online learning only:
- HIGH repair => `STRONG_NEAR_PASS`
- MEDIUM repair => `NEAR_PASS`
- `PPL_TECHNICALLY_READY` => `PPL_SUCCESS`

This keeps the existing adaptive-ranking stage weights compatible while the primary report uses the new PPL statuses.

## Existing run_0005

The first five batches remain valid. Rebuild reuses the durable Simulation facts and persisted `/check` facts and recalculates only derived classifications/telemetry.

After overlay, run:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
python ppl_runner.py --round-status --run-id run_0005
```

Expected safety effects of rebuild:
- network_requests = 0
- simulation_posts = 0
- check_requests = 0
- submit_requests = 0
- power_pool_selected_requests = 0

For the user's current live Batch-5 database, budget facts should remain:
- current_batch = 5
- search_consumed = 200
- repair_consumed = 0
- remaining_total = 1800

New report:
- `reports/round_run_0005/ppl_classification.csv`
- flat compatibility copy: `reports/round_run_0005_ppl_classification.csv`

Do not start Batch 6 until the rebuilt status/classification output is reviewed.

## Validation performed

- Full test suite was run in three non-overlapping groups: 613 tests passed total.
- Python compileall passed.
- `machine_lib_V2_1.py` SHA256 remained:
  `0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762`
- Migration/rebuild tested on the available uploaded run_0005 Batch-1 database snapshot:
  - policy upgrade succeeded
  - DB integrity = ok
  - current_batch/search/repair/post counters unchanged
  - rebuild reported 0 network / 0 simulation POST / 0 check / 0 submit / 0 PowerPoolSelected
- The available standalone database is an older Batch-1 snapshot; the user's live Batch-5 database must be validated after overlay with the two commands above.
