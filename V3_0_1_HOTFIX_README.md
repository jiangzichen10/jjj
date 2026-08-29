# WorldQuant BRAIN V3.0.1 Research Telemetry Hotfix

This hotfix is designed for an existing V3 production round that is paused after Batch 1.
It does not modify `machine_lib_V2_1.py` and does not require re-running completed simulations.

## Fixes

1. Backfills live PRE-TAG numeric metrics from durable `raw_value_json` / `effective_limit_json`
   when normalized numeric columns are null. This restores HT Ratio, PP Corr, Prod Corr,
   Sub-universe and 2Y Sharpe values in `simulation_ledger.csv`.
2. Separates raw workflow lifecycle `NEAR_PASS` counts from the actual V3 PPL Near-Pass queue.
3. Adds explicit failure-matrix scopes: `BATCH_NEW_POST`, `BATCH_CACHE`, `ROUND_CUMULATIVE`,
   and `HISTORICAL_BASELINE`.
4. Renames round-status read-only counters under `status_query_side_effects` to avoid confusing
   them with cumulative round execution totals.
5. Adds `--rebuild-round-reports --run-id <run_id>` to rebuild telemetry from durable local facts
   without network I/O or Simulation POST.

## Safe update procedure

Before updating, keep the backup of the current `ppl_runner.db`, `alpha_results.db`, report directory,
and audit log.

Copy only the hotfix code files over the existing V3 project. Do not replace your databases,
credentials, logs, or report history.

Then run:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
```

This rebuild is local-only. It performs zero BRAIN network requests and zero Simulation POSTs.
It may update V3 telemetry/report tables and regenerate report files from existing durable facts.

Verify status:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

Expected first-canary invariants remain:

- post_attempted = 40
- post_confirmed = 40
- post_uncertain = 0
- post_consumed = 40
- search_consumed = 40
- repair_consumed = 0
- remaining_total = 1960

Only after verifying those values should the same round be resumed.
