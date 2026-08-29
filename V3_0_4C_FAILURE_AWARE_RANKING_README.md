# V3.0.4c — Failure-Aware Adaptive Ranking

This is a code-only hotfix for the existing V3 production-research round. It is designed to be overlaid on the existing project and to continue the same run without replaying historical Simulation POSTs.

## Why this hotfix exists

Batch 6 exposed a ranking defect: paid failures did not contribute negative online evidence, so a Dataset×Operator combination with repeated failures could score similarly to an untested combination, while a high static prior could still place an unproven combination in the 70% EXPLOIT share. In the observed Batch 6, an unproven `pv30 × ts_mean` pocket consumed 12 paid SEARCH POSTs and all returned Sharpe below 1.

V3.0.4c makes paid failure informative and separates empirical exploitation from explicit exploration.

## Changes

### Failure-aware online evidence

Current-round SEARCH / NEW_POST / COMPLETE facts are re-evaluated under the current PPL fixed-gate policy. Historical `local_gate` labels are not reused, so retired High-Turnover Theme rules such as a 20% turnover floor cannot poison current GLB Liquid ranking.

Evidence now tracks:

- attempts
- signal_viable
- local_pass
- fixed_repairable
- terminal_fail
- pretag_resolved
- ppl_near_pass
- ppl_strong_near_pass
- ppl_success

Terminal failures receive a negative score. A combination with enough attempts and zero viable signals receives an additional zero-viable penalty.

### EXPLOIT proof gate

After the first paid-search bootstrap batch, a Dataset×Operator combination may enter EXPLOIT only when the current round has at least:

- 2 paid attempts, and
- 1 viable signal with Sharpe >= the current PPL minimum and valid structure.

Unproven combinations remain eligible for EXPLORE, not EXPLOIT. The initial round bootstrap still works from static priors when there is no paid round evidence yet.

### Exploration concentration caps

Unproven EXPLORE candidates are capped by:

- maximum 15% of a nominal batch from one unproven Dataset, and
- maximum 4 candidates from one unproven Dataset×Operator combination.

Backfill is restricted to evidence-qualified EXPLOIT combinations. If there are not enough empirically supported candidates, the paid batch may intentionally contain fewer than 40 NEW_POSTs instead of silently spending the remainder on unproven candidates.

### Failure-aware Dataset cooldown

Rolling Dataset Discovery can now cool down weak datasets even when a refresh admits no replacement dataset, provided the active pool remains above the configured minimum. A dataset becomes weak only after sufficient current-round paid attempts and a low viable-signal rate. Up to 3 weak datasets may cool at one refresh.

### Better batch auditability

`batch_###.json` now includes selection decisions showing:

- EXPLOIT / EXPLORE / BACKFILL
- exploit score
- explore score
- online evidence score
- static-prior contribution
- Dataset×Operator attempts
- viable count
- exploit eligibility and gate reason

This makes future batch allocation directly auditable without reconstructing selection order.

## Policy versions

- Ranking: V3_RANK_003
- Dataset discovery: V3_DATASET_002
- Telemetry: V3_TELEMETRY_003
- Allocation: V3_ALLOC_003 (unchanged)
- PPL classification: V3_PPL_CLASS_002 (unchanged)

The V3.0.4b -> V3.0.4c policy migration is narrowly allow-listed. Budget, batch size, exploration fraction and execution semantics cannot drift through this migration.

## Preserved safety constraints

Unchanged:

- total logical budget 2000
- SEARCH 1600 / REPAIR 400
- batch size 40 nominal
- concurrency 4
- 70% EXPLOIT / 30% EXPLORE target
- Resume-first
- PostGate
- cache reuse
- family dedup and protected-family skip
- no automatic Submit
- no automatic PowerPoolSelected
- no PATCH
- no DELETE
- `machine_lib_V2_1.py` is not included or modified

## Upgrade procedure for run_0005

Overlay the files in this package onto the existing project directory, then run only:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
python ppl_runner.py --round-status --run-id run_0005
```

The rebuild is local/report telemetry work only and must report zero network requests and zero Simulation POSTs.

Before another live batch, verify the existing durable state remains approximately:

- current_batch = 6
- post_consumed = 240
- search_consumed = 240
- repair_consumed = 0
- remaining_total = 1760
- protected families = 3

Do not run Batch 7 until the rebuilt status has been reviewed.
