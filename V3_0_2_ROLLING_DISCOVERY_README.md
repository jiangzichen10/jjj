# WorldQuant BRAIN V3.0.2 Rolling Dataset Discovery Hotfix

V3.0.2 adds a durable, additive rolling Dataset discovery layer to an existing V3 production round.
It does not modify `machine_lib_V2_1.py`, does not restart `run_0005`, and does not replay the first 40
Simulation POSTs.

## New behavior

- Initial Dataset discovery remains unchanged.
- During SEARCH, V3 refreshes the Dataset pool after every 5 completed SEARCH batches by default.
- A refresh can also happen early when the remaining safe paid-family pool becomes small.
- Each refresh performs one read-only Dataset catalog discovery and probes a small number of previously
  unseen Datasets (`6` by default), then admits up to `3` that have usable Data Coverage / Field evidence.
- Existing Candidate, Simulation, Check, Family and Repair facts are never rebuilt or deleted.
- Newly admitted Dataset candidates are appended to the same run.
- The weakest non-protected ACTIVE Datasets are moved to `COOLDOWN` so paid Initial Search moves to new
  Dataset families. Cache/Resume facts remain reusable even for a cooled Dataset because they cost no new
  Simulation POST.
- A Dataset that already produced a protected winner is not cooled by the default policy.
- Previously seen Dataset IDs are not re-admitted within the same round.

Default policy:

```yaml
rolling_discovery:
  enabled: true
  refresh_every_search_batches: 5
  min_search_batches_before_refresh: 5
  max_refreshes: 12
  probe_new_datasets_per_refresh: 6
  admit_new_datasets_per_refresh: 3
  min_active_datasets: 10
  low_pool_trigger_families: 80
  low_pool_min_batches_since_refresh: 2
  cooldown_min_attempts: 8
  cooldown_checked_rate_max: 0.10
  preserve_success_datasets: true
  never_revisit_dataset_within_round: true
```

## Durable research records

V3.0.2 adds two V3-only coordination tables:

- `ppl_round_dataset_states`
- `ppl_round_dataset_refreshes`

and two research exports:

- `reports/round_<run_id>/dataset_pool_state.csv`
- `reports/round_<run_id>/dataset_refresh_history.csv`

Every refresh is also recorded in `timeline.jsonl` as `DATASET_REFRESH_STARTED` and
`DATASET_REFRESH_COMPLETE`.

## Existing run_0005 upgrade

The existing V3.0.1 round policy is upgraded additively. V3.0.2 permits only the audited policy change
that adds rolling Dataset discovery and changes the allocation policy version from `V3_ALLOC_001` to
`V3_ALLOC_002`. Any unrelated Round Policy drift is still rejected.

Run the local-only rebuild immediately after copying the hotfix code:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
```

This performs zero BRAIN network requests and zero Simulation POSTs. It also bootstraps the Dataset pool
state from the existing run. For the supplied first-canary DB snapshot, this produces 13 ACTIVE Datasets,
0 COOLDOWN Datasets and 0 refreshes while preserving:

- post_attempted = 40
- post_confirmed = 40
- post_uncertain = 0
- post_consumed = 40
- search_consumed = 40
- repair_consumed = 0
- remaining_total = 1960

Then verify:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

The status now includes `dataset_pool`.

No rolling refresh should happen immediately after Batch 1. With the default policy the first periodic
refresh is evaluated after five completed SEARCH batches, before Batch 6.

## Resume

After the local rebuild/status check, continue the same round. For a controlled validation, run four more
batches so the round reaches five completed SEARCH batches:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 4
```

Then inspect status and the Dataset refresh records. The next invocation will evaluate the first periodic
refresh before selecting Batch 6. If you want to observe the refresh as a separate canary, resume only one
additional batch after that check.

## Safety invariants

Unchanged:

- `machine_lib_V2_1.py` is not modified.
- Cache-first and Resume-first remain active.
- Logical sim_key identity and POST budget guards remain active.
- SERVER SLOT GUARD / writer lock / PostGate remain active.
- No automatic Submit, PowerPoolSelected, PATCH or DELETE.
- Rolling discovery uses GET-only `ReadOnlySession`.
