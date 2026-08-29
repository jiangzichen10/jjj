# WorldQuant BRAIN v3 — Build Audit

Build date: 2026-08-17

## Scope

v3 is an additive, reusable round-orchestration and research-telemetry layer on top of the audited V2.2 workflow. It does not replace the V2.1 simulation engine or the V2.2 candidate/check/repair facts.

This build adds full-process telemetry before the first real long production round so that later optimization can be based on complete denominators, decisions, batch evolution and reproducible configuration rather than only final winners.

## Safety baseline preserved

- `machine_lib_V2_1.py` was not modified.
- `ppl_rules.yaml` was not modified during the v3 orchestration/telemetry upgrade.
- `rescue_evidence.json` was not modified.
- The delivered `alpha_results.db` and `ppl_runner.db` remain byte-for-byte identical to the uploaded V2.2 audited snapshots.
- No real BRAIN Simulation POST was made during development or QA.
- v3 never performs automatic Submit, PowerPoolSelected, PATCH, PUT or DELETE.
- Cache-first, Resume-first, PostGate, SERVER SLOT GUARD, writer lock, V2.2 sim_key/settings validation and Repair Budget remain in the execution path.

## Preserved hashes

- `machine_lib_V2_1.py`: `0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762`
- `alpha_results.db`: `ffaac6fdbca0f3fddb8eb80aca90d869377a79083e4020ba99e67688a78b8ee0`
- `ppl_runner.db`: `6296110b176d37f0612912c012fa6d802f425702dfc7f3b30f99958bcd8ef254`
- `ppl_rules.yaml`: `692f0fe7c227eaba48dad7f04d5fe61d5e392bcf7ebdf34558ec4edb2ee8b223`
- `rescue_evidence.json`: `14026604c90d01af6c9c09fce634c699af25db2aa95b9aa79e4bf6f3e98c50a4`

## Database validation

- `alpha_results.db`: `PRAGMA integrity_check = ok`, 2 tables.
- `ppl_runner.db`: `PRAGMA integrity_check = ok`, 30 V2.2 tables in the delivered snapshot.
- The V2.2 core schema contract remains `SCHEMA_VERSION = 13`.
- v3 round schema is additive and currently `ROUND_SCHEMA_VERSION = 3`.
- v3 round/telemetry tables are created only when a v3 round is initialized; the packaged V2.2 database snapshot is not pre-migrated.

Additive v3 tables include:

- `ppl_rounds`
- `ppl_round_batches`
- `ppl_round_family_winners`
- `ppl_round_meta`
- `ppl_round_events`
- `ppl_round_candidate_decisions`
- `ppl_round_simulation_ledger`
- `ppl_round_snapshots`
- `ppl_round_manifests`

## v3 orchestration defaults

- Objective: `MAXIMIZE_DISTINCT_PPL_READY_SIGNAL_FAMILIES`
- Logical new Simulation POST hard limit: 2000
- Initial Search budget: 1600
- Repair Reserve: 400
- Batch size: 40 paid logical posts
- Exploration fraction: 30%
- GLB concurrency: 4, still bounded by existing server-slot guards
- Normal Near-Pass paid repair cap: 1 / family
- Strong Near-Pass paid repair cap: 2 / family
- One paid Initial Search representative per signal family
- Cache/Resume sibling variants can still be evaluated for free and compared before selecting the family winner
- One protected Family Winner per successful signal family

## Research telemetry design

The telemetry layer is network-free and additive. It records enough state to reconstruct why a long round behaved as it did.

### Event timeline

`ppl_round_events` records idempotent round events such as discovery, ranking, selection, cache/resume decisions, simulation intent/completion, gate/check results, family protection, phase changes and round stop/completion.

### Candidate decision denominator

`ppl_round_candidate_decisions` records the initial discovery universe plus per-batch selected/skipped decisions and scoring components. This prevents later analysis from confusing “tested more often” with “actually more productive”.

### Logical Simulation ledger

`ppl_round_simulation_ledger` stores one row per `(round_id, sim_key)` and distinguishes NEW_POST / CACHE / RESUME. It joins simulation facts, key metrics, PRE-TAG checks, repair lineage and family outcome into an analysis-friendly ledger.

### Batch snapshots

`ppl_round_snapshots` stores budget, candidate/family state and productivity snapshots after batches so later versions can study marginal yield and adaptive-allocation behavior over time.

### Reproducible manifest

`ppl_round_manifests` freezes code/config/rule hashes, simulation settings, budgets, runtime policy and policy-version IDs. Initial policy versions are:

- `V3_RANK_001`
- `V3_WINNER_001`
- `V3_ALLOC_001`
- `V3_REPAIR_001`
- `V3_FAMILY_001`
- `V3_TELEMETRY_001`

This allows future `_002` policy changes to be compared against old rounds without rewriting history.

## Protected existing success

The user-confirmed submitted Alpha `rK2ZVL93` seeds its signal family into the protected set:

`techindi_model/predicted_first_quantile_one_day_return_2/IDENTITY/NORMAL/TS_MEAN`

Therefore `ZYEVroY0`, `9qpNzjOr` and related same-family variants are not allowed to consume new v3 Simulation budget.

## Crash / resume design

Before delegating a real batch to V2.1, v3 durably records the exact planned POST and Resume sim_keys. On resume it rebuilds logical budget consumption from durable batch intent plus `alpha_results.db` facts.

- COMPLETE/RUNNING/SUBMITTED/UNCERTAIN facts count as logically consumed where appropriate.
- Existing RUNNING/SUBMITTED candidates are resumed before any new POST.
- A process failure before the network-intent checkpoint is recoverable with zero charge.
- A process failure after a POST-intent checkpoint but without any durable alpha fact is fail-closed as `ROUND_UNRESOLVED_POST_INTENT`; v3 will not automatically re-POST an ambiguous identity.
- Any durable `UNCERTAIN_SUBMISSION` or `AUTH_ERROR` pauses new round execution.
- Duplicate batch history cannot be overwritten, and the POST-intent update must match an existing durable batch row.
- Telemetry DB rows are durable truth; research CSV/JSON files are refreshed after each completed batch and can be regenerated after interruption.

## Research report bundle

In addition to legacy flat v3 reports, each round exports a nested bundle at `reports/<round_id>/`:

- `manifest.json`
- `summary.json`
- `summary.md`
- `timeline.jsonl`
- `simulation_ledger.csv`
- `candidate_decisions.csv`
- `batch_snapshots.jsonl`
- `ppl_family_winners.csv`
- `manual_tag_queue.csv`
- `near_pass_queue.csv`
- `repair_history.csv`
- `failure_matrix.csv`
- `budget_audit.csv`
- `candidates_final.csv`
- `batches/batch_XXX.json`

For `run_0005`, the corresponding round directory is normally `reports/round_run_0005/`.

## QA

Final regression suite: 588 / 588 passed in four coverage groups:

- 116 passed
- 109 passed
- 253 passed
- 110 passed

A single monolithic pytest invocation exceeded the container's long-command timeout, so the complete collection was validated in four groups.

The v3-specific suite now contains 23 tests. In addition to the original orchestration coverage, telemetry tests verify:

- round schema v3 telemetry tables;
- selected and skipped Candidate decisions;
- one Simulation ledger row per sim_key and durable origin;
- manifest and batch-snapshot persistence;
- idempotent mirroring of durable state transitions into timeline;
- full rich research report bundle generation.

The broader v3/V2.2 regression coverage still includes budget allocation, family dedup/protection, one-paid-search-per-family, free cache sibling comparison, budget clamp, winner transitions, observational round status, crash budget reconciliation, ambiguous POST-intent fail-closed behavior, pre-dispatch recovery, next Run ID selection, AUTH/UNCERTAIN global guard, durable batch intent and non-overwrite batch history.

## Offline smoke test

A smoke test was run against a temporary copy of `ppl_runner.db` with offline discovery and without Simulation authorization. It created `run_0005` / `round_run_0005` with:

- state `READY / SEARCH`
- total/search/repair budget `2000 / 1600 / 400`
- round schema version `3`
- 1 protected family
- 432 candidates from the legacy offline discovery snapshot
- 432 batch-0 Candidate discovery/decision records
- 6 initial round timeline events
- 1 manifest
- 0 Simulation-ledger rows because no Simulation was authorized
- 0 network requests
- 0 Simulation POSTs

The research report exporter was also smoke-tested against a temporary database/report directory and successfully produced the complete nested report bundle.

The old offline snapshot is intentionally not treated as sufficient coverage for a full 1600-search production round; production should normally use online read-only discovery.

## New/changed v3 files

- `ppl_engine/round_store.py`
- `ppl_engine/round_orchestrator.py`
- `ppl_engine/research_telemetry.py`
- `ppl_plan_v3.yaml`
- `ppl_round_v3.yaml`
- `tests/test_v3_round_orchestrator.py`
- `V3_README.md`
- `V3_BUILD_AUDIT.md`
- `VERSION.txt`
- `ppl_runner.py` (v3 CLI routing added; existing V2.2 commands retained)

## Expected first production Run ID

The delivered V2.2 snapshot contains `run_0001` through `run_0004`, so the next automatically created production round is expected to use `run_0005` unless the local database has changed after deployment.
