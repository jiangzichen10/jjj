# V3.1 Progress 04 — B2A Unattended Auth / Check Queue / Due-Time Control

Baseline: `V3.1-B1` on frozen `v3.0.4o` source  
Date: 2026-08-27  
Checkpoint: `V3.1-B2A`  
Status: DEVELOPMENT CHECKPOINT COMPLETE — NOT PRODUCTION READY

## Scope completed

1. Added durable `ppl_check_work` queue for PRE_TAG checks.
2. Continuous PRE_TAG work performs at most one GET per due scheduler cycle; pending checks are requeued instead of sleeping in the legacy semantic-poll loop.
3. Check outcomes are scoped:
   - 429 -> `WAIT_RATE_LIMIT` with durable Retry-After;
   - network/5xx -> `WAIT_NETWORK` with bounded backoff;
   - 401/403 -> `WAIT_AUTH`;
   - resolved -> durable check evidence + workflow advancement;
   - deterministic non-2xx -> check-work terminal failure without Simulation repost.
4. Added `recover_waiting_auth()` as the V3.1 Auth Coordinator boundary.
   - one coordinator call performs one `ensure_session()` refresh;
   - success releases all durable WAIT_AUTH remote/check work;
   - failure preserves WAIT_AUTH and schedules retry;
   - it never creates a Simulation POST.
5. Added durable `ppl_endpoint_waits` control-plane state for auth/endpoint wait evidence.
6. Added `due_snapshot()` so WAIT sleep duration can follow the earliest durable Remote/Check due-time instead of always using fixed `idle_wait_seconds`.
7. Search and async Remote completions now hand local-gate-pass PRE_TAG work to the Check Queue in Continuous mode.
8. Continuous Round Repair also queues PRE_TAG work instead of running the synchronous check loop.
9. Legacy V3.0.x synchronous PRE_TAG semantics remain unchanged when Continuous/nonblocking mode is disabled.
10. v3.0.4o SQLite fail-closed diagnostics remain unchanged.

## Safety invariants retained

- no duplicate Simulation POST;
- `sim_key` identity unchanged;
- durable `simulation_url` resume-first semantics unchanged;
- UNCERTAIN remains no-auto-repost;
- core DB write failures remain fail-closed;
- check queue is GET-only;
- auth coordination may POST only through the existing narrowly authorized BRAIN authentication path;
- `machine_lib_V2_1.py` is unchanged.

## Files added

- `ppl_engine/continuous_check.py`
- `ppl_engine/continuous_control.py`
- `tests/test_v31_unattended_b2.py`
- `V3_1_PROGRESS_04.md`
- `V3_1_CHANGESET_04.json`
- `V3_1_B2A_SHA256SUMS.txt`

## Files changed

- `ppl_engine/store.py`
- `ppl_engine/continuous_policy.py`
- `ppl_engine/round_orchestrator.py`
- `ppl_engine/production_repair.py`
- `ppl_round_v31.yaml`

## Test evidence

- Static compile: PASS
- New B2A tests: `6 passed`
- V3.1 B1 + B2A + A1/Foundation: `42 passed`
- Production Repair + B1/B2A compatibility: `63 passed`
- V3 Round + lifecycle/foundation + SQLite diagnostics: `115 passed`
- v3.0.4o remote resolver/SQLite subset: `46 passed`
- V3 Round orchestrator alone: `83 passed`
- Legacy Production Repair/PPC subset remains passing.

One legacy CLI preview test can fail in the sanitized package if it expects the excluded root production `ppl_runner.db`; this is an environment-fixture dependency, not a B2A code failure. The frozen v3.0.4o production baseline remains the prior full `808 passed / 0 failed` run.

## Not yet completed

B2A does not yet make V3.1 production-ready. Remaining B2 work includes:

1. Discovery endpoint-level nonblocking cooldown/retry instead of legacy nested retry sleeps.
2. Converting every remaining recoverable global guard into scoped WAIT/quarantine.
3. Integrating durable endpoint waits into all control-plane HTTP paths.
4. More exhaustive Check Queue lifecycle/classification tests with realistic platform fixtures.
5. Queue-aware graceful idle for all remaining fixed-wait branches.
6. Report degradation/retry separation.
7. Long-running synthetic soak tests across Remote + Check + Auth queues.

Do not replace frozen v3.0.4o production runner with this checkpoint yet.
