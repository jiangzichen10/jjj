# UNCERTAIN REPAIR RETRY PATCH

Purpose: allow one human-authorized retry of REPAIR `UNCERTAIN_SUBMISSION` without consuming an extra logical repair-strategy attempt.

For run_0005 Batch 44 after overwriting the patch files:

```powershell
python ppl_runner.py --retry-uncertain-repair --run-id run_0005 --batch-no 44 --confirm-duplicate-risk
```

Expected local-only result for the current four records:
- released_count: 4
- post_uncertain_active_after: 0
- network_requests: 0
- simulation_posts: 0
- original_post_budget_preserved: true
- strategy_attempts_consumed_by_release: 0

Then verify:

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

Then resume one batch:

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 1
```

Safety semantics:
- first uncertain POST budget is NOT refunded;
- retry POST consumes normal additional remote/repair budget;
- same repair plan / same expression / same settings / same sim_key is retried;
- uncertain release itself does not consume a rescue-strategy attempt;
- each sim_key can be authorized for uncertain retry only once;
- a second uncertain outcome fails closed and cannot be released again by this command;
- original audit/telemetry history is retained; no schema change and no machine_lib_V2_1.py change.

Offline verification in build workspace:
- py_compile: PASS
- targeted tests: 105 passed
- complete suite executed in two deterministic groups because a single-process all-suite invocation stalled in the container after ~74% despite the same files passing independently:
  - group A: 564 passed
  - group B: 209 passed
  - total collected/passed: 773 / 773
- live/network requests: 0
- run_0005 was validated only against temporary copies of the uploaded databases; uploaded originals were not mutated by the validation.
