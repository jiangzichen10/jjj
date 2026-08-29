# V3.1 Progress 12 — Scheduler Evidence Calibration Readiness

Date: 2026-08-28
Status: `D2 CALIBRATION TOOLING COMPLETE / REAL EVIDENCE PENDING`
Production status: `NOT PRODUCTION READY`
Authoritative Scheduler: `PHASE_COMPATIBILITY`
Adaptive Scheduler: `SHADOW_ONLY / authoritative=false`
D3: `NOT STARTED`

## 1. Baseline

Only baseline used: `WorldQuant_BRAIN_v3.1_D2_COMPLETE.zip`.

Baseline SHA256:
`bd4ddfd2a9386378586d514620e358645a69f20085197a6bed9a5a58d547e9e7`

D2 baseline remained intact before this checkpoint. `machine_lib_V2_1.py` SHA256 remained:
`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`.

## 2. Fact corrected before implementation

The clean D2 package intentionally contains no runtime `ppl_runner.db` and therefore no real `run_0006` Scheduler observations.

Consequently, activation thresholds cannot yet be derived from real Shadow evidence. This checkpoint does not invent `minimum_observations`, `minimum_search_samples`, or `minimum_repair_samples` and does not begin D3.

## 3. Added read-only calibration report

New module:
- `ppl_engine/scheduler_evidence_report.py`

New CLI action:
- `python ppl_runner.py --scheduler-evidence-report --run-id run_0006`

The command:
- requires an explicit run id;
- opens SQLite with `mode=ro` and `PRAGMA query_only=ON`;
- does not call `ensure_scheduler_evidence_schema()`;
- creates no table and performs no migration;
- does not configure/append the audit log merely by being observed;
- does not require or permit Simulation POST;
- does not mutate Scheduler/Evidence policy or activation thresholds.

A direct CLI smoke test compared the runner DB SHA256 before and after the report. The hashes were byte-for-byte identical.

## 4. Evidence summarized

For the current durable Scheduler/Evidence identity, the report includes:

- total/current/excluded identity observations;
- same-version/different-hash identity conflicts;
- actual action counts;
- shadow action counts;
- agreement/disagreement counts and matrix;
- 95% Wilson interval for agreement rate;
- replay PASS/FAIL counts;
- matured/censored NEW_POST counts and maturity ratio;
- actual Search COMPLETE / READY / Near-Pass yield and Wilson intervals;
- actual Repair resolved / TARGET_PASS / IMPROVED / ACCEPT yield and Wilson interval;
- Search-vs-Repair decision margin distribution;
- both-backlog + free-slot evidence coverage;
- hard-starvation-guard observations;
- zero-free-slot evidence and observed WAIT safety.

Unexecuted alternatives remain D2 counterfactual proxies only. The calibration report does not infer a true counterfactual outcome.

## 5. Threshold policy

Current D2 configuration remains unchanged:

- `minimum_observations: null`
- `minimum_search_samples: null`
- `minimum_repair_samples: null`

The calibration report intentionally emits:

- `automatic_threshold_recommendation=false`
- `recommended_thresholds=null`

Reason:
`REAL_D2_EVIDENCE_PRECISION_AND_EXPLICIT_RISK_TOLERANCE_REQUIRED`

Real observations must first accumulate. Statistical precision/coverage can then support a threshold proposal instead of a guessed fixed number.

## 6. Validation

After adding the read-only report:

- collected tests: `956`;
- sanitized-source runnable: `941`;
- historical production-DB-bound unavailable: `15`, unchanged;
- Compatibility: `247 passed` + 1 known production-DB-bound unavailable;
- Phase 2.1–10B: `320 passed` + 2 known production-DB-bound unavailable;
- Production/Remote/Repair: `149 passed`;
- V3.1 A/B/C/D1/D2 + calibration: `142 passed`;
- V3 Round Orchestrator: `83 passed`;
- D1/D2/calibration focused: `32 passed`;
- new calibration focused: `8 passed`;
- Foundation production snapshot tests: `12 unavailable by design` because clean source has no root `alpha_results.db`;
- `compileall`: PASS.

A single all-runnable pytest invocation was attempted but exceeded the 180-second execution window at ~53%; the non-overlapping groups above completed independently and account for all 941 sanitized runnable tests.

## 7. Safety invariants rechecked

- `ROUND_SCHEMA_VERSION=4`;
- `machine_lib_V2_1.py` SHA256 unchanged;
- `.shadow_action` execution branch references in `round_orchestrator.py`: 0;
- active source/config contains no `2Y Sharpe >= 1.58` production rule;
- real Search/Repair execution remains `PHASE_COMPATIBILITY`;
- no live WorldQuant request was made;
- no run_0005 resume/open for execution;
- D3 not started.

## 8. Roadmap dependency found before live evidence collection

There is one sequencing conflict that must not be hidden:

- D2 says real Shadow evidence must accumulate before D3 thresholds/canary approval;
- the earlier roadmap says F, after D3/D4/E, is the first real `run_0006` WorldQuant canary.

Both cannot be literal. Real D2 evidence requires some live compatibility-mode execution before D3. That execution must keep `PHASE_COMPATIBILITY` authoritative and `SHADOW_ONLY` non-authoritative, so it is an evidence-collection run rather than an adaptive canary.

Before starting live execution, confirm whether the pre-D3 evidence collection should use the eventual `run_0006` production run or an isolated evidence run. This checkpoint deliberately does not make that run-identity decision silently.

Once the evidence run exists, inspect it with:

`python ppl_runner.py --scheduler-evidence-report --run-id <evidence_run_id>`

The resulting JSON is the basis for deciding whether sample precision, Search/Repair balance, disagreement behavior and fairness/slot coverage are sufficient to propose explicit Safety Gate thresholds. Do not activate D3 merely because data exists. Threshold proposal and D3 authorization remain separate decisions.
