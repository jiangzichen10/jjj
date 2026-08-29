# V3.0.4b — GLB Liquid Theme Update

## Purpose

This hotfix updates the active V3 Power Pool research policy for the simplified GLB Liquid theme announced on 2026-08-19 and valid through 2026-09-30.

The user-provided announcement states that the ongoing Power Pool is simplified to GLB Liquid, that GLB / TOPDIV3000 alphas qualify, and that the High Turnover Returns Ratio constraint no longer applies.

This patch deliberately keeps `model110` exclusion unchanged for now, per the user's instruction. It also keeps the current run's D1 simulation setting unchanged.

## Main behavior changes

1. `HT_HIGH_TURNOVER_RETURNS_RATIO` is no longer a current-theme repair driver.
2. `HT_TURNOVER` is no longer a current-theme repair driver.
3. Legacy HT checks can still appear in `/alphas/{id}/check`; they are retained as observed evidence but do not create a PPL blocker or consume Repair Budget.
4. The current legacy theme gate requires only `THEME_MATCH` / `MATCHES_THEMES` as the platform final theme outcome.
5. The local pre-gate no longer rejects turnover below 20% unless a future active theme explicitly re-enables `HIGH_TURNOVER`.
6. Legacy HT-derived repair planning is disabled while the active theme does not require `HIGH_TURNOVER_RETURNS_RATIO`.
7. PPL fixed repair logic is unchanged: Turnover outside 1%-70%, Sub-universe failure, and platform Power Pool correlation failure/warning can still drive repair. Sharpe < 1 remains terminal.
8. Platform `/check` PASS/WARNING/FAIL remains authoritative. Numeric value/limit is used for repair distance only and never overrides platform PASS.

## Theme configuration

V3 round classification now has no configured theme-specific repair signal:

```yaml
theme_specific:
  theme_name: "GLB/D1 Liquid Power Pool Aug`26"
  valid_until: "2026-09-30"
  required_settings:
    region: GLB
    delay: 1
    universe: TOPDIV3000
  excluded_datasets:
    - model110
  repair_signals: {}
  capture_prefixes:
    - HT_
  observe_only:
    - MATCHES_CLASSIFICATION
    - HT_TURNOVER
    - HT_HIGH_TURNOVER_RETURNS_RATIO
```

The `model110` exclusion is intentionally unchanged in this patch.

## Historical run compatibility

This patch supports continuing the existing `run_0005` without replaying Batch 1-5.

The only execution-rule relaxation in `ppl_rules.yaml` is the current theme's live required-check list. The compatibility validator recognizes this narrowly audited change as:

`THEME_POLICY_RELAXATION_MATCH`

It does not authorize arbitrary execution drift. Simulation settings, budgets, dataset exclusion, expressions, POST semantics, cache/resume rules and safety controls remain unchanged.

Existing Simulation and `/check` facts remain Fact Truth. V3.0.4b only re-derives classification/repair labels from them.

Important: historical `MATCHES_THEMES=WARNING` facts may have been produced before the simplified theme took effect. A local report rebuild cannot turn an old platform WARNING into PASS without a new GET `/check`. Therefore `PPL_THEME_UNRESOLVED` after rebuild can be normal for old candidates. New/live checks use the current platform result.

## Safety invariants

Unchanged:

- Total budget: 2000
- SEARCH: 1600
- REPAIR reserve: 400
- Batch size: 40
- GLB concurrency: 4
- 70% exploitation / 30% exploration
- Resume-first
- Cache-first
- Family dedup / protected-family skip
- No automatic Submit
- No automatic PowerPoolSelected
- No PATCH
- No DELETE
- `machine_lib_V2_1.py` is not included or modified

Expected `machine_lib_V2_1.py` SHA256:

`0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762`

## Validation performed

Offline test suite was run in three groups:

- 242 passed
- 320 passed
- 56 passed
- Total: 618 passed

`python -m compileall -q ppl_engine ppl_runner.py` passed.

A V3.0.4a -> V3.0.4b migration was tested on a copied historical `run_0005` database snapshot:

- report rebuild succeeded
- round policy upgraded successfully
- `network_requests = 0`
- `simulation_posts = 0`
- `check_requests = 0`
- budget counters remained unchanged
- execution compatibility returned `THEME_POLICY_RELAXATION_MATCH`
- a resume preview with Simulation POST disabled succeeded and preserved the existing budget state

## Install / first verification

Overlay this code-only package into the existing V3.0.4a project directory.

Then run only:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
python ppl_runner.py --round-status --run-id run_0005
```

The rebuild is expected to be local-only and must show zero network / Simulation / Check / Submit / PowerPoolSelected requests.

Do not launch Batch 6 until the rebuilt classification and budget counters are reviewed.
