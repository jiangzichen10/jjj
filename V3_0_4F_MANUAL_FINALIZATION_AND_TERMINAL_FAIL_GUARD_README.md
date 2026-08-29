# V3.0.4f — Manual Finalization Queue + Terminal FAIL Resume Guard

This CodeOnly hotfix is designed to overlay V3.0.4d directly. It also includes
the V3.0.4e terminal-FAIL resume fix, so a separate V3.0.4e install is not
required.

## What changed

1. PRE-TAG Description warnings are phase-aware.
   - `POWER_POOL_DESCRIPTION_LENGTH` and `POWER_POOL_DESCRIPTION_FORMAT` are not
     PPL quality blockers.
   - If fixed PPL gates pass, there is no configured Theme repair blocker,
     `MATCHES_THEMES` is still PRE-TAG `WARNING/PENDING`, and Description checks
     are pending, classification becomes `PPL_READY_FOR_MANUAL_FINALIZATION`.
   - This status does not consume Repair budget and does not pretend the Theme
     has failed.

2. A copy-ready Power Pool Description is generated locally.
   - Exact headings: `Idea:`, `Rationale for data used:`,
     `Rationale for operators used:`.
   - Drafts are deterministic and use only recorded field/dataset/operator data.
   - No PATCH/PUT is performed. The user manually copies the description, saves
     Properties, and clicks Check Submission.

3. New reports:
   - `reports/round_run_xxxx_manual_finalization_queue.csv`
   - `reports/round_run_xxxx/manual_finalization_queue.csv`
   Each row contains `generated_description`, `manual_action`, and diagnostics.

4. Duplicate Alpha-ID safety for the manual queue.
   - If one Alpha ID maps to multiple distinct candidate expressions, the queue
     emits one row with `VERIFY_ALPHA_CODE_BEFORE_DESCRIPTION` and refuses to
     auto-draft from an ambiguous mapping.

5. V3.0.4e terminal remote failure guard is included.
   - Resume-first GET returning HTTP 200 with `status=FAIL/FAILED/ERROR` twice is
     finalized locally as remote failure.
   - The original logical POST stays consumed.
   - The candidate is never automatically re-POSTed.
   - Existing confirmed 404/410 stale-URL quarantine remains intact.

## Safety boundaries unchanged

- No automatic Submit.
- No automatic PowerPoolSelected.
- No PATCH/DELETE.
- No automatic Alpha property write.
- `machine_lib_V2_1.py` is not included and must remain unchanged.
- Budget, batch size, concurrency, 70/30 allocation, PostGate, cache,
  Resume-first and family protection remain unchanged.

## Upgrade / validation

After overlaying the ZIP into the project directory:

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
python ppl_runner.py --round-status --run-id run_0005
```

The rebuild is local-only and should report zero network/check/Simulation POST
side effects. Inspect:

```text
reports\round_run_0005\manual_finalization_queue.csv
```

Only after validating the rebuilt status should the next SEARCH batch be
resumed.
