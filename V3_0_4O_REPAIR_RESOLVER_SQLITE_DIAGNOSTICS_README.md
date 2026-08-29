# WorldQuant_BRAIN_v3 v3.0.4o — Repair Resolver / SQLite diagnostics hotfix

## Scope

`v3.0.4o` is a minimal production-safety hotfix over `v3.0.4n`.

- Round-owned REPAIR execution may use the existing, URL-validated Simulation DELETE path required by Remote Simulation Resolution.
- Independent Production Repair and every other caller retain the existing HTTP method firewall.
- Local HTTP policy rejection is reported as `REMOTE_CANCEL_LOCAL_POLICY_ERROR`, not as a network error.
- SQLite write failures emit best-effort diagnostics with DB, thread, PRAGMA and DB/WAL/SHM state while preserving the original exception.
- Alpha cache persistence remains before RunnerStore candidate synchronization.
- Progress text distinguishes finished worker futures from durable Simulation terminality.

## Non-goals

This release does not claim that the historical SQLite readonly root cause is fixed. It adds evidence needed to identify the failing DB, stage, connection and filesystem state if the failure recurs. It does not rewrite telemetry transactions.

It does not change candidate generation, Search/Repair scheduling, budget accounting, PPC/near-pass rules, datasets, current `run_0005` durable state, or existing Simulation identities.

## Safety invariants

- A stored `simulation_url` remains resume-first and is never converted into a new POST merely because RunnerStore synchronization failed.
- Simulation DELETE remains restricted to a validated WorldQuant Simulation URL and an explicit Round Repair execution scope.
- PATCH, PUT, arbitrary DELETE and arbitrary POST remain prohibited.
- Remote cancellation failures retain the URL and fail closed.

