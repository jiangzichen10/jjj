# V3.0.4d — Stale Resume 404 Guard

Purpose: prevent an old saved Simulation URL that is already gone on the server from blocking the round forever as a false RUNNING slot.

Behavior:
- Resume-first is preserved.
- Before handing RUNNING/SUBMITTED candidates to unchanged V2.1, V3 performs GET-only preflight on their saved Simulation URL.
- A URL is quarantined only after two consecutive HTTP 404/410 responses, separated by 1 second.
- Quarantine is local workflow state only: `SIMULATION_REMOTE_MISSING` / `REMOTE_NOT_FOUND`.
- The original consumed POST remains consumed. No repost is generated for that candidate.
- `alpha_results.db` is not rewritten by this guard; historical Simulation fact remains untouched.
- 200, 429, 5xx, auth errors and request exceptions are not interpreted as missing; unchanged V2.1 resume behavior remains authoritative.
- If other genuine RUNNING simulations remain, resume-first/server-slot safety still applies.

Safety invariants:
- No automatic Submit.
- No PowerPoolSelected.
- No PATCH/PUT/DELETE.
- No machine_lib_V2_1.py change.
- No budget reset.
- No policy/batch/ranking/theme config change.
