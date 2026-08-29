# V3.0.4a — Manual Submitted Alpha Protection

Adds one local-only command for user-confirmed manual submissions:

```powershell
python ppl_runner.py --protect-alpha --run-id run_0005 --alpha-id P07JvaXK
```

Behavior:
- resolves the Alpha inside the specified run;
- protects the entire signal family in `ppl_round_family_winners`;
- records `USER_CONFIRMED_SUBMITTED` as a local user-confirmed protection state;
- does not rewrite historical simulation/check/PPL classification facts;
- is idempotent;
- performs zero network, Simulation POST, Check, Submit, PowerPoolSelected, PATCH, or DELETE requests;
- the existing repair selector already skips protected families, so no future repair POST can be selected for that family.

This hotfix does not modify `machine_lib_V2_1.py`, budgets, ranking, rolling discovery, or PPL classification policy.
