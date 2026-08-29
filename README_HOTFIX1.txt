WorldQuant BRAIN V3.1 D2E HOTFIX1

Purpose:
Fix historical/shared alpha cache COMPLETE facts being locally analyzed while a run candidate is still PLANNED/SIMULATION_PENDING/SIMULATION_RUNNING, which caused illegal workflow transitions such as PLANNED -> PRE_CHECK_REPAIR.

Behavior after patch:
Durable COMPLETE fact is reconciled first:
PLANNED/SIMULATION_PENDING/SIMULATION_RUNNING -> SIMULATION_COMPLETE -> SIGNAL_ANALYZED -> downstream local gate state.

Safety:
- Does not change sim_key or SimulationSpec.
- Does not alter D2E authority lock.
- Does not enable Adaptive control.
- Does not modify machine_lib_V2_1.py.
- Does not contain or overwrite any DB.

Validated:
- tests/test_production_logging_phase2.py + tests/test_v31_d2e_compatibility_evidence.py: 37 passed
- tests/test_v3_round_orchestrator.py: 83 passed
- compileall modified source: PASS
- machine_lib_V2_1.py SHA256 unchanged: 58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4
