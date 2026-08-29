import inspect
import sqlite3
from contextlib import contextmanager

import pytest

from ppl_engine.live_execution import _instrument_v21, _preview_row
from ppl_engine.research_telemetry import record_event
from ppl_engine.store import RunnerStore


def test_runner_store_sqlite_diagnostic_records_stage_db_thread_and_pragmas(
    tmp_path, monkeypatch,
):
    import ppl_engine.store as store_module

    store = RunnerStore(tmp_path / "ppl_runner.db")
    store.initialize()
    captured = []
    monkeypatch.setattr(store_module, "audit_event", lambda **payload: captured.append(payload))

    original = sqlite3.OperationalError("attempt to write a readonly database")
    with pytest.raises(sqlite3.OperationalError) as raised:
        with store.connect(stage="RESEARCH_TELEMETRY_EVENT") as conn:
            conn.execute("CREATE TABLE diagnostic_rollback(id INTEGER)")
            raise original

    assert raised.value is original
    assert captured and captured[-1]["action"] == "SQLITE_WRITE_ERROR_DIAGNOSTIC"
    event = captured[-1]
    assert event["stage"] == "RESEARCH_TELEMETRY_EVENT"
    assert event["resolved_db_path"].endswith("ppl_runner.db")
    assert event["thread_id"] and event["thread_name"]
    assert set(event["pragmas"]) == {
        "database_list", "query_only", "journal_mode", "locking_mode", "synchronous",
    }
    assert set(event["files"]) == {"db", "wal", "shm"}


def test_diagnostic_secondary_failure_never_masks_original_sqlite_error(tmp_path, monkeypatch):
    import ppl_engine.store as store_module

    store = RunnerStore(tmp_path / "ppl_runner.db")
    store.initialize()
    monkeypatch.setattr(store_module, "audit_event", lambda **_payload: (_ for _ in ()).throw(RuntimeError("log failed")))
    original = sqlite3.OperationalError("attempt to write a readonly database")
    with pytest.raises(sqlite3.OperationalError) as raised:
        with store.connect(stage="RUNNER_STORE_WRITE"):
            raise original
    assert raised.value is original


def test_research_telemetry_event_uses_explicit_diagnostic_stage(tmp_path, monkeypatch):
    store = RunnerStore(tmp_path / "ppl_runner.db")
    store.initialize()
    stages = []

    @contextmanager
    def failing_connect(*, stage="RUNNER_STORE_WRITE"):
        stages.append(stage)
        raise sqlite3.OperationalError("attempt to write a readonly database")
        yield  # pragma: no cover

    monkeypatch.setattr(store, "connect", failing_connect)
    with pytest.raises(sqlite3.OperationalError):
        record_event(store, "round", "run", "TEST")
    assert stages == ["RESEARCH_TELEMETRY_EVENT"]


class _CacheMachine:
    def __init__(self):
        self.records = {}

        class Session:
            @staticmethod
            def request(*_args, **_kwargs):
                raise AssertionError("network must not be used")

        class Sessions:
            pass

        Sessions.Session = Session

        class Requests:
            pass

        Requests.sessions = Sessions()
        self.requests = Requests()

    def cache_put(self, _db, sim_key, _candidate, _settings, result):
        self.records[sim_key] = dict(result)

    def cache_get(self, _db, sim_key):
        return dict(self.records.get(sim_key) or {})


class _FailingStore:
    def connect(self, **_kwargs):
        raise sqlite3.OperationalError("attempt to write a readonly database")


def test_alpha_cache_url_survives_runner_sync_failure_and_classifies_resume_existing():
    machine = _CacheMachine()
    fact = {
        "status": "RUNNING",
        "simulation_url": "https://api.worldquantbrain.com/simulations/sim123",
        "submitted_at": "2026-08-27T00:00:00+00:00",
    }
    with _instrument_v21(machine, _FailingStore(), {"key": "candidate"}):
        with pytest.raises(sqlite3.OperationalError):
            machine.cache_put("alpha_results.db", "key", {"expr": "x"}, {}, fact)

    assert machine.records["key"]["simulation_url"] == fact["simulation_url"]
    preview = _preview_row({"candidate_id": "candidate", "sim_key": "key"}, machine.cache_get("", "key"))
    assert preview["execution_action"] == "RESUME_EXISTING"


def test_alpha_cache_failure_has_separate_diagnostic_stage(monkeypatch):
    import ppl_engine.live_execution as live

    machine = _CacheMachine()
    diagnostics = []
    monkeypatch.setattr(
        live, "_emit_sqlite_write_diagnostic", lambda **payload: diagnostics.append(payload),
    )
    machine.cache_put = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        sqlite3.OperationalError("attempt to write a readonly database")
    )
    with _instrument_v21(machine, None, {}):
        with pytest.raises(sqlite3.OperationalError):
            machine.cache_put("alpha_results.db", "key", {"expr": "x"}, {}, {})
    assert diagnostics and diagnostics[0]["stage"] == "ALPHA_CACHE_WRITE"


def test_progress_labels_workers_separately_from_durable_status(capsys):
    from ppl_engine.production_repair import _durable_worker_progress_summary

    summary = _durable_worker_progress_summary(
        {"processed": 8, "submitted_futures": 8},
        {
            **{f"complete-{i}": {"status": "COMPLETE"} for i in range(6)},
            "running-1": {"status": "RUNNING"},
            "running-2": {"status": "RUNNING"},
        },
    )
    output = capsys.readouterr().out
    assert summary["workers_finished"] == 8
    assert summary["durable_complete"] == 6
    assert summary["durable_running"] == 2
    assert "Workers Finished: 8 / 8" in output
    assert "Durable Complete: 6" in output
    assert "Durable Running: 2" in output


@pytest.mark.parametrize(
    "candidate_id,simulation_url",
    [
        (
            "repair_9c12de4477019f978a459f20",
            "https://api.worldquantbrain.com/simulations/uVbHT68H4FVbIKAOZfUggx",
        ),
        (
            "repair_fa2c9c03947666da768f2ad2",
            "https://api.worldquantbrain.com/simulations/4ueGwv5AI4S0bz014nF2wTvc",
        ),
    ],
)
def test_run_0005_running_recovery_facts_are_resume_existing_not_post(
    candidate_id, simulation_url,
):
    preview = _preview_row(
        {"candidate_id": candidate_id, "sim_key": "durable-key"},
        {
            "status": "RUNNING", "simulation_url": simulation_url,
            "submitted_at": "2026-08-27T00:00:00+00:00",
        },
    )
    assert preview["execution_action"] == "RESUME_EXISTING"
    assert preview["execution_action"] != "NEW_SIMULATION_REQUIRED"


def test_telemetry_stage_labels_are_explicit_without_transaction_rewrite():
    import ppl_engine.research_telemetry as telemetry

    event_source = inspect.getsource(telemetry.record_event)
    ledger_source = inspect.getsource(telemetry.sync_simulation_ledger)
    snapshot_source = inspect.getsource(telemetry.upsert_snapshot)
    assert 'stage="RESEARCH_TELEMETRY_EVENT"' in event_source
    assert 'stage="RESEARCH_TELEMETRY_LEDGER"' in ledger_source
    assert 'stage="RESEARCH_TELEMETRY_SNAPSHOT"' in snapshot_source
    # The hotfix deliberately preserves the existing event mirroring algorithm.
    assert "for t in transitions" in inspect.getsource(telemetry.sync_durable_events)


def test_six_resolved_pretag_facts_survive_later_telemetry_failure_without_repost():
    facts = {
        **{
            f"complete-{i}": {
                "status": "COMPLETE", "alpha_id": f"alpha-{i}",
                "simulation_url": f"https://api.worldquantbrain.com/simulations/complete{i}",
            }
            for i in range(6)
        },
        "running-3": {
            "status": "RUNNING",
            "simulation_url": "https://api.worldquantbrain.com/simulations/uVbHT68H4FVbIKAOZfUggx",
        },
        "running-8": {
            "status": "RUNNING",
            "simulation_url": "https://api.worldquantbrain.com/simulations/4ueGwv5AI4S0bz014nF2wTvc",
        },
    }
    resolved_check_ids = {f"complete-{i}" for i in range(6)}

    with pytest.raises(sqlite3.OperationalError):
        raise sqlite3.OperationalError("research telemetry write failed")

    actions = {
        key: _preview_row({"candidate_id": key, "sim_key": key}, fact)["execution_action"]
        for key, fact in facts.items()
    }
    assert resolved_check_ids == {key for key, action in actions.items() if action == "CACHE_RESTORE"}
    assert actions["running-3"] == "RESUME_EXISTING"
    assert actions["running-8"] == "RESUME_EXISTING"
    assert "NEW_SIMULATION_REQUIRED" not in actions.values()
