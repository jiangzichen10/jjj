from datetime import datetime, timedelta, timezone

import pytest

from ppl_engine.remote_simulation import (
    durable_running_age_seconds,
    resolve_remote_simulation,
)
from ppl_engine.simulation_adapter import (
    RemoteSimulationResolved,
    _durable_timeout_polling,
    production_remote_resolution_status,
)


URL = "https://api.worldquantbrain.com/simulations/sim123"


class Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


class Session:
    def __init__(self, gets=(), deletes=()):
        self.gets = list(gets)
        self.deletes = list(deletes)
        self.calls = []

    @staticmethod
    def _item(value):
        if isinstance(value, BaseException):
            raise value
        return Response(*value) if isinstance(value, tuple) else Response(value)

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self._item(self.gets.pop(0))

    def delete(self, url, **kwargs):
        self.calls.append(("DELETE", url, kwargs))
        return self._item(self.deletes.pop(0))


def resolve(session, *, trigger="MANUAL_CANCEL", reason="USER_CANCELLED"):
    return resolve_remote_simulation(
        session, URL, trigger_source=trigger,
        submitted_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        cancellation_reason=reason, verification_sleep_seconds=0,
        status_policy=production_remote_resolution_status,
    )


def test_running_delete_200_then_double_missing_closes_user_cancel():
    session = Session(gets=[(200, {"status": "RUNNING"}), 404, 404], deletes=[200])
    result = resolve(session)
    assert result.resolution_result == "REMOTE_NOT_FOUND"
    assert result.trigger_source == "MANUAL_CANCEL"
    assert result.resolution_reason == "USER_CANCELLED"
    assert result.delete_attempted is True
    assert [x[0] for x in session.calls] == ["GET", "DELETE", "GET", "GET"]


@pytest.mark.parametrize("trigger,reason,progress", [
    ("MANUAL_CANCEL", "USER_CANCELLED", 0.1),
    ("MANUAL_CANCEL", "USER_CANCELLED", 0.0),
    ("MANUAL_CANCEL", "USER_CANCELLED", 0.95),
    ("AUTO_TIMEOUT", "AUTO_CANCEL_TIMEOUT", 0.1),
    ("STALE_RECOVERY", "STALE_RECOVERY_CANCELLED", 0.1),
])
def test_progress_only_is_production_nonterminal_for_every_resolution_trigger(trigger, reason, progress):
    session = Session(gets=[(200, {"progress": progress}), 404, 404], deletes=[200])
    result = resolve(session, trigger=trigger, reason=reason)
    assert result.pre_resolution_remote_status == "PENDING"
    assert result.delete_attempted is True
    assert result.resolution_result == "REMOTE_NOT_FOUND"
    assert result.resolution_reason == reason
    assert [x[0] for x in session.calls] == ["GET", "DELETE", "GET", "GET"]


@pytest.mark.parametrize("payload", [{}, {"foo": "bar"}, {"unexpected": True}])
def test_unknown_payload_without_valid_progress_remains_fail_closed(payload):
    session = Session(gets=[(200, payload)])
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.error_reason == "REMOTE_RESOLUTION_UNKNOWN_STATUS"
    assert result.delete_attempted is False
    assert [x[0] for x in session.calls] == ["GET"]


@pytest.mark.parametrize("progress", [True, False, "0.1", -0.1, 1.1, float("nan"), float("inf")])
def test_invalid_progress_evidence_remains_fail_closed(progress):
    session = Session(gets=[(200, {"progress": progress})])
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.delete_attempted is False


def test_initial_double_missing_never_deletes_and_has_separate_reason():
    session = Session(gets=[404, 410])
    result = resolve(session, trigger="STALE_RECOVERY", reason="STALE_RECOVERY_CANCELLED")
    assert result.resolution_result == "REMOTE_NOT_FOUND"
    assert result.trigger_source == "STALE_RECOVERY"
    assert result.resolution_reason == "REMOTE_ALREADY_ABSENT"
    assert all(call[0] == "GET" for call in session.calls)


@pytest.mark.parametrize("payload", [
    {"status": "COMPLETE", "alpha": "a1"},
    {"status": "WARNING", "alpha": "a1"},
    {"status": "COMPLETE", "alpha": "a1", "progress": 0.95},
    {"status": "WARNING", "alpha": "a1", "progress": 0.95},
])
def test_existing_result_policy_delegates_without_delete(payload):
    session = Session(gets=[(200, payload)])
    result = resolve(session)
    assert result.resolution_result == "DELEGATE_RESULT"
    assert [x[0] for x in session.calls] == ["GET"]


def test_warning_without_alpha_is_not_redefined_as_complete():
    assert production_remote_resolution_status({"status": "WARNING"}) == "UNKNOWN"
    session = Session(gets=[(200, {"status": "WARNING"})])
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.error_reason == "REMOTE_RESOLUTION_UNKNOWN_STATUS"


def test_terminal_failure_requires_two_production_confirmations():
    session = Session(gets=[(200, {"status": "FAIL"}), (200, {"status": "FAILED"})])
    result = resolve(session)
    assert result.resolution_result == "DELEGATE_TERMINAL_FAILURE"
    assert result.verification_statuses == [200, 200]
    assert all(call[0] == "GET" for call in session.calls)


def test_delete_404_race_is_confirmed_as_already_absent():
    session = Session(gets=[(200, {"status": "PENDING"}), 404, 404], deletes=[404])
    result = resolve(session)
    assert result.resolution_result == "REMOTE_NOT_FOUND"
    assert result.resolution_reason == "REMOTE_ALREADY_ABSENT"


@pytest.mark.parametrize("status", ["RUNNING", "PENDING", "QUEUED"])
def test_explicit_production_nonterminal_statuses_still_delete(status):
    session = Session(gets=[(200, {"status": status}), 404, 404], deletes=[200])
    result = resolve(session)
    assert result.delete_attempted is True
    assert result.resolution_result == "REMOTE_NOT_FOUND"


@pytest.mark.parametrize("delete_value,reason", [
    (500, "REMOTE_CANCEL_HTTP_ERROR"),
    (TimeoutError("network"), "REMOTE_CANCEL_NETWORK_ERROR"),
])
def test_delete_failure_is_fail_closed(delete_value, reason):
    session = Session(gets=[(200, {"status": "RUNNING"})], deletes=[delete_value])
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.error_reason == reason
    assert result.verification_statuses == []


def test_local_http_method_policy_failure_is_not_misclassified_as_network():
    session = Session(
        gets=[(200, {"status": "RUNNING"})],
        deletes=[RuntimeError("PHASE10A_FORBIDDEN_HTTP_METHOD:DELETE")],
    )
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.error_reason == "REMOTE_CANCEL_LOCAL_POLICY_ERROR"
    assert result.simulation_url == URL
    assert result.delete_attempted is True


def test_real_delete_network_failure_remains_network_and_fail_closed():
    session = Session(
        gets=[(200, {"status": "RUNNING"})], deletes=[TimeoutError("network")],
    )
    result = resolve(session)
    assert result.resolution_result == "UNRESOLVED"
    assert result.error_reason == "REMOTE_CANCEL_NETWORK_ERROR"
    assert result.simulation_url == URL


class Machine:
    def __init__(self, submitted_at):
        self.record = {"status": "RUNNING", "simulation_url": URL,
                       "submitted_at": submitted_at, "retry_count": 0}
        self.original_wait_calls = []
        self.cache_writes = []
        self.wait_simulation = self._wait

    def cache_get(self, _db, _key):
        return dict(self.record)

    def cache_put(self, _db, _key, _candidate, _settings, result):
        self.cache_writes.append(dict(result))
        self.record.update(result)

    def _wait(self, _session, _url, **kwargs):
        self.original_wait_calls.append(kwargs["max_wait_seconds"])
        return {"status": "COMPLETE", "alpha": "a1"}


def test_1799_second_record_uses_only_remaining_durable_time():
    submitted = (datetime.now(timezone.utc) - timedelta(seconds=1799)).isoformat()
    machine = Machine(submitted)
    with _durable_timeout_polling(
        machine, operational_timeout_seconds=1800, stale_backstop_seconds=21600,
        auto_cancel=True, verify_attempts=2,
    ):
        result = machine.wait_simulation(
            Session(), URL, cache_db="db", sim_key="key", candidate={"expr": "x"}, settings={},
        )
    assert result["status"] == "COMPLETE"
    assert machine.original_wait_calls == [1]


def test_eight_hour_record_resolves_immediately_without_new_poll_window():
    submitted = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    machine = Machine(submitted)
    session = Session(gets=[404, 404])
    with _durable_timeout_polling(
        machine, operational_timeout_seconds=1800, stale_backstop_seconds=21600,
        auto_cancel=True, verify_attempts=2,
    ):
        with pytest.raises(RemoteSimulationResolved):
            machine.wait_simulation(
                session, URL, cache_db="db", sim_key="key",
                candidate={"expr": "x"}, settings={},
            )
    assert machine.original_wait_calls == []
    assert machine.record["status"] == "REMOTE_NOT_FOUND"
    assert all(call[0] == "GET" for call in session.calls)
    metadata = machine.cache_writes[-1]["error"]
    assert '"trigger_source": "STALE_RECOVERY"' in metadata
    assert '"resolution_reason": "REMOTE_ALREADY_ABSENT"' in metadata


def test_durable_age_never_falls_back_to_updated_at():
    assert durable_running_age_seconds(None) is None


def test_cancel_cli_requires_explicit_separate_confirmation_flag():
    from ppl_runner import _parser
    args = _parser().parse_args([
        "--cancel-simulation", "--run-id", "run_0005",
        "--simulation-id", "sim123",
    ])
    assert args.cancel_simulation is True
    assert args.confirm_cancel_simulation is False
    confirmed = _parser().parse_args([
        "--cancel-simulation", "--run-id", "run_0005",
        "--simulation-id", "sim123", "--confirm-cancel-simulation",
    ])
    assert confirmed.confirm_cancel_simulation is True


def test_budget_fact_is_submission_evidence_not_resolution_status():
    from ppl_engine.round_orchestrator import _durable_confirmed_post
    assert _durable_confirmed_post({"status": "REMOTE_NOT_FOUND"}) is False
    assert _durable_confirmed_post({
        "status": "REMOTE_NOT_FOUND", "simulation_url": URL,
        "submitted_at": "2026-08-21T00:00:00+00:00",
    }) is True
