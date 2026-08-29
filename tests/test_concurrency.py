import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

import machine_lib_V2_1 as ml


class FakeResponse:
    def __init__(self, status_code, *, headers=None, payload=None, url=""):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload or {}
        self.url = url
        self.text = "" if payload is None else str(payload)

    def json(self):
        return self._payload


class FakeSession(requests.Session):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.auth_failed_once = False
        self._closed = False

    def post(self, url, **kwargs):
        return self.server.post(self, url, **kwargs)

    def request(self, method, url, **kwargs):
        return self.server.request(self, method, url, **kwargs)

    def close(self):
        if not self._closed:
            self._closed = True
            self.server.mark_session_closed()


class FakeServer:
    def __init__(
        self,
        *,
        network_delay=0.01,
        retry_after="0",
        fail_post_network=False,
        first_post_429=False,
        auth_401_once=False,
        stop_event=None,
        stop_at_active=None,
        variable_delay=False,
    ):
        self.network_delay = network_delay
        self.retry_after = retry_after
        self.fail_post_network = fail_post_network
        self.first_post_429 = first_post_429
        self.auth_401_once = auth_401_once
        self.stop_event = stop_event
        self.stop_at_active = stop_at_active
        self.variable_delay = variable_delay
        self.lock = threading.Lock()
        self.post_count = 0
        self.poll_count = 0
        self.metrics_count = 0
        self.active = 0
        self.max_active = 0
        self.posts_after_stop = 0
        self.post_times = []
        self.sessions = []
        self.closed_sessions = 0
        self.simulations = {}
        self.alpha_expr = {}

    def new_session(self, _base_session):
        session = FakeSession(self)
        with self.lock:
            self.sessions.append(session)
        return session

    def mark_session_closed(self):
        with self.lock:
            self.closed_sessions += 1

    def register_existing(self, url, expression):
        alpha_id = "alpha-existing-" + str(len(self.simulations))
        self.simulations[url] = (expression, alpha_id)
        self.alpha_expr[alpha_id] = expression

    def post(self, session, url, **kwargs):
        with self.lock:
            if self.stop_event is not None and self.stop_event.is_set():
                self.posts_after_stop += 1
            self.post_count += 1
            call_number = self.post_count
            self.post_times.append(time.monotonic())
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if (
                self.stop_event is not None
                and self.stop_at_active is not None
                and self.active >= self.stop_at_active
            ):
                self.stop_event.set()

        payload = kwargs.get("json") or {}
        expression = payload.get("regular", "unknown")
        delay = self.network_delay
        if self.variable_delay:
            suffix = int(expression.rsplit("_", 1)[-1])
            delay += (suffix % 5) * 0.003
        try:
            if self.first_post_429 and call_number == 1:
                return FakeResponse(
                    429,
                    headers={"Retry-After": "0.08"},
                    payload={"error": "rate limited"},
                    url=url,
                )
            time.sleep(delay)
            if self.fail_post_network:
                raise ConnectionResetError("offline fake reset")
            if self.auth_401_once and not session.auth_failed_once:
                session.auth_failed_once = True
                return FakeResponse(401, payload={"error": "expired"}, url=url)
            simulation_url = f"https://fake.invalid/simulations/{call_number}"
            alpha_id = f"alpha-{call_number}"
            with self.lock:
                self.simulations[simulation_url] = (expression, alpha_id)
                self.alpha_expr[alpha_id] = expression
            return FakeResponse(
                201,
                headers={
                    "Location": simulation_url,
                    "Retry-After": str(self.retry_after),
                },
                url=url,
            )
        finally:
            with self.lock:
                self.active -= 1

    def request(self, _session, method, url, **kwargs):
        if method.upper() != "GET":
            raise AssertionError(f"Unexpected fake method: {method}")
        if "/simulations/" in url:
            with self.lock:
                self.poll_count += 1
            expression, alpha_id = self.simulations.get(
                url, ("existing-expression", "alpha-existing")
            )
            self.alpha_expr.setdefault(alpha_id, expression)
            return FakeResponse(
                200,
                payload={"status": "COMPLETE", "alpha": alpha_id},
                url=url,
            )
        if "/alphas/" in url:
            with self.lock:
                self.metrics_count += 1
            alpha_id = url.rsplit("/", 1)[-1]
            expression = self.alpha_expr.get(alpha_id, "unknown")
            return FakeResponse(
                200,
                payload={
                    "regular": {"code": expression},
                    "is": {
                        "sharpe": 1.0,
                        "fitness": 0.8,
                        "turnover": 0.2,
                        "margin": 0.001,
                        "returns": 0.05,
                        "longCount": 100,
                        "shortCount": 100,
                    },
                    "dateCreated": "2026-01-01",
                    "settings": {"decay": 4},
                },
                url=url,
            )
        raise AssertionError(f"Unexpected fake GET: {url}")


class FastEvent(threading.Event):
    def __init__(self):
        super().__init__()
        self.waits = []

    def wait(self, timeout=None):
        self.waits.append(timeout)
        return self.is_set()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "cache.db")
        self.base_session = requests.Session()

    def tearDown(self):
        self.base_session.close()
        self.temp_dir.cleanup()

    @staticmethod
    def candidates(count):
        return [
            {"expr": f"fake_expr_{index:02d}", "field": f"f{index}", "decay": 4}
            for index in range(count)
        ]

    @staticmethod
    def settings(candidate):
        return ml.build_settings(
            candidate,
            neutralization="SUBINDUSTRY",
            region="GBR",
            universe="TOP700",
            delay=1,
            truncation=0.08,
            test_period="P0Y",
        )

    def put_status(self, candidate, status, **extra):
        settings = self.settings(candidate)
        key = ml.simulation_key(candidate["expr"], settings)
        ml.cache_put(
            self.db_path,
            key,
            candidate,
            settings,
            {"status": status, **extra},
        )
        return key

    def run_fake(
        self,
        candidates,
        server,
        *,
        concurrency,
        stop_event=None,
        runtime_stats=None,
        submit_stagger_seconds=0.0,
    ):
        with patch.object(ml, "_clone_session", side_effect=server.new_session), patch.object(
            ml, "_safe_print", lambda *args, **kwargs: None
        ):
            return ml.simulate_candidates(
                candidates,
                neutralization="SUBINDUSTRY",
                region="GBR",
                universe="TOP700",
                session=self.base_session,
                cache_db=self.db_path,
                concurrency=concurrency,
                delay=1,
                truncation=0.08,
                test_period="P0Y",
                progress_every=1000,
                stop_event=stop_event,
                submit_stagger_seconds=submit_stagger_seconds,
                _runtime_stats=runtime_stats,
            )

    def test_01_concurrency_six_is_real_and_bounded(self):
        server = FakeServer(network_delay=0.025, variable_delay=True)
        stats = {}
        result = self.run_fake(
            self.candidates(20), server, concurrency=6, runtime_stats=stats
        )
        self.assertGreater(server.max_active, 1)
        self.assertLessEqual(server.max_active, 6)
        self.assertLessEqual(stats["max_futures"], 6)
        self.assertEqual(len(result), 20)
        self.assertEqual(set(result["status"]), {"COMPLETE"})

        self.base_session.cookies.set("brain-test", "base")
        cloned_a = ml._clone_session(self.base_session)
        cloned_b = ml._clone_session(self.base_session)
        try:
            self.assertIsNot(cloned_a, cloned_b)
            self.assertIsNot(cloned_a.cookies, cloned_b.cookies)
            self.assertEqual(cloned_a.cookies.get("brain-test"), "base")
            cloned_a.cookies.set("brain-test", "worker-a")
            self.assertEqual(cloned_b.cookies.get("brain-test"), "base")
            self.assertEqual(self.base_session.cookies.get("brain-test"), "base")
        finally:
            cloned_a.close()
            cloned_b.close()

    def test_02_concurrency_one_is_serial(self):
        server = FakeServer(network_delay=0.01)
        result = self.run_fake(self.candidates(8), server, concurrency=1)
        self.assertEqual(server.max_active, 1)
        self.assertEqual(len(result), 8)

    def test_03_complete_cache_never_posts_or_polls(self):
        candidate = self.candidates(1)[0]
        self.put_status(candidate, "COMPLETE", alpha_id="cached-alpha")
        server = FakeServer()
        result = self.run_fake([candidate], server, concurrency=6)
        self.assertEqual(server.post_count, 0)
        self.assertEqual(server.poll_count, 0)
        self.assertEqual(result.iloc[0]["status"], "COMPLETE")

    def test_04_submitted_resumes_existing_url_without_post(self):
        candidate = self.candidates(1)[0]
        url = "https://fake.invalid/simulations/existing"
        key = self.put_status(candidate, "SUBMITTED", simulation_url=url)
        server = FakeServer()
        server.register_existing(url, candidate["expr"])
        self.run_fake([candidate], server, concurrency=6)
        self.assertEqual(server.post_count, 0)
        self.assertEqual(server.poll_count, 1)
        self.assertEqual(ml.cache_get(self.db_path, key)["status"], "COMPLETE")

    def test_05_error_is_retried_once_and_completes(self):
        candidate = self.candidates(1)[0]
        key = self.put_status(candidate, "ERROR", error="old error")
        server = FakeServer()
        self.run_fake([candidate], server, concurrency=6)
        self.assertEqual(server.post_count, 1)
        self.assertEqual(ml.cache_get(self.db_path, key)["status"], "COMPLETE")

    def test_06_uncertain_submission_is_not_posted(self):
        candidate = self.candidates(1)[0]
        self.put_status(candidate, "UNCERTAIN_SUBMISSION", error="unknown outcome")
        server = FakeServer()
        self.run_fake([candidate], server, concurrency=6)
        self.assertEqual(server.post_count, 0)
        self.assertEqual(server.poll_count, 0)

    def test_07_201_retry_after_only_delays_first_poll(self):
        event = FastEvent()
        server = FakeServer(retry_after="5")
        candidate = self.candidates(1)[0]
        self.put_status(candidate, "ERROR", last_retry_after=30.0)
        result = self.run_fake(
            [candidate],
            server,
            concurrency=1,
            stop_event=event,
        )
        self.assertEqual(server.post_count, 1)
        self.assertEqual(server.poll_count, 1)
        self.assertIn(5.0, event.waits)
        self.assertNotIn(30.0, event.waits)
        self.assertEqual(result.iloc[0]["status"], "COMPLETE")

    def test_08_post_network_reset_is_uncertain_and_not_retried(self):
        candidate = self.candidates(1)[0]
        settings = self.settings(candidate)
        key = ml.simulation_key(candidate["expr"], settings)
        server = FakeServer(fail_post_network=True)
        self.run_fake([candidate], server, concurrency=6)
        self.assertEqual(server.post_count, 1)
        self.assertEqual(
            ml.cache_get(self.db_path, key)["status"], "UNCERTAIN_SUBMISSION"
        )

    def test_09_six_way_sqlite_writes_have_no_duplicates_or_lock_errors(self):
        candidates = self.candidates(20)
        server = FakeServer(network_delay=0.01)
        result = self.run_fake(candidates, server, concurrency=6)
        self.assertNotIn("database is locked", " ".join(result["error"].dropna().astype(str)))
        connection = sqlite3.connect(self.db_path)
        try:
            total, distinct_count = connection.execute(
                "SELECT COUNT(sim_key), COUNT(DISTINCT sim_key) FROM alpha_results"
            ).fetchone()
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(total, 20)
        self.assertEqual(total, distinct_count)
        self.assertEqual(journal_mode.lower(), "wal")

    def test_10_rolling_scheduler_never_queues_more_than_concurrency(self):
        stats = {}
        server = FakeServer(network_delay=0.005)
        result = self.run_fake(
            self.candidates(50), server, concurrency=6, runtime_stats=stats
        )
        self.assertEqual(stats["submitted_futures"], 50)
        self.assertLessEqual(stats["max_futures"], 6)
        self.assertEqual(len(result), 50)

    def test_11_stop_event_stops_new_posts_and_preserves_urls(self):
        event = threading.Event()
        server = FakeServer(
            network_delay=0.05,
            stop_event=event,
            stop_at_active=6,
        )
        stats = {}
        started = time.monotonic()
        self.run_fake(
            self.candidates(20),
            server,
            concurrency=6,
            stop_event=event,
            runtime_stats=stats,
        )
        scheduler_elapsed = time.monotonic() - started
        deadline = time.time() + 1.0
        while (
            server.closed_sessions < len(server.sessions)
            and time.time() < deadline
        ):
            time.sleep(0.01)
        self.assertLess(scheduler_elapsed, 0.5)
        self.assertEqual(server.closed_sessions, len(server.sessions))
        self.assertLessEqual(server.post_count, 6)
        self.assertEqual(server.posts_after_stop, 0)
        self.assertLessEqual(stats["max_futures"], 6)
        connection = sqlite3.connect(self.db_path)
        try:
            rows = connection.execute(
                "SELECT status, simulation_url FROM alpha_results"
            ).fetchall()
        finally:
            connection.close()
        self.assertTrue(rows)
        self.assertTrue(
            all(status in ("SUBMITTED", "RUNNING") and url for status, url in rows)
        )

    def test_12_result_order_matches_unique_candidate_order(self):
        candidates = self.candidates(20)
        server = FakeServer(network_delay=0.005, variable_delay=True)
        result = self.run_fake(candidates, server, concurrency=6)
        returned = [candidate["expr"] for candidate in result["candidate"]]
        expected = [candidate["expr"] for candidate in candidates]
        self.assertEqual(returned, expected)

    def test_13_429_activates_global_post_cooldown(self):
        candidates = self.candidates(6)
        server = FakeServer(first_post_429=True, network_delay=0.001)
        result = self.run_fake(
            candidates,
            server,
            concurrency=6,
            submit_stagger_seconds=0.005,
        )
        self.assertEqual(server.post_count, len(candidates) + 1)
        self.assertGreaterEqual(server.post_times[1] - server.post_times[0], 0.06)
        self.assertEqual(set(result["status"]), {"COMPLETE"})

    def test_14_concurrent_401_refreshes_are_serialized(self):
        server = FakeServer(auth_401_once=True, network_delay=0.005)
        login_lock = threading.Lock()
        active_logins = 0
        max_active_logins = 0
        login_count = 0

        def fake_login(*args, **kwargs):
            nonlocal active_logins, max_active_logins, login_count
            with login_lock:
                active_logins += 1
                login_count += 1
                max_active_logins = max(max_active_logins, active_logins)
            time.sleep(0.02)
            with login_lock:
                active_logins -= 1
            return requests.Session()

        with patch.object(ml, "login", side_effect=fake_login):
            result = self.run_fake(self.candidates(6), server, concurrency=6)
        self.assertEqual(login_count, 6)
        self.assertEqual(max_active_logins, 1)
        self.assertEqual(set(result["status"]), {"COMPLETE"})

    def test_15_keyboard_interrupt_is_nonblocking_and_stops_new_posts(self):
        server = FakeServer(network_delay=0.1)
        stats = {}
        posts_at_interrupt = []

        def interrupting_wait(*args, **kwargs):
            deadline = time.time() + 1.0
            while server.active < 6 and time.time() < deadline:
                time.sleep(0.005)
            posts_at_interrupt.append(server.post_count)
            raise KeyboardInterrupt()

        started = time.monotonic()
        with patch.object(ml, "_clone_session", side_effect=server.new_session), patch.object(
            ml, "_safe_print", lambda *args, **kwargs: None
        ), patch.object(ml, "wait", side_effect=interrupting_wait):
            with self.assertRaises(KeyboardInterrupt):
                ml.simulate_candidates(
                    self.candidates(20),
                    neutralization="SUBINDUSTRY",
                    region="GBR",
                    universe="TOP700",
                    session=self.base_session,
                    cache_db=self.db_path,
                    concurrency=6,
                    delay=1,
                    truncation=0.08,
                    test_period="P0Y",
                    progress_every=1000,
                    submit_stagger_seconds=0.0,
                    _runtime_stats=stats,
                )
        scheduler_elapsed = time.monotonic() - started
        deadline = time.time() + 1.0
        while server.closed_sessions < len(server.sessions) and time.time() < deadline:
            time.sleep(0.01)
        self.assertLess(scheduler_elapsed, 0.5)
        self.assertEqual(server.closed_sessions, len(server.sessions))
        self.assertEqual(server.post_count, posts_at_interrupt[0])
        self.assertLessEqual(server.post_count, 6)
        self.assertLessEqual(stats["max_futures"], 6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
