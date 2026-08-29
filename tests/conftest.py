"""Make the repository root importable without a user-managed PYTHONPATH."""

import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True, scope="session")
def _disable_audit_log_for_tests():
    """Never write the real production audit trail during test runs.

    ``AUDIT_LOG_ENABLED=false`` keeps ``configure_audit_log`` (and therefore
    every ``audit_event`` / state-transition hook) inert across the whole suite,
    including subprocess CLI invocations that inherit this environment. Tests
    that exercise the audit module opt back in with an explicit ``enabled=True``
    config pointing at a temporary directory.
    """
    prev = os.environ.get("AUDIT_LOG_ENABLED")
    os.environ["AUDIT_LOG_ENABLED"] = "false"
    yield
    if prev is None:
        os.environ.pop("AUDIT_LOG_ENABLED", None)
    else:
        os.environ["AUDIT_LOG_ENABLED"] = prev

