"""Canonical Simulation settings contract shared by all execution paths.

V3.1 persists the complete BRAIN Simulation settings alongside every candidate.
That durable object is part of the candidate's simulation identity and must be
the same object represented by ``sim_key``, cache identity, and any HTTP POST.
Compact call-scope projections are allowed only as transient adapter views.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Tuple

from .config import ConfigError


FULL_SIMULATION_SETTING_KEYS = (
    "instrumentType", "region", "universe", "delay", "decay",
    "neutralization", "truncation", "pasteurization", "testPeriod",
    "unitHandling", "nanHandling", "language", "visualization",
)


def validate_full_simulation_settings(
    settings: Mapping[str, Any], *, context: str = "SIMULATION_SETTINGS"
) -> Dict[str, Any]:
    """Require the complete BRAIN Simulation payload shape.

    This guard is intentionally structural.  Value semantics and platform
    validation remain owned by the unchanged V2.1 builder and BRAIN API.  Its
    purpose is to make it impossible for the six-field compatibility scope to
    be mistaken for a durable/POST settings object again.
    """
    if not isinstance(settings, Mapping):
        raise ConfigError(f"SIMULATION_SETTINGS_INVALID_TYPE: context={context}")
    missing = [key for key in FULL_SIMULATION_SETTING_KEYS if key not in settings]
    if missing:
        raise ConfigError(
            f"SIMULATION_SETTINGS_INCOMPLETE: context={context} missing="
            + ",".join(missing)
        )
    return dict(settings)


def full_settings_identity(settings: Mapping[str, Any]) -> Tuple[Any, ...]:
    """Return a hashable exact identity over all canonical settings fields."""
    full = validate_full_simulation_settings(settings, context="FULL_SETTINGS_IDENTITY")
    return tuple(full[key] for key in FULL_SIMULATION_SETTING_KEYS)
