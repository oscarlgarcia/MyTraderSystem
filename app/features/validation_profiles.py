from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationProfile:
    name: str
    max_invalid_ratio: float
    max_staleness_seconds: float


VALIDATION_PROFILES = {
    "research": ValidationProfile(name="research", max_invalid_ratio=1.0, max_staleness_seconds=900.0),
    "paper": ValidationProfile(name="paper", max_invalid_ratio=0.05, max_staleness_seconds=300.0),
    "live": ValidationProfile(name="live", max_invalid_ratio=0.01, max_staleness_seconds=30.0),
}


def get_validation_profile(target: str) -> ValidationProfile:
    return VALIDATION_PROFILES.get(target, VALIDATION_PROFILES["research"])
