from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.features.parity import ParityReport


@dataclass(frozen=True)
class FeatureSLOs:
    max_staleness_seconds: float
    max_parity_mismatches: int
    max_compute_latency_seconds: float


def paper_feature_slos() -> FeatureSLOs:
    return FeatureSLOs(max_staleness_seconds=300.0, max_parity_mismatches=0, max_compute_latency_seconds=0.5)


def live_feature_slos() -> FeatureSLOs:
    return FeatureSLOs(max_staleness_seconds=30.0, max_parity_mismatches=0, max_compute_latency_seconds=0.1)


def evaluate_release_blocking(*, parity_report: ParityReport, stale_count: int, latency_breaches: int, target: str) -> tuple[bool, tuple[str, ...]]:
    slos = paper_feature_slos() if target == "paper" else live_feature_slos()
    reasons = []
    if len(parity_report.mismatches) > slos.max_parity_mismatches:
        reasons.append("parity_mismatch")
    if stale_count > 0:
        reasons.append("stale_features_detected")
    if latency_breaches > 0:
        reasons.append("latency_slo_breached")
    return (not reasons, tuple(reasons))
