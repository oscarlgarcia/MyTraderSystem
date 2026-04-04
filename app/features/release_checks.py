from __future__ import annotations

from dataclasses import dataclass

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport


@dataclass(frozen=True)
class FeatureSLOs:
    max_staleness_seconds: float
    max_parity_mismatches: int
    max_compute_latency_seconds: float


@dataclass(frozen=True)
class FeatureReleaseGateReport:
    pass_ok: bool
    target: str
    stale_count: int
    latency_breaches: int
    reasons: tuple[str, ...]


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


def run_feature_release_gate(*, parity_report: ParityReport, metrics: FeatureMetrics, target: str) -> FeatureReleaseGateReport:
    slos = paper_feature_slos() if target == "paper" else live_feature_slos()
    stale_count = metrics.stale_serves
    latency_breaches = 1 if metrics.serving_latency_max > slos.max_compute_latency_seconds else 0
    pass_ok, reasons = evaluate_release_blocking(
        parity_report=parity_report,
        stale_count=stale_count,
        latency_breaches=latency_breaches,
        target=target,
    )
    return FeatureReleaseGateReport(
        pass_ok=pass_ok,
        target=target,
        stale_count=stale_count,
        latency_breaches=latency_breaches,
        reasons=reasons,
    )
