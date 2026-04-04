from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import FeatureReleaseGateReport, run_feature_release_gate
from app.features.releases import FeatureReleaseRegistry, ReleasedFeatureSet


@dataclass(frozen=True)
class ReleaseWorkflowResult:
    action: str
    released: ReleasedFeatureSet
    gate_report: FeatureReleaseGateReport | None = None


def gate_and_publish_feature_release(
    *,
    registry_path: str | Path,
    feature_set_name: str,
    version: str,
    parity_report: ParityReport,
    metrics: FeatureMetrics,
    target: str,
) -> ReleaseWorkflowResult:
    gate_report = run_feature_release_gate(parity_report=parity_report, metrics=metrics, target=target)
    return publish_feature_release(
        registry_path=registry_path,
        feature_set_name=feature_set_name,
        version=version,
        gate_report=gate_report,
    )


def publish_feature_release(*, registry_path: str | Path, feature_set_name: str, version: str, gate_report: FeatureReleaseGateReport) -> ReleaseWorkflowResult:
    if not gate_report.pass_ok:
        raise ValueError(f"release gate failed for {feature_set_name}: {', '.join(gate_report.reasons)}")
    registry = FeatureReleaseRegistry(registry_path)
    released = registry.activate(name=feature_set_name, version=version)
    return ReleaseWorkflowResult(action="publish", released=released, gate_report=gate_report)


def rollback_feature_release(*, registry_path: str | Path, feature_set_name: str) -> ReleaseWorkflowResult:
    registry = FeatureReleaseRegistry(registry_path)
    released = registry.rollback(name=feature_set_name)
    return ReleaseWorkflowResult(action="rollback", released=released, gate_report=None)
