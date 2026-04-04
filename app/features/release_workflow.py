from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.features.release_checks import FeatureReleaseGateReport
from app.features.releases import FeatureReleaseRegistry, ReleasedFeatureSet


@dataclass(frozen=True)
class ReleaseWorkflowResult:
    action: str
    released: ReleasedFeatureSet
    gate_report: FeatureReleaseGateReport | None = None


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
