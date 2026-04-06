from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import FeatureReleaseGateReport, run_feature_release_gate
from app.features.releases import FeatureReleaseRegistry, ReleasedFeatureSet


@dataclass(frozen=True)
class ReleaseWorkflowResult:
    action: str
    released: ReleasedFeatureSet
    gate_report: FeatureReleaseGateReport | None = None
    audit_path: Path | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _audit_path_for(registry_path: str | Path) -> Path:
    registry = Path(registry_path)
    return registry.with_name(f"{registry.stem}_audit.jsonl")


def _append_audit_event(
    *,
    registry_path: str | Path,
    action: str,
    released: ReleasedFeatureSet,
    target: str | None,
    gate_report: FeatureReleaseGateReport | None,
    actor: str,
) -> Path:
    audit_path = _audit_path_for(registry_path)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "timestamp": _utc_now().isoformat(),
        "action": action,
        "feature_set_name": released.name,
        "active_version": released.active_version,
        "previous_version": released.previous_version,
        "target": target,
        "actor": actor,
    }
    if gate_report is not None:
        payload["gate_report"] = {
            "pass_ok": gate_report.pass_ok,
            "target": gate_report.target,
            "stale_count": gate_report.stale_count,
            "latency_breaches": gate_report.latency_breaches,
            "invalid_ratio": gate_report.invalid_ratio,
            "invalid_ratio_breaches": gate_report.invalid_ratio_breaches,
            "cardinality_breaches": gate_report.cardinality_breaches,
            "reasons": list(gate_report.reasons),
        }
    with audit_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return audit_path


def gate_and_publish_feature_release(
    *,
    registry_path: str | Path,
    feature_set_name: str,
    version: str,
    parity_report: ParityReport,
    metrics: FeatureMetrics,
    target: str,
    actor: str = "system",
) -> ReleaseWorkflowResult:
    gate_report = run_feature_release_gate(parity_report=parity_report, metrics=metrics, target=target)
    return publish_feature_release(
        registry_path=registry_path,
        feature_set_name=feature_set_name,
        version=version,
        gate_report=gate_report,
        target=target,
        actor=actor,
    )


def publish_feature_release(
    *,
    registry_path: str | Path,
    feature_set_name: str,
    version: str,
    gate_report: FeatureReleaseGateReport,
    target: str | None = None,
    actor: str = "system",
) -> ReleaseWorkflowResult:
    if not gate_report.pass_ok:
        raise ValueError(f"release gate failed for {feature_set_name}: {', '.join(gate_report.reasons)}")
    registry = FeatureReleaseRegistry(registry_path)
    released = registry.activate(name=feature_set_name, version=version)
    audit_path = _append_audit_event(
        registry_path=registry_path,
        action="publish",
        released=released,
        target=target or gate_report.target,
        gate_report=gate_report,
        actor=actor,
    )
    return ReleaseWorkflowResult(action="publish", released=released, gate_report=gate_report, audit_path=audit_path)


def rollback_feature_release(
    *,
    registry_path: str | Path,
    feature_set_name: str,
    target: str | None = None,
    actor: str = "system",
) -> ReleaseWorkflowResult:
    registry = FeatureReleaseRegistry(registry_path)
    released = registry.rollback(name=feature_set_name)
    audit_path = _append_audit_event(
        registry_path=registry_path,
        action="rollback",
        released=released,
        target=target,
        gate_report=None,
        actor=actor,
    )
    return ReleaseWorkflowResult(action="rollback", released=released, gate_report=None, audit_path=audit_path)
