from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.features.metrics import FeatureMetrics


@dataclass(frozen=True)
class FeatureObservabilityBundle:
    target: str
    generated_at: datetime
    metrics: dict[str, Any]
    alerts: tuple[str, ...]


def build_feature_observability_bundle(*, metrics: FeatureMetrics, target: str) -> FeatureObservabilityBundle:
    alerts = []
    if metrics.stale_serves > 0:
        alerts.append("stale_features_detected")
    if metrics.invalid_serves > 0:
        alerts.append("invalid_features_detected")
    if metrics.parity_mismatches > 0:
        alerts.append("feature_parity_mismatch_detected")
    if metrics.shadow_failures > 0:
        alerts.append("shadow_divergence_detected")
    return FeatureObservabilityBundle(
        target=target,
        generated_at=datetime.now(timezone.utc),
        metrics=metrics.as_dict(),
        alerts=tuple(alerts),
    )


def export_feature_observability_bundle(
    *,
    metrics: FeatureMetrics,
    target: str,
    output_path: str | Path,
) -> Path:
    bundle = build_feature_observability_bundle(metrics=metrics, target=target)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "target": bundle.target,
                "generated_at": bundle.generated_at.isoformat(),
                "metrics": bundle.metrics,
                "alerts": list(bundle.alerts),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path
