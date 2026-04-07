from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import httpx

from app.features.metrics import FeatureMetrics

logger = logging.getLogger("features.observability")


@dataclass(frozen=True)
class FeatureObservabilityBundle:
    target: str
    generated_at: datetime
    metrics: dict[str, Any]
    alerts: tuple[str, ...]


class FeatureObservabilitySink(Protocol):
    def emit(self, bundle: FeatureObservabilityBundle) -> object:
        ...


class MemoryObservabilitySink:
    def __init__(self) -> None:
        self.bundles: list[FeatureObservabilityBundle] = []

    def emit(self, bundle: FeatureObservabilityBundle) -> FeatureObservabilityBundle:
        self.bundles.append(bundle)
        return bundle


class CompositeObservabilitySink:
    def __init__(self, *sinks: FeatureObservabilitySink) -> None:
        self.sinks = sinks

    def emit(self, bundle: FeatureObservabilityBundle) -> tuple[object, ...]:
        return tuple(sink.emit(bundle) for sink in self.sinks)


class JsonlObservabilitySink:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, bundle: FeatureObservabilityBundle) -> Path:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "target": bundle.target,
                        "generated_at": bundle.generated_at.isoformat(),
                        "metrics": bundle.metrics,
                        "alerts": list(bundle.alerts),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
        return self.path


class LoggerObservabilitySink:
    def __init__(self, *, sink_logger: logging.Logger | None = None) -> None:
        self.logger = sink_logger or logger

    def emit(self, bundle: FeatureObservabilityBundle) -> FeatureObservabilityBundle:
        self.logger.info(
            "feature observability bundle",
            extra={
                "target": bundle.target,
                "generated_at": bundle.generated_at.isoformat(),
                "metrics": bundle.metrics,
                "alerts": list(bundle.alerts),
            },
        )
        return bundle


class HttpObservabilitySink:
    def __init__(self, url: str, *, timeout_seconds: float = 5.0) -> None:
        self.url = url
        self.timeout_seconds = timeout_seconds

    def emit(self, bundle: FeatureObservabilityBundle) -> dict[str, Any]:
        payload = {
            "target": bundle.target,
            "generated_at": bundle.generated_at.isoformat(),
            "metrics": bundle.metrics,
            "alerts": list(bundle.alerts),
        }
        response = httpx.post(self.url, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        return payload


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
    if metrics.contract_validation_failures > 0:
        alerts.append("training_serving_contract_failures_detected")
    return FeatureObservabilityBundle(
        target=target,
        generated_at=datetime.now(timezone.utc),
        metrics=metrics.as_dict(),
        alerts=tuple(alerts),
    )


def emit_feature_observability_bundle(
    *,
    metrics: FeatureMetrics,
    target: str,
    sink: FeatureObservabilitySink,
) -> object:
    bundle = build_feature_observability_bundle(metrics=metrics, target=target)
    return sink.emit(bundle)


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
