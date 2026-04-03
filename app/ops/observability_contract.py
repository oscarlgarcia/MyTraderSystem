from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.observability.alerts import ALERT_SPECS, AlertSeverity


ReleaseTarget = Literal["paper", "live"]

REQUIRED_INGESTION_METRICS: tuple[str, ...] = (
    "messages_in_total",
    "messages_invalid_total",
    "duplicates_total",
    "gaps_total",
    "gap_irreparable_total",
    "reconnects_total",
    "heartbeat_missed_total",
    "exchange_receive_skew_seconds",
    "receive_process_skew_seconds",
    "invalid_timestamp_total",
    "marketdata_anomaly_total",
    "segments_pending_total",
    "compaction_lag_seconds",
    "compaction_failures_total",
)

REQUIRED_INGESTION_ALERTS: tuple[str, ...] = (
    "reconnect_storm",
    "heartbeat_missed",
    "gap_irreparable",
    "recovery_exactness_violation",
    "invalid_timestamp_detected",
    "exchange_receive_skew_high",
    "receive_process_skew_high",
    "schema_drift_detected",
    "provider_metadata_drift",
    "marketdata_anomaly_detected",
    "compaction_backlog_high",
    "compaction_failure_detected",
    "shadow_semantic_diff",
)


@dataclass(frozen=True, slots=True)
class MetricThresholdSpec:
    unit: str
    paper_warning: float
    paper_critical: float
    live_warning: float
    live_critical: float


@dataclass(frozen=True, slots=True)
class AlertContractSpec:
    severity: AlertSeverity
    threshold: int
    recommended_action: str


@dataclass(frozen=True, slots=True)
class ObservabilityContractReport:
    target: ReleaseTarget
    required_metrics: tuple[str, ...]
    required_alerts: tuple[str, ...]
    required_metric_thresholds: dict[str, dict[str, float | str]]
    alert_specs: dict[str, AlertContractSpec]
    missing_alerts: tuple[str, ...]
    missing_metric_thresholds: tuple[str, ...]
    invalid_alert_specs: tuple[str, ...]
    pass_ok: bool


REQUIRED_METRIC_THRESHOLDS: dict[str, MetricThresholdSpec] = {
    "reconnects_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=3.0,
        paper_critical=5.0,
        live_warning=2.0,
        live_critical=3.0,
    ),
    "heartbeat_missed_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=1.0,
        paper_critical=2.0,
        live_warning=1.0,
        live_critical=1.0,
    ),
    "exchange_receive_skew_seconds": MetricThresholdSpec(
        unit="seconds",
        paper_warning=5.0,
        paper_critical=30.0,
        live_warning=2.0,
        live_critical=10.0,
    ),
    "receive_process_skew_seconds": MetricThresholdSpec(
        unit="seconds",
        paper_warning=1.0,
        paper_critical=5.0,
        live_warning=0.5,
        live_critical=2.0,
    ),
    "invalid_timestamp_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=1.0,
        paper_critical=1.0,
        live_warning=1.0,
        live_critical=1.0,
    ),
    "gap_irreparable_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=1.0,
        paper_critical=1.0,
        live_warning=1.0,
        live_critical=1.0,
    ),
    "marketdata_anomaly_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=1.0,
        paper_critical=5.0,
        live_warning=1.0,
        live_critical=3.0,
    ),
    "compaction_lag_seconds": MetricThresholdSpec(
        unit="seconds",
        paper_warning=300.0,
        paper_critical=900.0,
        live_warning=120.0,
        live_critical=300.0,
    ),
    "compaction_failures_total": MetricThresholdSpec(
        unit="count_per_run",
        paper_warning=1.0,
        paper_critical=1.0,
        live_warning=1.0,
        live_critical=1.0,
    ),
}


def _threshold_payload(target: ReleaseTarget, spec: MetricThresholdSpec) -> dict[str, float | str]:
    if target == "live":
        return {
            "unit": spec.unit,
            "warning": spec.live_warning,
            "critical": spec.live_critical,
        }
    return {
        "unit": spec.unit,
        "warning": spec.paper_warning,
        "critical": spec.paper_critical,
    }


def build_observability_contract_report(*, target: ReleaseTarget = "paper") -> ObservabilityContractReport:
    missing_alerts = tuple(sorted(alert for alert in REQUIRED_INGESTION_ALERTS if alert not in ALERT_SPECS))
    missing_metric_thresholds = tuple(sorted(metric for metric in REQUIRED_METRIC_THRESHOLDS if metric not in REQUIRED_INGESTION_METRICS))
    invalid_alert_specs: list[str] = []
    alert_specs: dict[str, AlertContractSpec] = {}
    for alert_name in REQUIRED_INGESTION_ALERTS:
        spec = ALERT_SPECS.get(alert_name)
        if spec is None:
            continue
        if spec.threshold <= 0:
            invalid_alert_specs.append(f"{alert_name}: threshold must be > 0")
        alert_specs[alert_name] = AlertContractSpec(
            severity=spec.severity,
            threshold=spec.threshold,
            recommended_action=spec.recommended_action,
        )
    threshold_payload = {
        metric: _threshold_payload(target, spec)
        for metric, spec in REQUIRED_METRIC_THRESHOLDS.items()
    }
    return ObservabilityContractReport(
        target=target,
        required_metrics=REQUIRED_INGESTION_METRICS,
        required_alerts=REQUIRED_INGESTION_ALERTS,
        required_metric_thresholds=threshold_payload,
        alert_specs=alert_specs,
        missing_alerts=missing_alerts,
        missing_metric_thresholds=missing_metric_thresholds,
        invalid_alert_specs=tuple(sorted(invalid_alert_specs)),
        pass_ok=not missing_alerts and not missing_metric_thresholds and not invalid_alert_specs,
    )
