from __future__ import annotations

from dataclasses import dataclass

from app.observability.alerts import ALERT_SPECS


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
    "gap_irreparable",
    "recovery_exactness_violation",
    "schema_drift_detected",
    "provider_metadata_drift",
    "marketdata_anomaly_detected",
    "compaction_backlog_high",
    "compaction_failure_detected",
    "shadow_semantic_diff",
)


@dataclass(frozen=True, slots=True)
class ObservabilityContractReport:
    required_metrics: tuple[str, ...]
    required_alerts: tuple[str, ...]
    missing_alerts: tuple[str, ...]
    pass_ok: bool


def build_observability_contract_report() -> ObservabilityContractReport:
    missing_alerts = tuple(sorted(alert for alert in REQUIRED_INGESTION_ALERTS if alert not in ALERT_SPECS))
    return ObservabilityContractReport(
        required_metrics=REQUIRED_INGESTION_METRICS,
        required_alerts=REQUIRED_INGESTION_ALERTS,
        missing_alerts=missing_alerts,
        pass_ok=not missing_alerts,
    )
