from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal


AlertSeverity = Literal["warning", "error"]
AlertType = Literal[
    "reconnect_storm",
    "gap_detected",
    "gap_irreparable",
    "heartbeat_missed",
    "dlq_spike",
    "sink_failure",
]


@dataclass(frozen=True)
class AlertSpec:
    severity: AlertSeverity
    threshold: int
    recommended_action: str


ALERT_SPECS: dict[AlertType, AlertSpec] = {
    "reconnect_storm": AlertSpec(
        severity="warning",
        threshold=3,
        recommended_action="Inspect source connectivity and vendor availability before continuing.",
    ),
    "gap_detected": AlertSpec(
        severity="warning",
        threshold=1,
        recommended_action="Verify feed continuity and recovery behavior for the affected stream.",
    ),
    "gap_irreparable": AlertSpec(
        severity="error",
        threshold=1,
        recommended_action="Treat the stream as degraded and do not assume data continuity.",
    ),
    "heartbeat_missed": AlertSpec(
        severity="warning",
        threshold=1,
        recommended_action="Reconnect the stream and verify liveness thresholds for the connector.",
    ),
    "dlq_spike": AlertSpec(
        severity="warning",
        threshold=3,
        recommended_action="Inspect invalid payloads and schema drift before trusting the stream.",
    ),
    "sink_failure": AlertSpec(
        severity="error",
        threshold=1,
        recommended_action="Stop trusting persistence until sink health is restored.",
    ),
}


def alert_spec(alert_type: AlertType) -> AlertSpec:
    return ALERT_SPECS[alert_type]


def should_emit_threshold_alert(alert_type: AlertType, observed: int) -> bool:
    threshold = ALERT_SPECS[alert_type].threshold
    if observed < threshold:
        return False
    if threshold <= 1:
        return True
    return observed == threshold or observed % threshold == 0


def emit_operational_alert(
    logger: logging.Logger,
    *,
    alert_type: AlertType,
    observed: int | float,
    extra: dict[str, object] | None = None,
) -> None:
    spec = ALERT_SPECS[alert_type]
    level = logging.ERROR if spec.severity == "error" else logging.WARNING
    payload = {
        "alert_type": alert_type,
        "alert_severity": spec.severity,
        "observed": observed,
        "threshold": spec.threshold,
        "recommended_action": spec.recommended_action,
    }
    if extra:
        payload.update(extra)
    logger.log(level, "operational alert", extra=payload)
