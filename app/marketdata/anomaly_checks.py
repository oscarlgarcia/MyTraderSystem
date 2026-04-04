from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.marketdata.models import IngestionEvent


AnomalySeverity = Literal["warn", "quarantine", "fail"]
FeedType = Literal["trade", "kline", "book"]
ANOMALY_ACTION_PRIORITY = {"warn": 0, "quarantine": 1, "fail": 2}


@dataclass(frozen=True, slots=True)
class MarketdataAnomaly:
    anomaly_type: str
    feed_type: str
    severity: AnomalySeverity
    action: AnomalySeverity
    previous_price: float | None = None
    current_price: float | None = None
    relative_jump: float | None = None
    previous_volume: float | None = None
    current_volume: float | None = None
    volume_ratio: float | None = None
    threshold: float | None = None


@dataclass(frozen=True, slots=True)
class FeedAnomalyPolicy:
    price_jump_warn_threshold: float
    price_jump_quarantine_threshold: float
    price_jump_fail_threshold: float
    volume_spike_warn_threshold: float
    volume_spike_quarantine_threshold: float
    volume_spike_fail_threshold: float


FEED_ANOMALY_POLICIES: dict[str, FeedAnomalyPolicy] = {
    "trade": FeedAnomalyPolicy(
        price_jump_warn_threshold=0.20,
        price_jump_quarantine_threshold=0.35,
        price_jump_fail_threshold=0.60,
        volume_spike_warn_threshold=8.0,
        volume_spike_quarantine_threshold=20.0,
        volume_spike_fail_threshold=50.0,
    ),
    "kline": FeedAnomalyPolicy(
        price_jump_warn_threshold=0.15,
        price_jump_quarantine_threshold=0.30,
        price_jump_fail_threshold=0.50,
        volume_spike_warn_threshold=4.0,
        volume_spike_quarantine_threshold=10.0,
        volume_spike_fail_threshold=20.0,
    ),
    "book": FeedAnomalyPolicy(
        price_jump_warn_threshold=0.10,
        price_jump_quarantine_threshold=0.20,
        price_jump_fail_threshold=0.35,
        volume_spike_warn_threshold=4.0,
        volume_spike_quarantine_threshold=8.0,
        volume_spike_fail_threshold=15.0,
    ),
}


def anomaly_policy_for_feed(feed_type: str) -> FeedAnomalyPolicy:
    normalized = str(feed_type).strip().lower()
    return FEED_ANOMALY_POLICIES.get(normalized, FEED_ANOMALY_POLICIES["trade"])


def detect_price_jump(
    *,
    previous_price: float | None,
    current_price: float,
    relative_jump_threshold: float = 0.20,
    feed_type: str = "trade",
    severity: AnomalySeverity = "warn",
    action: AnomalySeverity | None = None,
) -> MarketdataAnomaly | None:
    if previous_price in (None, 0):
        return None
    relative_jump = abs(float(current_price) - float(previous_price)) / abs(float(previous_price))
    if relative_jump < float(relative_jump_threshold):
        return None
    resolved_action = action or severity
    return MarketdataAnomaly(
        anomaly_type="price_jump",
        feed_type=str(feed_type),
        severity=severity,
        action=resolved_action,
        previous_price=float(previous_price),
        current_price=float(current_price),
        relative_jump=relative_jump,
        threshold=float(relative_jump_threshold),
    )


def detect_volume_spike(
    *,
    previous_volume: float | None,
    current_volume: float | None,
    volume_ratio_threshold: float,
    feed_type: str,
    severity: AnomalySeverity,
    action: AnomalySeverity | None = None,
) -> MarketdataAnomaly | None:
    if previous_volume in (None, 0) or current_volume in (None, 0):
        return None
    volume_ratio = abs(float(current_volume)) / abs(float(previous_volume))
    if volume_ratio < float(volume_ratio_threshold):
        return None
    resolved_action = action or severity
    return MarketdataAnomaly(
        anomaly_type="volume_spike",
        feed_type=str(feed_type),
        severity=severity,
        action=resolved_action,
        previous_volume=float(previous_volume),
        current_volume=float(current_volume),
        volume_ratio=volume_ratio,
        threshold=float(volume_ratio_threshold),
    )


def event_volume_value(event: IngestionEvent) -> float | None:
    if hasattr(event, "size"):
        return float(getattr(event, "size"))
    if hasattr(event, "volume"):
        return float(getattr(event, "volume"))
    return None


def detect_marketdata_anomalies(
    *,
    event: IngestionEvent,
    previous_price: float | None,
    previous_volume: float | None,
) -> tuple[MarketdataAnomaly, ...]:
    feed_type = str(getattr(event, "source", "trade")).lower()
    policy = anomaly_policy_for_feed(feed_type)
    current_volume = event_volume_value(event)
    anomalies: list[MarketdataAnomaly] = []

    price_jump_fail = detect_price_jump(
        previous_price=previous_price,
        current_price=float(event.price),
        relative_jump_threshold=policy.price_jump_fail_threshold,
        feed_type=feed_type,
        severity="fail",
    )
    if price_jump_fail is not None:
        anomalies.append(price_jump_fail)
    else:
        price_jump_quarantine = detect_price_jump(
            previous_price=previous_price,
            current_price=float(event.price),
            relative_jump_threshold=policy.price_jump_quarantine_threshold,
            feed_type=feed_type,
            severity="quarantine",
        )
        if price_jump_quarantine is not None:
            anomalies.append(price_jump_quarantine)
        else:
            price_jump_warn = detect_price_jump(
                previous_price=previous_price,
                current_price=float(event.price),
                relative_jump_threshold=policy.price_jump_warn_threshold,
                feed_type=feed_type,
                severity="warn",
            )
            if price_jump_warn is not None:
                anomalies.append(price_jump_warn)

    volume_spike_fail = detect_volume_spike(
        previous_volume=previous_volume,
        current_volume=current_volume,
        volume_ratio_threshold=policy.volume_spike_fail_threshold,
        feed_type=feed_type,
        severity="fail",
    )
    if volume_spike_fail is not None:
        anomalies.append(volume_spike_fail)
    else:
        volume_spike_quarantine = detect_volume_spike(
            previous_volume=previous_volume,
            current_volume=current_volume,
            volume_ratio_threshold=policy.volume_spike_quarantine_threshold,
            feed_type=feed_type,
            severity="quarantine",
        )
        if volume_spike_quarantine is not None:
            anomalies.append(volume_spike_quarantine)
        else:
            volume_spike_warn = detect_volume_spike(
                previous_volume=previous_volume,
                current_volume=current_volume,
                volume_ratio_threshold=policy.volume_spike_warn_threshold,
                feed_type=feed_type,
                severity="warn",
            )
            if volume_spike_warn is not None:
                anomalies.append(volume_spike_warn)

    return tuple(anomalies)


def stream_price_key(event: IngestionEvent) -> str:
    venue = str(getattr(event, "venue", "BINANCE")).upper()
    return f"{venue}:{event.symbol}:{event.source}"


def dominant_anomaly(anomalies: tuple[MarketdataAnomaly, ...]) -> MarketdataAnomaly | None:
    if not anomalies:
        return None
    return max(anomalies, key=lambda anomaly: ANOMALY_ACTION_PRIORITY.get(anomaly.action, -1))
