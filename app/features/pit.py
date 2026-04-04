from __future__ import annotations

from app.common.dto import FeatureVector, MarketEvent


def ensure_point_in_time_safe(*, decision_ts, available_ts, context: str = "") -> None:
    if available_ts > decision_ts:
        suffix = f" for {context}" if context else ""
        raise ValueError(f"point-in-time violation: available_ts exceeds decision_ts{suffix}")


def feature_vector_is_servable_at(feature: FeatureVector, decision_ts) -> bool:
    return feature.available_ts <= decision_ts and feature.ts <= decision_ts


def event_is_available_at(event: MarketEvent, decision_ts) -> bool:
    return event.available_ts <= decision_ts and event.event_ts <= decision_ts
