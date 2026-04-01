"""
Explicit support matrix for live ingestion feeds.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeedSupport:
    feed_type: str
    supports_live: bool
    supports_exact_recovery: bool
    supports_handoff: bool


FEED_SUPPORT_MATRIX: dict[str, FeedSupport] = {
    "trade": FeedSupport(
        feed_type="trade",
        supports_live=False,
        supports_exact_recovery=False,
        supports_handoff=False,
    ),
    "kline": FeedSupport(
        feed_type="kline",
        supports_live=True,
        supports_exact_recovery=True,
        supports_handoff=True,
    ),
    "book": FeedSupport(
        feed_type="book",
        supports_live=False,
        supports_exact_recovery=False,
        supports_handoff=False,
    ),
}


def normalize_feed_types(stream_types: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if not stream_types:
        return ("kline",)
    normalized = tuple(str(stream_type).strip().lower() for stream_type in stream_types if str(stream_type).strip())
    if not normalized:
        raise ValueError("ingest stream types cannot be empty")
    unknown = [stream_type for stream_type in normalized if stream_type not in FEED_SUPPORT_MATRIX]
    if unknown:
        raise ValueError(f"unsupported ingest stream types: {', '.join(sorted(set(unknown)))}")
    return normalized


def feed_support(feed_type: str) -> FeedSupport:
    normalized = str(feed_type).strip().lower()
    if normalized not in FEED_SUPPORT_MATRIX:
        raise ValueError(f"unsupported ingest stream type: {feed_type}")
    return FEED_SUPPORT_MATRIX[normalized]


def validate_live_feed_support(
    stream_types: tuple[str, ...] | list[str] | None,
    *,
    require_exact_recovery: bool,
    require_handoff: bool,
) -> tuple[str, ...]:
    normalized = normalize_feed_types(stream_types)
    errors: list[str] = []
    for stream_type in normalized:
        support = feed_support(stream_type)
        if not support.supports_live:
            errors.append(f"{stream_type} does not support live ingestion")
            continue
        if require_exact_recovery and not support.supports_exact_recovery:
            errors.append(f"{stream_type} does not support exact recovery")
        if require_handoff and not support.supports_handoff:
            errors.append(f"{stream_type} does not support historical-to-live handoff")
    if errors:
        raise ValueError("; ".join(errors))
    return normalized
