"""
Explicit support matrix for live ingestion feeds.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


RecoveryCapability = Literal["none", "approximate", "exact", "exact_verified"]
PaperValidationBasis = Literal["none", "replay_validated", "runtime_validated"]
LiveValidationBasis = Literal["none", "runtime_validated"]
OperationalTier = Literal["supported", "experimental", "excluded"]


@dataclass(frozen=True, slots=True)
class FeedSupport:
    feed_type: str
    operational_tier: OperationalTier
    supports_paper: bool
    paper_validation_basis: PaperValidationBasis
    supports_live: bool
    live_validation_basis: LiveValidationBasis
    recovery_capability: RecoveryCapability
    supports_handoff: bool
    paper_scope_note: str = ""
    live_scope_note: str = ""

    @property
    def supports_exact_recovery(self) -> bool:
        return self.recovery_capability in {"exact", "exact_verified"}

    @property
    def supports_exact_verified_recovery(self) -> bool:
        return self.recovery_capability == "exact_verified"


FEED_SUPPORT_MATRIX: dict[str, FeedSupport] = {
    "trade": FeedSupport(
        feed_type="trade",
        operational_tier="supported",
        supports_paper=True,
        paper_validation_basis="replay_validated",
        supports_live=True,
        live_validation_basis="runtime_validated",
        recovery_capability="exact_verified",
        supports_handoff=True,
        paper_scope_note="trade is supported for paper via replay parity, vendor contracts, and storage validation",
        live_scope_note="trade live uses aggregate-trade runtime validation, exact recovery, and historical-to-live handoff via aggregate trade ids",
    ),
    "kline": FeedSupport(
        feed_type="kline",
        operational_tier="supported",
        supports_paper=True,
        paper_validation_basis="runtime_validated",
        supports_live=True,
        live_validation_basis="runtime_validated",
        recovery_capability="exact_verified",
        supports_handoff=True,
        paper_scope_note="kline paper readiness requires runtime canary and soak evidence",
        live_scope_note="kline is the only feed currently approved for live runtime and production-mode gating",
    ),
    "book": FeedSupport(
        feed_type="book",
        operational_tier="excluded",
        supports_paper=False,
        paper_validation_basis="none",
        supports_live=False,
        live_validation_basis="none",
        recovery_capability="none",
        supports_handoff=False,
        paper_scope_note="book is explicitly excluded from the supported paper contract until a dedicated runtime, schema, and recovery strategy exist",
        live_scope_note="book is explicitly excluded from the supported live contract until a dedicated runtime, schema, and recovery strategy exist",
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


def paper_supported_feed_types() -> tuple[str, ...]:
    return tuple(stream_type for stream_type, support in FEED_SUPPORT_MATRIX.items() if support.supports_paper)


def live_supported_feed_types() -> tuple[str, ...]:
    return tuple(stream_type for stream_type, support in FEED_SUPPORT_MATRIX.items() if support.supports_live)


def runtime_validated_paper_feed_types() -> tuple[str, ...]:
    return tuple(
        stream_type
        for stream_type, support in FEED_SUPPORT_MATRIX.items()
        if support.supports_paper and support.paper_validation_basis == "runtime_validated"
    )


def replay_validated_paper_feed_types() -> tuple[str, ...]:
    return tuple(
        stream_type
        for stream_type, support in FEED_SUPPORT_MATRIX.items()
        if support.supports_paper and support.paper_validation_basis == "replay_validated"
    )


def runtime_validated_live_feed_types() -> tuple[str, ...]:
    return tuple(
        stream_type
        for stream_type, support in FEED_SUPPORT_MATRIX.items()
        if support.supports_live and support.live_validation_basis == "runtime_validated"
    )


def excluded_feed_types() -> tuple[str, ...]:
    return tuple(
        stream_type for stream_type, support in FEED_SUPPORT_MATRIX.items() if support.operational_tier == "excluded"
    )


def validate_paper_feed_support(stream_types: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    normalized = normalize_feed_types(stream_types)
    errors: list[str] = []
    for stream_type in normalized:
        support = feed_support(stream_type)
        if not support.supports_paper:
            errors.append(f"{stream_type} does not support paper ingestion")
    if errors:
        raise ValueError("; ".join(errors))
    return normalized


def validate_live_feed_support(
    stream_types: tuple[str, ...] | list[str] | None,
    *,
    require_exact_recovery: bool,
    require_exact_verified: bool = False,
    require_handoff: bool,
) -> tuple[str, ...]:
    normalized = normalize_feed_types(stream_types)
    errors: list[str] = []
    for stream_type in normalized:
        support = feed_support(stream_type)
        if not support.supports_live:
            errors.append(f"{stream_type} does not support live ingestion")
            continue
        if require_exact_verified and not support.supports_exact_verified_recovery:
            errors.append(f"{stream_type} does not support exact_verified recovery")
            continue
        if require_exact_recovery and not support.supports_exact_recovery:
            errors.append(f"{stream_type} does not support exact recovery")
        if require_handoff and not support.supports_handoff:
            errors.append(f"{stream_type} does not support historical-to-live handoff")
    if errors:
        raise ValueError("; ".join(errors))
    return normalized
