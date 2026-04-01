"""
Canonical market data contracts and adapters.
"""

from .models import (
    BarEvent,
    BaseMarketEvent,
    BookEvent,
    CanonicalMarketEvent,
    IngestionEvent,
    TradeEvent,
    ensure_legacy_market_event,
    legacy_market_event_to_bar,
    legacy_market_event_to_trade,
    typed_event_to_legacy,
)
from .normalization import NORMALIZER_VERSION, SUPPORTED_NORMALIZER_VERSIONS, resolve_normalizer_version
from .raw_sink import JsonlRawSink, NullRawSink, RawRecord, RawSink
from .gaps import GapObservation, detect_gap
from .recovery import BarRecoveryPolicy, RecoveryPolicy, TradeRecoveryPolicy, recovery_policy_for_event
from .support_matrix import (
    FEED_SUPPORT_MATRIX,
    FeedSupport,
    feed_support,
    normalize_feed_types,
    validate_live_feed_support,
)
from .temporal_state import (
    CursorState,
    TemporalPartitionKey,
    TemporalStateStore,
    TemporalStreamState,
    cursor_from_event,
    temporal_partition_key,
)
from .validators import (
    validate_bar_event,
    validate_book_event,
    validate_ingestion_event,
    validate_kline_payload,
    validate_trade_event,
    validate_trade_payload,
)

__all__ = [
    "BaseMarketEvent",
    "TradeEvent",
    "BarEvent",
    "BookEvent",
    "CanonicalMarketEvent",
    "IngestionEvent",
    "legacy_market_event_to_trade",
    "legacy_market_event_to_bar",
    "typed_event_to_legacy",
    "ensure_legacy_market_event",
    "NORMALIZER_VERSION",
    "SUPPORTED_NORMALIZER_VERSIONS",
    "resolve_normalizer_version",
    "TemporalPartitionKey",
    "TemporalStreamState",
    "TemporalStateStore",
    "CursorState",
    "cursor_from_event",
    "temporal_partition_key",
    "RawRecord",
    "RawSink",
    "NullRawSink",
    "JsonlRawSink",
    "GapObservation",
    "detect_gap",
    "FeedSupport",
    "FEED_SUPPORT_MATRIX",
    "feed_support",
    "normalize_feed_types",
    "validate_live_feed_support",
    "RecoveryPolicy",
    "TradeRecoveryPolicy",
    "BarRecoveryPolicy",
    "recovery_policy_for_event",
    "validate_trade_payload",
    "validate_kline_payload",
    "validate_trade_event",
    "validate_bar_event",
    "validate_book_event",
    "validate_ingestion_event",
]
