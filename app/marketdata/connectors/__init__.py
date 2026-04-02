"""
Feed-specific connector adapters for market data ingestion.
"""

from .binance import (
    BINANCE_FEED_NORMALIZERS,
    BinanceBarNormalizer,
    BinanceTradeNormalizer,
    assert_binance_payload_schema,
    build_binance_stream,
    normalize_binance_event,
    snapshot_payload_from_row,
)

__all__ = [
    "BINANCE_FEED_NORMALIZERS",
    "BinanceTradeNormalizer",
    "BinanceBarNormalizer",
    "assert_binance_payload_schema",
    "build_binance_stream",
    "normalize_binance_event",
    "snapshot_payload_from_row",
]
