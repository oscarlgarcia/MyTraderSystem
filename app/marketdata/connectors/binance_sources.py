"""
Feed-scoped Binance source wrappers.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.ingestion.sources import BinanceSource


@dataclass
class BinanceTradeSource(BinanceSource):
    def __post_init__(self) -> None:
        self.stream_types = ("trade",)
        super().__post_init__()


@dataclass
class BinanceBarSource(BinanceSource):
    def __post_init__(self) -> None:
        self.stream_types = ("kline",)
        super().__post_init__()
