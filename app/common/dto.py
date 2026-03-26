"""
Data Transfer Objects (DTOs) used across the trading system.

These dataclasses are intentionally stdlib-only to stay lightweight and easily serializable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(timezone.utc)


def normalize_symbol(symbol: str) -> str:
    """Normalize trading symbols to uppercase without surrounding spaces."""
    return symbol.strip().upper()


@dataclass(slots=True)
class TraceContext:
    """Correlation context for logs/metrics/traces."""

    trace_id: str
    span_id: Optional[str] = None


@dataclass(slots=True)
class MarketEvent:
    """Normalized market data event."""

    symbol: str
    event_ts: datetime
    price: float
    size: float
    source: Literal["trade", "kline", "book"]
    metadata: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.event_ts.tzinfo is None or self.event_ts.tzinfo.utcoffset(self.event_ts) is None:
            raise ValueError("event_ts must be timezone-aware (UTC)")
        if self.price < 0:
            raise ValueError("price must be non-negative")
        if self.size < 0:
            raise ValueError("size must be non-negative")


@dataclass(slots=True)
class FeatureVector:
    """Derived features for a given symbol and timestamp."""

    symbol: str
    ts: datetime
    values: Dict[str, float]

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")


@dataclass(slots=True)
class Signal:
    """Strategy signal output."""

    symbol: str
    ts: datetime
    side: Literal["buy", "sell", "flat"]
    size: float
    confidence: float = 1.0
    ttl_seconds: Optional[int] = None
    strategy_id: str = "default"

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if self.side not in {"buy", "sell", "flat"}:
            raise ValueError("side must be buy, sell, or flat")


@dataclass(slots=True)
class OrderIntent:
    """Order request after risk checks."""

    symbol: str
    ts: datetime
    side: Literal["buy", "sell"]
    quantity: float
    price_limit: Optional[float] = None
    time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC"
    intent_id: str = ""
    strategy_id: str = "default"

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.time_in_force not in {"GTC", "IOC", "FOK"}:
            raise ValueError("time_in_force must be GTC, IOC, or FOK")


@dataclass(slots=True)
class ExecutionReport:
    """Execution status returned by the exchange adapter."""

    symbol: str
    ts: datetime
    status: Literal["accepted", "partial", "filled", "rejected", "cancelled"]
    filled_qty: float
    avg_price: float
    client_order_id: str
    exchange_order_id: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")
        if self.filled_qty < 0:
            raise ValueError("filled_qty must be non-negative")
        if self.avg_price < 0:
            raise ValueError("avg_price must be non-negative")
        if self.status not in {"accepted", "partial", "filled", "rejected", "cancelled"}:
            raise ValueError("invalid status")


@dataclass(slots=True)
class PortfolioState:
    """Aggregate portfolio state used for risk and reporting."""

    ts: datetime
    positions: Dict[str, float]  # symbol -> position size (base units)
    cash: float
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    open_orders: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")

    def total_value(self) -> float:
        """Compute a conservative total value using realized + unrealized + cash."""
        return self.cash + self.unrealized_pnl + self.realized_pnl
