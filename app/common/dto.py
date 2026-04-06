from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional, Tuple


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
    """Normalized market data event with explicit temporal semantics."""

    symbol: str
    event_ts: datetime
    price: float
    size: float
    source: Literal["trade", "kline", "book"]
    metadata: Dict[str, str] = field(default_factory=dict)
    published_ts: Optional[datetime] = None
    available_ts: Optional[datetime] = None
    processed_ts: Optional[datetime] = None
    observation_ts: Optional[datetime] = None
    _explicit_published_ts: bool = field(init=False, repr=False)
    _explicit_available_ts: bool = field(init=False, repr=False)
    _explicit_processed_ts: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        self._explicit_published_ts = self.published_ts is not None
        self._explicit_available_ts = self.available_ts is not None
        self._explicit_processed_ts = self.processed_ts is not None
        if self.event_ts.tzinfo is None or self.event_ts.tzinfo.utcoffset(self.event_ts) is None:
            raise ValueError("event_ts must be timezone-aware (UTC)")
        if self.price < 0:
            raise ValueError("price must be non-negative")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        self.published_ts = self.published_ts or self.event_ts
        self.available_ts = self.available_ts or self.published_ts
        self.processed_ts = self.processed_ts or self.available_ts
        self.observation_ts = self.observation_ts or self.event_ts
        for attr in ("published_ts", "available_ts", "processed_ts", "observation_ts"):
            value = getattr(self, attr)
            if value is None or value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                raise ValueError(f"{attr} must be timezone-aware (UTC)")

    @property
    def has_explicit_available_ts(self) -> bool:
        return self._explicit_available_ts or self._explicit_published_ts or self._explicit_processed_ts


@dataclass(slots=True)
class FeatureVector:
    """Derived features for a given entity and timestamp."""

    symbol: str
    ts: datetime
    values: Dict[str, float]
    feature_set_name: str = "legacy"
    feature_set_version: str = "legacy"
    available_ts: Optional[datetime] = None
    source_cutoff_ts: Optional[datetime] = None
    lineage_id: str = ""
    quality_flags: Tuple[str, ...] = ()
    entity_keys: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")
        self.available_ts = self.available_ts or self.ts
        self.source_cutoff_ts = self.source_cutoff_ts or self.ts
        if self.available_ts.tzinfo is None or self.available_ts.tzinfo.utcoffset(self.available_ts) is None:
            raise ValueError("available_ts must be timezone-aware (UTC)")
        if self.source_cutoff_ts.tzinfo is None or self.source_cutoff_ts.tzinfo.utcoffset(self.source_cutoff_ts) is None:
            raise ValueError("source_cutoff_ts must be timezone-aware (UTC)")
        if not self.entity_keys:
            self.entity_keys = {"symbol": self.symbol}
        self.quality_flags = tuple(self.quality_flags)


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
    metadata: Dict[str, str] = field(default_factory=dict)

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
    metadata: Dict[str, str] = field(default_factory=dict)

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
    metadata: Dict[str, str] = field(default_factory=dict)

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
    positions: Dict[str, float]
    cash: float
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    open_orders: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("ts must be timezone-aware (UTC)")

    def total_value(self) -> float:
        return self.cash + self.unrealized_pnl + self.realized_pnl
