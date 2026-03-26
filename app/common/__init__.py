"""
Common utilities, constants, and shared types live here.

Keep imports minimal to avoid dependency cycles across packages.
"""

from app.common.dto import (
    ExecutionReport,
    FeatureVector,
    MarketEvent,
    OrderIntent,
    PortfolioState,
    Signal,
    TraceContext,
    normalize_symbol,
    utc_now,
)

__all__ = [
    "MarketEvent",
    "FeatureVector",
    "Signal",
    "OrderIntent",
    "ExecutionReport",
    "PortfolioState",
    "TraceContext",
    "normalize_symbol",
    "utc_now",
]
