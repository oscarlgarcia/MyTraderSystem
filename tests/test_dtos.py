from datetime import datetime, timezone

import pytest

from app.common.dto import (
    ExecutionReport,
    FeatureVector,
    MarketEvent,
    OrderIntent,
    PortfolioState,
    Signal,
    TraceContext,
    normalize_symbol,
)


def test_market_event_roundtrip():
    ts = datetime.now(timezone.utc)
    ev = MarketEvent(symbol="ethusdt", event_ts=ts, price=1000.5, size=1.2, source="trade")
    assert ev.symbol == "ETHUSDT"
    assert ev.event_ts is ts


def test_signal_validation_confidence():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        Signal(symbol="BTCUSDT", ts=ts, side="buy", size=1, confidence=1.5)


def test_order_intent_quantity_positive():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        OrderIntent(symbol="BTCUSDT", ts=ts, side="buy", quantity=0)

def test_order_intent_time_in_force_validation():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        OrderIntent(symbol="BTCUSDT", ts=ts, side="buy", quantity=1, time_in_force="BAD")  # type: ignore[arg-type]


def test_execution_report_non_negative():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        ExecutionReport(
            symbol="BTCUSDT",
            ts=ts,
            status="filled",
            filled_qty=-1,
            avg_price=10,
            client_order_id="c1",
        )


def test_feature_vector_requires_aware_ts():
    with pytest.raises(ValueError):
        FeatureVector(symbol="BTCUSDT", ts=datetime.utcnow(), values={"x": 1.0})

def test_market_event_negative_price_rejected():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        MarketEvent(symbol="BTCUSDT", event_ts=ts, price=-1, size=1, source="trade")


def test_signal_negative_size():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        Signal(symbol="BTCUSDT", ts=ts, side="buy", size=-1)


def test_signal_invalid_side():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        Signal(symbol="BTCUSDT", ts=ts, side="hold", size=1)  # type: ignore[arg-type]


def test_portfolio_total_value():
    ts = datetime.now(timezone.utc)
    pf = PortfolioState(ts=ts, positions={}, cash=100, unrealized_pnl=5, realized_pnl=2)
    assert pf.total_value() == 107


def test_normalize_symbol():
    assert normalize_symbol("  ethusdt ") == "ETHUSDT"


def test_trace_context_init():
    ctx = TraceContext(trace_id="abc", span_id="def")
    assert ctx.trace_id == "abc"
    assert ctx.span_id == "def"


def test_execution_report_status_validation():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        ExecutionReport(
            symbol="BTCUSDT",
            ts=ts,
            status="bad",  # type: ignore[arg-type]
            filled_qty=0,
            avg_price=0,
            client_order_id="c1",
        )


def test_execution_report_negative_avg_price():
    ts = datetime.now(timezone.utc)
    with pytest.raises(ValueError):
        ExecutionReport(
            symbol="BTCUSDT",
            ts=ts,
            status="filled",
            filled_qty=1,
            avg_price=-1,
            client_order_id="c1",
        )


def test_portfolio_rejects_naive_ts():
    with pytest.raises(ValueError):
        PortfolioState(ts=datetime.utcnow(), positions={}, cash=0)
