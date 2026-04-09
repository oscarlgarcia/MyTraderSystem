from datetime import UTC, datetime

from app.marketdata.anomaly_checks import (
    detect_marketdata_anomalies,
    detect_price_jump,
    detect_volume_spike,
)
from app.marketdata.models import BarEvent, BookEvent, TradeEvent


def _trade(*, price: float, size: float) -> TradeEvent:
    return TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=UTC),
        price=price,
        size=size,
    )


def _bar(*, close: float, volume: float) -> BarEvent:
    return BarEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=UTC),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        interval="1m",
    )


def _book(*, bid_price: float, ask_price: float, bid_size: float, ask_size: float) -> BookEvent:
    return BookEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=UTC),
        bid_price=bid_price,
        ask_price=ask_price,
        bid_size=bid_size,
        ask_size=ask_size,
    )


def test_detect_price_jump_returns_none_without_previous_price():
    assert detect_price_jump(previous_price=None, current_price=100.0) is None


def test_detect_price_jump_detects_large_relative_move_with_requested_severity():
    anomaly = detect_price_jump(
        previous_price=100.0,
        current_price=130.0,
        relative_jump_threshold=0.2,
        feed_type="trade",
        severity="warn",
    )
    assert anomaly is not None
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.feed_type == "trade"
    assert anomaly.severity == "warn"
    assert anomaly.action == "warn"
    assert anomaly.relative_jump >= 0.3


def test_detect_marketdata_anomalies_emits_trade_warn_for_price_jump_below_quarantine():
    anomalies = detect_marketdata_anomalies(
        event=_trade(price=125.0, size=2.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.feed_type == "trade"
    assert anomaly.severity == "warn"
    assert anomaly.action == "warn"
    assert anomaly.threshold == 0.20


def test_detect_marketdata_anomalies_emits_trade_quarantine_for_large_price_jump():
    anomalies = detect_marketdata_anomalies(
        event=_trade(price=150.0, size=2.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.severity == "quarantine"
    assert anomaly.action == "quarantine"
    assert anomaly.threshold == 0.35


def test_detect_marketdata_anomalies_emits_trade_fail_for_extreme_price_jump():
    anomalies = detect_marketdata_anomalies(
        event=_trade(price=170.0, size=2.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.severity == "fail"
    assert anomaly.action == "fail"
    assert anomaly.threshold == 0.60


def test_detect_marketdata_anomalies_skips_trade_volume_spike_detection_for_single_trade_sizes():
    anomalies = detect_marketdata_anomalies(
        event=_trade(price=101.0, size=100.0),
        previous_price=100.0,
        previous_volume=1.0,
    )

    assert anomalies == ()


def test_detect_marketdata_anomalies_emits_kline_volume_spike_warn():
    anomalies = detect_marketdata_anomalies(
        event=_bar(close=101.0, volume=6.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "volume_spike"
    assert anomaly.feed_type == "kline"
    assert anomaly.severity == "warn"
    assert anomaly.action == "warn"
    assert anomaly.volume_ratio == 6.0
    assert anomaly.threshold == 4.0


def test_detect_marketdata_anomalies_emits_kline_volume_spike_fail():
    anomalies = detect_marketdata_anomalies(
        event=_bar(close=101.0, volume=25.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "volume_spike"
    assert anomaly.feed_type == "kline"
    assert anomaly.severity == "fail"
    assert anomaly.action == "fail"
    assert anomaly.threshold == 20.0


def test_detect_marketdata_anomalies_emits_book_quarantine_for_mid_price_jump():
    anomalies = detect_marketdata_anomalies(
        event=_book(bid_price=129.0, ask_price=131.0, bid_size=1.0, ask_size=1.0),
        previous_price=100.0,
        previous_volume=1.0,
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.feed_type == "book"
    assert anomaly.severity == "quarantine"
    assert anomaly.threshold == 0.20


def test_detect_volume_spike_returns_none_without_previous_volume():
    assert (
        detect_volume_spike(
            previous_volume=None,
            current_volume=10.0,
            volume_ratio_threshold=4.0,
            feed_type="kline",
            severity="warn",
        )
        is None
    )
