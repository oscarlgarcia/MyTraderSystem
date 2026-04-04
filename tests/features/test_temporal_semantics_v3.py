from datetime import datetime, timezone

import pytest

from app.features.definitions import build_legacy_feature_set_definition
from app.features.runtime import FeatureRuntimeEngine
from app.marketdata.models import TradeEvent, typed_event_to_legacy


def test_canonical_trade_event_exposes_temporal_semantics():
    exchange_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    provider_ts = datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    receive_ts = datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)
    event = TradeEvent(symbol="BTCUSDT", exchange_ts=exchange_ts, provider_ts=provider_ts, receive_ts=receive_ts, price=100.0, size=1.0)
    assert event.published_ts == provider_ts
    assert event.available_ts == receive_ts
    legacy = typed_event_to_legacy(event)
    assert legacy.published_ts == provider_ts
    assert legacy.available_ts == receive_ts
    assert legacy.has_explicit_available_ts is True


def test_runtime_requires_explicit_available_ts_in_paper_mode():
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    engine = FeatureRuntimeEngine(feature_set=feature_set, runtime_mode="paper")
    event = TradeEvent(symbol="BTCUSDT", exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc), price=100.0, size=1.0)
    with pytest.raises(ValueError, match="strict temporal semantics"):
        engine.update(event)


def test_runtime_accepts_explicit_available_ts_in_paper_mode():
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    engine = FeatureRuntimeEngine(feature_set=feature_set, runtime_mode="paper")
    event = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        provider_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        receive_ts=datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
        price=100.0,
        size=1.0,
    )
    assert engine.update(event) is not None
