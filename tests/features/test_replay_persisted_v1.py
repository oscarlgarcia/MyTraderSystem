from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.runtime import FeatureRuntimeEngine


def _ev(offset: int, price: float) -> MarketEvent:
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, available_ts=ts, price=price, size=1.0, source="trade")


def test_recompute_replays_events_beyond_recent_window(tmp_path):
    feature_set = build_legacy_feature_set_definition(
        name="default",
        version="1.0.0",
        description="baseline",
        windows=[2],
        aggregators=["sma"],
        transformers=[],
    )
    engine = FeatureRuntimeEngine(
        feature_set=feature_set,
        out_of_order_policy="recompute",
        journal_path=tmp_path / "journal.sqlite",
    )
    for index in range(40):
        engine.update(_ev(index * 60, 100.0 + index))

    late = engine.update(_ev(30, 50.0))
    assert late is not None
    assert late.ts == datetime.fromtimestamp(1700000000 + 39 * 60, tz=timezone.utc)
    history = engine.state.recent_events[engine.state.scope_for_event(_ev(0, 0.0))]
    assert len(history) < 40


def test_recompute_reapplies_recent_window_bound_after_journal_replay(tmp_path):
    feature_set = build_legacy_feature_set_definition(
        name="default",
        version="1.0.0",
        description="baseline",
        windows=[2],
        aggregators=["sma"],
        transformers=[],
    )
    engine = FeatureRuntimeEngine(
        feature_set=feature_set,
        out_of_order_policy="recompute",
        journal_path=tmp_path / "journal.sqlite",
    )
    for index in range(50):
        engine.update(_ev(index * 60, 100.0 + index))

    engine.update(_ev(30, 50.0))
    scope = engine.state.scope_for_event(_ev(0, 0.0))
    history = engine.state.recent_events[scope]
    assert len(history) <= max(max(feature_set.windows) * 4, 32)
