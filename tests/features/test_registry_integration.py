from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definition_registry import DefinitionRegistry
from app.features.definitions import build_legacy_feature_set_definition
from app.features.engine import FeatureEngine


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_feature_engine_uses_registered_definition(tmp_path):
    reg = DefinitionRegistry(storage_dir=tmp_path)
    feature_set = reg.register(
        build_legacy_feature_set_definition(
            name="default",
            version="1.0.0",
            description="baseline",
            windows=[3],
            aggregators=["sma"],
            transformers=[],
        )
    )
    engine = FeatureEngine(feature_set=feature_set)
    out = engine.update_batch([_ev(i * 60, p) for i, p in enumerate([100, 101, 103])])
    assert out[-1] is not None
    assert "sma_3" in out[-1].values
