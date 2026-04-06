from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import FeatureDefinition, FeatureNodeDefinition, FeatureSetDefinition
from app.features.runtime import FeatureRuntimeEngine


def _feature_set() -> FeatureSetDefinition:
    return FeatureSetDefinition(
        name="quant",
        version="1.0.0",
        description="quant nodes",
        feature_definitions=(
            FeatureDefinition(name="price", version="1.0.0", description="price", owner="test"),
            FeatureDefinition(name="price_lag_1", version="1.0.0", description="lagged price", owner="test"),
            FeatureDefinition(name="price_delta", version="1.0.0", description="price delta", owner="test"),
            FeatureDefinition(name="price_ratio", version="1.0.0", description="price ratio", owner="test"),
            FeatureDefinition(name="price_clipped", version="1.0.0", description="price clipped", owner="test"),
        ),
        node_definitions=(
            FeatureNodeDefinition(name="price", kind="price", outputs=("price",)),
            FeatureNodeDefinition(name="price_lag_1", kind="lag", outputs=("price_lag_1",), dependencies=("price",), params={"periods": 1}),
            FeatureNodeDefinition(name="price_delta", kind="difference", outputs=("price_delta",), dependencies=("price", "price_lag_1")),
            FeatureNodeDefinition(name="price_ratio", kind="ratio", outputs=("price_ratio",), dependencies=("price", "price_lag_1")),
            FeatureNodeDefinition(name="price_clipped", kind="clip", outputs=("price_clipped",), dependencies=("price",), params={"min": 99.0, "max": 100.5}),
        ),
    )


def _event(offset: int, price: float) -> MarketEvent:
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, available_ts=ts, price=price, size=1.0, source="trade")


def test_quant_nodes_compute_difference_ratio_and_clip():
    engine = FeatureRuntimeEngine(feature_set=_feature_set())
    assert engine.update(_event(0, 100.0)) is not None
    second = engine.update(_event(60, 101.0))
    assert second is not None
    assert second.values["price_delta"] == 1.0
    assert second.values["price_ratio"] == 1.01
    assert second.values["price_clipped"] == 100.5
