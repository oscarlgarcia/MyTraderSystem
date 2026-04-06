from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import FeatureDefinition, FeatureNodeDefinition, FeatureSetDefinition
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.runtime import FeatureRuntimeEngine


def _ev(offset: int, price: float, *, account: str) -> MarketEvent:
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=ts,
        price=price,
        size=1.0,
        source="trade",
        available_ts=ts,
        metadata={"account": account},
    )


def _feature_set() -> FeatureSetDefinition:
    return FeatureSetDefinition(
        name="multi",
        version="1.0.0",
        description="multi entity",
        entity_keys=("symbol", "account"),
        feature_definitions=(
            FeatureDefinition(name="price", version="1.0.0", description="price", owner="test", entity_keys=("symbol", "account")),
        ),
        node_definitions=(FeatureNodeDefinition(name="price", kind="price", outputs=("price",)),),
    )


def test_runtime_isolates_composite_entity_scopes():
    engine = FeatureRuntimeEngine(feature_set=_feature_set())
    paper = engine.update(_ev(0, 100.0, account="paper"))
    live = engine.update(_ev(60, 200.0, account="live"))
    assert paper is not None and live is not None
    assert paper.entity_keys["account"] == "paper"
    assert live.entity_keys["account"] == "live"
    assert paper.values["price"] == 100.0
    assert live.values["price"] == 200.0


def test_stores_roundtrip_composite_entity_scope(tmp_path):
    engine = FeatureRuntimeEngine(feature_set=_feature_set())
    vector = engine.update(_ev(0, 100.0, account="paper"))
    assert vector is not None
    offline = OfflineFeatureStore(tmp_path / "offline.sqlite")
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    offline.put_many([vector], run_id="multi")
    online.upsert(vector)
    assert offline.reconstruct_run(run_id="multi", entity_keys={"symbol": "BTCUSDT", "account": "paper"})
    assert online.get_latest(entity_keys={"symbol": "BTCUSDT", "account": "paper"}, feature_set_name="multi", feature_set_version="1.0.0") is not None
