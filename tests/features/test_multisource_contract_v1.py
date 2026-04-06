from datetime import datetime, timezone

import pytest

from app.common.dto import MarketEvent
from app.features.definitions import AuxiliaryInputDefinition, FeatureNodeDefinition, FeatureSetDefinition
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore


def _ev(symbol, offset, price, *, source="trade"):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol=symbol, event_ts=ts, available_ts=ts, price=price, size=1.0, source=source)


def _feature_set():
    return FeatureSetDefinition(
        name="default",
        version="1.0.0",
        description="multisource",
        node_definitions=(
            FeatureNodeDefinition(name="price", kind="price", outputs=("price",)),
            FeatureNodeDefinition(name="aux_price", kind="metadata_join", outputs=("aux_price",), params={"alias": "aux", "field": "price"}),
        ),
        auxiliary_inputs=(AuxiliaryInputDefinition(alias="aux", description="aux feed"),),
    )


def test_materializer_rejects_undeclared_auxiliary_input(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    with pytest.raises(ValueError, match="undeclared auxiliary inputs"):
        FeatureMaterializer().materialize(
            [_ev("BTCUSDT", 0, 100.0)],
            feature_set=_feature_set(),
            store=store,
            auxiliary_events={"wrong": [_ev("BTCUSDT", -10, 99.0, source="kline")]},
        )


def test_materializer_accepts_declared_auxiliary_input(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    out = FeatureMaterializer().materialize(
        [_ev("BTCUSDT", 0, 100.0)],
        feature_set=_feature_set(),
        store=store,
        auxiliary_events={"aux": [_ev("BTCUSDT", -10, 99.0, source="kline")]},
    )
    assert out
