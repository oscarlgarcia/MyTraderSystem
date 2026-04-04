from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore
from app.features.query import FeatureQueryService


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade", available_ts=ts)


def test_materialization_persists_run_manifest_and_reconstructs(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[3], aggregators=["sma"], transformers=[])
    outputs = FeatureMaterializer().materialize([_ev(0, 100), _ev(60, 101), _ev(120, 102)], feature_set=feature_set, store=store, run_id="run-42")
    record = store.get_materialization_run("run-42")
    assert record is not None
    assert record.row_count == len(outputs)
    query = FeatureQueryService(offline_store=store)
    rebuilt = query.reconstruct_run(run_id="run-42")
    assert [fv.ts for fv in rebuilt] == [fv.ts for fv in outputs]
    assert rebuilt[-1].values == outputs[-1].values
