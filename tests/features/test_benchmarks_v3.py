from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.benchmarks import run_feature_benchmarks
from app.features.definitions import build_legacy_feature_set_definition


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade", available_ts=ts)


def test_feature_benchmarks_return_metrics(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    report = run_feature_benchmarks(
        [_ev(0, 100), _ev(60, 101), _ev(120, 102)],
        feature_set=feature_set,
        offline_store_path=tmp_path / "offline.sqlite",
        online_store_path=tmp_path / "online.sqlite",
    )
    assert report.materialization_rows == 3
    assert report.online_updates == 3
    assert report.serving_requests > 0
