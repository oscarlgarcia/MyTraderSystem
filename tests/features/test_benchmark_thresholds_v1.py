from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.benchmarks import FeatureBenchmarkThresholds, run_feature_benchmarks
from app.features.definitions import build_legacy_feature_set_definition


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, available_ts=ts, price=price, size=1.0, source="trade")


def test_benchmarks_evaluate_thresholds(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    report = run_feature_benchmarks(
        [_ev(0, 100), _ev(60, 101), _ev(120, 102)],
        feature_set=feature_set,
        offline_store_path=tmp_path / "offline.sqlite",
        online_store_path=tmp_path / "online.sqlite",
        thresholds=FeatureBenchmarkThresholds(
            min_materialization_rows_per_second=0.01,
            min_online_updates_per_second=0.01,
            min_serving_requests_per_second=0.01,
        ),
    )
    assert report.threshold_pass_ok is True
