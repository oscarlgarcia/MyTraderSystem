from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.recovery import run_operational_recovery_smoke_test


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade", available_ts=ts)


def test_operational_recovery_smoke_passes(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    report = run_operational_recovery_smoke_test(
        [_ev(0, 100), _ev(60, 101), _ev(120, 102), _ev(180, 103)],
        feature_set=feature_set,
        snapshot_path=tmp_path / "snapshot.json",
        online_store_path=tmp_path / "online.sqlite",
    )
    assert report.pass_ok
