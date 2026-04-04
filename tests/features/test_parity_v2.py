from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.parity import run_parity_check


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade")


def test_parity_check_passes(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2, 3], aggregators=["sma", "ema"], transformers=[])
    report = run_parity_check([_ev(0, 100), _ev(60, 101), _ev(120, 102)], feature_set=feature_set, offline_store_path=tmp_path / "offline.sqlite", online_store_path=tmp_path / "online.sqlite")
    assert report.pass_ok
    assert report.mismatches == ()
