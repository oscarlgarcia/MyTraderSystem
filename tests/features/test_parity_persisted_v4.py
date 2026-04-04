import sqlite3
from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.offline_store import OfflineFeatureStore
from app.features.parity import run_parity_check


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, available_ts=ts, price=price, size=1.0, source="trade")


def test_parity_persists_offline_materialization(tmp_path):
    feature_set = build_legacy_feature_set_definition(
        name="default",
        version="1.0.0",
        description="baseline",
        windows=[2, 3],
        aggregators=["sma", "ema"],
        transformers=[],
    )
    offline_path = tmp_path / "offline.sqlite"
    report = run_parity_check(
        [_ev(0, 100), _ev(60, 101), _ev(120, 102)],
        feature_set=feature_set,
        offline_store_path=offline_path,
        online_store_path=tmp_path / "online.sqlite",
    )
    assert report.pass_ok

    with sqlite3.connect(offline_path) as conn:
        row = conn.execute("SELECT run_id FROM materialization_runs LIMIT 1").fetchone()
    assert row is not None

    vectors = OfflineFeatureStore(offline_path).reconstruct_run(run_id=row[0])
    assert vectors
