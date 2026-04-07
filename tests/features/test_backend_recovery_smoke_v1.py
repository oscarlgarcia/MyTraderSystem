from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.online_store_factory import OnlineStoreConfig, create_online_store
from app.features.recovery import run_operational_recovery_smoke_test
from tests.features.http_test_support import feature_http_server


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade", available_ts=ts)


def test_operational_recovery_smoke_supports_non_sqlite_backend(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    online_store = create_online_store(OnlineStoreConfig(backend="json_file", path=tmp_path / "online.json"))
    report = run_operational_recovery_smoke_test(
        [_ev(0, 100), _ev(60, 101), _ev(120, 102), _ev(180, 103)],
        feature_set=feature_set,
        snapshot_path=tmp_path / "snapshot.json",
        online_store=online_store,
    )
    assert report.pass_ok


def test_operational_recovery_smoke_supports_http_backend(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    with feature_http_server() as (server, _):
        online_store = create_online_store(OnlineStoreConfig(backend="http", url=f"http://127.0.0.1:{server.server_port}"))
        report = run_operational_recovery_smoke_test(
            [_ev(0, 100), _ev(60, 101), _ev(120, 102), _ev(180, 103)],
            feature_set=feature_set,
            snapshot_path=tmp_path / "snapshot.json",
            online_store=online_store,
        )
    assert report.pass_ok
