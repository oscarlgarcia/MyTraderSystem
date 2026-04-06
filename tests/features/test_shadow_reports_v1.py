import json
from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store import OnlineFeatureStore
from app.features.serving import FeatureServingService
from app.features.shadow import ShadowServingService
from app.features.shadow_report_store import ShadowReportStore


def test_shadow_service_persists_divergence_report(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    a = OnlineFeatureStore(tmp_path / "a.sqlite")
    b = OnlineFeatureStore(tmp_path / "b.sqlite")
    a.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-a"))
    b.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 101.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-b"))
    store = ShadowReportStore(tmp_path / "shadow.jsonl")
    shadow = ShadowServingService(
        primary=FeatureServingService(online_store=a),
        shadow=FeatureServingService(online_store=b),
        report_store=store,
    )
    report = shadow.get_latest_servable(symbol="BTCUSDT", decision_ts=ts, feature_set_name="default", feature_set_version="1.0.0")
    assert report.pass_ok is False
    rows = [json.loads(line) for line in store.path.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["reason"].startswith("value:")
