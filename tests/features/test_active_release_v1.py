from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store import OnlineFeatureStore
from app.features.release_workflow import gate_and_publish_feature_release
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.serving import FeatureServingService


def test_serving_resolves_active_release(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    registry_path = tmp_path / "releases.json"
    gate_and_publish_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        version="2.0.0",
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=FeatureMetrics(),
        target="paper",
    )
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    online.upsert(
        FeatureVector(
            symbol="BTCUSDT",
            ts=ts,
            available_ts=ts,
            values={"price": 100.0},
            feature_set_name="default",
            feature_set_version="2.0.0",
            lineage_id="bundle-2",
        )
    )
    service = FeatureServingService(online_store=online, release_registry_path=str(registry_path))
    result = service.get_latest_servable(symbol="BTCUSDT", decision_ts=ts, feature_set_name="default")
    assert result.status == "ok"
    assert result.feature_vector is not None
    assert result.feature_vector.feature_set_version == "2.0.0"
