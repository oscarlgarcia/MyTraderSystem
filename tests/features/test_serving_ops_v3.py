from datetime import datetime, timezone
import logging

from app.common.dto import FeatureVector
from app.features.metrics import FeatureMetrics
from app.features.online_store import OnlineFeatureStore
from app.features.offline_store import OfflineFeatureStore
from app.features.serving import FeatureServingService, ServingPolicy


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _fv(offset, available_offset=None, flags=()):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    available_ts = datetime.fromtimestamp(1700000000 + (available_offset if available_offset is not None else offset), tz=timezone.utc)
    return FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=available_ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle", quality_flags=tuple(flags))


def test_serving_records_metrics_and_logs(tmp_path):
    metrics = FeatureMetrics()
    handler = _ListHandler()
    logger = logging.getLogger("features.serving.test")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    offline = OfflineFeatureStore(tmp_path / "offline.sqlite")
    fv = _fv(0)
    online.upsert(fv)
    offline.put_many([fv])
    service = FeatureServingService(online_store=online, offline_store=offline, metrics=metrics, logger=logger)
    result = service.get_latest_servable(symbol="BTCUSDT", decision_ts=datetime.fromtimestamp(1700000060, tz=timezone.utc), feature_set_name="default", feature_set_version="1.0.0")
    assert result.status == "ok"
    assert metrics.serving_requests == 1
    assert handler.records
    assert handler.records[0].lineage_id == "bundle"
