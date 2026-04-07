from datetime import datetime, timezone
import logging

from app.common.dto import FeatureVector
from app.features.model_contract import FeatureConsumerContract
from app.features.metrics import FeatureMetrics
from app.features.online_store import OnlineFeatureStore
from app.features.offline_store import OfflineFeatureStore
from app.features.serving import FeatureServingService, ServingPolicy
from app.features.training_bundle_registry import TrainingBundleRecord, TrainingBundleRegistry


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


def test_serving_fails_when_training_serving_contract_is_invalid(tmp_path):
    ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    registry = TrainingBundleRegistry(tmp_path / "training-bundles")
    registry.register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v2",
            feature_set_name="default",
            feature_set_version="1.0.0",
        )
    )
    fv = _fv(0)
    online.upsert(fv)
    service = FeatureServingService(online_store=online, training_bundle_registry=registry)
    result = service.get_latest_servable(
        symbol="BTCUSDT",
        decision_ts=ts,
        feature_set_name="default",
        feature_set_version="1.0.0",
        contract=FeatureConsumerContract(
            consumer_name="paper",
            consumer_kind="strategy",
            feature_set_name="default",
            feature_set_version="1.0.0",
            required_features=("price",),
            required_metadata_keys=("dataset_id", "feature_schema_hash", "training_bundle_id"),
            required_dataset_id="dataset-2024-01",
            required_schema_hash="schema-v2",
            required_training_bundle_id="train-bundle-1",
        ),
        consumer_metadata={
            "dataset_id": "dataset-legacy",
            "feature_schema_hash": "schema-v1",
            "training_bundle_id": "train-bundle-1",
        },
    )
    assert result.status == "fail"
    assert result.reason == "contract_validation"
    assert service.metrics.contract_validation_failures == 1


def test_serving_requires_contract_metadata_for_live_targets(tmp_path):
    ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    online.upsert(_fv(0))
    registry = TrainingBundleRegistry(tmp_path / "training-bundles")
    registry.register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v1",
            feature_set_name="default",
            feature_set_version="1.0.0",
        )
    )
    service = FeatureServingService(online_store=online, training_bundle_registry=registry, target="live")
    result = service.get_latest_servable(
        symbol="BTCUSDT",
        decision_ts=ts,
        feature_set_name="default",
        feature_set_version="1.0.0",
    )
    assert result.status == "fail"
    assert result.reason == "contract_required"
    assert service.metrics.contract_validation_failures == 1
