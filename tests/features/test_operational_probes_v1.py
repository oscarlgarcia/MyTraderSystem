from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store_memory import MemoryOnlineFeatureStore
from app.features.operational_probes import run_serving_concurrency_probe, run_serving_soak_probe
from app.features.serving import FeatureServingService


def _service() -> tuple[FeatureServingService, datetime]:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    store = MemoryOnlineFeatureStore()
    store.upsert(
        FeatureVector(
            symbol="BTCUSDT",
            ts=ts,
            available_ts=ts,
            values={"price": 100.0},
            feature_set_name="default",
            feature_set_version="1.0.0",
            lineage_id="bundle-1",
            entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        )
    )
    return FeatureServingService(online_store=store), ts


def test_serving_soak_probe_reports_latency_and_statuses():
    service, ts = _service()
    report = run_serving_soak_probe(
        request_fn=lambda: service.get_latest_servable(
            decision_ts=ts,
            feature_set_name="default",
            feature_set_version="1.0.0",
            entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        ),
        iterations=20,
        max_latency_seconds=1.0,
    )
    assert report.pass_ok is True
    assert report.ok_count > 0
    assert report.unknown_count == 0


def test_serving_concurrency_probe_reports_concurrent_statuses():
    service, ts = _service()
    report = run_serving_concurrency_probe(
        request_fn=lambda: service.get_latest_servable(
            decision_ts=ts,
            feature_set_name="default",
            feature_set_version="1.0.0",
            entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        ),
        rounds=4,
        readers_per_round=6,
        max_workers=6,
        max_latency_seconds=1.0,
    )
    assert report.pass_ok is True
    assert report.total_requests == 24
    assert report.unknown_count == 0

