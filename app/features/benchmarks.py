from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from app.common.dto import MarketEvent
from app.features.definitions import FeatureSetDefinition
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.runtime import FeatureRuntimeEngine
from app.features.serving import FeatureServingService


@dataclass(frozen=True)
class FeatureBenchmarkReport:
    materialization_rows: int
    materialization_seconds: float
    online_updates: int
    online_update_seconds: float
    serving_requests: int
    serving_seconds: float
    materialization_rows_per_second: float
    online_updates_per_second: float
    serving_requests_per_second: float
    threshold_pass_ok: bool


@dataclass(frozen=True)
class FeatureBenchmarkThresholds:
    min_materialization_rows_per_second: float = 1.0
    min_online_updates_per_second: float = 1.0
    min_serving_requests_per_second: float = 1.0


def run_feature_benchmarks(
    events: Iterable[MarketEvent],
    *,
    feature_set: FeatureSetDefinition,
    offline_store_path: str | Path,
    online_store_path: str | Path,
    thresholds: FeatureBenchmarkThresholds | None = None,
) -> FeatureBenchmarkReport:
    ordered = list(events)
    offline_store = OfflineFeatureStore(offline_store_path)
    online_store = OnlineFeatureStore(online_store_path)

    start = time.perf_counter()
    materialized = FeatureMaterializer().materialize(ordered, feature_set=feature_set, store=offline_store, run_id="benchmark")
    materialization_seconds = time.perf_counter() - start

    runtime = FeatureRuntimeEngine(feature_set=feature_set)
    start = time.perf_counter()
    online_vectors = runtime.update_batch(ordered)
    for vector in online_vectors:
        online_store.upsert(vector)
    online_update_seconds = time.perf_counter() - start

    service = FeatureServingService(online_store=online_store, offline_store=offline_store)
    start = time.perf_counter()
    serving_requests = 0
    for vector in online_vectors[-10:]:
        service.get_latest_servable(
            symbol=vector.symbol,
            decision_ts=vector.available_ts,
            feature_set_name=feature_set.name,
            feature_set_version=feature_set.version,
        )
        serving_requests += 1
    serving_seconds = time.perf_counter() - start

    thresholds = thresholds or FeatureBenchmarkThresholds()
    materialization_rows_per_second = len(materialized) / materialization_seconds if materialization_seconds > 0 else float("inf")
    online_updates_per_second = len(online_vectors) / online_update_seconds if online_update_seconds > 0 else float("inf")
    serving_requests_per_second = serving_requests / serving_seconds if serving_seconds > 0 else float("inf")
    threshold_pass_ok = (
        materialization_rows_per_second >= thresholds.min_materialization_rows_per_second
        and online_updates_per_second >= thresholds.min_online_updates_per_second
        and serving_requests_per_second >= thresholds.min_serving_requests_per_second
    )

    return FeatureBenchmarkReport(
        materialization_rows=len(materialized),
        materialization_seconds=materialization_seconds,
        online_updates=len(online_vectors),
        online_update_seconds=online_update_seconds,
        serving_requests=serving_requests,
        serving_seconds=serving_seconds,
        materialization_rows_per_second=materialization_rows_per_second,
        online_updates_per_second=online_updates_per_second,
        serving_requests_per_second=serving_requests_per_second,
        threshold_pass_ok=threshold_pass_ok,
    )
