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


def run_feature_benchmarks(
    events: Iterable[MarketEvent],
    *,
    feature_set: FeatureSetDefinition,
    offline_store_path: str | Path,
    online_store_path: str | Path,
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

    return FeatureBenchmarkReport(
        materialization_rows=len(materialized),
        materialization_seconds=materialization_seconds,
        online_updates=len(online_vectors),
        online_update_seconds=online_update_seconds,
        serving_requests=serving_requests,
        serving_seconds=serving_seconds,
    )
