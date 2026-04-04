from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.runtime import FeatureRuntimeEngine
from app.features.serving import FeatureServingService, ServingPolicy


@dataclass(frozen=True)
class ParityMismatch:
    symbol: str
    ts: datetime
    feature_name: str
    offline_value: float | None
    online_value: float | None
    reason: str


@dataclass(frozen=True)
class ParityReport:
    pass_ok: bool
    mismatches: tuple[ParityMismatch, ...]


def run_parity_check(
    events: Iterable[MarketEvent],
    *,
    feature_set,
    offline_store_path,
    online_store_path,
    tolerance: float = 1e-9,
) -> ParityReport:
    materializer = FeatureMaterializer()
    offline_store = OfflineFeatureStore(offline_store_path)
    online_store = OnlineFeatureStore(online_store_path)
    offline_vectors = materializer.materialize(events, feature_set=feature_set, store=offline_store, run_id="parity")
    runtime = FeatureRuntimeEngine(feature_set=feature_set)
    online_vectors = runtime.update_batch(sorted(list(events), key=lambda e: (e.symbol, e.available_ts, e.event_ts)))
    for vector in online_vectors:
        online_store.upsert(vector)
    offline_by_key: Dict[tuple[str, datetime], FeatureVector] = {(fv.symbol, fv.ts): fv for fv in offline_vectors}
    online_by_key: Dict[tuple[str, datetime], FeatureVector] = {(fv.symbol, fv.ts): fv for fv in online_vectors}
    mismatches: List[ParityMismatch] = []
    for key, offline in offline_by_key.items():
        online = online_by_key.get(key)
        if online is None:
            mismatches.append(ParityMismatch(symbol=offline.symbol, ts=offline.ts, feature_name="*", offline_value=None, online_value=None, reason="missing_online"))
            continue
        names = sorted(set(offline.values) | set(online.values))
        for name in names:
            ov = offline.values.get(name)
            lv = online.values.get(name)
            if ov is None or lv is None:
                mismatches.append(ParityMismatch(symbol=offline.symbol, ts=offline.ts, feature_name=name, offline_value=ov, online_value=lv, reason="missing_value"))
                continue
            if abs(float(ov) - float(lv)) > tolerance:
                mismatches.append(ParityMismatch(symbol=offline.symbol, ts=offline.ts, feature_name=name, offline_value=float(ov), online_value=float(lv), reason="tolerance_exceeded"))
    return ParityReport(pass_ok=not mismatches, mismatches=tuple(mismatches))
