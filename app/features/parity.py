from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List
from uuid import uuid4

from app.common.dto import FeatureVector, MarketEvent
from app.features.batch_executor import BatchFeatureExecutor
from app.features.entity_codec import entity_scope
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.runtime import FeatureRuntimeEngine


@dataclass(frozen=True)
class ParityMismatch:
    symbol: str
    ts: datetime
    feature_name: str
    offline_value: float | None
    online_value: float | None
    reason: str
    entity_scope: str = ""


@dataclass(frozen=True)
class ParityReport:
    pass_ok: bool
    mismatches: tuple[ParityMismatch, ...]


def _event_ts(event):
    ts = getattr(event, "event_ts", None)
    if ts is None:
        ts = getattr(event, "exchange_ts")
    return ts


def _available_ts(event):
    ts = getattr(event, "available_ts", None)
    if ts is None:
        ts = _event_ts(event)
    return ts


def _feature_tolerances(feature_set, default_tolerance: float) -> Dict[str, float]:
    tolerances: Dict[str, float] = {}
    for feature in feature_set.feature_definitions:
        parity_tolerance = feature.validation_policy.get("parity_tolerance", default_tolerance)
        tolerances[feature.name] = float(parity_tolerance)
    return tolerances


def _parity_key(fv: FeatureVector) -> tuple[str, datetime]:
    return (entity_scope(fv.entity_keys, symbol=fv.symbol), fv.ts)


def run_parity_check(
    events: Iterable[MarketEvent],
    *,
    feature_set,
    offline_store_path,
    online_store_path,
    tolerance: float = 1e-9,
    runtime_mode: str = "research",
) -> ParityReport:
    sorted_events = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
    offline_store = OfflineFeatureStore(offline_store_path)
    run_id = f"parity-{uuid4()}"
    FeatureMaterializer().materialize(sorted_events, feature_set=feature_set, store=offline_store, run_id=run_id)
    persisted_vectors = offline_store.get_run_vectors(run_id)
    batch_vectors = BatchFeatureExecutor().execute(sorted_events, feature_set=feature_set)
    runtime = FeatureRuntimeEngine(feature_set=feature_set, runtime_mode=runtime_mode)
    online_store = OnlineFeatureStore(online_store_path)
    online_vectors = runtime.update_batch(sorted_events)
    for vector in online_vectors:
        online_store.upsert(vector)
    offline_by_key: Dict[tuple[str, datetime], FeatureVector] = {_parity_key(fv): fv for fv in persisted_vectors}
    online_by_key: Dict[tuple[str, datetime], FeatureVector] = {_parity_key(fv): fv for fv in online_vectors}
    batch_by_key: Dict[tuple[str, datetime], FeatureVector] = {_parity_key(fv): fv for fv in batch_vectors}
    tolerances = _feature_tolerances(feature_set, tolerance)
    mismatches: List[ParityMismatch] = []
    for key, offline in offline_by_key.items():
        scope, _ = key
        batch = batch_by_key.get(key)
        if batch is None:
            mismatches.append(
                ParityMismatch(
                    symbol=offline.symbol,
                    entity_scope=scope,
                    ts=offline.ts,
                    feature_name="*",
                    offline_value=None,
                    online_value=None,
                    reason="missing_batch",
                )
            )
            continue
        if offline.values != batch.values:
            names = sorted(set(offline.values) | set(batch.values))
            for name in names:
                ov = offline.values.get(name)
                bv = batch.values.get(name)
                if ov != bv:
                    mismatches.append(
                        ParityMismatch(
                            symbol=offline.symbol,
                            entity_scope=scope,
                            ts=offline.ts,
                            feature_name=name,
                            offline_value=float(ov) if ov is not None else None,
                            online_value=float(bv) if bv is not None else None,
                            reason="persisted_batch_diverged",
                        )
                    )
        online = online_by_key.get(key)
        if online is None:
            mismatches.append(
                ParityMismatch(
                    symbol=offline.symbol,
                    entity_scope=scope,
                    ts=offline.ts,
                    feature_name="*",
                    offline_value=None,
                    online_value=None,
                    reason="missing_online",
                )
            )
            continue
        names = sorted(set(offline.values) | set(online.values))
        for name in names:
            ov = offline.values.get(name)
            lv = online.values.get(name)
            if ov is None or lv is None:
                mismatches.append(
                    ParityMismatch(
                        symbol=offline.symbol,
                        entity_scope=scope,
                        ts=offline.ts,
                        feature_name=name,
                        offline_value=ov,
                        online_value=lv,
                        reason="missing_value",
                    )
                )
                continue
            allowed = tolerances.get(name, tolerance)
            if abs(float(ov) - float(lv)) > allowed:
                mismatches.append(
                    ParityMismatch(
                        symbol=offline.symbol,
                        entity_scope=scope,
                        ts=offline.ts,
                        feature_name=name,
                        offline_value=float(ov),
                        online_value=float(lv),
                        reason=f"tolerance_exceeded:{allowed}",
                    )
                )
    runtime.metrics.parity_mismatches += len(mismatches)
    return ParityReport(pass_ok=not mismatches, mismatches=tuple(mismatches))
