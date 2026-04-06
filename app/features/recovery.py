from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from app.common.dto import MarketEvent
from app.features.online_store_base import FeatureOnlineStore
from app.features.online_store import OnlineFeatureStore
from app.features.runtime import FeatureRuntimeEngine
from app.features.serving import FeatureServingService
from app.features.state_snapshot import StateSnapshotStore


@dataclass(frozen=True)
class RecoverySmokeReport:
    pass_ok: bool
    reason: str = ""


@dataclass(frozen=True)
class OperationalRecoveryReport:
    pass_ok: bool
    reason: str = ""


def _event_ts(event) -> datetime:
    ts = getattr(event, "event_ts", None)
    if ts is None:
        ts = getattr(event, "exchange_ts")
    return ts


def _available_ts(event) -> datetime:
    ts = getattr(event, "available_ts", None)
    if ts is None:
        ts = _event_ts(event)
    return ts


def run_recovery_smoke_test(events: Iterable[MarketEvent], *, feature_set, snapshot_path: str | Path) -> RecoverySmokeReport:
    ordered = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
    if len(ordered) < 2:
        return RecoverySmokeReport(pass_ok=False, reason="need at least two events")
    split = len(ordered) // 2
    engine_a = FeatureRuntimeEngine(feature_set=feature_set)
    first_half = engine_a.update_batch(ordered[:split])
    snapshot_store = StateSnapshotStore(snapshot_path)
    snapshot_store.save(engine_a.state)
    restored = snapshot_store.load()
    if restored is None:
        return RecoverySmokeReport(pass_ok=False, reason="snapshot_missing")
    engine_b = FeatureRuntimeEngine(feature_set=feature_set)
    engine_b.restore_state(restored)
    second_half = engine_b.update_batch(ordered[split:])

    engine_control = FeatureRuntimeEngine(feature_set=feature_set)
    full = engine_control.update_batch(ordered)
    if not second_half or not full or not first_half:
        return RecoverySmokeReport(pass_ok=False, reason="insufficient_outputs")
    last_ok = second_half[-1].values == full[-1].values
    return RecoverySmokeReport(pass_ok=last_ok, reason="" if last_ok else "restored_state_diverged")


def run_operational_recovery_smoke_test(
    events: Iterable[MarketEvent],
    *,
    feature_set,
    snapshot_path: str | Path,
    online_store_path: str | Path | None = None,
    online_store: FeatureOnlineStore | None = None,
) -> OperationalRecoveryReport:
    ordered = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
    if len(ordered) < 3:
        return OperationalRecoveryReport(pass_ok=False, reason="need at least three events")
    split = len(ordered) // 2
    snapshot_store = StateSnapshotStore(snapshot_path)
    if online_store is None:
        if online_store_path is None:
            raise ValueError("online_store or online_store_path is required")
        online_store = OnlineFeatureStore(online_store_path)
    engine = FeatureRuntimeEngine(feature_set=feature_set)
    first_half = engine.update_batch(ordered[:split])
    for vector in first_half:
        online_store.upsert(vector)
    snapshot_store.save(engine.state)
    restored = snapshot_store.load()
    if restored is None:
        return OperationalRecoveryReport(pass_ok=False, reason="snapshot_missing")
    restored_engine = FeatureRuntimeEngine(feature_set=feature_set)
    restored_engine.restore_state(restored)
    second_half = restored_engine.update_batch(ordered[split:])
    for vector in second_half:
        online_store.upsert(vector)

    control_engine = FeatureRuntimeEngine(feature_set=feature_set)
    control_vectors = control_engine.update_batch(ordered)
    if not second_half or not control_vectors:
        return OperationalRecoveryReport(pass_ok=False, reason="insufficient_outputs")
    service = FeatureServingService(online_store=online_store)
    decision_ts = control_vectors[-1].available_ts
    served = service.get_latest_servable(
        symbol=control_vectors[-1].symbol,
        decision_ts=decision_ts,
        feature_set_name=feature_set.name,
        feature_set_version=feature_set.version,
    )
    if served.feature_vector is None:
        return OperationalRecoveryReport(pass_ok=False, reason=f"serving_failed:{served.reason}")
    if served.feature_vector.values != control_vectors[-1].values:
        return OperationalRecoveryReport(pass_ok=False, reason="served_values_diverged")
    return OperationalRecoveryReport(pass_ok=True)
