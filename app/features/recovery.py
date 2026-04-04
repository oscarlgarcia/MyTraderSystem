from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from app.common.dto import MarketEvent
from app.features.runtime import FeatureRuntimeEngine
from app.features.state_snapshot import StateSnapshotStore


@dataclass(frozen=True)
class RecoverySmokeReport:
    pass_ok: bool
    reason: str = ""


def run_recovery_smoke_test(events: Iterable[MarketEvent], *, feature_set, snapshot_path: str | Path) -> RecoverySmokeReport:
    ordered = sorted(list(events), key=lambda e: (e.symbol, e.available_ts, e.event_ts))
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
    if not second_half or not full:
        return RecoverySmokeReport(pass_ok=False, reason="insufficient outputs")
    return RecoverySmokeReport(pass_ok=second_half[-1].values == full[-1].values, reason="" if second_half[-1].values == full[-1].values else "restored_state_diverged")
