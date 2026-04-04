from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import uuid4

from app.common.dto import FeatureVector, MarketEvent
from app.features.asof_join import asof_join
from app.features.definitions import FeatureSetDefinition
from app.features.lineage import attach_lineage, build_feature_bundle_id, fingerprint_events
from app.features.offline_store import MaterializationRunRecord, OfflineFeatureStore
from app.features.planner import FeaturePlanner
from app.features.runtime import FeatureRuntimeEngine
from app.features.validators import FeatureValidator


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


class FeatureMaterializer:
    def __init__(self, *, planner: Optional[FeaturePlanner] = None) -> None:
        self.planner = planner or FeaturePlanner()

    def _prepare_events(
        self,
        events: Sequence[MarketEvent],
        auxiliary_events: dict[str, Iterable[MarketEvent]] | None = None,
    ) -> list[tuple[MarketEvent, datetime]]:
        prepared: list[tuple[MarketEvent, datetime]] = [(event, _available_ts(event)) for event in events]
        if not auxiliary_events:
            return prepared
        for alias, aux_events in auxiliary_events.items():
            aux_sorted = sorted(list(aux_events), key=lambda event: (_available_ts(event), _event_ts(event), getattr(event, "symbol", "")))
            joined = asof_join(
                [item[0] for item in prepared],
                aux_sorted,
                left_ts_getter=lambda event: _available_ts(event),
                right_ts_getter=lambda event: _available_ts(event),
                predicate=lambda left, right: getattr(left, "symbol", None) == getattr(right, "symbol", None),
            )
            next_prepared: list[tuple[MarketEvent, datetime]] = []
            for (event, current_cutoff), (_, matched) in zip(prepared, joined):
                metadata = dict(getattr(event, "metadata", {}))
                cutoff = current_cutoff
                if matched is None:
                    metadata[f"join:{alias}:present"] = "false"
                else:
                    matched_available = _available_ts(matched)
                    metadata[f"join:{alias}:present"] = "true"
                    metadata[f"join:{alias}:symbol"] = getattr(matched, "symbol", "")
                    metadata[f"join:{alias}:available_ts"] = matched_available.isoformat()
                    metadata[f"join:{alias}:event_ts"] = _event_ts(matched).isoformat()
                    if hasattr(matched, "price"):
                        metadata[f"join:{alias}:price"] = str(getattr(matched, "price"))
                    cutoff = max(cutoff, matched_available)
                next_prepared.append((replace(event, metadata=metadata), cutoff))
            prepared = next_prepared
        return prepared

    def materialize(
        self,
        events: Iterable[MarketEvent],
        *,
        feature_set: FeatureSetDefinition,
        store: OfflineFeatureStore,
        run_id: str | None = None,
        auxiliary_events: dict[str, Iterable[MarketEvent]] | None = None,
    ) -> List[FeatureVector]:
        events_list = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
        run_id = run_id or str(uuid4())
        prepared_events = self._prepare_events(events_list, auxiliary_events=auxiliary_events)
        engine = FeatureRuntimeEngine(feature_set=feature_set)
        validator = FeatureValidator(feature_set)
        input_fingerprint = fingerprint_events([item[0] for item in prepared_events])
        bundle_id = build_feature_bundle_id(feature_set=feature_set, input_fingerprint=input_fingerprint, run_id=run_id)
        outputs: List[FeatureVector] = []
        for event, source_cutoff_ts in prepared_events:
            fv = engine.update(event)
            if fv is None:
                continue
            fv.source_cutoff_ts = source_cutoff_ts
            attach_lineage(fv, feature_set=feature_set, bundle_id=bundle_id)
            result = validator.validate(fv)
            fv.quality_flags = result.flags
            outputs.append(fv)
        store.put_many(outputs, run_id=run_id)
        if prepared_events:
            min_event_ts = min(_event_ts(event) for event, _ in prepared_events)
            max_event_ts = max(_event_ts(event) for event, _ in prepared_events)
        else:
            min_event_ts = None
            max_event_ts = None
        store.register_materialization_run(
            MaterializationRunRecord(
                run_id=run_id,
                feature_set_name=feature_set.name,
                feature_set_version=feature_set.version,
                definition_hash=feature_set.definition_hash,
                input_fingerprint=input_fingerprint,
                bundle_id=bundle_id,
                row_count=len(outputs),
                status="completed",
                created_at=datetime.now(timezone.utc),
                min_event_ts=min_event_ts,
                max_event_ts=max_event_ts,
            )
        )
        return outputs
