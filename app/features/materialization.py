from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import uuid4

from app.common.dto import FeatureVector, MarketEvent
from app.features.asof_join import asof_join
from app.features.definitions import FeatureSetDefinition
from app.features.entity_codec import normalize_entity_keys
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

    def _validate_auxiliary_inputs(
        self,
        *,
        feature_set: FeatureSetDefinition,
        auxiliary_events: dict[str, Iterable[MarketEvent]] | None,
    ) -> None:
        declared = {item.alias: item for item in feature_set.auxiliary_inputs}
        provided = set((auxiliary_events or {}).keys())
        unknown = provided - set(declared)
        if unknown:
            raise ValueError(f"undeclared auxiliary inputs: {', '.join(sorted(unknown))}")
        missing_required = [item.alias for item in feature_set.auxiliary_inputs if item.required and item.alias not in provided]
        if missing_required:
            raise ValueError(f"missing required auxiliary inputs: {', '.join(sorted(missing_required))}")

    def _prepare_events(
        self,
        events: Sequence[MarketEvent],
        auxiliary_events: dict[str, Iterable[MarketEvent]] | None = None,
        feature_set: FeatureSetDefinition | None = None,
    ) -> list[tuple[MarketEvent, datetime]]:
        prepared: list[tuple[MarketEvent, datetime]] = [(event, _available_ts(event)) for event in events]
        if not auxiliary_events:
            return prepared
        declared = {item.alias: item for item in (feature_set.auxiliary_inputs if feature_set else ())}
        for alias, aux_events in auxiliary_events.items():
            declaration = declared.get(alias)
            declaration_entity_keys = tuple(declaration.entity_keys) if declaration else ("symbol",)
            aux_sorted = sorted(list(aux_events), key=lambda event: (_available_ts(event), _event_ts(event), getattr(event, "symbol", "")))
            joined = asof_join(
                [item[0] for item in prepared],
                aux_sorted,
                left_ts_getter=lambda event: _available_ts(event),
                right_ts_getter=lambda event: _available_ts(event),
                predicate=lambda left, right: normalize_entity_keys(
                    {key: left.metadata.get(key) or left.metadata.get(f"entity:{key}") for key in declaration_entity_keys if key != "symbol"},
                    symbol=getattr(left, "symbol", None),
                    required_keys=declaration_entity_keys,
                )
                == normalize_entity_keys(
                    {key: right.metadata.get(key) or right.metadata.get(f"entity:{key}") for key in declaration_entity_keys if key != "symbol"},
                    symbol=getattr(right, "symbol", None),
                    required_keys=declaration_entity_keys,
                ),
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
        target: str = "research",
    ) -> List[FeatureVector]:
        events_list = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
        run_id = run_id or str(uuid4())
        self._validate_auxiliary_inputs(feature_set=feature_set, auxiliary_events=auxiliary_events)
        prepared_events = self._prepare_events(events_list, auxiliary_events=auxiliary_events, feature_set=feature_set)
        engine = FeatureRuntimeEngine(feature_set=feature_set)
        validator = FeatureValidator(feature_set, target=target)
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
