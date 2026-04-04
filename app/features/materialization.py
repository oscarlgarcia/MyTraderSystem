from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4
from typing import Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.definitions import FeatureSetDefinition
from app.features.lineage import attach_lineage, build_feature_bundle_id, fingerprint_events
from app.features.offline_store import OfflineFeatureStore
from app.features.planner import FeaturePlanner
from app.features.runtime import FeatureRuntimeEngine
from app.features.validators import FeatureValidator


class FeatureMaterializer:
    def __init__(self, *, planner: Optional[FeaturePlanner] = None) -> None:
        self.planner = planner or FeaturePlanner()

    def materialize(
        self,
        events: Iterable[MarketEvent],
        *,
        feature_set: FeatureSetDefinition,
        store: OfflineFeatureStore,
        run_id: str | None = None,
    ) -> List[FeatureVector]:
        events_list = sorted(list(events), key=lambda e: (e.symbol, e.available_ts, e.event_ts))
        run_id = run_id or str(uuid4())
        engine = FeatureRuntimeEngine(feature_set=feature_set)
        validator = FeatureValidator(feature_set)
        input_fingerprint = fingerprint_events(events_list)
        bundle_id = build_feature_bundle_id(feature_set=feature_set, input_fingerprint=input_fingerprint, run_id=run_id)
        outputs: List[FeatureVector] = []
        for event in events_list:
            fv = engine.update(event)
            if fv is None:
                continue
            attach_lineage(fv, feature_set=feature_set, bundle_id=bundle_id)
            result = validator.validate(fv)
            fv.quality_flags = result.flags
            outputs.append(fv)
        store.put_many(outputs)
        return outputs
