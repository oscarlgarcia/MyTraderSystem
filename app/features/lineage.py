from __future__ import annotations

import hashlib
import json
from typing import Iterable

from app.common.dto import FeatureVector, MarketEvent
from app.features.definitions import FeatureSetDefinition


def fingerprint_events(events: Iterable[MarketEvent]) -> str:
    payload = [
        {
            "symbol": ev.symbol,
            "event_ts": ev.event_ts.isoformat(),
            "published_ts": ev.published_ts.isoformat(),
            "available_ts": ev.available_ts.isoformat(),
            "price": ev.price,
            "size": ev.size,
            "source": ev.source,
        }
        for ev in events
    ]
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_feature_bundle_id(*, feature_set: FeatureSetDefinition, input_fingerprint: str, run_id: str) -> str:
    raw = f"{feature_set.definition_hash}|{input_fingerprint}|{run_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def attach_lineage(fv: FeatureVector, *, feature_set: FeatureSetDefinition, bundle_id: str) -> FeatureVector:
    fv.feature_set_name = feature_set.name
    fv.feature_set_version = feature_set.version
    fv.lineage_id = bundle_id
    return fv
