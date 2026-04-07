from __future__ import annotations

from datetime import datetime

from app.common.dto import FeatureVector


def serialize_feature_vector(vector: FeatureVector) -> dict[str, object]:
    return {
        "symbol": vector.symbol,
        "ts": vector.ts.isoformat(),
        "available_ts": vector.available_ts.isoformat(),
        "source_cutoff_ts": vector.source_cutoff_ts.isoformat(),
        "values": dict(vector.values),
        "feature_set_name": vector.feature_set_name,
        "feature_set_version": vector.feature_set_version,
        "lineage_id": vector.lineage_id,
        "quality_flags": list(vector.quality_flags),
        "entity_keys": dict(vector.entity_keys),
    }


def deserialize_feature_vector(payload: dict[str, object]) -> FeatureVector:
    return FeatureVector(
        symbol=str(payload["symbol"]),
        ts=datetime.fromisoformat(str(payload["ts"])),
        available_ts=datetime.fromisoformat(str(payload["available_ts"])),
        source_cutoff_ts=datetime.fromisoformat(str(payload["source_cutoff_ts"])),
        values={str(key): float(value) for key, value in dict(payload["values"]).items()},
        feature_set_name=str(payload["feature_set_name"]),
        feature_set_version=str(payload["feature_set_version"]),
        lineage_id=str(payload.get("lineage_id", "")),
        quality_flags=tuple(payload.get("quality_flags", [])),
        entity_keys={str(key): str(value) for key, value in dict(payload.get("entity_keys", {})).items()},
    )
