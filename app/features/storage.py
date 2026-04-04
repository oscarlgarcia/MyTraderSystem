"""Legacy JSON batch persistence kept for compatibility; V2 store lives in offline_store.py."""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from datetime import datetime

from app.common.dto import FeatureVector

STORAGE_VERSION = "2.0"


class StorageError(ValueError):
    """Errores de persistencia / esquema."""


def _warn_legacy_storage(name: str) -> None:
    warnings.warn(f"app.features.storage.{name} is legacy; migrate to OfflineFeatureStore/OnlineFeatureStore APIs", DeprecationWarning, stacklevel=2)


def _fv_to_dict(fv: FeatureVector) -> dict:
    return {
        "symbol": fv.symbol,
        "ts": fv.ts.isoformat(),
        "available_ts": fv.available_ts.isoformat(),
        "source_cutoff_ts": fv.source_cutoff_ts.isoformat(),
        "values": fv.values,
        "feature_set_name": fv.feature_set_name,
        "feature_set_version": fv.feature_set_version,
        "lineage_id": fv.lineage_id,
        "quality_flags": list(fv.quality_flags),
        "entity_keys": fv.entity_keys,
    }


def _fv_from_dict(payload: dict) -> FeatureVector:
    try:
        symbol = payload["symbol"]
        ts = datetime.fromisoformat(payload["ts"])
        values = payload["values"]
        available_ts = datetime.fromisoformat(payload.get("available_ts", payload["ts"]))
        source_cutoff_ts = datetime.fromisoformat(payload.get("source_cutoff_ts", payload.get("available_ts", payload["ts"])))
    except Exception as exc:
        raise StorageError(f"invalid feature payload: {payload}") from exc
    return FeatureVector(
        symbol=symbol,
        ts=ts,
        available_ts=available_ts,
        source_cutoff_ts=source_cutoff_ts,
        values=values,
        feature_set_name=payload.get("feature_set_name", "legacy"),
        feature_set_version=payload.get("feature_set_version", "legacy"),
        lineage_id=payload.get("lineage_id", ""),
        quality_flags=tuple(payload.get("quality_flags", [])),
        entity_keys=payload.get("entity_keys", {"symbol": symbol}),
    )


def save(features: Iterable[FeatureVector], path: str | Path, *, feature_set: Optional[Tuple[str, str]] = None) -> None:
    _warn_legacy_storage("save")
    p = Path(path)
    if p.suffix.lower() not in {".json", ".jsonl"}:
        raise StorageError("only .json/.jsonl supported (Parquet no incluido sin deps externas)")
    name, version = feature_set if feature_set else (None, None)
    data = {
        "storage_version": STORAGE_VERSION,
        "feature_set": {"name": name, "version": version} if feature_set else None,
        "features": [_fv_to_dict(fv) for fv in features],
    }
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load(path: str | Path) -> Tuple[List[FeatureVector], Optional[Tuple[str, str]]]:
    _warn_legacy_storage("load")
    p = Path(path)
    if not p.exists():
        raise StorageError(f"file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    version = data.get("storage_version")
    if version not in {"1.0", STORAGE_VERSION}:
        raise StorageError(f"incompatible storage_version: {version}")
    fs = data.get("feature_set")
    feature_set = (fs["name"], fs["version"]) if fs else None
    features = [_fv_from_dict(item) for item in data.get("features", [])]
    return features, feature_set
