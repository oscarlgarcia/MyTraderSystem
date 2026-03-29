"""
Persistencia opcional de FeatureVector en disco (uso batch/offline).
Solo JSON para evitar dependencias externas; Parquet queda como TODO controlado.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from datetime import datetime

from app.common.dto import FeatureVector

STORAGE_VERSION = "1.0"


class StorageError(ValueError):
    """Errores de persistencia / esquema."""


def _fv_to_dict(fv: FeatureVector) -> dict:
    return {"symbol": fv.symbol, "ts": fv.ts.isoformat(), "values": fv.values}


def _fv_from_dict(payload: dict) -> FeatureVector:
    try:
        symbol = payload["symbol"]
        ts = datetime.fromisoformat(payload["ts"])
        values = payload["values"]
    except Exception as exc:  # pragma: no cover - rutas de datos corruptos
        raise StorageError(f"invalid feature payload: {payload}") from exc
    return FeatureVector(symbol=symbol, ts=ts, values=values)


def save(
    features: Iterable[FeatureVector],
    path: str | Path,
    *,
    feature_set: Optional[Tuple[str, str]] = None,
) -> None:
    """
    Guarda un lote de FeatureVector en JSON.
    """
    p = Path(path)
    if p.suffix.lower() not in {".json", ".jsonl"}:
        raise StorageError("only .json/.jsonl supported (Parquet no incluido sin deps externas)")

    data = {
        "storage_version": STORAGE_VERSION,
        "feature_set": {"name": feature_set[0], "version": feature_set[1]} if feature_set else None,
        "features": [_fv_to_dict(fv) for fv in features],
    }
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load(path: str | Path) -> Tuple[List[FeatureVector], Optional[Tuple[str, str]]]:
    """
    Carga un lote de FeatureVector desde JSON.
    Devuelve (lista, feature_set opcional).
    """
    p = Path(path)
    if not p.exists():
        raise StorageError(f"file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    version = data.get("storage_version")
    if version != STORAGE_VERSION:
        raise StorageError(f"incompatible storage_version: {version}")
    fs = data.get("feature_set")
    feature_set = (fs["name"], fs["version"]) if fs else None
    features = [_fv_from_dict(item) for item in data.get("features", [])]
    return features, feature_set
