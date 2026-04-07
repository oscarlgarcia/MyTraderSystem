from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
from typing import Iterable, List, Optional

from app.common.dto import FeatureVector, Signal


@dataclass(frozen=True)
class DecisionAuditRecord:
    symbol: str
    decision_ts: datetime
    side: str
    size: float
    feature_bundle_id: str
    feature_set_name: str
    feature_set_version: str
    quality_flags: tuple[str, ...]
    dataset_id: str = ""
    feature_schema_hash: str = ""
    training_bundle_id: str = ""
    consumer_name: str = ""
    consumer_kind: str = ""


def build_decision_audit_record(
    feature: FeatureVector,
    signal: Signal,
    *,
    consumer_metadata: dict[str, str] | None = None,
) -> DecisionAuditRecord:
    metadata = consumer_metadata or {}
    return DecisionAuditRecord(
        symbol=signal.symbol,
        decision_ts=signal.ts,
        side=signal.side,
        size=signal.size,
        feature_bundle_id=feature.lineage_id,
        feature_set_name=feature.feature_set_name,
        feature_set_version=feature.feature_set_version,
        quality_flags=tuple(feature.quality_flags),
        dataset_id=metadata.get("dataset_id", ""),
        feature_schema_hash=metadata.get("feature_schema_hash", ""),
        training_bundle_id=metadata.get("training_bundle_id", ""),
        consumer_name=metadata.get("consumer_name", ""),
        consumer_kind=metadata.get("consumer_kind", ""),
    )


def persist_decision_audits(records: Iterable[DecisionAuditRecord], path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps({
                "symbol": record.symbol,
                "decision_ts": record.decision_ts.isoformat(),
                "side": record.side,
                "size": record.size,
                "feature_bundle_id": record.feature_bundle_id,
                "feature_set_name": record.feature_set_name,
                "feature_set_version": record.feature_set_version,
                "quality_flags": list(record.quality_flags),
                "dataset_id": record.dataset_id,
                "feature_schema_hash": record.feature_schema_hash,
                "training_bundle_id": record.training_bundle_id,
                "consumer_name": record.consumer_name,
                "consumer_kind": record.consumer_kind,
            }, ensure_ascii=False) + "\n")
