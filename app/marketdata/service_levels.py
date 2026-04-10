from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.marketdata.dataset_catalog import DatasetCatalog
from app.marketdata.dataset_quality import DatasetQualityRegistry
from app.marketdata.delivery import DeliveryContractRegistry


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class DatasetServiceLevel:
    dataset_id: str
    stream_type: str
    venue: str
    symbol: str
    quality_status: str | None
    quality_score: float | None
    target_freshness_seconds: float | None
    completeness_mode: str | None
    service_status: str
    generated_at: str


@dataclass(frozen=True, slots=True)
class DatasetServiceLevelRegistry:
    env: str
    generated_at: str
    records: tuple[DatasetServiceLevel, ...]


def dataset_service_levels_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "dataset-service-levels.json"


def build_dataset_service_levels(
    *,
    env: str,
    catalog: DatasetCatalog,
    quality_registry: DatasetQualityRegistry,
    delivery_registry: DeliveryContractRegistry,
) -> DatasetServiceLevelRegistry:
    delivery_by_stream = {(item.venue, item.stream_type): item for item in delivery_registry.contracts}
    quality_by_dataset = {item.dataset_id: item for item in quality_registry.reports}
    records: list[DatasetServiceLevel] = []
    for entry in catalog.entries:
        quality = quality_by_dataset.get(entry.dataset_id)
        delivery = delivery_by_stream.get((entry.venue, entry.stream_type))
        if quality is None or delivery is None:
            service_status = "unknown"
        elif quality.status == "healthy":
            service_status = "within_slo"
        elif quality.status == "degraded":
            service_status = "at_risk"
        else:
            service_status = "breached"
        records.append(
            DatasetServiceLevel(
                dataset_id=entry.dataset_id,
                stream_type=entry.stream_type,
                venue=entry.venue,
                symbol=entry.symbol,
                quality_status=quality.status if quality is not None else None,
                quality_score=quality.score if quality is not None else None,
                target_freshness_seconds=delivery.freshness_expectation_seconds if delivery is not None else None,
                completeness_mode=delivery.completeness_mode if delivery is not None else None,
                service_status=service_status,
                generated_at=_utc_now(),
            )
        )
    return DatasetServiceLevelRegistry(env=env, generated_at=_utc_now(), records=tuple(records))


def write_dataset_service_levels(path: Path, registry: DatasetServiceLevelRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "env": registry.env,
                "generated_at": registry.generated_at,
                "records": [asdict(item) for item in registry.records],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def read_dataset_service_levels(path: Path, *, env: str | None = None) -> DatasetServiceLevelRegistry:
    resolved = Path(path)
    if not resolved.exists():
        return DatasetServiceLevelRegistry(env=env or "unknown", generated_at=_utc_now(), records=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return DatasetServiceLevelRegistry(
        env=str(payload.get("env") or env or "unknown"),
        generated_at=str(payload.get("generated_at") or _utc_now()),
        records=tuple(DatasetServiceLevel(**item) for item in payload.get("records", ())),
    )
