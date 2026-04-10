from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.storage import list_normalized_partition_paths
from app.marketdata.dataset_contracts import (
    DatasetContractRegistry,
    build_dataset_contract_registry,
    dataset_contract_registry_path,
)
from app.marketdata.dataset_quality import (
    DatasetQualityRegistry,
    append_dataset_incidents,
    build_dataset_quality_registry,
    dataset_incident_log_path,
    dataset_quality_registry_path,
    write_dataset_quality_registry,
)
from app.ops.dataset_promotion import read_approved_dataset_registry


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class DatasetCatalogEntry:
    dataset_id: str
    dataset_version: str
    lineage_id: str
    env: str
    venue: str
    symbol: str
    stream_type: str
    feed_type: str
    partition_date: str
    partition_path: str
    contract_pass_ok: bool
    contract_mode: str
    normalizer_version: str | None
    historical_feed_kind: str | None
    quality_score: float | None
    quality_status: str | None
    incident_count: int
    approved_targets: tuple[str, ...]
    promoted_targets: tuple[str, ...]
    generated_at: str


@dataclass(frozen=True, slots=True)
class DatasetCatalog:
    generated_at: str
    entries: tuple[DatasetCatalogEntry, ...]


def dataset_catalog_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "datasets.json"


def build_dataset_catalog(
    base_dir: Path,
    env: str,
    *,
    contract_registry: DatasetContractRegistry,
    quality_registry: DatasetQualityRegistry,
) -> DatasetCatalog:
    promoted = read_approved_dataset_registry(Path(base_dir) / env / "catalog" / "approved-datasets.json")
    promoted_index: dict[tuple[str, str], tuple[str, ...]] = {}
    for entry in promoted.entries:
        key = (entry.normalized_path, entry.feed_type)
        promoted_index.setdefault(key, tuple())
        promoted_index[key] = tuple(sorted(set((*promoted_index[key], entry.target))))
    quality_by_path = {item.partition_path: item for item in quality_registry.reports}
    entries: list[DatasetCatalogEntry] = []
    for record in contract_registry.records:
        quality = quality_by_path.get(record.partition_path)
        entries.append(
            DatasetCatalogEntry(
                dataset_id=record.dataset_id,
                dataset_version=record.dataset_version,
                lineage_id=record.lineage_id,
                env=record.env,
                venue=record.venue,
                symbol=record.symbol,
                stream_type=record.stream_type,
                feed_type=record.feed_type,
                partition_date=record.partition_date,
                partition_path=record.partition_path,
                contract_pass_ok=record.contract.pass_ok,
                contract_mode=record.contract_mode,
                normalizer_version=record.normalizer_version,
                historical_feed_kind=record.historical_feed_kind,
                quality_score=quality.score if quality is not None else None,
                quality_status=quality.status if quality is not None else None,
                incident_count=quality.incident_count if quality is not None else 0,
                approved_targets=record.approved_targets,
                promoted_targets=promoted_index.get((record.partition_path, record.stream_type), ()),
                generated_at=_utc_now(),
            )
        )
    return DatasetCatalog(generated_at=_utc_now(), entries=tuple(sorted(entries, key=lambda item: item.dataset_id)))


def write_dataset_catalog(path: Path, catalog: DatasetCatalog) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": catalog.generated_at, "entries": [asdict(item) for item in catalog.entries]}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_dataset_catalog(path: Path) -> DatasetCatalog:
    resolved = Path(path)
    if not resolved.exists():
        return DatasetCatalog(generated_at=_utc_now(), entries=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    entries = tuple(DatasetCatalogEntry(**entry) for entry in payload.get("entries", ()))
    return DatasetCatalog(generated_at=str(payload.get("generated_at") or _utc_now()), entries=entries)


def refresh_dataset_catalog(base_dir: Path, env: str) -> DatasetCatalog:
    normalized_paths = list_normalized_partition_paths(Path(base_dir), env)
    contracts = build_dataset_contract_registry(normalized_paths)
    quality = build_dataset_quality_registry(normalized_paths)
    from app.marketdata.dataset_contracts import write_dataset_contract_registry

    write_dataset_contract_registry(dataset_contract_registry_path(base_dir, env), contracts)
    write_dataset_quality_registry(dataset_quality_registry_path(base_dir, env), quality)
    append_dataset_incidents(dataset_incident_log_path(base_dir, env), quality.reports)
    catalog = build_dataset_catalog(base_dir, env, contract_registry=contracts, quality_registry=quality)
    write_dataset_catalog(dataset_catalog_path(base_dir, env), catalog)
    return catalog
