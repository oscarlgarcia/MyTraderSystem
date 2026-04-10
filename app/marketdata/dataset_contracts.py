from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from app.ingestion.storage import read_parquet
from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX, FeedSupport, feed_support
from app.ops.normalized_contract import ContractMode, NormalizedContractReport, validate_normalized_contract


DatasetTarget = Literal["research", "backtesting", "paper", "live"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _partition_value(part: str, prefix: str) -> str:
    if part.startswith(prefix):
        return part[len(prefix) :]
    raise ValueError(f"partition component {part!r} does not start with {prefix!r}")


@dataclass(frozen=True, slots=True)
class DatasetPartitionRef:
    env: str
    venue: str
    symbol: str
    feed_type: str
    stream_type: str
    partition_date: str
    partition_path: str

    @property
    def dataset_id(self) -> str:
        return f"{self.env}:{self.venue}:{self.symbol}:{self.stream_type}:{self.partition_date}"


@dataclass(frozen=True, slots=True)
class DatasetContractRecord:
    dataset_id: str
    env: str
    venue: str
    symbol: str
    feed_type: str
    stream_type: str
    partition_date: str
    partition_path: str
    contract_mode: ContractMode
    contract: NormalizedContractReport
    support: FeedSupport
    dataset_version: str
    lineage_id: str
    normalizer_version: str | None
    historical_feed_kind: str | None
    approved_targets: tuple[DatasetTarget, ...]
    generated_at: str


@dataclass(frozen=True, slots=True)
class DatasetContractRegistry:
    generated_at: str
    records: tuple[DatasetContractRecord, ...]


def dataset_contract_registry_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "dataset-contracts.json"


def parse_normalized_partition_path(path: Path) -> DatasetPartitionRef:
    resolved = Path(path).resolve()
    parts = resolved.parts
    normalized_idx = parts.index("normalized")
    feed_type = parts[normalized_idx + 1]
    env = _partition_value(parts[normalized_idx + 2], "env=")
    venue = _partition_value(parts[normalized_idx + 3], "venue=")
    symbol = _partition_value(parts[normalized_idx + 4], "symbol=")
    partition_date = _partition_value(parts[normalized_idx + 5], "date=")
    for stream_type, candidate_feed_type in {"trade": "trades", "kline": "bars", "book": "quotes"}.items():
        if candidate_feed_type == feed_type:
            return DatasetPartitionRef(
                env=env,
                venue=venue,
                symbol=symbol,
                feed_type=feed_type,
                stream_type=stream_type,
                partition_date=partition_date,
                partition_path=str(resolved),
            )
    return DatasetPartitionRef(
        env=env,
        venue=venue,
        symbol=symbol,
        feed_type=feed_type,
        stream_type=feed_type.rstrip("s"),
        partition_date=partition_date,
        partition_path=str(resolved),
    )


def approved_targets_for_support(support: FeedSupport) -> tuple[DatasetTarget, ...]:
    targets: list[DatasetTarget] = ["research", "backtesting"]
    if support.supports_paper:
        targets.append("paper")
    if support.supports_live:
        targets.append("live")
    return tuple(targets)


def _build_dataset_version(
    ref: DatasetPartitionRef,
    *,
    contract: NormalizedContractReport,
    first: dict,
    last: dict,
) -> str:
    payload = json.dumps(
        {
            "dataset_id": ref.dataset_id,
            "partition_path": ref.partition_path,
            "contract_mode": contract.mode,
            "contract_pass_ok": contract.pass_ok,
            "row_count": int(contract.row_count),
            "normalizer_version": first.get("normalizer_version"),
            "first_raw_run_id": first.get("raw_run_id"),
            "first_raw_ingestion_seq": first.get("raw_ingestion_seq"),
            "last_raw_run_id": last.get("raw_run_id"),
            "last_raw_ingestion_seq": last.get("raw_ingestion_seq"),
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _build_lineage_id(ref: DatasetPartitionRef, *, first: dict, last: dict) -> str:
    payload = json.dumps(
        {
            "dataset_id": ref.dataset_id,
            "partition_path": ref.partition_path,
            "first_raw_run_id": first.get("raw_run_id"),
            "first_raw_ingestion_seq": first.get("raw_ingestion_seq"),
            "last_raw_run_id": last.get("raw_run_id"),
            "last_raw_ingestion_seq": last.get("raw_ingestion_seq"),
            "historical_feed_kind": first.get("historical_feed_kind"),
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_dataset_contract_record(
    normalized_path: Path,
    *,
    contract_mode: ContractMode = "strict",
) -> DatasetContractRecord:
    ref = parse_normalized_partition_path(Path(normalized_path))
    required_historical_feed_kind = "aggregate_trade" if ref.stream_type == "trade" else None
    contract = validate_normalized_contract(
        Path(normalized_path),
        mode=contract_mode,
        required_historical_feed_kind=required_historical_feed_kind,
    )
    support = feed_support(ref.stream_type if ref.stream_type in FEED_SUPPORT_MATRIX else "book")
    table = read_parquet(Path(normalized_path))
    rows = table.to_pylist()
    first = rows[0] if rows else {}
    last = rows[-1] if rows else {}
    metadata = dict(first.get("metadata") or {})
    dataset_version = _build_dataset_version(ref, contract=contract, first=first, last=last)
    lineage_id = _build_lineage_id(ref, first=first, last=last)
    return DatasetContractRecord(
        dataset_id=ref.dataset_id,
        env=ref.env,
        venue=ref.venue,
        symbol=ref.symbol,
        feed_type=ref.feed_type,
        stream_type=ref.stream_type,
        partition_date=ref.partition_date,
        partition_path=ref.partition_path,
        contract_mode=contract_mode,
        contract=contract,
        support=support,
        dataset_version=dataset_version,
        lineage_id=lineage_id,
        normalizer_version=first.get("normalizer_version") or metadata.get("normalizer_version"),
        historical_feed_kind=first.get("historical_feed_kind") or metadata.get("historical_feed_kind"),
        approved_targets=approved_targets_for_support(support),
        generated_at=_utc_now(),
    )


def build_dataset_contract_registry(
    normalized_paths: list[Path],
    *,
    contract_mode: ContractMode = "strict",
) -> DatasetContractRegistry:
    records = tuple(
        sorted(
            (build_dataset_contract_record(path, contract_mode=contract_mode) for path in normalized_paths),
            key=lambda item: item.dataset_id,
        )
    )
    return DatasetContractRegistry(generated_at=_utc_now(), records=records)


def write_dataset_contract_registry(path: Path, registry: DatasetContractRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": registry.generated_at,
        "records": [asdict(record) for record in registry.records],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_dataset_contract_registry(path: Path) -> DatasetContractRegistry:
    resolved = Path(path)
    if not resolved.exists():
        return DatasetContractRegistry(generated_at=_utc_now(), records=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    records: list[DatasetContractRecord] = []
    for raw in payload.get("records", ()):
        records.append(
            DatasetContractRecord(
                dataset_id=raw["dataset_id"],
                env=raw["env"],
                venue=raw["venue"],
                symbol=raw["symbol"],
                feed_type=raw["feed_type"],
                stream_type=raw["stream_type"],
                partition_date=raw["partition_date"],
                partition_path=raw["partition_path"],
                contract_mode=raw["contract_mode"],
                contract=NormalizedContractReport(**raw["contract"]),
                support=FeedSupport(**raw["support"]),
                dataset_version=str(raw.get("dataset_version") or "unknown"),
                lineage_id=str(raw.get("lineage_id") or "unknown"),
                normalizer_version=raw.get("normalizer_version"),
                historical_feed_kind=raw.get("historical_feed_kind"),
                approved_targets=tuple(raw.get("approved_targets", ())),
                generated_at=raw["generated_at"],
            )
        )
    return DatasetContractRegistry(
        generated_at=str(payload.get("generated_at") or _utc_now()),
        records=tuple(records),
    )
