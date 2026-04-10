from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from app.ingestion.storage import list_normalized_partition_paths, read_parquet
from app.marketdata.dataset_contracts import build_dataset_contract_record, parse_normalized_partition_path


@dataclass(frozen=True, slots=True)
class HistoricalQueryRequest:
    base_dir: Path
    env: str
    stream_type: str
    symbol: str | None = None
    venue: str = "BINANCE"
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    limit: int | None = None


def _matching_partitions(request: HistoricalQueryRequest) -> list[Path]:
    partitions: list[Path] = []
    for path in list_normalized_partition_paths(Path(request.base_dir), request.env):
        ref = parse_normalized_partition_path(path)
        if ref.stream_type != request.stream_type:
            continue
        if request.symbol and ref.symbol != request.symbol:
            continue
        if ref.venue != request.venue:
            continue
        partitions.append(path)
    return sorted(partitions)


def query_rows(request: HistoricalQueryRequest) -> list[dict]:
    rows: list[dict] = []
    for partition in _matching_partitions(request):
        contract_record = build_dataset_contract_record(partition)
        partition_ref = parse_normalized_partition_path(partition)
        for row in read_parquet(partition).to_pylist():
            exchange_ts = row.get("exchange_ts")
            if request.start_ts is not None and exchange_ts is not None and exchange_ts < request.start_ts:
                continue
            if request.end_ts is not None and exchange_ts is not None and exchange_ts > request.end_ts:
                continue
            row = dict(row)
            row.setdefault("dataset_id", partition_ref.dataset_id)
            row.setdefault("dataset_version", contract_record.dataset_version)
            row.setdefault("lineage_id", contract_record.lineage_id)
            row.setdefault("partition_path", str(partition))
            rows.append(row)
            if request.limit is not None and len(rows) >= request.limit:
                return rows
    return rows


def query_table(request: HistoricalQueryRequest) -> pa.Table:
    rows = query_rows(request)
    if not rows:
        return pa.table({})
    return pa.Table.from_pylist(rows)


def query_latest_row(request: HistoricalQueryRequest) -> dict | None:
    latest = None
    latest_ts = None
    for row in query_rows(request):
        exchange_ts = row.get("exchange_ts")
        if latest is None:
            latest = row
            latest_ts = exchange_ts
            continue
        if exchange_ts is not None and latest_ts is not None and exchange_ts >= latest_ts:
            latest = row
            latest_ts = exchange_ts
        elif latest_ts is None and exchange_ts is not None:
            latest = row
            latest_ts = exchange_ts
    return latest
