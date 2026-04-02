from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from app.ingestion.storage import read_parquet


@dataclass(frozen=True, slots=True)
class NormalizedContractReport:
    path: str
    feed_type: str
    row_count: int
    required_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    required_metadata_keys: tuple[str, ...]
    missing_metadata_keys: tuple[str, ...]
    pass_ok: bool


def _metadata_dict(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    out: dict[str, str] = {}
    if isinstance(value, list):
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out[str(item[0])] = str(item[1])
    return out


def _infer_feed_type(columns: set[str]) -> str:
    if {"trade_id", "side"}.issubset(columns):
        return "trade"
    if {"open", "high", "low", "close", "volume"}.issubset(columns):
        return "kline"
    return "unknown"


def validate_normalized_contract(path: Path) -> NormalizedContractReport:
    table = read_parquet(Path(path))
    columns = set(table.column_names)
    feed_type = _infer_feed_type(columns)
    required_columns = {
        "venue",
        "feed_type",
        "normalizer_version",
        "symbol",
        "exchange_ts",
        "provider_ts",
        "receive_ts",
        "process_ts",
        "event_ts",
        "source",
        "source_id",
        "raw_run_id",
        "raw_ingestion_seq",
        "metadata",
    }
    if feed_type == "trade":
        required_columns |= {"price", "size", "trade_id", "side", "historical_feed_kind"}
    elif feed_type == "kline":
        required_columns |= {"open", "high", "low", "close", "volume", "interval", "open_ts", "close_ts"}

    missing_columns = tuple(sorted(required_columns - columns))

    metadata_keys = {
        "instrument_catalog_version",
        "instrument_snapshot",
        "metadata_source",
        "venue_snapshot_version",
        "normalizer_version",
        "raw_run_id",
        "raw_ingestion_seq",
    }
    if feed_type == "trade":
        metadata_keys.add("historical_feed_kind")
    missing_metadata_keys = set(metadata_keys)
    if table.num_rows > 0 and "metadata" in columns:
        first_row = table.slice(0, 1).to_pylist()[0]
        row_meta = _metadata_dict(first_row.get("metadata"))
        missing_metadata_keys = {key for key in metadata_keys if row_meta.get(key) in (None, "")}

    return NormalizedContractReport(
        path=str(path),
        feed_type=feed_type,
        row_count=table.num_rows,
        required_columns=tuple(sorted(required_columns)),
        missing_columns=tuple(sorted(missing_columns)),
        required_metadata_keys=tuple(sorted(metadata_keys)),
        missing_metadata_keys=tuple(sorted(missing_metadata_keys)),
        pass_ok=(table.num_rows > 0 and not missing_columns and not missing_metadata_keys),
    )


def write_normalized_contract_report(path: Path, report: NormalizedContractReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path
