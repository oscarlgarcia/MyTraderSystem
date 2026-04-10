from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from app.ingestion.storage import read_parquet


ContractMode = Literal["strict", "compat"]

_COMMON_REQUIRED_COLUMNS = {
    "venue",
    "feed_type",
    "normalizer_version",
    "symbol",
    "exchange_ts",
    "receive_ts",
    "process_ts",
    "event_ts",
    "source",
    "source_id",
    "metadata",
}
_STRICT_ONLY_COLUMNS = {
    "provider_ts",
    "raw_run_id",
    "raw_ingestion_seq",
}
_COMMON_REQUIRED_METADATA_KEYS = {
    "instrument_catalog_version",
    "instrument_snapshot",
    "metadata_source",
    "venue_snapshot_version",
    "normalizer_version",
}
_STRICT_ONLY_METADATA_KEYS = {
    "raw_run_id",
    "raw_ingestion_seq",
    "metadata_snapshot_mode",
    "instrument_catalog_snapshot_json",
}


@dataclass(frozen=True, slots=True)
class NormalizedContractReport:
    path: str
    mode: ContractMode
    feed_type: str
    row_count: int
    required_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    required_metadata_keys: tuple[str, ...]
    missing_metadata_keys: tuple[str, ...]
    warnings: tuple[str, ...]
    historical_feed_kind: str | None
    required_historical_feed_kind: str | None
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
    if {"bid_price", "bid_size", "ask_price", "ask_size"}.issubset(columns):
        return "book"
    return "unknown"


def _required_columns_for(feed_type: str, mode: ContractMode) -> set[str]:
    required = set(_COMMON_REQUIRED_COLUMNS)
    if mode == "strict":
        required |= set(_STRICT_ONLY_COLUMNS)
    if feed_type == "trade":
        required |= {"price", "size", "trade_id", "side"}
        if mode == "strict":
            required.add("historical_feed_kind")
    elif feed_type == "kline":
        required |= {"open", "high", "low", "close", "volume", "interval", "open_ts", "close_ts"}
    elif feed_type == "book":
        required |= {"bid_price", "bid_size", "ask_price", "ask_size", "sequence_id"}
    return required


def _required_metadata_keys_for(feed_type: str, mode: ContractMode) -> set[str]:
    required = set(_COMMON_REQUIRED_METADATA_KEYS)
    if mode == "strict":
        required |= set(_STRICT_ONLY_METADATA_KEYS)
    if feed_type == "trade":
        required.add("historical_feed_kind")
    return required


def validate_normalized_contract(
    path: Path,
    *,
    mode: ContractMode = "strict",
    required_historical_feed_kind: str | None = None,
) -> NormalizedContractReport:
    table = read_parquet(Path(path))
    columns = set(table.column_names)
    feed_type = _infer_feed_type(columns)
    required_columns = _required_columns_for(feed_type, mode)
    missing_columns = tuple(sorted(required_columns - columns))
    strict_missing_columns = tuple(sorted(_STRICT_ONLY_COLUMNS - columns))

    metadata_keys = _required_metadata_keys_for(feed_type, mode)
    missing_metadata_keys = set(metadata_keys)
    strict_missing_metadata_keys = set(_STRICT_ONLY_METADATA_KEYS)
    first_row_metadata: dict[str, str] = {}
    historical_feed_kind: str | None = None
    if table.num_rows > 0 and "metadata" in columns:
        first_row = table.slice(0, 1).to_pylist()[0]
        first_row_metadata = _metadata_dict(first_row.get("metadata"))
        missing_metadata_keys = {key for key in metadata_keys if first_row_metadata.get(key) in (None, "")}
        strict_missing_metadata_keys = {key for key in _STRICT_ONLY_METADATA_KEYS if first_row_metadata.get(key) in (None, "")}
        historical_feed_kind = str(
            first_row.get("historical_feed_kind")
            or first_row_metadata.get("historical_feed_kind")
            or ""
        ).strip() or None

    warnings: list[str] = []
    if mode == "compat":
        compat_missing_columns = sorted(strict_missing_columns)
        compat_missing_metadata = sorted(strict_missing_metadata_keys)
        if compat_missing_columns:
            warnings.append(
                f"legacy-compatible dataset is missing strict-only columns: {', '.join(compat_missing_columns)}"
            )
        if compat_missing_metadata:
            warnings.append(
                f"legacy-compatible dataset is missing strict-only metadata: {', '.join(compat_missing_metadata)}"
            )
        missing_columns = tuple(sorted(set(missing_columns) - set(_STRICT_ONLY_COLUMNS)))
        missing_metadata_keys = tuple(sorted(set(missing_metadata_keys) - set(_STRICT_ONLY_METADATA_KEYS)))
    else:
        missing_metadata_keys = tuple(sorted(missing_metadata_keys))

    if feed_type == "trade" and required_historical_feed_kind is not None:
        if historical_feed_kind != required_historical_feed_kind:
            warnings.append(
                f"historical_feed_kind mismatch: expected {required_historical_feed_kind}, got {historical_feed_kind or 'missing'}"
            )

    pass_ok = bool(
        table.num_rows > 0
        and not missing_columns
        and not missing_metadata_keys
        and not (
            feed_type == "trade"
            and required_historical_feed_kind is not None
            and historical_feed_kind != required_historical_feed_kind
        )
    )
    return NormalizedContractReport(
        path=str(path),
        mode=mode,
        feed_type=feed_type,
        row_count=table.num_rows,
        required_columns=tuple(sorted(required_columns)),
        missing_columns=tuple(sorted(missing_columns)),
        required_metadata_keys=tuple(sorted(metadata_keys)),
        missing_metadata_keys=tuple(sorted(missing_metadata_keys)),
        warnings=tuple(warnings),
        historical_feed_kind=historical_feed_kind,
        required_historical_feed_kind=required_historical_feed_kind,
        pass_ok=pass_ok,
    )


def write_normalized_contract_report(path: Path, report: NormalizedContractReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path
