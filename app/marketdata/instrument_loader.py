"""
Authoritative instrument metadata loaders sourced from venue snapshots.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


def _precision_from_increment(value: str) -> int:
    text = str(value)
    if "." not in text:
        return 0
    return len(text.rstrip("0").split(".", 1)[1])


@dataclass(frozen=True, slots=True)
class InstrumentRecord:
    venue: str
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    tick_size: str
    step_size: str
    price_precision: int
    size_precision: int
    metadata_source: str
    venue_snapshot_version: str


def _bundled_snapshot_path(venue: str) -> Path:
    normalized = str(venue).strip().lower()
    return Path(__file__).resolve().parent / "data" / f"{normalized}_exchange_info.json"


def _snapshot_version(snapshot: dict[str, Any]) -> str:
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _filter_value(symbol_payload: dict[str, Any], filter_type: str, field: str) -> str:
    for item in symbol_payload.get("filters", []):
        if str(item.get("filterType", "")).upper() == filter_type.upper():
            value = item.get(field)
            if value in ("", None):
                break
            return str(value)
    raise KeyError(f"missing {filter_type}.{field} for instrument {symbol_payload.get('symbol')}")


def load_binance_exchange_info_snapshot(snapshot_path: Path | None = None) -> dict[str, Any]:
    path = snapshot_path or _bundled_snapshot_path("binance")
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_binance_instrument_records(
    *,
    symbols: Iterable[str] | None = None,
    snapshot_path: Path | None = None,
) -> tuple[InstrumentRecord, ...]:
    snapshot = load_binance_exchange_info_snapshot(snapshot_path)
    requested = None if symbols is None else {str(symbol).upper() for symbol in symbols}
    version = _snapshot_version(snapshot)
    records: list[InstrumentRecord] = []

    for item in snapshot.get("symbols", []):
        symbol = str(item.get("symbol", "")).upper()
        if not symbol:
            continue
        if requested is not None and symbol not in requested:
            continue
        records.append(
            InstrumentRecord(
                venue="BINANCE",
                symbol=symbol,
                base_asset=str(item["baseAsset"]).upper(),
                quote_asset=str(item["quoteAsset"]).upper(),
                contract_type="spot",
                tick_size=_filter_value(item, "PRICE_FILTER", "tickSize"),
                step_size=_filter_value(item, "LOT_SIZE", "stepSize"),
                price_precision=_precision_from_increment(_filter_value(item, "PRICE_FILTER", "tickSize")),
                size_precision=_precision_from_increment(_filter_value(item, "LOT_SIZE", "stepSize")),
                metadata_source="venue_snapshot",
                venue_snapshot_version=version,
            )
        )

    if requested is not None:
        resolved = {record.symbol for record in records}
        missing = sorted(requested - resolved)
        if missing:
            raise KeyError(f"missing authoritative Binance instrument metadata for symbols: {missing}")

    return tuple(records)
