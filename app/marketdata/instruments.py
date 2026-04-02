"""
Instrument catalog backed by authoritative venue metadata snapshots.
"""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

from app.common.dto import normalize_symbol
from app.marketdata.instrument_loader import (
    InstrumentRecord,
    fetch_binance_exchange_info_snapshot,
    load_binance_instrument_records,
    load_binance_instrument_records_from_snapshot,
)


KNOWN_QUOTE_ASSETS: tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "BTC",
    "ETH",
    "EUR",
)


def _normalize_venue(venue: str) -> str:
    normalized = str(venue).upper()
    if not normalized:
        raise ValueError("venue must be non-empty")
    return normalized


def _precision_from_increment(value: str) -> int:
    text = format(Decimal(str(value)).normalize(), "f")
    if "." not in text:
        return 0
    return len(text.rstrip("0").split(".", 1)[1])


def infer_spot_assets(symbol: str, *, known_quotes: Iterable[str] = KNOWN_QUOTE_ASSETS) -> tuple[str, str]:
    normalized = normalize_symbol(symbol)
    for quote_asset in sorted((str(item).upper() for item in known_quotes), key=len, reverse=True):
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            return normalized[: -len(quote_asset)], quote_asset
    raise KeyError(f"unsupported instrument symbol: {normalized}")


@dataclass(frozen=True, slots=True)
class Instrument:
    venue: str
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str = "spot"
    tick_size: str = "0.01"
    step_size: str = "0.000001"
    price_precision: int = 2
    size_precision: int = 6
    metadata_source: str = "manual"
    venue_snapshot_version: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", _normalize_venue(self.venue))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "base_asset", normalize_symbol(self.base_asset))
        object.__setattr__(self, "quote_asset", normalize_symbol(self.quote_asset))
        object.__setattr__(self, "contract_type", str(self.contract_type).lower())
        object.__setattr__(self, "tick_size", str(self.tick_size))
        object.__setattr__(self, "step_size", str(self.step_size))
        object.__setattr__(self, "price_precision", int(self.price_precision))
        object.__setattr__(self, "size_precision", int(self.size_precision))
        object.__setattr__(self, "metadata_source", str(self.metadata_source))
        object.__setattr__(
            self,
            "venue_snapshot_version",
            None if self.venue_snapshot_version in ("", None) else str(self.venue_snapshot_version),
        )

    def as_metadata(self) -> dict[str, str]:
        metadata = {
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "contract_type": self.contract_type,
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "price_precision": str(self.price_precision),
            "size_precision": str(self.size_precision),
            "metadata_source": self.metadata_source,
        }
        if self.venue_snapshot_version is not None:
            metadata["venue_snapshot_version"] = self.venue_snapshot_version
        return metadata

    def as_dict(self) -> dict[str, str | int]:
        payload: dict[str, str | int] = {
            "venue": self.venue,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "contract_type": self.contract_type,
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "price_precision": self.price_precision,
            "size_precision": self.size_precision,
            "metadata_source": self.metadata_source,
        }
        if self.venue_snapshot_version is not None:
            payload["venue_snapshot_version"] = self.venue_snapshot_version
        return payload


class InstrumentCatalog:
    def __init__(self, instruments: Iterable[Instrument] | None = None) -> None:
        self._by_key: dict[tuple[str, str], Instrument] = {}
        if instruments is not None:
            for instrument in instruments:
                self.register(instrument)

    def register(self, instrument: Instrument) -> None:
        self._by_key[(instrument.venue, instrument.symbol)] = instrument

    def register_authoritative_record(self, record: InstrumentRecord) -> Instrument:
        instrument = Instrument(
            venue=record.venue,
            symbol=record.symbol,
            base_asset=record.base_asset,
            quote_asset=record.quote_asset,
            contract_type=record.contract_type,
            tick_size=record.tick_size,
            step_size=record.step_size,
            price_precision=record.price_precision,
            size_precision=record.size_precision,
            metadata_source=record.metadata_source,
            venue_snapshot_version=record.venue_snapshot_version,
        )
        self.register(instrument)
        return instrument

    def register_static_spot_symbol(
        self,
        symbol: str,
        *,
        venue: str = "BINANCE",
        base_asset: str | None = None,
        quote_asset: str | None = None,
        tick_size: str = "0.01",
        step_size: str = "0.000001",
    ) -> Instrument:
        normalized_symbol = normalize_symbol(symbol)
        if base_asset is None or quote_asset is None:
            inferred_base, inferred_quote = infer_spot_assets(normalized_symbol)
            base_asset = base_asset or inferred_base
            quote_asset = quote_asset or inferred_quote
        instrument = Instrument(
            venue=venue,
            symbol=normalized_symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            contract_type="spot",
            tick_size=tick_size,
            step_size=step_size,
            price_precision=_precision_from_increment(tick_size),
            size_precision=_precision_from_increment(step_size),
            metadata_source="inferred_static",
        )
        self.register(instrument)
        return instrument

    def resolve(self, venue: str, symbol: str) -> Instrument:
        key = (_normalize_venue(venue), normalize_symbol(symbol))
        instrument = self._by_key.get(key)
        if instrument is None:
            raise KeyError(f"unsupported instrument for venue={key[0]} symbol={key[1]}")
        return instrument

    def has(self, venue: str, symbol: str) -> bool:
        return (_normalize_venue(venue), normalize_symbol(symbol)) in self._by_key

    def snapshot(self) -> tuple[dict[str, str | int], ...]:
        return tuple(
            instrument.as_dict()
            for _, instrument in sorted(self._by_key.items(), key=lambda item: item[0])
        )

    def instruments(self) -> tuple[Instrument, ...]:
        return tuple(
            instrument for _, instrument in sorted(self._by_key.items(), key=lambda item: item[0])
        )

    def version(self) -> str:
        encoded = json.dumps(self.snapshot(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]

    def instrument_snapshot(self, venue: str, symbol: str) -> dict[str, str | int]:
        return self.resolve(venue, symbol).as_dict()


@dataclass(frozen=True, slots=True)
class InstrumentCatalogDrift:
    has_drift: bool
    material: bool
    added_symbols: tuple[str, ...]
    removed_symbols: tuple[str, ...]
    changed_symbols: tuple[str, ...]
    changed_fields_by_symbol: dict[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class PersistedInstrumentCatalogSnapshot:
    env: str
    venue: str
    run_label: str
    persisted_at: str
    instrument_catalog_version: str
    instrument_catalog_snapshot: tuple[dict[str, str | int], ...]
    path: Path
    drift: InstrumentCatalogDrift | None = None
    metadata_snapshot_mode: str = "bundled"
    venue_snapshot_path: Path | None = None
    venue_snapshot_version: str | None = None
    venue_snapshot_sha256: str | None = None
    fallback_reason: str | None = None
    catalog: InstrumentCatalog | None = None

    def instrument_metadata(self, symbol: str, *, venue: str = "BINANCE") -> dict[str, str]:
        resolved_catalog = self.catalog or active_instrument_catalog()
        metadata = instrument_metadata(symbol, venue=venue, catalog=resolved_catalog)
        metadata["metadata_snapshot_mode"] = self.metadata_snapshot_mode
        metadata["instrument_catalog_snapshot_json"] = json.dumps(
            self.instrument_catalog_snapshot,
            sort_keys=True,
            separators=(",", ":"),
        )
        if self.venue_snapshot_path is not None:
            metadata["venue_snapshot_path"] = str(self.venue_snapshot_path)
        if self.venue_snapshot_version is not None:
            metadata["venue_snapshot_version"] = self.venue_snapshot_version
        if self.venue_snapshot_sha256 is not None:
            metadata["venue_snapshot_sha256"] = self.venue_snapshot_sha256
        if self.fallback_reason not in (None, ""):
            metadata["fallback_reason"] = str(self.fallback_reason)
        return metadata


_ACTIVE_INSTRUMENT_CATALOG: ContextVar[InstrumentCatalog | None] = ContextVar(
    "ACTIVE_INSTRUMENT_CATALOG",
    default=None,
)


def _snapshot_key(entry: dict[str, str | int]) -> tuple[str, str]:
    return _normalize_venue(str(entry["venue"])), normalize_symbol(str(entry["symbol"]))


def _snapshot_map(snapshot: Iterable[dict[str, str | int]]) -> dict[tuple[str, str], dict[str, str | int]]:
    return {_snapshot_key(entry): dict(entry) for entry in snapshot}


def _sanitize_run_label(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value).strip())
    return normalized or "catalog"


def active_instrument_catalog(*, fallback: InstrumentCatalog | None = None) -> InstrumentCatalog:
    return fallback or _ACTIVE_INSTRUMENT_CATALOG.get() or DEFAULT_INSTRUMENT_CATALOG


@contextmanager
def use_instrument_catalog(catalog: InstrumentCatalog | None) -> Iterator[InstrumentCatalog]:
    resolved_catalog = active_instrument_catalog(fallback=catalog)
    token = _ACTIVE_INSTRUMENT_CATALOG.set(resolved_catalog)
    try:
        yield resolved_catalog
    finally:
        _ACTIVE_INSTRUMENT_CATALOG.reset(token)


def instrument_catalog_snapshot(*, catalog: InstrumentCatalog | None = None) -> tuple[dict[str, str | int], ...]:
    return active_instrument_catalog(fallback=catalog).snapshot()


def instrument_catalog_snapshot_json(*, catalog: InstrumentCatalog | None = None) -> str:
    return json.dumps(instrument_catalog_snapshot(catalog=catalog), sort_keys=True, separators=(",", ":"))


def detect_instrument_catalog_drift(
    previous_snapshot: Iterable[dict[str, str | int]],
    current_snapshot: Iterable[dict[str, str | int]],
) -> InstrumentCatalogDrift:
    previous = _snapshot_map(previous_snapshot)
    current = _snapshot_map(current_snapshot)
    added = sorted(symbol for _, symbol in current.keys() - previous.keys())
    removed = sorted(symbol for _, symbol in previous.keys() - current.keys())
    changed_fields_by_symbol: dict[str, tuple[str, ...]] = {}
    material_fields = {"base_asset", "quote_asset", "contract_type", "tick_size", "step_size", "price_precision", "size_precision"}

    for key in sorted(current.keys() & previous.keys()):
        before = previous[key]
        after = current[key]
        changed_fields = tuple(
            sorted(
                field
                for field in set(before) | set(after)
                if before.get(field) != after.get(field)
            )
        )
        if changed_fields:
            changed_fields_by_symbol[key[1]] = changed_fields

    changed_symbols = tuple(sorted(changed_fields_by_symbol))
    material = bool(added or removed)
    if not material:
        material = any(any(field in material_fields for field in fields) for fields in changed_fields_by_symbol.values())

    return InstrumentCatalogDrift(
        has_drift=bool(added or removed or changed_symbols),
        material=material,
        added_symbols=tuple(added),
        removed_symbols=tuple(removed),
        changed_symbols=changed_symbols,
        changed_fields_by_symbol=changed_fields_by_symbol,
    )


def _catalog_snapshot_root(base_dir: Path, *, env: str, venue: str) -> Path:
    return Path(base_dir) / "metadata" / "instruments" / f"env={env}" / f"venue={_normalize_venue(venue)}"


def _vendor_snapshot_root(base_dir: Path, *, env: str, venue: str) -> Path:
    return Path(base_dir) / "metadata" / "vendor" / f"env={env}" / f"venue={_normalize_venue(venue)}"


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _sha256_for_path(path: Path | None) -> str | None:
    if path is None or not Path(path).exists():
        return None
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _catalog_from_records(records: Iterable[InstrumentRecord]) -> InstrumentCatalog:
    catalog = InstrumentCatalog()
    for record in records:
        catalog.register_authoritative_record(record)
    return catalog


def persist_instrument_catalog_snapshot(
    *,
    base_dir: Path,
    env: str,
    venue: str = "BINANCE",
    run_label: str,
    catalog: InstrumentCatalog | None = None,
    metadata_snapshot_mode: str = "bundled",
    venue_snapshot_path: Path | None = None,
    venue_snapshot_version: str | None = None,
    fallback_reason: str | None = None,
) -> PersistedInstrumentCatalogSnapshot:
    resolved_catalog = active_instrument_catalog(fallback=catalog)
    snapshot = resolved_catalog.snapshot()
    version = resolved_catalog.version()
    persisted_at = datetime.now(timezone.utc).isoformat()
    root = _catalog_snapshot_root(Path(base_dir), env=env, venue=venue)
    latest_path = root / "latest.json"
    run_path = root / "runs" / f"{_sanitize_run_label(run_label)}.json"

    drift: InstrumentCatalogDrift | None = None
    if latest_path.exists():
        previous_payload = json.loads(latest_path.read_text(encoding="utf-8"))
        previous_snapshot = tuple(previous_payload.get("instrument_catalog_snapshot", []))
        drift = detect_instrument_catalog_drift(previous_snapshot, snapshot)

    payload = {
        "env": env,
        "venue": _normalize_venue(venue),
        "run_label": run_label,
        "persisted_at": persisted_at,
        "instrument_catalog_version": version,
        "instrument_catalog_snapshot": snapshot,
        "metadata_snapshot_mode": metadata_snapshot_mode,
        "venue_snapshot_path": str(venue_snapshot_path) if venue_snapshot_path is not None else None,
        "venue_snapshot_version": venue_snapshot_version,
        "venue_snapshot_sha256": _sha256_for_path(venue_snapshot_path),
        "fallback_reason": fallback_reason,
        "drift": None
        if drift is None
        else {
            "has_drift": drift.has_drift,
            "material": drift.material,
            "added_symbols": list(drift.added_symbols),
            "removed_symbols": list(drift.removed_symbols),
            "changed_symbols": list(drift.changed_symbols),
            "changed_fields_by_symbol": {
                symbol: list(fields) for symbol, fields in drift.changed_fields_by_symbol.items()
            },
        },
    }
    _write_json_atomic(run_path, payload)
    _write_json_atomic(latest_path, payload)

    return PersistedInstrumentCatalogSnapshot(
        env=env,
        venue=_normalize_venue(venue),
        run_label=run_label,
        persisted_at=persisted_at,
        instrument_catalog_version=version,
        instrument_catalog_snapshot=snapshot,
        path=run_path,
        drift=drift,
        metadata_snapshot_mode=metadata_snapshot_mode,
        venue_snapshot_path=venue_snapshot_path,
        venue_snapshot_version=venue_snapshot_version,
        venue_snapshot_sha256=_sha256_for_path(venue_snapshot_path),
        fallback_reason=fallback_reason,
        catalog=resolved_catalog,
    )


def persist_runtime_instrument_catalog_snapshot(
    *,
    base_dir: Path,
    env: str,
    venue: str = "BINANCE",
    run_label: str,
    rest_base: str,
    symbols: Iterable[str] | None = None,
) -> PersistedInstrumentCatalogSnapshot:
    normalized_venue = _normalize_venue(venue)
    if normalized_venue != "BINANCE":
        raise KeyError(f"runtime instrument snapshot loader not available for venue={normalized_venue}")

    vendor_root = _vendor_snapshot_root(Path(base_dir), env=env, venue=normalized_venue)
    latest_vendor_path = vendor_root / "latest.json"
    run_vendor_path = vendor_root / "runs" / f"{_sanitize_run_label(run_label)}-exchange-info.json"

    try:
        snapshot = fetch_binance_exchange_info_snapshot(base_url=rest_base)
        _write_json_atomic(run_vendor_path, snapshot)
        _write_json_atomic(latest_vendor_path, snapshot)
        records = load_binance_instrument_records_from_snapshot(
            snapshot,
            symbols=None,
            metadata_source="venue_runtime_snapshot",
        )
        if symbols is not None:
            requested = {normalize_symbol(symbol) for symbol in symbols}
            resolved = {record.symbol for record in records}
            missing = sorted(requested - resolved)
            if missing:
                raise KeyError(f"missing authoritative Binance instrument metadata for symbols: {missing}")
        catalog = _catalog_from_records(records)
        venue_snapshot_version = records[0].venue_snapshot_version if records else catalog.version()
        return persist_instrument_catalog_snapshot(
            base_dir=base_dir,
            env=env,
            venue=normalized_venue,
            run_label=run_label,
            catalog=catalog,
            metadata_snapshot_mode="runtime",
            venue_snapshot_path=run_vendor_path,
            venue_snapshot_version=venue_snapshot_version,
        )
    except Exception as exc:
        fallback_catalog = InstrumentCatalog()
        fallback_records = load_binance_instrument_records(symbols=None)
        if symbols is not None:
            requested = {normalize_symbol(symbol) for symbol in symbols}
            resolved = {record.symbol for record in fallback_records}
            missing = sorted(requested - resolved)
            if missing:
                raise KeyError(f"missing authoritative Binance instrument metadata for symbols: {missing}") from exc
        for record in fallback_records:
            fallback_catalog.register_authoritative_record(record)
        return persist_instrument_catalog_snapshot(
            base_dir=base_dir,
            env=env,
            venue=normalized_venue,
            run_label=run_label,
            catalog=fallback_catalog,
            metadata_snapshot_mode="fallback",
            venue_snapshot_path=latest_vendor_path if latest_vendor_path.exists() else None,
            venue_snapshot_version=fallback_records[0].venue_snapshot_version if fallback_records else fallback_catalog.version(),
            fallback_reason=str(exc),
        )


def _load_default_instrument_catalog() -> InstrumentCatalog:
    catalog = InstrumentCatalog()
    for record in load_binance_instrument_records(symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT")):
        catalog.register_authoritative_record(record)
    return catalog


DEFAULT_INSTRUMENT_CATALOG = _load_default_instrument_catalog()
DEFAULT_INSTRUMENTS: tuple[Instrument, ...] = DEFAULT_INSTRUMENT_CATALOG.instruments()


def get_default_instrument_catalog() -> InstrumentCatalog:
    return DEFAULT_INSTRUMENT_CATALOG


def ensure_default_instruments(symbols: Iterable[str], *, venue: str = "BINANCE") -> None:
    normalized_venue = _normalize_venue(venue)
    missing = [normalize_symbol(symbol) for symbol in symbols if not DEFAULT_INSTRUMENT_CATALOG.has(normalized_venue, symbol)]
    if not missing:
        return
    if normalized_venue != "BINANCE":
        raise KeyError(f"authoritative instrument loader not available for venue={normalized_venue}")
    for record in load_binance_instrument_records(symbols=missing):
        DEFAULT_INSTRUMENT_CATALOG.register_authoritative_record(record)


def resolve_instrument(symbol: str, *, venue: str = "BINANCE", catalog: InstrumentCatalog | None = None) -> Instrument:
    return active_instrument_catalog(fallback=catalog).resolve(venue, symbol)


def instrument_catalog_version(*, catalog: InstrumentCatalog | None = None) -> str:
    return active_instrument_catalog(fallback=catalog).version()


def instrument_snapshot(symbol: str, *, venue: str = "BINANCE", catalog: InstrumentCatalog | None = None) -> dict[str, str | int]:
    return active_instrument_catalog(fallback=catalog).instrument_snapshot(venue, symbol)


def instrument_metadata(symbol: str, *, venue: str = "BINANCE", catalog: InstrumentCatalog | None = None) -> dict[str, str]:
    resolved_catalog = active_instrument_catalog(fallback=catalog)
    snapshot = instrument_snapshot(symbol, venue=venue, catalog=resolved_catalog)
    metadata = resolve_instrument(symbol, venue=venue, catalog=resolved_catalog).as_metadata()
    metadata["instrument_catalog_version"] = resolved_catalog.version()
    metadata["instrument_snapshot"] = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return metadata
