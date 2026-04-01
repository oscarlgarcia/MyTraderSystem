"""
Buffered Parquet writer for normalized market data.

Normalized v2 layout:
<data_dir>/normalized/{trades|bars|books}/env=<env>/venue=<venue>/symbol=<symbol>/date=<YYYY-MM-DD>/data.parquet

Legacy v1 reader compatibility is kept for files under:
<data_dir>/<env>/symbol=<symbol>/date=<YYYY-MM-DD>/data.parquet
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

from app.common.dto import MarketEvent
from app.ingestion.dedup import identity_from_fields
from app.marketdata.models import BaseMarketEvent, IngestionEvent, TradeEvent, ensure_legacy_market_event
from app.marketdata.normalization import NORMALIZER_VERSION, resolve_normalizer_version

FEED_TYPE_BY_SOURCE = {
    "trade": "trades",
    "kline": "bars",
    "book": "books",
}
STREAM_TYPE_BY_FEED_TYPE = {value: key for key, value in FEED_TYPE_BY_SOURCE.items()}


def validate_output_path(base_dir: Path, *, require_absolute: bool = False) -> Path:
    resolved = Path(base_dir).expanduser()
    if require_absolute and not resolved.is_absolute():
        raise ValueError(f"data_dir must be absolute in production mode: {base_dir}")
    resolved = resolved.resolve()
    if resolved == Path(resolved.anchor):
        raise ValueError(f"data_dir cannot be filesystem root: {resolved}")
    if resolved.exists() and not resolved.is_dir():
        raise ValueError(f"data_dir must be a directory: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    probe = resolved / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        raise ValueError(f"data_dir is not writable: {resolved}") from exc
    return resolved


def feed_type_for_source(source: str) -> str:
    return FEED_TYPE_BY_SOURCE.get(source, source.lower())


def _metadata_mapping(value: object | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, list):
        out: dict[str, str] = {}
        for item in value:
            if isinstance(item, tuple) and len(item) == 2:
                out[str(item[0])] = str(item[1])
        return out
    return {}


def _optional_ts(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def event_metadata(event: IngestionEvent) -> dict[str, str]:
    return _metadata_mapping(getattr(event, "metadata", None))


def event_venue(event: IngestionEvent) -> str:
    if isinstance(event, BaseMarketEvent):
        return event.venue
    return str(event_metadata(event).get("venue", "BINANCE")).upper()


def event_normalizer_version(event: IngestionEvent) -> str:
    return resolve_normalizer_version(event_metadata(event).get("normalizer_version"))


def event_exchange_ts(event: IngestionEvent) -> datetime:
    if isinstance(event, BaseMarketEvent):
        return event.exchange_ts
    return event.event_ts


def event_receive_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BaseMarketEvent):
        return event.receive_ts
    return _optional_ts(event_metadata(event).get("receive_ts"))


def event_process_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BaseMarketEvent):
        return event.process_ts
    return _optional_ts(event_metadata(event).get("process_ts"))


def event_source_id(event: IngestionEvent) -> str | None:
    if isinstance(event, BaseMarketEvent):
        return event.source_id
    metadata = event_metadata(event)
    return metadata.get("source_id")


def event_trade_id(event: IngestionEvent) -> str | None:
    if isinstance(event, TradeEvent):
        return event.trade_id
    return event_metadata(event).get("trade_id")


def event_trade_side(event: IngestionEvent) -> str | None:
    if isinstance(event, TradeEvent):
        return event.side
    return event_metadata(event).get("side")


@dataclass(frozen=True, slots=True)
class TradeParquetWriter:
    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                ("venue", pa.string()),
                ("feed_type", pa.string()),
                ("normalizer_version", pa.string()),
                ("symbol", pa.string()),
                ("exchange_ts", pa.timestamp("ms", tz="UTC")),
                ("receive_ts", pa.timestamp("ms", tz="UTC")),
                ("process_ts", pa.timestamp("ms", tz="UTC")),
                ("event_ts", pa.timestamp("ms", tz="UTC")),
                ("price", pa.float64()),
                ("size", pa.float64()),
                ("source", pa.string()),
                ("source_id", pa.string()),
                ("trade_id", pa.string()),
                ("side", pa.string()),
                ("metadata", pa.map_(pa.string(), pa.string())),
            ]
        )

    @classmethod
    def to_table(cls, events: list[IngestionEvent]) -> pa.Table:
        events_sorted = sorted(events, key=lambda ev: event_exchange_ts(ev))
        rows = []
        for event in events_sorted:
            metadata = event_metadata(event)
            metadata.setdefault("venue", event_venue(event))
            metadata.setdefault("normalizer_version", event_normalizer_version(event))
            if event_receive_ts(event) is not None:
                metadata.setdefault("receive_ts", event_receive_ts(event).isoformat())
            if event_process_ts(event) is not None:
                metadata.setdefault("process_ts", event_process_ts(event).isoformat())
            if event_source_id(event) is not None:
                metadata.setdefault("source_id", str(event_source_id(event)))
            if event_trade_id(event) is not None:
                metadata.setdefault("trade_id", str(event_trade_id(event)))
            if event_trade_side(event) is not None:
                metadata.setdefault("side", str(event_trade_side(event)))
            rows.append(
                {
                    "venue": event_venue(event),
                    "feed_type": feed_type_for_source(event.source),
                    "normalizer_version": event_normalizer_version(event),
                    "symbol": event.symbol,
                    "exchange_ts": event_exchange_ts(event),
                    "receive_ts": event_receive_ts(event),
                    "process_ts": event_process_ts(event),
                    "event_ts": event.event_ts,
                    "price": event.price,
                    "size": event.size,
                    "source": event.source,
                    "source_id": event_source_id(event),
                    "trade_id": event_trade_id(event),
                    "side": event_trade_side(event),
                    "metadata": metadata,
                }
            )
        return pa.Table.from_pylist(rows, schema=cls.schema())


def normalized_partition_path(
    base_dir: Path,
    env: str,
    *,
    source: str,
    symbol: str,
    day: str,
    venue: str = "BINANCE",
) -> Path:
    return (
        base_dir
        / "normalized"
        / feed_type_for_source(source)
        / f"env={env}"
        / f"venue={str(venue).upper()}"
        / f"symbol={symbol}"
        / f"date={day}"
        / "data.parquet"
    )


def legacy_partition_path(base_dir: Path, env: str, symbol: str, day: str) -> Path:
    return base_dir / env / f"symbol={symbol}" / f"date={day}" / "data.parquet"


def list_normalized_parquet_files(base_dir: Path, env: str, *, include_legacy: bool = True) -> list[Path]:
    files = sorted(base_dir.glob(f"normalized/*/env={env}/venue=*/symbol=*/date=*/data.parquet"))
    if files or not include_legacy:
        return files
    return sorted(base_dir.glob(f"{env}/symbol=*/date=*/data.parquet"))


@dataclass
class ParquetWriter:
    base_dir: Path
    env: str
    flush_size: int = 500
    max_dedup_rows: int = 200_000
    schema_version: str = "v2"
    dedup: bool = False
    buffer: List[IngestionEvent] = field(default_factory=list)
    accepted_events: int = 0
    persisted_events: int = 0
    flush_count: int = 0
    total_write_latency_seconds: float = 0.0
    last_write_latency_seconds: float = 0.0
    max_write_latency_seconds: float = 0.0
    stream_write_metrics: dict[str, dict[str, object]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.base_dir = validate_output_path(self.base_dir)

    def add(self, event: IngestionEvent | Iterable[IngestionEvent]) -> None:
        if isinstance(event, (MarketEvent, BaseMarketEvent)):
            self.buffer.append(event)
            self.accepted_events += 1
        else:
            batch = list(event)
            self.buffer.extend(batch)
            self.accepted_events += len(batch)
        if len(self.buffer) >= self.flush_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        started = time.perf_counter()
        grouped = list(_group_events_by_partition(self.buffer).items())
        remaining: list[IngestionEvent] = []
        persisted_now = 0
        try:
            for index, (partition_key, events) in enumerate(grouped):
                try:
                    partition_started = time.perf_counter()
                    _write_partition(
                        base_dir=self.base_dir,
                        env=self.env,
                        partition_key=partition_key,
                        events=events,
                        schema_version=self.schema_version,
                        dedup=self.dedup,
                        max_dedup_rows=self.max_dedup_rows,
                    )
                    self._record_stream_write_metric(partition_key, time.perf_counter() - partition_started)
                    persisted_now += len(events)
                except Exception:
                    remaining.extend(events)
                    for _, later_events in grouped[index + 1 :]:
                        remaining.extend(later_events)
                    self.buffer = remaining
                    raise
            self.buffer.clear()
            self.persisted_events += persisted_now
            self.flush_count += 1
        finally:
            duration = max(0.0, time.perf_counter() - started)
            self.last_write_latency_seconds = duration
            self.total_write_latency_seconds += duration
            if duration > self.max_write_latency_seconds:
                self.max_write_latency_seconds = duration

    @property
    def buffered_events(self) -> int:
        return len(self.buffer)

    def _record_stream_write_metric(self, partition_key: tuple[str, str, str, str], duration: float) -> None:
        feed_type, venue, symbol, _day = partition_key
        stream_type = STREAM_TYPE_BY_FEED_TYPE.get(feed_type, feed_type)
        label = f"{venue}:{symbol}:{stream_type}"
        metric = self.stream_write_metrics.setdefault(
            label,
            {
                "venue": venue,
                "symbol": symbol,
                "stream_type": stream_type,
                "normalized_write_latency": 0.0,
            },
        )
        metric["normalized_write_latency"] = max(float(metric["normalized_write_latency"]), max(0.0, duration))


def _group_events_by_partition(events: List[IngestionEvent]) -> dict[tuple[str, str, str, str], list[IngestionEvent]]:
    grouped: dict[tuple[str, str, str, str], list[IngestionEvent]] = {}
    for ev in events:
        day = ev.event_ts.date().isoformat()
        key = (feed_type_for_source(ev.source), event_venue(ev), ev.symbol, day)
        grouped.setdefault(key, []).append(ev)
    return grouped


def _to_table(events: List[IngestionEvent]) -> pa.Table:
    events_sorted = sorted(events, key=lambda ev: ev.event_ts)
    data = {
        "venue": [event_venue(ev) for ev in events_sorted],
        "feed_type": [feed_type_for_source(ev.source) for ev in events_sorted],
        "normalizer_version": [event_normalizer_version(ev) for ev in events_sorted],
        "symbol": [ev.symbol for ev in events_sorted],
        "event_ts": [pa.scalar(ev.event_ts).as_py() for ev in events_sorted],
        "price": [ev.price for ev in events_sorted],
        "size": [ev.size for ev in events_sorted],
        "source": [ev.source for ev in events_sorted],
        "metadata": [event_metadata(ev) for ev in events_sorted],
    }
    schema = pa.schema(
        [
            ("venue", pa.string()),
            ("feed_type", pa.string()),
            ("normalizer_version", pa.string()),
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    return pa.Table.from_pydict(data, schema=schema)


def _to_legacy_table(events: List[IngestionEvent]) -> pa.Table:
    events_sorted = sorted(events, key=lambda ev: ev.event_ts)
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    rows = []
    for ev in events_sorted:
        legacy_event = ensure_legacy_market_event(ev)
        metadata = dict(legacy_event.metadata)
        metadata.setdefault("venue", event_venue(ev))
        metadata.setdefault("normalizer_version", event_normalizer_version(ev))
        rows.append(
            {
                "symbol": legacy_event.symbol,
                "event_ts": legacy_event.event_ts,
                "price": legacy_event.price,
                "size": legacy_event.size,
                "source": legacy_event.source,
                "metadata": metadata,
            }
        )
    return pa.Table.from_pylist(rows, schema=schema)


def _write_partition(
    *,
    base_dir: Path,
    env: str,
    partition_key: tuple[str, str, str, str],
    events: list[IngestionEvent],
    schema_version: str,
    dedup: bool,
    max_dedup_rows: int,
) -> None:
    feed_type, venue, symbol, day = partition_key
    if schema_version == "v1":
        out_path = legacy_partition_path(base_dir, env, symbol, day)
        table = _to_legacy_table(events)
    else:
        out_path = (
            base_dir
            / "normalized"
            / feed_type
            / f"env={env}"
            / f"venue={venue}"
            / f"symbol={symbol}"
            / f"date={day}"
            / "data.parquet"
        )
        table = TradeParquetWriter.to_table(events) if feed_type == "trades" else _to_table(events)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = table.replace_schema_metadata(
        {
            b"schema_version": schema_version.encode("utf-8"),
            b"normalizer_version": NORMALIZER_VERSION.encode("utf-8"),
        }
    )
    fallback_legacy = legacy_partition_path(base_dir, env, symbol, day)
    if schema_version == "v1":
        existing_path = out_path
    else:
        existing_path = out_path if out_path.exists() else fallback_legacy if fallback_legacy.exists() else out_path
    merged = _merge_with_existing(existing_path, table, dedup=dedup, max_dedup_rows=max_dedup_rows)
    _write_table_atomic(merged, out_path)


def _merge_with_existing(out_path: Path, new_table: pa.Table, *, dedup: bool, max_dedup_rows: int) -> pa.Table:
    if not out_path.exists():
        return new_table
    existing = _read_existing_table_for_merge(out_path, new_table.schema)
    total_rows = existing.num_rows + new_table.num_rows
    if dedup:
        if total_rows <= max_dedup_rows:
            return _dedup_tables(existing, new_table)
        keys_existing = _key_set_from_table(existing)
        filtered_new = _filter_new_rows(new_table, keys_existing)
        return _concat_tables_ordered(existing, filtered_new)
    return _concat_tables_ordered(existing, new_table)


def _concat_tables_ordered(existing: pa.Table, new_table: pa.Table) -> pa.Table:
    if existing.num_rows == 0:
        return new_table
    if new_table.num_rows == 0:
        return existing
    last_ts = existing.column("event_ts")[-1].as_py()
    first_new_ts = new_table.column("event_ts")[0].as_py()
    combined = pa.concat_tables([existing, new_table], promote_options="default")
    if last_ts is not None and first_new_ts is not None and last_ts > first_new_ts:
        return combined.sort_by([("event_ts", "ascending")])
    return combined


def _write_table_atomic(table: pa.Table, out_path: Path) -> None:
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    try:
        pq.write_table(table, tmp_path, use_dictionary=False)
        tmp_path.replace(out_path)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


def read_parquet(path: Path) -> pa.Table:
    pf = pq.ParquetFile(path)
    table = pf.read()
    metadata = table.schema.metadata or {}
    version = metadata.get(b"schema_version")
    if version is None:
        raise ValueError("schema_version missing in parquet metadata")
    decoded = version.decode("utf-8")
    if decoded not in {"v1", "v2"}:
        raise ValueError(f"unsupported schema_version: {decoded}")
    if decoded == "v2":
        normalizer_version = metadata.get(b"normalizer_version")
        resolved = resolve_normalizer_version(
            normalizer_version.decode("utf-8") if normalizer_version is not None else None
        )
        if "normalizer_version" not in table.column_names:
            rows = []
            for row in table.to_pylist():
                row["normalizer_version"] = resolved
                rows.append(row)
            table = pa.Table.from_pylist(rows)
        table = table.replace_schema_metadata(
            {
                **(table.schema.metadata or {}),
                b"schema_version": b"v2",
                b"normalizer_version": resolved.encode("utf-8"),
            }
        )
    return table


def _is_trade_schema(schema: pa.Schema) -> bool:
    names = set(schema.names)
    return {"trade_id", "exchange_ts", "receive_ts", "process_ts", "source_id", "side"}.issubset(names)


def _identity_metadata_from_row(row: dict[str, object]) -> dict[str, object]:
    metadata = dict(_metadata_mapping(row.get("metadata")))
    for key in ("trade_id", "sequence_id", "source_id"):
        value = row.get(key)
        if value not in (None, ""):
            metadata[key] = str(value)
    venue = row.get("venue")
    if venue not in (None, ""):
        metadata.setdefault("venue", str(venue))
    return metadata


def _trade_row_from_existing(row: dict[str, object]) -> dict[str, object]:
    metadata = _identity_metadata_from_row(row)
    source = str(row.get("source", "trade"))
    exchange_ts = row.get("exchange_ts") or row.get("event_ts")
    return {
        "venue": str(row.get("venue") or metadata.get("venue", "BINANCE")).upper(),
        "feed_type": row.get("feed_type") or feed_type_for_source(source),
        "normalizer_version": resolve_normalizer_version(
            row.get("normalizer_version") or metadata.get("normalizer_version")
        ),
        "symbol": row["symbol"],
        "exchange_ts": exchange_ts,
        "receive_ts": row.get("receive_ts") or _optional_ts(metadata.get("receive_ts")),
        "process_ts": row.get("process_ts") or _optional_ts(metadata.get("process_ts")),
        "event_ts": row.get("event_ts") or exchange_ts,
        "price": row["price"],
        "size": row["size"],
        "source": source,
        "source_id": row.get("source_id") or metadata.get("source_id"),
        "trade_id": row.get("trade_id") or metadata.get("trade_id"),
        "side": row.get("side") or metadata.get("side"),
        "metadata": _metadata_mapping(metadata),
    }


def _row_identity(row: dict[str, object]) -> tuple:
    return identity_from_fields(
        symbol=row["symbol"],
        event_ts=row.get("event_ts") or row.get("exchange_ts"),
        price=row["price"],
        size=row["size"],
        source=row["source"],
        venue=row.get("venue"),
        metadata=_identity_metadata_from_row(row),
        source_id=str(row.get("source_id")) if row.get("source_id") not in (None, "") else None,
    )


def _read_existing_table_for_merge(out_path: Path, target_schema: pa.Schema) -> pa.Table:
    table = pq.ParquetFile(out_path).read()
    metadata = table.schema.metadata or {}
    version = metadata.get(b"schema_version", b"v1").decode("utf-8")
    target_columns = set(target_schema.names)
    if _is_trade_schema(target_schema):
        rows = [_trade_row_from_existing(row) for row in table.to_pylist()]
        return pa.Table.from_pylist(
            [{key: value for key, value in row.items() if key in target_columns} for row in rows],
            schema=target_schema,
        )
    if version == "v2":
        rows = []
        for row in table.to_pylist():
            metadata_row = row.get("metadata") or {}
            adapted = {
                "venue": row.get("venue") or str(metadata_row.get("venue", "BINANCE")).upper(),
                "feed_type": row.get("feed_type") or feed_type_for_source(row["source"]),
                "normalizer_version": resolve_normalizer_version(
                    row.get("normalizer_version") or metadata_row.get("normalizer_version")
                ),
                "symbol": row["symbol"],
                "event_ts": row["event_ts"],
                "price": row["price"],
                "size": row["size"],
                "source": row["source"],
                "metadata": metadata_row,
            }
            rows.append({key: value for key, value in adapted.items() if key in target_columns})
        return pa.Table.from_pylist(rows, schema=target_schema)

    rows = []
    for row in table.to_pylist():
        metadata = row.get("metadata") or {}
        adapted = {
            "venue": str(metadata.get("venue", "BINANCE")).upper(),
            "feed_type": feed_type_for_source(row["source"]),
            "normalizer_version": resolve_normalizer_version(metadata.get("normalizer_version")),
            "symbol": row["symbol"],
            "event_ts": row["event_ts"],
            "price": row["price"],
            "size": row["size"],
            "source": row["source"],
            "metadata": metadata,
        }
        rows.append({key: value for key, value in adapted.items() if key in target_columns})
    return pa.Table.from_pylist(rows, schema=target_schema)


def _dedup_tables(existing: pa.Table, new: pa.Table) -> pa.Table:
    schema = existing.schema
    merged: list[dict] = []
    seen = set()

    def add_rows(rows: list[dict]) -> None:
        for row in rows:
            key = _row_identity(row)
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

    add_rows(existing.to_pylist())
    add_rows(new.to_pylist())
    merged.sort(key=lambda r: r["event_ts"])
    return pa.Table.from_pylist(merged, schema=schema)


def _key_set_from_table(tbl: pa.Table) -> set[tuple]:
    keys = set()
    required_cols = ["symbol", "price", "size", "source"]
    optional_cols = ["event_ts", "exchange_ts", "venue", "metadata", "source_id", "trade_id", "sequence_id"]
    cols = [name for name in required_cols + optional_cols if name in tbl.column_names]
    for row in tbl.select(cols).to_pylist():
        keys.add(_row_identity(row))
    return keys


def _filter_new_rows(tbl: pa.Table, existing_keys: set[tuple]) -> pa.Table:
    schema = tbl.schema
    filtered = []
    for row in tbl.to_pylist():
        key = _row_identity(row)
        if key in existing_keys:
            continue
        filtered.append(row)
    sort_key = "event_ts" if "event_ts" in schema.names else "exchange_ts"
    if not filtered:
        return pa.Table.from_pylist([], schema=schema)
    filtered.sort(key=lambda r: r[sort_key])
    return pa.Table.from_pylist(filtered, schema=schema)
