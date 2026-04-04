"""
Buffered Parquet writer for normalized market data.

Normalized v2 layout:
<data_dir>/normalized/{trades|bars}/env=<env>/venue=<venue>/symbol=<symbol>/date=<YYYY-MM-DD>/
  - segments/segment-*.parquet   (online write path)
  - data.parquet                 (offline compacted snapshot, optional)

Legacy v1 reader compatibility is kept for files under:
<data_dir>/<env>/symbol=<symbol>/date=<YYYY-MM-DD>/data.parquet
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from app.common.dto import MarketEvent
from app.ingestion.dedup import identity_from_fields
from app.marketdata.models import (
    BarEvent,
    BaseMarketEvent,
    IngestionEvent,
    TradeEvent,
    ensure_legacy_market_event,
    is_supported_marketdata_source,
)
from app.marketdata.instruments import instrument_metadata
from app.marketdata.normalization import NORMALIZER_VERSION, resolve_normalizer_version

FEED_TYPE_BY_SOURCE = {
    "trade": "trades",
    "kline": "bars",
}
STREAM_TYPE_BY_FEED_TYPE = {value: key for key, value in FEED_TYPE_BY_SOURCE.items()}
PARTITION_DATA_FILENAME = "data.parquet"
PARTITION_SEGMENTS_DIRNAME = "segments"
PARTITION_COMPACTION_FAILURE_FILENAME = "compaction-failures.jsonl"


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


def _assert_supported_storage_source(source: str) -> None:
    normalized = str(source).lower()
    if not is_supported_marketdata_source(normalized):
        raise ValueError(
            f"{normalized} feed is out of scope for normalized storage; only trade and kline are supported"
        )


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
    metadata = _metadata_mapping(getattr(event, "metadata", None))
    if "instrument_catalog_version" not in metadata and is_supported_marketdata_source(getattr(event, "source", "")):
        try:
            venue = getattr(event, "venue", metadata.get("venue", "BINANCE"))
            metadata.update(
                {
                    key: value
                    for key, value in instrument_metadata(event.symbol, venue=str(venue)).items()
                    if key not in metadata
                }
            )
        except KeyError:
            pass
    return metadata


def _event_metadata_for_row(
    event: IngestionEvent,
    *,
    metadata_cache: dict[tuple[str, str], dict[str, str]] | None = None,
) -> dict[str, str]:
    metadata = _metadata_mapping(getattr(event, "metadata", None))
    if "instrument_catalog_version" in metadata or not is_supported_marketdata_source(getattr(event, "source", "")):
        return metadata
    venue = str(getattr(event, "venue", metadata.get("venue", "BINANCE"))).upper()
    cache_key = (event.symbol, venue)
    cached = metadata_cache.get(cache_key) if metadata_cache is not None else None
    if cached is None:
        try:
            cached = instrument_metadata(event.symbol, venue=venue)
        except KeyError:
            cached = {}
        if metadata_cache is not None:
            metadata_cache[cache_key] = dict(cached)
    if cached:
        metadata.update({key: value for key, value in cached.items() if key not in metadata})
    return metadata


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


def event_provider_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BaseMarketEvent):
        return event.provider_ts
    return _optional_ts(event_metadata(event).get("provider_ts"))


def event_raw_run_id(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("raw_run_id")


def event_raw_ingestion_seq(event: IngestionEvent) -> int | None:
    value = event_metadata(event).get("raw_ingestion_seq")
    if value in (None, ""):
        return None
    return int(value)


def event_historical_feed_kind(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("historical_feed_kind")


def event_source_id(event: IngestionEvent) -> str | None:
    if isinstance(event, BaseMarketEvent):
        return event.source_id
    metadata = event_metadata(event)
    return metadata.get("source_id")


def event_instrument_catalog_version(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("instrument_catalog_version")


def event_instrument_snapshot(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("instrument_snapshot")


def event_metadata_source(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("metadata_source")


def event_venue_snapshot_version(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("venue_snapshot_version")


def event_metadata_snapshot_mode(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("metadata_snapshot_mode")


def event_venue_snapshot_path(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("venue_snapshot_path")


def event_instrument_catalog_snapshot_json(event: IngestionEvent) -> str | None:
    return event_metadata(event).get("instrument_catalog_snapshot_json")


def event_trade_id(event: IngestionEvent) -> str | None:
    if isinstance(event, TradeEvent):
        return event.trade_id
    return event_metadata(event).get("trade_id")


def event_trade_side(event: IngestionEvent) -> str | None:
    if isinstance(event, TradeEvent):
        return event.side
    return event_metadata(event).get("side")


def event_bar_open(event: IngestionEvent) -> float:
    if isinstance(event, BarEvent):
        return event.open
    metadata = event_metadata(event)
    return float(metadata.get("open", event.price))


def event_bar_high(event: IngestionEvent) -> float:
    if isinstance(event, BarEvent):
        return event.high
    metadata = event_metadata(event)
    return float(metadata.get("high", event.price))


def event_bar_low(event: IngestionEvent) -> float:
    if isinstance(event, BarEvent):
        return event.low
    metadata = event_metadata(event)
    return float(metadata.get("low", event.price))


def event_bar_close(event: IngestionEvent) -> float:
    if isinstance(event, BarEvent):
        return event.close
    return event.price


def event_bar_volume(event: IngestionEvent) -> float:
    if isinstance(event, BarEvent):
        return event.volume
    return event.size


def event_bar_interval(event: IngestionEvent) -> str:
    if isinstance(event, BarEvent):
        return event.interval
    return event_metadata(event).get("interval", "1m")


def event_bar_open_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BarEvent):
        return event.open_ts
    return _optional_ts(event_metadata(event).get("open_ts"))


def event_bar_close_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BarEvent):
        return event.close_ts
    metadata = event_metadata(event)
    return _optional_ts(metadata.get("close_ts")) or event.event_ts


def _coerce_optional_int(value: object | None) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _event_storage_sort_key(event: IngestionEvent) -> tuple[object, ...]:
    return (
        event_exchange_ts(event),
        event_raw_run_id(event) or "",
        event_raw_ingestion_seq(event) if event_raw_ingestion_seq(event) is not None else -1,
        event_source_id(event) or "",
        event_trade_id(event) or "",
        event.symbol,
        event.source,
    )


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
                ("provider_ts", pa.timestamp("ms", tz="UTC")),
                ("receive_ts", pa.timestamp("ms", tz="UTC")),
                ("process_ts", pa.timestamp("ms", tz="UTC")),
                ("event_ts", pa.timestamp("ms", tz="UTC")),
                ("price", pa.float64()),
                ("size", pa.float64()),
                ("source", pa.string()),
                ("source_id", pa.string()),
                ("trade_id", pa.string()),
                ("side", pa.string()),
                ("historical_feed_kind", pa.string()),
                ("raw_run_id", pa.string()),
                ("raw_ingestion_seq", pa.int64()),
                ("metadata", pa.map_(pa.string(), pa.string())),
            ]
        )

    @classmethod
    def to_table(cls, events: list[IngestionEvent]) -> pa.Table:
        events_sorted = sorted(events, key=_event_storage_sort_key)
        metadata_cache: dict[tuple[str, str], dict[str, str]] = {}
        rows = []
        for event in events_sorted:
            metadata = _event_metadata_for_row(event, metadata_cache=metadata_cache)
            venue = event.venue if isinstance(event, BaseMarketEvent) else str(metadata.get("venue", "BINANCE")).upper()
            normalizer_version = (
                event_normalizer_version(event)
                if isinstance(event, BaseMarketEvent)
                else resolve_normalizer_version(metadata.get("normalizer_version"))
            )
            exchange_ts = event.exchange_ts if isinstance(event, BaseMarketEvent) else event.event_ts
            provider_ts = event.provider_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("provider_ts"))
            receive_ts = event.receive_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("receive_ts"))
            process_ts = event.process_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("process_ts"))
            source_id = event.source_id if isinstance(event, BaseMarketEvent) else metadata.get("source_id")
            trade_id = event.trade_id if isinstance(event, TradeEvent) else metadata.get("trade_id")
            side = event.side if isinstance(event, TradeEvent) else metadata.get("side")
            historical_feed_kind = metadata.get("historical_feed_kind")
            raw_run_id = metadata.get("raw_run_id")
            raw_ingestion_seq = _coerce_optional_int(metadata.get("raw_ingestion_seq"))

            metadata.setdefault("venue", venue)
            metadata.setdefault("normalizer_version", normalizer_version)
            if provider_ts is not None:
                metadata.setdefault("provider_ts", provider_ts.isoformat())
            if receive_ts is not None:
                metadata.setdefault("receive_ts", receive_ts.isoformat())
            if process_ts is not None:
                metadata.setdefault("process_ts", process_ts.isoformat())
            if source_id is not None:
                metadata.setdefault("source_id", str(source_id))
            if trade_id is not None:
                metadata.setdefault("trade_id", str(trade_id))
            if side is not None:
                metadata.setdefault("side", str(side))
            if historical_feed_kind is not None:
                metadata.setdefault("historical_feed_kind", str(historical_feed_kind))
            if raw_run_id is not None:
                metadata.setdefault("raw_run_id", str(raw_run_id))
            if raw_ingestion_seq is not None:
                metadata.setdefault("raw_ingestion_seq", str(raw_ingestion_seq))
            rows.append(
                {
                    "venue": venue,
                    "feed_type": feed_type_for_source(event.source),
                    "normalizer_version": normalizer_version,
                    "symbol": event.symbol,
                    "exchange_ts": exchange_ts,
                    "provider_ts": provider_ts,
                    "receive_ts": receive_ts,
                    "process_ts": process_ts,
                    "event_ts": event.event_ts,
                    "price": event.price,
                    "size": event.size,
                    "source": event.source,
                    "source_id": source_id,
                    "trade_id": trade_id,
                    "side": side,
                    "historical_feed_kind": historical_feed_kind,
                    "raw_run_id": raw_run_id,
                    "raw_ingestion_seq": raw_ingestion_seq,
                    "metadata": metadata,
                }
            )
        return pa.Table.from_pylist(rows, schema=cls.schema())


@dataclass(frozen=True, slots=True)
class BarParquetWriter:
    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                ("venue", pa.string()),
                ("feed_type", pa.string()),
                ("normalizer_version", pa.string()),
                ("symbol", pa.string()),
                ("exchange_ts", pa.timestamp("ms", tz="UTC")),
                ("provider_ts", pa.timestamp("ms", tz="UTC")),
                ("receive_ts", pa.timestamp("ms", tz="UTC")),
                ("process_ts", pa.timestamp("ms", tz="UTC")),
                ("event_ts", pa.timestamp("ms", tz="UTC")),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("volume", pa.float64()),
                ("interval", pa.string()),
                ("open_ts", pa.timestamp("ms", tz="UTC")),
                ("close_ts", pa.timestamp("ms", tz="UTC")),
                ("source", pa.string()),
                ("source_id", pa.string()),
                ("raw_run_id", pa.string()),
                ("raw_ingestion_seq", pa.int64()),
                ("metadata", pa.map_(pa.string(), pa.string())),
            ]
        )

    @classmethod
    def to_table(cls, events: list[IngestionEvent]) -> pa.Table:
        events_sorted = sorted(events, key=_event_storage_sort_key)
        metadata_cache: dict[tuple[str, str], dict[str, str]] = {}
        rows = []
        for event in events_sorted:
            metadata = _event_metadata_for_row(event, metadata_cache=metadata_cache)
            venue = event.venue if isinstance(event, BaseMarketEvent) else str(metadata.get("venue", "BINANCE")).upper()
            normalizer_version = (
                event_normalizer_version(event)
                if isinstance(event, BaseMarketEvent)
                else resolve_normalizer_version(metadata.get("normalizer_version"))
            )
            exchange_ts = event.exchange_ts if isinstance(event, BaseMarketEvent) else event.event_ts
            provider_ts = event.provider_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("provider_ts"))
            receive_ts = event.receive_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("receive_ts"))
            process_ts = event.process_ts if isinstance(event, BaseMarketEvent) else _optional_ts(metadata.get("process_ts"))
            source_id = event.source_id if isinstance(event, BaseMarketEvent) else metadata.get("source_id")
            raw_run_id = metadata.get("raw_run_id")
            raw_ingestion_seq = _coerce_optional_int(metadata.get("raw_ingestion_seq"))
            interval = event.interval if isinstance(event, BarEvent) else metadata.get("interval", "1m")
            open_price = event.open if isinstance(event, BarEvent) else float(metadata.get("open", event.price))
            high_price = event.high if isinstance(event, BarEvent) else float(metadata.get("high", event.price))
            low_price = event.low if isinstance(event, BarEvent) else float(metadata.get("low", event.price))
            close_price = event.close if isinstance(event, BarEvent) else event.price
            volume = event.volume if isinstance(event, BarEvent) else event.size
            open_ts = event.open_ts if isinstance(event, BarEvent) else _optional_ts(metadata.get("open_ts"))
            close_ts = (
                event.close_ts if isinstance(event, BarEvent) else (_optional_ts(metadata.get("close_ts")) or event.event_ts)
            )

            metadata.setdefault("venue", venue)
            metadata.setdefault("normalizer_version", normalizer_version)
            metadata.setdefault("interval", interval)
            metadata.setdefault("open", str(open_price))
            metadata.setdefault("high", str(high_price))
            metadata.setdefault("low", str(low_price))
            if provider_ts is not None:
                metadata.setdefault("provider_ts", provider_ts.isoformat())
            if receive_ts is not None:
                metadata.setdefault("receive_ts", receive_ts.isoformat())
            if process_ts is not None:
                metadata.setdefault("process_ts", process_ts.isoformat())
            if open_ts is not None:
                metadata.setdefault("open_ts", open_ts.isoformat())
            if close_ts is not None:
                metadata.setdefault("close_ts", close_ts.isoformat())
            if source_id is not None:
                metadata.setdefault("source_id", str(source_id))
            if raw_run_id is not None:
                metadata.setdefault("raw_run_id", str(raw_run_id))
            if raw_ingestion_seq is not None:
                metadata.setdefault("raw_ingestion_seq", str(raw_ingestion_seq))
            rows.append(
                {
                    "venue": venue,
                    "feed_type": feed_type_for_source(event.source),
                    "normalizer_version": normalizer_version,
                    "symbol": event.symbol,
                    "exchange_ts": exchange_ts,
                    "provider_ts": provider_ts,
                    "receive_ts": receive_ts,
                    "process_ts": process_ts,
                    "event_ts": event.event_ts,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "interval": interval,
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "source": event.source,
                    "source_id": source_id,
                    "raw_run_id": raw_run_id,
                    "raw_ingestion_seq": raw_ingestion_seq,
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
    _assert_supported_storage_source(source)
    return (
        base_dir
        / "normalized"
        / feed_type_for_source(source)
        / f"env={env}"
        / f"venue={str(venue).upper()}"
        / f"symbol={symbol}"
        / f"date={day}"
    )


def normalized_partition_data_path(
    base_dir: Path,
    env: str,
    *,
    source: str,
    symbol: str,
    day: str,
    venue: str = "BINANCE",
) -> Path:
    return normalized_partition_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    ) / PARTITION_DATA_FILENAME


def partition_segments_dir(partition_path: Path) -> Path:
    return partition_path / PARTITION_SEGMENTS_DIRNAME


def partition_compaction_failure_path(partition_path: Path) -> Path:
    return partition_path / PARTITION_COMPACTION_FAILURE_FILENAME


def legacy_partition_path(base_dir: Path, env: str, symbol: str, day: str) -> Path:
    return base_dir / env / f"symbol={symbol}" / f"date={day}" / "data.parquet"


def list_normalized_parquet_files(base_dir: Path, env: str, *, include_legacy: bool = True) -> list[Path]:
    partitions = sorted(base_dir.glob(f"normalized/*/env={env}/venue=*/symbol=*/date=*"))
    if partitions or not include_legacy:
        return partitions
    return sorted(base_dir.glob(f"{env}/symbol=*/date=*/data.parquet"))


def list_normalized_partition_paths(base_dir: Path, env: str) -> list[Path]:
    return sorted(base_dir.glob(f"normalized/*/env={env}/venue=*/symbol=*/date=*"))


def record_compaction_failure(partition_path: Path, error: Exception | str, *, occurred_at: datetime | None = None) -> Path:
    path = partition_compaction_failure_path(partition_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": (occurred_at or datetime.now(timezone.utc)).isoformat(),
        "error": str(error),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")
    return path


@dataclass
class ParquetWriter:
    base_dir: Path
    env: str
    flush_size: int = 500
    partition_flush_size: int | None = None
    max_dedup_rows: int = 200_000
    schema_version: str = "v2"
    dedup: bool = False
    max_parallel_partition_writes: int | None = None
    buffer: List[IngestionEvent] = field(default_factory=list)
    partition_buffers: dict[tuple[str, str, str, str], list[IngestionEvent]] = field(default_factory=dict)
    accepted_events: int = 0
    persisted_events: int = 0
    flush_count: int = 0
    total_write_latency_seconds: float = 0.0
    last_write_latency_seconds: float = 0.0
    max_write_latency_seconds: float = 0.0
    stream_write_metrics: dict[str, dict[str, object]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.base_dir = validate_output_path(self.base_dir)
        self.flush_size = max(1, int(self.flush_size))
        configured_partition_flush_size = self.partition_flush_size if self.partition_flush_size is not None else self.flush_size
        self.partition_flush_size = max(1, int(configured_partition_flush_size))
        configured_parallelism = self.max_parallel_partition_writes if self.max_parallel_partition_writes is not None else min(8, max(2, os.cpu_count() or 2))
        self.max_parallel_partition_writes = max(1, int(configured_parallelism))

    def add(self, event: IngestionEvent | Iterable[IngestionEvent]) -> None:
        if isinstance(event, (MarketEvent, BaseMarketEvent)):
            self._buffer_event(event)
        else:
            batch = list(event)
            for item in batch:
                self._buffer_event(item)
        self._flush_ready_partitions()

    def flush(self) -> None:
        self._flush_partitions(tuple(self.partition_buffers))

    def _buffer_event(self, event: IngestionEvent) -> None:
        self.accepted_events += 1
        partition_key = _partition_key_for_event(event)
        self.partition_buffers.setdefault(partition_key, []).append(event)

    def _flush_ready_partitions(self) -> None:
        if not self.partition_buffers:
            return
        ready_partition_keys = [
            partition_key
            for partition_key, events in self.partition_buffers.items()
            if len(events) >= int(self.partition_flush_size or self.flush_size)
        ]
        if ready_partition_keys:
            self._flush_partitions(tuple(ready_partition_keys))
            return
        if self.buffered_events >= self.flush_size:
            self._flush_partitions(tuple(self.partition_buffers))

    def _flush_partitions(self, partition_keys: tuple[tuple[str, str, str, str], ...]) -> None:
        if not partition_keys:
            return
        started = time.perf_counter()
        persisted_now = 0
        max_partition_duration = 0.0
        failures: list[Exception] = []
        try:
            partitions_to_flush = [
                (partition_key, list(events))
                for partition_key in partition_keys
                if (events := self.partition_buffers.get(partition_key))
            ]
            if len(partitions_to_flush) <= 1 or int(self.max_parallel_partition_writes or 1) <= 1:
                for partition_key, events in partitions_to_flush:
                    partition_duration = _persist_partition_events(
                        base_dir=self.base_dir,
                        env=self.env,
                        partition_key=partition_key,
                        events=events,
                        schema_version=self.schema_version,
                        dedup=self.dedup,
                        max_dedup_rows=self.max_dedup_rows,
                    )
                    self._record_stream_write_metric(partition_key, partition_duration)
                    max_partition_duration = max(max_partition_duration, partition_duration)
                    persisted_now += len(events)
                    del self.partition_buffers[partition_key]
            else:
                completed_keys: list[tuple[str, str, str, str]] = []
                worker_count = min(len(partitions_to_flush), int(self.max_parallel_partition_writes or 1))
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    future_map = {
                        executor.submit(
                            _persist_partition_events,
                            base_dir=self.base_dir,
                            env=self.env,
                            partition_key=partition_key,
                            events=events,
                            schema_version=self.schema_version,
                            dedup=self.dedup,
                            max_dedup_rows=self.max_dedup_rows,
                        ): (partition_key, len(events))
                        for partition_key, events in partitions_to_flush
                    }
                    for future in as_completed(future_map):
                        partition_key, event_count = future_map[future]
                        try:
                            partition_duration = future.result()
                        except Exception as exc:
                            failures.append(exc)
                            continue
                        self._record_stream_write_metric(partition_key, partition_duration)
                        max_partition_duration = max(max_partition_duration, partition_duration)
                        persisted_now += event_count
                        completed_keys.append(partition_key)
                for partition_key in completed_keys:
                    self.partition_buffers.pop(partition_key, None)
                if failures:
                    raise failures[0]
            self.persisted_events += persisted_now
            if persisted_now > 0:
                self.flush_count += 1
        finally:
            self._sync_flat_buffer()
            total_duration = max(0.0, time.perf_counter() - started)
            self.last_write_latency_seconds = total_duration
            self.total_write_latency_seconds += total_duration
            observed_write_latency = max_partition_duration if max_partition_duration > 0.0 else total_duration
            if observed_write_latency > self.max_write_latency_seconds:
                self.max_write_latency_seconds = observed_write_latency

    @property
    def buffered_events(self) -> int:
        return sum(len(events) for events in self.partition_buffers.values())

    def _sync_flat_buffer(self) -> None:
        buffered_events = self.buffered_events
        if buffered_events == 0:
            self.buffer = []
            return
        if buffered_events <= 1024:
            self.buffer = [event for events in self.partition_buffers.values() for event in events]
            return
        self.buffer = []

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
        key = _partition_key_for_event(ev)
        grouped.setdefault(key, []).append(ev)
    return grouped


def _partition_key_for_event(event: IngestionEvent) -> tuple[str, str, str, str]:
    _assert_supported_storage_source(event.source)
    day = event.event_ts.date().isoformat()
    return (feed_type_for_source(event.source), event_venue(event), event.symbol, day)


def _persist_partition_events(
    *,
    base_dir: Path,
    env: str,
    partition_key: tuple[str, str, str, str],
    events: list[IngestionEvent],
    schema_version: str,
    dedup: bool,
    max_dedup_rows: int,
) -> float:
    started = time.perf_counter()
    _write_partition(
        base_dir=base_dir,
        env=env,
        partition_key=partition_key,
        events=events,
        schema_version=schema_version,
        dedup=dedup,
        max_dedup_rows=max_dedup_rows,
    )
    return max(0.0, time.perf_counter() - started)


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


def _table_schema_metadata(
    *,
    schema_version: str,
    events: list[IngestionEvent],
    dedup: bool,
) -> dict[bytes, bytes]:
    metadata = {
        b"schema_version": schema_version.encode("utf-8"),
        b"normalizer_version": NORMALIZER_VERSION.encode("utf-8"),
        b"dedup_policy": b"true" if dedup else b"false",
    }
    catalog_version = next(
        (value for value in (event_instrument_catalog_version(event) for event in events) if value),
        None,
    )
    instrument_snapshot = next(
        (value for value in (event_instrument_snapshot(event) for event in events) if value),
        None,
    )
    instrument_catalog_snapshot = next(
        (value for value in (event_instrument_catalog_snapshot_json(event) for event in events) if value),
        None,
    )
    metadata_source = next(
        (value for value in (event_metadata_source(event) for event in events) if value),
        None,
    )
    venue_snapshot_version = next(
        (value for value in (event_venue_snapshot_version(event) for event in events) if value),
        None,
    )
    metadata_snapshot_mode = next(
        (value for value in (event_metadata_snapshot_mode(event) for event in events) if value),
        None,
    )
    venue_snapshot_path = next(
        (value for value in (event_venue_snapshot_path(event) for event in events) if value),
        None,
    )
    if catalog_version:
        metadata[b"instrument_catalog_version"] = catalog_version.encode("utf-8")
        metadata[b"instrument_catalog_snapshot_hash"] = catalog_version.encode("utf-8")
    if instrument_snapshot:
        metadata[b"instrument_snapshot"] = instrument_snapshot.encode("utf-8")
    if metadata_source:
        metadata[b"instrument_metadata_source"] = metadata_source.encode("utf-8")
    if venue_snapshot_version:
        metadata[b"venue_snapshot_version"] = venue_snapshot_version.encode("utf-8")
    if metadata_snapshot_mode:
        metadata[b"metadata_snapshot_mode"] = metadata_snapshot_mode.encode("utf-8")
    if venue_snapshot_path:
        metadata[b"venue_snapshot_path"] = venue_snapshot_path.encode("utf-8")
    return metadata


def _write_segment_atomic(table: pa.Table, partition_dir: Path) -> Path:
    segments_dir = partition_segments_dir(partition_dir)
    segments_dir.mkdir(parents=True, exist_ok=True)
    segment_path = segments_dir / f"segment-{time.time_ns()}-{uuid4().hex[:8]}.parquet"
    _write_table_atomic(table, segment_path)
    return segment_path


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
        out_path.parent.mkdir(parents=True, exist_ok=True)
        table = table.replace_schema_metadata(_table_schema_metadata(schema_version=schema_version, events=events, dedup=dedup))
        existing_path = out_path
        merged = _merge_with_existing(existing_path, table, dedup=dedup, max_dedup_rows=max_dedup_rows)
        _write_table_atomic(merged, out_path)
        return

    source = STREAM_TYPE_BY_FEED_TYPE.get(feed_type, feed_type)
    _assert_supported_storage_source(source)
    partition_dir = normalized_partition_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    )
    if feed_type == "trades":
        table = TradeParquetWriter.to_table(events)
    elif feed_type == "bars":
        table = BarParquetWriter.to_table(events)
    else:
        raise ValueError(
            f"{source} feed is out of scope for normalized storage; only trade and kline are supported"
        )
    table = table.replace_schema_metadata(_table_schema_metadata(schema_version=schema_version, events=events, dedup=dedup))
    _write_segment_atomic(table, partition_dir)


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
    if last_ts is not None and first_new_ts is not None and last_ts >= first_new_ts:
        sort_keys = [("event_ts", "ascending")]
        if "raw_run_id" in combined.column_names:
            sort_keys.append(("raw_run_id", "ascending"))
        if "raw_ingestion_seq" in combined.column_names:
            sort_keys.append(("raw_ingestion_seq", "ascending"))
        if "source_id" in combined.column_names:
            sort_keys.append(("source_id", "ascending"))
        if "trade_id" in combined.column_names:
            sort_keys.append(("trade_id", "ascending"))
        return combined.sort_by(sort_keys)
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
    path = Path(path)
    if path.is_dir():
        return _read_partition_dataset(path)
    if path.name == PARTITION_DATA_FILENAME and path.parent.exists():
        return _read_partition_dataset(path.parent)
    if not path.exists():
        raise FileNotFoundError(path)
    return _read_single_parquet(path)


def _append_missing_normalized_columns(table: pa.Table) -> pa.Table:
    metadata = table.schema.metadata or {}
    resolved = resolve_normalizer_version(
        metadata.get(b"normalizer_version").decode("utf-8") if metadata.get(b"normalizer_version") is not None else None
    )
    column_types: dict[str, pa.DataType] = {
        "normalizer_version": pa.string(),
        "provider_ts": pa.timestamp("ms", tz="UTC"),
        "historical_feed_kind": pa.string(),
        "raw_run_id": pa.string(),
        "raw_ingestion_seq": pa.int64(),
    }
    for column_name, column_type in column_types.items():
        if column_name in table.column_names:
            continue
        fill_value = resolved if column_name == "normalizer_version" else None
        table = table.append_column(column_name, pa.array([fill_value] * table.num_rows, type=column_type))
    return table.replace_schema_metadata(
        {
            **(table.schema.metadata or {}),
            b"schema_version": b"v2",
            b"normalizer_version": resolved.encode("utf-8"),
        }
    )


def _read_single_parquet(path: Path) -> pa.Table:
    pf = pq.ParquetFile(path)
    table = pf.read()
    metadata = table.schema.metadata or {}
    version = metadata.get(b"schema_version")
    if version is None:
        raise ValueError("schema_version missing in parquet metadata")
    decoded = version.decode("utf-8")
    if decoded not in {"v1", "v2", "v3"}:
        raise ValueError(f"unsupported schema_version: {decoded}")
    if decoded in {"v2", "v3"}:
        table = _append_missing_normalized_columns(table)
    return table


def _partition_parquet_files(partition_path: Path) -> list[Path]:
    compacted = partition_path / PARTITION_DATA_FILENAME
    segments = sorted(partition_segments_dir(partition_path).glob("*.parquet"))
    files: list[Path] = []
    if compacted.exists():
        files.append(compacted)
    files.extend(segments)
    return files


def _read_partition_dataset(partition_path: Path) -> pa.Table:
    files = _partition_parquet_files(partition_path)
    if not files:
        raise FileNotFoundError(partition_path)
    tables = [_read_single_parquet(file_path) for file_path in files]
    target_schema = max(tables, key=lambda table: len(table.column_names)).schema
    aligned_tables = [
        table if table.schema.equals(target_schema) else _read_existing_table_for_merge(file_path, target_schema)
        for file_path, table in zip(files, tables, strict=False)
    ]
    merged = aligned_tables[0]
    dedup = any((table.schema.metadata or {}).get(b"dedup_policy") == b"true" for table in tables)
    for table in aligned_tables[1:]:
        merged = _dedup_tables(merged, table) if dedup else _concat_tables_ordered(merged, table)
    return merged


def _is_trade_schema(schema: pa.Schema) -> bool:
    names = set(schema.names)
    return {"trade_id", "exchange_ts", "receive_ts", "process_ts", "source_id", "side"}.issubset(names)


def _is_bar_schema(schema: pa.Schema) -> bool:
    names = set(schema.names)
    return {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "interval",
        "open_ts",
        "close_ts",
        "exchange_ts",
        "receive_ts",
        "process_ts",
        "source_id",
    }.issubset(names)


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
    provider_ts = row.get("provider_ts") or _optional_ts(metadata.get("provider_ts"))
    historical_feed_kind = row.get("historical_feed_kind") or metadata.get("historical_feed_kind")
    raw_run_id = row.get("raw_run_id") or metadata.get("raw_run_id")
    raw_ingestion_seq = row.get("raw_ingestion_seq")
    if raw_ingestion_seq in (None, ""):
        raw_ingestion_seq = _coerce_optional_int(metadata.get("raw_ingestion_seq"))
    return {
        "venue": str(row.get("venue") or metadata.get("venue", "BINANCE")).upper(),
        "feed_type": row.get("feed_type") or feed_type_for_source(source),
        "normalizer_version": resolve_normalizer_version(
            row.get("normalizer_version") or metadata.get("normalizer_version")
        ),
        "symbol": row["symbol"],
        "exchange_ts": exchange_ts,
        "provider_ts": provider_ts,
        "receive_ts": row.get("receive_ts") or _optional_ts(metadata.get("receive_ts")),
        "process_ts": row.get("process_ts") or _optional_ts(metadata.get("process_ts")),
        "event_ts": row.get("event_ts") or exchange_ts,
        "price": row["price"],
        "size": row["size"],
        "source": source,
        "source_id": row.get("source_id") or metadata.get("source_id"),
        "trade_id": row.get("trade_id") or metadata.get("trade_id"),
        "side": row.get("side") or metadata.get("side"),
        "historical_feed_kind": historical_feed_kind,
        "raw_run_id": raw_run_id,
        "raw_ingestion_seq": _coerce_optional_int(raw_ingestion_seq),
        "metadata": _metadata_mapping(metadata),
    }


def _bar_row_from_existing(row: dict[str, object]) -> dict[str, object]:
    metadata = _identity_metadata_from_row(row)
    source = str(row.get("source", "kline"))
    exchange_ts = row.get("exchange_ts") or row.get("event_ts")
    raw_ingestion_seq = row.get("raw_ingestion_seq")
    if raw_ingestion_seq in (None, ""):
        raw_ingestion_seq = _coerce_optional_int(metadata.get("raw_ingestion_seq"))
    return {
        "venue": str(row.get("venue") or metadata.get("venue", "BINANCE")).upper(),
        "feed_type": row.get("feed_type") or feed_type_for_source(source),
        "normalizer_version": resolve_normalizer_version(
            row.get("normalizer_version") or metadata.get("normalizer_version")
        ),
        "symbol": row["symbol"],
        "exchange_ts": exchange_ts,
        "provider_ts": row.get("provider_ts") or _optional_ts(metadata.get("provider_ts")),
        "receive_ts": row.get("receive_ts") or _optional_ts(metadata.get("receive_ts")),
        "process_ts": row.get("process_ts") or _optional_ts(metadata.get("process_ts")),
        "event_ts": row.get("event_ts") or exchange_ts,
        "open": row.get("open") if row.get("open") is not None else float(metadata.get("open", row["price"])),
        "high": row.get("high") if row.get("high") is not None else float(metadata.get("high", row["price"])),
        "low": row.get("low") if row.get("low") is not None else float(metadata.get("low", row["price"])),
        "close": row.get("close") if row.get("close") is not None else row["price"],
        "volume": row.get("volume") if row.get("volume") is not None else row["size"],
        "interval": row.get("interval") or metadata.get("interval", "1m"),
        "open_ts": row.get("open_ts") or _optional_ts(metadata.get("open_ts")),
        "close_ts": row.get("close_ts") or _optional_ts(metadata.get("close_ts")) or row.get("event_ts") or exchange_ts,
        "source": source,
        "source_id": row.get("source_id") or metadata.get("source_id"),
        "raw_run_id": row.get("raw_run_id") or metadata.get("raw_run_id"),
        "raw_ingestion_seq": _coerce_optional_int(raw_ingestion_seq),
        "metadata": _metadata_mapping(metadata),
    }


def _row_identity(row: dict[str, object]) -> tuple:
    price = row.get("price", row.get("close"))
    size = row.get("size", row.get("volume"))
    return identity_from_fields(
        symbol=row["symbol"],
        event_ts=row.get("event_ts") or row.get("exchange_ts"),
        price=price,
        size=size,
        source=row["source"],
        venue=row.get("venue"),
        metadata=_identity_metadata_from_row(row),
        source_id=str(row.get("source_id")) if row.get("source_id") not in (None, "") else None,
    )


def _row_sort_key(row: dict[str, object]) -> tuple[object, ...]:
    return (
        row.get("event_ts") or row.get("exchange_ts"),
        str(row.get("raw_run_id") or ""),
        _coerce_optional_int(row.get("raw_ingestion_seq")) if row.get("raw_ingestion_seq") not in (None, "") else -1,
        str(row.get("source_id") or ""),
        str(row.get("trade_id") or ""),
        str(row.get("symbol") or ""),
        str(row.get("source") or ""),
    )


def _iter_table_rows(
    table: pa.Table,
    *,
    columns: list[str] | None = None,
    batch_size: int = 4096,
) -> Iterator[dict[str, object]]:
    selected = table.select(columns) if columns is not None else table
    for batch in selected.to_batches(max_chunksize=batch_size):
        for row in batch.to_pylist():
            yield row


def _read_existing_table_for_merge(out_path: Path, target_schema: pa.Schema) -> pa.Table:
    table = pq.ParquetFile(out_path).read()
    metadata = table.schema.metadata or {}
    version = metadata.get(b"schema_version", b"v1").decode("utf-8")
    target_columns = set(target_schema.names)
    if _is_trade_schema(target_schema):
        rows = [_trade_row_from_existing(row) for row in _iter_table_rows(table)]
        return pa.Table.from_pylist(
            [{key: value for key, value in row.items() if key in target_columns} for row in rows],
            schema=target_schema,
        )
    if _is_bar_schema(target_schema):
        rows = [_bar_row_from_existing(row) for row in _iter_table_rows(table)]
        return pa.Table.from_pylist(
            [{key: value for key, value in row.items() if key in target_columns} for row in rows],
            schema=target_schema,
        )
    if version == "v2":
        rows = []
        for row in _iter_table_rows(table):
            metadata_row = _metadata_mapping(row.get("metadata"))
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
    for row in _iter_table_rows(table):
        metadata = _metadata_mapping(row.get("metadata"))
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

    def add_rows(rows: Iterable[dict[str, object]]) -> None:
        for row in rows:
            key = _row_identity(row)
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

    add_rows(_iter_table_rows(existing))
    add_rows(_iter_table_rows(new))
    merged.sort(key=_row_sort_key)
    return pa.Table.from_pylist(merged, schema=schema)


def _key_set_from_table(tbl: pa.Table) -> set[tuple]:
    keys = set()
    price_cols = ["price", "close"]
    size_cols = ["size", "volume"]
    required_cols = ["symbol", "source"]
    cols = [name for name in required_cols if name in tbl.column_names]
    cols.extend(name for name in price_cols if name in tbl.column_names and name not in cols)
    cols.extend(name for name in size_cols if name in tbl.column_names and name not in cols)
    optional_cols = ["event_ts", "exchange_ts", "venue", "metadata", "source_id", "trade_id", "sequence_id"]
    cols.extend(name for name in optional_cols if name in tbl.column_names and name not in cols)
    for row in _iter_table_rows(tbl, columns=cols):
        keys.add(_row_identity(row))
    return keys


def _filter_new_rows(tbl: pa.Table, existing_keys: set[tuple]) -> pa.Table:
    schema = tbl.schema
    filtered = []
    for row in _iter_table_rows(tbl):
        key = _row_identity(row)
        if key in existing_keys:
            continue
        filtered.append(row)
    if not filtered:
        return pa.Table.from_pylist([], schema=schema)
    filtered.sort(key=_row_sort_key)
    return pa.Table.from_pylist(filtered, schema=schema)
