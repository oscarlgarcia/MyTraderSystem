from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq

from app.marketdata.errors import ShadowPromotionError
from app.ingestion.storage import (
    PARTITION_DATA_FILENAME,
    STREAM_TYPE_BY_FEED_TYPE,
    feed_type_for_source,
    legacy_partition_path,
    normalized_partition_path,
    partition_segments_dir,
    read_parquet,
)


@dataclass(frozen=True, slots=True)
class ShadowPartitionSnapshot:
    row_count: int
    identity_count: int
    identity_checksum: str
    row_checksum: str
    min_event_ts: str | None
    max_event_ts: str | None


@dataclass(frozen=True, slots=True)
class ShadowSnapshot:
    pipeline_version: str
    row_count: int
    identity_count: int
    identity_checksum: str
    row_checksum: str
    partitions: dict[str, ShadowPartitionSnapshot]
    min_event_ts: str | None
    max_event_ts: str | None
    gaps_total: int
    processing_latency_seconds: float
    write_latency_seconds: float
    scope_mode: str = "full_scan"
    scope_partitions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ShadowComparison:
    primary: ShadowSnapshot
    shadow: ShadowSnapshot
    diffs: dict[str, Any]
    significant: bool

def build_shadow_snapshot(
    base_dir: Path,
    *,
    env: str,
    pipeline_version: str,
    gaps_total: int,
    processing_latency_seconds: float,
    write_latency_seconds: float,
    partition_keys: Iterable[str] | None = None,
) -> ShadowSnapshot:
    scoped_partitions = tuple(sorted({str(key) for key in (partition_keys or ()) if str(key).strip()}))
    global_identities: set[str] = set()
    global_row_digest = hashlib.sha256()
    min_event_ts: str | None = None
    max_event_ts: str | None = None
    row_count = 0
    partitions: dict[str, ShadowPartitionSnapshot] = {}
    for partition_key, rows in _collect_partition_canonical_rows(
        base_dir=Path(base_dir),
        env=env,
        pipeline_version=pipeline_version,
        partition_keys=scoped_partitions or None,
    ):
        row_count += len(rows)
        for row in rows:
            global_identities.add(row["identity"])
            _checksum_update(global_row_digest, row["row_hash"])
            event_ts = row["event_ts"]
            if min_event_ts is None or event_ts < min_event_ts:
                min_event_ts = event_ts
            if max_event_ts is None or event_ts > max_event_ts:
                max_event_ts = event_ts
        partitions[partition_key] = _partition_snapshot(rows)
    return ShadowSnapshot(
        pipeline_version=pipeline_version,
        row_count=row_count,
        identity_count=len(global_identities),
        identity_checksum=_checksum_lines(sorted(global_identities)),
        row_checksum=global_row_digest.hexdigest(),
        partitions=partitions,
        min_event_ts=min_event_ts,
        max_event_ts=max_event_ts,
        gaps_total=int(gaps_total),
        processing_latency_seconds=float(processing_latency_seconds),
        write_latency_seconds=float(write_latency_seconds),
        scope_mode="partition_scope" if scoped_partitions else "full_scan",
        scope_partitions=scoped_partitions,
    )


def affected_shadow_partitions(events: Iterable[Any]) -> tuple[str, ...]:
    partitions = {
        key
        for key in (_shadow_partition_key_for_event(event) for event in events)
        if key is not None
    }
    return tuple(sorted(partitions))


def compare_shadow_snapshots(primary: ShadowSnapshot, shadow: ShadowSnapshot) -> ShadowComparison:
    all_partitions = sorted(set(primary.partitions) | set(shadow.partitions))
    partition_row_count_diffs: dict[str, int] = {}
    partition_identity_count_diffs: dict[str, int] = {}
    partition_checksum_mismatches: list[str] = []
    partition_timestamp_mismatches: list[str] = []

    for partition in all_partitions:
        primary_partition = primary.partitions.get(partition)
        shadow_partition = shadow.partitions.get(partition)
        primary_rows = primary_partition.row_count if primary_partition else 0
        shadow_rows = shadow_partition.row_count if shadow_partition else 0
        if primary_rows != shadow_rows:
            partition_row_count_diffs[partition] = primary_rows - shadow_rows

        primary_identities = primary_partition.identity_count if primary_partition else 0
        shadow_identities = shadow_partition.identity_count if shadow_partition else 0
        if primary_identities != shadow_identities:
            partition_identity_count_diffs[partition] = primary_identities - shadow_identities

        primary_identity_checksum = primary_partition.identity_checksum if primary_partition else None
        shadow_identity_checksum = shadow_partition.identity_checksum if shadow_partition else None
        primary_row_checksum = primary_partition.row_checksum if primary_partition else None
        shadow_row_checksum = shadow_partition.row_checksum if shadow_partition else None
        if (
            primary_identity_checksum != shadow_identity_checksum
            or primary_row_checksum != shadow_row_checksum
        ):
            partition_checksum_mismatches.append(partition)

        primary_min = primary_partition.min_event_ts if primary_partition else None
        primary_max = primary_partition.max_event_ts if primary_partition else None
        shadow_min = shadow_partition.min_event_ts if shadow_partition else None
        shadow_max = shadow_partition.max_event_ts if shadow_partition else None
        if primary_min != shadow_min or primary_max != shadow_max:
            partition_timestamp_mismatches.append(partition)

    diffs: dict[str, Any] = {
        "row_count": primary.row_count - shadow.row_count,
        "identity_count": primary.identity_count - shadow.identity_count,
        "identity_checksum_match": primary.identity_checksum == shadow.identity_checksum,
        "row_checksum_match": primary.row_checksum == shadow.row_checksum,
        "partition_row_count_diffs": partition_row_count_diffs,
        "partition_identity_count_diffs": partition_identity_count_diffs,
        "partition_checksum_mismatches": partition_checksum_mismatches,
        "partition_timestamp_mismatches": partition_timestamp_mismatches,
        "min_event_ts_match": primary.min_event_ts == shadow.min_event_ts,
        "max_event_ts_match": primary.max_event_ts == shadow.max_event_ts,
        "gaps_total": primary.gaps_total - shadow.gaps_total,
        "processing_latency_seconds": float(primary.processing_latency_seconds - shadow.processing_latency_seconds),
        "write_latency_seconds": float(primary.write_latency_seconds - shadow.write_latency_seconds),
    }
    significant = any(
        [
            diffs["row_count"] != 0,
            diffs["identity_count"] != 0,
            not diffs["identity_checksum_match"],
            not diffs["row_checksum_match"],
            bool(partition_row_count_diffs),
            bool(partition_identity_count_diffs),
            bool(partition_checksum_mismatches),
            bool(partition_timestamp_mismatches),
            not diffs["min_event_ts_match"],
            not diffs["max_event_ts_match"],
            diffs["gaps_total"] != 0,
        ]
    )
    return ShadowComparison(
        primary=primary,
        shadow=shadow,
        diffs=diffs,
        significant=significant,
    )


def persist_shadow_comparison(base_dir: Path, *, env: str, comparison: ShadowComparison) -> Path:
    out_path = Path(base_dir) / "shadow" / f"env={env}" / "comparisons.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "primary": asdict(comparison.primary),
        "shadow": asdict(comparison.shadow),
        "diffs": comparison.diffs,
        "significant": comparison.significant,
    }
    with out_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    return out_path


def assert_shadow_promotable(comparison: ShadowComparison, *, block_on_diff: bool) -> None:
    if block_on_diff and comparison.significant:
        raise ShadowPromotionError(
            diffs={
                key: value
                for key, value in comparison.diffs.items()
                if value not in (0, 0.0, True, {}, [])
            }
        )


def _shadow_paths(
    base_dir: Path,
    *,
    env: str,
    pipeline_version: str,
    partition_keys: tuple[str, ...] | None = None,
) -> list[Path]:
    if not partition_keys:
        if pipeline_version == "v1":
            return sorted(base_dir.glob(f"{env}/symbol=*/date=*/data.parquet"))
        if pipeline_version == "v2":
            return sorted(base_dir.glob(f"normalized/*/env={env}/venue=*/symbol=*/date=*"))
        return []

    paths: list[Path] = []
    seen: set[Path] = set()
    for partition_key in partition_keys:
        feed_type, venue, symbol, day = _parse_partition_key(partition_key)
        if pipeline_version == "v1":
            path = legacy_partition_path(base_dir, env, symbol, day)
        elif pipeline_version == "v2":
            source = STREAM_TYPE_BY_FEED_TYPE.get(feed_type)
            if source is None:
                continue
            path = normalized_partition_path(
                base_dir,
                env,
                source=source,
                symbol=symbol,
                day=day,
                venue=venue,
            )
        else:
            continue
        if path.exists() and path not in seen:
            seen.add(path)
            paths.append(path)
    return sorted(paths)


def _collect_partition_canonical_rows(
    base_dir: Path,
    *,
    env: str,
    pipeline_version: str,
    partition_keys: tuple[str, ...] | None = None,
) -> list[tuple[str, list[dict[str, Any]]]]:
    paths = _shadow_paths(
        base_dir,
        env=env,
        pipeline_version=pipeline_version,
        partition_keys=partition_keys,
    )
    if len(paths) <= 1:
        partition_rows: list[tuple[str, list[dict[str, Any]]]] = []
        for path in paths:
            partition_rows.extend(_partition_canonical_rows_from_path(path, partition_keys=partition_keys))
        return sorted(partition_rows, key=lambda item: item[0])

    collected: list[tuple[str, list[dict[str, Any]]]] = []
    with ThreadPoolExecutor(max_workers=min(8, len(paths))) as executor:
        future_map = {
            executor.submit(_partition_canonical_rows_from_path, path, partition_keys=partition_keys): path for path in paths
        }
        for future in as_completed(future_map):
            collected.extend(future.result())
    return sorted(collected, key=lambda item: item[0])


def _partition_canonical_rows_from_path(
    path: Path,
    *,
    partition_keys: tuple[str, ...] | None = None,
) -> list[tuple[str, list[dict[str, Any]]]]:
    partition_rows: dict[str, list[dict[str, Any]]] = {}
    table_paths: list[Path]
    if path.is_dir():
        table_paths = []
        compacted = path / PARTITION_DATA_FILENAME
        if compacted.exists():
            table_paths.append(compacted)
        table_paths.extend(sorted(partition_segments_dir(path).glob("*.parquet")))
    else:
        table_paths = [path]
    for table_path in table_paths:
        parquet_file = pq.ParquetFile(table_path)
        for batch in parquet_file.iter_batches(batch_size=4096):
            for canonical in _iter_batch_canonical_rows(batch):
                if partition_keys and canonical["partition_key"] not in partition_keys:
                    continue
                partition_rows.setdefault(canonical["partition_key"], []).append(canonical)
    results: list[tuple[str, list[dict[str, Any]]]] = []
    for partition_key, rows in sorted(partition_rows.items()):
        rows.sort(key=lambda row: (row["event_ts"], row["identity"], row["row_hash"]))
        results.append((partition_key, rows))
    return results


def _iter_batch_canonical_rows(batch: Any) -> Iterable[dict[str, Any]]:
    names = set(batch.schema.names)
    row_count = batch.num_rows

    def _column_values(name: str) -> list[Any]:
        if name not in names:
            return [None] * row_count
        return batch.column(batch.schema.get_field_index(name)).to_pylist()

    symbol_values = _column_values("symbol")
    source_values = _column_values("source")
    feed_type_values = _column_values("feed_type")
    venue_values = _column_values("venue")
    event_ts_values = _column_values("event_ts")
    exchange_ts_values = _column_values("exchange_ts")
    receive_ts_values = _column_values("receive_ts")
    process_ts_values = _column_values("process_ts")
    provider_ts_values = _column_values("provider_ts")
    source_id_values = _column_values("source_id")
    trade_id_values = _column_values("trade_id")
    side_values = _column_values("side")
    price_values = _column_values("price")
    size_values = _column_values("size")
    open_values = _column_values("open")
    high_values = _column_values("high")
    low_values = _column_values("low")
    close_values = _column_values("close")
    volume_values = _column_values("volume")
    interval_values = _column_values("interval")
    open_ts_values = _column_values("open_ts")
    close_ts_values = _column_values("close_ts")
    metadata_values = _column_values("metadata")

    for index in range(row_count):
        metadata = _metadata_mapping(metadata_values[index])
        yield _canonical_row_from_values(
            symbol=symbol_values[index],
            source=source_values[index],
            feed_type=feed_type_values[index],
            venue=venue_values[index],
            event_ts=event_ts_values[index],
            exchange_ts=exchange_ts_values[index],
            receive_ts=receive_ts_values[index],
            process_ts=process_ts_values[index],
            provider_ts=provider_ts_values[index],
            source_id=source_id_values[index],
            trade_id=trade_id_values[index],
            side=side_values[index],
            price=price_values[index],
            size=size_values[index],
            open_value=open_values[index],
            high_value=high_values[index],
            low_value=low_values[index],
            close_value=close_values[index],
            volume_value=volume_values[index],
            interval=interval_values[index],
            open_ts=open_ts_values[index],
            close_ts=close_ts_values[index],
            metadata=metadata,
        )


def _canonical_row_from_values(
    *,
    symbol: Any,
    source: Any,
    feed_type: Any,
    venue: Any,
    event_ts: Any,
    exchange_ts: Any,
    receive_ts: Any,
    process_ts: Any,
    provider_ts: Any,
    source_id: Any,
    trade_id: Any,
    side: Any,
    price: Any,
    size: Any,
    open_value: Any,
    high_value: Any,
    low_value: Any,
    close_value: Any,
    volume_value: Any,
    interval: Any,
    open_ts: Any,
    close_ts: Any,
    metadata: dict[str, str],
) -> dict[str, Any]:
    source_value = str(source or metadata.get("source", "unknown"))
    feed_type_value = str(feed_type or feed_type_for_source(source_value))
    venue_value = str(venue or metadata.get("venue", "BINANCE")).upper()
    symbol_value = str(symbol)
    event_ts_value = _iso_ts(event_ts or exchange_ts)
    exchange_ts_value = _iso_ts(exchange_ts or event_ts)
    receive_ts_value = _iso_ts(receive_ts or metadata.get("receive_ts"))
    process_ts_value = _iso_ts(process_ts or metadata.get("process_ts"))
    source_id_value = _string_or_none(source_id or metadata.get("source_id"))
    trade_id_value = _string_or_none(trade_id or metadata.get("trade_id"))
    side_value = _string_or_none(side or metadata.get("side"))
    price_value = _float_or_none(price if price not in (None, "") else close_value)
    size_value = _float_or_none(size if size not in (None, "") else volume_value)
    open_numeric = _float_or_none(open_value if open_value not in (None, "") else metadata.get("open"))
    high_numeric = _float_or_none(high_value if high_value not in (None, "") else metadata.get("high"))
    low_numeric = _float_or_none(low_value if low_value not in (None, "") else metadata.get("low"))
    close_numeric = _float_or_none(close_value if close_value not in (None, "") else metadata.get("close", price))
    volume_numeric = _float_or_none(volume_value if volume_value not in (None, "") else metadata.get("volume", size))
    interval_value = _string_or_none(interval or metadata.get("interval"))
    open_ts_value = _iso_ts(open_ts or metadata.get("open_ts"))
    close_ts_value = _iso_ts(close_ts or metadata.get("close_ts"))
    day = event_ts_value[:10]
    identity = _checksum_components(
        [
            feed_type_value,
            venue_value,
            symbol_value,
            event_ts_value,
            source_value,
            source_id_value,
            trade_id_value,
            side_value,
            _number_text(price_value if price_value is not None else close_numeric),
            _number_text(size_value if size_value is not None else volume_numeric),
            interval_value,
            open_ts_value,
            close_ts_value,
        ]
    )
    canonical = {
        "partition_key": f"{feed_type_value}:{venue_value}:{symbol_value}:{day}",
        "venue": venue_value,
        "feed_type": feed_type_value,
        "symbol": symbol_value,
        "source": source_value,
        "event_ts": event_ts_value,
        "exchange_ts": exchange_ts_value,
        "receive_ts": receive_ts_value,
        "process_ts": process_ts_value,
        "price": price_value,
        "size": size_value,
        "source_id": source_id_value,
        "trade_id": trade_id_value,
        "side": side_value,
        "open": open_numeric,
        "high": high_numeric,
        "low": low_numeric,
        "close": close_numeric,
        "volume": volume_numeric,
        "interval": interval_value,
        "open_ts": open_ts_value,
        "close_ts": close_ts_value,
        "identity": identity,
    }
    canonical["row_hash"] = _checksum_components(
        [
            canonical["partition_key"],
            canonical["event_ts"],
            canonical["exchange_ts"],
            canonical["receive_ts"],
            canonical["process_ts"],
            canonical["source"],
            canonical["source_id"],
            canonical["trade_id"],
            canonical["side"],
            _number_text(canonical["price"]),
            _number_text(canonical["size"]),
            _number_text(canonical["open"]),
            _number_text(canonical["high"]),
            _number_text(canonical["low"]),
            _number_text(canonical["close"]),
            _number_text(canonical["volume"]),
            canonical["interval"],
            canonical["open_ts"],
            canonical["close_ts"],
        ]
    )
    return canonical


def _iter_partition_canonical_rows(
    base_dir: Path,
    *,
    env: str,
    pipeline_version: str,
    partition_keys: tuple[str, ...] | None = None,
) -> Iterable[tuple[str, list[dict[str, Any]]]]:
    for partition_key, rows in _collect_partition_canonical_rows(
        base_dir,
        env=env,
        pipeline_version=pipeline_version,
        partition_keys=partition_keys,
    ):
        yield partition_key, rows


def _iter_table_rows(table: Any, *, batch_size: int = 4096) -> Iterable[dict[str, Any]]:
    for batch in table.to_batches(max_chunksize=batch_size):
        for row in batch.to_pylist():
            yield row


def _shadow_partition_key_for_event(event: Any) -> str | None:
    event_ts = getattr(event, "event_ts", None) or getattr(event, "exchange_ts", None)
    source = getattr(event, "source", None)
    symbol = getattr(event, "symbol", None)
    if event_ts is None or source in (None, "") or symbol in (None, ""):
        return None
    metadata = _metadata_mapping(getattr(event, "metadata", None))
    venue = str(getattr(event, "venue", metadata.get("venue", "BINANCE"))).upper()
    day = _iso_ts(event_ts)
    if day is None:
        return None
    return f"{feed_type_for_source(str(source))}:{venue}:{str(symbol)}:{day[:10]}"


def _parse_partition_key(partition_key: str) -> tuple[str, str, str, str]:
    parts = str(partition_key).split(":")
    if len(parts) != 4:
        raise ValueError(f"invalid shadow partition key: {partition_key}")
    feed_type, venue, symbol, day = parts
    return feed_type, venue.upper(), symbol, day


def _canonical_row(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _metadata_mapping(row.get("metadata"))
    source = str(row.get("source", metadata.get("source", "unknown")))
    feed_type = str(row.get("feed_type") or feed_type_for_source(source))
    venue = str(row.get("venue") or metadata.get("venue", "BINANCE")).upper()
    symbol = str(row["symbol"])
    event_ts = _iso_ts(row.get("event_ts") or row.get("exchange_ts"))
    exchange_ts = _iso_ts(row.get("exchange_ts") or row.get("event_ts"))
    receive_ts = _iso_ts(row.get("receive_ts") or metadata.get("receive_ts"))
    process_ts = _iso_ts(row.get("process_ts") or metadata.get("process_ts"))
    source_id = _string_or_none(row.get("source_id") or metadata.get("source_id"))
    trade_id = _string_or_none(row.get("trade_id") or metadata.get("trade_id"))
    side = _string_or_none(row.get("side") or metadata.get("side"))
    price = _float_or_none(row.get("price", row.get("close")))
    size = _float_or_none(row.get("size", row.get("volume")))
    open_value = _float_or_none(row.get("open", metadata.get("open")))
    high_value = _float_or_none(row.get("high", metadata.get("high")))
    low_value = _float_or_none(row.get("low", metadata.get("low")))
    close_value = _float_or_none(row.get("close", metadata.get("close", row.get("price"))))
    volume_value = _float_or_none(row.get("volume", metadata.get("volume", row.get("size"))))
    interval = _string_or_none(row.get("interval") or metadata.get("interval"))
    open_ts = _iso_ts(row.get("open_ts") or metadata.get("open_ts"))
    close_ts = _iso_ts(row.get("close_ts") or metadata.get("close_ts"))
    identity = identity_from_fields(
        symbol=symbol,
        event_ts=_parse_iso(event_ts),
        price=price if price is not None else close_value if close_value is not None else 0.0,
        size=size if size is not None else volume_value if volume_value is not None else 0.0,
        source=source,
        venue=venue,
        metadata={
            **metadata,
            **({"trade_id": trade_id} if trade_id is not None else {}),
            **({"source_id": source_id} if source_id is not None else {}),
        },
        source_id=source_id,
    )
    day = event_ts[:10]
    canonical = {
        "partition_key": f"{feed_type}:{venue}:{symbol}:{day}",
        "venue": venue,
        "feed_type": feed_type,
        "symbol": symbol,
        "source": source,
        "event_ts": event_ts,
        "exchange_ts": exchange_ts,
        "receive_ts": receive_ts,
        "process_ts": process_ts,
        "price": price,
        "size": size,
        "source_id": source_id,
        "trade_id": trade_id,
        "side": side,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": volume_value,
        "interval": interval,
        "open_ts": open_ts,
        "close_ts": close_ts,
        "identity": json.dumps(list(identity), ensure_ascii=False, default=str),
    }
    canonical["row_hash"] = _checksum_payload(canonical)
    return canonical


def _partition_snapshot(rows: list[dict[str, Any]]) -> ShadowPartitionSnapshot:
    identities = sorted({row["identity"] for row in rows})
    ordered_rows = sorted(rows, key=lambda row: (row["event_ts"], row["identity"], row["row_hash"]))
    return ShadowPartitionSnapshot(
        row_count=len(rows),
        identity_count=len(identities),
        identity_checksum=_checksum_lines(identities),
        row_checksum=_checksum_rows(ordered_rows),
        min_event_ts=min((row["event_ts"] for row in rows), default=None),
        max_event_ts=max((row["event_ts"] for row in rows), default=None),
    )


def _checksum_rows(rows: list[dict[str, Any]]) -> str:
    payloads = [row["row_hash"] if "row_hash" in row else _checksum_payload(row) for row in rows]
    return _checksum_lines(payloads)


def _checksum_lines(values: list[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        _checksum_update(digest, value)
    return digest.hexdigest()


def _checksum_update(digest: Any, value: str) -> None:
    digest.update(str(value).encode("utf-8"))
    digest.update(b"\n")


def _checksum_payload(value: dict[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key not in {"partition_key", "row_hash"}}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _checksum_components(values: list[Any]) -> str:
    digest = hashlib.sha256()
    for value in values:
        _checksum_update(digest, "" if value is None else value)
    return digest.hexdigest()


def _number_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return f"{float(value):.10f}"


def _metadata_mapping(value: Any) -> dict[str, str]:
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


def _iso_ts(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parse_iso(value: str | None) -> datetime:
    if value is None:
        raise ValueError("event_ts is required for shadow snapshot canonicalization")
    return datetime.fromisoformat(value)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _string_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
