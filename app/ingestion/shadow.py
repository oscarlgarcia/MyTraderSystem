from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.ingestion.dedup import identity_from_fields
from app.ingestion.storage import (
    STREAM_TYPE_BY_FEED_TYPE,
    feed_type_for_source,
    legacy_partition_path,
    normalized_partition_path,
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


class ShadowPromotionError(RuntimeError):
    pass


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
    rows = _load_canonical_rows(
        base_dir=Path(base_dir),
        env=env,
        pipeline_version=pipeline_version,
        partition_keys=scoped_partitions or None,
    )
    partition_rows: dict[str, list[dict[str, Any]]] = {}
    global_identities: set[str] = set()
    min_event_ts: str | None = None
    max_event_ts: str | None = None

    for row in rows:
        partition_rows.setdefault(row["partition_key"], []).append(row)
        global_identities.add(row["identity"])
        event_ts = row["event_ts"]
        if min_event_ts is None or event_ts < min_event_ts:
            min_event_ts = event_ts
        if max_event_ts is None or event_ts > max_event_ts:
            max_event_ts = event_ts

    partitions = {
        key: _partition_snapshot(items)
        for key, items in sorted(partition_rows.items())
    }
    return ShadowSnapshot(
        pipeline_version=pipeline_version,
        row_count=len(rows),
        identity_count=len(global_identities),
        identity_checksum=_checksum_lines(sorted(global_identities)),
        row_checksum=_checksum_rows(rows),
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
            "shadow comparison detected significant differences: "
            + ", ".join(
                f"{key}={value}"
                for key, value in comparison.diffs.items()
                if value not in (0, 0.0, True, {}, [])
            )
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


def _load_canonical_rows(
    base_dir: Path,
    *,
    env: str,
    pipeline_version: str,
    partition_keys: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _shadow_paths(
        base_dir,
        env=env,
        pipeline_version=pipeline_version,
        partition_keys=partition_keys,
    ):
        table = read_parquet(path)
        for row in table.to_pylist():
            canonical = _canonical_row(row)
            if partition_keys and canonical["partition_key"] not in partition_keys:
                continue
            rows.append(canonical)
    rows.sort(key=lambda row: (row["partition_key"], row["event_ts"], row["identity"], row["row_hash"]))
    return rows


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
        digest.update(str(value).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _checksum_payload(value: dict[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key not in {"partition_key", "row_hash"}}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


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
