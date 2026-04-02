from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from app.ingestion.storage import (
    PARTITION_DATA_FILENAME,
    normalized_partition_data_path,
    normalized_partition_path,
    partition_segments_dir,
    record_compaction_failure,
    read_parquet,
)


def compact_partition(
    base_dir: Path,
    env: str,
    *,
    source: str,
    symbol: str,
    day: str,
    venue: str = "BINANCE",
    remove_segments: bool = True,
) -> Path:
    partition_path = normalized_partition_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    )
    if not partition_path.exists():
        raise FileNotFoundError(partition_path)

    out_path = normalized_partition_data_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    )
    try:
        table = read_parquet(partition_path)
        _write_table_atomic(table, out_path)
    except Exception as exc:
        record_compaction_failure(partition_path, exc)
        raise

    if remove_segments:
        segments_dir = partition_segments_dir(partition_path)
        for segment in sorted(segments_dir.glob("*.parquet")):
            segment.unlink()
        try:
            segments_dir.rmdir()
        except OSError:
            pass
    return out_path


def compact_environment(
    base_dir: Path,
    env: str,
    *,
    remove_segments: bool = True,
) -> list[Path]:
    base = Path(base_dir)
    outputs: list[Path] = []
    for partition_path in sorted(base.glob(f"normalized/*/env={env}/venue=*/symbol=*/date=*")):
        feed_type = partition_path.parents[3].name
        source = "trade" if feed_type == "trades" else "kline" if feed_type == "bars" else feed_type
        venue = partition_path.parents[1].name.split("=", 1)[1]
        symbol = partition_path.parents[0].name.split("=", 1)[1]
        day = partition_path.name.split("=", 1)[1]
        outputs.append(
            compact_partition(
                base,
                env,
                source=source,
                symbol=symbol,
                day=day,
                venue=venue,
                remove_segments=remove_segments,
            )
        )
    return outputs


def _write_table_atomic(table, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{PARTITION_DATA_FILENAME}.tmp")
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
