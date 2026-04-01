"""
Historical backfill for bars only (`kline`).

Usage:
    python -m app.ingestion.backfill --env dev --symbol BTCUSDT --start 2024-01-01 --end 2024-01-02 --interval 1m --batch 1000 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from typing import List, Optional

import httpx

from app.common.dto import normalize_symbol
from app.ingestion.client import normalize_kline_typed
from app.config import load_config
from app.ingestion.dedup import Deduplicator, deduplicate_events as deduplicate_market_events
from app.ingestion.sinks import EventSink, ParquetEventSink
from app.ingestion.storage import ParquetWriter
from app.marketdata.instruments import ensure_default_instruments
from app.marketdata.models import BarEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord, RawSink
from app.observability.logger import get_logger, set_trace_id


SUPPORTED_HISTORICAL_BACKFILL_FEEDS = ("kline",)
HISTORICAL_BACKFILL_SCOPE = "bars-only"


def supports_historical_backfill(feed_type: str) -> bool:
    return str(feed_type).lower() in SUPPORTED_HISTORICAL_BACKFILL_FEEDS


def assert_historical_backfill_support(feed_type: str) -> None:
    normalized = str(feed_type).lower()
    if not supports_historical_backfill(normalized):
        raise ValueError(
            f"{normalized} historical backfill is not supported; current scope is {HISTORICAL_BACKFILL_SCOPE} ({', '.join(SUPPORTED_HISTORICAL_BACKFILL_FEEDS)})"
        )


def parse_iso_utc(value: str) -> dt.datetime:
    try:
        ts = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Fecha invalida: {value}") from exc
    if ts.tzinfo is None:
        raise argparse.ArgumentTypeError("Las fechas deben incluir zona horaria (ej: 2024-01-01T00:00:00+00:00)")
    return ts.astimezone(dt.timezone.utc)


def to_ms(ts: dt.datetime) -> int:
    return int(ts.timestamp() * 1000)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historico bars-only (`kline`): escribe raw + normalized o solo descarga con --dry-run",
        epilog="Trade historical backfill no esta soportado; el alcance historico actual es bars-only (`kline`).",
    )
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--symbol", required=True, help="Simbolo (ej: BTCUSDT)")
    parser.add_argument("--start", required=True, type=parse_iso_utc, help="Inicio (ISO UTC, ej: 2024-01-01T00:00:00+00:00)")
    parser.add_argument("--end", required=True, type=parse_iso_utc, help="Fin (ISO UTC)")
    parser.add_argument(
        "--interval",
        default="1m",
        help="Intervalo kline (default 1m). Trade historical backfill no esta soportado; el alcance historico actual es bars-only.",
    )
    parser.add_argument("--batch", type=int, default=1000, help="Limite por pagina (<=1000)")
    parser.add_argument("--dedup", action="store_true", help="Deduplica eventos con la clave compartida de ingestion")
    parser.add_argument("--dry-run", action="store_true", help="No persiste, solo descarga y resume")
    return parser.parse_args(argv)


def fetch_klines(
    client: httpx.Client,
    base_url: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
    limit: int = 1000,
    retries_429: int = 3,
    retries_5xx: int = 2,
    timeout: float = 10.0,
) -> List[dict]:
    """Descarga klines paginadas hasta end_ms (exclusivo) con manejo simple de errores."""
    url = f"{base_url.rstrip('/')}/api/v3/klines"
    out: List[dict] = []
    next_start = start_ms
    while next_start < end_ms:
        params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": next_start, "endTime": end_ms}
        attempt_429 = 0
        attempt_5xx = 0
        while True:
            try:
                resp = client.get(url, params=params, timeout=timeout)
            except httpx.TimeoutException as exc:
                attempt_5xx += 1
                if attempt_5xx > retries_5xx:
                    raise httpx.HTTPStatusError(
                        f"Timeout agotado al descargar klines {symbol} interval={interval} start={start_ms}",
                        request=None,
                        response=None,
                    ) from exc
                time.sleep(0.5 * attempt_5xx)
                continue

            if resp.status_code == 429:
                attempt_429 += 1
                if attempt_429 > retries_429:
                    raise httpx.HTTPStatusError(
                        f"Rate limit alcanzado ({resp.status_code}) tras {retries_429} reintentos "
                        f"para {symbol} interval={interval}",
                        request=getattr(resp, "request", None),
                        response=resp,
                    )
                time.sleep(0.5 * attempt_429)
                continue

            if resp.status_code >= 500:
                attempt_5xx += 1
                if attempt_5xx > retries_5xx:
                    raise httpx.HTTPStatusError(
                        f"Error servidor {resp.status_code} tras {retries_5xx} reintentos "
                        f"para {symbol} interval={interval}",
                        request=getattr(resp, "request", None),
                        response=resp,
                    )
                time.sleep(0.5 * attempt_5xx)
                continue

            resp.raise_for_status()
            break

        batch = resp.json()
        if not batch:
            break
        out.extend(batch)
        last_close_time = batch[-1][6]
        next_start = last_close_time + 1
        if len(batch) < limit:
            break
    return out


def _kline_payload_from_row(symbol: str, row: list, *, interval: str) -> dict:
    close = str(row[4])
    return {
        "s": symbol,
        "E": int(row[6]),
        "k": {
            "t": int(row[0]),
            "T": int(row[6]),
            "o": str(row[1]) if len(row) > 1 and row[1] not in ("", None) else close,
            "h": str(row[2]) if len(row) > 2 and row[2] not in ("", None) else close,
            "l": str(row[3]) if len(row) > 3 and row[3] not in ("", None) else close,
            "c": close,
            "q": str(row[5]),
            "i": interval,
        },
    }


def normalize_kline_row(
    symbol: str,
    row: list,
    *,
    interval: str = "1m",
    receive_ts: dt.datetime | None = None,
    process_ts: dt.datetime | None = None,
    venue: str = "BINANCE",
) -> BarEvent:
    payload = _kline_payload_from_row(symbol, row, interval=interval)
    return normalize_kline_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
        interval=interval,
    )


def raw_record_from_kline_row(
    symbol: str,
    row: list,
    *,
    interval: str,
    receive_ts: dt.datetime,
    trace_id: str | None,
    venue: str = "BINANCE",
) -> RawRecord:
    payload = _kline_payload_from_row(symbol, row, interval=interval)
    return RawRecord(
        payload=payload,
        venue=venue,
        stream_type="kline",
        symbol=symbol,
        exchange_ts=dt.datetime.fromtimestamp(int(row[6]) / 1000, tz=dt.timezone.utc),
        receive_ts=receive_ts,
        trace_id=trace_id,
        source_id=str(row[0]),
    )


def deduplicate_events(events: List[BarEvent]) -> tuple[List[BarEvent], int]:
    return deduplicate_market_events(
        events,
        deduplicator=Deduplicator(ttl_seconds=None, max_entries=max(len(events), 4096)),
    )


def run(argv: Optional[list[str]] = None, sink: Optional[EventSink] = None, raw_sink: Optional[RawSink] = None) -> int:
    args = parse_args(argv)
    assert_historical_backfill_support("kline")
    cfg = load_config(args.env)
    symbol = normalize_symbol(args.symbol)
    ensure_default_instruments([symbol], venue="BINANCE")
    start_ms = to_ms(args.start)
    end_ms = to_ms(args.end)
    interval_ms = _interval_to_ms(args.interval)

    trace_id = f"backfill-{int(time.time())}"
    logger = get_logger(name="backfill", level=cfg.log_level)
    set_trace_id(trace_id)

    logger.info(
        "backfill starting",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbol": symbol,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval,
            "historical_scope": HISTORICAL_BACKFILL_SCOPE,
            "supported_historical_feeds": list(SUPPORTED_HISTORICAL_BACKFILL_FEEDS),
            "batch": args.batch,
            "dedup": args.dedup,
            "dry_run": args.dry_run,
        },
    )

    with httpx.Client() as client:
        klines = fetch_klines(
            client=client,
            base_url=cfg.rest_base,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            interval=args.interval,
            limit=args.batch,
        )
    receive_ts = dt.datetime.now(dt.timezone.utc)
    raw_records = [
        raw_record_from_kline_row(
            symbol,
            row,
            interval=args.interval,
            receive_ts=receive_ts,
            trace_id=trace_id,
        )
        for row in klines
    ]
    events = [
        normalize_kline_row(
            symbol,
            row,
            interval=args.interval,
            receive_ts=receive_ts,
            process_ts=receive_ts,
        )
        for row in klines
    ]
    events.sort(key=lambda event: event.event_ts)
    raw_records.sort(key=lambda record: record.exchange_ts)

    duplicates_dropped = 0
    if args.dedup:
        events, duplicates_dropped = deduplicate_events(events)
        if duplicates_dropped:
            logger.info(
                "backfill duplicates dropped",
                extra={"trace_id": trace_id, "symbol": symbol, "duplicates_dropped": duplicates_dropped},
            )

    expected = ((end_ms - start_ms) // interval_ms) + 1
    gaps = _count_gaps(events, interval_ms)

    if not args.dry_run:
        raw_sink_impl = raw_sink or JsonlRawSink(base_dir=cfg.data_dir / "raw", env=cfg.env)
        sink_impl = sink or ParquetEventSink(
            ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.batch, dedup=args.dedup)
        )
        for record in raw_records:
            raw_sink_impl.write(record)
        for event in events:
            sink_impl.add(event)
        sink_impl.close()

    logger.info(
        "backfill finished",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbol": symbol,
            "rows": len(events),
            "expected": expected,
            "gaps": gaps,
            "dedup": args.dedup,
            "duplicates_dropped": duplicates_dropped,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval,
            "historical_scope": HISTORICAL_BACKFILL_SCOPE,
            "supported_historical_feeds": list(SUPPORTED_HISTORICAL_BACKFILL_FEEDS),
            "dry_run": args.dry_run,
        },
    )
    return 0


def _interval_to_ms(interval: str) -> int:
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
    }
    if interval not in mapping:
        raise ValueError(f"Intervalo no soportado para backfill: {interval}")
    return mapping[interval]


def _count_gaps(events: List[BarEvent], interval_ms: int) -> int:
    if len(events) < 2:
        return 0
    events_sorted = sorted(events, key=lambda event: event.event_ts)
    gap_count = 0
    expected_delta = dt.timedelta(milliseconds=interval_ms)
    for previous, current in zip(events_sorted, events_sorted[1:]):
        delta = current.event_ts - previous.event_ts
        if delta > expected_delta:
            gap_count += 1
    return gap_count


if __name__ == "__main__":
    raise SystemExit(run())
