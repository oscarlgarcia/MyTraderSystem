"""
Backfill histórico puntual (solo descarga en memoria; no escribe disco).

Uso:
    python -m app.ingestion.backfill --env dev --symbol BTCUSDT --start 2024-01-01 --end 2024-01-02 --interval 1m --batch 1000 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from typing import Iterable, List, Optional

import httpx

from app.common.dto import MarketEvent, normalize_symbol
from app.config import load_config
from app.observability.logger import get_logger, set_trace_id
from app.ingestion.storage import ParquetWriter


def parse_iso_utc(value: str) -> dt.datetime:
    try:
        ts = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Fecha inválida: {value}") from exc
    if ts.tzinfo is None:
        raise argparse.ArgumentTypeError("Las fechas deben incluir zona horaria (ej: 2024-01-01T00:00:00+00:00)")
    return ts.astimezone(dt.timezone.utc)


def to_ms(ts: dt.datetime) -> int:
    return int(ts.timestamp() * 1000)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill histórico (solo descarga, sin escritura)")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--symbol", required=True, help="Símbolo (ej: BTCUSDT)")
    parser.add_argument("--start", required=True, type=parse_iso_utc, help="Inicio (ISO UTC, ej: 2024-01-01T00:00:00+00:00)")
    parser.add_argument("--end", required=True, type=parse_iso_utc, help="Fin (ISO UTC)")
    parser.add_argument("--interval", default="1m", help="Intervalo kline (default 1m)")
    parser.add_argument("--batch", type=int, default=1000, help="Límite por página (<=1000)")
    parser.add_argument("--dry-run", action="store_true", help="No persiste, sólo descarga y resume")
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
        # kline format: [open time, open, high, low, close, volume, close time, ...]
        last_close_time = batch[-1][6]
        next_start = last_close_time + 1
        if len(batch) < limit:
            break
    return out


def normalize_kline_row(symbol: str, row: list) -> MarketEvent:
    close_time = dt.datetime.fromtimestamp(row[6] / 1000, tz=dt.timezone.utc)
    price_close = float(row[4])
    volume = float(row[5])
    return MarketEvent(symbol=symbol, event_ts=close_time, price=price_close, size=volume, source="kline")


def run(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.env)
    symbol = normalize_symbol(args.symbol)
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
            "batch": args.batch,
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
    events = [normalize_kline_row(symbol, row) for row in klines]
    events.sort(key=lambda e: e.event_ts)

    expected = ((end_ms - start_ms) // interval_ms) + 1
    received = len(events)
    gaps = _count_gaps(events, interval_ms)

    if not args.dry_run:
        writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.batch, dedup=True)
        for ev in events:
            writer.add(ev)
        writer.flush()

    logger.info(
        "backfill finished",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbol": symbol,
            "rows": len(events),
            "expected": expected,
            "gaps": gaps,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval,
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


def _count_gaps(events: List[MarketEvent], interval_ms: int) -> int:
    if len(events) < 2:
        return 0
    events_sorted = sorted(events, key=lambda e: e.event_ts)
    gap_count = 0
    expected_delta = dt.timedelta(milliseconds=interval_ms)
    for prev, curr in zip(events_sorted, events_sorted[1:]):
        delta = curr.event_ts - prev.event_ts
        if delta > expected_delta:
            gap_count += 1
    return gap_count


if __name__ == "__main__":
    raise SystemExit(run())
