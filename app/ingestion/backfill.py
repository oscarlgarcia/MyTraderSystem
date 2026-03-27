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
    max_retries: int = 3,
) -> List[dict]:
    """Descarga klines paginadas hasta end_ms (exclusivo)."""
    url = f"{base_url.rstrip('/')}/api/v3/klines"
    out: List[dict] = []
    next_start = start_ms
    while next_start < end_ms:
        params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": next_start, "endTime": end_ms}
        for attempt in range(max_retries):
            resp = client.get(url, params=params, timeout=10.0)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.5 * (2 ** attempt))
                continue
            resp.raise_for_status()
            break
        else:
            resp.raise_for_status()

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
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval,
            "dry_run": args.dry_run,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
