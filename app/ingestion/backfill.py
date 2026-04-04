"""
Historical backfill for Binance `kline` and `trade` feeds.

Usage:
    python -m app.ingestion.backfill --env dev --symbol BTCUSDT --feed-type kline --start 2024-01-01T00:00:00+00:00 --end 2024-01-02T00:00:00+00:00 --interval 1m --batch 1000 --dry-run
    python -m app.ingestion.backfill --env dev --symbol BTCUSDT --feed-type trade --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T00:15:00+00:00 --batch 1000 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from typing import Any, List, Optional

import httpx

from app.common.dto import normalize_symbol
from app.config import load_config
from app.ingestion.client import normalize_kline_typed, normalize_trade_typed
from app.ingestion.dedup import Deduplicator, deduplicate_events as deduplicate_market_events
from app.ingestion.sinks import EventSink, JsonlErrorSink, ParquetEventSink
from app.ingestion.storage import ParquetWriter
from app.marketdata.anomaly_checks import dominant_anomaly, detect_marketdata_anomalies, event_volume_value, stream_price_key
from app.marketdata.errors import MarketdataAnomalyError
from app.marketdata.instruments import persist_runtime_instrument_catalog_snapshot, use_instrument_catalog
from app.marketdata.models import BarEvent, IngestionEvent, TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord, RawSink
from app.observability.alerts import emit_operational_alert
from app.observability.logger import get_logger, set_trace_id


SUPPORTED_HISTORICAL_BACKFILL_FEEDS = ("kline", "trade")
HISTORICAL_BACKFILL_SCOPE = "bars-and-trades"
HISTORICAL_TRADE_FEED_KIND = "aggregate_trade"
HISTORICAL_TRADE_ENDPOINT = "aggTrades"


def _marketdata_anomaly_quarantine_path(data_dir: Any) -> Any:
    return data_dir / "errors" / "marketdata-anomaly-quarantine.jsonl"


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
        description=(
            "Backfill historico soportado para `kline` y `trade`: escribe raw + normalized o solo descarga con --dry-run"
        ),
        epilog=(
            "Trade historical backfill usa Binance REST `aggTrades` y queda tipado como `aggregate_trade`; "
            "no reclama raw trade history y live `trade` sigue fuera del scope live."
        ),
    )
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None, help="Config environment")
    parser.add_argument("--symbol", required=True, help="Simbolo (ej: BTCUSDT)")
    parser.add_argument(
        "--feed-type",
        choices=SUPPORTED_HISTORICAL_BACKFILL_FEEDS,
        default="kline",
        help="Feed historico a descargar: `kline` o `trade`.",
    )
    parser.add_argument("--start", required=True, type=parse_iso_utc, help="Inicio (ISO UTC, ej: 2024-01-01T00:00:00+00:00)")
    parser.add_argument("--end", required=True, type=parse_iso_utc, help="Fin (ISO UTC)")
    parser.add_argument(
        "--interval",
        default="1m",
        help="Intervalo kline (default 1m). Solo aplica a `--feed-type kline`.",
    )
    parser.add_argument("--batch", type=int, default=1000, help="Limite por pagina (<=1000)")
    parser.add_argument("--dedup", action="store_true", help="Deduplica eventos con la clave compartida de ingestion")
    parser.add_argument("--dry-run", action="store_true", help="No persiste, solo descarga y resume")
    return parser.parse_args(argv)


def _request_json_with_retries(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any],
    timeout: float,
    retries_429: int,
    retries_5xx: int,
) -> Any:
    attempt_429 = 0
    attempt_5xx = 0
    while True:
        try:
            resp = client.get(url, params=params, timeout=timeout)
        except httpx.TimeoutException as exc:
            attempt_5xx += 1
            if attempt_5xx > retries_5xx:
                raise httpx.HTTPStatusError(
                    f"Timeout agotado para {url} tras {retries_5xx} reintentos",
                    request=None,
                    response=None,
                ) from exc
            time.sleep(0.5 * attempt_5xx)
            continue

        if resp.status_code == 429:
            attempt_429 += 1
            if attempt_429 > retries_429:
                raise httpx.HTTPStatusError(
                    f"Rate limit alcanzado ({resp.status_code}) tras {retries_429} reintentos para {url}",
                    request=getattr(resp, "request", None),
                    response=resp,
                )
            time.sleep(0.5 * attempt_429)
            continue

        if resp.status_code >= 500:
            attempt_5xx += 1
            if attempt_5xx > retries_5xx:
                raise httpx.HTTPStatusError(
                    f"Error servidor {resp.status_code} tras {retries_5xx} reintentos para {url}",
                    request=getattr(resp, "request", None),
                    response=resp,
                )
            time.sleep(0.5 * attempt_5xx)
            continue

        resp.raise_for_status()
        return resp.json()


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
        batch = _request_json_with_retries(
            client,
            url,
            params={"symbol": symbol, "interval": interval, "limit": limit, "startTime": next_start, "endTime": end_ms},
            timeout=timeout,
            retries_429=retries_429,
            retries_5xx=retries_5xx,
        )
        if not batch:
            break
        out.extend(batch)
        last_close_time = batch[-1][6]
        next_start = last_close_time + 1
        if len(batch) < limit:
            break
    return out


def fetch_trades(
    client: httpx.Client,
    base_url: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
    retries_429: int = 3,
    retries_5xx: int = 2,
    timeout: float = 10.0,
) -> List[dict[str, Any]]:
    """Descarga aggregate trades de Binance en orden estable por `a` hasta end_ms (exclusivo)."""
    url = f"{base_url.rstrip('/')}/api/v3/aggTrades"
    out: List[dict[str, Any]] = []
    next_from_id: int | None = None
    first_request = True

    while True:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if first_request:
            params["startTime"] = start_ms
            params["endTime"] = end_ms
        else:
            params["fromId"] = next_from_id

        batch = _request_json_with_retries(
            client,
            url,
            params=params,
            timeout=timeout,
            retries_429=retries_429,
            retries_5xx=retries_5xx,
        )
        if not batch:
            break

        stop = False
        for row in batch:
            trade_ts = int(row["T"])
            if trade_ts < start_ms:
                continue
            if trade_ts >= end_ms:
                stop = True
                break
            out.append(row)

        next_from_id = int(batch[-1]["a"]) + 1
        first_request = False
        if stop or len(batch) < limit:
            break
    return out


def _kline_payload_from_row(symbol: str, row: list, *, interval: str) -> dict[str, Any]:
    close = str(row[4])
    if len(row) > 7 and row[7] not in ("", None):
        quote_volume = str(row[7])
    else:
        quote_volume = str(row[5])
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
            "q": quote_volume,
            "i": interval,
        },
    }


def _trade_payload_from_row(symbol: str, row: dict[str, Any]) -> dict[str, Any]:
    aggregate_trade_id = int(row["a"])
    return {
        "e": "trade",
        "s": symbol,
        "E": int(row["T"]),
        "p": str(row["p"]),
        "q": str(row["q"]),
        "t": aggregate_trade_id,
        "m": bool(row.get("m")) if row.get("m") is not None else None,
        "M": bool(row.get("M")) if row.get("M") is not None else None,
        "a": aggregate_trade_id,
        "f": int(row["f"]) if row.get("f") is not None else None,
        "l": int(row["l"]) if row.get("l") is not None else None,
        "_backfill_endpoint": HISTORICAL_TRADE_ENDPOINT,
        "_historical_trade_kind": HISTORICAL_TRADE_FEED_KIND,
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


def normalize_trade_row(
    symbol: str,
    row: dict[str, Any],
    *,
    receive_ts: dt.datetime | None = None,
    process_ts: dt.datetime | None = None,
    venue: str = "BINANCE",
) -> TradeEvent:
    payload = _trade_payload_from_row(symbol, row)
    return normalize_trade_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
    )


def raw_record_from_kline_row(
    symbol: str,
    row: list,
    *,
    interval: str,
    receive_ts: dt.datetime,
    process_ts: dt.datetime | None,
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
        process_ts=process_ts,
        trace_id=trace_id,
        source_id=str(row[0]),
    )


def raw_record_from_trade_row(
    symbol: str,
    row: dict[str, Any],
    *,
    receive_ts: dt.datetime,
    process_ts: dt.datetime | None,
    trace_id: str | None,
    venue: str = "BINANCE",
) -> RawRecord:
    payload = _trade_payload_from_row(symbol, row)
    return RawRecord(
        payload=payload,
        venue=venue,
        stream_type="trade",
        symbol=symbol,
        exchange_ts=dt.datetime.fromtimestamp(int(row["T"]) / 1000, tz=dt.timezone.utc),
        receive_ts=receive_ts,
        process_ts=process_ts,
        trace_id=trace_id,
        source_id=str(row["a"]),
    )


def deduplicate_events(events: List[IngestionEvent]) -> tuple[List[IngestionEvent], int]:
    return deduplicate_market_events(
        events,
        deduplicator=Deduplicator(ttl_seconds=None, max_entries=max(len(events), 4096)),
    )


def _event_sort_key(event: IngestionEvent) -> tuple[Any, ...]:
    native_id = getattr(event, "trade_id", None) or getattr(event, "source_id", None) or ""
    return (event.event_ts, str(native_id), event.symbol, event.source)


def _raw_sort_key(record: RawRecord) -> tuple[Any, ...]:
    return (record.exchange_ts, str(record.source_id or ""), record.symbol, record.stream_type)


def _stamp_event_with_raw_lineage(event: IngestionEvent, record: RawRecord) -> None:
    metadata = getattr(event, "metadata", None)
    if not isinstance(metadata, dict):
        return
    if record.provider_ts is not None:
        metadata.setdefault("provider_ts", record.provider_ts.isoformat())
    if record.run_id is not None:
        metadata["raw_run_id"] = str(record.run_id)
    if record.ingestion_seq is not None:
        metadata["raw_ingestion_seq"] = str(record.ingestion_seq)
    if record.stream_type == "trade":
        metadata.setdefault("historical_feed_kind", HISTORICAL_TRADE_FEED_KIND)


def _stamp_event_with_catalog_state(event: IngestionEvent, catalog_state) -> None:
    metadata = getattr(event, "metadata", None)
    if not isinstance(metadata, dict):
        return
    venue = str(getattr(event, "venue", "BINANCE"))
    metadata.update(
        {
            key: value
            for key, value in catalog_state.instrument_metadata(event.symbol, venue=venue).items()
            if key not in metadata
        }
    )


def run(argv: Optional[list[str]] = None, sink: Optional[EventSink] = None, raw_sink: Optional[RawSink] = None) -> int:
    args = parse_args(argv)
    assert_historical_backfill_support(args.feed_type)
    cfg = load_config(args.env)
    symbol = normalize_symbol(args.symbol)
    start_ms = to_ms(args.start)
    end_ms = to_ms(args.end)
    interval_ms = _interval_to_ms(args.interval) if args.feed_type == "kline" else None

    trace_id = f"backfill-{int(time.time())}"
    logger = get_logger(name="backfill", level=cfg.log_level)
    set_trace_id(trace_id)
    catalog_state = persist_runtime_instrument_catalog_snapshot(
        base_dir=cfg.data_dir,
        env=cfg.env,
        venue="BINANCE",
        run_label=trace_id,
        rest_base=cfg.rest_base,
        symbols=[symbol],
    )

    logger.info(
        "backfill starting",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbol": symbol,
            "feed_type": args.feed_type,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval if args.feed_type == "kline" else None,
            "historical_scope": HISTORICAL_BACKFILL_SCOPE,
            "supported_historical_feeds": list(SUPPORTED_HISTORICAL_BACKFILL_FEEDS),
            "batch": args.batch,
            "dedup": args.dedup,
            "dry_run": args.dry_run,
            "instrument_catalog_version": catalog_state.instrument_catalog_version,
            "instrument_catalog_snapshot_path": str(catalog_state.path),
            "metadata_snapshot_mode": catalog_state.metadata_snapshot_mode,
            "venue_snapshot_path": str(catalog_state.venue_snapshot_path) if catalog_state.venue_snapshot_path else None,
        },
    )
    if catalog_state.drift is not None and catalog_state.drift.has_drift:
        emit_operational_alert(
            logger,
            alert_type="provider_metadata_drift",
            observed=1,
            extra={
                "trace_id": trace_id,
                "env": cfg.env,
                "venue": "BINANCE",
                "run_mode": "backfill",
                "drift_mode": "material" if catalog_state.drift.material else "informational",
                "drift_added_symbols": list(catalog_state.drift.added_symbols),
                "drift_removed_symbols": list(catalog_state.drift.removed_symbols),
                "drift_changed_symbols": list(catalog_state.drift.changed_symbols),
                "drift_changed_fields_by_symbol": {
                    changed_symbol: list(fields)
                    for changed_symbol, fields in catalog_state.drift.changed_fields_by_symbol.items()
                },
                "instrument_catalog_version": catalog_state.instrument_catalog_version,
                "instrument_catalog_snapshot_path": str(catalog_state.path),
                "metadata_snapshot_mode": catalog_state.metadata_snapshot_mode,
                "venue_snapshot_path": str(catalog_state.venue_snapshot_path) if catalog_state.venue_snapshot_path else None,
                "fallback_reason": catalog_state.fallback_reason,
            },
        )

    with httpx.Client() as client:
        if args.feed_type == "kline":
            rows = fetch_klines(
                client=client,
                base_url=cfg.rest_base,
                symbol=symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                interval=args.interval,
                limit=args.batch,
            )
        else:
            rows = fetch_trades(
                client=client,
                base_url=cfg.rest_base,
                symbol=symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=args.batch,
            )

    receive_ts = dt.datetime.now(dt.timezone.utc)
    process_ts = receive_ts
    with use_instrument_catalog(catalog_state.catalog):
        if args.feed_type == "kline":
            assert interval_ms is not None
            raw_records = [
                raw_record_from_kline_row(
                    symbol,
                    row,
                    interval=args.interval,
                    receive_ts=receive_ts,
                    process_ts=process_ts,
                    trace_id=trace_id,
                )
                for row in rows
            ]
            events = [
                normalize_kline_row(
                    symbol,
                    row,
                    interval=args.interval,
                    receive_ts=receive_ts,
                    process_ts=process_ts,
                )
                for row in rows
            ]
            expected = ((end_ms - start_ms) // interval_ms) + 1
            gaps = _count_bar_gaps([event for event in events if isinstance(event, BarEvent)], interval_ms)
        else:
            raw_records = [
                raw_record_from_trade_row(
                    symbol,
                    row,
                    receive_ts=receive_ts,
                    process_ts=process_ts,
                    trace_id=trace_id,
                )
                for row in rows
            ]
            events = [
                normalize_trade_row(
                    symbol,
                    row,
                    receive_ts=receive_ts,
                    process_ts=process_ts,
                )
                for row in rows
            ]
            expected = len(events)
            gaps = None

    events.sort(key=_event_sort_key)
    raw_records.sort(key=_raw_sort_key)
    event_record_pairs = list(zip(events, raw_records, strict=True))
    for event, _record in event_record_pairs:
        _stamp_event_with_catalog_state(event, catalog_state)

    error_sink_impl = None
    if not args.dry_run:
        raw_sink_impl = raw_sink or JsonlRawSink(base_dir=cfg.data_dir / "raw", env=cfg.env)
        error_sink_impl = JsonlErrorSink(
            cfg.data_dir / "errors" / "ingestion-dlq.jsonl",
            schema_drift_path=cfg.data_dir / "errors" / "schema-drift-quarantine.jsonl",
        )
        sink_impl = sink or ParquetEventSink(
            ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.batch, dedup=args.dedup)
        )
        for record in raw_records:
            raw_sink_impl.write(record)
        for event, record in event_record_pairs:
            _stamp_event_with_raw_lineage(event, record)

    duplicates_dropped = 0
    if args.dedup:
        deduped_events, duplicates_dropped = deduplicate_events(events)
        retained_event_ids = {id(event) for event in deduped_events}
        event_record_pairs = [
            (event, record)
            for event, record in event_record_pairs
            if id(event) in retained_event_ids
        ]
        events = deduped_events
        if duplicates_dropped:
            logger.info(
                "backfill duplicates dropped",
                extra={
                    "trace_id": trace_id,
                    "symbol": symbol,
                    "feed_type": args.feed_type,
                    "duplicates_dropped": duplicates_dropped,
                },
            )

    previous_price: float | None = None
    previous_volume: float | None = None
    anomalies_detected = 0
    accepted_events: list[IngestionEvent] = []
    for event, record in event_record_pairs:
        anomalies = detect_marketdata_anomalies(
            event=event,
            previous_price=previous_price,
            previous_volume=previous_volume,
        )
        if not anomalies:
            previous_price = float(event.price)
            current_volume = event_volume_value(event)
            if current_volume is not None:
                previous_volume = current_volume
            accepted_events.append(event)
            continue
        for anomaly in anomalies:
            anomalies_detected += 1
            emit_operational_alert(
                logger,
                alert_type="marketdata_anomaly_detected",
                observed=anomalies_detected,
                extra={
                    "trace_id": trace_id,
                    "env": cfg.env,
                    "symbol": event.symbol,
                    "feed_type": event.source,
                    "anomaly_type": anomaly.anomaly_type,
                    "anomaly_severity": anomaly.severity,
                    "anomaly_action": anomaly.action,
                    "previous_price": anomaly.previous_price,
                    "current_price": anomaly.current_price,
                    "relative_jump": anomaly.relative_jump,
                    "previous_volume": anomaly.previous_volume,
                    "current_volume": anomaly.current_volume,
                    "volume_ratio": anomaly.volume_ratio,
                    "threshold": anomaly.threshold,
                },
            )
        dominant = dominant_anomaly(anomalies)
        if dominant is None or dominant.action == "warn":
            previous_price = float(event.price)
            current_volume = event_volume_value(event)
            if current_volume is not None:
                previous_volume = current_volume
            accepted_events.append(event)
            continue
        stream_key = stream_price_key(event)
        anomaly_error = MarketdataAnomalyError(
            stream_key=stream_key,
            venue=getattr(event, "venue", "BINANCE"),
            symbol=event.symbol,
            stream_type=event.source,
            anomaly_type=dominant.anomaly_type,
            anomaly_severity=dominant.severity,
            anomaly_action=dominant.action,
            previous_price=dominant.previous_price,
            current_price=dominant.current_price,
            relative_jump=dominant.relative_jump,
            previous_volume=dominant.previous_volume,
            current_volume=dominant.current_volume,
            volume_ratio=dominant.volume_ratio,
            threshold=dominant.threshold,
        )
        if error_sink_impl is not None:
            error_sink_impl.write(
                record.payload,
                anomaly_error,
                context={
                    "trace_id": trace_id,
                    "env": cfg.env,
                    "symbol": event.symbol,
                    "stream_type": event.source,
                    "stage": "backfill",
                    "quarantine_reason": "marketdata_anomaly",
                    "quarantine_path": str(_marketdata_anomaly_quarantine_path(cfg.data_dir)),
                    **anomaly_error.as_context(),
                },
            )
        if dominant.action == "fail":
            raise anomaly_error
    events = accepted_events

    if not args.dry_run:
        for event in events:
            sink_impl.add(event)
        sink_impl.close()

    logger.info(
        "backfill finished",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbol": symbol,
            "feed_type": args.feed_type,
            "rows": len(events),
            "expected": expected,
            "gaps": gaps,
            "dedup": args.dedup,
            "duplicates_dropped": duplicates_dropped,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "interval": args.interval if args.feed_type == "kline" else None,
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


def _count_bar_gaps(events: List[BarEvent], interval_ms: int) -> int:
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
