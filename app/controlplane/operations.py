from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import AppConfig
from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.ingestion.sources import BinanceSource
from app.marketdata.recovery import RecoveryRequest, verify_recovery_window
from app.marketdata.replay import ReplaySource
from app.marketdata.temporal_state import TemporalPartitionKey
from app.ops.quarantine_cli import replay_quarantine_records


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def execute_ack_alert(*, store, payload: dict[str, Any], requested_by: str) -> dict[str, Any]:
    alert_id = str(payload["alert_id"])
    changed = store.ack_alert(alert_id, acked_by=requested_by, acked_at=_now())
    return {
        "alert_id": alert_id,
        "acknowledged": changed,
    }


def execute_replay_range(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("mode") == "quarantine":
        report = replay_quarantine_records(
            base_dir=cfg.data_dir,
            env=cfg.env,
            trace_id=payload.get("trace_id"),
            symbol=payload.get("symbol"),
            stream_type=payload.get("stream_type"),
            record_id=payload.get("record_id"),
            limit=int(payload["limit"]) if payload.get("limit") not in (None, "") else None,
            write_normalized=bool(payload.get("write_normalized", False)),
            report_path=Path(payload["report_path"]) if payload.get("report_path") else None,
        )
        return asdict(report)

    symbol = str(payload["symbol"]).upper()
    stream_type = str(payload.get("stream_type", "trade"))
    start_ts = datetime.fromisoformat(str(payload["start_ts"])) if payload.get("start_ts") else None
    end_ts = datetime.fromisoformat(str(payload["end_ts"])) if payload.get("end_ts") else None
    source = ReplaySource(
        base_dir=Path(cfg.data_dir) / "raw",
        env=cfg.env,
        stream_types=(stream_type,),
        symbol=symbol,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, dedup=True)
    touched: set[str] = set()
    replayed = 0
    for event in source.stream():
        writer.add(event)
        replayed += 1
        touched.add(
            str(
                normalized_partition_path(
                    cfg.data_dir,
                    cfg.env,
                    source=event.source,
                    symbol=event.symbol,
                    day=event.event_ts.date().isoformat(),
                    venue=getattr(event, "venue", "BINANCE"),
                )
            )
        )
    writer.flush()
    return {
        "symbol": symbol,
        "stream_type": stream_type,
        "start_ts": payload.get("start_ts"),
        "end_ts": payload.get("end_ts"),
        "replayed_events": replayed,
        "persisted_events": writer.persisted_events,
        "touched_partitions": sorted(touched),
    }


def execute_resync_stream(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload["symbol"]).upper()
    stream_type = str(payload.get("stream_type", "kline"))
    interval = str(payload.get("interval", "1m"))
    start_ts = datetime.fromisoformat(str(payload["start_ts"])) if payload.get("start_ts") else None
    end_ts = datetime.fromisoformat(str(payload["end_ts"])) if payload.get("end_ts") else None
    limit = int(payload["limit"]) if payload.get("limit") not in (None, "") else None
    request = RecoveryRequest(
        partition=TemporalPartitionKey(venue="BINANCE", symbol=symbol, stream_type=stream_type),
        start_ts=start_ts,
        end_ts=end_ts,
        interval=interval,
        limit=limit,
        reason="control_plane_resync",
    )
    scoped_cfg = replace(cfg, symbols=[symbol])
    source = BinanceSource(scoped_cfg, stream_types=(stream_type,))
    recovered = list(source.snapshot(request=request))
    verification = verify_recovery_window(partition=request.partition, request=request, recovered_events=recovered)
    writer = None
    touched: set[str] = set()
    if payload.get("write_normalized", True):
        writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, dedup=True)
        for event in recovered:
            writer.add(event)
            touched.add(
                str(
                    normalized_partition_path(
                        cfg.data_dir,
                        cfg.env,
                        source=event.source,
                        symbol=event.symbol,
                        day=event.event_ts.date().isoformat(),
                        venue=getattr(event, "venue", "BINANCE"),
                    )
                )
            )
        writer.flush()
    return {
        "symbol": symbol,
        "stream_type": stream_type,
        "interval": interval,
        "start_ts": payload.get("start_ts"),
        "end_ts": payload.get("end_ts"),
        "requested_limit": limit,
        "received_rows": len(recovered),
        "persisted_events": writer.persisted_events if writer is not None else 0,
        "touched_partitions": sorted(touched),
        "verification": asdict(verification),
    }
