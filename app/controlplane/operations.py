from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import AppConfig
from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.ingestion.sources import BinanceSource
from app.marketdata.benchmarks import benchmark_query_and_serving
from app.marketdata.capabilities import build_venue_capability_registry, capability_registry_path, write_venue_capability_registry
from app.marketdata.dataset_catalog import dataset_catalog_path, refresh_dataset_catalog
from app.marketdata.dataset_quality import (
    append_dataset_incidents,
    build_dataset_quality_registry,
    dataset_incident_log_path,
    dataset_quality_registry_path,
    write_dataset_quality_registry,
)
from app.marketdata.delivery import (
    build_delivery_contract_registry,
    delivery_contract_registry_path,
    write_delivery_contract_registry,
)
from app.marketdata.future_scope import build_future_scope_registry, future_scope_registry_path, write_future_scope_registry
from app.marketdata.publication import PublicationRecord, publish_record
from app.marketdata.recovery import RecoveryRequest, verify_recovery_window
from app.marketdata.replay import ReplaySource
from app.marketdata.security import (
    build_security_baseline_report,
    security_baseline_report_path,
    write_security_baseline_report,
)
from app.marketdata.serving import refresh_curated_store
from app.marketdata.snapshot_service import SnapshotRequest, load_snapshot
from app.marketdata.storage_lifecycle import (
    build_storage_lifecycle_report,
    storage_lifecycle_report_path,
    write_storage_lifecycle_report,
)
from app.marketdata.subscriptions import update_subscription_config
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


def execute_refresh_dataset_catalog(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    del payload
    catalog = refresh_dataset_catalog(cfg.data_dir, cfg.env)
    capability_registry = build_venue_capability_registry()
    delivery_registry = build_delivery_contract_registry(env=cfg.env)
    lifecycle = build_storage_lifecycle_report(cfg.data_dir, cfg.env)
    security = build_security_baseline_report(cfg.data_dir, cfg.env)
    future_scope = build_future_scope_registry()
    write_venue_capability_registry(capability_registry_path(cfg.data_dir, cfg.env), capability_registry)
    write_delivery_contract_registry(delivery_contract_registry_path(cfg.data_dir, cfg.env), delivery_registry)
    write_storage_lifecycle_report(storage_lifecycle_report_path(cfg.data_dir, cfg.env), lifecycle)
    write_security_baseline_report(security_baseline_report_path(cfg.data_dir, cfg.env), security)
    write_future_scope_registry(future_scope_registry_path(cfg.data_dir, cfg.env), future_scope)
    return {
        "catalog_path": str(dataset_catalog_path(cfg.data_dir, cfg.env)),
        "dataset_count": len(catalog.entries),
        "capability_entries": len(capability_registry.entries),
        "delivery_contracts": len(delivery_registry.contracts),
        "storage_lifecycle_entries": len(lifecycle.entries),
        "security_baseline_path": str(security_baseline_report_path(cfg.data_dir, cfg.env)),
        "future_scope_entries": len(future_scope.entries),
    }


def execute_score_dataset_quality(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    del payload
    from app.ingestion.storage import list_normalized_partition_paths

    paths = list_normalized_partition_paths(cfg.data_dir, cfg.env)
    quality = build_dataset_quality_registry(paths)
    write_dataset_quality_registry(dataset_quality_registry_path(cfg.data_dir, cfg.env), quality)
    append_dataset_incidents(dataset_incident_log_path(cfg.data_dir, cfg.env), quality.reports)
    failed = sum(1 for item in quality.reports if item.status == "failed")
    degraded = sum(1 for item in quality.reports if item.status == "degraded")
    return {
        "quality_registry_path": str(dataset_quality_registry_path(cfg.data_dir, cfg.env)),
        "reports": len(quality.reports),
        "failed": failed,
        "degraded": degraded,
    }


def execute_refresh_curated(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    stream_type = str(payload.get("stream_type", "kline"))
    symbol = str(payload["symbol"]).upper() if payload.get("symbol") else None
    venue = str(payload.get("venue", "BINANCE")).upper()
    report = refresh_curated_store(base_dir=cfg.data_dir, env=cfg.env, stream_type=stream_type, venue=venue, symbol=symbol)
    return asdict(report)


def execute_update_subscriptions(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    symbols = tuple(str(item).upper() for item in payload.get("symbols", cfg.symbols))
    stream_types = tuple(str(item).lower() for item in payload.get("stream_types", ("trade", "kline")))
    updated_by = str(payload.get("updated_by", "control-plane"))
    config = update_subscription_config(
        base_dir=cfg.data_dir,
        env=cfg.env,
        symbols=symbols,
        stream_types=stream_types,
        updated_by=updated_by,
        venue=str(payload.get("venue", "BINANCE")).upper(),
    )
    return asdict(config)


def execute_benchmark_serving(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    stream_type = str(payload.get("stream_type", "kline"))
    symbol = str(payload["symbol"]).upper()
    report = benchmark_query_and_serving(base_dir=cfg.data_dir, env=cfg.env, stream_type=stream_type, symbol=symbol)
    return asdict(report)


def execute_publish_snapshot(*, cfg: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    stream_type = str(payload.get("stream_type", "kline"))
    symbol = str(payload["symbol"]).upper()
    snapshot = load_snapshot(
        SnapshotRequest(
            base_dir=cfg.data_dir,
            env=cfg.env,
            stream_type=stream_type,
            symbol=symbol,
        )
    )
    if snapshot is None:
        raise ValueError(f"snapshot not found for {stream_type}:{symbol}")
    record = PublicationRecord(
        env=cfg.env,
        venue=str(snapshot.get("venue", "BINANCE")).upper(),
        stream_type=stream_type,
        symbol=symbol,
        published_at=_now(),
        payload=snapshot,
    )
    path = publish_record(cfg.data_dir, record)
    return {"publication_path": str(path), "symbol": symbol, "stream_type": stream_type}
