"""
Isolated ingestion service entrypoint.
"""

from __future__ import annotations

from typing import Optional

from app.config import AppConfig, DEFAULT_INGEST_STREAM_TYPES
from app.ingestion.pipeline import collect_events
from app.marketdata.models import IngestionEvent


def run_ingestion_service(
    *,
    cfg: AppConfig,
    logger,
    mode: str = "dry",
    max_events: int = 50,
    duration_s: Optional[float] = None,
    compute_features_after_ingest: bool = False,
    ingest_max_buffer: int = 10_000,
    ingest_dedup: bool = True,
    ingest_batch_size: int = 1,
    snapshot_enabled: bool = True,
    live_summary_logging: bool = True,
    ingest_lag_warn: float | None = None,
    ingest_buffer_warn: int | None = None,
    ingest_backpressure_policy: str = "pause",
    ingest_temporal_policy: str = "accept",
    ingest_pipeline_version: str = "v2",
    ingest_shadow_mode: bool = False,
    ingest_shadow_block_on_diff: bool = False,
    ingest_stream_types: tuple[str, ...] = DEFAULT_INGEST_STREAM_TYPES,
    production_mode: bool = False,
    allow_live_fallback: bool = False,
    error_policy: str | None = None,
) -> list[IngestionEvent]:
    return collect_events(
        mode=mode,
        cfg=cfg,
        max_events=max_events,
        duration_s=duration_s,
        logger=logger,
        compute_features_after=compute_features_after_ingest,
        max_buffer=ingest_max_buffer,
        dedup_enabled=ingest_dedup,
        batch_size=ingest_batch_size,
        snapshot_enabled=snapshot_enabled,
        summary_logging=live_summary_logging,
        lag_warn_threshold=ingest_lag_warn,
        buffer_warn_threshold=ingest_buffer_warn,
        allow_live_fallback=allow_live_fallback,
        error_policy=error_policy,
        backpressure_policy=ingest_backpressure_policy,
        temporal_policy=ingest_temporal_policy,
        pipeline_version=ingest_pipeline_version,
        shadow_mode=ingest_shadow_mode,
        shadow_block_on_diff=ingest_shadow_block_on_diff,
        stream_types=ingest_stream_types,
        production_mode=production_mode,
    )
