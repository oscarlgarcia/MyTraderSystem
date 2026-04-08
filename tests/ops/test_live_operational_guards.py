import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import httpx
import pytest

from app import main
from app.ingestion.circuit_breaker import CircuitBreaker
from app.ingestion import pipeline
from app.ingestion.errors import IngestionError
from app.ingestion.resilience import ResilientRunner
from app.ingestion.shadow import ShadowComparison, ShadowPartitionSnapshot, ShadowPromotionError, ShadowSnapshot
from app.ingestion.sources import StaticSource
from app.marketdata.instruments import DEFAULT_INSTRUMENTS, Instrument, InstrumentCatalog, persist_instrument_catalog_snapshot, resolve_instrument
from app.marketdata.connectors.binance_sources import BinanceBarSource
from app.marketdata.models import BarEvent
from app.observability.logger import get_logger


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


def _cfg(tmp_path: Path):
    return mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )


def _bar_event() -> BarEvent:
    return BarEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        receive_ts=datetime(2024, 1, 1, 0, 1, 1, tzinfo=timezone.utc),
        process_ts=datetime(2024, 1, 1, 0, 1, 2, tzinfo=timezone.utc),
        venue="BINANCE",
        source_id=str(int(datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
        interval="1m",
        open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        close_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
    )


def _bar_at(ts: datetime) -> BarEvent:
    return BarEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=str(int((ts - timedelta(minutes=1)).timestamp() * 1000)),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
        interval="1m",
        open_ts=ts - timedelta(minutes=1),
        close_ts=ts,
    )


def test_operational_snapshot_breaker_opens_and_fails_fast_after_retry_exhaustion(tmp_path: Path):
    cfg = _cfg(tmp_path)
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)
    attempts = {"n": 0}

    def fake_http_get(url: str, **kwargs):
        del kwargs
        attempts["n"] += 1
        request = httpx.Request("GET", url)
        return httpx.Response(503, request=request)

    source = BinanceBarSource(
        cfg=cfg,
        http_get=fake_http_get,
        snapshot_sleeper=lambda _seconds: None,
        snapshot_retries_5xx=0,
        snapshot_breaker=CircuitBreaker(
            failure_threshold=1,
            reset_timeout_seconds=60.0,
            monotonic_fn=lambda: 0.0,
        ),
    )

    with pytest.raises(IngestionError):
        list(source.snapshot())
    with pytest.raises(IngestionError):
        list(source.snapshot())

    assert attempts["n"] == 1
    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    retry_alerts = [record for record in alerts if record["alert_type"] == "snapshot_retry_exhausted"]
    assert len(retry_alerts) >= 2
    assert any(record.get("reason") == "circuit_breaker_open" for record in retry_alerts)


def test_operational_shadow_diff_blocks_promotion(tmp_path: Path, monkeypatch):
    cfg = _cfg(tmp_path)
    buffer = io.StringIO()
    logger = get_logger(name="test.ops.shadow", level="INFO", stream=buffer)

    def fake_compare(primary, shadow):
        del primary, shadow
        return ShadowComparison(
            primary=ShadowSnapshot(
                pipeline_version="v2",
                row_count=2,
                identity_count=2,
                identity_checksum="a",
                row_checksum="a",
                partitions={
                    "bars:BINANCE:BTCUSDT:2024-01-01": ShadowPartitionSnapshot(
                        row_count=2,
                        identity_count=2,
                        identity_checksum="a",
                        row_checksum="a",
                        min_event_ts="2024-01-01T00:00:00+00:00",
                        max_event_ts="2024-01-01T00:01:00+00:00",
                    )
                },
                min_event_ts="2024-01-01T00:00:00+00:00",
                max_event_ts="2024-01-01T00:01:00+00:00",
                gaps_total=0,
                processing_latency_seconds=0.1,
                write_latency_seconds=0.1,
            ),
            shadow=ShadowSnapshot(
                pipeline_version="v1",
                row_count=1,
                identity_count=1,
                identity_checksum="b",
                row_checksum="b",
                partitions={},
                min_event_ts="2024-01-01T00:00:00+00:00",
                max_event_ts="2024-01-01T00:00:00+00:00",
                gaps_total=0,
                processing_latency_seconds=0.1,
                write_latency_seconds=0.1,
            ),
            diffs={
                "row_count": 1,
                "identity_count": 1,
                "identity_checksum_match": False,
                "row_checksum_match": False,
                "partition_row_count_diffs": {"bars:BINANCE:BTCUSDT:2024-01-01": 1},
                "partition_identity_count_diffs": {"bars:BINANCE:BTCUSDT:2024-01-01": 1},
                "partition_checksum_mismatches": ["bars:BINANCE:BTCUSDT:2024-01-01"],
                "partition_timestamp_mismatches": [],
                "min_event_ts_match": True,
                "max_event_ts_match": False,
                "gaps_total": 0.0,
                "processing_latency_seconds": 0.0,
                "write_latency_seconds": 0.0,
            },
            significant=True,
        )

    monkeypatch.setattr(pipeline, "compare_shadow_snapshots", fake_compare)

    with pytest.raises(ShadowPromotionError) as exc_info:
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            max_events=10,
            duration_s=0,
            logger=logger,
            dedup_enabled=True,
            snapshot_enabled=False,
            summary_logging=True,
            source=StaticSource(events=[_bar_event()]),
            shadow_mode=True,
            shadow_block_on_diff=True,
            pipeline_version="v2",
            stream_types=("kline",),
        )
    assert exc_info.value.error_type == "ShadowPromotionError"

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    shadow_alert = next(record for record in alerts if record["alert_type"] == "shadow_semantic_diff")
    assert shadow_alert["shadow_row_diff_total"] == 1
    assert shadow_alert["shadow_checksum_diff_total"] >= 1


def test_operational_production_gating_accepts_live_trade_with_runtime_metadata(tmp_path: Path):
    cfg = _cfg(tmp_path)
    cfg.env = "prod"
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("trade",),
    }
    metadata_path = tmp_path / "metadata" / "instruments" / "env=prod" / "venue=BINANCE" / "latest.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(
            {
                "metadata_snapshot_mode": "runtime",
                "drift": {"material": False},
            }
        ),
        encoding="utf-8",
    )

    main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_operational_incomplete_recovery_emits_exactness_violation_and_degrades_stream(tmp_path: Path):
    del tmp_path
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    buffer = io.StringIO()
    get_logger(name="ingest.resilience", level="INFO", stream=buffer)

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar_at(base),
            _bar_at(base + timedelta(minutes=12)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _bar_at(base + timedelta(minutes=1)),
            _bar_at(base + timedelta(minutes=12)),
        ],
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda _event: None, stop_on_complete=True)

    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert runner.metrics.recovery_exactness_violation_total == 1
    assert stream_metrics["gap_irreparable"] is True
    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    recovery_alert = next(record for record in alerts if record["alert_type"] == "recovery_exactness_violation")
    assert recovery_alert["error_type"] == "RecoveryExactnessError"
    assert recovery_alert["requested_rows"] == 13
    assert recovery_alert["received_rows"] == 2


def test_operational_provider_metadata_drift_alerts_when_authoritative_snapshot_changes(tmp_path: Path):
    cfg = _cfg(tmp_path)
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)
    btc = resolve_instrument("BTCUSDT", venue="BINANCE")
    previous_catalog = InstrumentCatalog(
        [
            Instrument(
                venue=btc.venue,
                symbol=btc.symbol,
                base_asset=btc.base_asset,
                quote_asset=btc.quote_asset,
                contract_type=btc.contract_type,
                tick_size="0.10000000",
                step_size=btc.step_size,
                price_precision=1,
                size_precision=btc.size_precision,
                metadata_source=btc.metadata_source,
                venue_snapshot_version="older-snapshot",
            ),
            *[instrument for instrument in DEFAULT_INSTRUMENTS if instrument.symbol != "BTCUSDT"],
        ]
    )
    persist_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="previous-source",
        catalog=previous_catalog,
    )

    source = BinanceBarSource(cfg=cfg)
    assert source is not None

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    drift_alert = next(record for record in alerts if record["alert_type"] == "provider_metadata_drift")
    assert drift_alert["drift_mode"] == "material"
    assert "BTCUSDT" in drift_alert["drift_changed_symbols"]
    assert "tick_size" in drift_alert["drift_changed_fields_by_symbol"]["BTCUSDT"]
