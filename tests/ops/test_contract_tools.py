from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

import pytest

from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord, write_raw_manifest
from app.ops.dataset_promotion import (
    assert_trade_dataset_usage,
    assert_promoted_trade_dataset_usage,
    build_dataset_promotion_report,
    build_trade_dataset_usage_report,
    TradeDatasetUsageError,
)
from app.ops.replay_parity import build_replay_parity_report


def _trade_envelope(symbol: str, event_ms: int, trade_id: int, price: str) -> dict:
    return {
        "stream": f"{symbol.lower()}@trade",
        "data": {
            "s": symbol,
            "E": event_ms,
            "p": price,
            "q": "1",
            "t": trade_id,
        },
    }


def _write_trade_dataset(tmp_path: Path, *, historical_feed_kind: str = "aggregate_trade") -> tuple[Path, Path]:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    raw_base_dir = tmp_path / "raw"
    raw_sink = JsonlRawSink(raw_base_dir, env="dev")
    record = RawRecord(
        payload=_trade_envelope("BTCUSDT", int(ts.timestamp() * 1000), 1, "100"),
        venue="BINANCE",
        stream_type="trade",
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        source_id="1",
    )
    raw_sink.write(record)
    write_raw_manifest(raw_sink.path_for(record))

    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="1",
            trade_id="1",
            side="buy",
            price=100.0,
            size=1.0,
            metadata={
                "raw_run_id": record.run_id,
                "raw_ingestion_seq": str(record.ingestion_seq),
                "historical_feed_kind": historical_feed_kind,
                "instrument_catalog_version": "catalog-v1",
                "instrument_snapshot": "{}",
                "metadata_source": "venue_runtime_snapshot",
                "venue_snapshot_version": "snapshot-v1",
                "metadata_snapshot_mode": "runtime",
                "instrument_catalog_snapshot_json": "[]",
                "normalizer_version": "v1",
            },
        )
    )
    writer.flush()
    normalized_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="trade",
        symbol="BTCUSDT",
        day="2024-01-01",
    )
    return raw_base_dir, normalized_path


def test_replay_parity_report_passes_for_single_trade(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)

    report = build_replay_parity_report(
        raw_base_dir=raw_base_dir,
        normalized_path=normalized_path,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
    )

    assert report.pass_ok is True
    assert report.order_match is True
    assert report.manifest_ok is True


def test_contract_and_parity_scripts_help_run():
    for script_name in (
        "scripts/check_normalized_contract.py",
        "scripts/check_replay_parity.py",
        "scripts/build_raw_manifests.py",
        "scripts/ingestion_release_gates.py",
        "scripts/ingestion_vendor_contracts.py",
        "scripts/promote_ingestion_dataset.py",
    ):
        result = subprocess.run([sys.executable, script_name, "--help"], capture_output=True, text=True, check=False)
        assert result.returncode == 0
        assert "--help" in result.stdout or "usage:" in result.stdout.lower()


def test_dataset_promotion_requires_aggregate_trade_contract(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)

    report = build_dataset_promotion_report(
        target="backtesting",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="strict",
    )

    assert report.pass_ok is True
    assert report.contract.historical_feed_kind == "aggregate_trade"
    assert report.approved_trade_dataset_usages == ("aggregate_trade",)


def test_trade_dataset_usage_report_allows_aggregate_trade_usage(tmp_path: Path):
    _, normalized_path = _write_trade_dataset(tmp_path)

    report = build_trade_dataset_usage_report(
        normalized_path=normalized_path,
        requested_usage="aggregate_trade",
    )

    assert report.allowed is True
    assert report.historical_feed_kind == "aggregate_trade"
    assert report.allowed_usages == ("aggregate_trade",)


def test_trade_dataset_usage_rejects_raw_trade_history_for_aggregate_trade(tmp_path: Path):
    _, normalized_path = _write_trade_dataset(tmp_path)

    with pytest.raises(TradeDatasetUsageError, match="cannot be used as raw trade history"):
        assert_trade_dataset_usage(
            normalized_path=normalized_path,
            requested_usage="raw_trade_history",
        )


def test_promoted_trade_dataset_usage_rejects_raw_trade_history_for_aggregate_trade(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)
    report = build_dataset_promotion_report(
        target="backtesting",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="strict",
    )

    with pytest.raises(TradeDatasetUsageError, match="cannot be used as raw trade history"):
        assert_promoted_trade_dataset_usage(
            report,
            requested_usage="raw_trade_history",
        )
