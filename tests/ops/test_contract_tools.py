import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pytest

from app.ingestion.storage import ParquetWriter, normalized_partition_path, partition_segments_dir, read_parquet
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord, write_raw_manifest
from app.ops.dataset_promotion import (
    DatasetPromotionApprovalError,
    TradeDatasetUsageError,
    assert_dataset_is_registered_as_approved,
    assert_promoted_trade_dataset_usage,
    assert_trade_dataset_usage,
    build_dataset_promotion_report,
    build_trade_dataset_usage_report,
    read_approved_dataset_registry,
    register_approved_dataset,
    write_dataset_promotion_report,
)
from app.ops.replay_parity import build_replay_parity_report
from app.ops.replay_parity import write_replay_parity_report


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


def _write_trade_dataset(
    tmp_path: Path,
    *,
    historical_feed_kind: str = "aggregate_trade",
    include_strict_metadata: bool = True,
) -> tuple[Path, Path]:
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
    metadata = {
        "raw_run_id": record.run_id,
        "raw_ingestion_seq": str(record.ingestion_seq),
        "historical_feed_kind": historical_feed_kind,
        "instrument_catalog_version": "catalog-v1",
        "instrument_snapshot": "{}",
        "metadata_source": "venue_runtime_snapshot",
        "venue_snapshot_version": "snapshot-v1",
        "normalizer_version": "v1",
    }
    if include_strict_metadata:
        metadata["metadata_snapshot_mode"] = "runtime"
        metadata["instrument_catalog_snapshot_json"] = "[]"
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts if include_strict_metadata else None,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="1",
            trade_id="1",
            side="buy",
            price=100.0,
            size=1.0,
            metadata=metadata,
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


def _downgrade_dataset_to_compat_only(normalized_path: Path) -> None:
    table = read_parquet(normalized_path)
    rows = table.to_pylist()
    downgraded_rows: list[dict[str, object]] = []
    for row in rows:
        metadata = dict(row["metadata"])
        metadata.pop("metadata_snapshot_mode", None)
        metadata.pop("instrument_catalog_snapshot_json", None)
        downgraded = dict(row)
        downgraded["provider_ts"] = None
        downgraded["raw_run_id"] = None
        downgraded["raw_ingestion_seq"] = None
        downgraded["metadata"] = metadata
        downgraded_rows.append(downgraded)
    downgraded_table = pa.Table.from_pylist(downgraded_rows, schema=table.schema)
    segments = sorted(partition_segments_dir(normalized_path).glob("*.parquet"))
    assert len(segments) == 1
    import pyarrow.parquet as pq

    pq.write_table(downgraded_table, segments[0], use_dictionary=False)


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
    assert report.generated_at
    assert report.raw_base_dir == str(raw_base_dir)
    assert report.normalized_path == str(normalized_path)
    assert report.env == "dev"
    assert report.symbol == "BTCUSDT"
    assert report.stream_type == "trade"


def test_replay_parity_report_writer_persists_operational_artifact_fields(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)
    report = build_replay_parity_report(
        raw_base_dir=raw_base_dir,
        normalized_path=normalized_path,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
    )
    output_path = tmp_path / "docs" / "validation" / "ingestion_replay_parity.json"

    write_replay_parity_report(output_path, report)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["pass_ok"] is True
    assert payload["order_match"] is True
    assert payload["manifest_ok"] is True
    assert payload["generated_at"]
    assert payload["raw_base_dir"] == str(raw_base_dir)
    assert payload["normalized_path"] == str(normalized_path)
    assert payload["env"] == "dev"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["stream_type"] == "trade"


def test_contract_and_parity_scripts_help_run():
    for script_name in (
        "scripts/check_normalized_contract.py",
        "scripts/check_replay_parity.py",
        "scripts/build_raw_manifests.py",
        "scripts/ingestion_release_gates.py",
        "scripts/ingestion_operational_evidence.py",
        "scripts/ingestion_failure_injection.py",
        "scripts/ingestion_vendor_contracts.py",
        "scripts/ingestion_ws_canary.py",
        "scripts/promote_ingestion_dataset.py",
    ):
        result = subprocess.run([sys.executable, script_name, "--help"], capture_output=True, text=True, check=False)
        assert result.returncode == 0
        assert "--help" in result.stdout or "usage:" in result.stdout.lower()


def test_dataset_promotion_requires_strict_contract_for_backtesting(tmp_path: Path):
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
    assert report.required_contract_mode == "strict"
    assert report.requested_contract_mode == "strict"


def test_dataset_promotion_fails_when_requested_mode_is_compat(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)

    report = build_dataset_promotion_report(
        target="paper",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="compat",
    )

    assert report.pass_ok is False
    assert report.contract.pass_ok is True
    assert any("requires contract_mode='strict'" in reason for reason in report.reasons)


def test_dataset_promotion_fails_when_dataset_only_passes_in_compat(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)
    _downgrade_dataset_to_compat_only(normalized_path)

    report = build_dataset_promotion_report(
        target="backtesting",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="strict",
    )

    assert report.pass_ok is False
    assert report.contract.pass_ok is False
    assert report.compat_contract is not None
    assert report.compat_contract.pass_ok is True
    assert any("only passes the normalized contract in compat mode" in reason for reason in report.reasons)


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


def test_register_approved_dataset_persists_single_registry_entry(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)
    report = build_dataset_promotion_report(
        target="paper",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="strict",
    )
    report_path = tmp_path / "docs" / "validation" / "promotion.json"
    registry_path = tmp_path / "docs" / "validation" / "approved.json"

    write_dataset_promotion_report(report_path, report)
    registry = register_approved_dataset(
        registry_path,
        report=report,
        promotion_report_path=report_path,
    )

    assert len(registry.entries) == 1
    entry = registry.entries[0]
    assert entry.target == "paper"
    assert entry.normalized_path == str(normalized_path)
    assert entry.promotion_report_path == str(report_path)
    assert entry.approved_contract_mode == "strict"

    loaded = read_approved_dataset_registry(registry_path)
    assert loaded.entries == registry.entries
    approved_entry = assert_dataset_is_registered_as_approved(
        normalized_path=normalized_path,
        target="paper",
        registry_path=registry_path,
    )
    assert approved_entry.normalized_path == str(normalized_path)


def test_register_approved_dataset_rejects_failed_promotion(tmp_path: Path):
    raw_base_dir, normalized_path = _write_trade_dataset(tmp_path)
    report = build_dataset_promotion_report(
        target="backtesting",
        normalized_path=normalized_path,
        raw_base_dir=raw_base_dir,
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
        contract_mode="compat",
    )

    with pytest.raises(DatasetPromotionApprovalError, match="failed promotion"):
        register_approved_dataset(
            tmp_path / "approved.json",
            report=report,
            promotion_report_path=tmp_path / "promotion.json",
        )
