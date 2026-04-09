import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app import main
from app.config import load_config
from app.marketdata.replay import read_raw_entries
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.ops.release_gates import run_release_gates


NOW = datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def test_failure_injection_release_gate_fails_with_stale_ws_artifact(tmp_path: Path):
    _write_json(
        tmp_path / "metadata" / "instruments" / "env=dev" / "venue=BINANCE" / "latest.json",
        {"metadata_snapshot_mode": "runtime", "drift": {"material": False}},
    )
    _write_json(tmp_path / "rest.json", {"generated_at": NOW.isoformat(), "pass_ok": True, "diffs": {}, "comparison_reason": "ok"})
    _write_json(
        tmp_path / "ws.json",
        {
            "report_generated_at": (NOW - timedelta(days=2)).isoformat(),
            "pass_ok": True,
            "symbol": "BTCUSDT",
            "stream_type": "kline",
            "continuity": {"reconnects": 1, "duplicates": 0, "gaps": 0, "gap_irreparable": 0},
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "ok",
        },
    )
    _write_json(
        tmp_path / "benchmark.json",
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
            ],
        },
    )
    _write_json(
        tmp_path / "parity.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "normalized_path": str(tmp_path / "normalized" / "bars" / "env=dev" / "venue=BINANCE" / "symbol=BTCUSDT" / "date=2024-01-01"),
            "symbol": "BTCUSDT",
            "stream_type": "kline",
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        tmp_path / "soak.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 0,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )
    _write_json(
        tmp_path / "vendor.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=tmp_path / "rest.json",
        ws_canary_path=tmp_path / "ws.json",
        replay_parity_path=tmp_path / "parity.json",
        benchmark_path=tmp_path / "benchmark.json",
        soak_path=tmp_path / "soak.json",
        network_contracts_path=tmp_path / "vendor.json",
        live_drill_path=tmp_path / "live-drill.json",
    )

    assert report.pass_ok is False
    block = next(item for item in report.blocks if item.name == "canary_ws")
    assert any("artifact stale" in reason for reason in block.reasons)



def test_failure_injection_prod_rejects_fallback_metadata_snapshot(tmp_path: Path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env="prod",
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    metadata_path = tmp_path / "metadata" / "instruments" / "env=prod" / "venue=BINANCE" / "latest.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps({"metadata_snapshot_mode": "fallback", "drift": {"material": False}}), encoding="utf-8")
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("kline",),
    }

    with pytest.raises(ValueError, match="runtime instrument metadata snapshot"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)



def test_failure_injection_replay_reader_survives_tail_corruption(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="dev")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sink.write(
        RawRecord(
            payload={"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "E": int(ts.timestamp() * 1000), "p": "100", "q": "1", "t": 1}},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=ts,
            receive_ts=ts,
            source_id="1",
        )
    )
    path = next((tmp_path / "raw").glob("env=dev/venue=BINANCE/stream_type=trade/symbol=BTCUSDT/date=*/events.jsonl"))
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"payload": ')

    entries = read_raw_entries(tmp_path / "raw", "dev", symbol="BTCUSDT", stream_types=("trade",))
    assert len(entries) == 1
    quarantine = tmp_path / "errors" / "replay-corruption-dlq.jsonl"
    assert quarantine.exists()


def test_failure_injection_release_gate_fails_with_manifest_mismatch(tmp_path: Path):
    _write_json(
        tmp_path / "metadata" / "instruments" / "env=dev" / "venue=BINANCE" / "latest.json",
        {"metadata_snapshot_mode": "runtime", "drift": {"material": False}},
    )
    _write_json(tmp_path / "rest.json", {"generated_at": NOW.isoformat(), "pass_ok": True, "diffs": {}, "comparison_reason": "ok"})
    _write_json(
        tmp_path / "ws.json",
        {
            "report_generated_at": NOW.isoformat(),
            "pass_ok": True,
            "symbol": "BTCUSDT",
            "stream_type": "trade",
            "continuity": {"reconnects": 1, "duplicates": 0, "gaps": 0, "gap_irreparable": 0},
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "ok",
        },
    )
    _write_json(
        tmp_path / "benchmark.json",
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
            ],
        },
    )
    _write_json(
        tmp_path / "parity.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": False,
            "order_match": True,
            "manifest_ok": False,
            "normalized_path": str(tmp_path / "normalized" / "trades" / "env=dev" / "venue=BINANCE" / "symbol=BTCUSDT" / "date=2024-01-01"),
            "symbol": "BTCUSDT",
            "stream_type": "trade",
            "manifest_missing_files": [],
            "manifest_mismatches": ["events.jsonl:sha256"],
        },
    )
    _write_json(
        tmp_path / "soak.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 0,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )
    _write_json(
        tmp_path / "vendor.json",
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("trade",),
        rest_canary_path=tmp_path / "rest.json",
        ws_canary_path=tmp_path / "ws.json",
        replay_parity_path=tmp_path / "parity.json",
        benchmark_path=tmp_path / "benchmark.json",
        soak_path=tmp_path / "soak.json",
        network_contracts_path=tmp_path / "vendor.json",
        live_drill_path=tmp_path / "live-drill.json",
    )

    block = next(item for item in report.blocks if item.name == "replay_parity")
    assert block.status == "fail"
    assert any("manifest" in reason for reason in block.reasons)
