import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

import app.ops.release_gates as release_gates
from app.ops.release_gates import render_release_gate_summary, run_release_gates


NOW = datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_shadow_comparison(path: Path, *, significant: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": NOW.isoformat(),
        "diffs": {"row_count": 0, "identity_count": 0},
        "significant": significant,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_metadata_snapshot(base_dir: Path, *, env: str, mode: str = "runtime", material_drift: bool = False) -> None:
    path = base_dir / "metadata" / "instruments" / f"env={env}" / "venue=BINANCE" / "latest.json"
    _write_json(
        path,
        {
            "metadata_snapshot_mode": mode,
            "venue_snapshot_path": str(base_dir / "metadata" / "vendor" / "exchangeInfo.json"),
            "venue_snapshot_version": "runtime-v1",
            "fallback_reason": None if mode == "runtime" else "network down",
            "drift": {
                "has_drift": material_drift,
                "material": material_drift,
                "added_symbols": [],
                "removed_symbols": [],
                "changed_symbols": ["BTCUSDT"] if material_drift else [],
                "changed_fields_by_symbol": {"BTCUSDT": ["tick_size"]} if material_drift else {},
            },
        },
    )


def _write_release_artifacts(tmp_path: Path, *, stale_rest: bool = False) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path]:
    rest_path = tmp_path / "rest.json"
    ws_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    soak_path = tmp_path / "soak.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    live_drill_path = tmp_path / "live-drill.json"
    rest_generated_at = (NOW - timedelta(days=2)).isoformat() if stale_rest else NOW.isoformat()
    _write_json(
        rest_path,
        {
            "generated_at": rest_generated_at,
            "pass_ok": True,
            "diffs": {"row_count": 0},
            "comparison_reason": "semantic_match",
        },
    )
    _write_json(
        ws_path,
        {
            "report_generated_at": NOW.isoformat(),
            "pass_ok": True,
            "symbol": "BTCUSDT",
            "stream_type": "kline",
            "continuity": {
                "reconnects": 1,
                "duplicates": 0,
                "gaps": 0,
                "gap_irreparable": 0,
            },
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "continuity_ok",
        },
    )
    _write_json(
        benchmark_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0},
        },
    )
    _write_json(
        parity_path,
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
        soak_path,
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
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        failure_injection_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/ops/test_failure_injection.py",
            "critical_test_ids": [
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact",
                "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot",
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch",
            ],
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        live_drill_path,
        {
            "generated_at": NOW.isoformat(),
            "drill_executed": True,
            "promote_ready": True,
            "rollback_ready": True,
            "overall_status": "PASS",
        },
    )
    return rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path


def test_release_gates_paper_passes_with_clean_artifacts(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        output_path=tmp_path / "release-gates.json",
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    assert report.base_dir == str(tmp_path)
    blocks = {block.name: block for block in report.blocks}
    assert blocks["instrument_metadata"].status == "pass"
    assert blocks["storage_benchmark"].status == "pass"
    assert blocks["replay_parity"].status == "pass"
    assert blocks["paper_soak"].status == "pass"
    assert blocks["vendor_contracts"].status == "pass"
    assert blocks["observability_contract"].status == "pass"
    assert blocks["observability_contract"].details["target"] == "paper"
    assert "exchange_receive_skew_seconds" in blocks["observability_contract"].details["required_metric_thresholds"]
    assert "invalid_timestamp_detected" in blocks["observability_contract"].details["required_alerts"]
    assert blocks["live_drill"].status == "pass"
    assert blocks["replay_parity"].details["path"] == str(parity_path)
    assert blocks["canary_ws"].details["path"] == str(ws_path)
    assert blocks["storage_benchmark"].details["path"] == str(benchmark_path)
    assert blocks["vendor_contracts"].details["path"] == str(vendor_contracts_path)
    written = json.loads((tmp_path / "release-gates.json").read_text(encoding="utf-8"))
    assert written["overall_status"] == "PASS"
    assert written["base_dir"] == str(tmp_path)
    assert "Release gates: PASS (paper)" in render_release_gate_summary(report)


def test_release_gates_fail_when_required_artifact_is_stale(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path, stale_rest=True)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is False
    canary_block = next(block for block in report.blocks if block.name == "canary_rest")
    assert canary_block.status == "fail"
    assert any("artifact stale" in reason for reason in canary_block.reasons)


def test_release_gates_live_requires_runtime_metadata_and_live_drill(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_metadata_snapshot(tmp_path, env="dev", mode="fallback")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is False
    metadata_block = next(block for block in report.blocks if block.name == "instrument_metadata")
    assert metadata_block.status == "fail"
    assert any("runtime instrument metadata snapshot" in reason for reason in metadata_block.reasons)


def test_release_gates_fail_when_vendor_contract_artifact_is_incomplete(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "command": [],
            "returncode": 0,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "vendor_contracts")
    assert block.status == "fail"
    assert any("pytest_target" in reason or "command" in reason or "duration_seconds" in reason for reason in block.reasons)


def test_release_gates_fail_when_soak_records_compaction_failures(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_json(
        soak_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 1,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "paper_soak")
    assert block.status == "fail"
    assert any("compaction failures exceed soak threshold" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_failure_injection_artifact_is_missing(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, _failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    missing_failure_injection_path = tmp_path / "missing-failure-injection.json"

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=missing_failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "failure_injection")
    assert block.status == "fail"
    assert any("missing artifact" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_live_drill_artifact_is_stale(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_json(
        live_drill_path,
        {
            "generated_at": (NOW - timedelta(days=2)).isoformat(),
            "drill_executed": True,
            "promote_ready": True,
            "rollback_ready": True,
            "overall_status": "PASS",
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "live_drill")
    assert block.status == "fail"
    assert any("artifact stale" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_live_drill_not_pass(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_json(
        live_drill_path,
        {
            "generated_at": NOW.isoformat(),
            "drill_executed": True,
            "promote_ready": False,
            "rollback_ready": True,
            "overall_status": "FAIL",
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "live_drill")
    assert block.status == "fail"
    assert any("overall_status is not PASS" in reason for reason in block.reasons)


def test_release_gates_fail_when_observability_contract_is_incomplete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    fake_report = mock.Mock(
        target="paper",
        required_metrics=("exchange_receive_skew_seconds",),
        required_alerts=("invalid_timestamp_detected",),
        required_metric_thresholds={},
        alert_specs={},
        missing_alerts=(),
        missing_metric_thresholds=("exchange_receive_skew_seconds",),
        invalid_alert_specs=(),
        pass_ok=False,
    )
    monkeypatch.setattr(release_gates, "build_observability_contract_report", lambda target="paper": fake_report)

    report = release_gates.run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "observability_contract")
    assert block.status == "fail"
    assert any("metric thresholds" in reason for reason in block.reasons)
