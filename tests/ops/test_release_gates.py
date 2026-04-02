import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def _write_release_artifacts(tmp_path: Path, *, stale_rest: bool = False) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    rest_path = tmp_path / "rest.json"
    ws_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    soak_path = tmp_path / "soak.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
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
        },
    )
    _write_json(
        parity_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        soak_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "max_gaps": 0,
            "max_gap_irreparable": 0,
        },
    )
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "command": ["python", "-m", "pytest"],
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
    return rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, live_drill_path


def test_release_gates_paper_passes_with_clean_artifacts(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, live_drill_path = _write_release_artifacts(tmp_path)
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
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    blocks = {block.name: block for block in report.blocks}
    assert blocks["instrument_metadata"].status == "pass"
    assert blocks["storage_benchmark"].status == "pass"
    assert blocks["replay_parity"].status == "pass"
    assert blocks["paper_soak"].status == "pass"
    assert blocks["vendor_contracts"].status == "pass"
    assert blocks["observability_contract"].status == "pass"
    assert blocks["live_drill"].status == "pass"
    assert "Release gates: PASS (paper)" in render_release_gate_summary(report)


def test_release_gates_fail_when_required_artifact_is_stale(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, live_drill_path = _write_release_artifacts(tmp_path, stale_rest=True)
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
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is False
    canary_block = next(block for block in report.blocks if block.name == "canary_rest")
    assert canary_block.status == "fail"
    assert any("artifact stale" in reason for reason in canary_block.reasons)


def test_release_gates_live_requires_runtime_metadata_and_live_drill(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, live_drill_path = _write_release_artifacts(tmp_path)
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
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is False
    metadata_block = next(block for block in report.blocks if block.name == "instrument_metadata")
    assert metadata_block.status == "fail"
    assert any("runtime instrument metadata snapshot" in reason for reason in metadata_block.reasons)
