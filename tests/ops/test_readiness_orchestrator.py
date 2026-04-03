from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.ops.readiness_orchestrator import run_ingestion_readiness
from app.ops.release_gates import run_release_gates


def _success_executor(commands: list[tuple[str, ...]]):
    def _run(command, _cwd):
        commands.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    return _run


def test_readiness_orchestrator_runs_paper_steps_in_order(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    normalized_path = tmp_path / "normalized"
    raw_base_dir.mkdir()
    normalized_path.mkdir()

    report = run_ingestion_readiness(
        workspace=tmp_path,
        target="paper",
        env="papercand",
        raw_base_dir=raw_base_dir,
        normalized_path=normalized_path,
        symbol="BTCUSDT",
        stream_type="trade",
        interval="1m",
        runtime_env="dev",
        runtime_base_dir=tmp_path / "data" / "dev",
        validation_dir=tmp_path / "docs" / "validation",
        output_path=tmp_path / "docs" / "validation" / "paper.json",
        executor=_success_executor(commands),
    )

    assert report.pass_ok is True
    assert [step.name for step in report.steps] == [
        "replay_parity",
        "rest_canary",
        "ws_canary",
        "storage_benchmark",
        "vendor_contracts",
        "soak",
        "release_gates_final",
    ]
    assert "--target" in commands[-1]
    assert "paper" in commands[-1]
    assert "--env" in commands[-1]
    assert "dev" in commands[-1]
    assert "--base-dir" in commands[-1]
    assert str(tmp_path / "data" / "dev") in commands[-1]
    assert "--stream-types" in commands[-1]
    assert "kline" in commands[-1]
    written = json.loads((tmp_path / "docs" / "validation" / "paper.json").read_text(encoding="utf-8"))
    assert written["overall_status"] == "PASS"
    assert written["dataset_env"] == "papercand"
    assert written["runtime_env"] == "dev"


def test_readiness_orchestrator_runs_live_predrill_and_final_gate(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    normalized_path = tmp_path / "normalized"
    raw_base_dir.mkdir()
    normalized_path.mkdir()

    report = run_ingestion_readiness(
        workspace=tmp_path,
        target="live",
        env="dev",
        raw_base_dir=raw_base_dir,
        normalized_path=normalized_path,
        symbol="BTCUSDT",
        stream_type="kline",
        interval="1m",
        validation_dir=tmp_path / "docs" / "validation",
        output_path=tmp_path / "docs" / "validation" / "live.json",
        executor=_success_executor(commands),
    )

    assert report.pass_ok is True
    assert [step.name for step in report.steps] == [
        "replay_parity",
        "rest_canary",
        "ws_canary",
        "storage_benchmark",
        "vendor_contracts",
        "soak",
        "failure_injection",
        "release_gates_predrill",
        "live_drill",
        "release_gates_final",
    ]
    predrill = commands[7]
    drill = commands[8]
    final_gate = commands[9]
    assert "--phase" in predrill and "predrill" in predrill
    assert "--release-gates-path" in drill
    assert str(tmp_path / "docs" / "validation" / "ingestion_release_gates_pre_drill.json") in drill
    assert "--phase" in final_gate and "final" in final_gate


def test_readiness_orchestrator_fails_clearly_when_prereq_paths_are_missing(tmp_path: Path):
    with pytest.raises(ValueError, match="raw_base_dir does not exist"):
        run_ingestion_readiness(
            workspace=tmp_path,
            target="paper",
            env="dev",
            raw_base_dir=tmp_path / "missing-raw",
            normalized_path=tmp_path / "normalized",
            symbol="BTCUSDT",
            stream_type="trade",
            interval="1m",
            validation_dir=tmp_path / "docs" / "validation",
            output_path=tmp_path / "docs" / "validation" / "paper.json",
        )


def test_release_gates_live_predrill_can_pass_without_live_drill(tmp_path: Path):
    base_dir = tmp_path / "data"
    base_dir.mkdir()
    metadata_path = base_dir / "metadata" / "instruments" / "env=dev" / "venue=BINANCE" / "latest.json"
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
    shadow_path = base_dir / "shadow" / "env=dev" / "comparisons.jsonl"
    shadow_path.parent.mkdir(parents=True, exist_ok=True)
    shadow_path.write_text(json.dumps({"significant": False, "diffs": {}, "ts": "2026-04-03T00:00:00+00:00"}) + "\n", encoding="utf-8")

    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    now = "2026-04-03T00:00:00+00:00"
    for name, payload in {
        "rest.json": {"generated_at": now, "pass_ok": True, "diffs": {}, "comparison_reason": "semantic_match"},
        "ws.json": {"report_generated_at": now, "pass_ok": True, "continuity": {"reconnects": 1, "duplicates": 0, "gaps": 0}, "reconnects_observed": 1, "reconnects_target": 1, "symbol": "BTCUSDT", "stream_type": "kline"},
        "benchmark.json": {"generated_at": now, "pass_ok": True, "slo": {"min_rows_per_second": 1.0}, "synthetic_case": {"pass_ok": True, "rows_per_second": 1.0}, "replay_case": {"pass_ok": True, "rows_per_second": 1.0}, "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 1.0}, "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 1.0}},
        "parity.json": {"generated_at": now, "pass_ok": True, "order_match": True, "manifest_ok": True, "normalized_path": str(tmp_path / "normalized"), "symbol": "BTCUSDT", "stream_type": "kline", "manifest_missing_files": [], "manifest_mismatches": []},
        "soak.json": {"generated_at": now, "pass_ok": True, "max_allowed_gaps": 0, "max_gaps": 0, "max_allowed_gap_irreparable": 0, "max_gap_irreparable": 0, "max_allowed_compaction_failures": 0, "compaction_failures_total": 0, "reconnects_observed": 1, "reconnects_target": 1},
        "vendor.json": {"generated_at": now, "pass_ok": True, "pytest_target": "tests/network/test_binance_contracts.py", "command": ["python", "-m", "pytest"], "returncode": 0, "duration_seconds": 1.0},
        "failure.json": {"generated_at": now, "pass_ok": True, "pytest_target": "tests/ops/test_failure_injection.py", "critical_test_ids": ["tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact", "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot", "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch"], "command": ["python", "-m", "pytest"], "returncode": 0, "duration_seconds": 1.0},
    }.items():
        (artifact_dir / name).write_text(json.dumps(payload), encoding="utf-8")

    report = run_release_gates(
        base_dir=base_dir,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=artifact_dir / "rest.json",
        ws_canary_path=artifact_dir / "ws.json",
        replay_parity_path=artifact_dir / "parity.json",
        benchmark_path=artifact_dir / "benchmark.json",
        soak_path=artifact_dir / "soak.json",
        network_contracts_path=artifact_dir / "vendor.json",
        failure_injection_path=artifact_dir / "failure.json",
        require_live_drill=False,
    )

    assert report.pass_ok is True
    live_block = next(block for block in report.blocks if block.name == "live_drill")
    assert live_block.required is False


def test_ingestion_readiness_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_readiness.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--target" in result.stdout
    assert "--raw-base-dir" in result.stdout
    assert "--normalized-path" in result.stdout
    assert "--runtime-env" in result.stdout
    assert "--runtime-base-dir" in result.stdout
    assert "--gate-stream-types" in result.stdout
