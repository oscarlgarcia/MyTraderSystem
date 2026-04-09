from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from app.ops.operational_cycle import run_ingestion_operational_cycle


def _executor_with_reports(commands: list[tuple[str, ...]], workspace: Path):
    def _run(command, _cwd):
        commands.append(tuple(command))
        command = tuple(command)
        output_path = Path(command[command.index("--output") + 1])
        validation_dir = Path(command[command.index("--validation-dir") + 1])
        stream_type = str(command[command.index("--stream-type") + 1])
        target = str(command[command.index("--target") + 1])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        validation_dir.mkdir(parents=True, exist_ok=True)
        profile = f"{target}_{stream_type}"
        payload = {
            "overall_status": "PASS",
            "steps": [
                {
                    "name": "replay_parity",
                    "artifact_path": str(validation_dir / f"ingestion_replay_parity_{profile}.json"),
                },
                {
                    "name": "operational_evidence_final",
                    "artifact_path": str(validation_dir / f"ingestion_operational_evidence_{profile}.json"),
                },
                {
                    "name": "release_gates_final",
                    "artifact_path": str(validation_dir / f"ingestion_release_gates_{profile}.json"),
                },
            ],
        }
        output_path.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    return _run


def test_operational_cycle_runs_paper_for_trade_and_kline(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    trade_normalized = tmp_path / "normalized" / "trade"
    trade_normalized.mkdir(parents=True)
    kline_normalized = tmp_path / "normalized" / "kline"
    kline_normalized.mkdir(parents=True)

    report = run_ingestion_operational_cycle(
        workspace=tmp_path,
        target="paper",
        env="dev",
        raw_base_dir=raw_base_dir,
        normalized_paths={"trade": trade_normalized, "kline": kline_normalized},
        symbol="BTCUSDT",
        interval="1m",
        output_dir=tmp_path / "docs" / "validation",
        runner_id="ops-cycle-paper",
        trigger="scheduled_paper_cycle",
        provenance_source="ingestion_operational_cycle",
        execution_ref="exec-paper-001",
        channel="scheduled",
        schedule_name="ingestion-paper-cadence",
        job_id="paper-job-001",
        job_url="https://ops.example/paper-job-001",
        stream_types=("trade", "kline"),
        executor=_executor_with_reports(commands, tmp_path),
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    assert report.channel == "scheduled"
    assert report.execution_ref == "exec-paper-001"
    assert [step.stream_type for step in report.steps] == ["trade", "kline"]
    assert "--execution-ref" in commands[0] and "exec-paper-001" in commands[0]
    assert "--channel" in commands[1] and "scheduled" in commands[1]
    assert "--stream-type" in commands[0] and "trade" in commands[0]
    assert "--stream-type" in commands[1] and "kline" in commands[1]
    manifest = json.loads((tmp_path / "docs" / "validation" / "ingestion_operational_cycle_paper.json").read_text(encoding="utf-8"))
    assert manifest["overall_status"] == "PASS"
    assert manifest["stream_types"] == ["trade", "kline"]
    assert manifest["execution_ref"] == "exec-paper-001"
    assert manifest["cadence_state"] == "bootstrap"
    governance = json.loads((tmp_path / "docs" / "validation" / "ingestion_operational_governance_paper.json").read_text(encoding="utf-8"))
    assert governance["pass_ok"] is True
    assert governance["schedule_name"] == "ingestion-paper-cadence"
    assert governance["context_source"] == "cli"
    assert manifest["steps"][0]["artifacts_generated"]


def test_operational_cycle_runs_live_with_runtime_overrides(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    trade_normalized = tmp_path / "normalized" / "trade"
    trade_normalized.mkdir(parents=True)
    runtime_base_dir = tmp_path / "data" / "dev"
    runtime_base_dir.mkdir(parents=True)

    report = run_ingestion_operational_cycle(
        workspace=tmp_path,
        target="live",
        env="dev",
        runtime_env="prodshadow",
        runtime_base_dir=runtime_base_dir,
        raw_base_dir=raw_base_dir,
        normalized_paths={"trade": trade_normalized},
        symbol="BTCUSDT",
        interval="1m",
        output_dir=tmp_path / "docs" / "validation",
        runner_id="ops-cycle-live",
        trigger="pipeline_live_cycle",
        provenance_source="ingestion_operational_cycle",
        execution_ref="exec-live-001",
        channel="pipeline",
        schedule_name="ingestion-live-cadence",
        job_id="live-job-001",
        job_url="https://ops.example/live-job-001",
        stream_types=("trade",),
        runtime_owner="team-ingestion",
        runtime_surface_ref="grafana://ingestion/live/runtime",
        runtime_verification_ref="check://grafana/runtime",
        executor=_executor_with_reports(commands, tmp_path),
    )

    assert report.pass_ok is True
    assert report.runtime_env == "prodshadow"
    assert report.runtime_base_dir == str(runtime_base_dir)
    assert "--runtime-base-dir" in commands[0]
    assert str(runtime_base_dir) in commands[0]
    assert "--runtime-owner" in commands[0]
    assert "team-ingestion" in commands[0]
    assert "--runner-governance-path" in commands[0]
    assert "--ws-max-events" in commands[0] and "12" in commands[0]
    assert "--ws-duration-seconds" in commands[0] and "120.0" in commands[0]
    assert "--ws-reconnect-after-events" in commands[0] and "4" in commands[0]
    assert "--soak-iterations" in commands[0] and "3" in commands[0]
    assert "--soak-events-per-iteration" in commands[0] and "200" in commands[0]
    assert "--soak-duration-seconds" in commands[0] and "180.0" in commands[0]
    assert "--soak-reconnect-after-events" in commands[0] and "100" in commands[0]


def test_operational_cycle_passes_surface_manifest_and_runtime_tunables(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    trade_normalized = tmp_path / "normalized" / "trade"
    trade_normalized.mkdir(parents=True)
    surface_manifest = tmp_path / "surface-manifest.json"
    surface_manifest.write_text(
        json.dumps({"runtime": {"surface_ref": "grafana://paper/runtime"}}),
        encoding="utf-8",
    )

    report = run_ingestion_operational_cycle(
        workspace=tmp_path,
        target="paper",
        env="dev",
        raw_base_dir=raw_base_dir,
        normalized_paths={"trade": trade_normalized},
        symbol="BTCUSDT",
        interval="1m",
        output_dir=tmp_path / "docs" / "validation",
        runner_id="ops-cycle-paper",
        trigger="scheduled_paper_cycle",
        provenance_source="ingestion_operational_cycle",
        execution_ref="exec-paper-rt-001",
        channel="pipeline",
        schedule_name="ingestion-paper-cadence",
        job_id="paper-job-rt-001",
        job_url="https://ops.example/paper-job-rt-001",
        stream_types=("trade",),
        surface_manifest_path=surface_manifest,
        ws_max_events=1,
        ws_duration_seconds=9.0,
        benchmark_symbol_count=6,
        benchmark_high_cardinality_symbol_counts=(50,),
        benchmark_bursts=2,
        benchmark_events_per_symbol_per_burst=3,
        benchmark_min_rows_per_second=5.0,
        soak_mode="deterministic",
        soak_iterations=1,
        soak_events_per_iteration=20,
        soak_duration_seconds=8.0,
        executor=_executor_with_reports(commands, tmp_path),
    )

    assert report.pass_ok is True
    command = commands[0]
    assert "--surface-manifest" in command and str(surface_manifest) in command
    assert "--ws-max-events" in command and "1" in command
    assert "--ws-duration-seconds" in command and "9.0" in command
    assert "--benchmark-symbol-count" in command and "6" in command
    assert "--benchmark-high-cardinality-symbol-counts" in command and "50" in command
    assert "--benchmark-bursts" in command and "2" in command
    assert "--benchmark-events-per-symbol-per-burst" in command and "3" in command
    assert "--benchmark-min-rows-per-second" in command and "5.0" in command
    assert "--soak-mode" in command and "deterministic" in command
    assert "--soak-iterations" in command and "1" in command
    assert "--soak-events-per-iteration" in command and "20" in command
    assert "--soak-duration-seconds" in command and "8.0" in command


def test_operational_cycle_rejects_book_stream_type(tmp_path: Path):
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    with pytest.raises(ValueError, match="stream_type=book"):
        run_ingestion_operational_cycle(
            workspace=tmp_path,
            target="paper",
            env="dev",
            raw_base_dir=raw_base_dir,
            normalized_paths={},
            symbol="BTCUSDT",
            interval="1m",
            output_dir=tmp_path / "docs" / "validation",
            runner_id="ops-cycle-paper",
            trigger="scheduled_paper_cycle",
            provenance_source="ingestion_operational_cycle",
            execution_ref="exec-paper-001",
            channel="scheduled",
            schedule_name="ingestion-paper-cadence",
            job_id="paper-job-001",
            job_url="https://ops.example/paper-job-001",
            stream_types=("book",),
        )


def test_operational_cycle_requires_normalized_path_mapping(tmp_path: Path):
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    with pytest.raises(ValueError, match="missing normalized path mapping"):
        run_ingestion_operational_cycle(
            workspace=tmp_path,
            target="paper",
            env="dev",
            raw_base_dir=raw_base_dir,
            normalized_paths={},
            symbol="BTCUSDT",
            interval="1m",
            output_dir=tmp_path / "docs" / "validation",
            runner_id="ops-cycle-paper",
            trigger="scheduled_paper_cycle",
            provenance_source="ingestion_operational_cycle",
            execution_ref="exec-paper-001",
            channel="scheduled",
            schedule_name="ingestion-paper-cadence",
            job_id="paper-job-001",
            job_url="https://ops.example/paper-job-001",
            stream_types=("trade",),
        )


def test_operational_cycle_marks_manual_governance_as_failing(tmp_path: Path):
    commands: list[tuple[str, ...]] = []
    raw_base_dir = tmp_path / "raw"
    raw_base_dir.mkdir()
    trade_normalized = tmp_path / "normalized" / "trade"
    trade_normalized.mkdir(parents=True)

    report = run_ingestion_operational_cycle(
        workspace=tmp_path,
        target="paper",
        env="dev",
        raw_base_dir=raw_base_dir,
        normalized_paths={"trade": trade_normalized},
        symbol="BTCUSDT",
        interval="1m",
        output_dir=tmp_path / "docs" / "validation",
        runner_id="ops-cycle-paper",
        trigger="manual_run",
        provenance_source="ingestion_operational_cycle",
        execution_ref="exec-paper-manual-001",
        channel="manual",
        schedule_name="manual-paper",
        job_id="manual-job",
        job_url="https://ops.example/manual-job",
        stream_types=("trade",),
        executor=_executor_with_reports(commands, tmp_path),
    )

    governance = json.loads((tmp_path / "docs" / "validation" / "ingestion_operational_governance_paper.json").read_text(encoding="utf-8"))
    assert report.pass_ok is False
    assert report.overall_status == "FAIL"
    assert governance["pass_ok"] is False
    assert governance["cadence_state"] == "manual"
