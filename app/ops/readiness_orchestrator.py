from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal, Sequence


ReadinessTarget = Literal["paper", "live"]


@dataclass(frozen=True, slots=True)
class ReadinessStepResult:
    name: str
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    artifact_path: str | None

    @property
    def pass_ok(self) -> bool:
        return self.returncode == 0


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    generated_at: str
    target: ReadinessTarget
    dataset_env: str
    runtime_env: str
    runtime_base_dir: str | None
    symbol: str
    stream_type: str
    interval: str
    raw_base_dir: str
    normalized_path: str
    validation_dir: str
    overall_status: Literal["PASS", "FAIL"]
    pass_ok: bool
    steps: tuple[ReadinessStepResult, ...]


Executor = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


def write_readiness_report(path: Path, report: ReadinessReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def run_ingestion_readiness(
    *,
    workspace: Path,
    target: ReadinessTarget,
    env: str,
    raw_base_dir: Path,
    normalized_path: Path,
    symbol: str,
    stream_type: str,
    interval: str,
    runtime_env: str | None = None,
    runtime_base_dir: Path | None = None,
    gate_stream_types: tuple[str, ...] | None = None,
    validation_dir: Path,
    output_path: Path,
    ws_max_events: int = 2,
    ws_duration_seconds: float = 130.0,
    ws_reconnect_after_events: int = 1,
    ws_induced_reconnects: int = 1,
    benchmark_symbol_count: int = 12,
    benchmark_high_cardinality_symbol_counts: tuple[int, ...] = (100, 500),
    benchmark_bursts: int = 4,
    benchmark_events_per_symbol_per_burst: int = 12,
    benchmark_min_rows_per_second: float = 100.0,
    soak_mode: Literal["deterministic", "ws-live"] = "ws-live",
    soak_iterations: int = 5,
    soak_events_per_iteration: int = 500,
    soak_duration_seconds: float = 130.0,
    soak_reconnect_after_events: int = 1,
    soak_induced_reconnects: int = 1,
    executor: Executor | None = None,
) -> ReadinessReport:
    raw_base_dir = Path(raw_base_dir)
    normalized_path = Path(normalized_path)
    runtime_base_dir = Path(runtime_base_dir) if runtime_base_dir is not None else None
    validation_dir = Path(validation_dir)
    output_path = Path(output_path)
    runtime_env = str(runtime_env or env)
    gate_stream_types = tuple(gate_stream_types or (("kline",) if target in {"paper", "live"} else (stream_type,)))
    gate_stream_types_arg = ",".join(gate_stream_types)

    if not raw_base_dir.exists():
        raise ValueError(f"raw_base_dir does not exist: {raw_base_dir}")
    if not normalized_path.exists():
        raise ValueError(f"normalized_path does not exist: {normalized_path}")
    if target == "live" and stream_type != "kline":
        raise ValueError("live readiness only supports stream_type=kline")

    workspace = Path(workspace)
    validation_dir.mkdir(parents=True, exist_ok=True)
    executor = executor or _default_executor

    replay_parity_path = validation_dir / "ingestion_replay_parity.json"
    rest_canary_path = validation_dir / "ingestion_canary_report.json"
    ws_canary_path = validation_dir / "ingestion_ws_canary_report.json"
    benchmark_path = validation_dir / "ingestion_storage_benchmark.json"
    vendor_contracts_path = validation_dir / "ingestion_vendor_contracts.json"
    soak_path = validation_dir / "ingestion_soak_evidence.json"
    failure_injection_path = validation_dir / "ingestion_failure_injection.json"
    release_gates_path = validation_dir / "ingestion_release_gates.json"
    release_gates_predrill_path = validation_dir / "ingestion_release_gates_pre_drill.json"
    live_drill_path = validation_dir / "ingestion_live_drill_report.json"

    high_cardinality_arg = ",".join(str(value) for value in benchmark_high_cardinality_symbol_counts)

    gate_base_dir_args: tuple[str, ...] = ()
    if runtime_base_dir is not None:
        gate_base_dir_args = ("--base-dir", str(runtime_base_dir))

    steps: list[tuple[str, tuple[str, ...], str | None]] = [
        (
            "replay_parity",
            (
                sys.executable,
                "scripts/check_replay_parity.py",
                "--raw-base-dir",
                str(raw_base_dir),
                "--normalized-path",
                str(normalized_path),
                "--env",
                env,
                "--symbol",
                symbol,
                "--stream-type",
                stream_type,
                "--output",
                str(replay_parity_path),
            ),
            str(replay_parity_path),
        ),
        (
            "rest_canary",
            (
                sys.executable,
                "scripts/ingestion_canary.py",
                "--mode",
                "rest-baseline",
                "--symbol",
                symbol,
                "--interval",
                interval,
                "--bars",
                "5",
                "--refresh-baseline",
                "--output",
                str(rest_canary_path),
            ),
            str(rest_canary_path),
        ),
        (
            "ws_canary",
            (
                sys.executable,
                "scripts/ingestion_ws_canary.py",
                "--symbol",
                symbol,
                "--stream-type",
                "kline",
                "--interval",
                interval,
                "--max-events",
                str(ws_max_events),
                "--duration-seconds",
                str(ws_duration_seconds),
                "--reconnect-after-events",
                str(ws_reconnect_after_events),
                "--induced-reconnects",
                str(ws_induced_reconnects),
                "--output",
                str(ws_canary_path),
            ),
            str(ws_canary_path),
        ),
        (
            "storage_benchmark",
            (
                sys.executable,
                "scripts/ingestion_storage_benchmark.py",
                "--symbol-count",
                str(benchmark_symbol_count),
                "--high-cardinality-symbol-counts",
                high_cardinality_arg,
                "--bursts",
                str(benchmark_bursts),
                "--events-per-symbol-per-burst",
                str(benchmark_events_per_symbol_per_burst),
                "--min-rows-per-second",
                str(benchmark_min_rows_per_second),
                "--output",
                str(benchmark_path),
            ),
            str(benchmark_path),
        ),
        (
            "vendor_contracts",
            (
                sys.executable,
                "scripts/ingestion_vendor_contracts.py",
                "--output",
                str(vendor_contracts_path),
            ),
            str(vendor_contracts_path),
        ),
        (
            "soak",
            (
                sys.executable,
                "scripts/ingestion_soak.py",
                "--mode",
                soak_mode,
                "--iterations",
                str(soak_iterations),
                "--events-per-iteration",
                str(soak_events_per_iteration),
                "--duration-seconds",
                str(soak_duration_seconds),
                "--symbol",
                symbol,
                "--stream-type",
                "kline",
                "--interval",
                interval,
                "--reconnect-after-events",
                str(soak_reconnect_after_events),
                "--induced-reconnects",
                str(soak_induced_reconnects),
                "--output",
                str(soak_path),
            ),
            str(soak_path),
        ),
    ]

    if target == "live":
        steps.extend(
            [
                (
                    "failure_injection",
                    (
                        sys.executable,
                        "scripts/ingestion_failure_injection.py",
                        "--output",
                        str(failure_injection_path),
                    ),
                    str(failure_injection_path),
                ),
                (
                    "release_gates_predrill",
                    (
                        sys.executable,
                        "scripts/ingestion_release_gates.py",
                        "--env",
                        runtime_env,
                        "--target",
                        "live",
                        "--phase",
                        "predrill",
                        "--stream-types",
                        gate_stream_types_arg,
                        "--rest-canary-path",
                        str(rest_canary_path),
                        "--ws-canary-path",
                        str(ws_canary_path),
                        "--replay-parity-path",
                        str(replay_parity_path),
                        "--benchmark-path",
                        str(benchmark_path),
                        "--soak-path",
                        str(soak_path),
                        "--network-contracts-path",
                        str(vendor_contracts_path),
                        "--failure-injection-path",
                        str(failure_injection_path),
                        *gate_base_dir_args,
                        "--output",
                        str(release_gates_predrill_path),
                    ),
                    str(release_gates_predrill_path),
                ),
                (
                    "live_drill",
                    (
                        sys.executable,
                        "scripts/ingestion_live_drill.py",
                        "--env",
                        runtime_env,
                        *gate_base_dir_args,
                        "--release-gates-path",
                        str(release_gates_predrill_path),
                        "--rest-canary-path",
                        str(rest_canary_path),
                        "--ws-canary-path",
                        str(ws_canary_path),
                        "--benchmark-path",
                        str(benchmark_path),
                        "--failure-injection-path",
                        str(failure_injection_path),
                        "--output",
                        str(live_drill_path),
                    ),
                    str(live_drill_path),
                ),
            ]
        )

    steps.append(
        (
            "release_gates_final",
            (
                sys.executable,
                "scripts/ingestion_release_gates.py",
                "--env",
                runtime_env,
                "--target",
                target,
                "--phase",
                "final",
                "--stream-types",
                gate_stream_types_arg,
                "--rest-canary-path",
                str(rest_canary_path),
                "--ws-canary-path",
                str(ws_canary_path),
                "--replay-parity-path",
                str(replay_parity_path),
                "--benchmark-path",
                str(benchmark_path),
                "--soak-path",
                str(soak_path),
                "--network-contracts-path",
                str(vendor_contracts_path),
                "--failure-injection-path",
                str(failure_injection_path),
                "--live-drill-path",
                str(live_drill_path),
                *gate_base_dir_args,
                "--output",
                str(release_gates_path),
            ),
            str(release_gates_path),
        )
    )

    step_results: list[ReadinessStepResult] = []
    overall_status: Literal["PASS", "FAIL"] = "PASS"

    for name, command, artifact_path in steps:
        result = executor(command, workspace)
        step_result = ReadinessStepResult(
            name=name,
            command=tuple(command),
            returncode=int(result.returncode),
            stdout=result.stdout or "",
            stderr=result.stderr or "",
            artifact_path=artifact_path,
        )
        step_results.append(step_result)
        if step_result.returncode != 0:
            overall_status = "FAIL"
            break

    report = ReadinessReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target=target,
        dataset_env=env,
        runtime_env=runtime_env,
        runtime_base_dir=str(runtime_base_dir) if runtime_base_dir is not None else None,
        symbol=symbol,
        stream_type=stream_type,
        interval=interval,
        raw_base_dir=str(raw_base_dir),
        normalized_path=str(normalized_path),
        validation_dir=str(validation_dir),
        overall_status=overall_status,
        pass_ok=overall_status == "PASS",
        steps=tuple(step_results),
    )
    write_readiness_report(output_path, report)
    return report


def _default_executor(command: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
