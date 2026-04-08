from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal, Sequence

from app.marketdata.support_matrix import (
    feed_support,
    live_supported_feed_types,
    paper_supported_feed_types,
    replay_validated_paper_feed_types,
    runtime_validated_live_feed_types,
    runtime_validated_paper_feed_types,
)


ReadinessTarget = Literal["paper", "live"]
ReadinessProfile = Literal["paper_trade", "paper_kline", "live_trade", "live_kline"]
ReadinessEvidenceBasis = Literal["replay_validated", "runtime_validated"]


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
    profile: ReadinessProfile
    dataset_env: str
    runtime_env: str
    runtime_base_dir: str | None
    symbol: str
    stream_type: str
    interval: str
    evidence_basis: ReadinessEvidenceBasis
    paper_scope: tuple[str, ...]
    live_scope: tuple[str, ...]
    paper_replay_validated_scope: tuple[str, ...]
    paper_runtime_validated_scope: tuple[str, ...]
    live_runtime_validated_scope: tuple[str, ...]
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
    benchmark_high_cardinality_symbol_counts: tuple[int, ...] | None = None,
    benchmark_bursts: int = 4,
    benchmark_events_per_symbol_per_burst: int = 12,
    benchmark_min_rows_per_second: float | None = None,
    soak_mode: Literal["deterministic", "ws-live"] = "ws-live",
    soak_iterations: int = 5,
    soak_events_per_iteration: int = 500,
    soak_duration_seconds: float = 150.0,
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
    profile, gate_stream_types, evidence_basis = _resolve_readiness_contract(
        target=target,
        stream_type=stream_type,
        gate_stream_types=gate_stream_types,
    )
    gate_stream_types_arg = ",".join(gate_stream_types)

    if not raw_base_dir.exists():
        raise ValueError(f"raw_base_dir does not exist: {raw_base_dir}")
    if not normalized_path.exists():
        raise ValueError(f"normalized_path does not exist: {normalized_path}")

    workspace = Path(workspace)
    validation_dir.mkdir(parents=True, exist_ok=True)
    executor = executor or _default_executor
    benchmark_high_cardinality_symbol_counts = (
        (100, 500) if target == "live" else (100,)
        if benchmark_high_cardinality_symbol_counts is None
        else tuple(int(value) for value in benchmark_high_cardinality_symbol_counts)
    )

    replay_parity_path = _profile_artifact_path(validation_dir, "ingestion_replay_parity", profile)
    rest_canary_path = _profile_artifact_path(validation_dir, "ingestion_canary_report", profile)
    ws_canary_path = _profile_artifact_path(validation_dir, "ingestion_ws_canary_report", profile)
    benchmark_path = _profile_artifact_path(validation_dir, "ingestion_storage_benchmark", profile)
    vendor_contracts_path = _profile_artifact_path(validation_dir, "ingestion_vendor_contracts", profile)
    soak_path = _profile_artifact_path(validation_dir, "ingestion_soak_evidence", profile)
    failure_injection_path = _profile_artifact_path(validation_dir, "ingestion_failure_injection", profile)
    release_gates_path = _profile_artifact_path(validation_dir, "ingestion_release_gates", profile)
    release_gates_predrill_path = _profile_artifact_path(validation_dir, "ingestion_release_gates_pre_drill", profile)
    live_drill_path = _profile_artifact_path(validation_dir, "ingestion_live_drill_report", profile)
    operational_evidence_path = _profile_artifact_path(validation_dir, "ingestion_operational_evidence", profile)
    operational_evidence_predrill_path = _profile_artifact_path(validation_dir, "ingestion_operational_evidence_pre_drill", profile)

    high_cardinality_arg = ",".join(str(value) for value in benchmark_high_cardinality_symbol_counts)

    gate_base_dir_args: tuple[str, ...] = ()
    if runtime_base_dir is not None:
        gate_base_dir_args = ("--base-dir", str(runtime_base_dir))

    storage_benchmark_command = [
        sys.executable,
        "scripts/ingestion_storage_benchmark.py",
        "--target-profile",
        "live" if target == "live" else "paper",
        "--symbol-count",
        str(benchmark_symbol_count),
        "--high-cardinality-symbol-counts",
        high_cardinality_arg,
        "--bursts",
        str(benchmark_bursts),
        "--events-per-symbol-per-burst",
        str(benchmark_events_per_symbol_per_burst),
    ]
    if benchmark_min_rows_per_second is not None:
        storage_benchmark_command.extend(
            [
                "--min-rows-per-second",
                str(benchmark_min_rows_per_second),
            ]
        )
    storage_benchmark_command.extend(
        [
            "--output",
            str(benchmark_path),
        ]
    )

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
            "storage_benchmark",
            tuple(storage_benchmark_command),
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
    ]

    if _requires_rest_validation(profile):
        steps[1:1] = [
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
            )
        ]

    if _requires_runtime_validation(profile):
        steps[2 if _requires_rest_validation(profile) else 1:2 if _requires_rest_validation(profile) else 1] = [
            (
                "ws_canary",
                (
                    sys.executable,
                    "scripts/ingestion_ws_canary.py",
                    "--target-profile",
                    target,
                    "--symbol",
                    symbol,
                    "--stream-type",
                    stream_type,
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
        ]
        steps.append(
            (
                "soak",
                (
                    sys.executable,
                    "scripts/ingestion_soak.py",
                    "--target-profile",
                    target,
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
                    stream_type,
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
            )
        )

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
                    "operational_evidence_predrill",
                    (
                        sys.executable,
                        "scripts/ingestion_operational_evidence.py",
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
                        "--live-drill-path",
                        str(live_drill_path),
                        "--output",
                        str(operational_evidence_predrill_path),
                    ),
                    str(operational_evidence_predrill_path),
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
                        "--operational-evidence-path",
                        str(operational_evidence_predrill_path),
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
                (
                    "operational_evidence_final",
                    (
                        sys.executable,
                        "scripts/ingestion_operational_evidence.py",
                        "--target",
                        "live",
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
                        "--output",
                        str(operational_evidence_path),
                    ),
                    str(operational_evidence_path),
                ),
            ]
        )
    else:
        steps.append(
            (
                "operational_evidence_final",
                (
                    sys.executable,
                    "scripts/ingestion_operational_evidence.py",
                    "--target",
                    "paper",
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
                    "--output",
                    str(operational_evidence_path),
                ),
                str(operational_evidence_path),
            )
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
                "--operational-evidence-path",
                str(operational_evidence_path),
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
        profile=profile,
        dataset_env=env,
        runtime_env=runtime_env,
        runtime_base_dir=str(runtime_base_dir) if runtime_base_dir is not None else None,
        symbol=symbol,
        stream_type=stream_type,
        interval=interval,
        evidence_basis=evidence_basis,
        paper_scope=paper_supported_feed_types(),
        live_scope=live_supported_feed_types(),
        paper_replay_validated_scope=replay_validated_paper_feed_types(),
        paper_runtime_validated_scope=runtime_validated_paper_feed_types(),
        live_runtime_validated_scope=runtime_validated_live_feed_types(),
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


def _resolve_readiness_contract(
    *,
    target: ReadinessTarget,
    stream_type: str,
    gate_stream_types: tuple[str, ...] | None,
) -> tuple[ReadinessProfile, tuple[str, ...], ReadinessEvidenceBasis]:
    normalized_stream_type = str(stream_type).strip().lower()
    support = feed_support(normalized_stream_type)
    if target == "live":
        if not support.supports_live:
            raise ValueError(f"live readiness does not support stream_type={stream_type}")
        if normalized_stream_type == "trade":
            profile = "live_trade"
            expected_gate_stream_types = ("trade",)
        elif normalized_stream_type == "kline":
            profile = "live_kline"
            expected_gate_stream_types = ("kline",)
        else:
            raise ValueError(f"live readiness profile is not implemented for stream_type={stream_type}")
        evidence_basis: ReadinessEvidenceBasis = "runtime_validated"
    elif not support.supports_paper:
        raise ValueError(f"paper readiness does not support stream_type={stream_type}")
    elif normalized_stream_type == "trade":
        profile = "paper_trade"
        expected_gate_stream_types = ("trade",)
        evidence_basis = "replay_validated"
    elif normalized_stream_type == "kline":
        profile = "paper_kline"
        expected_gate_stream_types = ("kline",)
        evidence_basis = "runtime_validated"
    else:
        raise ValueError(f"unsupported readiness stream_type={stream_type}")
    requested_gate_stream_types = tuple(gate_stream_types or expected_gate_stream_types)
    if requested_gate_stream_types != expected_gate_stream_types:
        raise ValueError(
            "readiness contract mismatch: "
            f"profile={profile} expects gate_stream_types={expected_gate_stream_types}, "
            f"got {requested_gate_stream_types}"
        )
    return profile, requested_gate_stream_types, evidence_basis


def _requires_runtime_validation(profile: ReadinessProfile) -> bool:
    return profile in {"paper_kline", "live_trade", "live_kline"}


def _requires_rest_validation(profile: ReadinessProfile) -> bool:
    return profile in {"paper_kline", "live_kline"}


def _profile_artifact_path(validation_dir: Path, stem: str, profile: ReadinessProfile) -> Path:
    return validation_dir / f"{stem}_{profile}.json"
