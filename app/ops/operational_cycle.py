from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal, Sequence

from app.marketdata.support_matrix import feed_support, normalize_feed_types
from app.ops.operational_governance import (
    append_operational_governance_history,
    build_operational_governance_report,
    write_operational_governance_report,
)


OperationalTarget = Literal["paper", "live"]
OperationalChannel = Literal["manual", "scheduled", "pipeline"]
Executor = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


@dataclass(frozen=True, slots=True)
class OperationalCycleStepResult:
    stream_type: str
    profile: str
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    report_path: str
    artifacts_generated: tuple[str, ...]

    @property
    def pass_ok(self) -> bool:
        return self.returncode == 0


@dataclass(frozen=True, slots=True)
class OperationalCycleReport:
    generated_at: str
    target: OperationalTarget
    env: str
    runtime_env: str
    runtime_base_dir: str | None
    raw_base_dir: str
    normalized_paths: dict[str, str]
    symbol: str
    interval: str
    stream_types: tuple[str, ...]
    output_dir: str
    governance_artifact_path: str
    history_path: str
    runner_id: str
    trigger: str
    provenance_source: str
    execution_ref: str
    channel: str
    schedule_name: str
    job_id: str
    job_url: str
    owner: str
    cadence_state: str
    overall_status: Literal["PASS", "FAIL"]
    pass_ok: bool
    steps: tuple[OperationalCycleStepResult, ...]


def write_operational_cycle_report(path: Path, report: OperationalCycleReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def run_ingestion_operational_cycle(
    *,
    workspace: Path,
    target: OperationalTarget,
    env: str,
    raw_base_dir: Path,
    normalized_paths: dict[str, Path],
    symbol: str,
    interval: str,
    output_dir: Path,
    runner_id: str,
    trigger: str,
    provenance_source: str,
    execution_ref: str,
    channel: OperationalChannel,
    schedule_name: str,
    job_id: str,
    job_url: str,
    owner: str | None = None,
    stream_types: tuple[str, ...] | list[str],
    runtime_env: str | None = None,
    runtime_base_dir: Path | None = None,
    runtime_owner: str | None = None,
    runtime_surface_ref: str | None = None,
    runtime_verification_ref: str | None = None,
    alerts_owner: str | None = None,
    alerts_surface_ref: str | None = None,
    alerts_verification_ref: str | None = None,
    logs_owner: str | None = None,
    logs_surface_ref: str | None = None,
    logs_verification_ref: str | None = None,
    promotion_owner: str | None = None,
    promotion_surface_ref: str | None = None,
    promotion_verification_ref: str | None = None,
    cutover_owner: str | None = None,
    cutover_surface_ref: str | None = None,
    cutover_verification_ref: str | None = None,
    output_path: Path | None = None,
    executor: Executor | None = None,
) -> OperationalCycleReport:
    workspace = Path(workspace)
    raw_base_dir = Path(raw_base_dir)
    output_dir = Path(output_dir)
    runtime_env = str(runtime_env or env)
    runtime_base_dir = Path(runtime_base_dir) if runtime_base_dir is not None else None
    if not raw_base_dir.exists():
        raise ValueError(f"raw_base_dir does not exist: {raw_base_dir}")
    if not execution_ref.strip():
        raise ValueError("execution_ref cannot be empty")

    normalized_stream_types = normalize_feed_types(stream_types)
    if "book" in normalized_stream_types:
        raise ValueError("operational closure does not support stream_type=book")
    for stream_type in normalized_stream_types:
        support = feed_support(stream_type)
        if target == "paper" and not support.supports_paper:
            raise ValueError(f"paper operational closure does not support stream_type={stream_type}")
        if target == "live" and not support.supports_live:
            raise ValueError(f"live operational closure does not support stream_type={stream_type}")
        if stream_type not in normalized_paths:
            raise ValueError(f"missing normalized path mapping for stream_type={stream_type}")
        if not Path(normalized_paths[stream_type]).exists():
            raise ValueError(f"normalized_path does not exist for stream_type={stream_type}: {normalized_paths[stream_type]}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = Path(output_path) if output_path is not None else output_dir / f"ingestion_operational_cycle_{target}.json"
    governance_path = output_dir / f"ingestion_operational_governance_{target}.json"
    history_path = output_dir / f"ingestion_operational_history_{target}.jsonl"
    executor = executor or _default_executor
    governance_report = build_operational_governance_report(
        target=target,
        output_dir=output_dir,
        runner_id=runner_id,
        trigger=trigger,
        provenance_source=provenance_source,
        execution_ref=execution_ref,
        channel=channel,
        schedule_name=schedule_name,
        job_id=job_id,
        job_url=job_url,
        owner=owner,
        history_path=history_path,
        governance_artifact_path=governance_path,
    )
    write_operational_governance_report(governance_path, governance_report)

    steps: list[OperationalCycleStepResult] = []
    overall_status: Literal["PASS", "FAIL"] = "PASS" if governance_report.pass_ok else "FAIL"
    for stream_type in normalized_stream_types:
        profile = f"{target}_{stream_type}"
        report_path = output_dir / f"ingestion_readiness_{profile}.json"
        command = [
            sys.executable,
            "scripts/ingestion_readiness.py",
            "--target",
            target,
            "--env",
            env,
            "--runtime-env",
            runtime_env,
            "--raw-base-dir",
            str(raw_base_dir),
            "--normalized-path",
            str(normalized_paths[stream_type]),
            "--symbol",
            symbol,
            "--stream-type",
            stream_type,
            "--interval",
            interval,
            "--validation-dir",
            str(output_dir),
            "--output",
            str(report_path),
            "--provenance-source",
            provenance_source,
            "--execution-ref",
            execution_ref,
            "--channel",
            channel,
            "--runner-governance-path",
            str(governance_path),
        ]
        if runtime_base_dir is not None:
            command.extend(["--runtime-base-dir", str(runtime_base_dir)])
        for flag, value in (
            ("--runtime-owner", runtime_owner),
            ("--runtime-surface-ref", runtime_surface_ref),
            ("--runtime-verification-ref", runtime_verification_ref),
            ("--alerts-owner", alerts_owner),
            ("--alerts-surface-ref", alerts_surface_ref),
            ("--alerts-verification-ref", alerts_verification_ref),
            ("--logs-owner", logs_owner),
            ("--logs-surface-ref", logs_surface_ref),
            ("--logs-verification-ref", logs_verification_ref),
            ("--promotion-owner", promotion_owner),
            ("--promotion-surface-ref", promotion_surface_ref),
            ("--promotion-verification-ref", promotion_verification_ref),
            ("--cutover-owner", cutover_owner),
            ("--cutover-surface-ref", cutover_surface_ref),
            ("--cutover-verification-ref", cutover_verification_ref),
        ):
            if value:
                command.extend([flag, str(value)])
        result = executor(command, workspace)
        artifacts_generated = _collect_artifacts_from_readiness_report(report_path)
        step = OperationalCycleStepResult(
            stream_type=stream_type,
            profile=profile,
            command=tuple(command),
            returncode=int(result.returncode),
            stdout=result.stdout or "",
            stderr=result.stderr or "",
            report_path=str(report_path),
            artifacts_generated=artifacts_generated,
        )
        steps.append(step)
        if step.returncode != 0:
            overall_status = "FAIL"
            break

    report = OperationalCycleReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target=target,
        env=env,
        runtime_env=runtime_env,
        runtime_base_dir=str(runtime_base_dir) if runtime_base_dir is not None else None,
        raw_base_dir=str(raw_base_dir),
        normalized_paths={stream_type: str(Path(path)) for stream_type, path in normalized_paths.items()},
        symbol=symbol,
        interval=interval,
        stream_types=normalized_stream_types,
        output_dir=str(output_dir),
        governance_artifact_path=str(governance_path),
        history_path=str(history_path),
        runner_id=runner_id,
        trigger=trigger,
        provenance_source=provenance_source,
        execution_ref=execution_ref,
        channel=channel,
        schedule_name=schedule_name,
        job_id=job_id,
        job_url=job_url,
        owner=governance_report.owner,
        cadence_state=governance_report.cadence_state,
        overall_status=overall_status,
        pass_ok=overall_status == "PASS" and governance_report.pass_ok and all(step.pass_ok for step in steps),
        steps=tuple(steps),
    )
    write_operational_cycle_report(output_path, report)
    append_operational_governance_history(
        history_path,
        report=governance_report,
        overall_status=report.overall_status,
    )
    return report


def _collect_artifacts_from_readiness_report(report_path: Path) -> tuple[str, ...]:
    if not report_path.exists():
        return (str(report_path),)
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return (str(report_path),)
    artifacts = [str(report_path)]
    for step in payload.get("steps", []):
        artifact_path = step.get("artifact_path")
        if isinstance(artifact_path, str) and artifact_path.strip():
            artifacts.append(artifact_path)
    unique_artifacts = tuple(dict.fromkeys(artifacts))
    return unique_artifacts


def _default_executor(command: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
