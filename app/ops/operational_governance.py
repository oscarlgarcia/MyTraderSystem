from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal


OperationalTarget = Literal["paper", "live"]
OperationalChannel = Literal["manual", "scheduled", "pipeline"]
CadenceState = Literal["bootstrap", "healthy", "stale", "manual", "invalid"]


@dataclass(frozen=True, slots=True)
class OperationalCadencePolicy:
    interval_seconds: int
    max_interval_seconds: int
    owner: str
    required_channels: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationalGovernanceReport:
    generated_at: str
    target: OperationalTarget
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
    context_source: str
    cadence_policy: OperationalCadencePolicy
    cadence_state: CadenceState
    previous_success_at: str | None
    previous_execution_ref: str | None
    successful_runs_seen: int
    pass_ok: bool
    reasons: tuple[str, ...]


def cadence_policy_for_target(target: OperationalTarget) -> OperationalCadencePolicy:
    if target == "live":
        return OperationalCadencePolicy(
            interval_seconds=6 * 60 * 60,
            max_interval_seconds=8 * 60 * 60,
            owner="team-ingestion-oncall",
            required_channels=("scheduled", "pipeline"),
        )
    return OperationalCadencePolicy(
        interval_seconds=6 * 60 * 60,
        max_interval_seconds=12 * 60 * 60,
        owner="team-ingestion",
        required_channels=("scheduled", "pipeline"),
    )


def write_operational_governance_report(path: Path, report: OperationalGovernanceReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def append_operational_governance_history(
    path: Path,
    *,
    report: OperationalGovernanceReport,
    overall_status: str,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": report.generated_at,
        "target": report.target,
        "runner_id": report.runner_id,
        "execution_ref": report.execution_ref,
        "channel": report.channel,
        "schedule_name": report.schedule_name,
        "job_id": report.job_id,
        "job_url": report.job_url,
        "owner": report.owner,
        "cadence_state": report.cadence_state,
        "pass_ok": report.pass_ok,
        "overall_status": overall_status,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return path


def build_operational_governance_report(
    *,
    target: OperationalTarget,
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
    context_source: str = "cli",
    generated_at: str | None = None,
    history_path: Path | None = None,
    governance_artifact_path: Path | None = None,
) -> OperationalGovernanceReport:
    checked_at = generated_at or datetime.now(timezone.utc).isoformat()
    output_dir = Path(output_dir)
    history_path = Path(history_path) if history_path is not None else output_dir / f"ingestion_operational_history_{target}.jsonl"
    governance_artifact_path = (
        Path(governance_artifact_path)
        if governance_artifact_path is not None
        else output_dir / f"ingestion_operational_governance_{target}.json"
    )
    policy = cadence_policy_for_target(target)
    resolved_owner = str(owner or policy.owner)
    reasons: list[str] = []
    cadence_state: CadenceState = "bootstrap"

    if not str(execution_ref).strip():
        reasons.append("missing execution_ref")
        cadence_state = "invalid"
    if not str(runner_id).strip():
        reasons.append("missing runner_id")
        cadence_state = "invalid"
    if not str(trigger).strip():
        reasons.append("missing trigger")
        cadence_state = "invalid"
    if not str(provenance_source).strip():
        reasons.append("missing provenance_source")
        cadence_state = "invalid"
    if channel not in {"manual", "scheduled", "pipeline"}:
        reasons.append(f"unsupported channel: {channel}")
        cadence_state = "invalid"
    if channel in policy.required_channels:
        if not str(schedule_name).strip():
            reasons.append("scheduled or pipeline runs require schedule_name")
        if not str(job_id).strip():
            reasons.append("scheduled or pipeline runs require job_id")
        if not str(job_url).strip():
            reasons.append("scheduled or pipeline runs require job_url")
    elif channel == "manual":
        cadence_state = "manual"
        reasons.append("manual channel is informational only and cannot close operational readiness")

    previous_success_at: str | None = None
    previous_execution_ref: str | None = None
    successful_runs_seen = 0
    if history_path.exists():
        for raw_line in history_path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if payload.get("target") != target:
                continue
            if payload.get("overall_status") != "PASS":
                continue
            successful_runs_seen += 1
            previous_success_at = payload.get("generated_at") or previous_success_at
            previous_execution_ref = payload.get("execution_ref") or previous_execution_ref
    if channel in policy.required_channels and cadence_state != "invalid":
        if previous_success_at:
            try:
                previous_ts = datetime.fromisoformat(str(previous_success_at))
                current_ts = datetime.fromisoformat(checked_at)
                delta_seconds = (current_ts - previous_ts).total_seconds()
            except ValueError:
                reasons.append("previous success timestamp is invalid")
                cadence_state = "invalid"
            else:
                if delta_seconds > policy.max_interval_seconds:
                    cadence_state = "stale"
                    reasons.append(
                        f"cadence stale: previous success is older than {policy.max_interval_seconds}s"
                    )
                else:
                    cadence_state = "healthy"
        elif cadence_state not in {"manual", "invalid"}:
            cadence_state = "bootstrap"

    pass_ok = not reasons or (cadence_state in {"bootstrap", "healthy"} and all("manual channel" not in reason for reason in reasons))
    if cadence_state in {"manual", "stale", "invalid"}:
        pass_ok = False
    if not reasons and pass_ok:
        reasons.append("runner governance aligned with cadence policy")

    return OperationalGovernanceReport(
        generated_at=checked_at,
        target=target,
        output_dir=str(output_dir),
        governance_artifact_path=str(governance_artifact_path),
        history_path=str(history_path),
        runner_id=str(runner_id),
        trigger=str(trigger),
        provenance_source=str(provenance_source),
        execution_ref=str(execution_ref),
        channel=str(channel),
        schedule_name=str(schedule_name),
        job_id=str(job_id),
        job_url=str(job_url),
        owner=resolved_owner,
        context_source=str(context_source),
        cadence_policy=policy,
        cadence_state=cadence_state,
        previous_success_at=previous_success_at,
        previous_execution_ref=previous_execution_ref,
        successful_runs_seen=successful_runs_seen,
        pass_ok=pass_ok,
        reasons=tuple(reasons),
    )
