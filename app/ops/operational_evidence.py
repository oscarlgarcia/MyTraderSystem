from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from app.marketdata.support_matrix import excluded_feed_types, normalize_feed_types
from app.ops.observability_contract import build_observability_contract_report


ReleaseTarget = Literal["paper", "live"]
EvidencePhase = Literal["predrill", "final"]


@dataclass(frozen=True, slots=True)
class OperationalArtifactEvidence:
    name: str
    path: str
    required: bool
    fresh: bool
    pass_ok: bool
    max_age_seconds: int
    age_seconds: float | None
    generated_at: str | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationalObservabilityEvidence:
    external_surfaces: tuple[dict[str, object], ...]
    repo_runbooks: tuple[str, ...]
    verification_artifact_path: str | None
    verification_generated_at: str | None
    verification_source: str
    pass_ok: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationalGovernanceEvidence:
    artifact_path: str | None
    schedule_name: str | None
    job_id: str | None
    job_url: str | None
    owner: str | None
    cadence_state: str | None
    cadence_policy: dict[str, object]
    previous_success_at: str | None
    previous_execution_ref: str | None
    successful_runs_seen: int
    pass_ok: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationalEvidenceProvenance:
    source: str
    runner_id: str
    trigger: str
    generated_by: str
    execution_ref: str
    channel: str
    verification_scope: str
    derived_in_process: bool


@dataclass(frozen=True, slots=True)
class OperationalEvidenceReport:
    generated_at: str
    target: ReleaseTarget
    phase: EvidencePhase
    stream_types: tuple[str, ...]
    cadence_policy: dict[str, object]
    evidence_origin: str
    provenance: OperationalEvidenceProvenance
    governance: OperationalGovernanceEvidence
    excluded_feed_policy: dict[str, str]
    observability: OperationalObservabilityEvidence
    artifacts: tuple[OperationalArtifactEvidence, ...]
    pass_ok: bool
    reasons: tuple[str, ...]


def write_operational_evidence_report(path: Path, report: OperationalEvidenceReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def build_operational_evidence_report(
    *,
    target: ReleaseTarget,
    phase: EvidencePhase,
    stream_types: tuple[str, ...] | list[str],
    rest_canary_path: Path | None = None,
    ws_canary_path: Path | None = None,
    replay_parity_path: Path | None = None,
    benchmark_path: Path | None = None,
    soak_path: Path | None = None,
    network_contracts_path: Path | None = None,
    failure_injection_path: Path | None = None,
    live_drill_path: Path | None = None,
    provenance_source: str = "scripted_operational_evidence",
    runner_id: str = "ingestion_operational_evidence",
    trigger: str = "manual",
    generated_by: str = "scripts/ingestion_operational_evidence.py",
    execution_ref: str = "manual-local",
    channel: str = "manual",
    observability_verification_path: Path | None = None,
    runner_governance_path: Path | None = None,
    derived_in_process: bool = False,
) -> OperationalEvidenceReport:
    normalized_stream_types = normalize_feed_types(stream_types)
    artifact_specs = _required_artifact_specs(
        target=target,
        phase=phase,
        stream_types=normalized_stream_types,
        rest_canary_path=rest_canary_path,
        ws_canary_path=ws_canary_path,
        replay_parity_path=replay_parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=network_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )
    artifacts = tuple(_collect_artifact_evidence(name=name, path=path, required=required, max_age=max_age) for name, path, required, max_age in artifact_specs)
    generated_at = datetime.now(timezone.utc).isoformat()
    governance = _collect_governance_evidence(
        target=target,
        execution_ref=execution_ref,
        channel=channel,
        runner_governance_path=runner_governance_path,
    )
    observability = _collect_observability_evidence(
        target=target,
        generated_at=generated_at,
        observability_verification_path=observability_verification_path,
    )
    reasons = [reason for artifact in artifacts for reason in artifact.reasons]
    reasons.extend(governance.reasons)
    reasons.extend(observability.reasons)
    pass_ok = (
        all((artifact.pass_ok and artifact.fresh) if artifact.required else True for artifact in artifacts)
        and governance.pass_ok
        and observability.pass_ok
    )
    if not pass_ok and not reasons:
        reasons.append("operational evidence incomplete")
    cadence_policy = {
        "runtime_artifact_max_age_seconds": int(timedelta(hours=24).total_seconds()),
        "benchmark_and_replay_max_age_seconds": int(timedelta(days=7).total_seconds()),
        "runtime_artifact_expected_interval_seconds": int(timedelta(hours=6).total_seconds()),
        "live_drill_expected_interval_seconds": int(timedelta(hours=12).total_seconds()),
        "required_runtime_artifacts": [artifact.name for artifact in artifacts if artifact.required and artifact.name in {"rest_canary", "ws_canary", "soak", "failure_injection", "live_drill"}],
        "required_artifacts": [artifact.name for artifact in artifacts if artifact.required],
    }
    evidence_origin = "operational_runtime" if target == "live" else "paper_operational"
    return OperationalEvidenceReport(
        generated_at=generated_at,
        target=target,
        phase=phase,
        stream_types=normalized_stream_types,
        cadence_policy=cadence_policy,
        evidence_origin=evidence_origin,
        provenance=OperationalEvidenceProvenance(
            source=provenance_source,
            runner_id=runner_id,
            trigger=trigger,
            generated_by=generated_by,
            execution_ref=execution_ref,
            channel=channel,
            verification_scope="external_operational_surfaces",
            derived_in_process=derived_in_process,
        ),
        governance=governance,
        excluded_feed_policy={feed_type: "excluded" for feed_type in excluded_feed_types()},
        observability=observability,
        artifacts=artifacts,
        pass_ok=pass_ok,
        reasons=tuple(reasons or ["operational evidence fresh and aligned"]),
    )


def _collect_governance_evidence(
    *,
    target: ReleaseTarget,
    execution_ref: str,
    channel: str,
    runner_governance_path: Path | None,
) -> OperationalGovernanceEvidence:
    reasons: list[str] = []
    if runner_governance_path is None or not runner_governance_path.exists():
        return OperationalGovernanceEvidence(
            artifact_path=str(runner_governance_path) if runner_governance_path is not None else None,
            schedule_name=None,
            job_id=None,
            job_url=None,
            owner=None,
            cadence_state=None,
            cadence_policy={},
            previous_success_at=None,
            previous_execution_ref=None,
            successful_runs_seen=0,
            pass_ok=False,
            reasons=("runner governance artifact missing",),
        )
    payload = json.loads(runner_governance_path.read_text(encoding="utf-8"))
    if payload.get("target") != target:
        reasons.append(f"runner governance target {payload.get('target')!r} does not match {target!r}")
    if str(payload.get("execution_ref") or "") != execution_ref:
        reasons.append("runner governance execution_ref does not match operational evidence")
    payload_channel = str(payload.get("channel") or "").strip().lower()
    if payload_channel != str(channel).strip().lower():
        reasons.append("runner governance channel does not match operational evidence")
    if payload.get("pass_ok") is not True:
        reasons.append("runner governance artifact is not passing")
    cadence_state = str(payload.get("cadence_state") or "")
    if cadence_state not in {"bootstrap", "healthy"}:
        reasons.append(f"runner governance cadence_state {cadence_state!r} is not promotable")
    if not str(payload.get("schedule_name") or "").strip():
        reasons.append("runner governance missing schedule_name")
    if not str(payload.get("job_id") or "").strip():
        reasons.append("runner governance missing job_id")
    if not str(payload.get("job_url") or "").strip():
        reasons.append("runner governance missing job_url")
    if not str(payload.get("owner") or "").strip():
        reasons.append("runner governance missing owner")
    return OperationalGovernanceEvidence(
        artifact_path=str(runner_governance_path),
        schedule_name=payload.get("schedule_name"),
        job_id=payload.get("job_id"),
        job_url=payload.get("job_url"),
        owner=payload.get("owner"),
        cadence_state=payload.get("cadence_state"),
        cadence_policy=dict(payload.get("cadence_policy") or {}),
        previous_success_at=payload.get("previous_success_at"),
        previous_execution_ref=payload.get("previous_execution_ref"),
        successful_runs_seen=int(payload.get("successful_runs_seen") or 0),
        pass_ok=not reasons,
        reasons=tuple(reasons or ["runner governance aligned with cadence policy"]),
    )


def build_observability_verification_report(
    *,
    target: ReleaseTarget,
    generated_at: str | None = None,
    verification_source: str = "manual_surface_check",
    surface_overrides: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    report = build_observability_contract_report(target=target)
    checked_at = generated_at or datetime.now(timezone.utc).isoformat()
    overrides = surface_overrides or {}
    surfaces: list[dict[str, object]] = []
    reasons: list[str] = []
    for surface in report.external_surfaces:
        override = overrides.get(surface.surface_id, {})
        payload = {
            "surface_id": surface.surface_id,
            "kind": surface.kind,
            "description": surface.description,
            "repo_reference": surface.repo_reference,
            "owner": str(override.get("owner", surface.owner)),
            "surface_ref": str(override.get("surface_ref", surface.surface_ref)),
            "verification_mode": str(override.get("verification_mode", surface.verification_mode)),
            "verified_at": str(override.get("verified_at", checked_at)),
            "verification_ref": str(override.get("verification_ref", f"manual://{surface.surface_id}")),
            "pass_ok": bool(override.get("pass_ok", True)),
        }
        if not payload["owner"]:
            reasons.append(f"missing observability owner for {surface.surface_id}")
        if not payload["surface_ref"]:
            reasons.append(f"missing observability surface_ref for {surface.surface_id}")
        if not payload["verification_ref"]:
            reasons.append(f"missing observability verification_ref for {surface.surface_id}")
        if not payload["verified_at"]:
            reasons.append(f"missing observability verified_at for {surface.surface_id}")
        if payload["pass_ok"] is not True:
            reasons.append(f"observability surface not passing: {surface.surface_id}")
        surfaces.append(payload)
    repo_runbooks = tuple(sorted({surface.repo_reference for surface in report.external_surfaces if surface.kind == "runbook"}))
    for repo_reference in repo_runbooks:
        if not Path(repo_reference).exists():
            reasons.append(f"missing observability runbook reference: {repo_reference}")
    return {
        "generated_at": checked_at,
        "target": target,
        "verification_source": verification_source,
        "repo_runbooks": list(repo_runbooks),
        "external_surfaces": surfaces,
        "pass_ok": report.pass_ok and not reasons,
        "reasons": reasons or ["observability verification artifact aligned"],
    }


def write_observability_verification_report(path: Path, report: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _collect_observability_evidence(
    *,
    target: ReleaseTarget,
    generated_at: str,
    observability_verification_path: Path | None,
) -> OperationalObservabilityEvidence:
    report = build_observability_contract_report(target=target)
    reasons: list[str] = []
    repo_runbooks: tuple[str, ...]
    verification_artifact_path: str | None = None
    verification_generated_at: str | None = None
    verification_source = "inline_contract_derivation"
    if observability_verification_path is not None and observability_verification_path.exists():
        payload = json.loads(observability_verification_path.read_text(encoding="utf-8"))
        verification_artifact_path = str(observability_verification_path)
        verification_generated_at = str(payload.get("generated_at") or "")
        verification_source = str(payload.get("verification_source") or "persisted_operational_verification")
        repo_runbooks = tuple(str(item) for item in payload.get("repo_runbooks", []))
        surfaces = tuple(payload.get("external_surfaces", []))
        if payload.get("target") != target:
            reasons.append(f"observability verification target {payload.get('target')!r} does not match {target!r}")
        if payload.get("pass_ok") is not True:
            reasons.append("observability verification artifact is not passing")
    else:
        repo_runbooks = tuple(sorted({surface.repo_reference for surface in report.external_surfaces if surface.kind == "runbook"}))
        surfaces = tuple(
            {
                "surface_id": surface.surface_id,
                "kind": surface.kind,
                "description": surface.description,
                "repo_reference": surface.repo_reference,
                "owner": surface.owner,
                "surface_ref": surface.surface_ref,
                "verification_mode": surface.verification_mode,
                "verified_at": generated_at,
                "verification_ref": f"artifact://ingestion-operational-evidence/{target}/{surface.surface_id}",
                "pass_ok": True,
            }
            for surface in report.external_surfaces
        )
        reasons.append("observability verification artifact missing; using inline contract derivation")
    for repo_reference in repo_runbooks:
        if not Path(repo_reference).exists():
            reasons.append(f"missing observability runbook reference: {repo_reference}")
    if not surfaces:
        reasons.append("missing external observability surfaces")
    for surface in surfaces:
        if not surface["owner"]:
            reasons.append(f"missing observability owner for {surface['surface_id']}")
        if not surface["surface_ref"]:
            reasons.append(f"missing observability surface_ref for {surface['surface_id']}")
        if not surface["verification_ref"]:
            reasons.append(f"missing observability verification_ref for {surface['surface_id']}")
    return OperationalObservabilityEvidence(
        external_surfaces=surfaces,
        repo_runbooks=repo_runbooks,
        verification_artifact_path=verification_artifact_path,
        verification_generated_at=verification_generated_at,
        verification_source=verification_source,
        pass_ok=report.pass_ok and not reasons,
        reasons=tuple(reasons or ["observability surfaces verified and runbooks present"]),
    )


def _required_artifact_specs(
    *,
    target: ReleaseTarget,
    phase: EvidencePhase,
    stream_types: tuple[str, ...],
    rest_canary_path: Path | None,
    ws_canary_path: Path | None,
    replay_parity_path: Path | None,
    benchmark_path: Path | None,
    soak_path: Path | None,
    network_contracts_path: Path | None,
    failure_injection_path: Path | None,
    live_drill_path: Path | None,
) -> tuple[tuple[str, Path, bool, timedelta], ...]:
    runtime_required = target == "live" or "kline" in stream_types
    rest_required = "kline" in stream_types
    specs: list[tuple[str, Path, bool, timedelta]] = [
        ("replay_parity", Path(replay_parity_path or "docs/validation/ingestion_replay_parity.json"), True, timedelta(days=7)),
        ("storage_benchmark", Path(benchmark_path or "docs/validation/ingestion_storage_benchmark.json"), True, timedelta(days=7)),
        ("vendor_contracts", Path(network_contracts_path or "docs/validation/ingestion_vendor_contracts.json"), True, timedelta(hours=24)),
        ("rest_canary", Path(rest_canary_path or "docs/validation/ingestion_canary_report.json"), rest_required, timedelta(hours=24)),
        ("ws_canary", Path(ws_canary_path or "docs/validation/ingestion_ws_canary_report.json"), runtime_required, timedelta(hours=24)),
        ("soak", Path(soak_path or "docs/validation/ingestion_soak_evidence.json"), runtime_required, timedelta(hours=24)),
    ]
    if target == "live":
        specs.append(
            ("failure_injection", Path(failure_injection_path or "docs/validation/ingestion_failure_injection.json"), True, timedelta(hours=24))
        )
        specs.append(
            ("live_drill", Path(live_drill_path or "docs/validation/ingestion_live_drill_report.json"), phase == "final", timedelta(hours=24))
        )
    return tuple(specs)


def _collect_artifact_evidence(
    *,
    name: str,
    path: Path,
    required: bool,
    max_age: timedelta,
) -> OperationalArtifactEvidence:
    if not path.exists():
        reasons = (f"missing artifact: {path}",)
        return OperationalArtifactEvidence(
            name=name,
            path=str(path),
            required=required,
            fresh=not required,
            pass_ok=not required,
            max_age_seconds=int(max_age.total_seconds()),
            age_seconds=None,
            generated_at=None,
            reasons=reasons,
        )
    payload = _load_payload(path)
    generated_at = _artifact_generated_at(path, payload)
    age = datetime.now(timezone.utc) - generated_at if generated_at is not None else None
    reasons: list[str] = []
    pass_ok = bool(payload.get("pass_ok", payload.get("overall_status") == "PASS"))
    if name == "live_drill":
        pass_ok = bool(payload.get("drill_executed")) and bool(payload.get("promote_ready")) and bool(payload.get("rollback_ready")) and payload.get("overall_status") == "PASS"
    if required and not pass_ok:
        reasons.append(f"{name} artifact is not passing")
    fresh = age is not None and age <= max_age
    if required and not fresh:
        reasons.append(f"{name} artifact stale: older than {int(max_age.total_seconds())}s")
    return OperationalArtifactEvidence(
        name=name,
        path=str(path),
        required=required,
        fresh=fresh or not required,
        pass_ok=pass_ok or not required,
        max_age_seconds=int(max_age.total_seconds()),
        age_seconds=age.total_seconds() if age is not None else None,
        generated_at=generated_at.isoformat() if generated_at is not None else None,
        reasons=tuple(reasons or ["artifact fresh and passing"]),
    )


def _load_payload(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        lines = [line for line in text.splitlines() if line.strip()]
        return json.loads(lines[-1]) if lines else {}
    return json.loads(text)


def _artifact_generated_at(path: Path, payload: dict[str, object]) -> datetime | None:
    for key in ("generated_at", "report_generated_at", "fetched_at", "ts"):
        candidate = payload.get(key)
        if candidate in (None, ""):
            continue
        try:
            timestamp = datetime.fromisoformat(str(candidate))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return timestamp
        except ValueError:
            continue
    timestamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return timestamp
