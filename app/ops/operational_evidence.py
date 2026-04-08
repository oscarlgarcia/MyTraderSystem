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
    pass_ok: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationalEvidenceReport:
    generated_at: str
    target: ReleaseTarget
    phase: EvidencePhase
    stream_types: tuple[str, ...]
    cadence_policy: dict[str, object]
    evidence_origin: str
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
    observability = _collect_observability_evidence(target=target)
    reasons = [reason for artifact in artifacts for reason in artifact.reasons]
    reasons.extend(observability.reasons)
    pass_ok = all((artifact.pass_ok and artifact.fresh) if artifact.required else True for artifact in artifacts) and observability.pass_ok
    if not pass_ok and not reasons:
        reasons.append("operational evidence incomplete")
    cadence_policy = {
        "runtime_artifact_max_age_seconds": int(timedelta(hours=24).total_seconds()),
        "benchmark_and_replay_max_age_seconds": int(timedelta(days=7).total_seconds()),
        "required_runtime_artifacts": [artifact.name for artifact in artifacts if artifact.required and artifact.name in {"rest_canary", "ws_canary", "soak", "failure_injection", "live_drill"}],
        "required_artifacts": [artifact.name for artifact in artifacts if artifact.required],
    }
    evidence_origin = "operational_runtime" if target == "live" else "paper_operational"
    return OperationalEvidenceReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target=target,
        phase=phase,
        stream_types=normalized_stream_types,
        cadence_policy=cadence_policy,
        evidence_origin=evidence_origin,
        excluded_feed_policy={feed_type: "excluded" for feed_type in excluded_feed_types()},
        observability=observability,
        artifacts=artifacts,
        pass_ok=pass_ok,
        reasons=tuple(reasons or ["operational evidence fresh and aligned"]),
    )


def _collect_observability_evidence(*, target: ReleaseTarget) -> OperationalObservabilityEvidence:
    report = build_observability_contract_report(target=target)
    repo_runbooks = tuple(sorted({surface.repo_reference for surface in report.external_surfaces if surface.kind == "runbook"}))
    reasons: list[str] = []
    for repo_reference in repo_runbooks:
        if not Path(repo_reference).exists():
            reasons.append(f"missing observability runbook reference: {repo_reference}")
    surfaces = tuple(
        {
            "surface_id": surface.surface_id,
            "kind": surface.kind,
            "description": surface.description,
            "repo_reference": surface.repo_reference,
        }
        for surface in report.external_surfaces
    )
    if not surfaces:
        reasons.append("missing external observability surfaces")
    return OperationalObservabilityEvidence(
        external_surfaces=surfaces,
        repo_runbooks=repo_runbooks,
        pass_ok=report.pass_ok and not reasons,
        reasons=tuple(reasons or ["observability surfaces declared and runbooks present"]),
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
