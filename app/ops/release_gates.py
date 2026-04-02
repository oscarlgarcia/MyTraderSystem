from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from app.config import DEFAULT_INGEST_STREAM_TYPES
from app.ingestion.storage_health import collect_storage_health, storage_health_payload
from app.marketdata.support_matrix import FeedSupport, feed_support, normalize_feed_types
from app.ops.observability_contract import build_observability_contract_report


GateStatus = Literal["pass", "fail", "warn"]
ReleaseTarget = Literal["paper", "live"]


@dataclass(frozen=True, slots=True)
class GateBlockReport:
    name: str
    status: GateStatus
    required: bool
    reasons: tuple[str, ...]
    details: dict[str, object]

    @property
    def pass_ok(self) -> bool:
        return self.status == "pass" or (self.status == "warn" and not self.required)


@dataclass(frozen=True, slots=True)
class ReleaseGateReport:
    target: ReleaseTarget
    env: str
    generated_at: str
    overall_status: Literal["PASS", "FAIL"]
    pass_ok: bool
    blocks: tuple[GateBlockReport, ...]


def write_release_gate_report(path: Path, report: ReleaseGateReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(report), handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
    return path


def render_release_gate_summary(report: ReleaseGateReport) -> str:
    lines = [f"Release gates: {report.overall_status} ({report.target})"]
    for block in report.blocks:
        prefix = block.status.upper()
        suffix = "required" if block.required else "informational"
        reason = "; ".join(block.reasons) if block.reasons else "ok"
        lines.append(f"- {block.name}: {prefix} [{suffix}] - {reason}")
    return "\n".join(lines)


def run_release_gates(
    *,
    base_dir: Path,
    env: str,
    target: ReleaseTarget,
    stream_types: tuple[str, ...] | list[str] | None = DEFAULT_INGEST_STREAM_TYPES,
    output_path: Path | None = None,
    rest_canary_path: Path | None = None,
    ws_canary_path: Path | None = None,
    replay_parity_path: Path | None = None,
    benchmark_path: Path | None = None,
    soak_path: Path | None = None,
    network_contracts_path: Path | None = None,
    live_drill_path: Path | None = None,
) -> ReleaseGateReport:
    normalized_stream_types = normalize_feed_types(stream_types)
    blocks = (
        _support_matrix_block(normalized_stream_types, target=target),
        _exact_recovery_block(normalized_stream_types, target=target),
        _live_scope_block(normalized_stream_types, target=target),
        _metadata_snapshot_block(base_dir=Path(base_dir), env=env, required=True, target=target),
        _canary_block(
            name="canary_rest",
            path=Path(rest_canary_path or "docs/validation/ingestion_canary_report.json"),
            required=True,
            expected_keys=("pass_ok", "diffs", "comparison_reason"),
            bool_paths=(("pass_ok",),),
            max_age=timedelta(hours=24),
        ),
        _canary_block(
            name="canary_ws",
            path=Path(ws_canary_path or "docs/validation/ingestion_ws_canary_report.json"),
            required=True,
            expected_keys=("pass_ok", "continuity", "reconnects_observed"),
            bool_paths=(("pass_ok",),),
            extra_checks=_validate_ws_canary_payload,
            max_age=timedelta(hours=24),
        ),
        _canary_block(
            name="storage_benchmark",
            path=Path(benchmark_path or "docs/validation/ingestion_storage_benchmark.json"),
            required=True,
            expected_keys=("pass_ok", "slo"),
            bool_paths=(("pass_ok",),),
            max_age=timedelta(days=7),
        ),
        _canary_block(
            name="replay_parity",
            path=Path(replay_parity_path or "docs/validation/ingestion_replay_parity.json"),
            required=True,
            expected_keys=("pass_ok", "order_match", "manifest_ok"),
            bool_paths=(("pass_ok",), ("order_match",), ("manifest_ok",)),
            extra_checks=_validate_replay_parity_payload,
            max_age=timedelta(days=7),
        ),
        _canary_block(
            name="paper_soak",
            path=Path(soak_path or "docs/validation/ingestion_soak_evidence.json"),
            required=(target in {"paper", "live"}),
            expected_keys=("pass_ok", "max_gaps", "max_gap_irreparable"),
            bool_paths=(("pass_ok",),),
            extra_checks=_validate_soak_payload,
            max_age=timedelta(hours=24),
        ),
        _canary_block(
            name="vendor_contracts",
            path=Path(network_contracts_path or "docs/validation/ingestion_vendor_contracts.json"),
            required=(target in {"paper", "live"}),
            expected_keys=("pass_ok", "command", "returncode"),
            bool_paths=(("pass_ok",),),
            max_age=timedelta(hours=24),
        ),
        _observability_contract_block(required=(target in {"paper", "live"})),
        _canary_block(
            name="live_drill",
            path=Path(live_drill_path or "docs/validation/ingestion_live_drill_report.json"),
            required=(target == "live"),
            expected_keys=("drill_executed", "promote_ready", "rollback_ready", "overall_status"),
            bool_paths=(("drill_executed",), ("promote_ready",), ("rollback_ready",)),
            extra_checks=_validate_live_drill_payload,
            max_age=timedelta(hours=24),
        ),
        _shadow_block(base_dir=Path(base_dir), env=env, required=(target == "live")),
        _storage_health_block(base_dir=Path(base_dir), env=env, required=True),
    )
    pass_ok = all(block.pass_ok for block in blocks)
    report = ReleaseGateReport(
        target=target,
        env=env,
        generated_at=datetime.now(timezone.utc).isoformat(),
        overall_status="PASS" if pass_ok else "FAIL",
        pass_ok=pass_ok,
        blocks=blocks,
    )
    if output_path is not None:
        write_release_gate_report(Path(output_path), report)
    return report


def _support_matrix_block(stream_types: tuple[str, ...], *, target: ReleaseTarget) -> GateBlockReport:
    del target
    reasons: list[str] = []
    details: dict[str, object] = {"feeds": {}}
    for stream_type in stream_types:
        support = feed_support(stream_type)
        details["feeds"][stream_type] = _feed_support_payload(support)
        if not support.supports_live:
            reasons.append(f"{stream_type} does not support live ingestion")
        if not support.supports_handoff:
            reasons.append(f"{stream_type} does not support historical-to-live handoff")
    status: GateStatus = "pass" if not reasons else "fail"
    return GateBlockReport(
        name="support_matrix",
        status=status,
        required=True,
        reasons=tuple(reasons or ["support matrix aligned"]),
        details=details,
    )


def _exact_recovery_block(stream_types: tuple[str, ...], *, target: ReleaseTarget) -> GateBlockReport:
    reasons: list[str] = []
    details: dict[str, object] = {"feeds": {}}
    for stream_type in stream_types:
        support = feed_support(stream_type)
        details["feeds"][stream_type] = {
            "recovery_capability": support.recovery_capability,
            "supports_exact_recovery": support.supports_exact_recovery,
            "supports_exact_verified_recovery": support.supports_exact_verified_recovery,
        }
        if target == "live":
            if not support.supports_exact_verified_recovery:
                reasons.append(f"{stream_type} is not exact_verified")
        else:
            if not support.supports_exact_verified_recovery:
                reasons.append(f"{stream_type} exact recovery suite still pending")
    if target == "live":
        status: GateStatus = "pass" if not reasons else "fail"
        required = True
    else:
        status = "pass" if not reasons else "warn"
        required = False
    return GateBlockReport(
        name="exact_recovery",
        status=status,
        required=required,
        reasons=tuple(reasons or ["exact recovery capability aligned"]),
        details=details,
    )


def _live_scope_block(stream_types: tuple[str, ...], *, target: ReleaseTarget) -> GateBlockReport:
    if target != "live":
        return GateBlockReport(
            name="live_scope",
            status="pass",
            required=False,
            reasons=("live scope not enforced for non-live targets",),
            details={"allowed_live_stream_types": ["kline"], "requested_stream_types": list(stream_types)},
        )
    disallowed = [stream_type for stream_type in stream_types if stream_type != "kline"]
    status: GateStatus = "pass" if not disallowed else "fail"
    reasons = ["live scope limited to kline feeds"] if not disallowed else [f"live scope forbids stream types: {', '.join(disallowed)}"]
    return GateBlockReport(
        name="live_scope",
        status=status,
        required=True,
        reasons=tuple(reasons),
        details={"allowed_live_stream_types": ["kline"], "requested_stream_types": list(stream_types)},
    )


def _canary_block(
    *,
    name: str,
    path: Path,
    required: bool,
    expected_keys: tuple[str, ...],
    bool_paths: tuple[tuple[str, ...], ...],
    extra_checks=None,
    max_age: timedelta | None = None,
) -> GateBlockReport:
    if not path.exists():
        return GateBlockReport(
            name=name,
            status="fail" if required else "warn",
            required=required,
            reasons=(f"missing artifact: {path}",),
            details={"path": str(path)},
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    reasons: list[str] = []
    for key in expected_keys:
        if key not in payload:
            reasons.append(f"missing key: {key}")
    for bool_path in bool_paths:
        value = payload
        for key in bool_path:
            if not isinstance(value, dict) or key not in value:
                value = None
                break
            value = value[key]
        if value is not True:
            reasons.append(f"{'.'.join(bool_path)} is not true")
    age = _artifact_age(path, payload)
    if max_age is not None and age is not None and age > max_age:
        reasons.append(f"artifact stale: {path} older than {int(max_age.total_seconds())}s")
    if callable(extra_checks):
        reasons.extend(extra_checks(payload))
    status: GateStatus = "pass" if not reasons else ("fail" if required else "warn")
    return GateBlockReport(
        name=name,
        status=status,
        required=required,
        reasons=tuple(reasons or ["artifact validated"]),
        details={
            "path": str(path),
            "pass_ok": bool(payload.get("pass_ok", False)),
            "comparison_reason": payload.get("comparison_reason"),
            "artifact_age_seconds": age.total_seconds() if age is not None else None,
        },
    )


def _validate_ws_canary_payload(payload: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    continuity = payload.get("continuity")
    if not isinstance(continuity, dict):
        return ["continuity payload missing"]
    for key in ("reconnects", "duplicates", "gaps"):
        if key not in continuity:
            reasons.append(f"continuity.{key} missing")
    reconnects_observed = payload.get("reconnects_observed")
    reconnects_target = payload.get("reconnects_target", 0)
    try:
        if int(reconnects_observed) < int(reconnects_target):
            reasons.append("reconnect target not met")
    except (TypeError, ValueError):
        reasons.append("invalid reconnect counters")
    return reasons


def _validate_live_drill_payload(payload: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    if payload.get("overall_status") != "PASS":
        reasons.append("overall_status is not PASS")
    return reasons


def _validate_replay_parity_payload(payload: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    if payload.get("manifest_missing_files"):
        reasons.append("raw manifest files missing")
    if payload.get("manifest_mismatches"):
        reasons.append("raw manifest mismatches detected")
    return reasons


def _validate_soak_payload(payload: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    if int(payload.get("max_gap_irreparable", 0) or 0) > 0:
        reasons.append("gap_irreparable observed during soak")
    if int(payload.get("max_gaps", 0) or 0) > 0:
        reasons.append("gaps observed during soak")
    return reasons


def _artifact_age(path: Path, payload: dict[str, object]) -> timedelta | None:
    candidates = [
        payload.get("generated_at"),
        payload.get("report_generated_at"),
        payload.get("fetched_at"),
    ]
    timestamp: datetime | None = None
    for candidate in candidates:
        if candidate in (None, ""):
            continue
        try:
            timestamp = datetime.fromisoformat(str(candidate))
            break
        except ValueError:
            continue
    if timestamp is None:
        timestamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timestamp


def _metadata_snapshot_block(*, base_dir: Path, env: str, required: bool, target: ReleaseTarget) -> GateBlockReport:
    path = base_dir / "metadata" / "instruments" / f"env={env}" / "venue=BINANCE" / "latest.json"
    if not path.exists():
        return GateBlockReport(
            name="instrument_metadata",
            status="fail" if required else "warn",
            required=required,
            reasons=(f"missing instrument metadata artifact: {path}",),
            details={"path": str(path)},
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    reasons: list[str] = []
    mode = str(payload.get("metadata_snapshot_mode") or "bundled")
    drift_payload = payload.get("drift") or {}
    material_drift = bool(isinstance(drift_payload, dict) and drift_payload.get("material"))
    if target == "live" and mode != "runtime":
        reasons.append("live requires runtime instrument metadata snapshot")
    elif target == "paper" and mode != "runtime":
        reasons.append(f"paper is running with metadata snapshot mode={mode}")
    if material_drift:
        reasons.append("material provider metadata drift detected")
    if mode == "fallback" and payload.get("fallback_reason") in (None, ""):
        reasons.append("fallback metadata snapshot missing fallback_reason")
    status: GateStatus = "pass" if not reasons else ("fail" if required else "warn")
    return GateBlockReport(
        name="instrument_metadata",
        status=status,
        required=required,
        reasons=tuple(reasons or ["instrument metadata snapshot valid"]),
        details={
            "path": str(path),
            "metadata_snapshot_mode": mode,
            "venue_snapshot_path": payload.get("venue_snapshot_path"),
            "venue_snapshot_version": payload.get("venue_snapshot_version"),
            "venue_snapshot_sha256": payload.get("venue_snapshot_sha256"),
            "fallback_reason": payload.get("fallback_reason"),
            "drift": drift_payload,
        },
    )


def _shadow_block(*, base_dir: Path, env: str, required: bool) -> GateBlockReport:
    path = base_dir / "shadow" / f"env={env}" / "comparisons.jsonl"
    if not path.exists():
        return GateBlockReport(
            name="shadow_diffs",
            status="fail" if required else "warn",
            required=required,
            reasons=(f"missing shadow comparison artifact: {path}",),
            details={"path": str(path)},
        )
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return GateBlockReport(
            name="shadow_diffs",
            status="fail" if required else "warn",
            required=required,
            reasons=("shadow comparison artifact is empty",),
            details={"path": str(path)},
        )
    payload = json.loads(lines[-1])
    significant = bool(payload.get("significant", False))
    diffs = payload.get("diffs", {})
    reasons = ["shadow comparison clean"] if not significant else ["shadow comparison has significant diffs"]
    status: GateStatus = "pass" if not significant else "fail"
    return GateBlockReport(
        name="shadow_diffs",
        status=status,
        required=required,
        reasons=tuple(reasons),
        details={
            "path": str(path),
            "significant": significant,
            "diffs": diffs,
            "ts": payload.get("ts"),
        },
    )


def _storage_health_block(*, base_dir: Path, env: str, required: bool) -> GateBlockReport:
    report = collect_storage_health(base_dir, env)
    reasons: list[str] = []
    if report.compaction_failures_total > 0:
        reasons.append("compaction failures detected")
    if report.compaction_lag_seconds >= 900.0:
        reasons.append("compaction lag exceeds critical threshold")
    status: GateStatus = "pass" if not reasons else "fail"
    return GateBlockReport(
        name="storage_health",
        status=status,
        required=required,
        reasons=tuple(reasons or ["storage health clean"]),
        details=storage_health_payload(report),
    )


def _observability_contract_block(*, required: bool) -> GateBlockReport:
    report = build_observability_contract_report()
    reasons = ["observability contract defined"] if report.pass_ok else ["missing required alert definitions"]
    return GateBlockReport(
        name="observability_contract",
        status="pass" if report.pass_ok else ("fail" if required else "warn"),
        required=required,
        reasons=tuple(reasons),
        details={
            "required_metrics": list(report.required_metrics),
            "required_alerts": list(report.required_alerts),
            "missing_alerts": list(report.missing_alerts),
        },
    )


def _feed_support_payload(support: FeedSupport) -> dict[str, object]:
    return {
        "feed_type": support.feed_type,
        "supports_live": support.supports_live,
        "supports_handoff": support.supports_handoff,
        "recovery_capability": support.recovery_capability,
        "supports_exact_recovery": support.supports_exact_recovery,
        "supports_exact_verified_recovery": support.supports_exact_verified_recovery,
    }
