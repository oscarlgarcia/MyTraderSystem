from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from app.features.live_readiness import FeatureLiveReadinessDecision, FeatureLiveReadinessInputs, evaluate_feature_live_readiness
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import FeatureReleaseGateReport, run_feature_release_gate
from app.features.shadow_summary import summarize_shadow_reports


GateStatus = Literal["pass", "fail", "warn"]
FeatureTarget = Literal["paper", "live"]


@dataclass(frozen=True, slots=True)
class FeatureGateBlockReport:
    name: str
    status: GateStatus
    required: bool
    reasons: tuple[str, ...]
    details: dict[str, object]

    @property
    def pass_ok(self) -> bool:
        return self.status == "pass" or (self.status == "warn" and not self.required)


@dataclass(frozen=True, slots=True)
class FeatureReleaseOpsReport:
    target: FeatureTarget
    generated_at: str
    overall_status: Literal["PASS", "FAIL"]
    pass_ok: bool
    gate_report: FeatureReleaseGateReport
    live_readiness: dict[str, object] | None
    blocks: tuple[FeatureGateBlockReport, ...]


def write_feature_release_report(path: Path, report: FeatureReleaseOpsReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def render_feature_release_summary(report: FeatureReleaseOpsReport) -> str:
    lines = [f"Feature release gates: {report.overall_status} ({report.target})"]
    for block in report.blocks:
        reason = "; ".join(block.reasons) if block.reasons else "ok"
        lines.append(f"- {block.name}: {block.status.upper()} - {reason}")
    return "\n".join(lines)


def run_feature_release_gates(
    *,
    target: FeatureTarget,
    parity_path: Path,
    benchmark_path: Path,
    observability_path: Path,
    contract_path: Path,
    online_backend: str,
    observability_sink: str,
    output_path: Path | None = None,
    shadow_path: Path | None = None,
    soak_path: Path | None = None,
    concurrency_path: Path | None = None,
    rollout_audit_path: Path | None = None,
) -> FeatureReleaseOpsReport:
    parity_payload = _read_json(parity_path)
    metrics_payload = _read_json(observability_path)
    benchmark_payload = _read_json(benchmark_path)
    contract_payload = _read_json(contract_path)

    parity_report = _parity_report_from_payload(parity_payload)
    metrics = _metrics_from_payload(metrics_payload)
    gate_report = run_feature_release_gate(parity_report=parity_report, metrics=metrics, target=target)

    blocks: list[FeatureGateBlockReport] = [
        _artifact_block(
            name="parity",
            payload=parity_payload,
            path=parity_path,
            required=True,
            expected_keys=("pass_ok",),
            pass_value=bool(parity_payload.get("pass_ok", False)),
            max_age=timedelta(days=7),
        ),
        _artifact_block(
            name="benchmark",
            payload=benchmark_payload,
            path=benchmark_path,
            required=True,
            expected_keys=("threshold_pass_ok",),
            pass_value=bool(benchmark_payload.get("threshold_pass_ok", benchmark_payload.get("pass_ok", False))),
            max_age=timedelta(days=7),
        ),
        _artifact_block(
            name="observability",
            payload=metrics_payload,
            path=observability_path,
            required=True,
            expected_keys=("metrics",),
            pass_value=True,
            max_age=timedelta(hours=24),
        ),
        _artifact_block(
            name="contract_validation",
            payload=contract_payload,
            path=contract_path,
            required=True,
            expected_keys=("pass_ok",),
            pass_value=bool(contract_payload.get("pass_ok", False)),
            max_age=timedelta(days=7),
        ),
    ]

    live_readiness: FeatureLiveReadinessDecision | None = None
    if target == "live":
        if shadow_path is None or soak_path is None or concurrency_path is None or rollout_audit_path is None:
            raise ValueError("live feature release gates require shadow, soak, concurrency and rollout audit artifacts")
        shadow_block, shadow_failures = _shadow_block(shadow_path)
        soak_payload = _read_json(soak_path)
        concurrency_payload = _read_json(concurrency_path)
        rollout_payload = _read_json(rollout_audit_path)
        soak_block = _artifact_block(
            name="serving_soak",
            payload=soak_payload,
            path=soak_path,
            required=True,
            expected_keys=("pass_ok", "max_latency_seconds"),
            pass_value=bool(soak_payload.get("pass_ok", False)),
            max_age=timedelta(hours=24),
        )
        concurrency_block = _artifact_block(
            name="serving_concurrency",
            payload=concurrency_payload,
            path=concurrency_path,
            required=True,
            expected_keys=("pass_ok", "max_latency_seconds"),
            pass_value=bool(concurrency_payload.get("pass_ok", False)),
            max_age=timedelta(hours=24),
        )
        rollout_block = _artifact_block(
            name="rollout_audit",
            payload=rollout_payload,
            path=rollout_audit_path,
            required=True,
            expected_keys=("pass_ok",),
            pass_value=bool(rollout_payload.get("pass_ok", False)),
            max_age=timedelta(days=7),
        )
        blocks.extend((shadow_block, soak_block, concurrency_block, rollout_block))
        live_readiness = evaluate_feature_live_readiness(
            inputs=FeatureLiveReadinessInputs(
                online_backend=online_backend,
                observability_sink=observability_sink,
                serving_soak_pass_ok=soak_block.pass_ok,
                rollout_audit_enabled=rollout_block.pass_ok,
                contract_validation_pass_ok=bool(contract_payload.get("pass_ok", False)),
                benchmark_pass_ok=bool(benchmark_payload.get("threshold_pass_ok", benchmark_payload.get("pass_ok", False))),
                shadow_failures=shadow_failures,
                invalid_ratio=float(metrics_payload.get("metrics", {}).get("invalid_serves", 0)) / max(
                    int(metrics_payload.get("metrics", {}).get("serving_requests", 0)),
                    1,
                ),
            )
        )

    pass_ok = gate_report.pass_ok and all(block.pass_ok for block in blocks) and (live_readiness.pass_ok if live_readiness is not None else True)
    report = FeatureReleaseOpsReport(
        target=target,
        generated_at=datetime.now(timezone.utc).isoformat(),
        overall_status="PASS" if pass_ok else "FAIL",
        pass_ok=pass_ok,
        gate_report=gate_report,
        live_readiness=(
            None
            if live_readiness is None
            else {
                "pass_ok": live_readiness.pass_ok,
                "action": live_readiness.action,
                "reasons": list(live_readiness.reasons),
            }
        ),
        blocks=tuple(blocks),
    )
    if output_path is not None:
        write_feature_release_report(Path(output_path), report)
    return report


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _read_timestamp(payload: dict[str, object]) -> datetime | None:
    for key in ("generated_at", "report_generated_at", "timestamp"):
        value = payload.get(key)
        if isinstance(value, str):
            return datetime.fromisoformat(value)
    return None


def _artifact_block(
    *,
    name: str,
    payload: dict[str, object],
    path: Path,
    required: bool,
    expected_keys: tuple[str, ...],
    pass_value: bool,
    max_age: timedelta,
) -> FeatureGateBlockReport:
    reasons: list[str] = []
    for key in expected_keys:
        if key not in payload:
            reasons.append(f"missing_key:{key}")
    timestamp = _read_timestamp(payload)
    if timestamp is not None and datetime.now(timezone.utc) - timestamp > max_age:
        reasons.append("artifact_stale")
    if not pass_value:
        reasons.append("artifact_not_pass_ok")
    status: GateStatus = "pass" if not reasons else "fail"
    return FeatureGateBlockReport(
        name=name,
        status=status,
        required=required,
        reasons=tuple(reasons or ["ok"]),
        details={"path": str(path)},
    )


def _parity_report_from_payload(payload: dict[str, object]) -> ParityReport:
    mismatches = int(payload.get("parity_mismatches", 0))
    if mismatches == 0 and payload.get("pass_ok") is False:
        mismatches = 1
    return ParityReport(pass_ok=mismatches == 0, mismatches=tuple(object() for _ in range(mismatches)))


def _metrics_from_payload(payload: dict[str, object]) -> FeatureMetrics:
    metrics_payload = payload.get("metrics", payload)
    if not isinstance(metrics_payload, dict):
        metrics_payload = {}
    return FeatureMetrics(
        serving_requests=int(metrics_payload.get("serving_requests", 0)),
        invalid_serves=int(metrics_payload.get("invalid_serves", 0)),
        stale_serves=int(metrics_payload.get("stale_serves", 0)),
        serving_latency_max=float(metrics_payload.get("serving_latency_max", 0.0)),
        parity_mismatches=int(metrics_payload.get("parity_mismatches", 0)),
        shadow_failures=int(metrics_payload.get("shadow_failures", 0)),
        contract_validation_failures=int(metrics_payload.get("contract_validation_failures", 0)),
    )


def _shadow_block(path: Path) -> tuple[FeatureGateBlockReport, int]:
    if path.suffix.lower() == ".jsonl":
        summary = summarize_shadow_reports(path)
        payload: dict[str, object] = asdict(summary)
    else:
        payload = _read_json(path)
    failed_reports = int(payload.get("failed_reports", 0))
    pass_value = bool(payload.get("pass_ok", False))
    block = _artifact_block(
        name="shadow_validation",
        payload=payload,
        path=path,
        required=True,
        expected_keys=("pass_ok",),
        pass_value=pass_value,
        max_age=timedelta(hours=24),
    )
    return block, failed_reports

