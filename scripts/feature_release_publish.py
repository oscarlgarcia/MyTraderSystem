from __future__ import annotations

import json
import sys
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_workflow import gate_and_publish_feature_release, rollback_feature_release


def _load_gate_inputs(path: str | Path) -> tuple[ParityReport, FeatureMetrics]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    mismatches = int(payload.get("parity_mismatches", 0))
    stale_serves = int(payload.get("stale_serves", 0))
    serving_latency_max = float(payload.get("serving_latency_max", 0.0))
    parity_report = ParityReport(pass_ok=mismatches == 0, mismatches=tuple(object() for _ in range(mismatches)))
    metrics = FeatureMetrics(stale_serves=stale_serves, serving_latency_max=serving_latency_max)
    return parity_report, metrics


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        raise SystemExit(
            "usage: feature_release_publish.py <publish|rollback> <registry-path> <feature-set-name> [version] [target] [gate-input-json]"
        )
    action = argv[1]
    registry_path = Path(argv[2])
    feature_set_name = argv[3]
    if action == "publish":
        if len(argv) < 7:
            raise SystemExit("publish requires version, target and gate-input-json")
        version = argv[4]
        target = argv[5]
        parity_report, metrics = _load_gate_inputs(argv[6])
        gate_and_publish_feature_release(
            registry_path=registry_path,
            feature_set_name=feature_set_name,
            version=version,
            parity_report=parity_report,
            metrics=metrics,
            target=target,
            actor="scripts.feature_release_publish",
        )
        return 0
    if action == "rollback":
        target = argv[4] if len(argv) >= 5 else None
        rollback_feature_release(
            registry_path=registry_path,
            feature_set_name=feature_set_name,
            target=target,
            actor="scripts.feature_release_publish",
        )
        return 0
    raise SystemExit(f"unknown action {action}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
