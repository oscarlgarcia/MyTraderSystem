from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.features.live_readiness import FeatureLiveReadinessDecision
from app.features.release_workflow import publish_feature_release
from app.ops.feature_release_gates import render_feature_release_summary, run_feature_release_gates


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run feature go/no-go gates and optionally publish.")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--registry-path", required=True)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--feature-set-version", required=True)
    parser.add_argument("--parity-path", required=True)
    parser.add_argument("--benchmark-path", required=True)
    parser.add_argument("--observability-path", required=True)
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--online-backend", required=True)
    parser.add_argument("--observability-sink", required=True)
    parser.add_argument("--shadow-path")
    parser.add_argument("--soak-path")
    parser.add_argument("--concurrency-path")
    parser.add_argument("--rollout-audit-path")
    parser.add_argument("--gates-output", required=True)
    parser.add_argument("--publish", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    report = run_feature_release_gates(
        target=args.target,
        parity_path=Path(args.parity_path),
        benchmark_path=Path(args.benchmark_path),
        observability_path=Path(args.observability_path),
        contract_path=Path(args.contract_path),
        online_backend=args.online_backend,
        observability_sink=args.observability_sink,
        shadow_path=Path(args.shadow_path) if args.shadow_path else None,
        soak_path=Path(args.soak_path) if args.soak_path else None,
        concurrency_path=Path(args.concurrency_path) if args.concurrency_path else None,
        rollout_audit_path=Path(args.rollout_audit_path) if args.rollout_audit_path else None,
        output_path=Path(args.gates_output),
    )
    print(render_feature_release_summary(report))
    if not report.pass_ok:
        return 1
    if not args.publish:
        return 0
    live_readiness = None
    if report.live_readiness is not None:
        live_readiness = FeatureLiveReadinessDecision(
            pass_ok=bool(report.live_readiness.get("pass_ok", False)),
            action=str(report.live_readiness.get("action", "")),
            reasons=tuple(str(reason) for reason in report.live_readiness.get("reasons", [])),
        )
    publish_feature_release(
        registry_path=Path(args.registry_path),
        feature_set_name=args.feature_set_name,
        version=args.feature_set_version,
        gate_report=report.gate_report,
        target=report.target,
        actor="scripts.feature_live_go_no_go",
        live_readiness=live_readiness,
    )
    print(f"feature_release published {args.feature_set_name}:{args.feature_set_version} target={args.target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
