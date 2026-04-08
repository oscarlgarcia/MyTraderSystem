from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.feature_release_gates import render_feature_release_summary, run_feature_release_gates


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run feature release gates from precomputed artifacts.")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
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
    parser.add_argument("--evidence-manifest-path")
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv[1:])
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
        evidence_manifest_path=Path(args.evidence_manifest_path) if args.evidence_manifest_path else None,
        output_path=Path(args.output),
    )
    print(render_feature_release_summary(report))
    return 0 if report.pass_ok else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
