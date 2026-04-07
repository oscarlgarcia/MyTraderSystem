from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.config import load_config
from app.ops.release_gates import render_release_gate_summary, run_release_gates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ingestion release gates and persist the JSON report")
    parser.add_argument("--base-dir", default=None)
    parser.add_argument("--env", default="dev")
    parser.add_argument("--target", choices=["paper", "live"], default="paper")
    parser.add_argument("--phase", choices=["predrill", "final"], default="final")
    parser.add_argument("--stream-types", default="kline")
    parser.add_argument("--output", default="docs/validation/ingestion_release_gates.json")
    parser.add_argument("--rest-canary-path", default="docs/validation/ingestion_canary_report.json")
    parser.add_argument("--ws-canary-path", default="docs/validation/ingestion_ws_canary_report.json")
    parser.add_argument("--replay-parity-path", default="docs/validation/ingestion_replay_parity.json")
    parser.add_argument("--benchmark-path", default="docs/validation/ingestion_storage_benchmark.json")
    parser.add_argument("--soak-path", default="docs/validation/ingestion_soak_evidence.json")
    parser.add_argument("--network-contracts-path", default="docs/validation/ingestion_vendor_contracts.json")
    parser.add_argument("--failure-injection-path", default="docs/validation/ingestion_failure_injection.json")
    parser.add_argument("--live-drill-path", default="docs/validation/ingestion_live_drill_report.json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    base_dir = Path(args.base_dir) if args.base_dir else Path(load_config(args.env).data_dir)
    report = run_release_gates(
        base_dir=base_dir,
        env=args.env,
        target=args.target,
        stream_types=tuple(part.strip() for part in str(args.stream_types).split(",") if part.strip()),
        output_path=Path(args.output),
        rest_canary_path=Path(args.rest_canary_path),
        ws_canary_path=Path(args.ws_canary_path),
        replay_parity_path=Path(args.replay_parity_path),
        benchmark_path=Path(args.benchmark_path),
        soak_path=Path(args.soak_path),
        network_contracts_path=Path(args.network_contracts_path),
        failure_injection_path=Path(args.failure_injection_path),
        live_drill_path=Path(args.live_drill_path),
        require_live_drill=not (args.target == "live" and args.phase == "predrill"),
    )
    print(render_release_gate_summary(report))
    print(f"- phase: {args.phase}")
    print(f"- report_path: {args.output}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
