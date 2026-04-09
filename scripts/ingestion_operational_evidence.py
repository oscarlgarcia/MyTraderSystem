from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.operational_evidence import build_operational_evidence_report, write_operational_evidence_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build aggregated operational evidence for ingestion promotion")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--phase", choices=["predrill", "final"], default="final")
    parser.add_argument("--stream-types", default="kline")
    parser.add_argument("--output", default="docs/validation/ingestion_operational_evidence.json")
    parser.add_argument("--rest-canary-path", default="docs/validation/ingestion_canary_report.json")
    parser.add_argument("--ws-canary-path", default="docs/validation/ingestion_ws_canary_report.json")
    parser.add_argument("--replay-parity-path", default="docs/validation/ingestion_replay_parity.json")
    parser.add_argument("--benchmark-path", default="docs/validation/ingestion_storage_benchmark.json")
    parser.add_argument("--soak-path", default="docs/validation/ingestion_soak_evidence.json")
    parser.add_argument("--network-contracts-path", default="docs/validation/ingestion_vendor_contracts.json")
    parser.add_argument("--failure-injection-path", default="docs/validation/ingestion_failure_injection.json")
    parser.add_argument("--live-drill-path", default="docs/validation/ingestion_live_drill_report.json")
    parser.add_argument("--provenance-source", default="scripted_operational_evidence")
    parser.add_argument("--runner-id", default="ingestion_operational_evidence")
    parser.add_argument("--trigger", default="manual")
    parser.add_argument("--execution-ref", default="manual-local")
    parser.add_argument("--channel", default="manual", choices=["manual", "scheduled", "pipeline"])
    parser.add_argument("--observability-verification-path", default=None)
    parser.add_argument("--runner-governance-path", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    stream_types = tuple(part.strip() for part in str(args.stream_types).split(",") if part.strip())
    if "book" in {stream_type.lower() for stream_type in stream_types}:
        raise SystemExit("operational evidence does not support stream_type=book")
    report = build_operational_evidence_report(
        target=args.target,
        phase=args.phase,
        stream_types=stream_types,
        rest_canary_path=Path(args.rest_canary_path),
        ws_canary_path=Path(args.ws_canary_path),
        replay_parity_path=Path(args.replay_parity_path),
        benchmark_path=Path(args.benchmark_path),
        soak_path=Path(args.soak_path),
        network_contracts_path=Path(args.network_contracts_path),
        failure_injection_path=Path(args.failure_injection_path),
        live_drill_path=Path(args.live_drill_path),
        provenance_source=str(args.provenance_source),
        runner_id=str(args.runner_id),
        trigger=str(args.trigger),
        execution_ref=str(args.execution_ref),
        channel=str(args.channel),
        observability_verification_path=Path(args.observability_verification_path) if args.observability_verification_path else None,
        runner_governance_path=Path(args.runner_governance_path) if args.runner_governance_path else None,
    )
    output_path = Path(args.output)
    write_operational_evidence_report(output_path, report)
    print(f"ingestion operational evidence: {'PASS' if report.pass_ok else 'FAIL'} ({report.target}/{report.phase})")
    print(f"- report_path: {output_path}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
