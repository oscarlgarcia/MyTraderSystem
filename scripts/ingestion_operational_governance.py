from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.operational_governance import build_operational_governance_report, write_operational_governance_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persist runner governance and cadence state for ingestion operational closure")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--output-dir", default="docs/validation")
    parser.add_argument("--output", default=None)
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--trigger", required=True)
    parser.add_argument("--provenance-source", required=True)
    parser.add_argument("--execution-ref", required=True)
    parser.add_argument("--channel", choices=["manual", "scheduled", "pipeline"], required=True)
    parser.add_argument("--schedule-name", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--job-url", required=True)
    parser.add_argument("--owner", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_path = Path(args.output) if args.output else output_dir / f"ingestion_operational_governance_{args.target}.json"
    report = build_operational_governance_report(
        target=args.target,
        output_dir=output_dir,
        runner_id=args.runner_id,
        trigger=args.trigger,
        provenance_source=args.provenance_source,
        execution_ref=args.execution_ref,
        channel=args.channel,
        schedule_name=args.schedule_name,
        job_id=args.job_id,
        job_url=args.job_url,
        owner=args.owner,
        governance_artifact_path=output_path,
    )
    write_operational_governance_report(output_path, report)
    print(f"ingestion operational governance: {'PASS' if report.pass_ok else 'FAIL'} ({report.target})")
    print(f"- cadence_state: {report.cadence_state}")
    print(f"- report_path: {output_path}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
