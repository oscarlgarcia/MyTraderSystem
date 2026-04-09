from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.operational_cycle import run_ingestion_operational_cycle


def _parse_stream_types(value: str) -> tuple[str, ...]:
    items = tuple(part.strip() for part in str(value).split(",") if part.strip())
    if not items:
        raise argparse.ArgumentTypeError("stream types cannot be empty")
    return items


def _parse_normalized_path_mapping(values: list[str] | None) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for raw in values or []:
        if "=" not in raw:
            raise argparse.ArgumentTypeError(f"normalized path mapping must use feed=path syntax: {raw}")
        stream_type, path = raw.split("=", 1)
        normalized_stream_type = stream_type.strip().lower()
        normalized_path = Path(path.strip())
        if not normalized_stream_type or not str(normalized_path).strip():
            raise argparse.ArgumentTypeError(f"normalized path mapping must use feed=path syntax: {raw}")
        mapping[normalized_stream_type] = normalized_path
    return mapping


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the standard operational closure cycle for ingestion paper/live readiness")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--env", default="dev")
    parser.add_argument("--runtime-env", default=None)
    parser.add_argument("--runtime-base-dir", default=None)
    parser.add_argument("--raw-base-dir", required=True)
    parser.add_argument("--normalized-path", action="append", default=[], help="Repeat feed=path mappings, e.g. trade=data/... kline=data/...")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--stream-types", type=_parse_stream_types, required=True)
    parser.add_argument("--interval", default="1m")
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
    parser.add_argument("--runtime-owner", default=None)
    parser.add_argument("--runtime-surface-ref", default=None)
    parser.add_argument("--runtime-verification-ref", default=None)
    parser.add_argument("--alerts-owner", default=None)
    parser.add_argument("--alerts-surface-ref", default=None)
    parser.add_argument("--alerts-verification-ref", default=None)
    parser.add_argument("--logs-owner", default=None)
    parser.add_argument("--logs-surface-ref", default=None)
    parser.add_argument("--logs-verification-ref", default=None)
    parser.add_argument("--promotion-owner", default=None)
    parser.add_argument("--promotion-surface-ref", default=None)
    parser.add_argument("--promotion-verification-ref", default=None)
    parser.add_argument("--cutover-owner", default=None)
    parser.add_argument("--cutover-surface-ref", default=None)
    parser.add_argument("--cutover-verification-ref", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = run_ingestion_operational_cycle(
        workspace=Path.cwd(),
        target=args.target,
        env=args.env,
        runtime_env=args.runtime_env,
        runtime_base_dir=Path(args.runtime_base_dir) if args.runtime_base_dir else None,
        raw_base_dir=Path(args.raw_base_dir),
        normalized_paths=_parse_normalized_path_mapping(args.normalized_path),
        symbol=args.symbol,
        interval=args.interval,
        output_dir=Path(args.output_dir),
        output_path=Path(args.output) if args.output else None,
        runner_id=args.runner_id,
        trigger=args.trigger,
        provenance_source=args.provenance_source,
        execution_ref=args.execution_ref,
        channel=args.channel,
        schedule_name=args.schedule_name,
        job_id=args.job_id,
        job_url=args.job_url,
        owner=args.owner,
        stream_types=args.stream_types,
        runtime_owner=args.runtime_owner,
        runtime_surface_ref=args.runtime_surface_ref,
        runtime_verification_ref=args.runtime_verification_ref,
        alerts_owner=args.alerts_owner,
        alerts_surface_ref=args.alerts_surface_ref,
        alerts_verification_ref=args.alerts_verification_ref,
        logs_owner=args.logs_owner,
        logs_surface_ref=args.logs_surface_ref,
        logs_verification_ref=args.logs_verification_ref,
        promotion_owner=args.promotion_owner,
        promotion_surface_ref=args.promotion_surface_ref,
        promotion_verification_ref=args.promotion_verification_ref,
        cutover_owner=args.cutover_owner,
        cutover_surface_ref=args.cutover_surface_ref,
        cutover_verification_ref=args.cutover_verification_ref,
    )
    print(f"ingestion operational cycle: {report.overall_status} ({report.target})")
    print(f"- execution_ref: {report.execution_ref}")
    print(f"- channel: {report.channel}")
    print(f"- cadence_state: {report.cadence_state}")
    print(f"- governance_artifact_path: {report.governance_artifact_path}")
    print(f"- stream_types: {', '.join(report.stream_types)}")
    print(f"- output_dir: {report.output_dir}")
    for step in report.steps:
        print(f"- {step.profile}: {'PASS' if step.pass_ok else 'FAIL'}")
        print(f"  report_path: {step.report_path}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
