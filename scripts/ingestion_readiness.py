from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.readiness_orchestrator import run_ingestion_readiness


def _parse_counts(value: str) -> tuple[int, ...]:
    if str(value).strip() == "":
        return ()
    return tuple(int(part.strip()) for part in str(value).split(",") if part.strip())


def _parse_stream_types(value: str | None) -> tuple[str, ...] | None:
    if value is None or str(value).strip() == "":
        return None
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ingestion readiness end-to-end for paper or live")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--env", default="dev")
    parser.add_argument("--runtime-env", default=None)
    parser.add_argument("--runtime-base-dir", default=None)
    parser.add_argument("--raw-base-dir", required=True)
    parser.add_argument("--normalized-path", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--stream-type", choices=["trade", "kline"], required=True)
    parser.add_argument("--gate-stream-types", default=None)
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--validation-dir", default="docs/validation")
    parser.add_argument("--output", default=None)
    parser.add_argument("--ws-max-events", type=int, default=2)
    parser.add_argument("--ws-duration-seconds", type=float, default=130.0)
    parser.add_argument("--ws-reconnect-after-events", type=int, default=1)
    parser.add_argument("--ws-induced-reconnects", type=int, default=1)
    parser.add_argument("--benchmark-symbol-count", type=int, default=12)
    parser.add_argument("--benchmark-high-cardinality-symbol-counts", type=_parse_counts, default=(100, 500))
    parser.add_argument("--benchmark-bursts", type=int, default=4)
    parser.add_argument("--benchmark-events-per-symbol-per-burst", type=int, default=12)
    parser.add_argument("--benchmark-min-rows-per-second", type=float, default=None)
    parser.add_argument("--soak-mode", choices=["deterministic", "ws-live"], default="ws-live")
    parser.add_argument("--soak-iterations", type=int, default=5)
    parser.add_argument("--soak-events-per-iteration", type=int, default=500)
    parser.add_argument("--soak-duration-seconds", type=float, default=150.0)
    parser.add_argument("--soak-reconnect-after-events", type=int, default=1)
    parser.add_argument("--soak-induced-reconnects", type=int, default=1)
    parser.add_argument("--provenance-source", default="readiness_orchestrator")
    parser.add_argument("--execution-ref", default=None)
    parser.add_argument("--channel", choices=["manual", "scheduled", "pipeline"], default="pipeline")
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
    validation_dir = Path(args.validation_dir)
    stream_type = str(args.stream_type).strip().lower()
    if args.target == "live":
        default_profile = f"live_{stream_type}"
    else:
        default_profile = f"paper_{stream_type}"
    output_path = Path(args.output) if args.output else validation_dir / f"ingestion_readiness_{default_profile}.json"
    report = run_ingestion_readiness(
        workspace=Path.cwd(),
        target=args.target,
        env=args.env,
        raw_base_dir=Path(args.raw_base_dir),
        normalized_path=Path(args.normalized_path),
        symbol=args.symbol,
        stream_type=stream_type,
        interval=args.interval,
        runtime_env=args.runtime_env,
        runtime_base_dir=Path(args.runtime_base_dir) if args.runtime_base_dir else None,
        gate_stream_types=_parse_stream_types(args.gate_stream_types),
        validation_dir=validation_dir,
        output_path=output_path,
        ws_max_events=args.ws_max_events,
        ws_duration_seconds=args.ws_duration_seconds,
        ws_reconnect_after_events=args.ws_reconnect_after_events,
        ws_induced_reconnects=args.ws_induced_reconnects,
        benchmark_symbol_count=args.benchmark_symbol_count,
        benchmark_high_cardinality_symbol_counts=args.benchmark_high_cardinality_symbol_counts,
        benchmark_bursts=args.benchmark_bursts,
        benchmark_events_per_symbol_per_burst=args.benchmark_events_per_symbol_per_burst,
        benchmark_min_rows_per_second=args.benchmark_min_rows_per_second,
        soak_mode=args.soak_mode,
        soak_iterations=args.soak_iterations,
        soak_events_per_iteration=args.soak_events_per_iteration,
        soak_duration_seconds=args.soak_duration_seconds,
        soak_reconnect_after_events=args.soak_reconnect_after_events,
        soak_induced_reconnects=args.soak_induced_reconnects,
        provenance_source=args.provenance_source,
        execution_ref=args.execution_ref,
        channel=args.channel,
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
    print(f"ingestion readiness: {report.overall_status} ({report.target})")
    for step in report.steps:
        print(f"- {step.name}: {'PASS' if step.pass_ok else 'FAIL'}")
        if step.artifact_path:
            print(f"  artifact: {step.artifact_path}")
    print(f"- report_path: {output_path}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
