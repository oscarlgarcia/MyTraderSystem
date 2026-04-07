from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.ingestion_validation import run_soak_validation


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestion soak validation for paper/live readiness")
    parser.add_argument("--target-profile", choices=["paper", "live"], default="paper")
    parser.add_argument("--mode", choices=["deterministic", "ws-live"], default="ws-live")
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--events-per-iteration", type=int, default=500)
    parser.add_argument("--duration-seconds", type=float, default=150.0)
    parser.add_argument("--pipeline-version", choices=["v1", "v2"], default="v2")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--stream-type", choices=["kline"], default="kline")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--ws-base", default="wss://stream.binance.com:9443")
    parser.add_argument("--rest-base", default="https://api.binance.com")
    parser.add_argument("--reconnect-after-events", type=int, default=1)
    parser.add_argument("--induced-reconnects", type=int, default=1)
    parser.add_argument("--max-allowed-gaps", type=int)
    parser.add_argument("--max-allowed-gap-irreparable", type=int, default=0)
    parser.add_argument("--max-allowed-compaction-failures", type=int, default=0)
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_soak_evidence.json",
        help="JSON evidence output path",
    )
    args = parser.parse_args()

    evidence = run_soak_validation(
        Path(args.output),
        target_profile=args.target_profile,
        mode=args.mode,
        iterations=args.iterations,
        events_per_iteration=args.events_per_iteration,
        duration_seconds=args.duration_seconds,
        pipeline_version=args.pipeline_version,
        symbol=args.symbol,
        stream_type=args.stream_type,
        interval=args.interval,
        ws_base=args.ws_base,
        rest_base=args.rest_base,
        reconnect_after_events=args.reconnect_after_events,
        induced_reconnects=args.induced_reconnects,
        max_allowed_gaps=args.max_allowed_gaps,
        max_allowed_gap_irreparable=args.max_allowed_gap_irreparable,
        max_allowed_compaction_failures=args.max_allowed_compaction_failures,
    )
    print(f"soak: {'PASS' if evidence.pass_ok else 'FAIL'}")
    print(f"- generated_at: {evidence.generated_at}")
    print(f"- mode: {evidence.mode}")
    print(f"- iterations: {evidence.iterations}")
    print(f"- reconnects_observed: {evidence.reconnects_observed}")
    print(f"- reconnects_target: {evidence.reconnects_target}")
    print(f"- max_allowed_gaps: {evidence.max_allowed_gaps}")
    print(f"- max_gaps: {evidence.max_gaps}")
    print(f"- max_allowed_gap_irreparable: {evidence.max_allowed_gap_irreparable}")
    print(f"- max_gap_irreparable: {evidence.max_gap_irreparable}")
    print(f"- max_allowed_compaction_failures: {evidence.max_allowed_compaction_failures}")
    print(f"- compaction_failures_total: {evidence.compaction_failures_total}")
    print(f"- report_path: {args.output}")
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
