from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.ingestion_validation import run_ws_live_canary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the live WS canary and persist a JSON artifact")
    parser.add_argument("--target-profile", choices=["paper", "live"], default="live")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--stream-type", choices=["kline"], default="kline")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--max-events", type=int, default=2)
    parser.add_argument("--duration-seconds", type=float, default=130.0)
    parser.add_argument("--reconnect-after-events", type=int, default=1)
    parser.add_argument("--induced-reconnects", type=int, default=1)
    parser.add_argument("--ws-base", default="wss://stream.binance.com:9443")
    parser.add_argument("--rest-base", default="https://api.binance.com")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_ws_canary_report.json",
        help="JSON artifact output path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence = run_ws_live_canary(
        Path(args.output),
        target_profile=args.target_profile,
        symbol=args.symbol,
        stream_type=args.stream_type,
        interval=args.interval,
        ws_base=args.ws_base,
        rest_base=args.rest_base,
        max_events=args.max_events,
        duration_seconds=args.duration_seconds,
        reconnect_after_events=args.reconnect_after_events,
        induced_reconnects=args.induced_reconnects,
    )
    print(f"ws canary: {'PASS' if evidence.pass_ok else 'FAIL'}")
    print(f"- report_generated_at: {evidence.report_generated_at}")
    print(f"- symbol: {evidence.symbol}")
    print(f"- stream_type: {evidence.stream_type}")
    print(f"- reconnects_observed: {evidence.reconnects_observed}")
    print(f"- gaps: {evidence.continuity['gaps']}")
    print(f"- gap_irreparable: {evidence.continuity['gap_irreparable']}")
    print(f"- report_path: {args.output}")
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
