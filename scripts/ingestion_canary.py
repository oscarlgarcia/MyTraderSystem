from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from app.ops.ingestion_validation import run_canary_validation


def _parse_iso_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--end-time invalido: {value}") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--end-time debe incluir zona horaria (ej: 2026-04-02T10:00:00+00:00)")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Real-feed ingestion canary validation with persisted vendor baseline"
    )
    parser.add_argument("--baseline-version", choices=["v1", "v2"], default="v1")
    parser.add_argument("--candidate-version", choices=["v1", "v2"], default="v2")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--bars", type=int, default=30)
    parser.add_argument("--rest-base", default="https://api.binance.com")
    parser.add_argument("--refresh-baseline", action="store_true")
    parser.add_argument("--end-time", type=_parse_iso_utc, default=None)
    parser.add_argument(
        "--baseline-path",
        default="docs/validation/ingestion_canary_baseline.json",
        help="Persisted vendor baseline path",
    )
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_canary_report.json",
        help="JSON report output path",
    )
    args = parser.parse_args()

    evidence = run_canary_validation(
        Path(args.output),
        baseline_version=args.baseline_version,
        candidate_version=args.candidate_version,
        bars=args.bars,
        rest_base=args.rest_base,
        symbol=args.symbol,
        interval=args.interval,
        baseline_path=Path(args.baseline_path),
        refresh_baseline=args.refresh_baseline,
        end_time=args.end_time,
    )
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
