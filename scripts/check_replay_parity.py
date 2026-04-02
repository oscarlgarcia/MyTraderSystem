from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.replay_parity import build_replay_parity_report, write_replay_parity_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate raw -> replay -> normalized parity")
    parser.add_argument("--raw-base-dir", required=True, help="Base raw landing dir")
    parser.add_argument("--normalized-path", required=True, help="Normalized partition path")
    parser.add_argument("--env", required=True, help="Environment name")
    parser.add_argument("--symbol", required=True, help="Instrument symbol")
    parser.add_argument("--stream-type", choices=["trade", "kline"], required=True, help="Stream type")
    parser.add_argument("--output", default=None, help="Optional JSON report output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_replay_parity_report(
        raw_base_dir=Path(args.raw_base_dir),
        normalized_path=Path(args.normalized_path),
        env=args.env,
        symbol=args.symbol,
        stream_type=args.stream_type,
    )
    if args.output:
        write_replay_parity_report(Path(args.output), report)
    print(f"replay parity: {'PASS' if report.pass_ok else 'FAIL'}")
    print(f"- raw_rows: {report.raw_rows}")
    print(f"- replay_rows: {report.replay_rows}")
    print(f"- normalized_rows: {report.normalized_rows}")
    print(f"- replay_identity_count: {report.replay_identity_count}")
    print(f"- normalized_identity_count: {report.normalized_identity_count}")
    print(f"- manifest_ok: {report.manifest_ok}")
    if report.manifest_missing_files:
        print(f"- manifest_missing_files: {list(report.manifest_missing_files)}")
    if report.manifest_mismatches:
        print(f"- manifest_mismatches: {list(report.manifest_mismatches)}")
    print(f"- order_match: {report.order_match}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
