from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.dataset_promotion import build_dataset_promotion_report, write_dataset_promotion_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a normalized ingestion dataset for backtesting or paper use")
    parser.add_argument("--target", choices=["backtesting", "paper"], default="backtesting")
    parser.add_argument("--normalized-path", required=True)
    parser.add_argument("--raw-base-dir", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--stream-type", choices=["trade", "kline"], required=True)
    parser.add_argument("--contract-mode", choices=["strict", "compat"], default="strict")
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_dataset_promotion_report(
        target=args.target,
        normalized_path=Path(args.normalized_path),
        raw_base_dir=Path(args.raw_base_dir),
        env=args.env,
        symbol=args.symbol,
        stream_type=args.stream_type,
        contract_mode=args.contract_mode,
    )
    if args.output:
        write_dataset_promotion_report(Path(args.output), report)
    print(f"dataset promotion: {'PASS' if report.pass_ok else 'FAIL'}")
    print(f"- target: {report.target}")
    print(f"- contract_pass: {report.contract.pass_ok}")
    print(f"- parity_pass: {report.parity.pass_ok}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
