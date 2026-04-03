from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.dataset_promotion import (
    build_dataset_promotion_report,
    register_approved_dataset,
    write_dataset_promotion_report,
)


def _default_report_path(*, target: str, symbol: str, stream_type: str) -> Path:
    symbol_slug = str(symbol).lower()
    return Path("docs/validation") / f"ingestion_dataset_promotion_{target}_{symbol_slug}_{stream_type}.json"


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
    parser.add_argument("--registry-path", default="docs/validation/approved_ingestion_datasets.json")
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
    output_path = Path(args.output) if args.output else _default_report_path(
        target=args.target,
        symbol=args.symbol,
        stream_type=args.stream_type,
    )
    write_dataset_promotion_report(output_path, report)
    if report.pass_ok:
        register_approved_dataset(
            Path(args.registry_path),
            report=report,
            promotion_report_path=output_path,
        )
    print(f"dataset promotion: {'PASS' if report.pass_ok else 'FAIL'}")
    print(f"- target: {report.target}")
    print(f"- requested_contract_mode: {report.requested_contract_mode}")
    print(f"- required_contract_mode: {report.required_contract_mode}")
    print(f"- contract_pass: {report.contract.pass_ok}")
    print(f"- parity_pass: {report.parity.pass_ok}")
    if report.reasons:
        for reason in report.reasons:
            print(f"- reason: {reason}")
    if report.pass_ok:
        print(f"- report_path: {output_path}")
        print(f"- registry_path: {args.registry_path}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
