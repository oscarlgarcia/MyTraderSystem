from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.normalized_contract import validate_normalized_contract, write_normalized_contract_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate normalized dataset contract")
    parser.add_argument("--path", required=True, help="Path to normalized partition or parquet file")
    parser.add_argument("--output", default=None, help="Optional JSON report output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate_normalized_contract(Path(args.path))
    if args.output:
        write_normalized_contract_report(Path(args.output), report)
    print(f"normalized contract: {'PASS' if report.pass_ok else 'FAIL'}")
    print(f"- path: {report.path}")
    print(f"- feed_type: {report.feed_type}")
    print(f"- row_count: {report.row_count}")
    if report.missing_columns:
        print(f"- missing_columns: {list(report.missing_columns)}")
    if report.missing_metadata_keys:
        print(f"- missing_metadata_keys: {list(report.missing_metadata_keys)}")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
