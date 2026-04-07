from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.marketdata.raw_sink import write_raw_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate manifest files for raw JSONL ingestion files")
    parser.add_argument("--raw-base-dir", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_base_dir = Path(args.raw_base_dir)
    count = 0
    for path in raw_base_dir.glob("env=*/venue=*/stream_type=*/symbol=*/date=*/events.jsonl"):
        write_raw_manifest(path)
        count += 1
    print(f"raw manifests generated: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
