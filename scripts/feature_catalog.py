from __future__ import annotations

import argparse
import json

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.features.catalog import get_default_feature_catalog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect the feature catalog")
    parser.add_argument("--family", default=None)
    parser.add_argument("--strategy-family", default=None)
    parser.add_argument("--phase", default=None)
    parser.add_argument("--source-scope", default=None)
    parser.add_argument("--status", default=None)
    parser.add_argument("--bundle-name", default=None)
    parser.add_argument("--format", choices=("json", "table", "names"), default="table")
    return parser


def _render_table(payload: list[dict[str, object]]) -> str:
    lines = ["feature_name | family | phase | status | bundle_name", "--- | --- | --- | --- | ---"]
    for item in payload:
        lines.append(
            f"{item['feature_name']} | {item['feature_family']} | {item['phase']} | {item['status']} | {item['bundle_name']}"
        )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    catalog = get_default_feature_catalog()
    entries = catalog.list_features(
        family=args.family,
        strategy_family=args.strategy_family,
        phase=args.phase,
        source_scope=args.source_scope,
        status=args.status,
        bundle_name=args.bundle_name,
    )
    payload = [entry.to_dict() for entry in entries]
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.format == "names":
        for item in payload:
            print(item["feature_name"])
        return 0
    print(_render_table(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
