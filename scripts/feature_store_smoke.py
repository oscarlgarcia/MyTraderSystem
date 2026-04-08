from __future__ import annotations

import argparse
from pathlib import Path

import httpx

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.config import load_config


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke-check the feature store HTTP deployment.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    return parser


def _resolve_base_url(args: argparse.Namespace) -> str:
    if args.base_url:
        return str(args.base_url)
    if args.env is None:
        raise SystemExit("--base-url is required when --env is not provided")
    cfg = load_config(args.env)
    if not cfg.feature_online_store_url:
        raise SystemExit(f"config.{cfg.env}.yaml does not define feature_online_store_url")
    return cfg.feature_online_store_url


def main() -> int:
    args = _parser().parse_args()
    base_url = _resolve_base_url(args).rstrip("/")
    response = httpx.get(f"{base_url}/healthz", timeout=float(args.timeout_seconds))
    response.raise_for_status()
    payload = response.json()
    if payload.get("ok") is not True:
        raise SystemExit("feature store healthz did not return ok=true")
    print(f"feature_store_smoke ok base_url={base_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
