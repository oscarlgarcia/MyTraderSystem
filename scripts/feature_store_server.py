from __future__ import annotations

import argparse

import uvicorn

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.config import load_config
from app.features.api import create_feature_store_api
from app.features.online_store_factory import OnlineStoreConfig, create_online_store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the feature online store over HTTP")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None)
    parser.add_argument("--backend", choices=["local_sqlite", "json_file"], default=None)
    parser.add_argument("--path", default=None, help="Ruta del store persistido usado por el servicio.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--history-max-rows-per-scope", type=int, default=None)
    parser.add_argument("--history-retention-seconds", type=float, default=None)
    return parser


def _resolve_server_args(args: argparse.Namespace) -> tuple[str, str, str, int]:
    if args.env is None:
        if args.backend is None or args.path is None:
            raise SystemExit("--backend and --path are required when --env is not provided")
        return args.backend, args.path, args.host or "127.0.0.1", int(args.port or 8011)
    cfg = load_config(args.env)
    backend = args.backend or cfg.feature_store_server_backend
    path = args.path or str(cfg.feature_store_server_path)
    host = args.host or cfg.feature_store_server_host
    port = int(args.port or cfg.feature_store_server_port)
    return backend, path, host, port


def main() -> int:
    args = build_parser().parse_args()
    backend, path, host, port = _resolve_server_args(args)
    store = create_online_store(
        OnlineStoreConfig(
            backend=backend,
            path=path,
            history_max_rows_per_scope=args.history_max_rows_per_scope,
            history_retention_seconds=args.history_retention_seconds,
        )
    )
    app = create_feature_store_api(online_store=store)
    uvicorn.run(app, host=host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
