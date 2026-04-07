from __future__ import annotations

import argparse

import uvicorn

from app.features.api import create_feature_store_api
from app.features.online_store_factory import OnlineStoreConfig, create_online_store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the feature online store over HTTP")
    parser.add_argument("--backend", choices=["local_sqlite", "json_file"], default="local_sqlite")
    parser.add_argument("--path", required=True, help="Ruta del store persistido usado por el servicio.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8011)
    parser.add_argument("--history-max-rows-per-scope", type=int, default=None)
    parser.add_argument("--history-retention-seconds", type=float, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = create_online_store(
        OnlineStoreConfig(
            backend=args.backend,
            path=args.path,
            history_max_rows_per_scope=args.history_max_rows_per_scope,
            history_retention_seconds=args.history_retention_seconds,
        )
    )
    app = create_feature_store_api(online_store=store)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
