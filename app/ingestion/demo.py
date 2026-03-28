"""
Demo de ingesta real: conecta al stream (ej. Binance testnet) durante unos segundos,
escribe en Parquet y ejecuta el pipeline de features, mostrando métricas básicas.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import List

from app.config import load_config
from app.ingestion.pipeline import collect_events
from app.features.pipeline import run_feature_pipeline
from app.observability.logger import get_logger
import logging


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo de ingesta y features (real)")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--duration", type=float, default=30.0, help="Duración en segundos del stream")
    parser.add_argument("--max-events", type=int, default=200, help="Límite de eventos a procesar")
    parser.add_argument("--symbol", type=str, default=None, help="Símbolo único (opcional, usa config si no)")
    parser.add_argument("--trace-steps", action="store_true", help="Muestra trazas por fase")
    return parser.parse_args()


def main() -> int:
    args = _build_args()
    cfg = load_config(args.env)
    logger = get_logger(name="ingest.demo", level="INFO")
    # Alinear logging de feature pipeline con el logger estructurado
    logging.getLogger("features.pipeline").handlers = logger.handlers
    logging.getLogger("features.pipeline").setLevel(logging.INFO)
    logging.getLogger("features.pipeline").propagate = False

    # Ajustar símbolos si se solicita uno específico
    if args.symbol:
        cfg = cfg.__class__(
            env=cfg.env,
            data_dir=cfg.data_dir,
            log_level=cfg.log_level,
            ws_base=cfg.ws_base,
            rest_base=cfg.rest_base,
            symbols=[args.symbol.upper()],
        )

    start = datetime.utcnow()
    events: List = collect_events(
        mode="live",
        cfg=cfg,
        max_events=args.max_events,
        duration_s=args.duration,
        logger=logger,
        compute_features_after=False,  # los calculamos abajo para mostrar resumen
    )
    features = run_feature_pipeline(events, window=5)

    elapsed = (datetime.utcnow() - start).total_seconds()
    throughput = round(len(events) / elapsed, 2) if elapsed else 0.0
    logger.info(
        "demo summary",
        extra={
            "events": len(events),
            "features": len(features),
            "duration_secs": round(elapsed, 2),
            "throughput_eps": throughput,
            "symbols": cfg.symbols,
        },
    )
    if events:
        logger.info("sample event", extra={"event": events[0].__dict__})
    if features:
        logger.info("sample feature", extra={"feature": features[-1].__dict__})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
