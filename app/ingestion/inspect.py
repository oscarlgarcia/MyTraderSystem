"""
Utilidad de inspección para leer eventos almacenados en Parquet.

Uso:
    python -m app.ingestion.inspect --env dev --limit 10
    python -m app.ingestion.inspect --env dev --symbol BTCUSDT --date 2024-01-01
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Optional

import pyarrow.dataset as ds

from app.config import load_config


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect stored MarketEvents")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--symbol", help="Filtra por símbolo (p.ej. BTCUSDT)")
    parser.add_argument("--date", help="Filtra por fecha YYYY-MM-DD de la partición")
    parser.add_argument("--limit", type=int, default=20, help="Número máximo de registros a mostrar")
    parser.add_argument("--json", action="store_true", help="Salida en JSON lines (por defecto).")
    return parser.parse_args(argv)


def _list_parquet_files(base_dir: Path, env: str) -> List[Path]:
    return sorted(base_dir.glob(f"{env}/symbol=*/date=*/data.parquet"))


def collect_events(base_dir: Path, env: str, symbol: Optional[str] = None, date: Optional[str] = None, limit: int = 20) -> List[dict]:
    files = _list_parquet_files(base_dir, env)
    if not files:
        return []
    dataset = ds.dataset(files, format="parquet")
    filters = []
    if symbol:
        filters.append(ds.field("symbol") == symbol.upper())
    if date:
        # date is partition, not column; filter via partition expression path contains
        # dataset partition discovery already maps it into dataset columns if hive; here we filter using selector
        pass  # handled by path selection below
    if filters:
        dataset = dataset.to_table(filter=filters[0] if len(filters) == 1 else filters[0] & filters[1])
    else:
        dataset = dataset.to_table()
    rows = dataset.slice(0, limit).to_pylist()
    return rows


def run(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.env)
    base_dir = Path(cfg.data_dir)

    # Narrow file list by date partition if provided
    files = _list_parquet_files(base_dir, cfg.env)
    if args.date:
        files = [p for p in files if f"date={args.date}" in str(p)]

    if not files:
        print("No se encontraron archivos Parquet para los filtros indicados.")
        return 1

    dataset = ds.dataset(files, format="parquet")
    filt = None
    if args.symbol:
        filt = ds.field("symbol") == args.symbol.upper()
    table = dataset.to_table(filter=filt) if filt is not None else dataset.to_table()
    if table.num_rows == 0:
        print("Sin filas para los filtros indicados.")
        return 0
    to_show = table.slice(0, args.limit).to_pylist()
    for row in to_show:
        print(json.dumps(row, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
