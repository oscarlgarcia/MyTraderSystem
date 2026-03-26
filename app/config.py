"""
Simple config loader with environment selection and minimal validation.

The config files are JSON-compatible YAML to avoid extra dependencies.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

DEFAULT_ENV = "dev"
REQUIRED_KEYS = {"env", "data_dir", "log_level"}


@dataclass(frozen=True, slots=True)
class AppConfig:
    env: str
    data_dir: Path
    log_level: str


def _load_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def load_config(env: str | None = None) -> AppConfig:
    env_name = env or os.getenv("APP_ENV", DEFAULT_ENV)
    path = Path(f"config.{env_name}.yaml")
    raw = _load_file(path)

    missing = REQUIRED_KEYS - set(raw.keys())
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(sorted(missing))}")

    # Env var override example
    data_dir_override = os.getenv("APP_DATA_DIR")
    data_dir = Path(data_dir_override) if data_dir_override else Path(raw["data_dir"])

    return AppConfig(
        env=raw["env"],
        data_dir=data_dir,
        log_level=raw["log_level"],
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MyTraderSystem")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    return parser.parse_args(argv)
