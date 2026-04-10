from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class SubscriptionConfig:
    env: str
    venue: str
    symbols: tuple[str, ...]
    stream_types: tuple[str, ...]
    updated_at: str
    updated_by: str


def subscriptions_config_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "control-plane" / "subscriptions.json"


def read_subscription_config(
    base_dir: Path,
    env: str,
    *,
    default_symbols: tuple[str, ...] | list[str],
    default_stream_types: tuple[str, ...] | list[str],
    venue: str = "BINANCE",
) -> SubscriptionConfig:
    path = subscriptions_config_path(base_dir, env)
    if not path.exists():
        return SubscriptionConfig(
            env=env,
            venue=venue,
            symbols=tuple(str(item).upper() for item in default_symbols),
            stream_types=tuple(str(item).lower() for item in default_stream_types),
            updated_at=_utc_now(),
            updated_by="defaults",
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    return SubscriptionConfig(
        env=str(payload["env"]),
        venue=str(payload.get("venue", venue)).upper(),
        symbols=tuple(str(item).upper() for item in payload.get("symbols", ())),
        stream_types=tuple(str(item).lower() for item in payload.get("stream_types", ())),
        updated_at=str(payload.get("updated_at") or _utc_now()),
        updated_by=str(payload.get("updated_by") or "unknown"),
    )


def write_subscription_config(path: Path, config: SubscriptionConfig) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(config), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def update_subscription_config(
    *,
    base_dir: Path,
    env: str,
    symbols: tuple[str, ...] | list[str],
    stream_types: tuple[str, ...] | list[str],
    updated_by: str,
    venue: str = "BINANCE",
) -> SubscriptionConfig:
    config = SubscriptionConfig(
        env=env,
        venue=venue,
        symbols=tuple(sorted({str(item).upper() for item in symbols})),
        stream_types=tuple(sorted({str(item).lower() for item in stream_types})),
        updated_at=_utc_now(),
        updated_by=updated_by,
    )
    write_subscription_config(subscriptions_config_path(base_dir, env), config)
    return config
