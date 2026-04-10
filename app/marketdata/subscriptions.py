from __future__ import annotations

import json
import hashlib
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
    revision: str
    updated_at: str
    updated_by: str


def subscriptions_config_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "control-plane" / "subscriptions.json"


def _subscription_revision(*, env: str, venue: str, symbols: tuple[str, ...], stream_types: tuple[str, ...], updated_at: str) -> str:
    payload = json.dumps(
        {
            "env": env,
            "venue": venue,
            "symbols": list(symbols),
            "stream_types": list(stream_types),
            "updated_at": updated_at,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


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
        symbols = tuple(str(item).upper() for item in default_symbols)
        stream_types = tuple(str(item).lower() for item in default_stream_types)
        updated_at = "defaults"
        return SubscriptionConfig(
            env=env,
            venue=venue,
            symbols=symbols,
            stream_types=stream_types,
            revision=_subscription_revision(
                env=env,
                venue=venue,
                symbols=symbols,
                stream_types=stream_types,
                updated_at=updated_at,
            ),
            updated_at=updated_at,
            updated_by="defaults",
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbols = tuple(str(item).upper() for item in payload.get("symbols", ()))
    stream_types = tuple(str(item).lower() for item in payload.get("stream_types", ()))
    updated_at = str(payload.get("updated_at") or _utc_now())
    normalized_env = str(payload["env"])
    normalized_venue = str(payload.get("venue", venue)).upper()
    return SubscriptionConfig(
        env=normalized_env,
        venue=normalized_venue,
        symbols=symbols,
        stream_types=stream_types,
        revision=str(
            payload.get("revision")
            or _subscription_revision(
                env=normalized_env,
                venue=normalized_venue,
                symbols=symbols,
                stream_types=stream_types,
                updated_at=updated_at,
            )
        ),
        updated_at=updated_at,
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
    normalized_symbols = tuple(sorted({str(item).upper() for item in symbols}))
    normalized_stream_types = tuple(sorted({str(item).lower() for item in stream_types}))
    updated_at = _utc_now()
    config = SubscriptionConfig(
        env=env,
        venue=venue,
        symbols=normalized_symbols,
        stream_types=normalized_stream_types,
        revision=_subscription_revision(
            env=env,
            venue=venue,
            symbols=normalized_symbols,
            stream_types=normalized_stream_types,
            updated_at=updated_at,
        ),
        updated_at=updated_at,
        updated_by=updated_by,
    )
    write_subscription_config(subscriptions_config_path(base_dir, env), config)
    return config
