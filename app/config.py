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

from app.marketdata.instruments import ensure_default_instruments
from app.marketdata.support_matrix import normalize_feed_types

DEFAULT_ENV = "dev"
DEFAULT_INGEST_STREAM_TYPES = ("kline",)
REQUIRED_KEYS = {"env", "data_dir", "log_level", "ws_base", "rest_base", "symbols"}
ALLOWED_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
SECRET_ENV_PREFIX = "APP_SECRET_"
FORBIDDEN_CONFIG_KEY_MARKERS = (
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "authorization",
)


@dataclass(frozen=True, slots=True)
class AppConfig:
    env: str
    data_dir: Path
    log_level: str
    ws_base: str
    rest_base: str
    symbols: list[str]
    control_plane_backend: str = "sqlite"
    control_plane_db_path: Path = Path("control_plane.sqlite")
    control_plane_db_url: str | None = None
    control_plane_telemetry_dir: Path = Path("ui-telemetry")
    control_plane_poll_interval_seconds: float = 5.0
    control_plane_command_poll_interval_seconds: float = 2.0
    feature_online_backend: str = "local_sqlite"
    feature_online_store_path: Path = Path("feature-store/online.sqlite")
    feature_online_store_url: str | None = None
    feature_offline_store_path: Path = Path("feature-store/offline.sqlite")
    feature_release_registry_path: Path = Path("docs/validation/feature_releases.json")
    feature_training_bundle_registry_dir: Path = Path("feature-store/training-bundles")


def get_secret_env(name: str, *, required: bool = False) -> str | None:
    env_name = f"{SECRET_ENV_PREFIX}{name.upper()}"
    value = os.getenv(env_name)
    if required and not value:
        raise ValueError(f"Missing required secret env var: {env_name}")
    return value


def _load_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data


def _contains_forbidden_secret_keys(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(marker in lowered for marker in FORBIDDEN_CONFIG_KEY_MARKERS):
                return True
            if _contains_forbidden_secret_keys(item):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_secret_keys(item) for item in value)
    return False


def _resolve_data_dir(value: str | os.PathLike[str]) -> Path:
    raw_path = Path(value)
    if raw_path.is_absolute():
        return raw_path
    raw_text = str(value)
    if os.name == "nt" and raw_text.startswith(("/", "\\")):
        drive_root = Path.cwd().anchor or "C:\\"
        return Path(drive_root) / raw_text.lstrip("/\\")
    return raw_path


def load_config(env: str | None = None) -> AppConfig:
    env_name = env or os.getenv("APP_ENV", DEFAULT_ENV)
    path = Path(f"config.{env_name}.yaml")
    raw = _load_file(path)
    if _contains_forbidden_secret_keys(raw):
        raise ValueError(
            f"Config file {path} contains secret-like keys; move secrets to {SECRET_ENV_PREFIX}* environment variables"
        )

    missing = REQUIRED_KEYS - set(raw.keys())
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(sorted(missing))}")
    log_level = str(raw["log_level"]).upper()
    if log_level not in ALLOWED_LOG_LEVELS:
        raise ValueError(f"log_level must be one of {sorted(ALLOWED_LOG_LEVELS)}")

    data_dir_override = os.getenv("APP_DATA_DIR")
    data_dir = Path(data_dir_override) if data_dir_override else _resolve_data_dir(raw["data_dir"])
    control_plane_backend = str(raw.get("control_plane_backend", "sqlite")).strip().lower() or "sqlite"
    control_plane_db_root = data_dir / raw["env"] / "control-plane"
    control_plane_db_path = Path(raw.get("control_plane_db_path", control_plane_db_root / "control_plane.sqlite"))
    control_plane_db_url = raw.get("control_plane_db_url")
    control_plane_telemetry_dir = Path(raw.get("control_plane_telemetry_dir", data_dir / raw["env"] / "ui-telemetry"))
    control_plane_poll_interval_seconds = float(raw.get("control_plane_poll_interval_seconds", 5.0))
    control_plane_command_poll_interval_seconds = float(raw.get("control_plane_command_poll_interval_seconds", 2.0))
    feature_root = data_dir / raw["env"] / "feature-store"
    feature_online_backend = str(raw.get("feature_online_backend", "local_sqlite")).strip().lower() or "local_sqlite"
    feature_online_store_path = Path(raw.get("feature_online_store_path", feature_root / "online.sqlite"))
    feature_online_store_url = raw.get("feature_online_store_url")
    feature_offline_store_path = Path(raw.get("feature_offline_store_path", feature_root / "offline.sqlite"))
    feature_release_registry_path = Path(raw.get("feature_release_registry_path", Path("docs") / "validation" / "feature_releases.json"))
    feature_training_bundle_registry_dir = Path(
        raw.get("feature_training_bundle_registry_dir", feature_root / "training-bundles")
    )

    symbols = [str(symbol).upper() for symbol in raw.get("symbols", [])]
    if not symbols:
        raise ValueError("symbols list cannot be empty")
    ensure_default_instruments(symbols, venue="BINANCE")

    for endpoint_key in ("ws_base", "rest_base"):
        if not str(raw.get(endpoint_key, "")).startswith(("ws://", "wss://", "http://", "https://")):
            raise ValueError(f"{endpoint_key} must be a valid http(s)/ws(s) URL")

    return AppConfig(
        env=raw["env"],
        data_dir=data_dir,
        log_level=log_level,
        ws_base=str(raw["ws_base"]),
        rest_base=str(raw["rest_base"]),
        symbols=symbols,
        control_plane_backend=control_plane_backend,
        control_plane_db_path=control_plane_db_path,
        control_plane_db_url=str(control_plane_db_url) if control_plane_db_url not in (None, "") else None,
        control_plane_telemetry_dir=control_plane_telemetry_dir,
        control_plane_poll_interval_seconds=control_plane_poll_interval_seconds,
        control_plane_command_poll_interval_seconds=control_plane_command_poll_interval_seconds,
        feature_online_backend=feature_online_backend,
        feature_online_store_path=feature_online_store_path,
        feature_online_store_url=str(feature_online_store_url) if feature_online_store_url not in (None, "") else None,
        feature_offline_store_path=feature_offline_store_path,
        feature_release_registry_path=feature_release_registry_path,
        feature_training_bundle_registry_dir=feature_training_bundle_registry_dir,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    def _parse_stream_types(value: str) -> tuple[str, ...]:
        return normalize_feed_types([part.strip() for part in str(value).split(",")])

    parser = argparse.ArgumentParser(description="MyTraderSystem")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None, help="Config environment")
    parser.add_argument(
        "--production-mode",
        action="store_true",
        help="Activa validaciones operativas estrictas: sin fallback, sin fast-path y con data_dir seguro.",
    )
    parser.add_argument(
        "--release-gates",
        action="store_true",
        help="Ejecuta el comando operativo de release gating de ingestion y termina sin correr trading.",
    )
    parser.add_argument(
        "--release-gates-target",
        choices=["paper", "live"],
        default="paper",
        help="Objetivo del gating operativo: paper o live.",
    )
    parser.add_argument(
        "--release-gates-output",
        default="docs/validation/ingestion_release_gates.json",
        help="Ruta del reporte JSON de release gates.",
    )
    parser.add_argument(
        "--release-gates-rest-canary-path",
        default="docs/validation/ingestion_canary_report.json",
        help="Ruta al artifact del canary REST baseline.",
    )
    parser.add_argument(
        "--release-gates-ws-canary-path",
        default="docs/validation/ingestion_ws_canary_report.json",
        help="Ruta al artifact del canary WS live.",
    )
    parser.add_argument(
        "--release-gates-replay-parity-path",
        default="docs/validation/ingestion_replay_parity.json",
        help="Ruta al artifact de replay parity raw -> replay -> normalized.",
    )
    parser.add_argument(
        "--release-gates-benchmark-path",
        default="docs/validation/ingestion_storage_benchmark.json",
        help="Ruta al artifact del benchmark de storage segmentado.",
    )
    parser.add_argument(
        "--release-gates-soak-path",
        default="docs/validation/ingestion_soak_evidence.json",
        help="Ruta al artifact del soak test operativo.",
    )
    parser.add_argument(
        "--release-gates-vendor-contracts-path",
        default="docs/validation/ingestion_vendor_contracts.json",
        help="Ruta al artifact de contract tests contra el vendor.",
    )
    parser.add_argument(
        "--release-gates-live-drill-path",
        default="docs/validation/ingestion_live_drill_report.json",
        help="Ruta al artifact del drill operativo de live cutover.",
    )
    parser.add_argument(
        "--mode",
        choices=["dry", "paper", "live"],
        default="dry",
        help="Pipeline mode: dry (deterministic), paper (live market data + paper execution) o live (strict runtime path).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=50,
        help="Limite de eventos a procesar en run_cycle (aplica a dry y live)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Duracion maxima en segundos para live; ignora en dry si no se indica",
    )
    parser.add_argument(
        "--feature-audit-path",
        default=None,
        help="Ruta del audit trail de features requerido fuera de dry mode.",
    )
    parser.add_argument(
        "--feature-release-action",
        choices=["publish", "rollback"],
        default=None,
        help="Ejecuta la operacion oficial de governance de feature versions y termina.",
    )
    parser.add_argument(
        "--feature-release-registry",
        default="docs/validation/feature_releases.json",
        help="Registry persistido de versiones activas de features.",
    )
    parser.add_argument(
        "--feature-release-name",
        default=None,
        help="Nombre del feature set a publicar o revertir.",
    )
    parser.add_argument(
        "--feature-release-version",
        default=None,
        help="Version a activar cuando feature-release-action=publish.",
    )
    parser.add_argument(
        "--feature-release-target",
        choices=["paper", "live"],
        default="paper",
        help="Objetivo operativo del release de features.",
    )
    parser.add_argument(
        "--feature-release-gate-input",
        default=None,
        help="JSON con parity/latency/staleness para evaluar el gate antes de publish.",
    )
    parser.add_argument(
        "--feature-release-gates",
        action="store_true",
        help="Ejecuta los release gates operativos del modulo de features y termina.",
    )
    parser.add_argument(
        "--feature-release-gates-target",
        choices=["paper", "live"],
        default="paper",
        help="Objetivo de los release gates de features.",
    )
    parser.add_argument(
        "--feature-release-gates-output",
        default="docs/validation/feature_release_gates.json",
        help="Ruta del reporte JSON de release gates de features.",
    )
    parser.add_argument(
        "--feature-release-gates-parity-path",
        default="docs/validation/feature_parity_report.json",
        help="Ruta al artifact de parity online/offline de features.",
    )
    parser.add_argument(
        "--feature-release-gates-benchmark-path",
        default="docs/validation/feature_benchmark_report.json",
        help="Ruta al artifact de benchmark de serving/features.",
    )
    parser.add_argument(
        "--feature-release-gates-observability-path",
        default="docs/validation/feature_observability.json",
        help="Ruta al artifact de observabilidad de features.",
    )
    parser.add_argument(
        "--feature-release-gates-contract-path",
        default="docs/validation/feature_contract_validation.json",
        help="Ruta al artifact de validacion contractual training-serving.",
    )
    parser.add_argument(
        "--feature-release-gates-shadow-path",
        default=None,
        help="Ruta al artifact JSONL de shadow validation de features.",
    )
    parser.add_argument(
        "--feature-release-gates-soak-path",
        default=None,
        help="Ruta al artifact JSON del soak operativo de features.",
    )
    parser.add_argument(
        "--feature-release-gates-concurrency-path",
        default=None,
        help="Ruta al artifact JSON del test de concurrencia de features.",
    )
    parser.add_argument(
        "--feature-release-gates-rollout-audit-path",
        default=None,
        help="Ruta al artifact JSON del rollout audit de features.",
    )
    parser.add_argument(
        "--feature-release-gates-online-backend",
        default="http",
        help="Backend online declarado para el release gate de features.",
    )
    parser.add_argument(
        "--feature-release-gates-observability-sink",
        default="http",
        help="Sink de observabilidad declarado para el release gate de features.",
    )
    parser.add_argument(
        "--feature-consumer-name",
        default="",
        help="Nombre del consumidor de features para auditoria/contract validation.",
    )
    parser.add_argument(
        "--feature-consumer-kind",
        default="",
        help="Tipo del consumidor de features: strategy, model, risk, execution, etc.",
    )
    parser.add_argument(
        "--feature-dataset-id",
        default="",
        help="Dataset id asociado al consumidor/modelo para auditoria contractual.",
    )
    parser.add_argument(
        "--feature-schema-hash",
        default="",
        help="Schema hash esperado del vector de features para auditoria contractual.",
    )
    parser.add_argument(
        "--feature-training-bundle-id",
        default="",
        help="Training bundle id requerido para paper/live.",
    )
    parser.add_argument(
        "--feature-training-bundle-registry",
        default=None,
        help="Ruta del registry persistido de training bundles.",
    )
    parser.add_argument(
        "--feature-observability-output",
        default=None,
        help="Ruta opcional para exportar observabilidad agregada del pipeline de features.",
    )
    parser.add_argument(
        "--feature-online-backend",
        choices=["memory", "local_sqlite", "json_file", "http"],
        default=None,
        help="Backend canonico del online feature store usado por el runtime operativo.",
    )
    parser.add_argument(
        "--feature-online-store-path",
        default=None,
        help="Ruta del backend persistido del online feature store.",
    )
    parser.add_argument(
        "--feature-online-store-url",
        default=None,
        help="URL base del feature store HTTP cuando feature-online-backend=http.",
    )
    parser.add_argument(
        "--feature-offline-store-path",
        default=None,
        help="Ruta del offline feature store usado para PIT y serving historico.",
    )
    parser.add_argument(
        "--trace-steps",
        action="store_true",
        help="Emite trazas start/done por fase del pipeline para debugging visual",
    )
    parser.add_argument(
        "--ingest-max-buffer",
        type=int,
        default=10_000,
        help="Tamano maximo del buffer en ResilientRunner (control de memoria/throughput).",
    )
    parser.add_argument(
        "--ingest-batch-size",
        type=int,
        default=1,
        help="Tamano del lote local antes de escribir en live; reduce llamadas a writer.add/flush.",
    )
    parser.add_argument(
        "--ingest-lag-warn",
        type=float,
        default=None,
        help="Umbral experimental de WARNING para latencia de ingestion en segundos.",
    )
    parser.add_argument(
        "--ingest-buffer-warn",
        type=int,
        default=None,
        help="Umbral experimental de WARNING para eventos descartados por buffer.",
    )
    parser.add_argument(
        "--ingest-backpressure-policy",
        choices=["pause", "drop_oldest", "drop_newest", "fail"],
        default="pause",
        help="Politica de saturacion del buffer de ingestion: pause, drop_oldest, drop_newest o fail.",
    )
    parser.add_argument(
        "--ingest-temporal-policy",
        choices=["accept", "drop", "fail"],
        default="accept",
        help="Politica para eventos tardios o fuera de orden: accept, drop o fail.",
    )
    parser.add_argument(
        "--ingest-pipeline-version",
        choices=["v1", "v2"],
        default="v2",
        help="Version principal del pipeline/sink normalized: v1 legacy o v2 actual.",
    )
    parser.add_argument(
        "--ingest-shadow-mode",
        action="store_true",
        help="Activa doble escritura y comparacion old-vs-new para migracion controlada.",
    )
    parser.add_argument(
        "--ingest-shadow-block-on-diff",
        action="store_true",
        help="Bloquea la ejecucion si el comparador shadow detecta diferencias relevantes.",
    )
    parser.add_argument(
        "--ingest-stream-types",
        type=_parse_stream_types,
        default=DEFAULT_INGEST_STREAM_TYPES,
        help="Streams live a ingerir, separados por coma. Ejemplo: kline. El scope live soportado hoy es solo `kline`; `trade` y `book` quedan bloqueados.",
    )
    parser.add_argument(
        "--fast-path",
        action="store_true",
        help="Modo experimental de alto throughput: menos garantias, menos logs, mas batching.",
    )
    parser.add_argument(
        "--no-ingest-dedup",
        dest="ingest_dedup",
        action="store_false",
        help="Desactiva la deduplicacion en vivo para maximizar throughput (riesgo de duplicados).",
    )
    parser.add_argument(
        "--allow-live-fallback",
        action="store_true",
        help="Permite fallback explicito de live a dry si la ingesta real falla (solo para debugging).",
    )
    parser.add_argument(
        "--error-policy",
        choices=["fail_fast", "allow_fallback", "degraded"],
        default=None,
        help="Politica explicita de error para ingestion live: fail_fast, allow_fallback o degraded.",
    )
    parser.set_defaults(ingest_dedup=True)
    args, _unknown = parser.parse_known_args(argv)
    return args
