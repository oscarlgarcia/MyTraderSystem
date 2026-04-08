"""
Binance feed-specific normalizers and payload helpers.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Protocol

from app.common.dto import MarketEvent, normalize_symbol
from app.marketdata.errors import SchemaDriftError
from app.marketdata.instruments import instrument_metadata
from app.marketdata.models import BarEvent, IngestionEvent, TradeEvent, ensure_legacy_market_event
from app.marketdata.normalization import stamp_normalizer_version
from app.marketdata.validators import validate_ingestion_event, validate_kline_payload, validate_trade_payload


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _process_ts(process_ts: datetime | None = None) -> datetime:
    return process_ts or datetime.now(timezone.utc)


def _instrument_metadata(symbol: str, venue: str) -> dict[str, str]:
    return instrument_metadata(symbol, venue=venue)


def _quote_volume_from_snapshot_row(row: list[Any]) -> str:
    # Full Binance REST klines expose quote asset volume at row[7]. Older
    # fixtures in the test suite only carry seven columns, so keep row[5] as a
    # compatibility fallback for those synthetic rows.
    if len(row) > 7 and row[7] not in ("", None):
        return str(row[7])
    return str(row[5])


def _schema_node_kind(value: Any) -> str:
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    return type(value).__name__.lower()


def _flatten_payload_shape(payload: Any, *, prefix: str = "") -> dict[str, str]:
    shape: dict[str, str] = {}
    if isinstance(payload, dict):
        if prefix:
            shape[prefix] = "object"
        for key, value in sorted(payload.items()):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            shape.update(_flatten_payload_shape(value, prefix=child_prefix))
        return shape
    if isinstance(payload, list):
        shape[prefix] = "array"
        return shape
    shape[prefix] = _schema_node_kind(payload)
    return shape


def _shape_hash(shape: dict[str, str]) -> str:
    canonical = json.dumps(sorted(shape.items()), separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class _SchemaSpec:
    event_type: str
    required_paths: dict[str, str]
    optional_paths: dict[str, str]

    @property
    def allowed_paths(self) -> dict[str, str]:
        return {
            **self.required_paths,
            **self.optional_paths,
        }

    @property
    def expected_shape_hash(self) -> str:
        return _shape_hash(self.allowed_paths)


BINANCE_SCHEMA_SPECS: dict[str, _SchemaSpec] = {
    "trade": _SchemaSpec(
        event_type="trade",
        required_paths={
            "E": "int",
            "p": "str",
            "q": "str",
            "s": "str",
        },
        optional_paths={
            "a": "int",
            "b": "int",
            "e": "str",
            "m": "bool",
            "M": "bool",
            "t": "int",
            "T": "int",
        },
    ),
    "kline": _SchemaSpec(
        event_type="kline",
        required_paths={
            "E": "int",
            "k": "object",
            "k.T": "int",
            "k.c": "str",
            "k.h": "str",
            "k.l": "str",
            "k.o": "str",
            "k.q": "str",
            "k.t": "int",
            "s": "str",
        },
        optional_paths={
            "e": "str",
            "k.B": "str",
            "k.f": "int",
            "k.i": "str",
            "k.L": "int",
            "k.n": "int",
            "k.Q": "str",
            "k.s": "str",
            "k.V": "str",
            "k.v": "str",
            "k.x": "bool",
        },
    ),
}


def assert_binance_payload_schema(event_type: str, payload: dict[str, Any]) -> None:
    spec = BINANCE_SCHEMA_SPECS.get(event_type)
    if spec is None:
        return
    actual_shape = _flatten_payload_shape(payload)
    allowed_paths = spec.allowed_paths
    missing_required_paths = sorted(
        path for path in spec.required_paths if path not in actual_shape
    )
    observed_required_paths = [
        path for path in spec.required_paths if path in actual_shape
    ]
    unexpected_paths = sorted(
        path for path in actual_shape if path not in allowed_paths
    )
    kind_mismatches = sorted(
        path
        for path, actual_kind in actual_shape.items()
        if path in allowed_paths and allowed_paths[path] != actual_kind
    )
    if kind_mismatches or (unexpected_paths and observed_required_paths):
        raise SchemaDriftError(
            vendor="BINANCE",
            stream_type=event_type,
            shape_hash=_shape_hash(actual_shape),
            expected_shape_hash=spec.expected_shape_hash,
            unexpected_paths=unexpected_paths,
            missing_required_paths=missing_required_paths,
            kind_mismatches=kind_mismatches,
            drift_mode="blocking",
        )


class BinanceFeedNormalizer(Protocol):
    event_type: str
    stream_type: str
    supports_snapshot: bool

    @staticmethod
    def build_stream(symbol: str) -> str: ...

    @staticmethod
    def normalize_typed(
        payload: dict[str, Any],
        *,
        venue: str = "BINANCE",
        receive_ts: datetime | None = None,
        process_ts: datetime | None = None,
    ) -> IngestionEvent: ...


class BinanceTradeNormalizer:
    event_type = "trade"
    stream_type = "trade"
    supports_snapshot = True

    @staticmethod
    def build_stream(symbol: str) -> str:
        return f"{normalize_symbol(symbol).lower()}@trade"

    @staticmethod
    def normalize_typed(
        payload: dict[str, Any],
        *,
        venue: str = "BINANCE",
        receive_ts: datetime | None = None,
        process_ts: datetime | None = None,
    ) -> TradeEvent:
        validate_trade_payload(payload)
        metadata = _instrument_metadata(str(payload["s"]), venue)
        if payload.get("_backfill_endpoint") is not None:
            metadata["historical_trade_endpoint"] = str(payload["_backfill_endpoint"])
        if payload.get("_historical_trade_kind") is not None:
            metadata["historical_trade_kind"] = str(payload["_historical_trade_kind"])
        if payload.get("a") is not None:
            metadata["aggregate_trade_id"] = str(payload["a"])
        if payload.get("f") is not None:
            metadata["aggregate_trade_first_id"] = str(payload["f"])
        if payload.get("l") is not None:
            metadata["aggregate_trade_last_id"] = str(payload["l"])
        event = TradeEvent(
            symbol=normalize_symbol(str(payload["s"])),
            exchange_ts=_ts_from_ms(int(payload["E"])),
            receive_ts=receive_ts,
            process_ts=_process_ts(process_ts),
            venue=venue,
            source_id=str(payload.get("t")) if payload.get("t") is not None else None,
            metadata=stamp_normalizer_version(metadata),
            price=float(payload["p"]),
            size=float(payload["q"]),
            trade_id=str(payload.get("t")) if payload.get("t") is not None else None,
            side="sell" if payload.get("m") else "buy" if payload.get("m") is not None else None,
        )
        validate_ingestion_event(event)
        return event

    @classmethod
    def normalize_legacy(cls, payload: dict[str, Any], **kwargs: Any) -> MarketEvent:
        return ensure_legacy_market_event(cls.normalize_typed(payload, **kwargs))

    @staticmethod
    def snapshot_payload_from_row(symbol: str, row: dict[str, Any]) -> dict[str, Any]:
        aggregate_trade_id = int(row["a"])
        return {
            "e": "trade",
            "s": normalize_symbol(symbol),
            "E": int(row["T"]),
            "p": str(row["p"]),
            "q": str(row["q"]),
            "t": aggregate_trade_id,
            "m": bool(row.get("m")) if row.get("m") is not None else None,
            "M": bool(row.get("M")) if row.get("M") is not None else None,
            "a": aggregate_trade_id,
            "f": int(row["f"]) if row.get("f") is not None else None,
            "l": int(row["l"]) if row.get("l") is not None else None,
            "_backfill_endpoint": "aggTrades",
            "_historical_trade_kind": "aggregate_trade",
        }


class BinanceBarNormalizer:
    event_type = "kline"
    stream_type = "kline"
    supports_snapshot = True

    @staticmethod
    def build_stream(symbol: str) -> str:
        return f"{normalize_symbol(symbol).lower()}@kline_1m"

    @staticmethod
    def exchange_ts_from_payload(payload: dict[str, Any]) -> datetime:
        kline = payload["k"]
        if "T" in kline:
            return _ts_from_ms(int(kline["T"]))
        return _ts_from_ms(int(payload["E"]))

    @staticmethod
    def normalize_typed(
        payload: dict[str, Any],
        *,
        venue: str = "BINANCE",
        receive_ts: datetime | None = None,
        process_ts: datetime | None = None,
        interval: str | None = None,
    ) -> BarEvent:
        validate_kline_payload(payload)
        kline = payload["k"]
        provider_ts = None
        if payload.get("E") is not None and kline.get("T") is not None and int(payload["E"]) != int(kline["T"]):
            provider_ts = _ts_from_ms(int(payload["E"]))
        event = BarEvent(
            symbol=normalize_symbol(str(payload["s"])),
            exchange_ts=BinanceBarNormalizer.exchange_ts_from_payload(payload),
            provider_ts=provider_ts,
            receive_ts=receive_ts,
            process_ts=_process_ts(process_ts),
            venue=venue,
            source_id=str(kline.get("t")) if kline.get("t") is not None else None,
            metadata=stamp_normalizer_version(
                {
                    **_instrument_metadata(str(payload["s"]), venue),
                    "volume_kind": "quote",
                    "volume_semantics": "quote_asset_volume",
                }
            ),
            open=float(kline.get("o", kline["c"])),
            high=float(kline.get("h", kline["c"])),
            low=float(kline.get("l", kline["c"])),
            close=float(kline["c"]),
            volume=float(kline["q"]),
            volume_kind="quote",
            interval=interval or str(kline.get("i", "1m")),
            open_ts=_ts_from_ms(int(kline["t"])) if kline.get("t") is not None else None,
            close_ts=_ts_from_ms(int(kline["T"])) if kline.get("T") is not None else None,
        )
        validate_ingestion_event(event)
        return event

    @classmethod
    def normalize_legacy(cls, payload: dict[str, Any], **kwargs: Any) -> MarketEvent:
        return ensure_legacy_market_event(cls.normalize_typed(payload, **kwargs))

    @staticmethod
    def snapshot_payload_from_row(symbol: str, row: list[Any], *, interval: str = "1m") -> dict[str, Any]:
        close = str(row[4])
        return {
            "s": normalize_symbol(symbol),
            "E": int(row[6]),
            "k": {
                "t": int(row[0]),
                "T": int(row[6]),
                "o": str(row[1]) if len(row) > 1 and row[1] not in ("", None) else close,
                "h": str(row[2]) if len(row) > 2 and row[2] not in ("", None) else close,
                "l": str(row[3]) if len(row) > 3 and row[3] not in ("", None) else close,
                "c": close,
                "q": _quote_volume_from_snapshot_row(row),
                "i": interval,
            },
        }


BINANCE_FEED_NORMALIZERS: dict[str, BinanceFeedNormalizer] = {
    "trade": BinanceTradeNormalizer,
    "kline": BinanceBarNormalizer,
}


def build_binance_stream(stream_type: str, symbol: str) -> str:
    normalizer = BINANCE_FEED_NORMALIZERS.get(stream_type)
    if normalizer is None:
        raise KeyError(f"Unknown stream type: {stream_type}")
    return normalizer.build_stream(symbol)


def normalize_binance_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
) -> IngestionEvent:
    normalizer = BINANCE_FEED_NORMALIZERS.get(event_type)
    if normalizer is None:
        raise KeyError(f"Unknown event type: {event_type}")
    assert_binance_payload_schema(event_type, payload)
    return normalizer.normalize_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
    )


def snapshot_payload_from_row(
    stream_type: str,
    symbol: str,
    row: list[Any],
    *,
    interval: str = "1m",
) -> dict[str, Any]:
    normalizer = BINANCE_FEED_NORMALIZERS.get(stream_type)
    if normalizer is None or not getattr(normalizer, "supports_snapshot", False):
        raise KeyError(f"stream type does not support snapshot payloads: {stream_type}")
    if stream_type == "trade":
        return BinanceTradeNormalizer.snapshot_payload_from_row(symbol, row)
    if stream_type == "kline":
        return BinanceBarNormalizer.snapshot_payload_from_row(symbol, row, interval=interval)
    raise KeyError(f"stream type does not support snapshot payloads: {stream_type}")
