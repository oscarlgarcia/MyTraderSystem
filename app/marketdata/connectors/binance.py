"""
Binance feed-specific normalizers and payload helpers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from app.common.dto import MarketEvent, normalize_symbol
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
    supports_snapshot = False

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
        event = TradeEvent(
            symbol=normalize_symbol(str(payload["s"])),
            exchange_ts=_ts_from_ms(int(payload["E"])),
            receive_ts=receive_ts,
            process_ts=_process_ts(process_ts),
            venue=venue,
            source_id=str(payload.get("t")) if payload.get("t") is not None else None,
            metadata=stamp_normalizer_version(_instrument_metadata(str(payload["s"]), venue)),
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
    if stream_type == "kline":
        return BinanceBarNormalizer.snapshot_payload_from_row(symbol, row, interval=interval)
    raise KeyError(f"stream type does not support snapshot payloads: {stream_type}")
