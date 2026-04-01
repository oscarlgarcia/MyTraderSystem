"""
Minimal static instrument catalog for supported market data feeds.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from app.common.dto import normalize_symbol


KNOWN_QUOTE_ASSETS: tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "BTC",
    "ETH",
    "EUR",
)


def _normalize_venue(venue: str) -> str:
    normalized = str(venue).upper()
    if not normalized:
        raise ValueError("venue must be non-empty")
    return normalized


def _precision_from_increment(value: str) -> int:
    text = format(Decimal(str(value)).normalize(), "f")
    if "." not in text:
        return 0
    return len(text.rstrip("0").split(".", 1)[1])


def infer_spot_assets(symbol: str, *, known_quotes: Iterable[str] = KNOWN_QUOTE_ASSETS) -> tuple[str, str]:
    normalized = normalize_symbol(symbol)
    for quote_asset in sorted((str(item).upper() for item in known_quotes), key=len, reverse=True):
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            return normalized[: -len(quote_asset)], quote_asset
    raise KeyError(f"unsupported instrument symbol: {normalized}")


@dataclass(frozen=True, slots=True)
class Instrument:
    venue: str
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str = "spot"
    tick_size: str = "0.01"
    step_size: str = "0.000001"
    price_precision: int = 2
    size_precision: int = 6

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", _normalize_venue(self.venue))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "base_asset", normalize_symbol(self.base_asset))
        object.__setattr__(self, "quote_asset", normalize_symbol(self.quote_asset))
        object.__setattr__(self, "contract_type", str(self.contract_type).lower())
        object.__setattr__(self, "tick_size", str(self.tick_size))
        object.__setattr__(self, "step_size", str(self.step_size))
        object.__setattr__(self, "price_precision", int(self.price_precision))
        object.__setattr__(self, "size_precision", int(self.size_precision))

    def as_metadata(self) -> dict[str, str]:
        return {
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "contract_type": self.contract_type,
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "price_precision": str(self.price_precision),
            "size_precision": str(self.size_precision),
        }


class InstrumentCatalog:
    def __init__(self, instruments: Iterable[Instrument] | None = None) -> None:
        self._by_key: dict[tuple[str, str], Instrument] = {}
        if instruments is not None:
            for instrument in instruments:
                self.register(instrument)

    def register(self, instrument: Instrument) -> None:
        self._by_key[(instrument.venue, instrument.symbol)] = instrument

    def register_static_spot_symbol(
        self,
        symbol: str,
        *,
        venue: str = "BINANCE",
        base_asset: str | None = None,
        quote_asset: str | None = None,
        tick_size: str = "0.01",
        step_size: str = "0.000001",
    ) -> Instrument:
        normalized_symbol = normalize_symbol(symbol)
        if base_asset is None or quote_asset is None:
            inferred_base, inferred_quote = infer_spot_assets(normalized_symbol)
            base_asset = base_asset or inferred_base
            quote_asset = quote_asset or inferred_quote
        instrument = Instrument(
            venue=venue,
            symbol=normalized_symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            contract_type="spot",
            tick_size=tick_size,
            step_size=step_size,
            price_precision=_precision_from_increment(tick_size),
            size_precision=_precision_from_increment(step_size),
        )
        self.register(instrument)
        return instrument

    def resolve(self, venue: str, symbol: str) -> Instrument:
        key = (_normalize_venue(venue), normalize_symbol(symbol))
        instrument = self._by_key.get(key)
        if instrument is None:
            raise KeyError(f"unsupported instrument for venue={key[0]} symbol={key[1]}")
        return instrument

    def has(self, venue: str, symbol: str) -> bool:
        return (_normalize_venue(venue), normalize_symbol(symbol)) in self._by_key


DEFAULT_INSTRUMENTS: tuple[Instrument, ...] = (
    Instrument(
        venue="BINANCE",
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="spot",
        tick_size="0.01",
        step_size="0.000001",
        price_precision=2,
        size_precision=6,
    ),
    Instrument(
        venue="BINANCE",
        symbol="ETHUSDT",
        base_asset="ETH",
        quote_asset="USDT",
        contract_type="spot",
        tick_size="0.01",
        step_size="0.0001",
        price_precision=2,
        size_precision=4,
    ),
)

DEFAULT_INSTRUMENT_CATALOG = InstrumentCatalog(DEFAULT_INSTRUMENTS)


def get_default_instrument_catalog() -> InstrumentCatalog:
    return DEFAULT_INSTRUMENT_CATALOG


def ensure_default_instruments(symbols: Iterable[str], *, venue: str = "BINANCE") -> None:
    for symbol in symbols:
        if not DEFAULT_INSTRUMENT_CATALOG.has(venue, symbol):
            DEFAULT_INSTRUMENT_CATALOG.register_static_spot_symbol(symbol, venue=venue)


def resolve_instrument(symbol: str, *, venue: str = "BINANCE", catalog: InstrumentCatalog | None = None) -> Instrument:
    return (catalog or DEFAULT_INSTRUMENT_CATALOG).resolve(venue, symbol)
