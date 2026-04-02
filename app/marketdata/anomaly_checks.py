from __future__ import annotations

from dataclasses import dataclass

from app.marketdata.models import IngestionEvent


@dataclass(frozen=True, slots=True)
class MarketdataAnomaly:
    anomaly_type: str
    previous_price: float
    current_price: float
    relative_jump: float



def detect_price_jump(
    *,
    previous_price: float | None,
    current_price: float,
    relative_jump_threshold: float = 0.20,
) -> MarketdataAnomaly | None:
    if previous_price in (None, 0):
        return None
    relative_jump = abs(float(current_price) - float(previous_price)) / abs(float(previous_price))
    if relative_jump < float(relative_jump_threshold):
        return None
    return MarketdataAnomaly(
        anomaly_type="price_jump",
        previous_price=float(previous_price),
        current_price=float(current_price),
        relative_jump=relative_jump,
    )



def stream_price_key(event: IngestionEvent) -> str:
    venue = str(getattr(event, "venue", "BINANCE")).upper()
    return f"{venue}:{event.symbol}:{event.source}"
