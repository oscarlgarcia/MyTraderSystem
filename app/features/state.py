from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, List, Tuple

from app.common.dto import MarketEvent


@dataclass
class SymbolRuntimeState:
    prices: Deque[float]
    previous_price: float | None = None
    watermark_ts: datetime | None = None
    recent_events: List[MarketEvent] = field(default_factory=list)


class RuntimeStateStore:
    def __init__(self, effective_window: int) -> None:
        self.effective_window = effective_window
        self.prices: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=self.effective_window))
        self.previous_price: Dict[str, float | None] = defaultdict(lambda: None)
        self.agg_state: Dict[Tuple[str, str, int], float] = {}
        self.watermarks: Dict[str, datetime] = {}
        self.recent_events: Dict[str, List[MarketEvent]] = defaultdict(list)

    def reset(self) -> None:
        self.prices.clear()
        self.previous_price.clear()
        self.agg_state.clear()
        self.watermarks.clear()
        self.recent_events.clear()

    def snapshot(self) -> dict:
        return {
            "effective_window": self.effective_window,
            "prices": {sym: list(values) for sym, values in self.prices.items()},
            "previous_price": dict(self.previous_price),
            "agg_state": {"|".join([k[0], k[1], str(k[2])]): v for k, v in self.agg_state.items()},
            "watermarks": {sym: ts.isoformat() for sym, ts in self.watermarks.items()},
            "recent_events": {
                sym: [
                    {
                        "symbol": ev.symbol,
                        "event_ts": ev.event_ts.isoformat(),
                        "price": ev.price,
                        "size": ev.size,
                        "source": ev.source,
                        "metadata": ev.metadata,
                        "published_ts": ev.published_ts.isoformat(),
                        "available_ts": ev.available_ts.isoformat(),
                        "processed_ts": ev.processed_ts.isoformat(),
                        "observation_ts": ev.observation_ts.isoformat(),
                    }
                    for ev in events
                ]
                for sym, events in self.recent_events.items()
            },
        }

    @classmethod
    def from_snapshot(cls, payload: dict) -> "RuntimeStateStore":
        from datetime import datetime
        from app.common.dto import MarketEvent

        state = cls(effective_window=int(payload["effective_window"]))
        for sym, prices in payload.get("prices", {}).items():
            state.prices[sym].extend(prices)
        for sym, prev in payload.get("previous_price", {}).items():
            state.previous_price[sym] = prev
        for key, value in payload.get("agg_state", {}).items():
            name, sym, window = key.split("|")
            state.agg_state[(name, sym, int(window))] = value
        for sym, ts in payload.get("watermarks", {}).items():
            state.watermarks[sym] = datetime.fromisoformat(ts)
        for sym, events in payload.get("recent_events", {}).items():
            state.recent_events[sym] = [
                MarketEvent(
                    symbol=item["symbol"],
                    event_ts=datetime.fromisoformat(item["event_ts"]),
                    price=item["price"],
                    size=item["size"],
                    source=item["source"],
                    metadata=item.get("metadata", {}),
                    published_ts=datetime.fromisoformat(item["published_ts"]),
                    available_ts=datetime.fromisoformat(item["available_ts"]),
                    processed_ts=datetime.fromisoformat(item["processed_ts"]),
                    observation_ts=datetime.fromisoformat(item["observation_ts"]),
                )
                for item in events
            ]
        return state
