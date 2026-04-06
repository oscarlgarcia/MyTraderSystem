from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, List, Tuple

from app.common.dto import MarketEvent
from app.features.entity_codec import decode_entity_scope, entity_scope, primary_symbol


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
        self.node_history: Dict[Tuple[str, str], Deque[float]] = defaultdict(lambda: deque(maxlen=512))

    def scope_for_event(self, event: MarketEvent) -> str:
        return event.metadata.get("feature_entity_scope", entity_scope({"symbol": event.symbol}))

    def entity_keys_for_event(self, event: MarketEvent) -> dict[str, str]:
        scope = self.scope_for_event(event)
        keys = decode_entity_scope(scope)
        if not keys:
            keys = {"symbol": event.symbol}
        return keys

    def symbol_for_scope(self, scope: str) -> str:
        return primary_symbol(decode_entity_scope(scope))

    def reset(self) -> None:
        self.prices.clear()
        self.previous_price.clear()
        self.agg_state.clear()
        self.watermarks.clear()
        self.recent_events.clear()
        self.node_history.clear()

    def reset_scope(self, scope: str) -> None:
        self.prices.pop(scope, None)
        self.previous_price.pop(scope, None)
        self.watermarks.pop(scope, None)
        self.recent_events.pop(scope, None)
        for key in [key for key in self.agg_state if key[1] == scope]:
            self.agg_state.pop(key, None)
        for key in [key for key in self.node_history if key[0] == scope]:
            self.node_history.pop(key, None)

    def snapshot(self) -> dict:
        return {
            "schema_version": "v2",
            "effective_window": self.effective_window,
            "prices": {scope: list(values) for scope, values in self.prices.items()},
            "previous_price": dict(self.previous_price),
            "agg_state": {"|".join([k[0], k[1], str(k[2])]): v for k, v in self.agg_state.items()},
            "watermarks": {scope: ts.isoformat() for scope, ts in self.watermarks.items()},
            "recent_events": {
                scope: [
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
                for scope, events in self.recent_events.items()
            },
            "node_history": {"|".join([scope, name]): list(values) for (scope, name), values in self.node_history.items()},
        }

    @classmethod
    def from_snapshot(cls, payload: dict) -> "RuntimeStateStore":
        from datetime import datetime
        from app.common.dto import MarketEvent

        state = cls(effective_window=int(payload["effective_window"]))
        for scope, prices in payload.get("prices", {}).items():
            state.prices[scope].extend(prices)
        for scope, prev in payload.get("previous_price", {}).items():
            state.previous_price[scope] = prev
        for key, value in payload.get("agg_state", {}).items():
            name, scope, window = key.split("|")
            state.agg_state[(name, scope, int(window))] = value
        for scope, ts in payload.get("watermarks", {}).items():
            state.watermarks[scope] = datetime.fromisoformat(ts)
        for scope, events in payload.get("recent_events", {}).items():
            state.recent_events[scope] = [
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
        for key, values in payload.get("node_history", {}).items():
            scope, name = key.split("|", 1)
            state.node_history[(scope, name)].extend(float(value) for value in values)
        return state
