from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

from app.common.dto import FeatureVector, MarketEvent
from app.features.cache import FeatureCache
from app.features.entity_codec import entity_scope, normalize_entity_keys, primary_symbol
from app.features.event_journal import FeatureEventJournal
from app.features.definitions import FeatureSetDefinition, build_legacy_feature_set_definition
from app.features.metrics import FeatureMetrics
from app.features.planner import FeaturePlanner
from app.features.state import RuntimeStateStore

logger = logging.getLogger("features.runtime")


STRICT_TEMPORAL_MODES = {"paper", "live"}


def _event_ts(event) -> datetime:
    ts = getattr(event, "event_ts", None)
    if ts is None:
        ts = getattr(event, "exchange_ts")
    return ts


def _available_ts(event) -> datetime:
    ts = getattr(event, "available_ts", None)
    if ts is None:
        return _event_ts(event)
    return ts


def _has_explicit_available_ts(event) -> bool:
    explicit = getattr(event, "has_explicit_available_ts", None)
    if explicit is not None:
        return bool(explicit)
    event_ts = _event_ts(event)
    available_ts = getattr(event, "available_ts", None)
    published_ts = getattr(event, "published_ts", None)
    processed_ts = getattr(event, "processed_ts", None)
    receive_ts = getattr(event, "receive_ts", None)
    provider_ts = getattr(event, "provider_ts", None)
    return any(
        ts is not None and ts != event_ts
        for ts in (available_ts, published_ts, processed_ts, receive_ts, provider_ts)
    )


class FeatureRuntimeEngine:
    def __init__(
        self,
        *,
        feature_set: FeatureSetDefinition,
        cache: FeatureCache | None = None,
        out_of_order_policy: str = "reject",
        strict_temporal_semantics: bool = False,
        runtime_mode: str = "research",
        event_journal: FeatureEventJournal | None = None,
        journal_path: str | Path | None = None,
    ) -> None:
        self.feature_set = feature_set
        self.cache = cache or FeatureCache()
        self.plan = FeaturePlanner().build_plan(feature_set)
        effective_window = max(feature_set.windows) if feature_set.windows else 5
        self.state = RuntimeStateStore(effective_window=effective_window)
        self.metrics = FeatureMetrics()
        self.out_of_order_policy = out_of_order_policy
        self.strict_temporal_semantics = strict_temporal_semantics
        self.runtime_mode = runtime_mode
        self.event_journal = event_journal or (FeatureEventJournal(journal_path) if journal_path is not None else None)

    def _max_recent_events(self) -> int:
        return max(self.state.effective_window * 4, 32)

    def _truncate_recent_events(self, scope: str) -> None:
        max_recent = self._max_recent_events()
        recent = self.state.recent_events[scope]
        if len(recent) > max_recent:
            self.state.recent_events[scope] = recent[-max_recent:]

    def _entity_keys_for_event(self, event: MarketEvent) -> dict[str, str]:
        payload = {}
        for key in self.feature_set.entity_keys:
            if key == "symbol":
                continue
            payload[key] = event.metadata.get(key) or event.metadata.get(f"entity:{key}")
        keys = normalize_entity_keys(payload, symbol=event.symbol, required_keys=self.feature_set.entity_keys)
        event.metadata.setdefault("feature_entity_scope", entity_scope(keys))
        for key, value in keys.items():
            if key != "symbol":
                event.metadata.setdefault(f"entity:{key}", value)
        return keys

    def _enforce_temporal_semantics(self, event) -> None:
        if not (self.strict_temporal_semantics or self.runtime_mode in STRICT_TEMPORAL_MODES):
            return
        if not _has_explicit_available_ts(event):
            raise ValueError(
                "strict temporal semantics require explicit available_ts/published_ts/processing timestamps"
            )

    def _compute_from_event(self, event: MarketEvent, *, record_event: bool = True) -> FeatureVector | None:
        if not isinstance(event.price, (int, float)) or not math.isfinite(event.price):
            self.metrics.dropped_non_finite += 1
            return None
        entity_keys = self._entity_keys_for_event(event)
        scope = entity_scope(entity_keys)
        event_ts = _event_ts(event)
        event_available_ts = _available_ts(event)
        prices = self.state.prices[scope]
        prices.append(float(event.price))
        if record_event:
            self.state.recent_events[scope].append(event)
            self._truncate_recent_events(scope)
        values: Dict[str, float] = {}
        context: Dict[str, float] = {}
        for node in self.plan.nodes:
            computed = node.compute(event=event, price_history=prices, context=context, runtime_state=self.state)
            if computed:
                values.update(computed)
                context.update(computed)
        for name, value in values.items():
            self.state.node_history[(scope, name)].append(float(value))
        fv = FeatureVector(
            symbol=primary_symbol(entity_keys, fallback_symbol=event.symbol),
            ts=event_ts,
            available_ts=event_available_ts,
            source_cutoff_ts=event_available_ts,
            values=values,
            feature_set_name=self.feature_set.name,
            feature_set_version=self.feature_set.version,
            entity_keys=entity_keys,
        )
        if event.price > 0:
            self.state.previous_price[scope] = float(event.price)
        self.state.watermarks[scope] = event_available_ts
        self.cache.put(fv)
        return fv

    def _recompute_scope(self, scope: str) -> List[FeatureVector]:
        if self.event_journal is not None:
            events = self.event_journal.load_scope_events(scope)
        else:
            events = list(self.state.recent_events[scope])
        events = sorted(events, key=lambda ev: (_available_ts(ev), _event_ts(ev)))
        self.state.reset_scope(scope)
        out: List[FeatureVector] = []
        self.state.recent_events[scope] = list(events)
        self._truncate_recent_events(scope)
        for ev in events:
            fv = self._compute_from_event(ev, record_event=False)
            if fv is not None:
                out.append(fv)
        return out

    def update(self, event: MarketEvent) -> FeatureVector | None:
        self.metrics.events_in += 1
        self._enforce_temporal_semantics(event)
        entity_keys = self._entity_keys_for_event(event)
        scope = entity_scope(entity_keys)
        if self.event_journal is not None:
            self.event_journal.append(event, entity_keys=entity_keys)
        watermark = self.state.watermarks.get(scope)
        event_available_ts = _available_ts(event)
        if watermark is not None and event_available_ts < watermark:
            if self.out_of_order_policy == "reject":
                self.metrics.transform_errors += 1
                return None
            if self.out_of_order_policy == "recompute":
                if self.event_journal is None:
                    self.state.recent_events[scope].append(event)
                recomputed = self._recompute_scope(scope)
                if recomputed:
                    self.metrics.features_out += 1
                    return recomputed[-1]
                return None
        start = time.perf_counter()
        fv = self._compute_from_event(event)
        elapsed = time.perf_counter() - start
        self.metrics.compute_latency_total += elapsed
        self.metrics.compute_latency_max = max(self.metrics.compute_latency_max, elapsed)
        if fv is not None:
            self.metrics.features_out += 1
        return fv

    def update_batch(self, events: Iterable[MarketEvent]) -> List[FeatureVector]:
        out: List[FeatureVector] = []
        for event in events:
            fv = self.update(event)
            if fv is not None:
                out.append(fv)
        return out

    def restore_state(self, state: RuntimeStateStore) -> None:
        self.state = state

    def avg_latency(self) -> float:
        if self.metrics.features_out == 0:
            return 0.0
        return self.metrics.compute_latency_total / self.metrics.features_out


def build_legacy_runtime_feature_set(
    *,
    window: int = 5,
    windows: Iterable[int] | None = None,
    aggregators: Iterable[str] | None = None,
    transformers: Iterable[str] | None = None,
) -> FeatureSetDefinition:
    return build_legacy_feature_set_definition(
        name="legacy",
        version="legacy",
        description="Legacy runtime feature set",
        windows=tuple(windows or [window]),
        aggregators=tuple(aggregators or ["sma", "ema", "max", "min"]),
        transformers=tuple(transformers or []),
    )
