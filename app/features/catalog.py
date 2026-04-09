from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Iterable


class StrategyFamily(str, Enum):
    TREND_FOLLOWING = "trend_following"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    MOMENTUM = "momentum"
    VOLATILITY_TRADING = "volatility_trading"
    CROSS_SECTIONAL = "cross_sectional"
    STAT_ARB = "stat_arb"
    PORTFOLIO_OVERLAY = "portfolio_overlay"
    RISK_AWARE_ALLOCATION = "risk_aware_allocation"
    EXECUTION_SENSITIVE = "execution_sensitive"
    MARKET_MAKING_MICROSTRUCTURE = "market_making_microstructure"
    EVENT_REGIME = "event_regime"


class SourceScope(str, Enum):
    TRADE = "trade"
    KLINE = "kline"
    PORTFOLIO_STATE = "portfolio_state"
    EXECUTION_REPORTS = "execution_reports"
    BOOK = "book"
    FUNDING_OPEN_INTEREST = "funding/open_interest"
    MACRO_NEWS_EXOGENOUS = "macro/news/exogenous"


class FeatureFamily(str, Enum):
    PRICE_RETURNS = "price_returns"
    TREND = "trend"
    MOMENTUM = "momentum"
    VOLATILITY_RANGE = "volatility_range"
    VOLUME_ACTIVITY = "volume_activity"
    VWAP_FLOW = "vwap_flow"
    CANDLE_STRUCTURE = "candle_structure"
    TIME_SESSION = "time_session"
    REGIME_CONTEXT = "regime_context"
    CROSS_ASSET_RELATIVE = "cross_asset_relative"
    PORTFOLIO_RISK = "portfolio_risk"
    QUALITY_OPERABILITY = "quality_operability"
    MICROSTRUCTURE_BOOK = "microstructure_book"
    DERIVATIVES_CARRY = "derivatives_carry"
    EXOGENOUS_MACRO_NEWS = "exogenous_macro_news"


class CatalogPhase(str, Enum):
    PHASE_1 = "phase_1"
    PHASE_2 = "phase_2"
    PHASE_3 = "phase_3"


class CatalogStatus(str, Enum):
    IMPLEMENTED = "implemented"
    PLANNED = "planned"
    FUTURE = "future"


ALL_TARGETS = ("research", "backtesting", "paper", "live")
NON_LIVE_TARGETS = ("research", "backtesting", "paper")
RESEARCH_TARGETS = ("research", "backtesting")
DEFAULT_ENTITY_KEYS = ("symbol",)

TREND_STRATEGIES = (
    StrategyFamily.TREND_FOLLOWING,
    StrategyFamily.BREAKOUT,
    StrategyFamily.MEAN_REVERSION,
)
MOMENTUM_STRATEGIES = (
    StrategyFamily.MOMENTUM,
    StrategyFamily.TREND_FOLLOWING,
    StrategyFamily.BREAKOUT,
)
VOLATILITY_STRATEGIES = (
    StrategyFamily.VOLATILITY_TRADING,
    StrategyFamily.BREAKOUT,
    StrategyFamily.RISK_AWARE_ALLOCATION,
)
FLOW_STRATEGIES = (
    StrategyFamily.EXECUTION_SENSITIVE,
    StrategyFamily.MEAN_REVERSION,
)
PORTFOLIO_STRATEGIES = (
    StrategyFamily.PORTFOLIO_OVERLAY,
    StrategyFamily.RISK_AWARE_ALLOCATION,
)
REGIME_STRATEGIES = (
    StrategyFamily.EVENT_REGIME,
    StrategyFamily.TREND_FOLLOWING,
    StrategyFamily.MEAN_REVERSION,
    StrategyFamily.VOLATILITY_TRADING,
)


@dataclass(frozen=True)
class FeatureCatalogEntry:
    feature_name: str
    feature_family: FeatureFamily
    strategy_families: tuple[StrategyFamily, ...]
    source_scope: tuple[SourceScope, ...]
    phase: CatalogPhase
    bundle_name: str
    entity_keys: tuple[str, ...]
    required_inputs: tuple[str, ...]
    lookback: int
    warmup: int
    target_support: tuple[str, ...]
    status: CatalogStatus
    runtime_aliases: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "feature_name": self.feature_name,
            "feature_family": self.feature_family.value,
            "strategy_families": [item.value for item in self.strategy_families],
            "source_scope": [item.value for item in self.source_scope],
            "phase": self.phase.value,
            "bundle_name": self.bundle_name,
            "entity_keys": list(self.entity_keys),
            "required_inputs": list(self.required_inputs),
            "lookback": self.lookback,
            "warmup": self.warmup,
            "target_support": list(self.target_support),
            "status": self.status.value,
            "runtime_aliases": list(self.runtime_aliases),
        }


def _entry(
    feature_name: str,
    *,
    family: FeatureFamily,
    strategies: tuple[StrategyFamily, ...],
    source_scope: tuple[SourceScope, ...],
    phase: CatalogPhase,
    bundle_name: str,
    required_inputs: Iterable[str] = (),
    lookback: int = 0,
    warmup: int = 0,
    target_support: tuple[str, ...] = ALL_TARGETS,
    status: CatalogStatus = CatalogStatus.PLANNED,
    runtime_aliases: Iterable[str] = (),
    entity_keys: tuple[str, ...] = DEFAULT_ENTITY_KEYS,
) -> FeatureCatalogEntry:
    return FeatureCatalogEntry(
        feature_name=feature_name,
        feature_family=family,
        strategy_families=tuple(strategies),
        source_scope=tuple(source_scope),
        phase=phase,
        bundle_name=bundle_name,
        entity_keys=tuple(entity_keys),
        required_inputs=tuple(required_inputs),
        lookback=lookback,
        warmup=warmup,
        target_support=tuple(target_support),
        status=status,
        runtime_aliases=tuple(runtime_aliases),
    )


def _rolling_entries(
    *,
    prefix: str,
    windows: Iterable[int],
    family: FeatureFamily,
    strategies: tuple[StrategyFamily, ...],
    source_scope: tuple[SourceScope, ...],
    phase: CatalogPhase,
    bundle_name: str,
    status: CatalogStatus,
    required_inputs: Iterable[str] = ("price.last",),
) -> list[FeatureCatalogEntry]:
    return [
        _entry(
            f"{prefix}.{window}",
            family=family,
            strategies=strategies,
            source_scope=source_scope,
            phase=phase,
            bundle_name=bundle_name,
            required_inputs=required_inputs,
            lookback=window,
            warmup=window,
            status=status,
            runtime_aliases=(f"{prefix.split('.')[-1]}_{window}",),
        )
        for window in windows
    ]


class FeatureCatalog:
    def __init__(self, entries: Iterable[FeatureCatalogEntry]) -> None:
        self._entries = tuple(entries)

    def all(self) -> tuple[FeatureCatalogEntry, ...]:
        return self._entries

    def list_features(
        self,
        *,
        family: str | None = None,
        strategy_family: str | None = None,
        phase: str | None = None,
        source_scope: str | None = None,
        status: str | None = None,
        bundle_name: str | None = None,
    ) -> tuple[FeatureCatalogEntry, ...]:
        entries = self._entries
        if family:
            entries = tuple(item for item in entries if item.feature_family.value == family)
        if strategy_family:
            entries = tuple(item for item in entries if any(group.value == strategy_family for group in item.strategy_families))
        if phase:
            entries = tuple(item for item in entries if item.phase.value == phase)
        if source_scope:
            entries = tuple(item for item in entries if any(scope.value == source_scope for scope in item.source_scope))
        if status:
            entries = tuple(item for item in entries if item.status.value == status)
        if bundle_name:
            entries = tuple(item for item in entries if item.bundle_name == bundle_name)
        return entries

    def list_families(self) -> tuple[str, ...]:
        return tuple(item.value for item in FeatureFamily)

    def list_strategy_families(self) -> tuple[str, ...]:
        return tuple(item.value for item in StrategyFamily)

    def list_source_scopes(self) -> tuple[str, ...]:
        return tuple(item.value for item in SourceScope)

    def list_bundles(self) -> tuple[str, ...]:
        return tuple(sorted({item.bundle_name for item in self._entries}))


def _phase1_entries() -> list[FeatureCatalogEntry]:
    entries: list[FeatureCatalogEntry] = [
        _entry(
            "price.last",
            family=FeatureFamily.PRICE_RETURNS,
            strategies=(StrategyFamily.TREND_FOLLOWING, StrategyFamily.MEAN_REVERSION, StrategyFamily.MOMENTUM, StrategyFamily.BREAKOUT, StrategyFamily.STAT_ARB),
            source_scope=(SourceScope.TRADE, SourceScope.KLINE),
            phase=CatalogPhase.PHASE_1,
            bundle_name="core_market_bundle",
            status=CatalogStatus.IMPLEMENTED,
            runtime_aliases=("price",),
        ),
        _entry(
            "ret.log.1s",
            family=FeatureFamily.PRICE_RETURNS,
            strategies=MOMENTUM_STRATEGIES + (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB),
            source_scope=(SourceScope.TRADE, SourceScope.KLINE),
            phase=CatalogPhase.PHASE_1,
            bundle_name="core_market_bundle",
            required_inputs=("price.last",),
            lookback=1,
            warmup=1,
            status=CatalogStatus.IMPLEMENTED,
            runtime_aliases=("ret_1",),
        ),
    ]
    for horizon in ("5s", "30s", "1m"):
        entries.append(
            _entry(
                f"ret.log.{horizon}",
                family=FeatureFamily.PRICE_RETURNS,
                strategies=MOMENTUM_STRATEGIES + (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB),
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="core_market_bundle",
                required_inputs=("price.last",),
                lookback=1,
                warmup=1,
            )
        )
    for horizon in ("1s", "5s", "30s"):
        entries.append(
            _entry(
                f"ret.simple.{horizon}",
                family=FeatureFamily.PRICE_RETURNS,
                strategies=MOMENTUM_STRATEGIES + (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB),
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="core_market_bundle",
                required_inputs=("price.last",),
                lookback=1,
                warmup=1,
            )
        )
    for horizon in (10, 30):
        entries.append(
            _entry(
                f"ret.cum.{horizon}",
                family=FeatureFamily.PRICE_RETURNS,
                strategies=(StrategyFamily.MOMENTUM, StrategyFamily.TREND_FOLLOWING, StrategyFamily.STAT_ARB),
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="core_market_bundle",
                required_inputs=("ret.log.1s",),
                lookback=horizon,
                warmup=horizon,
            )
        )

    entries.extend(
        _rolling_entries(
            prefix="trend.sma",
            windows=(3, 5, 10, 20, 50),
            family=FeatureFamily.TREND,
            strategies=TREND_STRATEGIES,
            source_scope=(SourceScope.TRADE, SourceScope.KLINE),
            phase=CatalogPhase.PHASE_1,
            bundle_name="trend_bundle",
            status=CatalogStatus.IMPLEMENTED,
        )
    )
    entries.extend(
        _rolling_entries(
            prefix="trend.ema",
            windows=(5, 10, 20, 50),
            family=FeatureFamily.TREND,
            strategies=TREND_STRATEGIES,
            source_scope=(SourceScope.TRADE, SourceScope.KLINE),
            phase=CatalogPhase.PHASE_1,
            bundle_name="trend_bundle",
            status=CatalogStatus.IMPLEMENTED,
        )
    )
    for window in (20, 50):
        entries.append(
            _entry(
                f"trend.price_distance_sma.{window}",
                family=FeatureFamily.TREND,
                strategies=TREND_STRATEGIES,
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="trend_bundle",
                required_inputs=("price.last", f"trend.sma.{window}"),
                lookback=window,
                warmup=window,
            )
        )
    entries.extend(
        [
            _entry("trend.sma_cross.5_20", family=FeatureFamily.TREND, strategies=(StrategyFamily.TREND_FOLLOWING, StrategyFamily.BREAKOUT), source_scope=(SourceScope.TRADE, SourceScope.KLINE), phase=CatalogPhase.PHASE_1, bundle_name="trend_bundle", required_inputs=("trend.sma.5", "trend.sma.20"), lookback=20, warmup=20),
            _entry("trend.sma_cross.20_50", family=FeatureFamily.TREND, strategies=(StrategyFamily.TREND_FOLLOWING, StrategyFamily.BREAKOUT), source_scope=(SourceScope.TRADE, SourceScope.KLINE), phase=CatalogPhase.PHASE_1, bundle_name="trend_bundle", required_inputs=("trend.sma.20", "trend.sma.50"), lookback=50, warmup=50),
            _entry("trend.ema_cross.20_50", family=FeatureFamily.TREND, strategies=(StrategyFamily.TREND_FOLLOWING, StrategyFamily.BREAKOUT), source_scope=(SourceScope.TRADE, SourceScope.KLINE), phase=CatalogPhase.PHASE_1, bundle_name="trend_bundle", required_inputs=("trend.ema.20", "trend.ema.50"), lookback=50, warmup=50),
        ]
    )

    entries.extend(
        [
            _entry("momentum.10", family=FeatureFamily.MOMENTUM, strategies=MOMENTUM_STRATEGIES, source_scope=(SourceScope.TRADE, SourceScope.KLINE), phase=CatalogPhase.PHASE_1, bundle_name="momentum_bundle", required_inputs=("ret.log.1s",), lookback=10, warmup=10),
            _entry("momentum.30", family=FeatureFamily.MOMENTUM, strategies=MOMENTUM_STRATEGIES, source_scope=(SourceScope.TRADE, SourceScope.KLINE), phase=CatalogPhase.PHASE_1, bundle_name="momentum_bundle", required_inputs=("ret.log.1s",), lookback=30, warmup=30),
        ]
    )
    for window in (1, 5, 10):
        entries.append(
            _entry(
                f"momentum.delta.{window}",
                family=FeatureFamily.MOMENTUM,
                strategies=MOMENTUM_STRATEGIES,
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="momentum_bundle",
                required_inputs=("price.last",),
                lookback=window,
                warmup=window,
            )
        )
    for window in (1, 5):
        entries.append(
            _entry(
                f"momentum.ratio.{window}",
                family=FeatureFamily.MOMENTUM,
                strategies=MOMENTUM_STRATEGIES,
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="momentum_bundle",
                required_inputs=("price.last",),
                lookback=window,
                warmup=window,
            )
        )
    for window in (5, 10):
        entries.append(
            _entry(
                f"momentum.acceleration.{window}",
                family=FeatureFamily.MOMENTUM,
                strategies=MOMENTUM_STRATEGIES,
                source_scope=(SourceScope.TRADE, SourceScope.KLINE),
                phase=CatalogPhase.PHASE_1,
                bundle_name="momentum_bundle",
                required_inputs=("ret.log.1s",),
                lookback=window,
                warmup=window,
            )
        )

    phase1_specs = [
        ("vol.rolling_std.1m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.rolling_std.5m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.realized.1m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.realized.5m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.true_range", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.atr.14", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.atr.20", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.range.1m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.range.5m", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.high_low_spread", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.close_open_gap", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("vol.volatility_zscore", FeatureFamily.VOLATILITY_RANGE, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("norm.price_zscore.20", FeatureFamily.QUALITY_OPERABILITY, (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB), (SourceScope.TRADE, SourceScope.KLINE)),
        ("norm.price_zscore.50", FeatureFamily.QUALITY_OPERABILITY, (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB), (SourceScope.TRADE, SourceScope.KLINE)),
        ("norm.ret_zscore.20", FeatureFamily.QUALITY_OPERABILITY, (StrategyFamily.MEAN_REVERSION, StrategyFamily.STAT_ARB), (SourceScope.TRADE, SourceScope.KLINE)),
        ("norm.vol_zscore.20", FeatureFamily.QUALITY_OPERABILITY, VOLATILITY_STRATEGIES, (SourceScope.KLINE,)),
        ("norm.price_clip", FeatureFamily.QUALITY_OPERABILITY, (StrategyFamily.MEAN_REVERSION, StrategyFamily.VOLATILITY_TRADING), (SourceScope.TRADE, SourceScope.KLINE)),
        ("norm.ret_clip", FeatureFamily.QUALITY_OPERABILITY, (StrategyFamily.MEAN_REVERSION, StrategyFamily.VOLATILITY_TRADING), (SourceScope.TRADE, SourceScope.KLINE)),
        ("activity.volume.1m", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.MOMENTUM, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.KLINE,)),
        ("activity.volume.5m", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.MOMENTUM, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.KLINE,)),
        ("activity.trade_count.1m", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.TRADE,)),
        ("activity.trade_count.5m", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.TRADE,)),
        ("activity.volume_spike", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.MOMENTUM), (SourceScope.TRADE, SourceScope.KLINE)),
        ("activity.volume_zscore", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.BREAKOUT, StrategyFamily.MOMENTUM), (SourceScope.TRADE, SourceScope.KLINE)),
        ("activity.volume_per_trade", FeatureFamily.VOLUME_ACTIVITY, (StrategyFamily.EXECUTION_SENSITIVE, StrategyFamily.MOMENTUM), (SourceScope.TRADE,)),
        ("flow.vwap.1m", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.vwap.5m", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.vwap_distance.1m", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.vwap_distance.5m", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.trade_side_imbalance", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.buy_volume_ratio", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("flow.sell_volume_ratio", FeatureFamily.VWAP_FLOW, FLOW_STRATEGIES, (SourceScope.TRADE,)),
        ("candle.body_size", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.BREAKOUT, StrategyFamily.MEAN_REVERSION, StrategyFamily.EVENT_REGIME), (SourceScope.KLINE,)),
        ("candle.upper_wick_ratio", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.BREAKOUT, StrategyFamily.MEAN_REVERSION), (SourceScope.KLINE,)),
        ("candle.lower_wick_ratio", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.BREAKOUT, StrategyFamily.MEAN_REVERSION), (SourceScope.KLINE,)),
        ("candle.close_position_in_range", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.MEAN_REVERSION, StrategyFamily.BREAKOUT), (SourceScope.KLINE,)),
        ("candle.direction_streak.3", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.BREAKOUT, StrategyFamily.TREND_FOLLOWING), (SourceScope.KLINE,)),
        ("candle.direction_streak.5", FeatureFamily.CANDLE_STRUCTURE, (StrategyFamily.BREAKOUT, StrategyFamily.TREND_FOLLOWING), (SourceScope.KLINE,)),
        ("time.minute_of_day", FeatureFamily.TIME_SESSION, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("time.day_of_week", FeatureFamily.TIME_SESSION, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("time.time_since_last_event", FeatureFamily.TIME_SESSION, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("time.bar_age", FeatureFamily.TIME_SESSION, REGIME_STRATEGIES, (SourceScope.KLINE,)),
        ("time.session_bucket", FeatureFamily.TIME_SESSION, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("regime.trend_strength", FeatureFamily.REGIME_CONTEXT, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("regime.volatility_regime", FeatureFamily.REGIME_CONTEXT, REGIME_STRATEGIES, (SourceScope.KLINE,)),
        ("regime.volume_regime", FeatureFamily.REGIME_CONTEXT, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("regime.mean_reversion_score", FeatureFamily.REGIME_CONTEXT, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE)),
        ("risk.position_size", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("risk.position_notional", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("risk.cash_available_ratio", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("risk.exposure_utilization", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("risk.distance_to_limit", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("risk.inventory_direction", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE,)),
        ("quality.available_delay", FeatureFamily.QUALITY_OPERABILITY, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE, SourceScope.PORTFOLIO_STATE)),
        ("quality.source_cutoff_lag", FeatureFamily.QUALITY_OPERABILITY, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE, SourceScope.PORTFOLIO_STATE)),
        ("quality.missing_input_flag", FeatureFamily.QUALITY_OPERABILITY, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE, SourceScope.PORTFOLIO_STATE)),
        ("quality.staleness_score", FeatureFamily.QUALITY_OPERABILITY, REGIME_STRATEGIES, (SourceScope.TRADE, SourceScope.KLINE, SourceScope.PORTFOLIO_STATE)),
    ]
    for name, family, strategies, scopes in phase1_specs:
        bundle_name = {
            FeatureFamily.VOLATILITY_RANGE: "volatility_bundle",
            FeatureFamily.VOLUME_ACTIVITY: "activity_bundle",
            FeatureFamily.VWAP_FLOW: "flow_bundle",
            FeatureFamily.CANDLE_STRUCTURE: "candle_bundle",
            FeatureFamily.TIME_SESSION: "time_session_bundle",
            FeatureFamily.REGIME_CONTEXT: "regime_bundle",
            FeatureFamily.PORTFOLIO_RISK: "portfolio_risk_bundle",
            FeatureFamily.QUALITY_OPERABILITY: "quality_bundle",
        }.get(family, "core_market_bundle")
        entries.append(
            _entry(
                name,
                family=family,
                strategies=strategies,
                source_scope=scopes,
                phase=CatalogPhase.PHASE_1,
                bundle_name=bundle_name,
            )
        )
    return entries


def _phase2_entries() -> list[FeatureCatalogEntry]:
    specs = [
        ("cross_asset.spread", FeatureFamily.CROSS_ASSET_RELATIVE, (StrategyFamily.STAT_ARB, StrategyFamily.CROSS_SECTIONAL), (SourceScope.TRADE, SourceScope.KLINE)),
        ("cross_asset.ratio", FeatureFamily.CROSS_ASSET_RELATIVE, (StrategyFamily.STAT_ARB, StrategyFamily.CROSS_SECTIONAL), (SourceScope.TRADE, SourceScope.KLINE)),
        ("cross_asset.relative_strength", FeatureFamily.CROSS_ASSET_RELATIVE, (StrategyFamily.MOMENTUM, StrategyFamily.CROSS_SECTIONAL), (SourceScope.TRADE, SourceScope.KLINE)),
        ("cross_section.rank_return", FeatureFamily.CROSS_ASSET_RELATIVE, (StrategyFamily.CROSS_SECTIONAL,), (SourceScope.TRADE, SourceScope.KLINE)),
        ("cross_section.rank_volume", FeatureFamily.CROSS_ASSET_RELATIVE, (StrategyFamily.CROSS_SECTIONAL,), (SourceScope.TRADE, SourceScope.KLINE)),
        ("portfolio.beta_proxy", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE, SourceScope.TRADE, SourceScope.KLINE)),
        ("portfolio.correlation_cluster", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE, SourceScope.TRADE, SourceScope.KLINE)),
        ("execution.fill_slippage_proxy", FeatureFamily.PORTFOLIO_RISK, (StrategyFamily.EXECUTION_SENSITIVE,), (SourceScope.EXECUTION_REPORTS,)),
        ("execution.latency_proxy", FeatureFamily.PORTFOLIO_RISK, (StrategyFamily.EXECUTION_SENSITIVE,), (SourceScope.EXECUTION_REPORTS,)),
        ("risk.turnover_estimate", FeatureFamily.PORTFOLIO_RISK, PORTFOLIO_STRATEGIES, (SourceScope.PORTFOLIO_STATE, SourceScope.EXECUTION_REPORTS)),
    ]
    return [
        _entry(
            name,
            family=family,
            strategies=strategies,
            source_scope=scopes,
            phase=CatalogPhase.PHASE_2,
            bundle_name="phase2_extension_bundle",
            target_support=NON_LIVE_TARGETS,
        )
        for name, family, strategies, scopes in specs
    ]


def _phase3_entries() -> list[FeatureCatalogEntry]:
    specs = [
        ("book.bid_ask_spread", FeatureFamily.MICROSTRUCTURE_BOOK, (StrategyFamily.MARKET_MAKING_MICROSTRUCTURE, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.BOOK,)),
        ("book.microprice", FeatureFamily.MICROSTRUCTURE_BOOK, (StrategyFamily.MARKET_MAKING_MICROSTRUCTURE, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.BOOK,)),
        ("book.depth_imbalance", FeatureFamily.MICROSTRUCTURE_BOOK, (StrategyFamily.MARKET_MAKING_MICROSTRUCTURE, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.BOOK,)),
        ("book.queue_pressure", FeatureFamily.MICROSTRUCTURE_BOOK, (StrategyFamily.MARKET_MAKING_MICROSTRUCTURE, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.BOOK,)),
        ("book.order_flow_imbalance", FeatureFamily.MICROSTRUCTURE_BOOK, (StrategyFamily.MARKET_MAKING_MICROSTRUCTURE, StrategyFamily.EXECUTION_SENSITIVE), (SourceScope.BOOK,)),
        ("derivatives.funding_rate", FeatureFamily.DERIVATIVES_CARRY, (StrategyFamily.EVENT_REGIME, StrategyFamily.STAT_ARB), (SourceScope.FUNDING_OPEN_INTEREST,)),
        ("derivatives.open_interest_change", FeatureFamily.DERIVATIVES_CARRY, (StrategyFamily.EVENT_REGIME, StrategyFamily.STAT_ARB), (SourceScope.FUNDING_OPEN_INTEREST,)),
        ("derivatives.basis", FeatureFamily.DERIVATIVES_CARRY, (StrategyFamily.EVENT_REGIME, StrategyFamily.STAT_ARB), (SourceScope.FUNDING_OPEN_INTEREST,)),
        ("options.iv_level", FeatureFamily.DERIVATIVES_CARRY, (StrategyFamily.EVENT_REGIME, StrategyFamily.VOLATILITY_TRADING), (SourceScope.FUNDING_OPEN_INTEREST,)),
        ("options.skew", FeatureFamily.DERIVATIVES_CARRY, (StrategyFamily.EVENT_REGIME, StrategyFamily.VOLATILITY_TRADING), (SourceScope.FUNDING_OPEN_INTEREST,)),
        ("macro.calendar_event_proximity", FeatureFamily.EXOGENOUS_MACRO_NEWS, (StrategyFamily.EVENT_REGIME,), (SourceScope.MACRO_NEWS_EXOGENOUS,)),
        ("news.sentiment_score", FeatureFamily.EXOGENOUS_MACRO_NEWS, (StrategyFamily.EVENT_REGIME,), (SourceScope.MACRO_NEWS_EXOGENOUS,)),
    ]
    return [
        _entry(
            name,
            family=family,
            strategies=strategies,
            source_scope=scopes,
            phase=CatalogPhase.PHASE_3,
            bundle_name="phase3_extension_bundle",
            target_support=RESEARCH_TARGETS,
            status=CatalogStatus.FUTURE,
        )
        for name, family, strategies, scopes in specs
    ]


@lru_cache(maxsize=1)
def get_default_feature_catalog() -> FeatureCatalog:
    return FeatureCatalog(tuple(_phase1_entries() + _phase2_entries() + _phase3_entries()))
