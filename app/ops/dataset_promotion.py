from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from app.ops.normalized_contract import NormalizedContractReport, validate_normalized_contract
from app.ops.replay_parity import ReplayParityReport, build_replay_parity_report


PromotionTarget = Literal["backtesting", "paper"]
TradeDatasetUsage = Literal["aggregate_trade", "raw_trade_history"]


class TradeDatasetUsageError(ValueError):
    """Raised when a trade dataset is used outside its historical-feed contract."""


def _allowed_trade_dataset_usages(
    *,
    feed_type: str,
    historical_feed_kind: str | None,
) -> tuple[TradeDatasetUsage, ...]:
    if feed_type != "trade":
        return ()
    if historical_feed_kind == "aggregate_trade":
        return ("aggregate_trade",)
    if historical_feed_kind == "raw_trade":
        return ("aggregate_trade", "raw_trade_history")
    return ()


@dataclass(frozen=True, slots=True)
class DatasetPromotionReport:
    target: PromotionTarget
    normalized_path: str
    raw_base_dir: str
    contract: NormalizedContractReport
    parity: ReplayParityReport
    feed_type: str
    historical_feed_kind: str | None
    approved_trade_dataset_usages: tuple[TradeDatasetUsage, ...]
    pass_ok: bool


@dataclass(frozen=True, slots=True)
class TradeDatasetUsageReport:
    normalized_path: str
    requested_usage: TradeDatasetUsage
    feed_type: str
    historical_feed_kind: str | None
    allowed_usages: tuple[TradeDatasetUsage, ...]
    reasons: tuple[str, ...]
    allowed: bool


def build_dataset_promotion_report(
    *,
    target: PromotionTarget,
    normalized_path: Path,
    raw_base_dir: Path,
    env: str,
    symbol: str,
    stream_type: str,
    contract_mode: Literal["strict", "compat"] = "strict",
) -> DatasetPromotionReport:
    required_historical_feed_kind = "aggregate_trade" if stream_type == "trade" else None
    contract = validate_normalized_contract(
        Path(normalized_path),
        mode=contract_mode,
        required_historical_feed_kind=required_historical_feed_kind,
    )
    parity = build_replay_parity_report(
        raw_base_dir=Path(raw_base_dir),
        normalized_path=Path(normalized_path),
        env=env,
        symbol=symbol,
        stream_type=stream_type,
    )
    approved_trade_dataset_usages = _allowed_trade_dataset_usages(
        feed_type=contract.feed_type,
        historical_feed_kind=contract.historical_feed_kind,
    )
    return DatasetPromotionReport(
        target=target,
        normalized_path=str(normalized_path),
        raw_base_dir=str(raw_base_dir),
        contract=contract,
        parity=parity,
        feed_type=contract.feed_type,
        historical_feed_kind=contract.historical_feed_kind,
        approved_trade_dataset_usages=approved_trade_dataset_usages,
        pass_ok=bool(contract.pass_ok and parity.pass_ok),
    )


def write_dataset_promotion_report(path: Path, report: DatasetPromotionReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def assert_promoted_trade_dataset_usage(
    report: DatasetPromotionReport,
    *,
    requested_usage: TradeDatasetUsage,
) -> DatasetPromotionReport:
    reasons: list[str] = []
    if report.feed_type != "trade":
        reasons.append(f"dataset feed_type={report.feed_type!r} is not a trade dataset")
    if not report.pass_ok:
        reasons.append("dataset promotion report is not approved for downstream usage")
    if requested_usage not in report.approved_trade_dataset_usages:
        if report.historical_feed_kind == "aggregate_trade" and requested_usage == "raw_trade_history":
            reasons.append(
                "dataset historical_feed_kind=aggregate_trade is aggregated trade history and cannot be used as raw trade history"
            )
        else:
            reasons.append(
                "requested trade usage is incompatible with promoted dataset historical_feed_kind="
                f"{report.historical_feed_kind or 'missing'}"
            )
    if reasons:
        raise TradeDatasetUsageError("; ".join(reasons))
    return report


def build_trade_dataset_usage_report(
    *,
    normalized_path: Path,
    requested_usage: TradeDatasetUsage,
    contract_mode: Literal["strict", "compat"] = "strict",
) -> TradeDatasetUsageReport:
    contract = validate_normalized_contract(Path(normalized_path), mode=contract_mode)
    allowed_usages = _allowed_trade_dataset_usages(
        feed_type=contract.feed_type,
        historical_feed_kind=contract.historical_feed_kind,
    )
    reasons: list[str] = []
    if contract.feed_type != "trade":
        reasons.append(f"dataset feed_type={contract.feed_type!r} is not a trade dataset")
    if not contract.pass_ok:
        reasons.append("dataset does not satisfy the normalized contract required for downstream usage")
    if requested_usage not in allowed_usages:
        if contract.historical_feed_kind == "aggregate_trade" and requested_usage == "raw_trade_history":
            reasons.append(
                "dataset historical_feed_kind=aggregate_trade is aggregated trade history and cannot be used as raw trade history"
            )
        else:
            reasons.append(
                "requested trade usage is incompatible with dataset historical_feed_kind="
                f"{contract.historical_feed_kind or 'missing'}"
            )
    return TradeDatasetUsageReport(
        normalized_path=str(normalized_path),
        requested_usage=requested_usage,
        feed_type=contract.feed_type,
        historical_feed_kind=contract.historical_feed_kind,
        allowed_usages=allowed_usages,
        reasons=tuple(reasons),
        allowed=not reasons,
    )


def assert_trade_dataset_usage(
    *,
    normalized_path: Path,
    requested_usage: TradeDatasetUsage,
    contract_mode: Literal["strict", "compat"] = "strict",
) -> TradeDatasetUsageReport:
    report = build_trade_dataset_usage_report(
        normalized_path=normalized_path,
        requested_usage=requested_usage,
        contract_mode=contract_mode,
    )
    if not report.allowed:
        raise TradeDatasetUsageError("; ".join(report.reasons))
    return report
