from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from app.ops.normalized_contract import ContractMode, NormalizedContractReport, validate_normalized_contract
from app.ops.replay_parity import ReplayParityReport, build_replay_parity_report


PromotionTarget = Literal["backtesting", "paper"]
TradeDatasetUsage = Literal["aggregate_trade", "raw_trade_history"]
REQUIRED_PROMOTION_CONTRACT_MODE: ContractMode = "strict"


class TradeDatasetUsageError(ValueError):
    """Raised when a trade dataset is used outside its historical-feed contract."""


class DatasetPromotionApprovalError(ValueError):
    """Raised when a dataset is used without a passing promotion report or registry entry."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    requested_contract_mode: ContractMode
    required_contract_mode: ContractMode
    contract: NormalizedContractReport
    compat_contract: NormalizedContractReport | None
    parity: ReplayParityReport
    feed_type: str
    historical_feed_kind: str | None
    approved_trade_dataset_usages: tuple[TradeDatasetUsage, ...]
    reasons: tuple[str, ...]
    generated_at: str
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


@dataclass(frozen=True, slots=True)
class ApprovedDatasetRegistryEntry:
    target: PromotionTarget
    normalized_path: str
    promotion_report_path: str
    registered_at: str
    approved_contract_mode: ContractMode
    feed_type: str
    historical_feed_kind: str | None
    approved_trade_dataset_usages: tuple[TradeDatasetUsage, ...]


@dataclass(frozen=True, slots=True)
class ApprovedDatasetRegistry:
    updated_at: str
    entries: tuple[ApprovedDatasetRegistryEntry, ...]


def _compat_contract_for_target(
    *,
    normalized_path: Path,
    required_historical_feed_kind: str | None,
    strict_contract: NormalizedContractReport,
    requested_contract_mode: ContractMode,
) -> NormalizedContractReport | None:
    if requested_contract_mode == "compat" or not strict_contract.pass_ok:
        return validate_normalized_contract(
            normalized_path,
            mode="compat",
            required_historical_feed_kind=required_historical_feed_kind,
        )
    return None


def build_dataset_promotion_report(
    *,
    target: PromotionTarget,
    normalized_path: Path,
    raw_base_dir: Path,
    env: str,
    symbol: str,
    stream_type: str,
    contract_mode: ContractMode = REQUIRED_PROMOTION_CONTRACT_MODE,
) -> DatasetPromotionReport:
    required_historical_feed_kind = "aggregate_trade" if stream_type == "trade" else None
    strict_contract = validate_normalized_contract(
        Path(normalized_path),
        mode=REQUIRED_PROMOTION_CONTRACT_MODE,
        required_historical_feed_kind=required_historical_feed_kind,
    )
    compat_contract = _compat_contract_for_target(
        normalized_path=Path(normalized_path),
        required_historical_feed_kind=required_historical_feed_kind,
        strict_contract=strict_contract,
        requested_contract_mode=contract_mode,
    )
    parity = build_replay_parity_report(
        raw_base_dir=Path(raw_base_dir),
        normalized_path=Path(normalized_path),
        env=env,
        symbol=symbol,
        stream_type=stream_type,
    )
    approved_trade_dataset_usages = _allowed_trade_dataset_usages(
        feed_type=strict_contract.feed_type,
        historical_feed_kind=strict_contract.historical_feed_kind,
    )
    reasons: list[str] = []
    if contract_mode != REQUIRED_PROMOTION_CONTRACT_MODE:
        reasons.append(
            f"target {target} requires contract_mode={REQUIRED_PROMOTION_CONTRACT_MODE!r}; compat is transition-only and cannot approve datasets"
        )
    if not strict_contract.pass_ok:
        if compat_contract is not None and compat_contract.pass_ok:
            reasons.append("dataset only passes the normalized contract in compat mode and cannot be approved")
        else:
            reasons.append("dataset does not satisfy the strict normalized contract required for approval")
    if not parity.pass_ok:
        reasons.append("dataset failed replay parity and cannot be approved")
    return DatasetPromotionReport(
        target=target,
        normalized_path=str(normalized_path),
        raw_base_dir=str(raw_base_dir),
        requested_contract_mode=contract_mode,
        required_contract_mode=REQUIRED_PROMOTION_CONTRACT_MODE,
        contract=strict_contract,
        compat_contract=compat_contract,
        parity=parity,
        feed_type=strict_contract.feed_type,
        historical_feed_kind=strict_contract.historical_feed_kind,
        approved_trade_dataset_usages=approved_trade_dataset_usages,
        reasons=tuple(reasons),
        generated_at=_utc_now_iso(),
        pass_ok=not reasons,
    )


def write_dataset_promotion_report(path: Path, report: DatasetPromotionReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_approved_dataset_registry(path: Path) -> ApprovedDatasetRegistry:
    if not Path(path).exists():
        return ApprovedDatasetRegistry(updated_at=_utc_now_iso(), entries=())
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    entries = tuple(
        ApprovedDatasetRegistryEntry(
            target=entry["target"],
            normalized_path=entry["normalized_path"],
            promotion_report_path=entry["promotion_report_path"],
            registered_at=entry["registered_at"],
            approved_contract_mode=entry["approved_contract_mode"],
            feed_type=entry["feed_type"],
            historical_feed_kind=entry.get("historical_feed_kind"),
            approved_trade_dataset_usages=tuple(entry.get("approved_trade_dataset_usages", ())),
        )
        for entry in payload.get("entries", ())
    )
    return ApprovedDatasetRegistry(
        updated_at=str(payload.get("updated_at") or _utc_now_iso()),
        entries=entries,
    )


def write_approved_dataset_registry(path: Path, registry: ApprovedDatasetRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(registry), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def register_approved_dataset(
    path: Path,
    *,
    report: DatasetPromotionReport,
    promotion_report_path: Path,
) -> ApprovedDatasetRegistry:
    if not report.pass_ok:
        raise DatasetPromotionApprovalError("cannot register dataset that failed promotion")
    existing = read_approved_dataset_registry(path)
    entry = ApprovedDatasetRegistryEntry(
        target=report.target,
        normalized_path=report.normalized_path,
        promotion_report_path=str(promotion_report_path),
        registered_at=_utc_now_iso(),
        approved_contract_mode=report.required_contract_mode,
        feed_type=report.feed_type,
        historical_feed_kind=report.historical_feed_kind,
        approved_trade_dataset_usages=report.approved_trade_dataset_usages,
    )
    deduped = [
        item
        for item in existing.entries
        if not (item.target == entry.target and item.normalized_path == entry.normalized_path)
    ]
    deduped.append(entry)
    registry = ApprovedDatasetRegistry(
        updated_at=_utc_now_iso(),
        entries=tuple(
            sorted(
                deduped,
                key=lambda item: (item.target, item.feed_type, item.normalized_path),
            )
        ),
    )
    write_approved_dataset_registry(path, registry)
    return registry


def assert_dataset_is_registered_as_approved(
    *,
    normalized_path: Path,
    target: PromotionTarget,
    registry_path: Path,
) -> ApprovedDatasetRegistryEntry:
    registry = read_approved_dataset_registry(registry_path)
    for entry in registry.entries:
        if entry.target == target and entry.normalized_path == str(normalized_path):
            return entry
    raise DatasetPromotionApprovalError(
        f"dataset {normalized_path} is not registered as approved for target={target}"
    )


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
    contract_mode: ContractMode = REQUIRED_PROMOTION_CONTRACT_MODE,
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
    contract_mode: ContractMode = REQUIRED_PROMOTION_CONTRACT_MODE,
) -> TradeDatasetUsageReport:
    report = build_trade_dataset_usage_report(
        normalized_path=normalized_path,
        requested_usage=requested_usage,
        contract_mode=contract_mode,
    )
    if not report.allowed:
        raise TradeDatasetUsageError("; ".join(report.reasons))
    return report
