from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from app.ops.normalized_contract import NormalizedContractReport, validate_normalized_contract
from app.ops.replay_parity import ReplayParityReport, build_replay_parity_report


PromotionTarget = Literal["backtesting", "paper"]


@dataclass(frozen=True, slots=True)
class DatasetPromotionReport:
    target: PromotionTarget
    normalized_path: str
    raw_base_dir: str
    contract: NormalizedContractReport
    parity: ReplayParityReport
    pass_ok: bool


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
    return DatasetPromotionReport(
        target=target,
        normalized_path=str(normalized_path),
        raw_base_dir=str(raw_base_dir),
        contract=contract,
        parity=parity,
        pass_ok=bool(contract.pass_ok and parity.pass_ok),
    )


def write_dataset_promotion_report(path: Path, report: DatasetPromotionReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path
