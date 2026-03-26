"""
Entry point for the trading system (stub).

The goal of this module in Fase 1.1 is to verify that the toolchain
boots correctly. No domain logic or external IO is performed here.
"""

from __future__ import annotations

# Import packages to ensure availability and detect missing modules early.
# No side effects or heavy logic should be triggered here.
from app import common, execution, features, ingestion, observability, ops, portfolio, risk, strategy  # noqa: F401
from app.config import load_config, parse_args
from app.observability.logger import get_logger, set_trace_id

from uuid import uuid4
from typing import List


def _stub_ingest(recorder: List[str]) -> None:
    recorder.append("ingestion")


def _stub_features(recorder: List[str]) -> None:
    recorder.append("features")


def _stub_strategy(recorder: List[str]) -> None:
    recorder.append("strategy")


def _stub_risk(recorder: List[str]) -> None:
    recorder.append("risk")


def _stub_execution(recorder: List[str]) -> None:
    recorder.append("execution")


def _stub_portfolio(recorder: List[str]) -> None:
    recorder.append("portfolio")


def run_cycle(recorder: List[str]) -> None:
    """Execute a minimal, no-IO pipeline sequence."""
    _stub_ingest(recorder)
    _stub_features(recorder)
    _stub_strategy(recorder)
    _stub_risk(recorder)
    _stub_execution(recorder)
    _stub_portfolio(recorder)


def run() -> int:
    """Basic bootstrap stub returning zero for success."""
    args = parse_args()
    config = load_config(args.env)
    trace_id = str(uuid4())
    set_trace_id(trace_id)
    logger = get_logger(level=config.log_level)
    # Demonstrate DTO usage in a minimal, non-I/O way.
    _ = common.TraceContext(trace_id=trace_id)

    recorder: List[str] = []
    run_cycle(recorder)

    logger.info(
        "pipeline stub ok",
        extra={"env": config.env, "data_dir": str(config.data_dir), "trace_id": trace_id, "steps": recorder},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
