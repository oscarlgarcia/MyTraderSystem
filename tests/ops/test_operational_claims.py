from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from app import main
from app.config import load_config
from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX


# Release-blocking registry: any feed promoted to exact recovery must point to
# explicit tests that prove the claim.
EXACT_RECOVERY_CLAIM_TESTS: dict[str, tuple[tuple[str, str], ...]] = {
    "kline": (
        (
            "tests.marketdata.recovery.test_recovery_guarantees",
            "test_exact_kline_recovery_uses_open_time_window_without_gap_or_double_count",
        ),
        (
            "tests.marketdata.recovery.test_recovery_guarantees",
            "test_exact_kline_recovery_marks_gap_irreparable_when_snapshot_window_is_incomplete",
        ),
    ),
}
EXACT_VERIFIED_RECOVERY_CLAIM_TESTS: dict[str, tuple[tuple[str, str], ...]] = {}


def _production_runtime(feed_type: str) -> dict[str, object]:
    return {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": (feed_type,),
    }


def _production_cfg(tmp_path: Path):
    cfg = load_config("dev")
    return type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )


def test_exact_recovery_claims_require_explicit_test_registry():
    claimed = sorted(
        feed_type
        for feed_type, support in FEED_SUPPORT_MATRIX.items()
        if support.supports_exact_recovery
    )
    registered = sorted(EXACT_RECOVERY_CLAIM_TESTS)
    missing = [feed_type for feed_type in claimed if feed_type not in EXACT_RECOVERY_CLAIM_TESTS]
    stale = [feed_type for feed_type in registered if feed_type not in claimed]

    assert not missing, (
        "feeds claiming exact recovery must be registered in "
        "EXACT_RECOVERY_CLAIM_TESTS with explicit proving tests: "
        f"{missing}"
    )
    assert not stale, (
        "EXACT_RECOVERY_CLAIM_TESTS contains feeds that no longer claim exact "
        f"recovery: {stale}"
    )


def test_exact_recovery_registry_points_to_real_tests():
    for feed_type, test_refs in EXACT_RECOVERY_CLAIM_TESTS.items():
        assert FEED_SUPPORT_MATRIX[feed_type].supports_exact_recovery is True
        assert test_refs, f"{feed_type} exact recovery claim needs at least one proving test"
        for module_name, test_name in test_refs:
            module = importlib.import_module(module_name)
            assert hasattr(module, test_name), f"missing proving test {module_name}.{test_name}"


def test_exact_verified_recovery_claims_require_explicit_test_registry():
    claimed = sorted(
        feed_type
        for feed_type, support in FEED_SUPPORT_MATRIX.items()
        if support.supports_exact_verified_recovery
    )
    registered = sorted(EXACT_VERIFIED_RECOVERY_CLAIM_TESTS)
    missing = [feed_type for feed_type in claimed if feed_type not in EXACT_VERIFIED_RECOVERY_CLAIM_TESTS]
    stale = [feed_type for feed_type in registered if feed_type not in claimed]

    assert not missing, (
        "feeds claiming exact_verified recovery must be registered in "
        "EXACT_VERIFIED_RECOVERY_CLAIM_TESTS with explicit proving tests: "
        f"{missing}"
    )
    assert not stale, (
        "EXACT_VERIFIED_RECOVERY_CLAIM_TESTS contains feeds that no longer "
        f"claim exact_verified recovery: {stale}"
    )


def test_exact_verified_recovery_registry_points_to_real_tests():
    for feed_type, test_refs in EXACT_VERIFIED_RECOVERY_CLAIM_TESTS.items():
        assert FEED_SUPPORT_MATRIX[feed_type].supports_exact_verified_recovery is True
        assert test_refs, f"{feed_type} exact_verified recovery claim needs at least one proving test"
        for module_name, test_name in test_refs:
            module = importlib.import_module(module_name)
            assert hasattr(module, test_name), f"missing proving test {module_name}.{test_name}"


@pytest.mark.parametrize("feed_type,support", FEED_SUPPORT_MATRIX.items(), ids=sorted(FEED_SUPPORT_MATRIX))
def test_production_mode_rejects_any_feed_without_full_live_claims(tmp_path: Path, feed_type: str, support) -> None:
    cfg = _production_cfg(tmp_path)
    runtime = _production_runtime(feed_type)

    if support.supports_live and support.supports_exact_verified_recovery and support.supports_handoff:
        main._validate_operational_security(cfg, mode="live", runtime=runtime)
        return

    with pytest.raises(ValueError):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)
