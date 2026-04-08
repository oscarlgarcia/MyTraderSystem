from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from app import main
from app.config import DEFAULT_INGEST_STREAM_TYPES, load_config, parse_args
from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX


# Release-blocking registry: any feed promoted to exact recovery must point to
# explicit tests that prove the claim.
EXACT_RECOVERY_CLAIM_TESTS: dict[str, tuple[tuple[str, str], ...]] = {
    "trade": (
        (
            "tests.marketdata.recovery.test_recovery_guarantees",
            "test_exact_trade_recovery_fills_gap_without_duplicate_delivery",
        ),
        (
            "tests.marketdata.recovery.test_recovery_guarantees",
            "test_verify_recovery_window_rejects_missing_and_unexpected_trade_rows",
        ),
    ),
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
EXACT_VERIFIED_RECOVERY_CLAIM_TESTS: dict[str, tuple[tuple[str, str], ...]] = {
    "trade": (
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_trade_recovery_handles_controlled_cuts",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_trade_recovery_tolerates_duplicates_during_catchup",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_trade_recovery_rejects_partial_snapshot_window",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_trade_recovery_falls_back_when_cursor_state_is_mismatched",
        ),
        (
            "tests.ingestion.test_exact_recovery_runtime",
            "test_trade_reconnect_old_resend_is_deduplicated_after_exact_recovery",
        ),
    ),
    "kline": (
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_kline_recovery_handles_controlled_cuts",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_kline_recovery_tolerates_duplicates_during_catchup",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_kline_recovery_rejects_partial_snapshot_window",
        ),
        (
            "tests.marketdata.recovery.test_exact_recovery_suite",
            "test_exact_verified_kline_recovery_falls_back_when_cursor_state_is_mismatched",
        ),
        (
            "tests.ingestion.test_exact_recovery_runtime",
            "test_reconnect_old_resend_is_deduplicated_after_exact_recovery",
        ),
    ),
}

LIVE_SCOPE_DOC_PATHS: tuple[Path, ...] = (
    Path("docs/definition.md"),
    Path("docs/tech_spec.md"),
    Path("docs/ingestion.md"),
    Path("docs/operations/ingestion_promotion_runbook.md"),
    Path("docs/operations/ingestion_runbook.md"),
)


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
        env="prod",
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


def test_live_scope_claim_is_kline_only_across_support_matrix_and_cli_defaults() -> None:
    live_supported = sorted(feed_type for feed_type, support in FEED_SUPPORT_MATRIX.items() if support.supports_live)
    assert live_supported == ["kline", "trade"]
    assert FEED_SUPPORT_MATRIX["kline"].supports_exact_verified_recovery is True
    assert FEED_SUPPORT_MATRIX["trade"].supports_live is True
    assert FEED_SUPPORT_MATRIX["trade"].supports_exact_verified_recovery is True
    assert FEED_SUPPORT_MATRIX["book"].supports_live is False
    assert DEFAULT_INGEST_STREAM_TYPES == ("kline",)
    assert parse_args([]).ingest_stream_types == ("kline",)


def test_paper_scope_claim_supports_trade_and_kline_but_rejects_book() -> None:
    paper_supported = sorted(feed_type for feed_type, support in FEED_SUPPORT_MATRIX.items() if support.supports_paper)
    assert paper_supported == ["kline", "trade"]
    assert FEED_SUPPORT_MATRIX["trade"].paper_validation_basis == "replay_validated"
    assert FEED_SUPPORT_MATRIX["kline"].paper_validation_basis == "runtime_validated"
    assert FEED_SUPPORT_MATRIX["book"].supports_paper is False


def test_live_scope_docs_do_not_advertise_trade_or_book_as_supported_live_runtime() -> None:
    required_markers = (
        "`trade` + `kline`",
        "`trade`",
        "`book`",
    )
    forbidden_markers = (
        'stream_types=("trade", "foo")',
        "supports_exact_recovery=False",
        "`kline`-only",
    )

    for path in LIVE_SCOPE_DOC_PATHS:
        content = path.read_text(encoding="utf-8")
        assert "`kline`" in content, f"{path} must mention the supported live feed explicitly"
        for marker in required_markers:
            assert marker in content, f"{path} must document live scope markers: missing {marker}"
        for marker in forbidden_markers:
            assert marker not in content, f"{path} still advertises stale live scope wording: {marker}"


def test_scope_docs_explain_trade_paper_and_book_exclusion() -> None:
    required_markers = (
        "paper",
        "`trade`",
        "replay",
        "live",
        "exact recovery",
        "`book`",
    )
    for path in LIVE_SCOPE_DOC_PATHS:
        content = path.read_text(encoding="utf-8").lower()
        for marker in required_markers:
            assert marker in content, f"{path} must document paper/live feed scope markers: missing {marker}"


@pytest.mark.parametrize("feed_type,support", FEED_SUPPORT_MATRIX.items(), ids=sorted(FEED_SUPPORT_MATRIX))
def test_production_mode_rejects_any_feed_without_full_live_claims(tmp_path: Path, feed_type: str, support) -> None:
    cfg = _production_cfg(tmp_path)
    runtime = _production_runtime(feed_type)

    if support.supports_live and support.supports_exact_verified_recovery and support.supports_handoff:
        metadata_path = tmp_path / "metadata" / "instruments" / "env=prod" / "venue=BINANCE" / "latest.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            '{"metadata_snapshot_mode":"runtime","drift":{"material":false}}',
            encoding="utf-8",
        )
        main._validate_operational_security(cfg, mode="live", runtime=runtime)
        return

    with pytest.raises(ValueError):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)
