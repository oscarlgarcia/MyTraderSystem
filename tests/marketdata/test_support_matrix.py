import pytest

from app.marketdata.support_matrix import (
    FEED_SUPPORT_MATRIX,
    feed_support,
    live_supported_feed_types,
    normalize_feed_types,
    paper_supported_feed_types,
    validate_paper_feed_support,
    validate_live_feed_support,
)


def test_feed_support_matrix_reflects_current_live_scope():
    assert FEED_SUPPORT_MATRIX["trade"].supports_paper is True
    assert FEED_SUPPORT_MATRIX["trade"].paper_validation_basis == "replay_validated"
    assert FEED_SUPPORT_MATRIX["trade"].supports_live is False
    assert FEED_SUPPORT_MATRIX["trade"].recovery_capability == "none"
    assert FEED_SUPPORT_MATRIX["trade"].supports_exact_recovery is False
    assert FEED_SUPPORT_MATRIX["trade"].supports_exact_verified_recovery is False
    assert FEED_SUPPORT_MATRIX["kline"].supports_paper is True
    assert FEED_SUPPORT_MATRIX["kline"].paper_validation_basis == "runtime_validated"
    assert FEED_SUPPORT_MATRIX["kline"].supports_live is True
    assert FEED_SUPPORT_MATRIX["kline"].recovery_capability == "exact_verified"
    assert FEED_SUPPORT_MATRIX["kline"].supports_exact_recovery is True
    assert FEED_SUPPORT_MATRIX["kline"].supports_exact_verified_recovery is True
    assert FEED_SUPPORT_MATRIX["book"].supports_paper is False
    assert FEED_SUPPORT_MATRIX["book"].supports_live is False


def test_normalize_feed_types_validates_and_normalizes():
    assert normalize_feed_types(["Trade", "kline"]) == ("trade", "kline")
    assert normalize_feed_types(None) == ("kline",)

    with pytest.raises(ValueError, match="unsupported ingest stream types"):
        normalize_feed_types(["foo"])


def test_feed_support_rejects_unknown_feed():
    with pytest.raises(ValueError, match="unsupported ingest stream type"):
        feed_support("foo")


def test_validate_paper_feed_support_matches_declared_scope():
    assert validate_paper_feed_support(("trade",)) == ("trade",)
    assert validate_paper_feed_support(("kline",)) == ("kline",)
    assert paper_supported_feed_types() == ("trade", "kline")
    assert live_supported_feed_types() == ("kline",)

    with pytest.raises(ValueError, match="book does not support paper ingestion"):
        validate_paper_feed_support(("book",))


def test_validate_live_feed_support_requires_exact_recovery_and_handoff():
    assert validate_live_feed_support(("kline",), require_exact_recovery=False, require_handoff=True) == ("kline",)
    assert validate_live_feed_support(("kline",), require_exact_recovery=True, require_handoff=True) == ("kline",)
    assert validate_live_feed_support(
        ("kline",),
        require_exact_recovery=True,
        require_exact_verified=True,
        require_handoff=True,
    ) == ("kline",)

    with pytest.raises(ValueError, match="trade does not support live ingestion"):
        validate_live_feed_support(("trade",), require_exact_recovery=True, require_handoff=True)

    with pytest.raises(ValueError, match="book does not support live ingestion"):
        validate_live_feed_support(("book",), require_exact_recovery=False, require_handoff=False)
