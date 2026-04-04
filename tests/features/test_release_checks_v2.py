from app.features.parity import ParityReport
from app.features.release_checks import evaluate_release_blocking


def test_release_blocking_fails_on_parity_mismatch():
    ok, reasons = evaluate_release_blocking(parity_report=ParityReport(pass_ok=False, mismatches=tuple([object()])), stale_count=0, latency_breaches=0, target="live")
    assert not ok
    assert "parity_mismatch" in reasons
