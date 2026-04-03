from app.ops.observability_contract import build_observability_contract_report


def test_observability_contract_exposes_required_bundle_for_paper():
    report = build_observability_contract_report(target="paper")

    assert report.pass_ok is True
    assert "exchange_receive_skew_seconds" in report.required_metrics
    assert "receive_process_skew_seconds" in report.required_metrics
    assert "invalid_timestamp_total" in report.required_metrics
    assert "exchange_receive_skew_high" in report.required_alerts
    assert "receive_process_skew_high" in report.required_alerts
    assert "invalid_timestamp_detected" in report.required_alerts
    assert report.required_metric_thresholds["exchange_receive_skew_seconds"]["warning"] == 5.0
    assert report.required_metric_thresholds["exchange_receive_skew_seconds"]["critical"] == 30.0
    assert report.required_metric_thresholds["receive_process_skew_seconds"]["warning"] == 1.0
    assert report.required_metric_thresholds["invalid_timestamp_total"]["critical"] == 1.0


def test_observability_contract_tightens_temporal_thresholds_for_live():
    report = build_observability_contract_report(target="live")

    assert report.pass_ok is True
    assert report.target == "live"
    assert report.required_metric_thresholds["exchange_receive_skew_seconds"]["warning"] == 2.0
    assert report.required_metric_thresholds["exchange_receive_skew_seconds"]["critical"] == 10.0
    assert report.required_metric_thresholds["receive_process_skew_seconds"]["warning"] == 0.5
    assert report.required_metric_thresholds["receive_process_skew_seconds"]["critical"] == 2.0
