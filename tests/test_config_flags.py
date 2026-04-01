from app.config import parse_args


def test_defaults_flags():
    args = parse_args([])
    assert args.mode == "dry"
    assert args.production_mode is False
    assert args.trace_steps is False
    assert not hasattr(args, "features_after_ingest")
    assert args.ingest_max_buffer == 10_000
    assert args.ingest_batch_size == 1
    assert args.ingest_lag_warn is None
    assert args.ingest_buffer_warn is None
    assert args.ingest_backpressure_policy == "pause"
    assert args.ingest_temporal_policy == "accept"
    assert args.ingest_dedup is True
    assert args.allow_live_fallback is False
    assert args.error_policy is None
    assert args.ingest_stream_types == ("kline",)
def test_trace_steps_flag():
    args = parse_args(["--trace-steps"])
    assert args.trace_steps is True


def test_ingest_batch_size_flag():
    args = parse_args(["--ingest-batch-size", "4"])
    assert args.ingest_batch_size == 4


def test_fast_path_flag():
    args = parse_args(["--fast-path"])
    assert args.fast_path is True


def test_ingest_warn_flags():
    args = parse_args(["--ingest-lag-warn", "1.5", "--ingest-buffer-warn", "3", "--ingest-backpressure-policy", "drop_newest", "--ingest-temporal-policy", "drop"])
    assert args.ingest_lag_warn == 1.5
    assert args.ingest_buffer_warn == 3
    assert args.ingest_backpressure_policy == "drop_newest"
    assert args.ingest_temporal_policy == "drop"


def test_allow_live_fallback_flag():
    args = parse_args(["--allow-live-fallback"])
    assert args.allow_live_fallback is True


def test_error_policy_flag():
    args = parse_args(["--error-policy", "degraded"])
    assert args.error_policy == "degraded"


def test_production_mode_flag():
    args = parse_args(["--production-mode"])
    assert args.production_mode is True


def test_shadow_mode_flags():
    args = parse_args(["--ingest-pipeline-version", "v1", "--ingest-shadow-mode", "--ingest-shadow-block-on-diff"])
    assert args.ingest_pipeline_version == "v1"
    assert args.ingest_shadow_mode is True
    assert args.ingest_shadow_block_on_diff is True


def test_ingest_stream_types_flag():
    args = parse_args(["--ingest-stream-types", "kline"])
    assert args.ingest_stream_types == ("kline",)
