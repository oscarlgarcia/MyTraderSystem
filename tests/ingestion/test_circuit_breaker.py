from app.ingestion.circuit_breaker import CircuitBreaker


def test_circuit_breaker_opens_after_failure_threshold():
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=30.0, monotonic_fn=lambda: 0.0)

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == "closed"
    assert breaker.failure_count == 1

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == "open"
    assert breaker.failure_count == 2


def test_circuit_breaker_transitions_to_half_open_after_timeout_and_closes_on_success():
    now = {"value": 0.0}
    breaker = CircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=10.0,
        monotonic_fn=lambda: now["value"],
    )

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == "open"

    now["value"] = 5.0
    assert breaker.allow_request() is False

    now["value"] = 11.0
    assert breaker.allow_request() is True
    assert breaker.state == "half-open"

    breaker.record_success()
    assert breaker.state == "closed"
    assert breaker.failure_count == 0


def test_circuit_breaker_reopens_after_half_open_failure():
    now = {"value": 0.0}
    breaker = CircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=10.0,
        monotonic_fn=lambda: now["value"],
    )

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == "open"

    now["value"] = 11.0
    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == "open"
