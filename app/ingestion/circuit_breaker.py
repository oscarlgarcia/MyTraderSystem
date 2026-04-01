from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Literal


CircuitBreakerState = Literal["closed", "open", "half-open"]


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    reset_timeout_seconds: float = 30.0
    monotonic_fn: Callable[[], float] = time.monotonic
    state: CircuitBreakerState = "closed"
    failure_count: int = 0
    opened_at: float | None = None
    _half_open_probe_active: bool = field(default=False, init=False, repr=False)

    def allow_request(self) -> bool:
        now = self.monotonic_fn()
        if self.state == "closed":
            return True
        if self.state == "open":
            if self.opened_at is None:
                self.opened_at = now
                return False
            if (now - self.opened_at) < self.reset_timeout_seconds:
                return False
            self.state = "half-open"
            self._half_open_probe_active = True
            return True
        if self.state == "half-open":
            if self._half_open_probe_active:
                return False
            self._half_open_probe_active = True
            return True
        return False

    def record_success(self) -> None:
        self.state = "closed"
        self.failure_count = 0
        self.opened_at = None
        self._half_open_probe_active = False

    def record_failure(self) -> None:
        self.failure_count += 1
        self._half_open_probe_active = False
        if self.state == "half-open" or self.failure_count >= self.failure_threshold:
            self.state = "open"
            self.opened_at = self.monotonic_fn()
