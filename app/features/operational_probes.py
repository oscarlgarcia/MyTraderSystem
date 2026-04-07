from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Callable


@dataclass(frozen=True)
class FeatureServingProbeReport:
    generated_at: str
    mode: str
    total_requests: int
    ok_count: int
    degrade_count: int
    fail_count: int
    unknown_count: int
    duration_seconds: float
    max_latency_seconds: float
    p95_latency_seconds: float
    pass_ok: bool


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[index]


def run_serving_soak_probe(
    *,
    request_fn: Callable[[], object],
    iterations: int = 100,
    max_latency_seconds: float = 1.0,
    max_failures: int = 0,
    max_unknown: int = 0,
) -> FeatureServingProbeReport:
    latencies: list[float] = []
    ok_count = degrade_count = fail_count = unknown_count = 0
    start = time.perf_counter()
    for _ in range(iterations):
        probe_start = time.perf_counter()
        result = request_fn()
        latencies.append(time.perf_counter() - probe_start)
        status = getattr(result, "status", "unknown")
        if status == "ok":
            ok_count += 1
        elif status == "degrade":
            degrade_count += 1
        elif status == "fail":
            fail_count += 1
        else:
            unknown_count += 1
    duration = time.perf_counter() - start
    pass_ok = fail_count <= max_failures and unknown_count <= max_unknown and max(latencies or [0.0]) <= max_latency_seconds
    return FeatureServingProbeReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        mode="soak",
        total_requests=iterations,
        ok_count=ok_count,
        degrade_count=degrade_count,
        fail_count=fail_count,
        unknown_count=unknown_count,
        duration_seconds=duration,
        max_latency_seconds=max(latencies or [0.0]),
        p95_latency_seconds=_percentile(latencies, 0.95),
        pass_ok=pass_ok,
    )


def run_serving_concurrency_probe(
    *,
    request_fn: Callable[[], object],
    writer_fn: Callable[[int], None] | None = None,
    rounds: int = 10,
    readers_per_round: int = 12,
    max_workers: int = 12,
    max_latency_seconds: float = 1.0,
    max_failures: int = 0,
    max_unknown: int = 0,
) -> FeatureServingProbeReport:
    latencies: list[float] = []
    ok_count = degrade_count = fail_count = unknown_count = 0
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for round_id in range(rounds):
            if writer_fn is not None:
                for writer_id in range(max(1, max_workers // 4)):
                    executor.submit(writer_fn, round_id * 100 + writer_id)

            def _reader() -> str:
                probe_start = time.perf_counter()
                result = request_fn()
                latencies.append(time.perf_counter() - probe_start)
                return getattr(result, "status", "unknown")

            for status in executor.map(lambda _: _reader(), range(readers_per_round)):
                if status == "ok":
                    ok_count += 1
                elif status == "degrade":
                    degrade_count += 1
                elif status == "fail":
                    fail_count += 1
                else:
                    unknown_count += 1
    duration = time.perf_counter() - start
    pass_ok = fail_count <= max_failures and unknown_count <= max_unknown and max(latencies or [0.0]) <= max_latency_seconds
    return FeatureServingProbeReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        mode="concurrency",
        total_requests=rounds * readers_per_round,
        ok_count=ok_count,
        degrade_count=degrade_count,
        fail_count=fail_count,
        unknown_count=unknown_count,
        duration_seconds=duration,
        max_latency_seconds=max(latencies or [0.0]),
        p95_latency_seconds=_percentile(latencies, 0.95),
        pass_ok=pass_ok,
    )


def write_probe_report(path: str | Path, report: FeatureServingProbeReport) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target

