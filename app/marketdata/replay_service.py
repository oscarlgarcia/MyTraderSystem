from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from app.marketdata.replay import ReplaySource


ReplaySpeedMode = Literal["full-speed", "step-by-step"]


@dataclass(frozen=True, slots=True)
class ReplayServiceRequest:
    base_dir: Path
    env: str
    stream_type: str
    symbol: str | None = None
    venue: str | None = None
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    speed: ReplaySpeedMode = "full-speed"
    step_seconds: float = 0.0
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayServiceReport:
    env: str
    stream_type: str
    symbol: str | None
    venue: str | None
    replayed_events: int
    first_exchange_ts: str | None
    last_exchange_ts: str | None
    speed: ReplaySpeedMode


def replay_events(request: ReplayServiceRequest):
    source = ReplaySource(
        base_dir=Path(request.base_dir),
        env=request.env,
        venue=request.venue,
        stream_types=(request.stream_type,),
        symbol=request.symbol,
        start_ts=request.start_ts,
        end_ts=request.end_ts,
        speed=request.speed,
        step_seconds=request.step_seconds,
    )
    count = 0
    for event in source.stream():
        yield event
        count += 1
        if request.limit is not None and count >= request.limit:
            break


def build_replay_service_report(request: ReplayServiceRequest) -> ReplayServiceReport:
    replayed = list(replay_events(request))
    first = replayed[0].exchange_ts.isoformat() if replayed else None
    last = replayed[-1].exchange_ts.isoformat() if replayed else None
    return ReplayServiceReport(
        env=request.env,
        stream_type=request.stream_type,
        symbol=request.symbol,
        venue=request.venue,
        replayed_events=len(replayed),
        first_exchange_ts=first,
        last_exchange_ts=last,
        speed=request.speed,
    )


def replay_service_report_payload(request: ReplayServiceRequest) -> dict:
    return asdict(build_replay_service_report(request))
