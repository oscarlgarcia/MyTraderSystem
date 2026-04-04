"""In-memory Feature Cache con ?ndice temporal por s?mbolo."""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict
from typing import Dict, List, Optional

from app.common.dto import FeatureVector


class FeatureCache:
    def __init__(self, capacity_per_symbol: int = 1000) -> None:
        self.capacity = capacity_per_symbol
        self.data: Dict[str, OrderedDict[float, FeatureVector]] = {}

    def put(self, fv: FeatureVector) -> None:
        sym = fv.symbol
        ts = fv.ts.timestamp()
        od = self.data.setdefault(sym, OrderedDict())
        if not od:
            od[ts] = fv
        else:
            last_key = next(reversed(od))
            if ts >= last_key:
                od[ts] = fv
            else:
                od[ts] = fv
                self.data[sym] = OrderedDict(sorted(od.items(), key=lambda x: x[0]))
                od = self.data[sym]
        if len(od) > self.capacity:
            od.popitem(last=False)

    def get_latest(self, symbol: str) -> Optional[FeatureVector]:
        od = self.data.get(symbol)
        if not od:
            return None
        return next(reversed(od.values()))

    def get_at(self, symbol: str, ts, tolerance: float | None = None) -> Optional[FeatureVector]:
        od = self.data.get(symbol)
        if not od:
            return None
        times: List[float] = list(od.keys())
        target = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)
        idx = bisect_right(times, target) - 1
        if idx < 0:
            return None
        ts_found = times[idx]
        if tolerance is not None and abs(ts_found - target) > tolerance:
            return None
        return od[times[idx]]
