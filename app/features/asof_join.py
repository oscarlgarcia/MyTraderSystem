from __future__ import annotations

from bisect import bisect_right
from typing import Iterable, Sequence, Tuple, TypeVar, Callable, List

T = TypeVar("T")


def asof_pick(records: Sequence[T], *, target_ts, ts_getter: Callable[[T], object], allow_exact: bool = True):
    if not records:
        return None
    keys = [ts_getter(r) for r in records]
    target = target_ts
    idx = bisect_right(keys, target) - (0 if allow_exact else 1) - 1
    if idx < 0:
        return None
    return records[idx]


def asof_join(left: Iterable[T], right: Sequence[T], *, left_ts_getter, right_ts_getter, predicate=None) -> List[Tuple[T, T | None]]:
    out: List[Tuple[T, T | None]] = []
    filtered_right = list(right)
    for item in left:
        if predicate:
            matches = [r for r in filtered_right if predicate(item, r)]
        else:
            matches = filtered_right
        out.append((item, asof_pick(matches, target_ts=left_ts_getter(item), ts_getter=right_ts_getter)))
    return out
