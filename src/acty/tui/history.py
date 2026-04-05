"""Time-series data collection for TUI plotting."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from acty_core.events.snapshot import Snapshot


@dataclass
class TimePoint:
    """Single point in time with metrics."""

    ts: float
    calls_rate: float
    queue_depth: int
    inflight_workers: int
    finished: int
    failed: int
    rejected: int
    active_groups: int
    done_groups: int
    runnable_by_kind: dict[str, int] = field(default_factory=dict)


class MetricsHistory:
    """Rolling history of metrics for time-series plotting."""

    def __init__(
        self,
        *,
        max_points: int = 120,
        window_seconds: float = 120.0,
    ) -> None:
        self._max_points = max_points
        self._window_seconds = window_seconds
        self._points: Deque[TimePoint] = deque(maxlen=max_points)
        self._start_ts: float | None = None

    def record(self, ts: float, snapshot: Snapshot) -> None:
        if self._start_ts is None:
            self._start_ts = ts
        active_groups = (
            snapshot.groups_primer_ready
            + snapshot.groups_primer_inflight
            + snapshot.groups_warm_delay
            + snapshot.groups_followers_ready
        )
        done_groups = snapshot.groups_done + snapshot.groups_failed
        point = TimePoint(
            ts=ts - self._start_ts,
            calls_rate=snapshot.calls_rate,
            queue_depth=snapshot.queue_depth,
            inflight_workers=snapshot.inflight_workers,
            finished=snapshot.finished,
            failed=snapshot.failed,
            rejected=snapshot.rejected,
            active_groups=active_groups,
            done_groups=done_groups,
            runnable_by_kind=dict(snapshot.runnable_by_kind),
        )
        self._points.append(point)

    def timestamps(self) -> list[float]:
        return [p.ts for p in self._points]

    def calls_rates(self) -> list[float]:
        return [p.calls_rate for p in self._points]

    def queue_depths(self) -> list[int]:
        return [p.queue_depth for p in self._points]

    def inflight_workers_series(self) -> list[int]:
        return [p.inflight_workers for p in self._points]

    def runnable_breakdown(self) -> dict[str, list[int]]:
        all_kinds: set[str] = set()
        for p in self._points:
            all_kinds.update(p.runnable_by_kind.keys())

        result: dict[str, list[int]] = {k: [] for k in sorted(all_kinds)}
        for p in self._points:
            for kind in result:
                result[kind].append(p.runnable_by_kind.get(kind, 0))
        return result

    def __len__(self) -> int:
        return len(self._points)
