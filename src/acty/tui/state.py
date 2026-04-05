"""State holder for the Textual UI (group-only)."""

from __future__ import annotations

from collections import deque
from typing import Deque, Iterable

from acty_core.events.collector import RuntimeStatsCollector
from acty_core.events.sinks.base import EventSink
from acty_core.events.snapshot import Snapshot
from acty_core.events.types import Event

from acty.tui.formatting import format_event


class TuiState(EventSink):
    def __init__(
        self,
        *,
        stats_collector: RuntimeStatsCollector | None = None,
        extra_sinks: Iterable[EventSink] | None = None,
        max_logs: int = 1000,
    ) -> None:
        self._stats_collector = stats_collector or RuntimeStatsCollector()
        self._extra_sinks = list(extra_sinks or [])
        self._max_logs = max_logs
        self._logs: Deque[str] = deque(maxlen=max_logs)

    async def handle(self, event: Event) -> None:
        for sink in self._extra_sinks:
            await sink.handle(event)
        await self._stats_collector.handle(event)
        self._logs.append(format_event(event))

    def snapshot(self) -> Snapshot:
        return self._stats_collector.snapshot()

    def log_lines(self) -> list[str]:
        return list(self._logs)

    @property
    def max_logs(self) -> int:
        return self._max_logs
