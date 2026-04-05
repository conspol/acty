"""Queue-backed EventSource."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

from acty_core.events.types import Event

from acty.tui.sources.base import EventSource


class QueueEventSource(EventSource):
    def __init__(self, queue: asyncio.Queue[Event | None], *, stop_on_none: bool = True) -> None:
        self._queue = queue
        self._stop_on_none = stop_on_none

    async def stream(self) -> AsyncIterator[Event]:
        while True:
            item = await self._queue.get()
            if item is None and self._stop_on_none:
                return
            if item is None:
                continue
            yield item
