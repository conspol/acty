"""Replay wrapper for EventSource streams."""

from __future__ import annotations

import asyncio
import contextlib
from typing import AsyncIterator

from acty_core.events.types import Event

from acty.tui.sources.base import EventSource


class ReplayEventSource(EventSource):
    def __init__(self, source: EventSource, *, speed: float = 1.0) -> None:
        self._source = source
        self._speed = speed
        self._paused = False
        self._pause_changed = asyncio.Event()

    def set_paused(self, paused: bool) -> None:
        if self._paused == paused:
            return
        self._paused = paused
        self._pause_changed.set()

    async def stream(self) -> AsyncIterator[Event]:
        last_ts: float | None = None
        async for event in self._source.stream():
            if last_ts is not None and self._speed > 0:
                delta = event.ts - last_ts
                if delta > 0:
                    await self._sleep_with_pause(delta / self._speed)
            if last_ts is None or event.ts > last_ts:
                last_ts = event.ts
            await self._wait_for_resume()
            yield event

    async def _wait_for_resume(self) -> None:
        while self._paused:
            self._pause_changed.clear()
            await self._pause_changed.wait()

    async def _sleep_with_pause(self, delay_s: float) -> None:
        remaining = delay_s
        loop = asyncio.get_running_loop()
        while remaining > 0:
            await self._wait_for_resume()
            start = loop.time()
            self._pause_changed.clear()
            sleep_task = asyncio.create_task(asyncio.sleep(remaining))
            pause_task = asyncio.create_task(self._pause_changed.wait())
            done, pending = await asyncio.wait(
                {sleep_task, pause_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            elapsed = loop.time() - start
            if pause_task in done:
                remaining = max(0.0, remaining - elapsed)
                continue
            return
