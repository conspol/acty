"""Small runtime helpers for wiring Acty TUI in scripts."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any, Callable

from acty.tui import TuiState, create_tui_app
from acty_core.events import EventQueueSink
from acty_core.events.sinks.base import EventSink
from acty_core.events.types import Event


@dataclass
class ActyTuiSession:
    app: Any | None
    task: asyncio.Task[None] | None
    mount_event: asyncio.Event | None
    event_queue: asyncio.Queue[Event] | None
    console_log_queue: asyncio.Queue[str] | None


async def setup_acty_tui(
    *,
    enabled: bool,
    event_sinks: list[EventSink],
    event_queue: asyncio.Queue[Event] | None = None,
    console_log_queue: asyncio.Queue[str] | None = None,
    event_queue_size: int = 0,
    console_queue_size: int = 0,
    console_tee: bool = False,
    startup_trace: bool = False,
    startup_log: Callable[[str], None] | None = None,
    log: Callable[[str], None] | None = None,
) -> ActyTuiSession:
    """Attach an in-process TUI and return its session details.

    - No env var handling here: all flags/queues are provided by the caller.
    - `event_sinks` is mutated in-place to include a queue sink.
    """
    if not enabled:
        return ActyTuiSession(
            app=None,
            task=None,
            mount_event=None,
            event_queue=None,
            console_log_queue=None,
        )

    if startup_log is not None:
        startup_log("before create_tui_app")

    mount_event = asyncio.Event() if startup_trace else None
    if event_queue is None:
        event_queue = asyncio.Queue(maxsize=event_queue_size)
    if console_log_queue is None:
        console_log_queue = asyncio.Queue(maxsize=console_queue_size)

    event_sinks.append(EventQueueSink(event_queue))
    tui_app = create_tui_app(
        event_queue,
        state=TuiState(),
        console_log_queue=console_log_queue,
        console_tee=console_tee,
        queue_maxsize=event_queue_size,
    )

    if startup_trace:
        orig_on_mount = tui_app.__class__.on_mount

        async def _timed_on_mount(self) -> None:
            if startup_log is not None:
                startup_log("tui on_mount start")
            await orig_on_mount(self)
            if startup_log is not None:
                startup_log("tui on_mount end")
            if mount_event is not None and not mount_event.is_set():
                mount_event.set()

        tui_app.__class__.on_mount = _timed_on_mount  # type: ignore[assignment]

    if startup_log is not None:
        startup_log("after create_tui_app")

    if log is None:
        log = print
    log("Acty TUI enabled: stdout/stderr will be captured into the TUI.")

    tui_task = asyncio.create_task(tui_app.run_async())

    if startup_log is not None:
        startup_log("tui_task scheduled")

    if startup_trace:
        await asyncio.sleep(0)
        if startup_log is not None:
            startup_log("after first loop tick")

    return ActyTuiSession(
        app=tui_app,
        task=tui_task,
        mount_event=mount_event,
        event_queue=event_queue,
        console_log_queue=console_log_queue,
    )


async def shutdown_acty_tui(session: ActyTuiSession) -> None:
    if session.app is None:
        return
    session.app.exit()
    if session.task is not None:
        with contextlib.suppress(Exception):
            await session.task
