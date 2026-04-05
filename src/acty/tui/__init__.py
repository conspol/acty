"""Textual UI helpers."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from acty.tui.app import RuntimeTuiApp
from acty.tui.sources import EventSource, QueueEventSource
from acty.tui.state import TuiState

if TYPE_CHECKING:
    from acty_core.events.types import Event

TuiVersion = Literal["v2"]


def create_tui_app(
    event_source: EventSource | asyncio.Queue[Event],
    *,
    version: TuiVersion = "v2",
    state: TuiState | None = None,
    queue_maxsize: int = 0,
    # Single public API: provide a queue to enable console capture + console pane.
    # This keeps the surface area small and allows external injection/tests.
    console_log_queue: asyncio.Queue[str] | None = None,
    console_tee: bool = False,
    **kwargs,
) -> RuntimeTuiApp:
    if version != "v2":
        raise ValueError("Only v2 TUI is supported in acty")
    source: EventSource
    if isinstance(event_source, asyncio.Queue):
        source = QueueEventSource(event_source)
    else:
        source = event_source
    return RuntimeTuiApp(
        source,
        state=state,
        queue_maxsize=queue_maxsize,
        console_log_queue=console_log_queue,
        console_tee=console_tee,
        **kwargs,
    )


__all__ = [
    "RuntimeTuiApp",
    "TuiState",
    "TuiVersion",
    "create_tui_app",
]
