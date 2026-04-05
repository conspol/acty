"""EventSource protocols for the TUI."""

from __future__ import annotations

from typing import AsyncIterator, Protocol

from acty_core.events.types import Event


class EventSource(Protocol):
    async def stream(self) -> AsyncIterator[Event]: ...
