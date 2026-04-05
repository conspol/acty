"""JSONL file-backed EventSource with follow support."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, AsyncIterator

from acty_core.events.types import Event

from acty.tui.sources.base import EventSource


def _coerce_ts(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _select_ts(data: dict[str, Any]) -> float:
    for key in ("ts", "timestamp", "time"):
        if key in data:
            ts = _coerce_ts(data.get(key))
            if ts is not None:
                return ts
    return time.time()


def _parse_event(data: dict[str, Any]) -> Event:
    return Event(
        schema_version=data.get("schema_version", 1),
        run_id=data.get("run_id", ""),
        seq=data.get("seq", 0),
        ts=_select_ts(data),
        type=data.get("type", ""),
        payload=data.get("payload"),
        job_id=data.get("job_id"),
        group_id=data.get("group_id"),
        follower_id=data.get("follower_id"),
        pool=data.get("pool"),
        kind=data.get("kind"),
    )


class FileEventSource(EventSource):
    def __init__(
        self,
        path: str | Path,
        *,
        follow: bool = False,
        poll_interval: float = 0.2,
        last_n: int | None = None,
        start_from_ts: float | None = None,
    ) -> None:
        if last_n is not None and last_n < 0:
            raise ValueError("last_n must be >= 0 or None")
        if last_n is not None and start_from_ts is not None:
            raise ValueError("last_n and start_from_ts are mutually exclusive")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        self._path = Path(path)
        self._follow = follow
        self._poll_interval = poll_interval
        self._last_n = last_n
        self._start_from_ts = start_from_ts

    async def stream(self) -> AsyncIterator[Event]:
        partial = ""
        with self._path.open("r", encoding="utf-8") as handle:
            if self._follow and self._last_n == 0 and self._start_from_ts is None:
                handle.seek(0, os.SEEK_END)
            else:
                data = handle.read()
                lines = data.splitlines(keepends=True)
                if lines and not lines[-1].endswith("\n"):
                    partial = lines.pop()
                events = []
                for line in lines:
                    parsed = self._parse_line(line)
                    if parsed is not None:
                        events.append(parsed)
                if self._start_from_ts is not None:
                    events = [event for event in events if event.ts >= self._start_from_ts]
                if self._last_n is not None:
                    if self._last_n == 0:
                        events = []
                    else:
                        events = events[-self._last_n :]
                for event in events:
                    yield event

            while True:
                line = handle.readline()
                if not line:
                    if not self._follow:
                        break
                    await asyncio.sleep(self._poll_interval)
                    continue
                if partial:
                    line = partial + line
                    partial = ""
                if not line.endswith("\n"):
                    if self._follow:
                        partial = line
                        continue
                    break
                parsed = self._parse_line(line)
                if parsed is not None:
                    yield parsed

    @staticmethod
    def _parse_line(line: str) -> Event | None:
        raw = line.strip()
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return _parse_event(data)
