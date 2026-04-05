"""Console capture utilities for the TUI (in-process only)."""

from __future__ import annotations

import asyncio
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, TextIO


def format_console_line(
    line: str,
    *,
    include_timestamp: bool = False,
    timestamp_format: str | None = None,
) -> str:
    if not include_timestamp:
        return line
    now = datetime.now()
    if timestamp_format is None:
        timestamp = now.strftime("%H:%M:%S") + f".{now.microsecond // 1000:03d}"
    else:
        timestamp = now.strftime(timestamp_format)
    return f"{timestamp} {line}"


class ConsoleCaptureStream:
    """File-like stream that buffers writes and emits lines to a queue."""

    def __init__(
        self,
        queue: asyncio.Queue[str],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        tee: TextIO | None = None,
        proxy: TextIO | None = None,
        include_timestamp: bool = False,
        formatter: Callable[[str], str] | None = None,
    ) -> None:
        self._queue = queue
        self._loop = loop
        self._tee = tee
        self._proxy = proxy or tee or sys.__stdout__
        self._lock = threading.Lock()
        self._local = threading.local()
        self._pending_cr = False
        self._line_buffer: list[str] = []
        self._formatter = formatter or (
            lambda line: format_console_line(line, include_timestamp=include_timestamp)
        )

        encoding_proxy = tee or proxy or sys.__stdout__
        self._encoding = getattr(encoding_proxy, "encoding", "utf-8")
        self._errors = getattr(encoding_proxy, "errors", "replace")

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def errors(self) -> str:
        return self._errors

    def isatty(self) -> bool:
        proxy = self._tee if self._tee is not None else self._proxy
        if proxy is not None and hasattr(proxy, "isatty"):
            return bool(proxy.isatty())
        return False

    def write(self, data: str | bytes) -> int:
        if data is None:
            return 0
        if isinstance(data, bytes):
            data = data.decode(self._encoding, errors=self._errors)
        if data == "":
            return 0

        if getattr(self._local, "in_write", False):
            if self._tee is not None and self._tee is not self:
                return self._tee.write(data)
            return len(data)

        with self._lock:
            self._local.in_write = True
            try:
                self._consume_text(data)
                if self._tee is not None and self._tee is not self:
                    self._tee.write(data)
            finally:
                self._local.in_write = False
        return len(data)

    def flush(self) -> None:
        if getattr(self._local, "in_write", False):
            return
        with self._lock:
            self._local.in_write = True
            try:
                self._flush_buffer()
                if self._tee is not None and self._tee is not self:
                    self._tee.flush()
            finally:
                self._local.in_write = False

    def _consume_text(self, text: str) -> None:
        index = 0
        if self._pending_cr:
            if text.startswith("\n"):
                index = 1
            self._pending_cr = False

        length = len(text)
        while index < length:
            char = text[index]
            if char == "\n":
                self._emit_current_line()
                index += 1
                continue
            if char == "\r":
                self._emit_current_line()
                if index + 1 < length and text[index + 1] == "\n":
                    index += 2
                else:
                    if index == length - 1:
                        self._pending_cr = True
                    index += 1
                continue
            self._line_buffer.append(char)
            index += 1

    def _flush_buffer(self) -> None:
        if self._pending_cr:
            self._pending_cr = False
        if self._line_buffer:
            self._emit_current_line()

    def _emit_current_line(self) -> None:
        line = "".join(self._line_buffer)
        self._line_buffer.clear()
        self._enqueue_line(line)

    def _enqueue_line(self, line: str) -> None:
        if self._formatter is not None:
            line = self._formatter(line)

        if self._loop is not None:
            def _safe_put() -> None:
                try:
                    self._queue.put_nowait(line)
                except (asyncio.QueueFull, RuntimeError):
                    return

            try:
                self._loop.call_soon_threadsafe(_safe_put)
                return
            except RuntimeError:
                pass

        try:
            self._queue.put_nowait(line)
        except (asyncio.QueueFull, RuntimeError):
            return


@dataclass
class ConsoleCaptureHandle:
    stdout: TextIO
    stderr: TextIO
    stdout_capture: ConsoleCaptureStream
    stderr_capture: ConsoleCaptureStream


def install_console_capture(
    queue: asyncio.Queue[str],
    *,
    loop: asyncio.AbstractEventLoop | None = None,
    include_timestamp: bool = False,
    tee: bool = False,
    enabled: bool = True,
) -> ConsoleCaptureHandle | None:
    if not enabled:
        return None

    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

    stdout = sys.stdout
    stderr = sys.stderr
    stdout_capture = ConsoleCaptureStream(
        queue,
        loop=loop,
        tee=stdout if tee else None,
        proxy=stdout,
        include_timestamp=include_timestamp,
    )
    stderr_capture = ConsoleCaptureStream(
        queue,
        loop=loop,
        tee=stderr if tee else None,
        proxy=stderr,
        include_timestamp=include_timestamp,
    )
    sys.stdout = stdout_capture
    sys.stderr = stderr_capture
    return ConsoleCaptureHandle(
        stdout=stdout,
        stderr=stderr,
        stdout_capture=stdout_capture,
        stderr_capture=stderr_capture,
    )


def restore_console_capture(handle: ConsoleCaptureHandle | None) -> None:
    if handle is None:
        return

    handle.stdout_capture.flush()
    handle.stderr_capture.flush()

    if sys.stdout is handle.stdout_capture:
        sys.stdout = handle.stdout
    if sys.stderr is handle.stderr_capture:
        sys.stderr = handle.stderr


__all__ = [
    "ConsoleCaptureHandle",
    "ConsoleCaptureStream",
    "format_console_line",
    "install_console_capture",
    "restore_console_capture",
]
