import asyncio
import logging
import sys

import pytest
from textual.css.query import NoMatches

from acty.tui.app import RuntimeTuiApp
from acty.tui.console_capture import ConsoleCaptureStream
from acty.tui.selectable_rich_log import SelectableRichLog, _iter_strip_text
from acty.tui.sources import QueueEventSource
from acty_core.events.types import Event


def _drain_queue(queue: asyncio.Queue[str]) -> list[str]:
    items: list[str] = []
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        items.append(item)
        queue.task_done()
    return items


def _log_text(log: SelectableRichLog) -> str:
    return "\n".join(_iter_strip_text(log.lines))


class _DummyStream:
    def __init__(self, is_tty: bool) -> None:
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty

    def write(self, data: str) -> int:
        return len(data)

    def flush(self) -> None:
        return None


def test_console_capture_buffers_newlines_and_carriage_returns() -> None:
    queue: asyncio.Queue[str] = asyncio.Queue()
    stream = ConsoleCaptureStream(queue)

    stream.write("hello")
    assert queue.empty()

    stream.write(" world\n")
    assert _drain_queue(queue) == ["hello world"]

    stream.write("partial")
    stream.flush()
    assert _drain_queue(queue) == ["partial"]

    stream.write("progress 1\rprogress 2\r\n")
    assert _drain_queue(queue) == ["progress 1", "progress 2"]


def test_console_capture_isatty_proxies_without_tee() -> None:
    queue: asyncio.Queue[str] = asyncio.Queue()
    proxy = _DummyStream(True)
    stream = ConsoleCaptureStream(queue, proxy=proxy)

    assert stream.isatty() is True


def test_console_capture_isatty_proxies_with_tee() -> None:
    queue: asyncio.Queue[str] = asyncio.Queue()
    tee = _DummyStream(True)
    stream = ConsoleCaptureStream(queue, tee=tee, proxy=_DummyStream(False))

    assert stream.isatty() is True


def test_console_state_skipped_when_capture_disabled() -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    assert app._console_logs is None
    assert app._pending_console_logs is None

    app.action_toggle_console_pause()
    app.action_toggle_console_follow()
    app.action_copy_console_logs()


@pytest.mark.asyncio
async def test_console_capture_swaps_and_restores_stdio_in_tui(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    original_stdout = sys.stdout
    original_stderr = sys.stderr

    async with tui_runner(app):
        assert isinstance(sys.stdout, ConsoleCaptureStream)
        assert isinstance(sys.stderr, ConsoleCaptureStream)
        assert sys.stdout is not original_stdout
        assert sys.stderr is not original_stderr

    assert sys.stdout is original_stdout
    assert sys.stderr is original_stderr


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "console_enabled,expected",
    [
        (False, {"P": False, "F": False, "C": False}),
        (True, {"P": True, "F": True, "C": True}),
    ],
)
async def test_console_bindings_only_when_console_enabled(tui_runner, console_enabled: bool, expected: dict) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] | None = asyncio.Queue() if console_enabled else None
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app):
        bindings = app.active_bindings
        assert ("P" in bindings) is expected["P"]
        assert ("F" in bindings) is expected["F"]
        assert ("C" in bindings) is expected["C"]


@pytest.mark.asyncio
async def test_logging_captured_in_console_pane(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    logger = logging.getLogger("acty.tui.console.capture.test")
    old_level = logger.level
    old_propagate = logger.propagate
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        async with tui_runner(app) as pilot:
            handler = logging.StreamHandler(stream=sys.stderr)
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
            try:
                logger.info("log line from logger")
                await pilot.pause(0.05)
                console_logs = app.query_one("#console-logs", SelectableRichLog)
                assert "log line from logger" in _log_text(console_logs)
            finally:
                logger.removeHandler(handler)
    finally:
        logger.setLevel(old_level)
        logger.propagate = old_propagate


@pytest.mark.asyncio
async def test_console_pane_visibility_toggle(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app):
        app.query_one("#logs", SelectableRichLog)
        with pytest.raises(NoMatches):
            app.query_one("#console-logs", SelectableRichLog)
        with pytest.raises(NoMatches):
            app.query_one("#scheduler-logs", SelectableRichLog)

    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app_with_console = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app_with_console):
        app_with_console.query_one("#scheduler-logs", SelectableRichLog)
        app_with_console.query_one("#console-logs", SelectableRichLog)
        with pytest.raises(NoMatches):
            app_with_console.query_one("#logs", SelectableRichLog)


@pytest.mark.asyncio
async def test_console_logs_render_in_console_pane(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app) as pilot:
        await event_queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=1,
                ts=1.0,
                type="job_queued",
                job_id="j1",
                group_id="g1",
                kind="primer",
            )
        )
        await console_queue.put("console line")

        await pilot.pause(0.05)

        scheduler_logs = app.query_one("#scheduler-logs", SelectableRichLog)
        console_logs = app.query_one("#console-logs", SelectableRichLog)
        scheduler_text = _log_text(scheduler_logs)
        console_text = _log_text(console_logs)

        assert "job_queued" in scheduler_text
        assert "console line" in console_text
        assert "console line" not in scheduler_text


@pytest.mark.asyncio
async def test_console_capture_does_not_affect_scheduler_copy(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )
    captured: list[str] = []

    async with tui_runner(app) as pilot:
        app.copy_to_clipboard = lambda text: captured.append(text)
        await event_queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=1,
                ts=1.0,
                type="job_queued",
                job_id="j1",
                group_id="g1",
                kind="primer",
            )
        )
        await console_queue.put("console line")
        await pilot.pause(0.05)

        scheduler_logs = app.query_one("#scheduler-logs", SelectableRichLog)
        scheduler_text = _log_text(scheduler_logs)

        app.action_copy_logs()

        assert "job_queued" in scheduler_text
        assert captured
        assert "job_queued" in captured[-1]
        assert "console line" not in scheduler_text
        assert "console line" not in captured[-1]


@pytest.mark.asyncio
async def test_console_pause_buffers_until_resumed(tui_runner) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(event_queue),
        console_log_queue=console_queue,
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app) as pilot:
        app.action_toggle_console_pause()
        await console_queue.put("paused line")
        await pilot.pause(0.05)

        assert app._pending_console_logs
        console_logs = app.query_one("#console-logs", SelectableRichLog)
        assert "paused line" not in _log_text(console_logs)

        app.action_toggle_console_pause()
        await pilot.pause(0.05)

        assert not app._pending_console_logs
        assert "paused line" in _log_text(console_logs)
