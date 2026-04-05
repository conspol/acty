"""Textual UI with plotext graphs for runtime events."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from typing import Deque

import structlog
from rich.text import Text

from textual.app import App, ComposeResult
from textual.css.query import NoMatches
from textual.containers import Grid, Horizontal, VerticalScroll
from textual.widgets import Footer, Header, Static

from acty_core.events.types import Event
from acty.tui.formatting import format_log_for_display, format_snapshot
from acty.tui.history import MetricsHistory
from acty.tui.selectable_rich_log import SelectableRichLog
from acty.tui.console_capture import ConsoleCaptureHandle, install_console_capture, restore_console_capture
from acty.tui.console_guardrails import CONSOLE_CAPTURE_GUARDRAILS
from acty.tui.sources.base import EventSource
from acty.tui.state import TuiState
from acty.tui.widgets import (
    BurstIndicator,
    DiagnosticWidget,
    QueueDepthPlot,
    ThroughputPlot,
    WorkerPoolWidget,
    WorkStructureWidget,
    TimelineRoadmapWidget,
    TimelineWidget,
)

logger = structlog.get_logger(__name__)


class RuntimeTuiApp(App):
    """TUI with real-time plots."""

    CSS = """
    Screen {
        layout: vertical;
    }

    #plot-grid {
        layout: grid;
        grid-size: 6 2;
        grid-rows: 2fr 3fr;
        height: 60%;
        padding: 0 1;
    }

    .plot-cell {
        border: solid $primary;
        padding: 0;
        height: 100%;
    }

    #throughput-plot {
        column-span: 3;
    }

    #queue-plot {
        column-span: 3;
    }

    #diagnostic-scroll {
        column-span: 2;
        border-title-align: left;
    }

    #diagnostic-widget {
        padding: 0 1;
    }

    #work-structure-scroll {
        column-span: 2;
        height: 100%;
        border: solid $success;
        border-title-align: left;
    }

    #work-structure {
        padding: 0 1;
    }

    #worker-pools-scroll {
        column-span: 1;
        height: 100%;
        border: solid $accent;
        border-title-align: left;
    }

    #worker-pools {
        padding: 0 1;
    }

    #stats-burst {
        column-span: 1;
        height: 100%;
        border: solid $secondary;
        padding: 0;
        border-title-align: left;
    }

    #stats-summary {
        padding: 0 1;
    }

    #burst-indicator {
        padding: 0 1;
    }

    #logs {
        height: 1fr;
        border: solid $secondary;
        padding: 0;
    }

    #dashboard {
        height: 1fr;
    }

    #timeline-view {
        height: 1fr;
        display: none;
        border: solid $primary;
    }

    #timeline {
        height: 1fr;
        border: solid $primary;
        padding: 1 1;
    }

    #timeline-roadmap-view {
        height: 1fr;
        display: none;
        border: solid $primary;
        padding: 1 1;
    }


    #log-split {
        height: 1fr;
    }

    #scheduler-logs {
        width: 1fr;
        border: solid $secondary;
        padding: 0;
    }

    #console-logs {
        width: 1fr;
        border: solid $secondary;
        padding: 0;
    }
    """

    BINDINGS = [
        ("p", "toggle_pause", "Pause scheduler logs"),
        ("f", "toggle_follow", "Follow scheduler logs"),
        ("c", "copy_logs", "Copy scheduler logs"),
        ("T", "toggle_timeline", "Timeline (compact)"),
        ("t", "toggle_roadmap", "Timeline (roadmap)"),
        ("s", "toggle_timeline_sort", "Timeline sort"),
        ("v", "cycle_viz", "Cycle viz mode"),
        ("b", "cycle_diagnostic", "Cycle diagnostic"),
        ("q", "quit", "Quit"),
    ]

    CONSOLE_BINDINGS = [
        ("P", "toggle_console_pause", "Pause console logs"),
        ("F", "toggle_console_follow", "Follow console logs"),
        ("C", "copy_console_logs", "Copy console logs"),
    ]

    def __init__(
        self,
        event_source: EventSource,
        *,
        state: TuiState | None = None,
        queue_maxsize: int = 0,
        poll_interval_s: float = 0.05,
        stats_interval_s: float = 0.5,
        plot_interval_s: float = 1.0,
        timeline_interval_s: float = 0.2,
        max_events_per_tick: int = 200,
        history_points: int = 120,
        exit_on_source_end: bool = False,
        console_log_queue: asyncio.Queue[str] | None = None,
        console_tee: bool = False,
    ) -> None:
        super().__init__()
        self._event_source = event_source
        self._pauseable_source = (
            event_source if callable(getattr(event_source, "set_paused", None)) else None
        )
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_maxsize)
        self._source_task: asyncio.Task[None] | None = None
        self._state = state or TuiState()
        self._console_log_queue = console_log_queue
        self._console_capture_enabled = self._console_log_queue is not None
        self._console_capture_tee = console_tee
        if self._console_capture_enabled:
            for keys, action, description in self.CONSOLE_BINDINGS:
                self.bind(keys, action, description=description)
        self._console_capture_handle: ConsoleCaptureHandle | None = None
        self._console_max_logs: int | None = None
        self._console_logs: Deque[str] | None = None
        self._pending_console_logs: Deque[str] | None = None
        self._console_paused: bool | None = None
        self._console_follow: bool | None = None
        self._console_last_scroll_y: float | None = None
        if self._console_capture_enabled:
            self._console_max_logs = self._state.max_logs
            self._console_logs = deque(maxlen=self._console_max_logs)
            self._pending_console_logs = deque()
            self._console_paused = False
            self._console_follow = True
            self._console_last_scroll_y = 0.0
        self._poll_interval_s = poll_interval_s
        self._stats_interval_s = stats_interval_s
        self._plot_interval_s = plot_interval_s
        self._timeline_interval_s = timeline_interval_s
        self._max_events_per_tick = max_events_per_tick
        self._exit_on_source_end = exit_on_source_end
        self._paused = False
        self._follow = True
        self._pending_logs: Deque[str] = deque()
        self._history = MetricsHistory(max_points=history_points)
        self._plot_time_offset = 0.0
        self._paused_at: float | None = None
        self._burst_active = False
        self._burst_cap = 0
        self._burst_runnable = 0
        self._burst_low_watermark = 0
        self._burst_base_cap = 0
        self._burst_allowance = 0
        self._last_scroll_y: float = 0.0
        self._last_empty_poll_log = 0.0
        self._empty_poll_log_interval_s = 2.0
        self._active_view = "dashboard"
        self._timeline_widget: TimelineWidget | None = None
        self._timeline_roadmap_widget: TimelineRoadmapWidget | None = None
        self._timeline_sort_mode = "time"

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="dashboard"):
            with Grid(id="plot-grid"):
                yield ThroughputPlot(id="throughput-plot", classes="plot-cell")
                yield QueueDepthPlot(id="queue-plot", classes="plot-cell")
                with VerticalScroll(id="diagnostic-scroll", classes="plot-cell") as diag:
                    diag.border_title = "Diagnostics"
                    yield DiagnosticWidget(id="diagnostic-widget")
                with VerticalScroll(id="work-structure-scroll", classes="plot-cell") as ws:
                    ws.border_title = "Work Structure"
                    yield WorkStructureWidget(id="work-structure")
                with VerticalScroll(id="worker-pools-scroll", classes="plot-cell") as wp:
                    wp.border_title = "Worker Pools"
                    yield WorkerPoolWidget(id="worker-pools")
                with VerticalScroll(id="stats-burst", classes="plot-cell") as sb:
                    sb.border_title = "Stats"
                    yield BurstIndicator(id="burst-indicator")
                    yield Static(id="stats-summary")

            if self._console_log_queue is None:
                yield SelectableRichLog(
                    id="logs",
                    wrap=True,
                    auto_scroll=False,
                    markup=True,
                    max_lines=self._state.max_logs,
                )
            else:
                with Horizontal(id="log-split"):
                    scheduler_logs = SelectableRichLog(
                        id="scheduler-logs",
                        wrap=True,
                        auto_scroll=False,
                        markup=True,
                        max_lines=self._state.max_logs,
                    )
                    scheduler_logs.border_title = "Scheduler Logs"
                    yield scheduler_logs
                    console_logs = SelectableRichLog(
                        id="console-logs",
                        wrap=True,
                        auto_scroll=False,
                        markup=True,
                        max_lines=self._state.max_logs,
                    )
                    console_logs.border_title = "Console Logs"
                    yield console_logs

        with VerticalScroll(id="timeline-view"):
            timeline = TimelineWidget(id="timeline")
            timeline.border_title = "Timeline"
            yield timeline

        roadmap = TimelineRoadmapWidget(id="timeline-roadmap-view")
        roadmap.border_title = "Timeline (Roadmap)"
        yield roadmap

        yield Footer()

    async def on_mount(self) -> None:
        self.set_interval(self._poll_interval_s, self._poll_events)
        if self._console_log_queue is not None:
            self.set_interval(self._poll_interval_s, self._poll_console_logs)
        self.set_interval(self._stats_interval_s, self._refresh_stats)
        self.set_interval(self._plot_interval_s, self._refresh_plots)
        self.set_interval(self._timeline_interval_s, self._refresh_timeline)
        if self._console_capture_enabled and self._console_log_queue is not None:
            self._console_capture_handle = install_console_capture(
                self._console_log_queue,
                tee=self._console_capture_tee
                or CONSOLE_CAPTURE_GUARDRAILS.output_mode == "tee",
            )
        with contextlib.suppress(NoMatches):
            self._timeline_widget = self.query_one("#timeline", TimelineWidget)
        with contextlib.suppress(NoMatches):
            self._timeline_roadmap_widget = self.query_one(
                "#timeline-roadmap-view", TimelineRoadmapWidget
            )
        self._apply_timeline_sort_mode()
        self._source_task = asyncio.create_task(self._consume_events())
        self._refresh_stats()

    async def on_shutdown(self) -> None:
        try:
            if self._source_task is None:
                return
            self._source_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._source_task
        finally:
            restore_console_capture(self._console_capture_handle)
            self._console_capture_handle = None

    async def _consume_events(self) -> None:
        try:
            async for event in self._event_source.stream():
                await self._event_queue.put(event)
        except asyncio.CancelledError:
            logger.debug(
                "Event consumption cancelled",
                exit_on_source_end=self._exit_on_source_end,
            )
            return
        if self._exit_on_source_end:
            self.exit()

    async def _poll_events(self) -> None:
        if self._paused and self._pauseable_source is not None:
            return
        events = 0
        while events < self._max_events_per_tick:
            if self._paused and self._pauseable_source is not None:
                break
            try:
                event = self._event_queue.get_nowait()
            except asyncio.QueueEmpty:
                now = time.monotonic()
                if now - self._last_empty_poll_log >= self._empty_poll_log_interval_s:
                    self._last_empty_poll_log = now
                    logger.debug(
                        "No events available in queue",
                        queue_size=self._event_queue.qsize(),
                        max_events_per_tick=self._max_events_per_tick,
                        events_processed=events,
                    )
                break
            events += 1

            if event.type == "burst_on":
                self._burst_active = True
                if event.payload:
                    self._burst_cap = event.payload.get("cap", self._burst_cap)
                    self._burst_runnable = event.payload.get("runnable", self._burst_runnable)
                    self._burst_low_watermark = event.payload.get(
                        "low_watermark", self._burst_low_watermark
                    )
                    self._burst_base_cap = event.payload.get("base_cap", self._burst_base_cap)
                    self._burst_allowance = event.payload.get(
                        "burst_allowance", self._burst_allowance
                    )
            elif event.type == "burst_off":
                self._burst_active = False
                if event.payload:
                    self._burst_cap = event.payload.get("cap", self._burst_cap)
                    self._burst_runnable = event.payload.get("runnable", self._burst_runnable)
                    self._burst_low_watermark = event.payload.get(
                        "low_watermark", self._burst_low_watermark
                    )
                    self._burst_base_cap = event.payload.get("base_cap", self._burst_base_cap)
                    self._burst_allowance = event.payload.get(
                        "burst_allowance", self._burst_allowance
                    )
            elif event.type == "cap_changed":
                if event.payload:
                    self._burst_cap = event.payload.get("cap", self._burst_cap)
                    self._burst_base_cap = event.payload.get("base_cap", self._burst_base_cap)
                    self._burst_allowance = event.payload.get(
                        "burst_allowance", self._burst_allowance
                    )
                    self._burst_low_watermark = event.payload.get(
                        "low_watermark", self._burst_low_watermark
                    )

            await self._state.handle(event)
            if self._timeline_widget is not None:
                self._timeline_widget.handle_event(event)
            if self._timeline_roadmap_widget is not None:
                self._timeline_roadmap_widget.handle_event(event)
            line = self._state.log_lines()[-1]
            if self._paused:
                self._pending_logs.append(line)
            else:
                self._append_log(line)
            self._event_queue.task_done()

        if not self._paused and self._pending_logs:
            while self._pending_logs:
                self._append_log(self._pending_logs.popleft())

    async def _poll_console_logs(self) -> None:
        if (
            self._console_log_queue is None
            or self._console_logs is None
            or self._pending_console_logs is None
        ):
            return
        drained = 0
        while drained < self._max_events_per_tick:
            try:
                line = self._console_log_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            drained += 1
            self._console_log_queue.task_done()
            self._console_logs.append(line)
            if self._console_paused:
                self._pending_console_logs.append(line)
            else:
                self._append_console_log(line)

        if not self._console_paused and self._pending_console_logs:
            while self._pending_console_logs:
                self._append_console_log(self._pending_console_logs.popleft())

    def _scheduler_log_selector(self) -> str:
        return "#scheduler-logs" if self._console_log_queue is not None else "#logs"

    def _append_log(self, line: str) -> None:
        try:
            log = self.query_one(self._scheduler_log_selector(), SelectableRichLog)
        except NoMatches:
            return
        current_scroll = log.scroll_y
        if self._follow and current_scroll < self._last_scroll_y:
            self._follow = False

        log.write(format_log_for_display(line))

        if self._follow:
            log.scroll_end(animate=False)

        self._last_scroll_y = log.scroll_y

    def _append_console_log(self, line: str) -> None:
        if self._console_follow is None or self._console_last_scroll_y is None:
            return
        try:
            log = self.query_one("#console-logs", SelectableRichLog)
        except NoMatches:
            return
        current_scroll = log.scroll_y
        if self._console_follow and current_scroll < self._console_last_scroll_y:
            self._console_follow = False

        renderable = Text.from_ansi(line) if "\x1b" in line else line
        log.write(renderable)

        if self._console_follow:
            log.scroll_end(animate=False)

        self._console_last_scroll_y = log.scroll_y

    def _refresh_stats(self) -> None:
        if self._paused and self._pauseable_source is not None:
            return
        snapshot = self._state.snapshot()

        self._history.record(time.time() - self._plot_time_offset, snapshot)

        try:
            stats = self.query_one("#stats-summary", Static)
            burst = self.query_one("#burst-indicator", BurstIndicator)
        except NoMatches:
            return

        stats.update(format_snapshot(snapshot))

        burst.update_burst(
            active=self._burst_active,
            low_watermark=self._burst_low_watermark,
            cap=self._burst_cap,
            runnable=self._burst_runnable or snapshot.queue_depth,
            inflight=snapshot.inflight_workers,
        )

    def _refresh_plots(self) -> None:
        if self._paused and self._pauseable_source is not None:
            return
        if len(self._history) < 2:
            return

        timestamps = self._history.timestamps()
        snapshot = self._state.snapshot()

        try:
            throughput_plot = self.query_one("#throughput-plot", ThroughputPlot)
            queue_plot = self.query_one("#queue-plot", QueueDepthPlot)
            diagnostic = self.query_one("#diagnostic-widget", DiagnosticWidget)
            work_widget = self.query_one("#work-structure", WorkStructureWidget)
            pool_widget = self.query_one("#worker-pools", WorkerPoolWidget)
        except NoMatches:
            return

        throughput_plot.update_data(
            timestamps,
            self._history.calls_rates(),
            self._history.inflight_workers_series(),
        )

        queue_plot.update_data(
            timestamps,
            self._history.runnable_breakdown(),
            self._history.queue_depths(),
        )

        admission_state = {
            "cap": self._burst_cap,
            "base_cap": self._burst_base_cap,
            "burst_allowance": self._burst_allowance,
            "low_watermark": self._burst_low_watermark,
            "runnable": snapshot.queue_depth,
            "pending": snapshot.groups_pending,
        }

        runnable_state = {
            "queue_depth": snapshot.queue_depth,
            "inflight": snapshot.inflight_workers,
            "runnable_by_kind": snapshot.runnable_by_kind,
            "inflight_by_kind": snapshot.inflight_by_kind,
        }

        watermark_state = {
            "runnable": snapshot.queue_depth,
            "low_watermark": self._burst_low_watermark,
            "burst_active": self._burst_active,
            "warm_delay_count": snapshot.groups_warm_delay,
        }

        diagnostic.update_data(
            admission_state=admission_state,
            runnable_state=runnable_state,
            pool_stats=snapshot.pool_stats,
            watermark_state=watermark_state,
        )

        work_widget.update_state(
            groups_primer_ready=snapshot.groups_primer_ready,
            groups_primer_inflight=snapshot.groups_primer_inflight,
            groups_warm_delay=snapshot.groups_warm_delay,
            groups_followers_ready=snapshot.groups_followers_ready,
            groups_pending=snapshot.groups_pending,
            groups_done=snapshot.groups_done,
            groups_failed=snapshot.groups_failed,
            runnable_by_kind=snapshot.runnable_by_kind,
            inflight_by_kind=snapshot.inflight_by_kind,
            followers_by_group=snapshot.followers_by_group,
            pending_by_lane=snapshot.pending_by_lane,
            inflight_by_lane=snapshot.inflight_by_lane,
            lane_targets=snapshot.lane_targets,
        )

        pool_widget.update_pools(snapshot.pool_stats)
        if self._timeline_widget is not None:
            self._timeline_widget.update_lane_summary(
                snapshot.pending_by_lane,
                snapshot.inflight_by_lane,
                snapshot.lane_targets,
            )
        if self._timeline_roadmap_widget is not None:
            self._timeline_roadmap_widget.update_lane_summary(
                snapshot.pending_by_lane,
                snapshot.inflight_by_lane,
                snapshot.lane_targets,
            )

    def _refresh_timeline(self) -> None:
        if self._active_view == "compact" and self._timeline_widget is not None:
            self._timeline_widget.tick()
            self._timeline_widget.refresh()
        if self._active_view == "roadmap" and self._timeline_roadmap_widget is not None:
            self._timeline_roadmap_widget.tick()
            self._timeline_roadmap_widget.refresh()

    def _apply_timeline_sort_mode(self) -> None:
        if self._timeline_widget is not None:
            self._timeline_widget.set_sort_mode(self._timeline_sort_mode)
        if self._timeline_roadmap_widget is not None:
            self._timeline_roadmap_widget.set_sort_mode(self._timeline_sort_mode)

    def action_toggle_timeline_sort(self) -> None:
        self._timeline_sort_mode = "group" if self._timeline_sort_mode == "time" else "time"
        self._apply_timeline_sort_mode()
        self._refresh_timeline()

    def action_toggle_pause(self) -> None:
        self._paused = not self._paused
        if self._pauseable_source is not None:
            self._pauseable_source.set_paused(self._paused)
            if self._paused:
                self._paused_at = time.time()
            else:
                if self._paused_at is not None:
                    self._plot_time_offset += time.time() - self._paused_at
                self._paused_at = None
        if not self._paused:
            self._flush_logs()

    def action_toggle_console_pause(self) -> None:
        if self._console_log_queue is None or self._console_paused is None:
            return
        self._console_paused = not self._console_paused
        if not self._console_paused:
            self._flush_console_logs()

    def action_toggle_follow(self) -> None:
        self._follow = not self._follow
        if self._follow:
            log = self.query_one(self._scheduler_log_selector(), SelectableRichLog)
            log.scroll_end(animate=False)
            self._last_scroll_y = log.scroll_y

    def action_toggle_console_follow(self) -> None:
        if self._console_log_queue is None or self._console_follow is None:
            return
        self._console_follow = not self._console_follow
        if self._console_follow:
            log = self.query_one("#console-logs", SelectableRichLog)
            log.scroll_end(animate=False)
            self._console_last_scroll_y = log.scroll_y

    def action_copy_logs(self) -> None:
        text = "\n".join(self._state.log_lines())
        self.copy_to_clipboard(text)

    def action_copy_console_logs(self) -> None:
        if self._console_log_queue is None or self._console_logs is None:
            return
        text = "\n".join(self._console_logs)
        self.copy_to_clipboard(text)

    def action_cycle_viz(self) -> None:
        work_widget = self.query_one("#work-structure", WorkStructureWidget)
        work_widget.cycle_mode()

    def _set_view(self, mode: str) -> None:
        self._active_view = mode
        dashboard = self.query_one("#dashboard")
        compact = self.query_one("#timeline-view")
        roadmap = self.query_one("#timeline-roadmap-view")
        dashboard.display = mode == "dashboard"
        compact.display = mode == "compact"
        roadmap.display = mode == "roadmap"
        if mode == "compact":
            self._refresh_timeline()
            with contextlib.suppress(NoMatches):
                compact.focus()
        if mode == "roadmap":
            self._refresh_timeline()
            with contextlib.suppress(NoMatches):
                roadmap.focus()
        self.refresh(layout=True)

    def action_toggle_timeline(self) -> None:
        if self._active_view == "compact":
            self._set_view("dashboard")
        else:
            self._set_view("compact")

    def action_toggle_roadmap(self) -> None:
        if self._active_view == "roadmap":
            self._set_view("dashboard")
        else:
            self._set_view("roadmap")

    def action_cycle_diagnostic(self) -> None:
        diagnostic = self.query_one("#diagnostic-widget", DiagnosticWidget)
        diagnostic.cycle_view()

    def _flush_logs(self) -> None:
        log = self.query_one(self._scheduler_log_selector(), SelectableRichLog)
        log.clear()
        for line in self._state.log_lines():
            log.write(format_log_for_display(line))
        if self._follow:
            log.scroll_end(animate=False)

    def _flush_console_logs(self) -> None:
        if self._console_logs is None or self._pending_console_logs is None:
            return
        try:
            log = self.query_one("#console-logs", SelectableRichLog)
        except NoMatches:
            return
        log.clear()
        for line in self._console_logs:
            log.write(line)
        self._pending_console_logs.clear()
        if self._console_follow:
            log.scroll_end(animate=False)
        self._console_last_scroll_y = log.scroll_y
