import time

import pytest
from rich.console import Console
from textual.app import App, ComposeResult

from acty.tui.widgets.timeline import TimelineWidget
from acty.tui.widgets.timeline import TimelineRoadmapWidget
from acty_core.events.types import Event


def test_timeline_renders_retry_marker_for_result_handler_retry() -> None:
    widget = TimelineWidget(window_s=10.0, max_jobs=10)
    now = time.time()
    events = [
        Event(
            schema_version=1,
            run_id="run",
            seq=1,
            ts=now - 2.0,
            type="job_started",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
        ),
        Event(
            schema_version=1,
            run_id="run",
            seq=2,
            ts=now - 1.0,
            type="job_retrying",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
            payload={
                "source": "result_handler",
                "result_handler_attempt": 1,
                "repair_attempt": 1,
            },
        ),
        Event(
            schema_version=1,
            run_id="run",
            seq=3,
            ts=now - 0.5,
            type="job_succeeded",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
        ),
    ]
    for event in events:
        widget.handle_event(event)

    console = Console(width=120, record=True)
    console.print(widget.render())
    output = console.export_text()
    assert "↻" in output


def test_compact_timeline_lane_column_toggle() -> None:
    widget = TimelineWidget(window_s=10.0, max_jobs=10)
    now = time.time()
    widget.update_lane_summary({"alpha": 1, "beta": 1}, {}, None)
    widget.handle_event(
        Event(
            schema_version=1,
            run_id="run",
            seq=1,
            ts=now - 1.0,
            type="job_started",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
            payload={"job_context": {"lane": "alpha"}},
        )
    )
    widget.handle_event(
        Event(
            schema_version=1,
            run_id="run",
            seq=2,
            ts=now - 0.5,
            type="job_succeeded",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
            payload={"job_context": {"lane": "alpha"}},
        )
    )

    console = Console(width=120, record=True)
    console.print(widget.render())
    output = console.export_text()
    header = next(line for line in output.splitlines() if line.strip().startswith("pool"))
    assert "lane" in header

    widget.update_lane_summary({"default": 1}, {}, None)
    console = Console(width=120, record=True)
    console.print(widget.render())
    output = console.export_text()
    header = next(line for line in output.splitlines() if line.strip().startswith("pool"))
    assert "lane" not in header


def test_roadmap_timeline_lane_summary_and_tags() -> None:
    widget = TimelineRoadmapWidget(seconds_per_col=1.0, max_jobs=5, max_backlog=5)
    now = time.time()
    widget.update_lane_summary({"alpha": 1, "beta": 1}, {}, {"alpha": 1, "beta": 1})
    widget.handle_event(
        Event(
            schema_version=1,
            run_id="run",
            seq=1,
            ts=now - 1.0,
            type="job_started",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
            payload={"job_context": {"lane": "alpha"}},
        )
    )
    widget.handle_event(
        Event(
            schema_version=1,
            run_id="run",
            seq=2,
            ts=now - 0.5,
            type="job_succeeded",
            job_id="job-1",
            group_id="group-1",
            pool="primer",
            kind="primer",
            payload={"job_context": {"lane": "alpha"}},
        )
    )

    widget._build_lines()
    assert any("Lane Summary" in line for line in widget._layout_prefix)
    assert any("alpha" in line for line in widget._layout_prefix)

    span = widget._layout_spans[0]
    group_label = widget._short_group(span.group_id)
    if widget._show_lane_tags:
        lane_tag = "alpha"
        group_label = f"{lane_tag}|{group_label}"
    assert "alpha|" in group_label


class _TimelineRoadmapApp(App[None]):
    def compose(self) -> ComposeResult:
        yield TimelineRoadmapWidget(id="timeline", seconds_per_col=1.0, max_jobs=5, max_backlog=5)


@pytest.mark.asyncio
async def test_roadmap_timeline_lane_tag_renders_in_screen(tui_runner) -> None:
    app = _TimelineRoadmapApp()
    async with tui_runner(app, size=(120, 30)) as pilot:
        widget = app.query_one("#timeline", TimelineRoadmapWidget)
        widget.update_lane_summary({"alpha": 1, "beta": 1}, {}, {"alpha": 1, "beta": 1})
        now = time.time()
        widget.handle_event(
            Event(
                schema_version=1,
                run_id="run",
                seq=1,
                ts=now - 1.0,
                type="job_started",
                job_id="job-1",
                group_id="group-1",
                pool="primer",
                kind="primer",
                payload={"job_context": {"lane": "alpha"}},
            )
        )
        widget.handle_event(
            Event(
                schema_version=1,
                run_id="run",
                seq=2,
                ts=now - 0.5,
                type="job_succeeded",
                job_id="job-1",
                group_id="group-1",
                pool="primer",
                kind="primer",
                payload={"job_context": {"lane": "alpha"}},
            )
        )
        await pilot.pause()
        lines = [widget.render_line(y).text for y in range(widget.size.height)]
        assert any("alpha|group-1" in line for line in lines)
