from __future__ import annotations

import pytest
from textual.app import App, ComposeResult
from textual.geometry import Offset
from textual.selection import Selection

from acty.tui.selectable_rich_log import SelectableRichLog


class _SelectableRichLogApp(App[None]):
    def compose(self) -> ComposeResult:
        yield SelectableRichLog(id="log", wrap=True, auto_scroll=False, markup=True)


@pytest.mark.asyncio
async def test_selectable_rich_log_renders_offsets_for_selection(tui_runner) -> None:
    app = _SelectableRichLogApp()
    async with tui_runner(app, size=(60, 10)):
        log = app.query_one("#log", SelectableRichLog)
        log.write("hello")

        strip = log.render_line(0)
        offsets = [
            segment.style.meta.get("offset")
            for segment in strip
            if segment.style is not None and segment.style.meta is not None
        ]
        assert any(offset is not None for offset in offsets)

        # First visible line at the top-left should have y==0.
        first_offset = next(offset for offset in offsets if offset is not None)
        assert first_offset[1] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start,end,expected",
    [
        (Offset(1, 0), Offset(4, 0), "ell"),
        (Offset(2, 0), Offset(3, 1), "llo\nwor"),
    ],
)
async def test_selectable_rich_log_get_selection_extracts_text(
    tui_runner,
    start: Offset,
    end: Offset,
    expected: str,
) -> None:
    app = _SelectableRichLogApp()
    async with tui_runner(app, size=(60, 10)):
        log = app.query_one("#log", SelectableRichLog)
        log.write("hello")
        log.write("world")

        selection = Selection.from_offsets(start, end)
        extracted = log.get_selection(selection)
        assert extracted is not None
        assert extracted[0] == expected


@pytest.mark.asyncio
async def test_selectable_rich_log_applies_selection_highlight_style(tui_runner) -> None:
    app = _SelectableRichLogApp()
    async with tui_runner(app, size=(60, 10)) as pilot:
        log = app.query_one("#log", SelectableRichLog)
        log.write("hello")

        # No selection => no selection background applied.
        strip = log.render_line(0)
        selection_style = log.screen.get_component_rich_style("screen--selection")
        assert all(
            segment.style is None or segment.style.bgcolor != selection_style.bgcolor
            for segment in strip
        )

        # Add selection and allow Screen watcher to update widgets.
        app.screen.selections = {
            log: Selection.from_offsets(Offset(1, 0), Offset(4, 0))
        }
        await pilot.pause()

        strip = log.render_line(0)
        assert any(
            segment.style is not None and segment.style.bgcolor == selection_style.bgcolor
            for segment in strip
        )
