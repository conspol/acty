"""Selectable RichLog widget.

Textual's built-in `RichLog` widget renders `Strip`s without applying the offset
metadata required for mouse selection (and doesn't provide a `get_selection`
implementation). This subclass adds:

- Offsets in `render_line` so Textual can compute selection coordinates.
- `get_selection` so Screen-level copy (Ctrl+C) works.
- Visual selection highlighting similar to `textual.widgets.Log`.
"""

from __future__ import annotations

from collections.abc import Iterable

from rich.segment import Segment
from rich.style import Style
from textual.selection import Selection
from textual.strip import Strip
from textual.widgets import RichLog


def _apply_style_to_strip_span(strip: Strip, *, start: int, end: int, style: Style) -> Strip:
    """Apply a Rich style to a character span within a Strip.

    Args:
        strip: Source strip.
        start: Start character index (inclusive), relative to `strip.text`.
        end: End character index (exclusive), relative to `strip.text`.
        style: Style to apply.

    Returns:
        New Strip with style applied to the requested span.
    """

    if start < 0:
        start = 0
    if end <= start:
        return strip

    cell_length = strip._cell_length
    pos = 0
    out: list[Segment] = []

    for segment in strip:
        text, seg_style, control = segment
        seg_len = len(text)
        seg_end = pos + seg_len

        if seg_end <= start or pos >= end:
            out.append(segment)
            pos = seg_end
            continue

        before_len = max(0, start - pos)
        after_len = max(0, seg_end - end)

        if before_len:
            out.append(Segment(text[:before_len], seg_style, control))

        mid_text = text[before_len : seg_len - after_len]
        if mid_text:
            merged_style = style if seg_style is None else seg_style + style
            out.append(Segment(mid_text, merged_style, control))

        if after_len:
            out.append(Segment(text[seg_len - after_len :], seg_style, control))

        pos = seg_end

    return Strip(out, cell_length)


class SelectableRichLog(RichLog):
    """`RichLog` with mouse selection + Ctrl+C copy support."""

    def get_selection(self, selection: Selection) -> tuple[str, str] | None:
        text = "\n".join(_iter_strip_text(self.lines))
        return selection.extract(text), "\n"

    def selection_updated(self, selection: Selection | None) -> None:
        # RichLog caches rendered lines; selection highlighting is dynamic.
        self._line_cache.clear()
        self.refresh()

    def render_line(self, y: int) -> Strip:
        scroll_x, scroll_y = self.scroll_offset
        line_index = scroll_y + y
        width = self.scrollable_content_region.width

        line = self._render_line(line_index, scroll_x, width).apply_style(self.rich_style)

        # Highlight current selection (if any).
        selection = self.text_selection
        if selection is not None:
            span = selection.get_span(line_index)
            if span is not None:
                span_start, span_end = span
                # Convert absolute (line) x offsets -> relative to the current horizontal scroll.
                rel_start = span_start - scroll_x
                rel_end = len(line.text) if span_end == -1 else span_end - scroll_x
                rel_start = max(0, rel_start)
                rel_end = min(len(line.text), rel_end)
                if rel_end > rel_start:
                    selection_style = self.screen.get_component_rich_style(
                        "screen--selection"
                    )
                    line = _apply_style_to_strip_span(
                        line, start=rel_start, end=rel_end, style=selection_style
                    )

        # Critical for selection: annotate segments with ("offset": (x, y)) metadata.
        return line.apply_offsets(scroll_x, line_index)


def _iter_strip_text(strips: Iterable[Strip]) -> Iterable[str]:
    for strip in strips:
        # RichLog pads rendered strips to a fixed width; avoid copying that padding.
        yield strip.text.rstrip(" ")
