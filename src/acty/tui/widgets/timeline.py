"""Timeline view for job execution in the TUI."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Mapping

import hashlib
import math

from rich.bar import BEGIN_BLOCK_ELEMENTS, END_BLOCK_ELEMENTS, FULL_BLOCK
from rich.console import Group
from rich.segment import Segment
from rich.style import Style
from rich.text import Text
from textual.geometry import Size
from textual.scroll_view import ScrollView
from textual.strip import Strip
from textual.widgets import Static

from acty_core.events.types import Event


@dataclass
class JobSpan:
    job_id: str
    group_id: str | None
    lane: str | None
    pool: str | None
    kind: str | None
    start_ts: float
    end_ts: float | None = None
    ok: bool | None = None
    retry_scheduled: bool = False
    retry_markers: list[float] = field(default_factory=list)
    tenacity_markers: list[float] = field(default_factory=list)
    queued_ts: float | None = None
    warmup_start_ts: float | None = None
    warmup_end_ts: float | None = None

    @property
    def duration(self) -> float:
        end = self.end_ts if self.end_ts is not None else time.time()
        return max(0.0, end - self.start_ts)


@dataclass
class BacklogSpan:
    job_id: str
    group_id: str | None
    lane: str | None
    kind: str | None
    queued_ts: float
    start_ts: float | None = None

    @property
    def end_ts(self) -> float | None:
        return self.start_ts


@dataclass
class WarmupSpan:
    group_id: str
    start_ts: float
    end_ts: float | None = None


def _payload(event: Event) -> Mapping:
    return event.payload if isinstance(event.payload, Mapping) else {}


def _is_resubmit_failure(event: Event) -> bool:
    if event.type != "job_failed":
        return False
    payload = _payload(event)
    return payload.get("error") == "result_handler_resubmit"


def _is_resubmit_marker(event: Event) -> bool:
    if event.type == "job_resubmitted":
        return True
    if event.type == "job_result_handled":
        payload = _payload(event)
        return payload.get("action") == "resubmit"
    return False


def _retry_job_id(event: Event) -> str | None:
    if event.job_id:
        return event.job_id
    payload = _payload(event)
    original = payload.get("original_job_id")
    if isinstance(original, str):
        return original
    return None


def _lane_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    return str(value)


def _lane_from_job_context(job_context: Mapping[str, Any]) -> str | None:
    lane = _lane_value(job_context.get("lane"))
    if lane:
        return lane
    group_context = job_context.get("group_context")
    if isinstance(group_context, Mapping):
        lane = _lane_value(group_context.get("lane"))
        if lane:
            return lane
    return None


def _lane_from_event(event: Event) -> str | None:
    payload = _payload(event)
    job_context = payload.get("job_context")
    if isinstance(job_context, Mapping):
        return _lane_from_job_context(job_context)
    return None


def _normalize_lane(lane: str | None) -> str:
    return lane or "default"


def _truncate_label(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _lane_rows(
    pending_by_lane: Mapping[str, int],
    inflight_by_lane: Mapping[str, int],
) -> list[tuple[int, str, int, int]]:
    lanes = set(pending_by_lane) | set(inflight_by_lane)
    rows: list[tuple[int, str, int, int]] = []
    for lane in lanes:
        pending = pending_by_lane.get(lane, 0)
        inflight = inflight_by_lane.get(lane, 0)
        total = pending + inflight
        if total <= 0:
            continue
        rows.append((total, lane, pending, inflight))
    rows.sort(key=lambda row: (-row[0], row[1]))
    return rows


def _lane_lines(
    pending_by_lane: Mapping[str, int],
    inflight_by_lane: Mapping[str, int],
    lane_targets: Mapping[str, int] | None,
    *,
    max_lines: int = 8,
    lane_width: int = 14,
) -> list[str]:
    rows = _lane_rows(pending_by_lane, inflight_by_lane)
    if not rows:
        return []
    if len(rows) == 1 and rows[0][1] == "default":
        return []
    lines: list[str] = []
    for _, lane, pending, inflight in rows[:max_lines]:
        lane_label = _truncate_label(lane, lane_width)
        target = lane_targets.get(lane) if lane_targets else None
        target_label = "-" if target is None else str(target)
        lines.append(f"{lane_label:<{lane_width}} P={pending}  I={inflight}  T={target_label}")
    if len(rows) > max_lines:
        lines.append(f"... +{len(rows) - max_lines} more")
    return lines


def _lane_set_from_counts(
    pending_by_lane: Mapping[str, int],
    inflight_by_lane: Mapping[str, int],
) -> set[str]:
    rows = _lane_rows(pending_by_lane, inflight_by_lane)
    return {_normalize_lane(lane) for _, lane, _, _ in rows}


def _should_show_lanes(lanes: set[str]) -> bool:
    return len(lanes) > 1


def _apply_retry_markers(
    cells: list[str],
    styles: list[Style | None],
    retry_markers: list[float],
    *,
    window_start: float,
    total: float,
    width: int,
    style: Style | None,
) -> None:
    if not retry_markers or total <= 0 or width <= 0:
        return
    for ts in retry_markers:
        pos = (ts - window_start) / total
        if pos < 0 or pos > 1:
            continue
        idx = int(pos * width)
        if idx >= width:
            idx = width - 1
        if idx < 0 or idx >= len(cells):
            continue
        cells[idx] = "↻"
        styles[idx] = style


def _tenacity_markers(start_ts: float, end_ts: float, attempts: int) -> list[float]:
    if attempts <= 1:
        return []
    if end_ts <= start_ts:
        return [end_ts] * (attempts - 1)
    markers: list[float] = []
    for step in range(1, attempts):
        markers.append(start_ts + (end_ts - start_ts) * (step / attempts))
    return markers


def _bar_string(begin: float, end: float, size: float, width: int) -> str:
    if width <= 0:
        return ""
    if size <= 0:
        return " " * width
    begin = max(0.0, min(begin, size))
    end = max(0.0, min(end, size))
    if begin >= end:
        return " " * width
    prefix_complete_eights = int(width * 8 * begin / size)
    prefix_bar_count = prefix_complete_eights // 8
    prefix_eights_count = prefix_complete_eights % 8

    body_complete_eights = int(width * 8 * end / size)
    body_bar_count = body_complete_eights // 8
    body_eights_count = body_complete_eights % 8

    prefix = " " * prefix_bar_count
    if prefix_eights_count:
        prefix += BEGIN_BLOCK_ELEMENTS[prefix_eights_count]

    body = FULL_BLOCK * body_bar_count
    if body_eights_count:
        body += END_BLOCK_ELEMENTS[body_eights_count]

    suffix = " " * (width - len(body))
    bar = prefix + body[len(prefix) :] + suffix
    if len(bar) < width:
        bar += " " * (width - len(bar))
    return bar[:width]


def _interval_bar(
    start_ts: float,
    end_ts: float,
    window_start: float,
    total: float,
    width: int,
) -> str:
    if end_ts <= start_ts or width <= 0:
        return " " * max(width, 0)
    begin = start_ts - window_start
    end = end_ts - window_start
    return _bar_string(begin, end, total, width)


def _overlay_bars(
    active_bar: str,
    warmup_bar: str,
    backlog_bar: str,
    *,
    active_style: Style | None,
    warmup_style: Style | None,
    backlog_style: Style | None,
    running: bool,
    arrow_pad: int = 0,
) -> tuple[list[str], list[Style | None]]:
    width = max(len(active_bar), len(warmup_bar), len(backlog_bar)) + max(arrow_pad, 0)
    cells = [" "] * width
    styles: list[Style | None] = [None] * width
    for idx in range(width):
        if idx < len(active_bar) and active_bar[idx] != " ":
            cells[idx] = active_bar[idx]
            styles[idx] = active_style
            continue
        if idx < len(warmup_bar) and warmup_bar[idx] != " ":
            cells[idx] = warmup_bar[idx]
            styles[idx] = warmup_style
            continue
        if idx < len(backlog_bar) and backlog_bar[idx] != " ":
            cells[idx] = backlog_bar[idx]
            styles[idx] = backlog_style
    if running and arrow_pad > 0:
        last = None
        for idx in range(len(active_bar) - 1, -1, -1):
            if active_bar[idx] != " ":
                last = idx
                break
        if last is not None:
            arrow_idx = min(last + 1, width - 1)
            cells[arrow_idx] = "▸"
            styles[arrow_idx] = active_style
    return cells, styles


def _text_from_cells(cells: list[str], styles: list[Style | None]) -> Text:
    text = Text()
    if not cells:
        return text
    start = 0
    current = styles[0]
    for idx in range(1, len(cells) + 1):
        if idx == len(cells) or styles[idx] != current:
            segment = "".join(cells[start:idx])
            text.append(segment, style=current)
            start = idx
            if idx < len(cells):
                current = styles[idx]
    return text


def _segments_from_bar(
    bar: str, bar_styles: list[Style | None] | None
) -> list[Segment]:
    if not bar:
        return []
    if not bar_styles:
        return [Segment(bar)]
    segments: list[Segment] = []
    def _style_at(idx: int) -> Style | None:
        if bar_styles is None or idx >= len(bar_styles):
            return None
        return bar_styles[idx]

    start = 0
    current = _style_at(0)
    for idx in range(1, len(bar) + 1):
        if idx == len(bar) or _style_at(idx) != current:
            segments.append(Segment(bar[start:idx], current))
            start = idx
            current = _style_at(idx)
    return segments


def _segments_from_label_bar(
    label: str, bar: str, bar_styles: list[Style | None] | None
) -> list[Segment]:
    segments = [Segment(label + " ")]
    segments.extend(_segments_from_bar(bar, bar_styles))
    return segments


class TimelineWidget(Static):
    """Text-only timeline view for recent job execution."""

    def __init__(
        self,
        *,
        window_s: float = 120.0,
        max_jobs: int = 200,
        max_backlog: int = 200,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._window_s = float(window_s)
        self._max_jobs = max_jobs
        self._max_backlog = max_backlog
        self._running_spans: dict[str, JobSpan] = {}
        self._completed_spans: Deque[JobSpan] = deque(maxlen=max_jobs)
        self._backlog_pending: dict[str, Deque[BacklogSpan]] = {}
        self._warmup_by_group: dict[str, WarmupSpan] = {}
        self._retry_pending: dict[str, list[float]] = {}
        self._lane_summary_lines: list[str] = []
        self._show_lane_column: bool | None = None

    def update_lane_summary(
        self,
        pending_by_lane: Mapping[str, int] | None,
        inflight_by_lane: Mapping[str, int] | None,
        lane_targets: Mapping[str, int] | None,
    ) -> None:
        pending_by_lane = pending_by_lane or {}
        inflight_by_lane = inflight_by_lane or {}
        self._lane_summary_lines = _lane_lines(pending_by_lane, inflight_by_lane, lane_targets)
        lanes = _lane_set_from_counts(pending_by_lane, inflight_by_lane)
        self._show_lane_column = _should_show_lanes(lanes)

    def handle_event(self, event: Event) -> None:
        if _is_resubmit_marker(event):
            self._mark_resubmitted(event.job_id)
            return
        if event.type == "job_retrying":
            self._mark_retrying(_retry_job_id(event), event.ts)
            return
        if event.type == "warm_delay_start" and event.group_id:
            self._warmup_by_group[event.group_id] = WarmupSpan(
                group_id=event.group_id,
                start_ts=event.ts,
            )
            return
        if event.type in {"warm_delay_end", "followers_ready"} and event.group_id:
            span = self._warmup_by_group.get(event.group_id)
            if span is not None:
                span.end_ts = event.ts
            return
        if event.type in {"group_done", "group_failed"} and event.group_id:
            self._warmup_by_group.pop(event.group_id, None)
            return
        if event.type in {"job_queued", "job_requeued"} and event.job_id:
            lane = _lane_from_event(event)
            span = BacklogSpan(
                job_id=event.job_id,
                group_id=event.group_id,
                lane=lane,
                kind=event.kind,
                queued_ts=event.ts,
            )
            self._backlog_pending.setdefault(event.job_id, deque()).append(span)
            return
        if event.type in {"job_cancelled", "job_ignored"} and event.job_id:
            self._backlog_pending.pop(event.job_id, None)
            span = self._running_spans.pop(event.job_id, None)
            if span is not None:
                span.end_ts = event.ts
                span.ok = None
                self._completed_spans.append(span)
            return
        if event.type == "job_started" and event.job_id:
            queued_ts: float | None = None
            lane = _lane_from_event(event)
            pending = self._backlog_pending.get(event.job_id)
            if pending:
                span = pending.popleft()
                span.start_ts = event.ts
                queued_ts = span.queued_ts
                if span.lane is not None:
                    lane = span.lane
                if not pending:
                    self._backlog_pending.pop(event.job_id, None)
            warmup_start_ts, warmup_end_ts = self._capture_warmup(
                event.kind, event.group_id, event.ts
            )
            self._running_spans[event.job_id] = JobSpan(
                job_id=event.job_id,
                group_id=event.group_id,
                lane=lane,
                pool=event.pool,
                kind=event.kind,
                start_ts=event.ts,
                queued_ts=queued_ts,
                warmup_start_ts=warmup_start_ts,
                warmup_end_ts=warmup_end_ts,
            )
            pending_markers = self._retry_pending.pop(event.job_id, None)
            if pending_markers:
                self._running_spans[event.job_id].retry_markers.extend(pending_markers)
            return
        if event.type in {"job_succeeded", "job_failed"} and event.job_id:
            lane = _lane_from_event(event)
            span = self._running_spans.pop(event.job_id, None)
            if span is None:
                span = JobSpan(
                    job_id=event.job_id,
                    group_id=event.group_id,
                    lane=lane,
                    pool=event.pool,
                    kind=event.kind,
                    start_ts=event.ts,
                )
            elif lane is not None and span.lane is None:
                span.lane = lane
            span.end_ts = event.ts
            resubmitted = _is_resubmit_failure(event)
            pending = self._retry_pending.pop(event.job_id, None)
            if pending:
                span.retry_markers.extend(pending)
            span.retry_scheduled = span.retry_scheduled or bool(span.retry_markers)
            payload = _payload(event)
            attempts = payload.get("tenacity_attempts")
            if isinstance(attempts, int) and attempts > 1:
                span.tenacity_markers.extend(_tenacity_markers(span.start_ts, span.end_ts, attempts))
            if resubmitted or span.retry_scheduled:
                span.ok = None
            else:
                span.ok = event.type == "job_succeeded"
            self._completed_spans.append(span)

    def _short_group(self, group_id: str | None) -> str:
        if not group_id:
            return "-"
        parts = group_id.split(":")
        if len(parts) <= 2:
            return group_id
        return ":".join(parts[-2:])

    def _fit(self, value: str, width: int, keep_tail: bool = False) -> str:
        if len(value) <= width:
            return value
        if width <= 3:
            return value[:width]
        if keep_tail:
            return "..." + value[-(width - 3) :]
        return value[: width - 3] + "..."

    def _warmup_span(self, kind: str | None, group_id: str | None) -> WarmupSpan | None:
        if not group_id or kind == "primer":
            return None
        return self._warmup_by_group.get(group_id)

    def _capture_warmup(
        self, kind: str | None, group_id: str | None, default_end: float
    ) -> tuple[float | None, float | None]:
        warmup = self._warmup_span(kind, group_id)
        if warmup is None:
            return None, None
        return warmup.start_ts, warmup.end_ts or default_end

    def _mark_resubmitted(self, job_id: str | None) -> None:
        if not job_id:
            return
        span = self._running_spans.get(job_id)
        if span is not None:
            span.ok = None
            return
        for span in reversed(self._completed_spans):
            if span.job_id == job_id:
                span.ok = None
                break

    def _mark_retrying(self, job_id: str | None, ts: float | None) -> None:
        if not job_id:
            return
        if ts is None:
            ts = time.time()
        span = self._running_spans.get(job_id)
        if span is not None:
            span.retry_scheduled = True
            span.ok = None
            span.retry_markers.append(ts)
            return
        for span in reversed(self._completed_spans):
            if span.job_id == job_id:
                span.retry_scheduled = True
                span.ok = None
                span.retry_markers.append(ts)
                return
        self._retry_pending.setdefault(job_id, []).append(ts)

    def _sort_key(self, group_id: str | None, kind: str | None, ts: float) -> tuple[str, str, float]:
        group_key = group_id or "~~~~"
        kind_key = kind or "~~~~"
        return (group_key, kind_key, ts)

    def set_sort_mode(self, mode: str) -> None:
        self._sort_mode = mode

    def tick(self) -> None:
        """Mark view for refresh even without new events."""
        # Compact timeline renders directly from current time, so refresh is enough.
        return

    def _render_bar(self, span: JobSpan, now: float, width: int) -> str:
        window_start = now - self._window_s
        start = max(span.start_ts, window_start)
        end = span.end_ts if span.end_ts is not None else now
        if end < window_start:
            return ""
        total = max(self._window_s, 1e-6)
        bar = _interval_bar(start, end, window_start, total, width)
        if span.end_ts is None:
            last = None
            for idx in range(len(bar) - 1, -1, -1):
                if bar[idx] != " ":
                    last = idx
                    break
            if last is not None:
                bar = f"{bar[:last]}▸{bar[last + 1:]}"
        return bar

    def _render_bar_with_backlog(self, span: JobSpan, now: float, width: int) -> Text:
        window_start = now - self._window_s
        total = max(self._window_s, 1e-6)
        warmup_start_ts = span.warmup_start_ts
        warmup_end_ts = span.warmup_end_ts or span.start_ts
        if warmup_start_ts is not None and warmup_end_ts is not None:
            warmup_bar = _interval_bar(
                warmup_start_ts, warmup_end_ts, window_start, total, width
            )
        else:
            warmup_bar = " " * width

        if span.queued_ts is not None and span.queued_ts < span.start_ts:
            backlog_bar = _interval_bar(
                span.queued_ts, span.start_ts, window_start, total, width
            )
        else:
            backlog_bar = " " * width

        start = max(span.start_ts, window_start)
        end = span.end_ts if span.end_ts is not None else now
        active_bar = _interval_bar(start, end, window_start, total, width)

        if span.ok is False:
            active_style = Style(color="bright_red")
        else:
            active_style = None
        cells, styles = _overlay_bars(
            active_bar,
            warmup_bar,
            backlog_bar,
            active_style=active_style,
            warmup_style=Style(color="grey15"),
            backlog_style=Style(color="grey58"),
            running=span.end_ts is None,
            arrow_pad=1,
        )
        _apply_retry_markers(
            cells,
            styles,
            span.retry_markers,
            window_start=window_start,
            total=total,
            width=width,
            style=Style(color="bright_magenta"),
        )
        _apply_retry_markers(
            cells,
            styles,
            span.tenacity_markers,
            window_start=window_start,
            total=total,
            width=width,
            style=Style(color="bright_cyan"),
        )
        return _text_from_cells(cells, styles)

    def _render_pending_backlog(self, span: BacklogSpan, now: float, width: int) -> Text:
        window_start = now - self._window_s
        total = max(self._window_s, 1e-6)
        backlog_bar = _interval_bar(
            span.queued_ts, now, window_start, total, width
        )

        warmup = self._warmup_span(span.kind, span.group_id)
        if warmup is not None:
            warmup_bar = _interval_bar(
                warmup.start_ts, warmup.end_ts or now, window_start, total, width
            )
        else:
            warmup_bar = " " * width

        cells, styles = _overlay_bars(
            " " * width,
            warmup_bar,
            backlog_bar,
            active_style=None,
            warmup_style=Style(color="grey15"),
            backlog_style=Style(color="grey58"),
            running=False,
            arrow_pad=1,
        )
        return _text_from_cells(cells, styles)

    def render(self) -> Group:
        now = time.time()
        total_width = self.size.width or 80
        pool_w = 6
        kind_w = 6
        dur_w = 6
        min_group_w = 10
        max_group_w = 22

        def _label_len(group_w: int, show_lane: bool, lane_w: int) -> int:
            if show_lane:
                template = (
                    f"{'':<{pool_w}} {'':<{kind_w}} {'':<{lane_w}} "
                    f"{'':<{group_w}} {'':>{dur_w}}  |"
                )
            else:
                template = f"{'':<{pool_w}} {'':<{kind_w}} {'':<{group_w}} {'':>{dur_w}}  |"
            return len(template)
        desired_bar = max(40, int(total_width * 0.7))

        spans = list(self._completed_spans) + list(self._running_spans.values())
        pending_backlog: list[BacklogSpan] = []
        for queue in self._backlog_pending.values():
            pending_backlog.extend(queue)
        backlog_spans = pending_backlog

        show_lane_column = self._show_lane_column
        if show_lane_column is None:
            lanes = {_normalize_lane(span.lane) for span in spans}
            lanes.update(_normalize_lane(span.lane) for span in backlog_spans)
            show_lane_column = _should_show_lanes(lanes)
        lane_w = 10 if show_lane_column else 0

        label_min = _label_len(min_group_w, show_lane_column, lane_w)
        if total_width - desired_bar < label_min:
            desired_bar = max(20, total_width - label_min)
        bar_width = max(20, desired_bar)
        base_fixed = _label_len(0, show_lane_column, lane_w)
        group_w = total_width - bar_width - base_fixed
        group_w = max(min_group_w, min(max_group_w, group_w))
        label_width = _label_len(group_w, show_lane_column, lane_w)
        bar_width = max(20, total_width - label_width)
        arrow_pad = 1
        bar_width = max(1, bar_width - arrow_pad)
        max_lines = max(5, (self.size.height or 24) - 6)
        window_start = now - self._window_s
        spans = [
            span
            for span in spans
            if span.start_ts >= window_start or (span.end_ts and span.end_ts >= window_start)
        ]
        if getattr(self, "_sort_mode", "time") == "time":
            spans.sort(key=lambda s: s.start_ts)
        else:
            spans.sort(key=lambda s: self._sort_key(s.group_id, s.kind, s.start_ts))
        spans = spans[-max_lines:]

        if getattr(self, "_sort_mode", "time") == "time":
            backlog_spans.sort(key=lambda s: s.queued_ts)
        else:
            backlog_spans.sort(key=lambda s: self._sort_key(s.group_id, s.kind, s.queued_ts))

        lines: list[Text] = [
            Text("Job Timeline", style="bold"),
            Text(f"Window: last {int(self._window_s)}s  |  now={now:.0f}"),
            Text("Press 'T' for dashboard; 't' for roadmap timeline; 's' to sort."),
            Text(""),
        ]

        if not spans and not backlog_spans:
            lines.append(Text("No jobs in window."))
            return Group(*lines)

        if show_lane_column:
            header = (
                f"{'pool':<{pool_w}} {'kind':<{kind_w}} {'lane':<{lane_w}} "
                f"{'group':<{group_w}} {'dur(s)':>{dur_w}}  timeline"
            )
        else:
            header = (
                f"{'pool':<{pool_w}} {'kind':<{kind_w}} {'group':<{group_w}} {'dur(s)':>{dur_w}}  timeline"
            )
        lines.append(Text(header))
        for span in spans:
            bar = self._render_bar_with_backlog(span, now, bar_width)
            if not bar.plain.strip():
                continue
            pool = span.pool or "-"
            kind = self._fit(span.kind or "-", kind_w)
            lane = _normalize_lane(span.lane)
            lane_label = self._fit(lane, lane_w) if show_lane_column else ""
            group = self._fit(self._short_group(span.group_id), group_w, keep_tail=True)
            dur = span.duration
            if show_lane_column:
                label = (
                    f"{pool:<{pool_w}} {kind:<{kind_w}} {lane_label:<{lane_w}} "
                    f"{group:<{group_w}} {dur:>{dur_w}.1f}  |"
                )
            else:
                label = (
                    f"{pool:<{pool_w}} {kind:<{kind_w}} {group:<{group_w}} {dur:>{dur_w}.1f}  |"
                )
            line = Text(label)
            line.append_text(bar)
            line.append("|")
            lines.append(line)

        if backlog_spans:
            lines.append(Text(""))
            lines.append(Text("Backlog (queued -> started)"))
            for span in backlog_spans[-self._max_backlog :]:
                end_ts = now
                dur = max(0.0, end_ts - span.queued_ts)
                bar = self._render_pending_backlog(span, now, bar_width)
                pool = "queue"
                kind = span.kind or "-"
                lane = _normalize_lane(span.lane)
                lane_label = self._fit(lane, lane_w) if show_lane_column else ""
                group = self._fit(self._short_group(span.group_id), group_w, keep_tail=True)
                if show_lane_column:
                    label = (
                        f"{pool:<{pool_w}} {kind:<{kind_w}} {lane_label:<{lane_w}} "
                        f"{group:<{group_w}} {dur:>{dur_w}.1f}  |"
                    )
                else:
                    label = (
                        f"{pool:<{pool_w}} {kind:<{kind_w}} {group:<{group_w}} {dur:>{dur_w}.1f}  |"
                    )
                line = Text(label)
                line.append_text(bar)
                line.append("|")
                lines.append(line)

        return Group(*lines)


class TimelineRoadmapWidget(ScrollView):
    """Roadmap-style timeline with scrollable axes."""

    def __init__(
        self,
        *,
        seconds_per_col: float = 1.0,
        max_columns: int = 2400,
        max_jobs: int = 500,
        max_backlog: int = 300,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._seconds_per_col = float(seconds_per_col)
        self._max_columns = max_columns
        self._max_jobs = max_jobs
        self._max_backlog = max_backlog
        self._running_spans: dict[str, JobSpan] = {}
        self._completed_spans: Deque[JobSpan] = deque(maxlen=max_jobs)
        self._backlog_pending: dict[str, Deque[BacklogSpan]] = {}
        self._warmup_by_group: dict[str, WarmupSpan] = {}
        self._retry_pending: dict[str, list[float]] = {}
        self._dirty = True
        self._lane_summary_lines: list[str] = []
        self._show_lane_tags: bool | None = None
        self._content_width = 0
        self._content_height = 0
        self._layout_prefix: list[str] = []
        self._layout_spans: list[JobSpan] = []
        self._layout_backlog: list[BacklogSpan] = []
        self._layout_window_line = ""
        self._layout_axis_labels = ""
        self._layout_axis_ticks = ""
        self._layout_header = ""
        self._layout_label_width = 0
        self._layout_pool_w = 0
        self._layout_kind_w = 0
        self._layout_group_w = 0
        self._layout_job_w = 0
        self._layout_pool_w = 0
        self._layout_kind_w = 0
        self._layout_group_w = 0
        self._layout_job_w = 0
        self._layout_columns = 0
        self._layout_seconds_per_col = 0.0
        self._layout_arrow_pad = 1
        self._layout_window_start = 0.0
        self._layout_window_end = 0.0
        self._layout_span_range = 0.0
        self._line_cache: dict[str, tuple[tuple, list[Segment]]] = {}
        self._layout_key: tuple[float, float, int, int, int, int, int, int] | None = None
        self._group_color_map: dict[str, str] = {}
        self._palette = [
            "#4E79A7",
            "#F28E2B",
            "#B92123",
            "#76B7B2",
            "#59A14F",
            "#EDC948",
            "#B07AA1",
            "#FF9DA7",
            "#6D5242",
            "#06D6A0",
            "#1B9E77",
            "#D95F02",
            "#7570B3",
            "#E7298A",
            "#66A61E",
            "#E6AB02",
            "#A6761D",
            "#118AB2",
            "#1F77B4",
            "#BC5800",
            "#2CA02C",
            "#D62728",
            "#683E8F",
            "#8C564B",
            "#E377C2",
            "#C8113D",
            "#BCBD22",
            "#108591",
            "#00A6ED",
            "#7FB800",
            "#F6511D",
            "#B27E00",
            "#00BFA0",
            "#9B5DE5",
            "#F15BB5",
            "#DDBF01",
            "#0083AE",
            "#00AB94",
            "#5B8FF9",
            "#61DDAA",
            "#65789B",
            "#F6BD16",
            "#7262FD",
            "#0FB1F3",
            "#9661BC",
            "#CD610A",
        ]
        self._fail_color = "bright_red"

    def update_lane_summary(
        self,
        pending_by_lane: Mapping[str, int] | None,
        inflight_by_lane: Mapping[str, int] | None,
        lane_targets: Mapping[str, int] | None,
    ) -> None:
        pending_by_lane = pending_by_lane or {}
        inflight_by_lane = inflight_by_lane or {}
        lane_summary_lines = _lane_lines(pending_by_lane, inflight_by_lane, lane_targets)
        lanes = _lane_set_from_counts(pending_by_lane, inflight_by_lane)
        show_lane_tags = _should_show_lanes(lanes)
        if lane_summary_lines != self._lane_summary_lines or show_lane_tags != self._show_lane_tags:
            self._dirty = True
        self._lane_summary_lines = lane_summary_lines
        self._show_lane_tags = show_lane_tags

    def handle_event(self, event: Event) -> None:
        if _is_resubmit_marker(event):
            self._mark_resubmitted(event.job_id)
            self._dirty = True
            return
        if event.type == "job_retrying":
            self._mark_retrying(_retry_job_id(event), event.ts)
            self._dirty = True
            return
        if event.type == "warm_delay_start" and event.group_id:
            self._warmup_by_group[event.group_id] = WarmupSpan(
                group_id=event.group_id,
                start_ts=event.ts,
            )
            self._dirty = True
            return
        if event.type in {"warm_delay_end", "followers_ready"} and event.group_id:
            span = self._warmup_by_group.get(event.group_id)
            if span is not None:
                span.end_ts = event.ts
                self._dirty = True
            return
        if event.type in {"group_done", "group_failed"} and event.group_id:
            self._warmup_by_group.pop(event.group_id, None)
            self._dirty = True
            return
        if event.type in {"job_queued", "job_requeued"} and event.job_id:
            lane = _lane_from_event(event)
            span = BacklogSpan(
                job_id=event.job_id,
                group_id=event.group_id,
                lane=lane,
                kind=event.kind,
                queued_ts=event.ts,
            )
            self._backlog_pending.setdefault(event.job_id, deque()).append(span)
            self._dirty = True
            return
        if event.type in {"job_cancelled", "job_ignored"} and event.job_id:
            self._backlog_pending.pop(event.job_id, None)
            span = self._running_spans.pop(event.job_id, None)
            if span is not None:
                span.end_ts = event.ts
                span.ok = None
                self._completed_spans.append(span)
            self._dirty = True
            return
        if event.type == "job_started" and event.job_id:
            queued_ts: float | None = None
            lane = _lane_from_event(event)
            pending = self._backlog_pending.get(event.job_id)
            if pending:
                span = pending.popleft()
                span.start_ts = event.ts
                queued_ts = span.queued_ts
                if span.lane is not None:
                    lane = span.lane
                if not pending:
                    self._backlog_pending.pop(event.job_id, None)
            warmup_start_ts, warmup_end_ts = self._capture_warmup(
                event.kind, event.group_id, event.ts
            )
            self._running_spans[event.job_id] = JobSpan(
                job_id=event.job_id,
                group_id=event.group_id,
                lane=lane,
                pool=event.pool,
                kind=event.kind,
                start_ts=event.ts,
                queued_ts=queued_ts,
                warmup_start_ts=warmup_start_ts,
                warmup_end_ts=warmup_end_ts,
            )
            pending_markers = self._retry_pending.pop(event.job_id, None)
            if pending_markers:
                self._running_spans[event.job_id].retry_markers.extend(pending_markers)
            self._dirty = True
            return
        if event.type in {"job_succeeded", "job_failed"} and event.job_id:
            lane = _lane_from_event(event)
            span = self._running_spans.pop(event.job_id, None)
            if span is None:
                span = JobSpan(
                    job_id=event.job_id,
                    group_id=event.group_id,
                    lane=lane,
                    pool=event.pool,
                    kind=event.kind,
                    start_ts=event.ts,
                )
            elif lane is not None and span.lane is None:
                span.lane = lane
            span.end_ts = event.ts
            resubmitted = _is_resubmit_failure(event)
            pending = self._retry_pending.pop(event.job_id, None)
            if pending:
                span.retry_markers.extend(pending)
            span.retry_scheduled = span.retry_scheduled or bool(span.retry_markers)
            payload = _payload(event)
            attempts = payload.get("tenacity_attempts")
            if isinstance(attempts, int) and attempts > 1:
                span.tenacity_markers.extend(_tenacity_markers(span.start_ts, span.end_ts, attempts))
            if resubmitted or span.retry_scheduled:
                span.ok = None
            else:
                span.ok = event.type == "job_succeeded"
            self._completed_spans.append(span)
        self._dirty = True

    def _short_group(self, group_id: str | None) -> str:
        if not group_id:
            return "-"
        parts = group_id.split(":")
        if len(parts) <= 2:
            return group_id
        return ":".join(parts[-2:])

    def _fit(self, value: str, width: int, keep_tail: bool = False) -> str:
        if len(value) <= width:
            return value
        if width <= 3:
            return value[:width]
        if keep_tail:
            return "..." + value[-(width - 3) :]
        return value[: width - 3] + "..."

    def _group_color(self, group_id: str | None) -> str:
        if not group_id:
            return "white"
        mapped = self._group_color_map.get(group_id)
        if mapped is not None:
            return mapped
        digest = hashlib.sha1(group_id.encode("utf-8")).hexdigest()
        idx = int(digest[:8], 16) % len(self._palette)
        return self._palette[idx]

    def _build_group_color_map(self, spans: list[JobSpan]) -> dict[str, str]:
        if not spans:
            return {}
        color_map: dict[str, str] = {}
        last_color: str | None = None
        for span in spans:
            group_id = span.group_id
            if not group_id or group_id in color_map:
                continue
            digest = hashlib.sha1(group_id.encode("utf-8")).hexdigest()
            idx = int(digest[:8], 16) % len(self._palette)
            color = self._palette[idx]
            if last_color is not None and color == last_color:
                for offset in range(1, len(self._palette)):
                    candidate = self._palette[(idx + offset) % len(self._palette)]
                    if candidate != last_color:
                        color = candidate
                        break
            color_map[group_id] = color
            last_color = color
        return color_map

    def _warmup_span(self, kind: str | None, group_id: str | None) -> WarmupSpan | None:
        if not group_id or kind == "primer":
            return None
        return self._warmup_by_group.get(group_id)

    def _capture_warmup(
        self, kind: str | None, group_id: str | None, default_end: float
    ) -> tuple[float | None, float | None]:
        warmup = self._warmup_span(kind, group_id)
        if warmup is None:
            return None, None
        return warmup.start_ts, warmup.end_ts or default_end

    def _mark_resubmitted(self, job_id: str | None) -> None:
        if not job_id:
            return
        span = self._running_spans.get(job_id)
        if span is not None:
            span.ok = None
            return
        for span in reversed(self._completed_spans):
            if span.job_id == job_id:
                span.ok = None
                break

    def _mark_retrying(self, job_id: str | None, ts: float | None) -> None:
        if not job_id:
            return
        if ts is None:
            ts = time.time()
        span = self._running_spans.get(job_id)
        if span is not None:
            span.retry_scheduled = True
            span.ok = None
            span.retry_markers.append(ts)
            return
        for span in reversed(self._completed_spans):
            if span.job_id == job_id:
                span.retry_scheduled = True
                span.ok = None
                span.retry_markers.append(ts)
                return
        self._retry_pending.setdefault(job_id, []).append(ts)

    def _sort_key(self, group_id: str | None, kind: str | None, ts: float) -> tuple[str, str, float]:
        group_key = group_id or "~~~~"
        kind_key = kind or "~~~~"
        return (group_key, kind_key, ts)

    def set_sort_mode(self, mode: str) -> None:
        self._sort_mode = mode

    def tick(self) -> None:
        """Mark view dirty when time-dependent spans should advance."""
        if self._running_spans:
            self._dirty = True
            return
        if any(self._backlog_pending.values()):
            self._dirty = True

    def _compute_window(self, spans: list[JobSpan], now: float) -> tuple[float, float]:
        if not spans:
            return now - 1.0, now
        start = min(span.start_ts for span in spans)
        end = max((span.end_ts or now) for span in spans)
        if end <= start:
            end = start + 1.0
        return start, end

    def _compute_scale(self, span_range: float) -> tuple[float, int]:
        seconds_per_col = max(self._seconds_per_col, 1e-3)
        columns = max(10, int(math.ceil(span_range / seconds_per_col)))
        return seconds_per_col, columns

    def _render_axis(self, columns: int, seconds_per_col: float) -> tuple[str, str]:
        tick_every_s = 5.0
        tick_every_cols = max(1, int(tick_every_s / seconds_per_col))
        ticks = ["─"] * columns
        labels = [" "] * columns
        for col in range(0, columns, tick_every_cols):
            ticks[col] = "│"
            label = str(int(col * seconds_per_col))
            for idx, ch in enumerate(label):
                if col + idx < columns:
                    labels[col + idx] = ch
        return "".join(labels), "".join(ticks)

    def _render_bar(
        self, span: JobSpan, start_ts: float, seconds_per_col: float, columns: int, now: float
    ) -> str:
        end_ts = span.end_ts or now
        total = seconds_per_col * columns
        bar = _interval_bar(span.start_ts, end_ts, start_ts, total, columns)
        if span.end_ts is None:
            last = None
            for idx in range(len(bar) - 1, -1, -1):
                if bar[idx] != " ":
                    last = idx
                    break
            if last is not None:
                bar = f"{bar[:last]}▸{bar[last + 1:]}"
        return bar

    def _render_bar_with_styles(
        self,
        span: JobSpan,
        window_start: float,
        seconds_per_col: float,
        columns: int,
        now: float,
    ) -> tuple[str, list[Style | None]]:
        total = seconds_per_col * columns
        backlog_style = Style(color="grey58")
        group_color = self._group_color(span.group_id)
        if span.ok is False:
            bar_style = Style(color=self._fail_color)
        else:
            bar_style = Style(color=group_color)
        warmup_style = Style(color="grey15")

        warmup_start_ts = span.warmup_start_ts
        warmup_end_ts = span.warmup_end_ts or span.start_ts
        if warmup_start_ts is not None and warmup_end_ts is not None:
            warm_start = max(warmup_start_ts, window_start)
            warm_end = min(warmup_end_ts, span.start_ts)
            if warm_end > warm_start:
                warmup_bar = _interval_bar(
                    warm_start, warm_end, window_start, total, columns
                )
            else:
                warmup_bar = " " * columns
        else:
            warmup_bar = " " * columns

        if span.queued_ts is not None and span.queued_ts < span.start_ts:
            backlog_start = max(span.queued_ts, window_start)
            backlog_end = min(span.start_ts, now)
            if backlog_end >= window_start:
                backlog_bar = _interval_bar(
                    backlog_start, backlog_end, window_start, total, columns
                )
            else:
                backlog_bar = " " * columns
        else:
            backlog_bar = " " * columns

        start = max(span.start_ts, window_start)
        end_ts = span.end_ts or now
        active_bar = _interval_bar(start, end_ts, window_start, total, columns)

        cells, styles = _overlay_bars(
            active_bar,
            warmup_bar,
            backlog_bar,
            active_style=bar_style,
            warmup_style=warmup_style,
            backlog_style=backlog_style,
            running=span.end_ts is None,
            arrow_pad=1,
        )
        _apply_retry_markers(
            cells,
            styles,
            span.retry_markers,
            window_start=window_start,
            total=total,
            width=columns,
            style=Style(color="bright_magenta"),
        )
        _apply_retry_markers(
            cells,
            styles,
            span.tenacity_markers,
            window_start=window_start,
            total=total,
            width=columns,
            style=Style(color="bright_cyan"),
        )
        if span.ok is False:
            bg_color = self.rich_style.bgcolor or "default"
            bar_color = bar_style.color
            if bar_color is not None:
                neg_style = Style(color=bg_color, bgcolor=bar_color)
                limit = min(len(active_bar), len(cells))
                for idx in range(limit):
                    if active_bar[idx] == FULL_BLOCK:
                        cells[idx] = "x"
                        styles[idx] = neg_style
        return "".join(cells), styles

    def _line_cache_key(
        self, span: JobSpan, layout_key: tuple[float, float, int, int, int, int, int, int]
    ) -> tuple:
        return (
            layout_key,
            self._show_lane_tags,
            span.start_ts,
            span.end_ts,
            span.ok,
            span.retry_scheduled,
            tuple(span.retry_markers),
            tuple(span.tenacity_markers),
            span.queued_ts,
            span.warmup_start_ts,
            span.warmup_end_ts,
            span.pool,
            span.kind,
            span.lane,
            span.group_id,
            span.job_id,
        )

    def _render_backlog_bar(
        self,
        span: BacklogSpan,
        window_start: float,
        seconds_per_col: float,
        columns: int,
        now: float,
    ) -> tuple[str, list[Style | None]]:
        total = seconds_per_col * columns
        backlog_style = Style(color="grey58")
        start = max(span.queued_ts, window_start)
        end_ts = now
        if end_ts >= window_start:
            backlog_bar = _interval_bar(start, end_ts, window_start, total, columns)
        else:
            backlog_bar = " " * columns
        warmup = self._warmup_span(span.kind, span.group_id)
        if warmup is not None:
            warm_start = max(warmup.start_ts, start)
            warm_end = min(warmup.end_ts or now, end_ts)
            if warm_end > warm_start:
                warmup_bar = _interval_bar(
                    warm_start, warm_end, window_start, total, columns
                )
            else:
                warmup_bar = " " * columns
        else:
            warmup_bar = " " * columns
        cells, styles = _overlay_bars(
            " " * columns,
            warmup_bar,
            backlog_bar,
            active_style=None,
            warmup_style=Style(color="grey15"),
            backlog_style=backlog_style,
            running=False,
            arrow_pad=1,
        )
        return "".join(cells), styles

    def _build_lines(self) -> None:
        now = time.time()
        spans = list(self._completed_spans) + list(self._running_spans.values())
        if getattr(self, "_sort_mode", "time") == "time":
            spans.sort(key=lambda s: s.start_ts)
        else:
            spans.sort(key=lambda s: self._sort_key(s.group_id, s.kind, s.start_ts))
        spans = spans[-self._max_jobs :]

        pending_backlog: list[BacklogSpan] = []
        for queue in self._backlog_pending.values():
            pending_backlog.extend(queue)
        backlog_spans = pending_backlog
        if getattr(self, "_sort_mode", "time") == "time":
            backlog_spans.sort(key=lambda s: s.queued_ts)
        else:
            backlog_spans.sort(key=lambda s: self._sort_key(s.group_id, s.kind, s.queued_ts))

        prefix_lines = [
            "Job Timeline (Roadmap)",
            "Press 't' for dashboard; 'T' for compact timeline; 's' to sort.",
            "",
        ]
        if self._lane_summary_lines:
            prefix_lines.append("Lane Summary")
            prefix_lines.extend(self._lane_summary_lines)
            prefix_lines.append("")
        if not spans:
            prefix_lines.append("No active jobs yet.")
            if not backlog_spans:
                self._layout_prefix = prefix_lines
                self._layout_spans = []
                self._layout_backlog = []
                self._layout_window_line = ""
                self._layout_axis_labels = ""
                self._layout_axis_ticks = ""
                self._layout_header = ""
                self._layout_label_width = 0
                self._layout_columns = 0
                self._layout_seconds_per_col = 0.0
                self._layout_arrow_pad = 1
                self._layout_window_start = 0.0
                self._layout_window_end = 0.0
                self._layout_span_range = 0.0
                self._content_width = max(len(line) for line in prefix_lines)
                self._content_height = len(prefix_lines)
                self.virtual_size = Size(self._content_width, self._content_height)
                self._dirty = False
                return

        show_lane_tags = self._show_lane_tags
        if show_lane_tags is None:
            lanes = {_normalize_lane(span.lane) for span in spans}
            lanes.update(_normalize_lane(span.lane) for span in backlog_spans)
            show_lane_tags = _should_show_lanes(lanes)

        window_start, window_end = self._compute_window(spans, now)
        if backlog_spans:
            backlog_start = min(span.queued_ts for span in backlog_spans)
            backlog_end = max((span.start_ts or now) for span in backlog_spans)
            window_start = min(window_start, backlog_start)
            window_end = max(window_end, backlog_end)
        span_range = max(1e-3, window_end - window_start)
        seconds_per_col, columns = self._compute_scale(span_range)
        arrow_pad = 1
        if columns > self._max_columns:
            columns = self._max_columns

        pool_w = 8
        kind_w = 8
        group_w = 26
        job_w = 8
        header = f"{'pool':<{pool_w}} {'kind':<{kind_w}} {'group':<{group_w}} {'job':<{job_w}}"
        label_width = len(header)
        layout_key = (
            window_start,
            seconds_per_col,
            columns,
            pool_w,
            kind_w,
            group_w,
            job_w,
            arrow_pad,
        )
        if layout_key != self._layout_key:
            self._layout_key = layout_key
            self._line_cache.clear()

        axis_labels, axis_ticks = self._render_axis(columns, seconds_per_col)
        if arrow_pad:
            axis_labels += " " * arrow_pad
            axis_ticks += " " * arrow_pad

        window_line = f"Window: {window_start:.0f}..{window_end:.0f}  |  span={span_range:.1f}s"
        axis_label_line = f"{'':<{label_width}} {axis_labels}"
        axis_tick_line = f"{'':<{label_width}} {axis_ticks}"
        header_line = f"{header} {' ' * (columns + arrow_pad)}"

        self._layout_prefix = prefix_lines
        self._layout_spans = spans
        self._layout_backlog = backlog_spans[-self._max_backlog :]
        self._group_color_map = self._build_group_color_map(spans)
        self._layout_window_line = window_line
        self._layout_axis_labels = axis_labels
        self._layout_axis_ticks = axis_ticks
        self._layout_header = header
        self._layout_label_width = label_width
        self._layout_pool_w = pool_w
        self._layout_kind_w = kind_w
        self._layout_group_w = group_w
        self._layout_job_w = job_w
        self._layout_columns = columns
        self._layout_seconds_per_col = seconds_per_col
        self._layout_arrow_pad = arrow_pad
        self._layout_window_start = window_start
        self._layout_window_end = window_end
        self._layout_span_range = span_range
        self._show_lane_tags = show_lane_tags

        content_height = len(prefix_lines) + 4 + len(spans)
        if backlog_spans:
            content_height += 2 + len(self._layout_backlog)
        content_width = max(
            label_width + 1 + columns + arrow_pad,
            len(window_line),
            len(axis_label_line),
            len(axis_tick_line),
            len(header_line),
            max(len(line) for line in prefix_lines),
            len("Backlog (queued -> started)"),
        )
        self._content_width = content_width
        self._content_height = content_height
        self.virtual_size = Size(self._content_width, self._content_height)
        self._dirty = False

    def render_line(self, y: int) -> Strip:
        if self._dirty:
            self._build_lines()
        content_y = y + int(self.scroll_offset.y)
        if content_y < 0 or content_y >= self._content_height:
            return Strip.blank(self.size.width, self.rich_style)

        def _strip_from_segments(segments: list[Segment]) -> Strip:
            line_length = Segment.get_line_length(segments)
            if line_length < self._content_width:
                segments = Segment.adjust_line_length(segments, self._content_width)
            width = self.scrollable_content_region.width
            start = int(self.scroll_offset.x)
            strip = Strip(segments).crop(start, start + width)
            return strip.apply_style(self.rich_style)

        def _to_strip(line: str) -> Strip:
            return _strip_from_segments([Segment(line)])

        prefix_len = len(self._layout_prefix)
        if content_y < prefix_len:
            return _to_strip(self._layout_prefix[content_y])

        content_y -= prefix_len
        if content_y == 0:
            return _to_strip(self._layout_window_line)
        if content_y == 1:
            return _to_strip(f"{'':<{self._layout_label_width}} {self._layout_axis_labels}")
        if content_y == 2:
            return _to_strip(f"{'':<{self._layout_label_width}} {self._layout_axis_ticks}")
        if content_y == 3:
            return _to_strip(
                f"{self._layout_header} {' ' * (self._layout_columns + self._layout_arrow_pad)}"
            )

        content_y -= 4
        now = time.time()

        if content_y < len(self._layout_spans):
            span = self._layout_spans[content_y]
            pool = self._fit(span.pool or "-", self._layout_pool_w)
            kind = self._fit(span.kind or "-", self._layout_kind_w)
            group_label = self._short_group(span.group_id)
            if self._show_lane_tags:
                lane_tag = _truncate_label(_normalize_lane(span.lane), 6)
                group_label = f"{lane_tag}|{group_label}"
            group = self._fit(group_label, self._layout_group_w, keep_tail=True)
            job = self._fit(span.job_id[-self._layout_job_w:] if span.job_id else "-", self._layout_job_w)
            label = (
                f"{pool:<{self._layout_pool_w}} {kind:<{self._layout_kind_w}} "
                f"{group:<{self._layout_group_w}} {job:<{self._layout_job_w}}"
            )
            if span.end_ts is not None:
                cache_key = self._line_cache_key(span, self._layout_key or (0, 0, 0, 0, 0, 0, 0, 0))
                cached = self._line_cache.get(span.job_id)
                if cached and cached[0] == cache_key:
                    segments = cached[1]
                else:
                    bar, bar_styles = self._render_bar_with_styles(
                        span,
                        self._layout_window_start,
                        self._layout_seconds_per_col,
                        self._layout_columns,
                        now,
                    )
                    segments = _segments_from_label_bar(label, bar, bar_styles)
                    self._line_cache[span.job_id] = (cache_key, segments)
            else:
                bar, bar_styles = self._render_bar_with_styles(
                    span,
                    self._layout_window_start,
                    self._layout_seconds_per_col,
                    self._layout_columns,
                    now,
                )
                segments = _segments_from_label_bar(label, bar, bar_styles)
            return _strip_from_segments(segments)

        content_y -= len(self._layout_spans)
        if self._layout_backlog:
            if content_y == 0:
                return _to_strip("")
            if content_y == 1:
                return _to_strip("Backlog (queued -> started)")
            content_y -= 2
            if content_y < len(self._layout_backlog):
                span = self._layout_backlog[content_y]
                pool = self._fit("queue", self._layout_pool_w)
                kind = self._fit(span.kind or "-", self._layout_kind_w)
                group_label = self._short_group(span.group_id)
                if self._show_lane_tags:
                    lane_tag = _truncate_label(_normalize_lane(span.lane), 6)
                    group_label = f"{lane_tag}|{group_label}"
                group = self._fit(group_label, self._layout_group_w, keep_tail=True)
                job = self._fit(span.job_id[-self._layout_job_w:] if span.job_id else "-", self._layout_job_w)
                label = (
                    f"{pool:<{self._layout_pool_w}} {kind:<{self._layout_kind_w}} "
                    f"{group:<{self._layout_group_w}} {job:<{self._layout_job_w}}"
                )
                bar, bar_styles = self._render_backlog_bar(
                    span,
                    self._layout_window_start,
                    self._layout_seconds_per_col,
                    self._layout_columns,
                    now,
                )
                segments = _segments_from_label_bar(label, bar, bar_styles)
                return _strip_from_segments(segments)

        return Strip.blank(self.size.width, self.rich_style)
