"""Burst mode indicator widget."""

from __future__ import annotations

from textual.reactive import reactive
from textual.widgets import Static


class BurstIndicator(Static):
    """Text-based burst mode status indicator with admission pressure bar."""

    burst_active: reactive[bool] = reactive(False)
    low_watermark: reactive[int] = reactive(0)
    cap: reactive[int] = reactive(0)
    runnable: reactive[int] = reactive(0)
    inflight: reactive[int] = reactive(0)

    def render(self) -> str:
        if self.burst_active:
            status = "[bold green]BURST ON[/]"
        else:
            status = "[dim]BURST OFF[/]"

        bar_width = 20
        if self.cap > 0:
            ratio = min(1.0, self.inflight / self.cap)
            filled = int(ratio * bar_width)
            bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"
            pressure_line = f"Admission: {bar} {self.inflight}/{self.cap}"
        else:
            bar = "[" + "-" * bar_width + "]"
            pressure_line = f"Admission: {bar} {self.inflight}/--"

        lines = [
            f"Burst: {status}",
            pressure_line,
            f"Queue: {self.runnable}",
        ]
        return "\n".join(lines)

    def update_burst(
        self,
        *,
        active: bool,
        low_watermark: int,
        cap: int = 0,
        runnable: int = 0,
        inflight: int = 0,
    ) -> None:
        self.burst_active = active
        self.low_watermark = low_watermark
        self.cap = cap
        self.runnable = runnable
        self.inflight = inflight
