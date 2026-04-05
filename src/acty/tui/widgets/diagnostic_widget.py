"""Multi-view diagnostic widget for runtime system diagnostics."""

from __future__ import annotations

from enum import Enum, auto
from typing import Any

from textual.reactive import reactive
from textual.widgets import Static

from acty_core.events.snapshot import PoolStats


class DiagnosticView(Enum):
    """Available diagnostic view modes."""

    RUNNABLE = auto()
    BURST = auto()
    BOTTLENECK = auto()

    def next(self) -> "DiagnosticView":
        views = list(DiagnosticView)
        idx = views.index(self)
        return views[(idx + 1) % len(views)]

    @property
    def label(self) -> str:
        return {
            DiagnosticView.RUNNABLE: "Queue vs Workers",
            DiagnosticView.BURST: "Burst/Watermark",
            DiagnosticView.BOTTLENECK: "Bottlenecks",
        }[self]


class DiagnosticWidget(Static):
    """Scrollable diagnostic widget with 3 switchable views."""

    view: reactive[DiagnosticView] = reactive(DiagnosticView.RUNNABLE)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._admission_state: dict[str, int] = {
            "cap": 0,
            "base_cap": 0,
            "burst_allowance": 0,
            "low_watermark": 0,
            "runnable": 0,
            "pending": 0,
        }
        self._runnable_state: dict[str, Any] = {
            "queue_depth": 0,
            "inflight": 0,
            "runnable_by_kind": {},
            "inflight_by_kind": {},
        }
        self._pool_stats: dict[str, PoolStats] = {}
        self._watermark_state: dict[str, Any] = {
            "runnable": 0,
            "low_watermark": 0,
            "burst_active": False,
            "warm_delay_count": 0,
        }

    def on_mount(self) -> None:
        self._update_border()

    def cycle_view(self) -> None:
        self.view = self.view.next()
        self._update_border()
        self.refresh()

    def _update_border(self) -> None:
        self.border_title = "Diagnostics"
        self.border_subtitle = f"({self.view.label}) press b to switch"

    def update_data(
        self,
        *,
        admission_state: dict[str, int] | None = None,
        runnable_state: dict[str, Any] | None = None,
        pool_stats: dict[str, PoolStats] | None = None,
        watermark_state: dict[str, Any] | None = None,
    ) -> None:
        if admission_state is not None:
            self._admission_state = admission_state
        if runnable_state is not None:
            self._runnable_state = runnable_state
        if pool_stats is not None:
            self._pool_stats = pool_stats
        if watermark_state is not None:
            self._watermark_state = watermark_state
        self.refresh()

    def render(self) -> str:
        if self.view == DiagnosticView.RUNNABLE:
            return self._render_runnable()
        if self.view == DiagnosticView.BOTTLENECK:
            return self._render_bottleneck()
        return self._render_burst()

    def _render_runnable(self) -> str:
        r = self._runnable_state
        queue_depth = r["queue_depth"]
        inflight = r["inflight"]
        total = queue_depth + inflight or 1

        def bar(count: int, width: int = 25) -> str:
            filled = int((count / total) * width) if total > 0 else 0
            return "#" * filled + "." * (width - filled)

        lines = [
            "[bold]Queue vs Execution[/bold]",
            "",
            f"[yellow]Queued[/yellow]    ({bar(queue_depth)}) {queue_depth}",
            f"[green]Inflight[/green]   ({bar(inflight)}) {inflight}",
            "",
            "[dim]By Kind:[/dim]",
        ]

        all_kinds = set(r["runnable_by_kind"].keys()) | set(r["inflight_by_kind"].keys())
        for kind in sorted(all_kinds):
            queued_k = r["runnable_by_kind"].get(kind, 0)
            inflight_k = r["inflight_by_kind"].get(kind, 0)
            lines.append(f"  {kind:12} Q={queued_k:3} I={inflight_k:3}")
        return "\n".join(lines)

    def _render_bottleneck(self) -> str:
        pool_stats = self._pool_stats
        runnable = self._runnable_state

        lines = [
            "[bold]Bottleneck Analysis[/bold]",
            "",
        ]

        total_idle = sum(p.idle_workers for p in pool_stats.values())
        total_workers = sum(p.total_workers for p in pool_stats.values())
        queue_depth = runnable["queue_depth"]

        if total_idle == 0:
            lines.append("[green]All workers busy[/green]")
        elif queue_depth == 0:
            lines.append(f"[yellow]Idle workers: {total_idle}/{total_workers}[/yellow]")
            lines.append("[dim]Cause: No runnable jobs[/dim]")
        else:
            lines.append(f"[red]Idle workers: {total_idle}/{total_workers}[/red]")
            lines.append(f"[yellow]Runnable jobs: {queue_depth}[/yellow]")
            lines.append("")
            lines.append("[dim]Possible causes:[/dim]")
            lines.append("  - Work stealing mismatch")
            lines.append("  - Burst cap limit")
        return "\n".join(lines)

    def _render_burst(self) -> str:
        a = self._admission_state
        w = self._watermark_state
        lines = [
            "[bold]Burst / Watermark[/bold]",
            "",
            f"Runnable:         [yellow]{a['runnable']}[/yellow]",
            f"Pending groups:   [yellow]{a['pending']}[/yellow]",
            f"Warm delay:       [cyan]{w['warm_delay_count']}[/cyan]",
            f"Current cap:      [cyan]{a['cap']}[/cyan]",
            "",
            "[dim]Cap Breakdown:[/dim]",
            f"  Base cap:       {a['base_cap']}",
            f"  Burst bonus:    {a['burst_allowance']}",
            f"  Low watermark:  {a['low_watermark']}",
            "",
        ]
        if w["burst_active"]:
            lines.append("[green]BURST ON[/green]")
        else:
            lines.append("[dim]BURST OFF[/dim]")
        return "\n".join(lines)
