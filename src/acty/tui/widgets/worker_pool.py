"""Worker pool status widget showing pool activity and work stealing."""

from __future__ import annotations

from textual.widgets import Static

from acty_core.events.snapshot import PoolStats


class WorkerPoolWidget(Static):
    """Widget showing worker pool status and work stealing activity."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool_stats: dict[str, PoolStats] = {}

    def on_mount(self) -> None:
        self.border_title = "Worker Pools"

    def update_pools(self, pool_stats: dict[str, PoolStats]) -> None:
        self._pool_stats = pool_stats
        self.refresh(layout=True)

    def render(self) -> str:
        if not self._pool_stats:
            return "[dim]No pool data[/dim]"

        lines: list[str] = []

        for pool_name, stats in sorted(self._pool_stats.items()):
            total = stats.total_workers
            idle = stats.idle_workers
            working = stats.working_workers
            stealing = stats.stealing_workers

            if total > 0:
                bar_width = 15
                idle_w = int((idle / total) * bar_width) if total else 0
                working_w = int((working / total) * bar_width) if total else 0
                stealing_w = bar_width - idle_w - working_w

                parts = []
                if idle_w > 0:
                    parts.append(f"[dim]{'.' * idle_w}[/dim]")
                if working_w > 0:
                    parts.append(f"[green]{'*' * working_w}[/green]")
                if stealing_w > 0:
                    parts.append(f"[yellow]{'!' * stealing_w}[/yellow]")

                bar = "".join(parts) if parts else "[dim]" + "." * bar_width + "[/dim]"

                lines.append(f"[bold cyan]{pool_name}[/bold cyan] ({bar})")

                status_parts = [f"[dim]i:[/dim]{idle}", f"[green]w:[/green]{working}"]
                if stealing > 0:
                    status_parts.append(f"[yellow]s:[/yellow]{stealing}")
                status = " ".join(status_parts)
                lines.append(f"  {status}")
            else:
                lines.append(f"[bold cyan]{pool_name}[/bold cyan] [dim]no workers[/dim]")

        lines.append("")
        lines.append("[dim].[/dim]=idle [green]*[/green]=work [yellow]![/yellow]=steal")

        return "\n".join(lines)
