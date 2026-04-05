"""Queue depth plot widget."""

from __future__ import annotations

from textual_plotext import PlotextPlot


class QueueDepthPlot(PlotextPlot):
    """Bar chart showing queue depth by job kind."""

    def on_mount(self) -> None:
        self.border_title = "Queue Depth"
        self._setup_empty_plot()

    def _setup_empty_plot(self) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.xlabel("Time (s)")
        plt.ylabel("Jobs")
        plt.theme("dark")

    def update_data(
        self,
        timestamps: list[float],
        breakdown: dict[str, list[int]],
        total_depth: list[int] | None = None,
    ) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.xlabel("Time (s)")
        plt.ylabel("Jobs")

        if not breakdown and total_depth and timestamps:
            plt.plot(timestamps, total_depth, label="total", marker="braille")
        elif breakdown and timestamps:
            colors = ["cyan", "magenta", "yellow", "green", "red", "blue"]
            for idx, (kind, values) in enumerate(breakdown.items()):
                color = colors[idx % len(colors)]
                if values:
                    plt.plot(timestamps, values, label=kind, marker="braille", color=color)

        plt.theme("dark")
        self.refresh()
