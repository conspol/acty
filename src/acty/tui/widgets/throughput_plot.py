"""Throughput line plot widget."""

from __future__ import annotations

from textual_plotext import PlotextPlot


class ThroughputPlot(PlotextPlot):
    """Line plot showing throughput (calls/sec) and workers over time."""

    def on_mount(self) -> None:
        self.border_title = "Throughput"
        self._setup_empty_plot()

    def _setup_empty_plot(self) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.xlabel("Time (s)")
        plt.ylabel("Rate")
        plt.theme("dark")

    def update_data(
        self,
        timestamps: list[float],
        rates: list[float],
        workers: list[int],
    ) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.xlabel("Time (s)")
        plt.ylabel("Rate")

        if timestamps and rates:
            plt.plot(timestamps, rates, label="calls/sec", marker="braille")
        if timestamps and workers:
            plt.plot(timestamps, workers, label="workers", marker="braille")

        if timestamps:
            plt.theme("dark")

        self.refresh()
