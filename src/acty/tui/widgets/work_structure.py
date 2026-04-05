"""Work structure visualization widget with multiple display modes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from textual.reactive import reactive
from textual.widgets import Static


class VizMode(Enum):
    PIPELINE = auto()
    STACKED = auto()

    def next(self) -> "VizMode":
        modes = list(VizMode)
        idx = modes.index(self)
        return modes[(idx + 1) % len(modes)]

    @property
    def label(self) -> str:
        return {
            VizMode.PIPELINE: "Pipeline",
            VizMode.STACKED: "Stacked",
        }[self]


@dataclass
class WorkState:
    active_groups: int = 0
    pending_groups: int = 0
    done_groups: int = 0
    failed_groups: int = 0
    primer_queued: int = 0
    primer_running: int = 0
    primer_done: int = 0
    follower_queued: int = 0
    follower_running: int = 0
    follower_done: int = 0

    @property
    def total_groups(self) -> int:
        return self.active_groups + self.pending_groups + self.done_groups + self.failed_groups

    @property
    def total_primers(self) -> int:
        return self.primer_queued + self.primer_running + self.primer_done

    @property
    def total_followers(self) -> int:
        return self.follower_queued + self.follower_running + self.follower_done


class WorkStructureWidget(Static):
    mode: reactive[VizMode] = reactive(VizMode.PIPELINE)

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._state = WorkState()
        self._lane_lines: list[str] = []

    def on_mount(self) -> None:
        self._update_border_title()

    def cycle_mode(self) -> None:
        self.mode = self.mode.next()
        self._update_border_title()

    def _update_border_title(self) -> None:
        self.border_title = "Work Structure"
        self.border_subtitle = f"({self.mode.label}) press v to switch"

    def update_state(
        self,
        *,
        groups_primer_ready: int = 0,
        groups_primer_inflight: int = 0,
        groups_warm_delay: int = 0,
        groups_followers_ready: int = 0,
        groups_pending: int = 0,
        groups_done: int = 0,
        groups_failed: int = 0,
        runnable_by_kind: dict[str, int] | None = None,
        inflight_by_kind: dict[str, int] | None = None,
        followers_by_group: dict[str, dict[str, int]] | None = None,
        pending_by_lane: dict[str, int] | None = None,
        inflight_by_lane: dict[str, int] | None = None,
        lane_targets: dict[str, int] | None = None,
    ) -> None:
        runnable = runnable_by_kind or {}
        inflight_map = inflight_by_kind or {}
        followers_by_group = followers_by_group or {}

        primer_queued = runnable.get("primer", 0)
        follower_queued = runnable.get("follower", 0)
        primer_running = inflight_map.get("primer", 0)
        follower_running = inflight_map.get("follower", 0)

        follower_done = sum(stats.get("done", 0) for stats in followers_by_group.values())
        primer_done = groups_warm_delay + groups_followers_ready + groups_done + groups_failed

        active_groups = (
            groups_primer_ready
            + groups_primer_inflight
            + groups_warm_delay
            + groups_followers_ready
        )

        self._state = WorkState(
            active_groups=active_groups,
            pending_groups=groups_pending,
            done_groups=groups_done,
            failed_groups=groups_failed,
            primer_queued=primer_queued,
            primer_running=primer_running,
            primer_done=primer_done,
            follower_queued=follower_queued,
            follower_running=follower_running,
            follower_done=follower_done,
        )
        self._lane_lines = self._build_lane_lines(
            pending_by_lane or {},
            inflight_by_lane or {},
            lane_targets,
        )
        self.refresh()

    def render(self) -> str:
        if self.mode == VizMode.PIPELINE:
            return self._render_pipeline()
        return self._render_stacked()

    def _render_pipeline(self) -> str:
        s = self._state
        lines = [
            "[bold]Pipeline[/bold]",
            "",
            f"Groups active:   {s.active_groups}",
            f"Groups pending:  {s.pending_groups}",
            f"Groups done:     {s.done_groups}",
            f"Groups failed:   {s.failed_groups}",
            "",
            f"Primers queued:  {s.primer_queued}",
            f"Primers running: {s.primer_running}",
            f"Primers done:    {s.primer_done}",
            "",
            f"Followers queued:  {s.follower_queued}",
            f"Followers running: {s.follower_running}",
            f"Followers done:    {s.follower_done}",
        ]
        if self._lane_lines:
            lines.append("")
            lines.append("[bold]Lanes[/bold]")
            lines.extend(self._lane_lines)
        return "\n".join(lines)

    def _render_stacked(self) -> str:
        s = self._state

        def stacked(queued: int, running: int, done: int, width: int = 24) -> str:
            total = queued + running + done
            if total == 0:
                return "." * width
            q = int((queued / total) * width)
            r = int((running / total) * width)
            d = width - q - r
            return f"[yellow]{':' * q}[/yellow][green]{'*' * r}[/green][blue]{'=' * d}[/blue]"

        lines = [
            "[bold]Stacked Status[/bold]",
            "",
            f"Primers   {stacked(s.primer_queued, s.primer_running, s.primer_done)} "
            f"Q={s.primer_queued} R={s.primer_running} D={s.primer_done}",
            f"Followers {stacked(s.follower_queued, s.follower_running, s.follower_done)} "
            f"Q={s.follower_queued} R={s.follower_running} D={s.follower_done}",
        ]
        if self._lane_lines:
            lines.append("")
            lines.append("[bold]Lanes[/bold]")
            lines.extend(self._lane_lines)
        return "\n".join(lines)

    @staticmethod
    def _truncate_lane(lane: str, width: int) -> str:
        if len(lane) <= width:
            return lane
        if width <= 3:
            return lane[:width]
        return lane[: width - 3] + "..."

    def _build_lane_lines(
        self,
        pending_by_lane: dict[str, int],
        inflight_by_lane: dict[str, int],
        lane_targets: dict[str, int] | None,
    ) -> list[str]:
        lanes = set(pending_by_lane) | set(inflight_by_lane)
        rows: list[tuple[int, str, int, int]] = []
        for lane in lanes:
            pending = pending_by_lane.get(lane, 0)
            inflight = inflight_by_lane.get(lane, 0)
            total = pending + inflight
            if total <= 0:
                continue
            rows.append((total, lane, pending, inflight))
        if not rows:
            return []
        rows.sort(key=lambda row: (-row[0], row[1]))
        if len(rows) == 1 and rows[0][1] == "default":
            return []
        lines: list[str] = []
        max_lines = 8
        for _, lane, pending, inflight in rows[:max_lines]:
            lane_label = self._truncate_lane(lane, 14)
            target = lane_targets.get(lane) if lane_targets else None
            target_label = "-" if target is None else str(target)
            lines.append(f"{lane_label:<14} P={pending}  I={inflight}  T={target_label}")
        if len(rows) > max_lines:
            lines.append(f"... +{len(rows) - max_lines} more")
        return lines
