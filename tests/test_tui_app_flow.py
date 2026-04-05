import asyncio

import pytest

from acty.tui.app import RuntimeTuiApp
from acty.tui.sources import QueueEventSource
from acty.tui.widgets import WorkStructureWidget
from acty_core.events.types import Event

from tests.support.asyncio_tools import wait_until

@pytest.mark.asyncio
async def test_app_consumes_events_and_handles_pause(tui_runner) -> None:
    queue: asyncio.Queue[Event] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(queue),
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app) as pilot:
        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=1,
                ts=1.0,
                type="job_queued",
                job_id="j1",
                group_id="g1",
                kind="primer",
            )
        )
        await pilot.pause(0.05)
        assert app._state.log_lines()

        app.action_toggle_pause()
        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=2,
                ts=2.0,
                type="job_started",
                job_id="j1",
                group_id="g1",
                kind="primer",
                pool="p1",
                payload={"worker": "p1-0"},
            )
        )
        await pilot.pause(0.05)
        assert app._pending_logs

        app.action_toggle_pause()
        await pilot.pause(0.05)
        assert not app._pending_logs


@pytest.mark.asyncio
async def test_tui_lane_targets_refresh_after_update(tui_runner) -> None:
    queue: asyncio.Queue[Event] = asyncio.Queue()
    app = RuntimeTuiApp(
        QueueEventSource(queue),
        poll_interval_s=0.01,
        stats_interval_s=0.01,
        plot_interval_s=0.05,
    )

    async with tui_runner(app) as pilot:
        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=1,
                ts=1.0,
                type="run_started",
                payload={
                    "pool_configs": [{"name": "p", "workers": 3}],
                        "lane_configs": [
                            {"lane": "alpha", "weight": 1, "max_inflight": None},
                            {"lane": "beta", "weight": 2, "max_inflight": None},
                        ],
                    "lane_default": {"weight": 1, "max_inflight": None},
                },
            )
        )
        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=2,
                ts=2.0,
                type="job_queued",
                job_id="j-alpha",
                kind="seed",
                payload={"job_context": {"lane": "alpha"}},
            )
        )
        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=3,
                ts=2.1,
                type="job_queued",
                job_id="j-beta",
                kind="seed",
                payload={"job_context": {"lane": "beta"}},
            )
        )

        await wait_until(
            lambda: any(
                "beta" in line and "T=2" in line
                for line in app.query_one("#work-structure", WorkStructureWidget)._lane_lines
            ),
            timeout=1.0,
            message="expected initial lane targets to render in work structure widget",
        )

        await queue.put(
            Event(
                schema_version=1,
                run_id="run",
                seq=4,
                ts=3.0,
                type="lane_configs_updated",
                payload={
                    "lane_configs": [
                        {"lane": "alpha", "weight": 3, "max_inflight": None},
                        {"lane": "beta", "weight": 1, "max_inflight": None},
                    ],
                    "default_config": {"weight": 1, "max_inflight": None},
                },
            )
        )

        await wait_until(
            lambda: any(
                "beta" in line and "T=1" in line
                for line in app.query_one("#work-structure", WorkStructureWidget)._lane_lines
            ),
            timeout=1.0,
            message="expected lane target update to render after lane_configs_updated",
        )
