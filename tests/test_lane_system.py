import asyncio
import math
from collections.abc import Mapping

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.collector import RuntimeStatsCollector
from acty_core.events.types import Event
from acty_core.scheduler import LaneConfig

from tests.support.asyncio_tools import wait_until


class _EventRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []

    async def handle(self, event: Event) -> None:
        self.events.append(event)


class _TimedExecutor:
    def __init__(self, *, delays: dict[str, float]) -> None:
        self._release = asyncio.Event()
        self._delays = dict(delays)
        self.jobs: list[tuple[Job, str]] = []
        self.started: list[tuple[str, float]] = []
        self.finished: list[tuple[str, float, float]] = []

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self.jobs.append((job, pool))
        lane = _lane_from_job(job)
        start = asyncio.get_running_loop().time()
        self.started.append((lane, start))
        await self._release.wait()
        delay = self._delays.get(lane, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)
        end = asyncio.get_running_loop().time()
        self.finished.append((lane, start, end))
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )

    def release(self) -> None:
        self._release.set()


def _lane_from_job(job: Job) -> str:
    context = job.context
    if isinstance(context, Mapping):
        group_context = context.get("group_context")
        if isinstance(group_context, Mapping):
            lane = group_context.get("lane")
            if isinstance(lane, str) and lane:
                return lane
    return "default"


def _lane_weight_from_events(events: list[Event], lane_id: str) -> float | None:
    for event in events:
        if event.type != "lane_configs_updated" or not event.payload:
            continue
        lane_configs = event.payload.get("lane_configs", [])
        for config in lane_configs:
            if config.get("lane") == lane_id:
                weight = config.get("weight")
                if isinstance(weight, (int, float)):
                    return float(weight)
    return None


@pytest.mark.asyncio
async def test_lane_system_default_weight_targets_and_payloads() -> None:
    recorder = _EventRecorder()
    collector = RuntimeStatsCollector()
    executor = _TimedExecutor(
        delays={
            "lane-default": 0.04,
            "lane-beta": 0.02,
            "lane-gamma": 0.01,
            "lane-delta": 0.06,
        }
    )
    config = EngineConfig(
        stats_emit_interval_s=None,
        lane_default=LaneConfig(weight=0.5),
        primer_workers=1,
        follower_workers=3,
    )
    engine = ActyEngine(executor=executor, config=config, event_sinks=[collector, recorder])
    try:
        async with engine:
            try:
                await wait_until(
                    lambda: any(event.type == "run_started" for event in recorder.events),
                    timeout=2.0,
                    message="run_started not observed",
                )
                engine.lane("lane-default")
                engine.lane("lane-beta", weight=1.0)
                engine.lane("lane-gamma", weight=1.5)
                engine.lane("lane-delta", weight=3.0)

                await wait_until(
                    lambda: _lane_weight_from_events(recorder.events, "lane-default") == 0.5,
                    timeout=2.0,
                    message="lane-default weight not observed in events",
                )

                # Create enough primer jobs per lane to exceed total workers (4),
                # ensuring pending work exists for each lane. Composition varies by lane.
                for idx in range(3):
                    await engine.open_group(
                        f"g-default-{idx}",
                        primer_payload={"p": idx},
                        group_context={"lane": "lane-default", "tier": "baseline"},
                    )
                for idx in range(4):
                    await engine.open_group(
                        f"g-beta-{idx}",
                        primer_payload={"p": idx, "feature": "beta"},
                        group_context={"lane": "lane-beta", "tier": "beta"},
                    )
                for idx in range(5):
                    await engine.open_group(
                        f"g-gamma-{idx}",
                        primer_payload={"p": idx, "feature": "gamma"},
                        group_context={"lane": "lane-gamma", "tier": "gamma"},
                    )
                for idx in range(6):
                    await engine.open_group(
                        f"g-delta-{idx}",
                        primer_payload={"p": idx, "feature": "delta"},
                        group_context={"lane": "lane-delta", "tier": "delta"},
                    )

                await wait_until(
                    lambda: _all_lanes_active(
                        collector.snapshot(),
                        {"lane-default", "lane-beta", "lane-gamma", "lane-delta"},
                    ),
                    timeout=2.0,
                    message="active lanes not observed",
                )

                snapshot = collector.snapshot()
                assert snapshot.lane_targets == {
                    "lane-default": 1,
                    "lane-beta": 1,
                    "lane-gamma": 1,
                    "lane-delta": 2,
                }

                # Ensure initial inflight distribution respects lane targets.
                inflight = snapshot.inflight_by_lane
                targets = snapshot.lane_targets or {}
                for lane, count in inflight.items():
                    target = targets.get(lane, 0)
                    if target:
                        assert count <= target

                assert any(
                    event.type == "job_queued"
                    and event.payload
                    and event.payload.get("lane") == "lane-default"
                    for event in recorder.events
                )
                assert any(
                    event.type == "job_queued"
                    and event.payload
                    and event.payload.get("lane") == "lane-beta"
                    for event in recorder.events
                )
                assert any(
                    event.type == "job_queued"
                    and event.payload
                    and event.payload.get("lane") == "lane-gamma"
                    for event in recorder.events
                )
                assert any(
                    event.type == "job_queued"
                    and event.payload
                    and event.payload.get("lane") == "lane-delta"
                    for event in recorder.events
                )

                await wait_until(
                    lambda: _all_lanes_started(
                        recorder.events,
                        {"lane-default", "lane-beta", "lane-gamma", "lane-delta"},
                    ),
                    timeout=2.0,
                    message="not all lanes started",
                )

                executor.release()

                await wait_until(
                    lambda: _lane_duration_checks(executor.finished, executor._delays),
                    timeout=2.0,
                    message="lane durations did not reflect configured delays",
                )
            finally:
                executor.release()
    finally:
        await engine.close()


def _all_lanes_active(snapshot, lanes: set[str]) -> bool:
    for lane in lanes:
        if snapshot.pending_by_lane.get(lane, 0) <= 0 and snapshot.inflight_by_lane.get(lane, 0) <= 0:
            return False
    return True


def _all_lanes_started(events: list[Event], lanes: set[str]) -> bool:
    started = {
        event.payload.get("lane")
        for event in events
        if event.type == "job_started" and event.payload
    }
    return lanes.issubset(started)


def _lane_duration_checks(
    finished: list[tuple[str, float, float]],
    delays: dict[str, float],
) -> bool:
    if not finished:
        return False
    observed: dict[str, float] = {}
    for lane, start, end in finished:
        duration = end - start
        current = observed.get(lane)
        if current is None or duration > current:
            observed[lane] = duration
    for lane, delay in delays.items():
        if lane not in observed:
            return False
        if observed[lane] + 1e-3 < delay:
            return False
    # Sanity: shortest delay should not finish after the longest by a wide margin.
    shortest = min(delays.values())
    longest = max(delays.values())
    shortest_finish = min(
        duration for lane, duration in observed.items() if math.isclose(delays[lane], shortest)
    )
    longest_finish = min(
        duration for lane, duration in observed.items() if math.isclose(delays[lane], longest)
    )
    return shortest_finish <= longest_finish
