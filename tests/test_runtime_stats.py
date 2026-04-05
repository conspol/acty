import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events import EventBus
from acty_core.events.sinks.base import EventSink
from tests.support.asyncio_tools import wait_until


class RecordingSink(EventSink):
    def __init__(self) -> None:
        self.events = []

    async def handle(self, event) -> None:
        self.events.append(event)


class SimpleExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        return JobResult(job_id=job.id, kind=job.kind, ok=True)


@pytest.mark.asyncio
async def test_runtime_stats_emitted() -> None:
    sink = RecordingSink()
    bus = EventBus([sink])
    config = EngineConfig(
        primer_workers=1,
        follower_workers=1,
        stats_emit_interval_s=0.01,
    )
    engine = ActyEngine(executor=SimpleExecutor(), config=config, event_bus=bus)
    try:
        submission = await engine.submit_group("g_stats", {"p": 1}, [{"f": 1}])
        if submission.primer is not None:
            await submission.primer
        await asyncio.gather(*submission.followers)
        await wait_until(
            lambda: any(event.type == "runtime_stats" for event in sink.events),
            timeout=1.0,
        )
    finally:
        await engine.close()

    stats_events = [event for event in sink.events if event.type == "runtime_stats"]
    assert stats_events
    payload = stats_events[-1].payload or {}

    assert "queue_depth" in payload
    assert "pending_by_kind" in payload
    assert "inflight_by_kind" in payload
    assert "inflight_workers" in payload
    assert "groups_by_state" in payload
    assert "warm_tasks" in payload
    assert "counts" in payload

    groups_by_state = payload.get("groups_by_state", {})
    for key in ["PRIMER_READY", "PRIMER_INFLIGHT", "WARM_DELAY", "FOLLOWERS_READY", "DONE"]:
        assert key in groups_by_state

    counts = payload.get("counts", {})
    for key in [
        "queued",
        "started",
        "finished",
        "failed",
        "rejected",
        "requeued",
        "resubmitted",
        "cancelled",
    ]:
        assert key in counts
