import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.base import EventSink
from acty_core.lifecycle import GroupTaskKind


class FastExecutor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None

    def bind(self, controller) -> None:
        self._controller = controller

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            await controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await controller.mark_follower_done(group_id)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class BlockingExecutor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None
        self._event = asyncio.Event()

    def bind(self, controller) -> None:
        self._controller = controller

    def release(self) -> None:
        self._event.set()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        await self._event.wait()
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            await controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await controller.mark_follower_done(group_id)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class RecordingEventSink(EventSink):
    def __init__(self) -> None:
        self.events = []
        self._cancel_event = asyncio.Event()

    async def handle(self, event) -> None:  # type: ignore[override]
        self.events.append(event)
        if event.type == "job_cancelled":
            payload = event.payload or {}
            if payload.get("reason") == "explicit_cancel":
                self._cancel_event.set()

    async def wait_for_explicit_cancel(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._cancel_event.wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_close_all_groups_closes_open_group() -> None:
    executor = FastExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
    )
    executor.bind(engine._controller)

    try:
        submission = await engine.submit_group("g1", {"p": 1}, None)
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=1.0)

        follower_future = await engine.submit_follower("g1", {"f": 1})
        await engine.close_all_groups()

        result = await asyncio.wait_for(follower_future, timeout=1.0)
        assert result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_close_all_groups_drop_pending_cancels_futures() -> None:
    executor = BlockingExecutor()
    sink = RecordingEventSink()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[sink],
    )
    executor.bind(engine._controller)

    try:
        submission = await engine.submit_group("g-drop", {"p": 1}, [{"f": 1}])
        await engine.close_all_groups(mode="drop_pending")

        assert submission.primer is not None
        assert submission.primer.cancelled()
        assert all(fut.cancelled() for fut in submission.followers)

        await sink.wait_for_explicit_cancel(timeout=1.0)
        cancelled = [event for event in sink.events if event.type == "job_cancelled"]
        assert cancelled
        assert (cancelled[0].payload or {}).get("reason") == "explicit_cancel"
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_close_all_groups_timeout_triggers_drop_pending() -> None:
    executor = BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
    )
    executor.bind(engine._controller)

    try:
        submission = await engine.submit_group("g-timeout", {"p": 1}, [{"f": 1}])
        await engine.close_all_groups(timeout_s=0.01, on_timeout="drop_pending")

        assert submission.primer is not None
        assert submission.primer.cancelled()
        assert all(fut.cancelled() for fut in submission.followers)
    finally:
        executor.release()
        await engine.close()
