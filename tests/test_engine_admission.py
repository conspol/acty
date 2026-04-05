import asyncio

import pytest

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.core.types import Job, JobResult
from acty_core.events import EventBus
from acty_core.events.types import Event
from acty_core.lifecycle import GroupTaskKind
from tests.support.asyncio_tools import wait_until


class EventRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self._cond = asyncio.Condition()

    async def handle(self, event: Event) -> None:
        async with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def count(self, event_type: str) -> int:
        return sum(1 for event in self.events if event.type == event_type)

    def index_of(self, event_type: str, occurrence: int) -> int:
        seen = 0
        for idx, event in enumerate(self.events):
            if event.type == event_type:
                seen += 1
                if seen == occurrence:
                    return idx
        raise AssertionError(f"event {event_type} occurrence {occurrence} not found")

    def last_payload(self, event_type: str) -> dict | None:
        for event in reversed(self.events):
            if event.type == event_type:
                return dict(event.payload or {})
        return None

    async def wait_for(self, event_type: str, count: int, timeout: float = 2.0) -> None:
        async with self._cond:
            await asyncio.wait_for(
                self._cond.wait_for(lambda: self.count(event_type) >= count),
                timeout=timeout,
            )


class BlockingExecutor:
    handles_lifecycle = True

    def __init__(self, controller, *, block_group: str) -> None:
        self._controller = controller
        self._block_group = block_group
        self._event = asyncio.Event()

    def release(self) -> None:
        self._event.set()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await self._controller.mark_primer_started(group_id)
            await self._controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            if group_id == self._block_group:
                await self._event.wait()
            await self._controller.mark_follower_done(group_id)
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
        )


@pytest.mark.asyncio
async def test_pending_group_returns_primer_future() -> None:
    executor = BlockingExecutor(None, block_group="g1")
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
    )
    executor._controller = engine._controller

    try:
        submission1 = await engine.submit_group("g1", {"p": 1}, [{"f": 1}])
        submission2 = await engine.submit_group("g2", {"p": 2}, None)

        assert submission1.primer is not None
        assert submission2.primer is not None

        await asyncio.wait_for(submission1.primer, timeout=2.0)
        with pytest.raises(TimeoutError):
            await wait_until(lambda: submission2.primer.done(), timeout=0.05)

        executor.release()
        await asyncio.wait_for(submission2.primer, timeout=2.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_submit_follower_before_admission() -> None:
    recorder = EventRecorder()
    bus = EventBus([recorder])
    engine = ActyEngine(
        executor=BlockingExecutor(None, block_group="g1"),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
        event_bus=bus,
    )
    engine._executor._controller = engine._controller  # type: ignore[attr-defined]

    try:
        submission1 = await engine.submit_group("g1", {"p": 1}, [{"f": 1}])
        submission2 = await engine.submit_group("g2", {"p": 2}, None)
        follower_future = await engine.submit_follower("g2", {"f": 2})
        await recorder.wait_for("group_admitted", count=1)

        engine._executor.release()  # type: ignore[attr-defined]
        await recorder.wait_for("group_admitted", count=2)

        result = await asyncio.wait_for(follower_future, timeout=2.0)
        assert result.kind == GroupTaskKind.FOLLOWER.value
        assert str(result.group_id) == "g2"
        await engine.close_group("g2")

        # Drain remaining futures.
        futures = [
            *submission1.followers,
            *submission2.followers,
        ]
        if submission1.primer is not None:
            futures.append(submission1.primer)
        if submission2.primer is not None:
            futures.append(submission2.primer)
        await asyncio.gather(*futures)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_close_closes_open_group_with_admission() -> None:
    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
    )
    executor.bind(engine._controller)

    submission = await engine.submit_group("g-open", {"p": 1}, None)
    if submission.primer is not None:
        await asyncio.wait_for(submission.primer, timeout=2.0)

    await asyncio.wait_for(engine.close(), timeout=2.0)


@pytest.mark.asyncio
async def test_engine_admission_blocks_until_group_done() -> None:
    recorder = EventRecorder()
    bus = EventBus([recorder])
    engine = ActyEngine(
        executor=BlockingExecutor(None, block_group="g1"),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            burst_allowance=0,
            admission_tick_s=0.01,
        ),
        event_bus=bus,
    )
    # Bind after engine init.
    engine._executor._controller = engine._controller  # type: ignore[attr-defined]

    try:
        submission1 = await engine.submit_group("g1", {"p": 1}, [{"f": 1}])
        submission2 = await engine.submit_group("g2", {"p": 2}, [{"f": 2}])

        await recorder.wait_for("group_admitted", count=1)
        assert recorder.count("group_admitted") == 1
        pending_payload = recorder.last_payload("groups_pending") or {}
        assert pending_payload.get("pending", 0) >= 1

        # g2 should not be admitted yet.
        assert all(e.group_id == "g1" for e in recorder.events if e.type == "group_admitted")

        engine._executor.release()  # type: ignore[attr-defined]
        await recorder.wait_for("group_admitted", count=2)

        idx_retired = recorder.index_of("group_retired", 1)
        idx_admit2 = recorder.index_of("group_admitted", 2)
        assert idx_retired < idx_admit2

        futures = [
            *submission1.followers,
            *submission2.followers,
        ]
        if submission1.primer is not None:
            futures.append(submission1.primer)
        if submission2.primer is not None:
            futures.append(submission2.primer)
        results = await asyncio.gather(*futures)
        assert len(results) == len(futures)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_burst_allows_overlap() -> None:
    recorder = EventRecorder()
    bus = EventBus([recorder])
    engine = ActyEngine(
        executor=BlockingExecutor(None, block_group="g1"),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            burst_allowance=1,
            low_watermark_factor=1,
            admission_tick_s=0.01,
        ),
        event_bus=bus,
    )
    engine._executor._controller = engine._controller  # type: ignore[attr-defined]

    try:
        submission1 = await engine.submit_group("g1", {"p": 1}, [{"f": 1}])
        submission2 = await engine.submit_group("g2", {"p": 2}, [{"f": 2}])

        await recorder.wait_for("group_admitted", count=2)
        assert recorder.count("group_admitted") == 2

        engine._executor.release()  # type: ignore[attr-defined]
        futures = [
            *submission1.followers,
            *submission2.followers,
        ]
        if submission1.primer is not None:
            futures.append(submission1.primer)
        if submission2.primer is not None:
            futures.append(submission2.primer)
        await asyncio.gather(*futures)
    finally:
        await engine.close()
