import asyncio

import pytest

from acty import ActyEngine, EngineConfig, GroupHandle
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import GroupTaskKind


class _Executor:
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


@pytest.mark.asyncio
async def test_open_group_is_awaitable() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g1", {"p": 1})
        assert isinstance(group, GroupHandle)

        if group.primer is not None:
            await asyncio.wait_for(group.primer, timeout=1.0)

        follower_future = await group.add_follower({"f": 1})
        await group.close()
        result = await asyncio.wait_for(follower_future, timeout=1.0)
        assert result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_open_group_context_manager_and_deferred_primer() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g2") as group:
            assert group.primer is None
            primer_future = await group.submit_primer({"p": 2})
            if primer_future is not None:
                await asyncio.wait_for(primer_future, timeout=1.0)
            follower_future = await group.add_follower({"f": 2})
            await asyncio.wait_for(follower_future, timeout=1.0)

        await asyncio.sleep(0)
        assert "g2" not in await engine._controller.list_group_ids()
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_open_group_submit_auto_primer_default() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g_auto") as group:
            primer_future = await group.submit({"p": 1})
            follower_future = await group.submit({"f": 1})

            assert primer_future is not None
            primer_result = await asyncio.wait_for(primer_future, timeout=1.0)
            follower_result = await asyncio.wait_for(follower_future, timeout=1.0)
            assert primer_result.kind == GroupTaskKind.PRIMER.value
            assert follower_result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_submit_requires_primer_when_auto_disabled() -> None:
    executor = _Executor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            auto_primer_from_first_payload=False,
        ),
    )
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g_no_auto") as group:
            with pytest.raises(ValueError, match="no primer"):
                await group.submit({"f": 1})
    finally:
        await engine.close()
