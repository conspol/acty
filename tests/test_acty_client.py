import asyncio

import pytest

from acty import ActyClient, ActyEngine, EngineConfig
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
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
        )


@pytest.mark.asyncio
async def test_acty_client_call_and_follower() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)
    client = ActyClient(engine)

    try:
        primer_result = await client.call("g1", {"p": 1})
        assert primer_result is not None
        assert primer_result.kind == GroupTaskKind.PRIMER.value

        follower_result = await client.call_follower("g1", {"f": 1})
        assert follower_result.kind == GroupTaskKind.FOLLOWER.value
        assert str(follower_result.group_id) == "g1"
        await client.close_group("g1")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_acty_client_call_raises_on_duplicate_group() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)
    client = ActyClient(engine)

    try:
        _ = await client.call("g2", {"p": 2})
        with pytest.raises(ValueError):
            await client.call("g2", {"p": 3})
    finally:
        await client.close()
