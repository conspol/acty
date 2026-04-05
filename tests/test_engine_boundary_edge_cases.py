import asyncio

import pytest

from acty import ActyEngine, EngineConfig, NoopExecutor


@pytest.mark.asyncio
async def test_engine_submit_empty_follower_list() -> None:
    executor = NoopExecutor()
    async with ActyEngine(executor=executor, config=EngineConfig()) as engine:
        executor.bind(engine._controller)
        submission = await engine.submit_group("g1", {"p": 1}, [])
        assert submission.primer is not None
        assert len(submission.followers) == 0
        await asyncio.wait_for(submission.primer, timeout=2.0)
