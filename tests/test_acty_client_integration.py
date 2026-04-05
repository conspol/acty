import asyncio

import pytest

from acty import ActyClient, ActyEngine, EngineConfig
from acty_core.cache import CacheHandle, CacheMode, CacheRegistry, InMemoryStorage
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import GroupTaskKind
from tests.support.asyncio_tools import wait_until


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


class _CacheProvider:
    def __init__(self) -> None:
        self.name = "dummy"

    def fingerprint(self, primer, context=None) -> str:
        return f"fp:{primer}"

    async def create(self, primer, context=None):
        return f"ref:{primer}"

    async def wait_ready(self, provider_ref, timeout_s=None):
        return None

    async def invalidate(self, provider_ref):
        return None


async def _collect_results(engine: ActyEngine) -> list[JobResult]:
    results: list[JobResult] = []
    async for item in engine.results():
        results.append(item)
    return results


@pytest.mark.asyncio
async def test_acty_client_with_admission_and_result_stream() -> None:
    executor = _Executor()
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
    client = ActyClient(engine)

    collector_task = asyncio.create_task(_collect_results(engine))
    try:
        primer_result = await asyncio.wait_for(client.call("g1", {"p": 1}), timeout=2.0)
        assert primer_result is not None
        assert primer_result.kind == GroupTaskKind.PRIMER.value

        call2 = asyncio.create_task(client.call("g2", {"p": 2}))
        follower2 = asyncio.create_task(client.call_follower("g2", {"f": 2}))
        await client.close_group("g2")

        # g2 should be pending while g1 remains open.
        with pytest.raises(TimeoutError):
            await wait_until(lambda: call2.done(), timeout=0.05)

        await client.close_group("g1")

        primer2 = await asyncio.wait_for(call2, timeout=2.0)
        assert primer2 is not None
        assert primer2.kind == GroupTaskKind.PRIMER.value
        follower2_result = await asyncio.wait_for(follower2, timeout=2.0)
        assert follower2_result.kind == GroupTaskKind.FOLLOWER.value

        await asyncio.wait_for(engine._controller.wait_group_done("g1"), timeout=2.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g2"), timeout=2.0)
    finally:
        await asyncio.wait_for(client.close(), timeout=2.0)

    results = await asyncio.wait_for(collector_task, timeout=2.0)
    kinds = [result.kind for result in results]
    assert kinds.count(GroupTaskKind.PRIMER.value) == 2
    assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1


@pytest.mark.asyncio
async def test_acty_client_cache_hit_returns_primer_with_admission() -> None:
    provider = _CacheProvider()
    storage = InMemoryStorage()
    primer_payload = {"p": 1}
    handle = CacheHandle(
        group_id="cache-g1",
        provider=provider.name,
        mode=CacheMode.EXPLICIT,
        fingerprint=provider.fingerprint(primer_payload),
        provider_ref="ref:1",
        created_at=0.0,
    )
    await storage.save(handle)
    registry = CacheRegistry(provider, storage=storage)

    executor = _Executor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    client = ActyClient(engine)

    try:
        result = await asyncio.wait_for(
            client.call("g1", primer_payload, cache_key="cache-g1"),
            timeout=2.0,
        )
        assert result is not None
        assert result.kind == GroupTaskKind.PRIMER.value
        await client.close_group("g1")
        await asyncio.wait_for(engine._controller.wait_group_done("g1"), timeout=2.0)
    finally:
        await client.close()
