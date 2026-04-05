import asyncio

import pytest

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.cache import CacheHandle, CacheMode, CacheRegistry, InMemoryStorage
from acty_core.lifecycle import GroupTaskKind


class DummyProvider:
    def __init__(self) -> None:
        self.name = "dummy"
        self.create_calls = 0

    def fingerprint(self, primer, context=None) -> str:
        return f"fp:{primer}"

    async def create(self, primer, context=None):
        self.create_calls += 1
        return f"ref:{primer}"

    async def wait_ready(self, provider_ref, timeout_s=None):
        return None

    async def invalidate(self, provider_ref):
        return None


@pytest.mark.asyncio
async def test_deferred_primer_submission() -> None:
    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(auto_primer_from_first_payload=False),
    )
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g1") as group:
            assert group.primer is None

            primer = await group.submit_primer({"p": 1})
            assert primer is not None
            primer_result = await asyncio.wait_for(primer, timeout=2.0)
            assert primer_result.kind == GroupTaskKind.PRIMER.value

            follower = await group.add_follower({"f": 1})
            follower_result = await asyncio.wait_for(follower, timeout=2.0)
            assert follower_result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_deferred_primer_cache_hit_runs_primer() -> None:
    provider = DummyProvider()
    storage = InMemoryStorage()
    primer_payload = {"p": 1}
    existing = CacheHandle(
        group_id="cache-g1",
        provider=provider.name,
        mode=CacheMode.EXPLICIT,
        fingerprint=provider.fingerprint(primer_payload),
        provider_ref="ref:1",
        created_at=0.0,
    )
    await storage.save(existing)
    registry = CacheRegistry(provider, storage=storage)

    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(auto_primer_from_first_payload=False),
        cache_registry=registry,
    )
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g1", cache_key="cache-g1") as group:
            assert group.primer is None

            primer = await group.submit_primer(primer_payload)
            assert primer is not None
            primer_result = await asyncio.wait_for(primer, timeout=2.0)
            assert primer_result.kind == GroupTaskKind.PRIMER.value

            follower = await group.add_follower({"f": 1})
            follower_result = await asyncio.wait_for(follower, timeout=2.0)
            assert follower_result.kind == GroupTaskKind.FOLLOWER.value

            handle = await engine._controller.get_cache_handle("g1")
            assert handle is not None
            assert handle.group_id == "cache-g1"
            assert provider.create_calls == 0
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_deferred_primer_cache_hit_skips_primer_when_disabled() -> None:
    provider = DummyProvider()
    storage = InMemoryStorage()
    primer_payload = {"p": 1}
    existing = CacheHandle(
        group_id="cache-g1",
        provider=provider.name,
        mode=CacheMode.EXPLICIT,
        fingerprint=provider.fingerprint(primer_payload),
        provider_ref="ref:1",
        created_at=0.0,
    )
    await storage.save(existing)
    registry = CacheRegistry(provider, storage=storage)

    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            auto_primer_from_first_payload=False,
            run_primer_on_cache_hit=False,
        ),
        cache_registry=registry,
    )
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g1", cache_key="cache-g1") as group:
            assert group.primer is None

            primer = await group.submit_primer(primer_payload)
            assert primer is None

            follower = await group.add_follower({"f": 1})
            follower_result = await asyncio.wait_for(follower, timeout=2.0)
            assert follower_result.kind == GroupTaskKind.FOLLOWER.value

            handle = await engine._controller.get_cache_handle("g1")
            assert handle is not None
            assert handle.group_id == "cache-g1"
            assert provider.create_calls == 0
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_auto_primer_from_first_payload() -> None:
    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(auto_primer_from_first_payload=True),
    )
    executor.bind(engine._controller)

    try:
        async with engine.open_group("g_auto") as group:
            first = await group.submit({"auto": 1})
            assert first is not None
            first_result = await asyncio.wait_for(first, timeout=2.0)
            assert first_result.kind == GroupTaskKind.PRIMER.value

            second = await group.submit({"f": 1})
            second_result = await asyncio.wait_for(second, timeout=2.0)
            assert second_result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()
