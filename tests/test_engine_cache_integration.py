import asyncio
from typing import Any, Mapping

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.cache import CacheHandle, CacheMode, CacheRegistry, InMemoryStorage
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import GroupTaskKind


class DummyProvider:
    def __init__(self) -> None:
        self.name = "dummy"
        self.create_calls = 0
        self.fingerprint_calls: list[tuple[object, object | None]] = []

    def fingerprint(self, primer, context=None) -> str:
        self.fingerprint_calls.append((primer, context))
        return f"fp:{primer}"

    async def create(self, primer, context=None):
        self.create_calls += 1
        return f"ref:{primer}"

    async def wait_ready(self, provider_ref, timeout_s=None):
        return None

    async def invalidate(self, provider_ref):
        return None


class RecordingExecutor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None
        self.kinds: list[str] = []
        self.cache_handles: list[CacheHandle | None] = []
        self.contexts: list[Mapping[str, Any] | None] = []

    def bind(self, controller) -> None:
        self._controller = controller

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        self.kinds.append(job.kind)
        self.cache_handles.append(job.cache_handle)
        self.contexts.append(job.context)
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


def _assert_cache_context(
    context: Mapping[str, Any] | None,
    *,
    hit: bool | None,
    created: bool | None,
    provider: str,
    provider_ref: str | None = None,
    fingerprint: str | None = None,
) -> Mapping[str, Any]:
    assert context is not None
    assert context["group_context"] is None
    cache = context["cache"]
    assert cache is not None
    assert cache["hit"] is hit
    assert cache["created"] is created
    if provider_ref is not None:
        assert cache["provider_ref"] == provider_ref
    summary = cache["handle_summary"]
    assert summary is not None
    assert summary["provider"] == provider
    if fingerprint is not None:
        assert summary["fingerprint"] == fingerprint
    return summary


@pytest.mark.asyncio
async def test_engine_cache_hit_runs_primer() -> None:
    provider = DummyProvider()
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

    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group(
            "g1",
            primer_payload,
            [{"f": 1}],
            cache_key="cache-g1",
        )
        assert submission.primer is not None

        results = await asyncio.wait_for(
            asyncio.gather(submission.primer, *submission.followers),
            timeout=2.0,
        )
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1
        assert GroupTaskKind.PRIMER.value in executor.kinds
        handles = [handle for handle in executor.cache_handles if handle is not None]
        assert handles
        assert all(handle.group_id == "cache-g1" for handle in handles)
        assert all(handle.provider_ref == "ref:1" for handle in handles)
        assert provider.create_calls == 0
        assert executor.contexts
        for context in executor.contexts:
            summary = _assert_cache_context(
                context,
                hit=True,
                created=False,
                provider=provider.name,
                provider_ref="ref:1",
                fingerprint=handle.fingerprint,
            )
            assert summary["created_at"] == 0.0
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_cache_hit_skips_primer_when_disabled() -> None:
    provider = DummyProvider()
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

    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            run_primer_on_cache_hit=False,
        ),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group(
            "g1",
            primer_payload,
            [{"f": 1}],
            cache_key="cache-g1",
        )
        follower_result = await asyncio.wait_for(submission.followers[0], timeout=2.0)
        assert follower_result.kind == GroupTaskKind.FOLLOWER.value
        assert GroupTaskKind.PRIMER.value not in executor.kinds
        assert provider.create_calls == 0
        assert executor.contexts
        for context in executor.contexts:
            _assert_cache_context(
                context,
                hit=True,
                created=False,
                provider=provider.name,
                provider_ref="ref:1",
                fingerprint=handle.fingerprint,
            )
        if submission.primer is not None:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(submission.primer, timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_cache_hit_runs_primer_with_admission() -> None:
    provider = DummyProvider()
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

    executor = RecordingExecutor()
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
    try:
        submission = await engine.submit_group(
            "g1",
            primer_payload,
            [{"f": 1}],
            cache_key="cache-g1",
        )
        assert submission.primer is not None

        results = await asyncio.wait_for(
            asyncio.gather(submission.primer, *submission.followers),
            timeout=2.0,
        )
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1
        assert GroupTaskKind.PRIMER.value in executor.kinds
        handles = [handle for handle in executor.cache_handles if handle is not None]
        assert handles
        assert all(handle.group_id == "cache-g1" for handle in handles)
        assert all(handle.provider_ref == "ref:1" for handle in handles)
        assert provider.create_calls == 0
        assert executor.contexts
        for context in executor.contexts:
            summary = _assert_cache_context(
                context,
                hit=True,
                created=False,
                provider=provider.name,
                provider_ref="ref:1",
                fingerprint=handle.fingerprint,
            )
            assert summary["created_at"] == 0.0
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_cache_hit_skips_primer_with_admission_when_disabled() -> None:
    provider = DummyProvider()
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

    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
            run_primer_on_cache_hit=False,
        ),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group(
            "g1",
            primer_payload,
            [{"f": 1}],
            cache_key="cache-g1",
        )
        follower_result = await asyncio.wait_for(submission.followers[0], timeout=2.0)
        assert follower_result.kind == GroupTaskKind.FOLLOWER.value
        assert GroupTaskKind.PRIMER.value not in executor.kinds
        assert provider.create_calls == 0
        assert executor.contexts
        for context in executor.contexts:
            _assert_cache_context(
                context,
                hit=True,
                created=False,
                provider=provider.name,
                provider_ref="ref:1",
                fingerprint=handle.fingerprint,
            )
        if submission.primer is not None:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(submission.primer, timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_cache_miss_runs_primer() -> None:
    provider = DummyProvider()
    registry = CacheRegistry(provider)

    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group("g2", {"p": 2}, [{"f": 1}, {"f": 2}])
        assert submission.primer is not None

        results = await asyncio.wait_for(
            asyncio.gather(submission.primer, *submission.followers),
            timeout=2.0,
        )
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 2
        assert provider.create_calls == 1
        assert executor.contexts
        for context in executor.contexts:
            summary = _assert_cache_context(
                context,
                hit=False,
                created=True,
                provider=provider.name,
            )
            assert summary["created_at"] is not None
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_cache_miss_runs_primer_when_disabled() -> None:
    provider = DummyProvider()
    registry = CacheRegistry(provider)

    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            run_primer_on_cache_hit=False,
        ),
        cache_registry=registry,
    )
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group("g2", {"p": 2}, [{"f": 1}, {"f": 2}])
        assert submission.primer is not None

        results = await asyncio.wait_for(
            asyncio.gather(submission.primer, *submission.followers),
            timeout=2.0,
        )
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 2
        assert provider.create_calls == 1
        assert executor.contexts
        for context in executor.contexts:
            summary = _assert_cache_context(
                context,
                hit=False,
                created=True,
                provider=provider.name,
            )
            assert summary["created_at"] is not None
    finally:
        await engine.close()
