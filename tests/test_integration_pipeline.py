import asyncio

import pytest

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.cache import CacheRegistry, InMemoryStorage
from acty_core.core.types import Job, JobId, JobResult
from acty_core.lifecycle import GroupTaskKind
from acty_core.scheduler import PoolConfig, WorkStealingScheduler


class RecordingExecutor:
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
            output={"processed": True},
            group_id=job.group_id,
        )


@pytest.mark.asyncio
async def test_full_pipeline_engine_to_results() -> None:
    results = []
    executor = RecordingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=2,
            follower_workers=4,
            max_active_groups=3,
            admission_tick_s=0.01,
        ),
    )
    executor.bind(engine._controller)

    async def collect_results():
        async for result in engine.results():
            results.append(result)

    collector = asyncio.create_task(collect_results())

    try:
        submissions = []
        for i in range(10):
            sub = await engine.submit_group(f"g{i}", {"p": i}, [{"f": j} for j in range(3)])
            submissions.append(sub)

        for sub in submissions:
            if sub.primer:
                await asyncio.wait_for(sub.primer, timeout=5.0)
            for f in sub.followers:
                await asyncio.wait_for(f, timeout=5.0)
    finally:
        await engine.close()

    await asyncio.wait_for(collector, timeout=5.0)

    assert len(results) == 40


@pytest.mark.asyncio
async def test_engine_pipeline_admission_to_results() -> None:
    results: list[JobResult] = []
    executor = RecordingExecutor()
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

    assert engine._admission is not None

    async def collect_results() -> None:
        async for result in engine.results():
            results.append(result)

    collector = asyncio.create_task(collect_results())

    try:
        submission = await engine.submit_group("g-admit", {"p": 1}, [{"f": 1}])
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        for f in submission.followers:
            await asyncio.wait_for(f, timeout=2.0)
    finally:
        await engine.close()

    await asyncio.wait_for(collector, timeout=2.0)

    kinds = [result.kind for result in results]
    assert kinds.count(GroupTaskKind.PRIMER.value) == 1
    assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1


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
async def test_engine_with_cache_integration() -> None:
    provider = DummyProvider()
    storage = InMemoryStorage()
    registry = CacheRegistry(provider, storage=storage)

    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        cache_registry=registry,
    )
    executor.bind(engine._controller)

    try:
        sub1 = await engine.submit_group("g1", {"p": 1}, [{"f": 1}], cache_key="shared")
        assert sub1.primer is not None
        await sub1.primer
        for f in sub1.followers:
            await f

        sub2 = await engine.submit_group("g2", {"p": 1}, [{"f": 2}], cache_key="shared")
        assert sub2.primer is not None
        await sub2.primer
        for f in sub2.followers:
            await f
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_scheduler_integration_with_result_sink() -> None:
    results: list[JobResult] = []

    class RecordingSink:
        async def handle(self, result: JobResult) -> None:
            results.append(result)

    pools = [PoolConfig(name="main", preferred_kinds=["test"], workers=2)]
    scheduler = WorkStealingScheduler(pools, result_sink=RecordingSink())

    async def source():
        for i in range(5):
            yield Job(id=JobId(f"j{i}"), kind="test")

    class Executor:
        async def execute(self, job: Job, *, pool: str) -> JobResult:
            return JobResult(job_id=job.id, kind=job.kind, ok=True)

    report = await scheduler.run(AsyncIterSource(source()), Executor())

    assert report.finished == 5
    assert len(results) == 5


class AsyncIterSource:
    def __init__(self, gen) -> None:
        self._gen = gen

    async def stream_jobs(self):
        async for job in self._gen:
            yield job
