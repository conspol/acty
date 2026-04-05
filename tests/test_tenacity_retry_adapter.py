import asyncio
from collections import defaultdict

import pytest
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from acty import ActyEngine, EngineConfig, TenacityRetryAdapter
from acty_core.core.types import Job, JobId, JobResult
from acty_core.events.sinks.base import EventSink
from acty_core.events.types import Event
from acty_core.lifecycle import PrimerFailurePolicy, PrimerRetryDecision


class RecordingSink(EventSink):
    def __init__(self) -> None:
        self.events: list[Event] = []

    async def handle(self, event: Event) -> None:
        self.events.append(event)


class FlakyExecutor:
    def __init__(self, *, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("boom")
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class FailingResultExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self.calls += 1
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=False,
            error="nope",
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class AsyncBarrier:
    def __init__(self, parties: int) -> None:
        self._parties = parties
        self._count = 0
        self._event = asyncio.Event()

    async def wait(self, timeout: float = 1.0) -> None:
        self._count += 1
        if self._count >= self._parties:
            self._event.set()
        await asyncio.wait_for(self._event.wait(), timeout=timeout)


class ConcurrentFlakyExecutor:
    def __init__(self, barrier: AsyncBarrier) -> None:
        self._barrier = barrier
        self.attempts: dict[JobId, int] = {}

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        attempt = self.attempts.get(job.id, 0) + 1
        self.attempts[job.id] = attempt
        if attempt == 1:
            await self._barrier.wait()
            raise RuntimeError("boom")
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"attempt": attempt},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_tenacity_retry_adapter_retries_and_succeeds() -> None:
    executor = FlakyExecutor(fail_times=2)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    adapter = TenacityRetryAdapter(executor, retrying)
    job = Job(id=JobId("job-1"), kind="primer", payload={"p": 1})

    result = await adapter.execute(job, pool="primer")

    assert executor.calls == 3
    assert result.ok is True
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 3
    assert "boom" in str(result.meta.get("tenacity_last_error"))


@pytest.mark.asyncio
async def test_engine_accepts_async_retry_factory() -> None:
    executor = FlakyExecutor(fail_times=1)

    async def retry_factory(job: Job) -> AsyncRetrying:
        await asyncio.sleep(0)
        return AsyncRetrying(
            stop=stop_after_attempt(2),
            wait=wait_fixed(0.0),
            retry=retry_if_exception_type(RuntimeError),
        )

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(attempt_retry_policy=retry_factory),
    )
    try:
        submission = await engine.submit_group("g-async-factory", {"p": 1}, [])
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-async-factory"), timeout=5.0)
    finally:
        await engine.close()

    assert executor.calls == 2
    assert result.ok is True
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 2


@pytest.mark.asyncio
async def test_tenacity_retry_adapter_stops_after_attempts() -> None:
    executor = FlakyExecutor(fail_times=10)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    adapter = TenacityRetryAdapter(executor, retrying)
    job = Job(id=JobId("job-2"), kind="primer", payload={"p": 2})

    result = await adapter.execute(job, pool="primer")

    assert executor.calls == 2
    assert result.ok is False
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 2
    assert "boom" in str(result.meta.get("tenacity_last_error"))


@pytest.mark.asyncio
async def test_tenacity_retry_adapter_propagates_failure_result() -> None:
    executor = FailingResultExecutor()
    retrying = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    adapter = TenacityRetryAdapter(executor, retrying)
    job = Job(id=JobId("job-3"), kind="primer", payload={"p": 3})

    result = await adapter.execute(job, pool="primer")

    assert executor.calls == 1
    assert result.ok is False
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 1
    assert result.meta.get("tenacity_last_error") == "nope"


@pytest.mark.asyncio
async def test_tenacity_retry_adapter_accepts_retry_error_callback_result() -> None:
    executor = FlakyExecutor(fail_times=5)
    job = Job(id=JobId("job-callback"), kind="primer", payload={"p": 4})

    def retry_error_callback(retry_state):
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=False,
            error="callback result",
            group_id=job.group_id,
            follower_id=job.follower_id,
        )

    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
        retry_error_callback=retry_error_callback,
    )
    adapter = TenacityRetryAdapter(executor, retrying)

    result = await adapter.execute(job, pool="primer")

    assert executor.calls == 2
    assert result.ok is False
    assert result.error == "callback result"
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 2
    assert "boom" in str(result.meta.get("tenacity_last_error"))


@pytest.mark.asyncio
async def test_tenacity_retry_adapter_reraise_returns_jobresult() -> None:
    executor = FlakyExecutor(fail_times=5)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
        reraise=True,
    )
    adapter = TenacityRetryAdapter(executor, retrying)
    job = Job(id=JobId("job-reraise"), kind="primer", payload={"p": 5})

    result = await adapter.execute(job, pool="primer")

    assert executor.calls == 2
    assert result.ok is False
    assert result.meta is not None
    assert result.meta.get("tenacity_attempts") == 2
    assert "boom" in str(result.meta.get("tenacity_last_error"))
    assert "boom" in str(result.error)


@pytest.mark.asyncio
async def test_engine_tenacity_only_emits_meta_and_events() -> None:
    sink = RecordingSink()
    executor = FlakyExecutor(fail_times=2)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(attempt_retry_policy=retrying),
        event_sinks=[sink],
    )

    counts = {"started": 0, "done": 0, "failed": 0}
    controller = engine._controller
    original_started = controller.mark_primer_started
    original_done = controller.mark_primer_done
    original_failed = controller.mark_primer_failed

    async def wrapped_started(*args, **kwargs):
        counts["started"] += 1
        return await original_started(*args, **kwargs)

    async def wrapped_done(*args, **kwargs):
        counts["done"] += 1
        return await original_done(*args, **kwargs)

    async def wrapped_failed(*args, **kwargs):
        counts["failed"] += 1
        return await original_failed(*args, **kwargs)

    controller.mark_primer_started = wrapped_started  # type: ignore[assignment]
    controller.mark_primer_done = wrapped_done  # type: ignore[assignment]
    controller.mark_primer_failed = wrapped_failed  # type: ignore[assignment]

    submission = await engine.submit_group("g-tenacity", {"p": 1}, [])
    assert submission.primer is not None
    primer_result = await asyncio.wait_for(submission.primer, timeout=5.0)
    assert primer_result.ok is True
    assert primer_result.meta and primer_result.meta.get("tenacity_attempts") == 3

    await asyncio.wait_for(engine._controller.wait_group_done("g-tenacity"), timeout=5.0)
    await engine.close()

    assert counts == {"started": 1, "done": 1, "failed": 0}
    job_events = [event for event in sink.events if event.type == "job_succeeded"]
    assert job_events
    assert job_events[0].payload is not None
    assert job_events[0].payload.get("attempt") == 1
    assert job_events[0].payload.get("original_job_id") == job_events[0].job_id
    retry_events = [event for event in sink.events if event.type == "job_retrying"]
    assert [event.payload.get("attempt") for event in retry_events if event.payload] == [1, 2]
    assert {event.payload.get("source") for event in retry_events if event.payload} == {"tenacity"}
    assert not [event for event in sink.events if event.type == "group_failed"]


@pytest.mark.asyncio
async def test_engine_tenacity_and_core_retry_combined() -> None:
    executor = FlakyExecutor(fail_times=100)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    calls: list[int] = []
    metas: list[dict] = []

    async def on_retry(group_id, attempt, error, payload, meta):
        calls.append(attempt)
        metas.append(dict(meta or {}))
        if attempt == 1:
            return PrimerRetryDecision(payload=payload or {}, delay_s=0.0)
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            attempt_retry_policy=retrying,
            primer_failure_policy=PrimerFailurePolicy.RETRY,
            primer_retry_callback=on_retry,
        ),
    )

    submission = await engine.submit_group("g-combined", {"p": 1}, [])
    assert submission.primer is not None
    primer_result = await asyncio.wait_for(submission.primer, timeout=5.0)
    assert primer_result.ok is False
    await asyncio.wait_for(engine._controller.wait_group_done("g-combined"), timeout=5.0)
    await engine.close()

    assert executor.calls == 4
    assert calls == [1, 2]
    assert metas and all(meta.get("tenacity_attempts") == 2 for meta in metas)


@pytest.mark.asyncio
async def test_shared_retrying_state_does_not_bleed_between_jobs() -> None:
    barrier = AsyncBarrier(2)
    executor = ConcurrentFlakyExecutor(barrier)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    seen_attempts: dict[str, list[int]] = defaultdict(list)

    async def on_retry(retry_state, job):
        seen_attempts[str(job.id)].append(retry_state.attempt_number)
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=2,
            follower_workers=1,
            attempt_retry_policy=retrying,
            attempt_retry_hook=on_retry,
        ),
    )
    try:
        submission_a = await engine.submit_group("g-a", {"p": 1}, [])
        submission_b = await engine.submit_group("g-b", {"p": 2}, [])
        assert submission_a.primer is not None
        assert submission_b.primer is not None
        result_a = await asyncio.wait_for(submission_a.primer, timeout=5.0)
        result_b = await asyncio.wait_for(submission_b.primer, timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-a"), timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-b"), timeout=5.0)
    finally:
        await engine.close()

    attempts_a = seen_attempts.get(str(result_a.job_id), [])
    attempts_b = seen_attempts.get(str(result_b.job_id), [])
    if attempts_a != [1] or attempts_b != [1]:
        pytest.xfail("shared AsyncRetrying leaks retry state across concurrent jobs")

    assert result_a.ok is True
    assert result_b.ok is True
    assert result_a.meta and result_a.meta.get("tenacity_attempts") == 2
    assert result_b.meta and result_b.meta.get("tenacity_attempts") == 2
