import pytest

from acty import MappingExecResolver, ResolverExecutor
from acty_core.core.types import GroupId, Job, JobId, JobResult

import acty.executors as acty_executors


class RecordingExecutor:
    def __init__(self) -> None:
        self.jobs: list[Job] = []
        self._supports_executor_retry = False

    @property
    def supports_executor_retry(self) -> bool:
        return self._supports_executor_retry

    @supports_executor_retry.setter
    def supports_executor_retry(self, value: bool) -> None:
        self._supports_executor_retry = bool(value)

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self.jobs.append(job)
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
        )


class CountingResolver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def resolve(self, exec_id: str, *, job: Job):
        self.calls.append(exec_id)
        return {"id": exec_id}


class DummyLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def error(self, event: str, **kwargs) -> None:
        self.calls.append((event, kwargs))


@pytest.mark.asyncio
async def test_resolver_executor_resolves_exec_id_top_level() -> None:
    runnable = object()
    resolver = MappingExecResolver({"exec-1": runnable})
    inner = RecordingExecutor()
    wrapper = ResolverExecutor(inner, resolver)

    payload = {"exec_id": "exec-1", "input": {"x": 1}}
    job = Job(id=JobId("job-1"), kind="primer", payload=payload, group_id=GroupId("g-1"))

    await wrapper.execute(job, pool="primer")

    assert "runnable" not in payload
    assert inner.jobs
    resolved_payload = inner.jobs[0].payload
    assert resolved_payload["exec_id"] == "exec-1"
    assert resolved_payload["runnable"] is runnable
    assert resolved_payload["input"] == {"x": 1}


@pytest.mark.asyncio
async def test_resolver_executor_resolves_exec_id_wrapped_payload() -> None:
    runnable = object()
    resolver = MappingExecResolver({"exec-1": runnable})
    inner = RecordingExecutor()
    wrapper = ResolverExecutor(inner, resolver)

    inner_payload = {"exec_id": "exec-1", "input": {"x": 1}}
    payload = {"payload": inner_payload, "meta": "keep"}
    job = Job(id=JobId("job-2"), kind="primer", payload=payload, group_id=GroupId("g-2"))

    await wrapper.execute(job, pool="primer")

    assert "runnable" not in inner_payload
    assert inner.jobs
    resolved_payload = inner.jobs[0].payload
    assert resolved_payload["meta"] == "keep"
    assert resolved_payload["payload"]["runnable"] is runnable
    assert resolved_payload["payload"]["input"] == {"x": 1}


@pytest.mark.asyncio
async def test_resolver_executor_missing_resolver_logs_error() -> None:
    inner = RecordingExecutor()
    wrapper = ResolverExecutor(inner, None)
    payload = {"exec_id": "exec-2", "input": 2}
    job = Job(id=JobId("job-3"), kind="primer", payload=payload, group_id=GroupId("g-3"))

    dummy_logger = DummyLogger()
    original_logger = acty_executors.logger
    acty_executors.logger = dummy_logger
    try:
        await wrapper.execute(job, pool="primer")
    finally:
        acty_executors.logger = original_logger

    assert inner.jobs
    assert inner.jobs[0].payload == payload
    assert dummy_logger.calls
    event, fields = dummy_logger.calls[0]
    assert event == "exec_id provided but no resolver configured"
    assert fields["exec_id"] == "exec-2"
    assert fields["job_id"] == "job-3"
    assert fields["group_id"] == "g-3"
    assert fields["kind"] == "primer"
    assert set(fields["payload_keys"]) == {"exec_id", "input"}


@pytest.mark.asyncio
async def test_resolver_executor_skips_when_runnable_present() -> None:
    resolver = CountingResolver()
    inner = RecordingExecutor()
    wrapper = ResolverExecutor(inner, resolver)

    runnable = object()
    payload = {"exec_id": "exec-3", "runnable": runnable, "input": "hi"}
    job = Job(id=JobId("job-4"), kind="primer", payload=payload, group_id=GroupId("g-4"))

    await wrapper.execute(job, pool="primer")

    assert resolver.calls == []
    assert inner.jobs
    assert inner.jobs[0].payload["runnable"] is runnable
