import pytest

from acty.result_handlers import compose_handlers
from acty_core.core import Job, JobId, JobResult
from acty_core.result_handlers import AcceptResult, FailResult, JobResultHandlerContext


def _build_context(job: Job) -> JobResultHandlerContext:
    return JobResultHandlerContext(
        run_id=None,
        job_id=job.id,
        group_id=None,
        kind=job.kind,
        original_job_id=job.id,
        result_handler_attempt=1,
        meta=None,
        job=job,
    )


@pytest.mark.asyncio
async def test_compose_handlers_runs_next_on_accept() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={})
    result = JobResult(job_id=job.id, kind=job.kind, ok=True)
    ctx = _build_context(job)
    calls = {"first": 0, "second": 0}

    async def first_handler(result, context):
        _ = result
        _ = context
        calls["first"] += 1
        return AcceptResult()

    def second_handler(result, context):
        _ = result
        _ = context
        calls["second"] += 1
        return FailResult("boom")

    handler = compose_handlers(first_handler, second_handler)
    action = await handler(result, ctx)

    assert calls == {"first": 1, "second": 1}
    assert isinstance(action, FailResult)


@pytest.mark.asyncio
async def test_compose_handlers_short_circuits_on_non_accept() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={})
    result = JobResult(job_id=job.id, kind=job.kind, ok=True)
    ctx = _build_context(job)
    calls = {"first": 0, "second": 0}

    def first_handler(result, context):
        _ = result
        _ = context
        calls["first"] += 1
        return FailResult("stop")

    async def second_handler(result, context):
        _ = result
        _ = context
        calls["second"] += 1
        return AcceptResult()

    handler = compose_handlers(first_handler, second_handler)
    action = await handler(result, ctx)

    assert calls == {"first": 1, "second": 0}
    assert isinstance(action, FailResult)


@pytest.mark.asyncio
async def test_compose_handlers_runs_next_on_none() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={})
    result = JobResult(job_id=job.id, kind=job.kind, ok=True)
    ctx = _build_context(job)
    calls = {"first": 0, "second": 0}

    def first_handler(result, context):
        _ = result
        _ = context
        calls["first"] += 1
        return None

    async def second_handler(result, context):
        _ = result
        _ = context
        calls["second"] += 1
        return AcceptResult()

    handler = compose_handlers(first_handler, second_handler)
    action = await handler(result, ctx)

    assert calls == {"first": 1, "second": 1}
    assert isinstance(action, AcceptResult)
