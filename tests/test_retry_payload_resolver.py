import asyncio
import functools

import pytest
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from acty import TenacityRetryAdapter, resolver_retry_payload_on_retry
from acty_core.core.types import Job, JobId, JobResult


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls = 0
        self.payloads: list[dict] = []

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self.calls += 1
        self.payloads.append(dict(job.payload))
        if self.calls == 1:
            raise RuntimeError("boom")
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class AsyncResolver:
    def __init__(self, runnable: object) -> None:
        self._runnable = runnable

    async def resolve(self, exec_id: str, *, job: Job):
        await asyncio.sleep(0)
        return self._runnable


@pytest.mark.asyncio
async def test_retry_payload_on_retry_resolves_exec_id() -> None:
    runnable = object()
    resolver = AsyncResolver(runnable)
    seen_payloads: list[dict] = []

    def retry_payload_fn(attempt: int, error: Exception, payload: dict):
        seen_payloads.append(dict(payload))
        return {"input": {"attempt": attempt}}

    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    payload = {
        "exec_id": "exec-1",
        "input": {"attempt": 0},
        "retry": retrying,
        "retry_payload_fn": retry_payload_fn,
    }
    job = Job(id=JobId("job-resolver"), kind="primer", payload=payload)
    executor = RecordingExecutor()
    adapter = TenacityRetryAdapter(
        executor,
        retrying,
        on_retry=functools.partial(resolver_retry_payload_on_retry, resolver=resolver),
    )

    result = await adapter.execute(job, pool="primer")

    assert result.ok is True
    assert executor.calls == 2
    assert "runnable" not in payload
    assert "runnable" not in executor.payloads[0]
    assert seen_payloads and seen_payloads[0]["runnable"] is runnable
    assert executor.payloads[1]["input"] == {"attempt": 1}
    assert executor.payloads[1]["exec_id"] == "exec-1"
    assert executor.payloads[1]["retry_payload_fn"] is retry_payload_fn
    assert executor.payloads[1]["retry"] is retrying
    assert "runnable" not in executor.payloads[1]


@pytest.mark.asyncio
async def test_retry_payload_preserves_wrapper_keys() -> None:
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    seen_payloads: list[dict] = []

    def retry_payload_fn(attempt: int, error: Exception, payload: dict):
        _ = error
        seen_payloads.append(dict(payload))
        return {"input": {"attempt": attempt}}

    payload = {
        "exec_id": "exec-1",
        "trace_context": {"trace_id": "t1"},
        "extra": "keep",
        "payload": {
            "input": {"attempt": 0},
            "retry": retrying,
            "retry_payload_fn": retry_payload_fn,
        },
    }
    job = Job(id=JobId("job-wrapper"), kind="primer", payload=payload)
    executor = RecordingExecutor()
    adapter = TenacityRetryAdapter(
        executor,
        retrying,
        on_retry=functools.partial(resolver_retry_payload_on_retry, resolver=None),
    )

    result = await adapter.execute(job, pool="primer")

    assert result.ok is True
    assert executor.calls == 2
    assert seen_payloads and "extra" not in seen_payloads[0]
    retry_payload = executor.payloads[1]
    assert retry_payload["exec_id"] == "exec-1"
    assert retry_payload["trace_context"] == {"trace_id": "t1"}
    assert retry_payload["extra"] == "keep"
    assert "retry" not in retry_payload
    assert "retry_payload_fn" not in retry_payload
    assert "payload" in retry_payload
    assert retry_payload["payload"]["input"] == {"attempt": 1}
    assert retry_payload["payload"]["retry"] is retrying
    assert retry_payload["payload"]["retry_payload_fn"] is retry_payload_fn
