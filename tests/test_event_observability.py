import asyncio

import pytest
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.base import EventSink
from acty_core.events.types import Event


class EmptyMessageExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        raise ConnectionError("")


@pytest.mark.asyncio
async def test_engine_emits_retry_and_failure_events() -> None:
    retrying = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(ConnectionError),
    )
    events: list[Event] = []

    class RecordingEventSink(EventSink):
        async def handle(self, event: Event) -> None:
            events.append(event)

    sink = RecordingEventSink()
    engine = ActyEngine(
        executor=EmptyMessageExecutor(),
        config=EngineConfig(attempt_retry_policy=retrying),
        event_sinks=[sink],
    )

    try:
        submission = await engine.submit_group(
            "g-empty-error",
            {"retry_attempts": 2, "invoke_kwargs": {"config": {"run_name": "empty-error"}}},
            [],
        )
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-empty-error"), timeout=5.0)
    finally:
        await engine.close()

    assert result.ok is False

    retry_events = [event for event in events if event.type == "job_retrying"]
    assert retry_events
    assert retry_events[0].payload is not None
    assert retry_events[0].payload.get("source") == "tenacity"

    failed_events = [event for event in events if event.type == "job_failed"]
    assert failed_events
    failed_payload = failed_events[0].payload or {}
    assert failed_payload.get("error")
