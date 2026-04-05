import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.base import EventSink


class _RecordingEventSink(EventSink):
    def __init__(self) -> None:
        self.events = []

    async def handle(self, event) -> None:  # type: ignore[override]
        self.events.append(event)


class _FailPrimerExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=False,
            error="primer failed",
            group_id=job.group_id,
        )


@pytest.mark.asyncio
async def test_job_events_include_invoke_kwargs() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailPrimerExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[sink],
    )
    try:
        payload = {
            "invoke_kwargs": {
                "config": {"run_name": "metric-x"},
                "temperature": 0.2,
                "blob": b"ok",
            }
        }
        submission = await engine.submit_group("g-invoke", payload, [])
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=2.0)
        assert result.ok is False
    finally:
        await engine.close()

    queued_events = [event for event in sink.events if event.type == "job_queued"]
    failed_events = [event for event in sink.events if event.type == "job_failed"]
    assert queued_events
    assert failed_events

    for event in (queued_events[0], failed_events[0]):
        assert event.payload is not None
        invoke_kwargs = event.payload.get("invoke_kwargs")
        assert invoke_kwargs["config"]["run_name"] == "metric-x"
        assert invoke_kwargs["temperature"] == 0.2
        assert "blob" not in invoke_kwargs
