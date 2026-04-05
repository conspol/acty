import asyncio

import pytest

from acty import ActyEngine, EngineConfig, ErrorPayloadPolicy
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.base import EventSink
from acty_core.events.types import Event
from acty_core.lifecycle import FollowerFailurePolicy, GroupTaskKind, PrimerFailurePolicy


class RecordingEventSink(EventSink):
    def __init__(self) -> None:
        self.events: list[Event] = []

    async def handle(self, event: Event) -> None:
        self.events.append(event)


class PolicyExecutor:
    def __init__(self, *, secret: str) -> None:
        self._secret = secret

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        group_id = str(job.group_id) if job.group_id is not None else ""
        if group_id == "g-primer-fail" and job.kind == GroupTaskKind.PRIMER.value:
            raise RuntimeError(f"token={self._secret}")
        if group_id == "g-follower-fail":
            if job.kind == GroupTaskKind.PRIMER.value:
                return JobResult(job_id=job.id, kind=job.kind, ok=True)
            if job.kind == GroupTaskKind.FOLLOWER.value:
                raise RuntimeError(f"token={self._secret}")
        return JobResult(job_id=job.id, kind=job.kind, ok=True)


async def _run_policy_scenario(config: EngineConfig) -> list[Event]:
    sink = RecordingEventSink()
    engine = ActyEngine(
        executor=PolicyExecutor(secret="topsecret"),
        config=config,
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group("g-primer-fail", {"value": "x"}, [])
        assert submission.primer is not None
        await asyncio.wait_for(submission.primer, timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-primer-fail"), timeout=5.0)

        submission = await engine.submit_group("g-follower-fail", {"value": "y"}, [{"value": "z"}])
        assert submission.primer is not None
        await asyncio.wait_for(submission.primer, timeout=5.0)
        assert submission.followers
        await asyncio.wait_for(submission.followers[0], timeout=5.0)
        await asyncio.wait_for(engine._controller.wait_group_done("g-follower-fail"), timeout=5.0)
    finally:
        await engine.close()
    return sink.events


def _error_info_for(events: list[Event], event_type: str, group_id: str) -> dict:
    for event in events:
        if event.type == event_type and event.group_id == group_id:
            return (event.payload or {}).get("error_info") or {}
    raise AssertionError(f"missing {event_type} for {group_id}")


@pytest.mark.asyncio
async def test_error_payload_policy_default_hides_raw_error() -> None:
    events = await _run_policy_scenario(
        EngineConfig(
            primer_failure_policy=PrimerFailurePolicy.FAIL_GROUP,
            follower_failure_policy=FollowerFailurePolicy.FAIL_GROUP,
        )
    )

    error_infos = [
        _error_info_for(events, "job_failed", "g-primer-fail"),
        _error_info_for(events, "primer_failed", "g-primer-fail"),
        _error_info_for(events, "group_failed", "g-primer-fail"),
        _error_info_for(events, "job_failed", "g-follower-fail"),
        _error_info_for(events, "follower_failed", "g-follower-fail"),
        _error_info_for(events, "group_failed", "g-follower-fail"),
    ]

    for info in error_infos:
        assert info.get("raw_error_included") is None
        assert info.get("raw_error_truncated") is None
        assert info.get("raw_error_hash") is not None


@pytest.mark.asyncio
async def test_error_payload_policy_opt_in_redacts_and_respects_hash() -> None:
    policy = ErrorPayloadPolicy(include_raw_error=True, raw_error_max_chars=200, hash_raw_error=False)
    events = await _run_policy_scenario(
        EngineConfig(
            error_payload_policy=policy,
            primer_failure_policy=PrimerFailurePolicy.FAIL_GROUP,
            follower_failure_policy=FollowerFailurePolicy.FAIL_GROUP,
        )
    )

    primer_job = _error_info_for(events, "job_failed", "g-primer-fail")
    primer_failed = _error_info_for(events, "primer_failed", "g-primer-fail")
    primer_group_failed = _error_info_for(events, "group_failed", "g-primer-fail")
    follower_job = _error_info_for(events, "job_failed", "g-follower-fail")
    follower_failed = _error_info_for(events, "follower_failed", "g-follower-fail")
    follower_group_failed = _error_info_for(events, "group_failed", "g-follower-fail")

    for info in [
        primer_job,
        primer_failed,
        primer_group_failed,
        follower_job,
        follower_failed,
        follower_group_failed,
    ]:
        assert info.get("raw_error_included") is True
        assert info.get("raw_error_hash") is None

    for info in [primer_job, primer_failed, follower_job, follower_failed]:
        raw = info.get("raw_error_truncated")
        assert raw is not None
        assert "[redacted]" in raw
        assert "topsecret" not in raw
