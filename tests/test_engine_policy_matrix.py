import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.base import EventSink
from acty_core.lifecycle import (
    FollowerDispatchPolicy,
    FollowerFailurePolicy,
    FollowerRetryDecision,
    GroupTaskKind,
    PrimerFailurePolicy,
    PrimerRetryDecision,
)


class _RecordingEventSink(EventSink):
    def __init__(self) -> None:
        self.events = []

    async def handle(self, event) -> None:  # type: ignore[override]
        self.events.append(event)

    def count(self, event_type: str, *, kind: str | None = None, group_id: str | None = None) -> int:
        return sum(
            1
            for event in self.events
            if event.type == event_type
            and (kind is None or event.kind == kind)
            and (group_id is None or event.group_id == group_id)
        )

    def has(self, event_type: str, *, kind: str | None = None, group_id: str | None = None) -> bool:
        return self.count(event_type, kind=kind, group_id=group_id) > 0


async def _wait_for_event_count(
    sink: _RecordingEventSink,
    *,
    event_type: str,
    kind: str | None,
    group_id: str | None,
    expected: int,
    timeout: float = 1.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while sink.count(event_type, kind=kind, group_id=group_id) < expected:
        if loop.time() >= deadline:
            raise AssertionError(
                f"Timed out waiting for {expected} {event_type} events (kind={kind}, group_id={group_id})"
            )
        await asyncio.sleep(0.01)


class _FailFirstFollowerExecutor:
    def __init__(self) -> None:
        self._failed = False

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value:
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)
        if job.kind == GroupTaskKind.FOLLOWER.value and not self._failed:
            self._failed = True
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=False,
                error="follower failed",
                group_id=job.group_id,
                follower_id=job.follower_id,
            )
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class _FailFollowerOnAttemptExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value:
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)
        if job.kind == GroupTaskKind.FOLLOWER.value and job.attempt == 1:
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=False,
                error="follower failed",
                group_id=job.group_id,
                follower_id=job.follower_id,
            )
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class _FailPrimerExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value:
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=False,
                error="primer failed",
                group_id=job.group_id,
            )
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class _FailPrimerOnAttemptExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value and job.attempt == 1:
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=False,
                error="primer failed",
                group_id=job.group_id,
            )
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class _BlockingExecutor:
    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        await self._gate.wait()
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class _BlockingFollowerExecutor:
    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value:
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)
        await self._gate.wait()
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_follower_policy_fail_group_scheduler_events_and_results() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailFirstFollowerExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1, max_followers_inflight=1),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-fail-group",
            {"p": 1},
            [{"f": 1}, {"f": 2}, {"f": 3}],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    finalized = [r for r in follower_results if (r.meta or {}).get("finalized_without_execution")]
    real = [r for r in follower_results if not (r.meta or {}).get("finalized_without_execution")]
    assert len(real) == 1
    assert not real[0].ok
    assert real[0].error == "follower failed"
    assert len(finalized) == 2
    assert sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-fail-group") == 1
    assert sink.has("group_failed", group_id="policy-fail-group")


@pytest.mark.asyncio
async def test_eager_dispatch_queues_all_followers_immediately() -> None:
    gate = asyncio.Event()
    sink = _RecordingEventSink()
    group_id = "policy-eager-queue"
    engine = ActyEngine(
        executor=_BlockingFollowerExecutor(gate),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_dispatch_policy=FollowerDispatchPolicy.eager(),
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            group_id,
            {"p": 1},
            [{"f": 1}, {"f": 2}, {"f": 3}],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        await _wait_for_event_count(
            sink,
            event_type="job_queued",
            kind=GroupTaskKind.FOLLOWER.value,
            group_id=group_id,
            expected=3,
            timeout=1.0,
        )
        assert (
            sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id=group_id)
            == 3
        )
    finally:
        gate.set()
        await engine.close()


@pytest.mark.asyncio
async def test_follower_policy_skip_follower_runs_remaining() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailFirstFollowerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_failure_policy=FollowerFailurePolicy.SKIP_FOLLOWER,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-skip-follower",
            {"p": 1},
            [{"f": 1}, {"f": 2}, {"f": 3}],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert all((r.meta or {}).get("finalized_without_execution") is None for r in follower_results)
    assert sum(1 for r in follower_results if not r.ok) == 1
    assert sum(1 for r in follower_results if r.ok) == 2
    assert sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-skip-follower") == 3
    assert not sink.has("group_failed", group_id="policy-skip-follower")


@pytest.mark.asyncio
async def test_follower_policy_count_as_done_runs_remaining() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailFirstFollowerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_failure_policy=FollowerFailurePolicy.COUNT_AS_DONE_WITH_ERROR,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-count-as-done",
            {"p": 1},
            [{"f": 10}, {"f": 20}, {"f": 30}],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert all((r.meta or {}).get("finalized_without_execution") is None for r in follower_results)
    assert sum(1 for r in follower_results if not r.ok) == 1
    assert sum(1 for r in follower_results if r.ok) == 2
    assert (
        sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-count-as-done")
        == 3
    )
    assert not sink.has("group_failed", group_id="policy-count-as-done")


@pytest.mark.asyncio
async def test_follower_policy_retry_requeues_and_succeeds() -> None:
    sink = _RecordingEventSink()

    async def on_retry(group_id, attempt, error, payload, meta, follower_id=None):
        return FollowerRetryDecision(payload=payload or {"f": 1}, delay_s=0.0)

    engine = ActyEngine(
        executor=_FailFollowerOnAttemptExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_failure_policy=FollowerFailurePolicy.RETRY,
            follower_retry_callback=on_retry,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-retry-follower",
            {"p": 1},
            [{"f": 1}],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_result = await asyncio.wait_for(submission.followers[0], timeout=2.0)
    finally:
        await engine.close()

    assert follower_result.ok
    assert follower_result.meta is not None
    assert follower_result.meta.get("attempt") == 2
    assert sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-retry-follower") == 2
    assert sink.has("job_failed", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-retry-follower")
    assert not sink.has("group_failed", group_id="policy-retry-follower")


@pytest.mark.asyncio
async def test_primer_policy_fail_group_skips_followers() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailPrimerExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1, primer_failure_policy=PrimerFailurePolicy.FAIL_GROUP),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-primer-fail",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        assert submission.primer is not None
        primer_result = await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert not primer_result.ok
    assert all((r.meta or {}).get("finalized_without_execution") for r in follower_results)
    assert sink.count("job_queued", kind=GroupTaskKind.FOLLOWER.value, group_id="policy-primer-fail") == 0
    assert sink.has("group_failed", group_id="policy-primer-fail")


@pytest.mark.asyncio
async def test_primer_policy_run_followers_without_primer() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailPrimerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            primer_failure_policy=PrimerFailurePolicy.RUN_FOLLOWERS_WITHOUT_PRIMER,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-primer-run-followers",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        assert submission.primer is not None
        primer_result = await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert not primer_result.ok
    assert all(r.ok for r in follower_results)
    assert (
        sink.count(
            "job_queued",
            kind=GroupTaskKind.FOLLOWER.value,
            group_id="policy-primer-run-followers",
        )
        == 2
    )
    assert not sink.has("group_failed", group_id="policy-primer-run-followers")


@pytest.mark.asyncio
async def test_primer_policy_skip_followers_marks_group_failed() -> None:
    sink = _RecordingEventSink()
    engine = ActyEngine(
        executor=_FailPrimerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            primer_failure_policy=PrimerFailurePolicy.SKIP_FOLLOWERS,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-primer-skip-followers",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        assert submission.primer is not None
        primer_result = await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert not primer_result.ok
    assert all((r.meta or {}).get("finalized_without_execution") for r in follower_results)
    assert (
        sink.count(
            "job_queued",
            kind=GroupTaskKind.FOLLOWER.value,
            group_id="policy-primer-skip-followers",
        )
        == 0
    )
    assert sink.has("group_failed", group_id="policy-primer-skip-followers")


@pytest.mark.asyncio
async def test_primer_policy_retry_requeues_and_succeeds() -> None:
    sink = _RecordingEventSink()

    async def on_retry(group_id, attempt, error, payload, meta):
        return PrimerRetryDecision(payload=payload or {"p": "retry"}, delay_s=0.0)

    engine = ActyEngine(
        executor=_FailPrimerOnAttemptExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            primer_failure_policy=PrimerFailurePolicy.RETRY,
            primer_retry_callback=on_retry,
        ),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group(
            "policy-primer-retry",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        assert submission.primer is not None
        primer_result = await asyncio.wait_for(submission.primer, timeout=2.0)
        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert primer_result.ok
    assert primer_result.meta is not None
    assert primer_result.meta.get("attempt") == 2
    assert all(r.ok for r in follower_results)
    assert sink.count("job_queued", kind=GroupTaskKind.PRIMER.value, group_id="policy-primer-retry") == 2
    assert not sink.has("group_failed", group_id="policy-primer-retry")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "follower_policy",
    [
        FollowerFailurePolicy.FAIL_GROUP,
        FollowerFailurePolicy.SKIP_FOLLOWER,
        FollowerFailurePolicy.COUNT_AS_DONE_WITH_ERROR,
        FollowerFailurePolicy.RETRY,
    ],
)
async def test_close_all_groups_drop_pending_cancels_futures_across_follower_policies(
    follower_policy: FollowerFailurePolicy,
) -> None:
    gate = asyncio.Event()

    async def _noop_follower_retry(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    engine = ActyEngine(
        executor=_BlockingExecutor(gate),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            follower_failure_policy=follower_policy,
            follower_retry_callback=_noop_follower_retry
            if follower_policy is FollowerFailurePolicy.RETRY
            else None,
        ),
    )
    try:
        submission = await engine.submit_group(
            f"policy-cancel-followers-{follower_policy.value}",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        await engine.close_all_groups(mode="drop_pending")
        gate.set()
    finally:
        await engine.close()

    futures = [submission.primer, *submission.followers]
    assert all(future is not None and future.cancelled() for future in futures)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "primer_policy",
    [
        PrimerFailurePolicy.FAIL_GROUP,
        PrimerFailurePolicy.RUN_FOLLOWERS_WITHOUT_PRIMER,
        PrimerFailurePolicy.SKIP_FOLLOWERS,
        PrimerFailurePolicy.RETRY,
    ],
)
async def test_close_all_groups_drop_pending_cancels_futures_across_primer_policies(
    primer_policy: PrimerFailurePolicy,
) -> None:
    gate = asyncio.Event()

    async def _noop_primer_retry(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    engine = ActyEngine(
        executor=_BlockingExecutor(gate),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            primer_failure_policy=primer_policy,
            primer_retry_callback=_noop_primer_retry if primer_policy is PrimerFailurePolicy.RETRY else None,
        ),
    )
    try:
        submission = await engine.submit_group(
            f"policy-cancel-primers-{primer_policy.value}",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        await engine.close_all_groups(mode="drop_pending")
        gate.set()
    finally:
        await engine.close()

    futures = [submission.primer, *submission.followers]
    assert all(future is not None and future.cancelled() for future in futures)
