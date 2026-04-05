import asyncio

import pytest

from acty import ActyEngine, EngineConfig, ResubmitJob
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import (
    FollowerDispatchPolicy,
    FollowerFailurePolicy,
    FollowerRetryDecision,
    GroupTaskKind,
    PrimerFailurePolicy,
)


class _Executor:
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
            output={"payload": job.payload},
            group_id=job.group_id,
        )


class _BareExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        return JobResult(job_id=job.id, kind=job.kind, ok=True)


class _RetryFollowerExecutor:
    def __init__(self) -> None:
        self._follower_calls = 0

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value:
            return JobResult(job_id=job.id, kind=job.kind, ok=True)
        if job.kind == GroupTaskKind.FOLLOWER.value:
            self._follower_calls += 1
            if self._follower_calls == 1:
                raise RuntimeError("boom")
            return JobResult(job_id=job.id, kind=job.kind, ok=True)
        return JobResult(job_id=job.id, kind=job.kind, ok=True)


class _FailPrimerExecutor:
    """Fail the primer so followers are never enqueued."""

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


class _FailFirstFollowerExecutor:
    """Fail the first follower to force group failure with pending followers."""

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


class _FailFirstFollowerDelaySecondExecutor:
    """Fail one follower, delay another to stay in-flight when group finalizes."""

    def __init__(self, delay_s: float = 0.01) -> None:
        self._failed = False
        self._delay_s = delay_s

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
        if job.kind == GroupTaskKind.FOLLOWER.value:
            # Small delay keeps one follower in-flight while finalization runs.
            await asyncio.sleep(self._delay_s)
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class _BlockingResubmitExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.proceed = asyncio.Event()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        _ = pool
        self.started.set()
        await self.proceed.wait()
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=False,
            error="boom",
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_engine_submit_group_returns_futures(engine_config_small) -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=engine_config_small)
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group("g1", {"p": 1}, [{"f": 1}, {"f": 2}])
        assert submission.primer is not None
        assert len(submission.followers) == 2

        results = await asyncio.gather(submission.primer, *submission.followers)
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 2
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_submit_follower_returns_future(engine_config_small) -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=engine_config_small)
    executor.bind(engine._controller)
    try:
        submission = await engine.submit_group("g2", {"p": 1}, None)
        if submission.primer is not None:
            await submission.primer
        follower_future = await engine.submit_follower("g2", {"f": 1})
        await engine.close_group("g2")
        follower_result = await follower_future
        assert follower_result.kind == GroupTaskKind.FOLLOWER.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_engine_results_stream_collects_results(engine_config_small) -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=engine_config_small)
    executor.bind(engine._controller)

    async def _collect():
        collected = []
        async for item in engine.results():
            collected.append(item)
        return collected

    collector = asyncio.create_task(_collect())
    try:
        submission = await engine.submit_group("g3", {"p": 1}, [{"f": 1}, {"f": 2}])
        if submission.primer is not None:
            await submission.primer
        await asyncio.gather(*submission.followers)
    finally:
        await engine.close()

    results = await asyncio.wait_for(collector, timeout=1.0)
    kinds = [result.kind for result in results]
    assert kinds.count(GroupTaskKind.PRIMER.value) == 1
    assert kinds.count(GroupTaskKind.FOLLOWER.value) == 2


@pytest.mark.asyncio
async def test_batch_follower_ids_propagate_to_results(engine_config_small) -> None:
    engine = ActyEngine(executor=_BareExecutor(), config=engine_config_small)
    try:
        submission = await engine.submit_group(
            "g4",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
            follower_ids=["f1", "f2"],
        )
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        results = await asyncio.gather(
            *[asyncio.wait_for(fut, timeout=2.0) for fut in submission.followers]
        )
    finally:
        await engine.close()

    assert [result.follower_id for result in results] == ["f1", "f2"]


@pytest.mark.asyncio
async def test_engine_results_stream_includes_retry_attempts(engine_config_small) -> None:
    async def on_retry(group_id, attempt, error, payload, meta, follower_id=None):
        return FollowerRetryDecision(payload=payload or {}, delay_s=0.0)

    executor = _RetryFollowerExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=engine_config_small.primer_workers,
            follower_workers=engine_config_small.follower_workers,
            follower_failure_policy=FollowerFailurePolicy.RETRY,
            follower_retry_callback=on_retry,
        ),
    )

    async def collect():
        collected = []
        async for item in engine.results():
            collected.append(item)
        return collected

    collector = asyncio.create_task(collect())
    try:
        submission = await engine.submit_group("g-retry-stream", {"p": 1}, [{"f": 1}])
        if submission.primer is not None:
            await asyncio.wait_for(submission.primer, timeout=2.0)
        await asyncio.wait_for(submission.followers[0], timeout=2.0)
    finally:
        await engine.close()

    results = await asyncio.wait_for(collector, timeout=2.0)
    assert len(results) == 3
    follower_results = [r for r in results if r.kind == GroupTaskKind.FOLLOWER.value]
    assert len(follower_results) == 2
    assert any(not r.ok for r in follower_results)
    assert any(r.ok for r in follower_results)
    attempts = sorted(r.meta.get("attempt") for r in follower_results if r.meta)
    assert attempts == [1, 2]
    original_ids = {r.meta.get("original_job_id") for r in follower_results if r.meta}
    assert len(original_ids) == 1


@pytest.mark.asyncio
async def test_resubmit_skipped_after_cancel_does_not_create_synthetic_failure(
    engine_config_small,
) -> None:
    executor = _BlockingResubmitExecutor()

    async def handler(result: JobResult, ctx):
        _ = result
        _ = ctx
        return ResubmitJob(payload={"value": "retry"}, reason="repair")

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=engine_config_small.primer_workers,
            follower_workers=engine_config_small.follower_workers,
            job_result_handler=handler,
        ),
    )
    try:
        submission = await engine.submit_group("g-resubmit-cancel", {"p": 1}, [])
        assert submission.primer is not None
        await asyncio.wait_for(executor.started.wait(), timeout=1.0)
        await engine._controller.cancel_group("g-resubmit-cancel", reason="test_cancel")
        executor.proceed.set()
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
    finally:
        executor.proceed.set()
        await engine.close()

    assert result.error != "group completed before job was enqueued"
    assert result.meta is not None
    assert result.meta.get("result_handler_action") == "resubmit_skipped"
    assert result.meta.get("result_handler_reason") == "group_cancelled"
    assert "result_handler_retry_scheduled" not in result.meta
    assert not result.meta.get("finalized_without_execution", False)


@pytest.mark.asyncio
async def test_resubmit_skipped_after_group_removed_does_not_create_synthetic_failure(
    engine_config_small,
) -> None:
    executor = _BlockingResubmitExecutor()

    async def handler(result: JobResult, ctx):
        _ = result
        _ = ctx
        return ResubmitJob(payload={"value": "retry"}, reason="repair")

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=engine_config_small.primer_workers,
            follower_workers=engine_config_small.follower_workers,
            job_result_handler=handler,
        ),
    )
    try:
        submission = await engine.submit_group("g-resubmit-missing", {"p": 1}, [])
        assert submission.primer is not None
        await asyncio.wait_for(executor.started.wait(), timeout=1.0)
        await engine._controller._finalize_group("g-resubmit-missing", ok=True, reason=None)
        executor.proceed.set()
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
    finally:
        executor.proceed.set()
        await engine.close()

    assert result.error != "group completed before job was enqueued"
    assert result.meta is not None
    assert result.meta.get("result_handler_action") == "resubmit_skipped"
    assert result.meta.get("result_handler_reason") == "group_missing"
    assert "result_handler_retry_scheduled" not in result.meta
    assert not result.meta.get("finalized_without_execution", False)


@pytest.mark.asyncio
async def test_group_failure_finalizes_pending_followers() -> None:
    engine = ActyEngine(
        executor=_FailPrimerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            primer_failure_policy=PrimerFailurePolicy.FAIL_GROUP,
        ),
    )
    try:
        submission = await engine.submit_group(
            "g-fail-primer",
            {"p": 1},
            [{"f": 1}, {"f": 2}],
        )
        assert submission.primer is not None
        primer_result = await asyncio.wait_for(submission.primer, timeout=2.0)
        assert not primer_result.ok

        follower_results = await asyncio.wait_for(
            asyncio.gather(*submission.followers), timeout=2.0
        )
    finally:
        await engine.close()

    assert all(not r.ok for r in follower_results)
    assert all(r.meta and r.meta.get("finalized_without_execution") for r in follower_results)


@pytest.mark.asyncio
async def test_follower_failure_finalizes_remaining_followers() -> None:
    engine = ActyEngine(
        executor=_FailFirstFollowerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_failure_policy=FollowerFailurePolicy.FAIL_GROUP,
        ),
    )
    try:
        submission = await engine.submit_group(
            "g-fail-follower",
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

    assert any(not r.ok and not (r.meta or {}).get("finalized_without_execution") for r in follower_results)
    assert any((r.meta or {}).get("finalized_without_execution") for r in follower_results)


@pytest.mark.asyncio
async def test_follower_failure_finalizes_remaining_followers_many_ids() -> None:
    engine = ActyEngine(
        executor=_FailFirstFollowerExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_followers_inflight=1,
            follower_failure_policy=FollowerFailurePolicy.FAIL_GROUP,
        ),
    )
    follower_payloads = [{"f": idx} for idx in range(1, 7)]
    follower_ids = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
    try:
        submission = await engine.submit_group(
            "group-fail-follower-long-id",
            {"p": "primer"},
            follower_payloads,
            follower_ids=follower_ids,
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
    assert len(finalized) == len(follower_payloads) - 1
    assert all(not r.ok for r in finalized)


@pytest.mark.asyncio
async def test_follower_failure_keeps_inflight_followers_real() -> None:
    engine = ActyEngine(
        executor=_FailFirstFollowerDelaySecondExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=2,
            max_followers_inflight=2,
            follower_dispatch_policy=FollowerDispatchPolicy(mode="target", target_total=2),
            follower_failure_policy=FollowerFailurePolicy.FAIL_GROUP,
        ),
    )
    follower_payloads = [{"f": "one"}, {"f": "two"}, {"f": "three"}, {"f": "four"}]
    follower_ids = ["red", "blue", "green", "yellow"]
    try:
        submission = await engine.submit_group(
            "group-inflight-followers",
            {"p": "primer"},
            follower_payloads,
            follower_ids=follower_ids,
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
    assert len(real) == 2
    assert any(not r.ok and r.error == "follower failed" for r in real)
    assert len(finalized) == len(follower_payloads) - 2
    assert all(not r.ok for r in finalized)
