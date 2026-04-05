import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import (
    FollowerFailurePolicy,
    FollowerRetryDecision,
    GroupTaskKind,
    PrimerFailurePolicy,
    PrimerRetryDecision,
)


class RetryExecutor:
    handles_lifecycle = True

    def __init__(self, *, fail_primer: bool = True) -> None:
        self._controller = None
        self.primer_payloads: list[dict] = []
        self.follower_payloads: list[dict] = []
        self._fail_primer = fail_primer

    def bind(self, controller) -> None:
        self._controller = controller

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            self.primer_payloads.append(dict(job.payload))
            if self._fail_primer and len(self.primer_payloads) == 1:
                retry_scheduled = await controller.mark_primer_failed(
                    group_id,
                    RuntimeError("primer-fail"),
                    payload=job.payload,
                    job_id=job.id,
                )
                return JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    error="primer-fail",
                    group_id=job.group_id,
                    meta={"core_retry_scheduled": retry_scheduled},
                )
            await controller.mark_primer_done(group_id)
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)

        if job.kind == GroupTaskKind.FOLLOWER.value:
            self.follower_payloads.append(dict(job.payload))
            if len(self.follower_payloads) == 1:
                retry_scheduled = await controller.mark_follower_failed(
                    group_id,
                    RuntimeError("follower-fail"),
                    payload=job.payload,
                    job_id=job.id,
                    follower_id="f1",
                )
                return JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    error="follower-fail",
                    group_id=job.group_id,
                    follower_id=job.follower_id,
                    meta={"core_retry_scheduled": retry_scheduled},
                )
            await controller.mark_follower_done(group_id)
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)

        return JobResult(job_id=job.id, kind=job.kind, ok=True)


class BareFailOnceExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []
        self._failed = False

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        follower_id = str(job.follower_id) if job.follower_id is not None else None
        self.calls.append((job.kind, follower_id))
        if job.kind == GroupTaskKind.FOLLOWER.value and not self._failed:
            self._failed = True
            raise RuntimeError("boom")
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["primer", "follower"])
async def test_engine_retry_policy_pass_through(kind: str) -> None:
    calls: list[tuple] = []
    payloads: list[dict] = []
    metas: list[dict] = []

    if kind == "primer":

        async def on_retry(group_id, attempt, error, payload, meta):
            calls.append((group_id, attempt))
            payloads.append(dict(payload or {}))
            metas.append(dict(meta or {}))
            return PrimerRetryDecision(payload={"retry": attempt}, delay_s=0.0)

        executor = RetryExecutor(fail_primer=True)
        engine = ActyEngine(
            executor=executor,
            config=EngineConfig(
                primer_failure_policy=PrimerFailurePolicy.RETRY,
                primer_retry_callback=on_retry,
            ),
        )
        executor.bind(engine._controller)

        submission = await engine.submit_group("g-primer", {"p": 1}, [])
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
        await engine.close()

        assert result.ok is True
        assert calls == [("g-primer", 1)]
        assert payloads == [{"p": 1}]
        assert metas and metas[0].get("kind") == GroupTaskKind.PRIMER.value
        assert executor.primer_payloads == [{"p": 1}, {"retry": 1}]
    else:

        async def on_retry(group_id, attempt, error, payload, meta, follower_id=None):
            calls.append((group_id, attempt, follower_id))
            payloads.append(dict(payload or {}))
            metas.append(dict(meta or {}))
            return FollowerRetryDecision(payload={"retry": attempt}, delay_s=0.0)

        executor = RetryExecutor(fail_primer=False)
        engine = ActyEngine(
            executor=executor,
            config=EngineConfig(
                follower_failure_policy=FollowerFailurePolicy.RETRY,
                follower_retry_callback=on_retry,
            ),
        )
        executor.bind(engine._controller)

        submission = await engine.submit_group("g-follow", {"p": 1}, [{"f": 1}])
        assert submission.primer is not None
        await asyncio.wait_for(submission.primer, timeout=5.0)
        assert len(submission.followers) == 1
        result = await asyncio.wait_for(submission.followers[0], timeout=5.0)
        assert result.ok is True
        await asyncio.wait_for(engine._controller.wait_group_done("g-follow"), timeout=5.0)
        await engine.close()

        assert calls == [("g-follow", 1, "f1")]
        assert payloads == [{"f": 1}]
        assert metas and metas[0].get("kind") == GroupTaskKind.FOLLOWER.value
        assert executor.follower_payloads == [{"f": 1}, {"retry": 1}]


@pytest.mark.asyncio
@pytest.mark.parametrize("include_meta, expected_ok", [(False, False), (True, True)])
async def test_final_future_uses_core_retry_meta(include_meta: bool, expected_ok: bool) -> None:
    async def on_retry(group_id, attempt, error, payload, meta):
        if attempt == 1:
            return PrimerRetryDecision(payload=payload or {}, delay_s=0.0)
        return None

    class LifecycleRetryExecutor:
        handles_lifecycle = True

        def __init__(self) -> None:
            self._controller = None
            self.attempts = 0

        def bind(self, controller) -> None:
            self._controller = controller

        async def execute(self, job: Job, *, pool: str) -> JobResult:
            controller = self._controller
            assert controller is not None
            group_id = str(job.group_id) if job.group_id is not None else ""
            await controller.mark_primer_started(group_id)
            self.attempts += 1
            if self.attempts == 1:
                retry_scheduled = await controller.mark_primer_failed(
                    group_id,
                    RuntimeError("primer-fail"),
                    payload=job.payload,
                    job_id=job.id,
                )
                meta = {"core_retry_scheduled": retry_scheduled} if include_meta else None
                return JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    error="primer-fail",
                    group_id=job.group_id,
                    meta=meta,
                )
            await controller.mark_primer_done(group_id)
            return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)

    executor = LifecycleRetryExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            auto_wrap_executor=False,
            primer_failure_policy=PrimerFailurePolicy.RETRY,
            primer_retry_callback=on_retry,
        ),
    )

    submission = await engine.submit_group("g-meta", {"p": 1}, [])
    assert submission.primer is not None
    primer_result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await asyncio.wait_for(engine._controller.wait_group_done("g-meta"), timeout=5.0)
    await engine.close()

    assert executor.attempts == 2
    assert primer_result.ok is expected_ok


@pytest.mark.asyncio
async def test_follower_id_propagates_with_adapter_retry() -> None:
    calls: list[str | None] = []

    async def on_retry(group_id, attempt, error, payload, meta, follower_id=None):
        calls.append(follower_id)
        return FollowerRetryDecision(payload={"retry": attempt}, delay_s=0.0)

    executor = BareFailOnceExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            follower_failure_policy=FollowerFailurePolicy.RETRY,
            follower_retry_callback=on_retry,
        ),
    )

    submission = await engine.submit_group("g-follow-id", {"p": 1}, None)
    assert submission.primer is not None
    await asyncio.wait_for(submission.primer, timeout=5.0)

    follower_future = await engine.submit_follower("g-follow-id", {"f": 1}, follower_id="f-123")
    result = await asyncio.wait_for(follower_future, timeout=5.0)
    assert result.ok is True
    assert result.follower_id == "f-123"

    await engine.close_group("g-follow-id")
    await asyncio.wait_for(engine._controller.wait_group_done("g-follow-id"), timeout=5.0)
    await engine.close()

    assert calls == ["f-123"]
    assert executor.calls.count(("follower", "f-123")) >= 1
