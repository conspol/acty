import asyncio

import pytest
from structlog.testing import capture_logs
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from acty import (
    ActyEngine,
    EngineConfig,
    FailResult,
    ResubmitJob,
    ResultHandlerErrorPolicy,
)
from acty_core.cache import CacheRegistry, InMemoryStorage
from acty_core.core.types import Job, JobResult
from acty_core.events import EventBus
from acty_core.events.sinks.base import EventSink
from acty_core.lifecycle import (
    GroupDependency,
    GroupDependencyPolicy,
    GroupTaskKind,
    PrimerFailurePolicy,
    PrimerRetryDecision,
)


class RecordingEventSink(EventSink):
    def __init__(self) -> None:
        self.events = []

    async def handle(self, event) -> None:
        self.events.append(event)


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        payload = dict(job.payload) if isinstance(job.payload, dict) else {}
        self.calls.append((job.kind, payload))
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class DummyProvider:
    def __init__(self) -> None:
        self.name = "dummy"

    def fingerprint(self, primer, context=None) -> str:
        _ = context
        return f"fp:{primer}"

    async def create(self, primer, context=None):
        _ = context
        return f"ref:{primer}"

    async def wait_ready(self, provider_ref, timeout_s=None):
        _ = provider_ref
        _ = timeout_s
        return None

    async def invalidate(self, provider_ref):
        _ = provider_ref
        return None


class ContextRecordingExecutor:
    def __init__(self) -> None:
        self.contexts: list[object | None] = []
        self.cache_handles: list[object | None] = []

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        _ = pool
        self.contexts.append(job.context)
        self.cache_handles.append(job.cache_handle)
        payload = dict(job.payload) if isinstance(job.payload, dict) else {}
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_result_handler_resubmit_primer() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        if ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"}, reason="repair")
        return None

    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(job_result_handler=handler),
    )
    submission = await engine.submit_group("g1", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert result.output is not None
    assert result.output["payload"]["value"] == "repair"
    assert executor.calls == [
        (GroupTaskKind.PRIMER.value, {"value": "orig"}),
        (GroupTaskKind.PRIMER.value, {"value": "repair"}),
    ]

    retry_events = [
        event
        for event in sink.events
        if event.type == "job_retrying"
        and event.payload
        and event.payload.get("source") == "result_handler"
    ]
    assert retry_events
    assert retry_events[0].payload.get("result_handler_attempt") == 1

    handled = [event for event in sink.events if event.type == "job_result_handled"]
    assert handled
    assert handled[0].payload.get("action") == "resubmit"
    assert handled[0].payload.get("reason") == "repair"


@pytest.mark.asyncio
async def test_result_handler_resubmit_with_admission() -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        if ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"})
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            job_result_handler=handler,
            max_active_groups=1,
        ),
    )
    submission = await engine.submit_group("g-admit", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert result.output is not None
    assert result.output["payload"]["value"] == "repair"


@pytest.mark.asyncio
async def test_result_handler_resubmit_with_dependency_gate() -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        if str(ctx.group_id) == "g-dependent" and ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"})
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(job_result_handler=handler),
    )

    dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
    submission_a = await engine.submit_group("g-dep", {"value": "a"}, [])
    submission_b = await engine.submit_group("g-dependent", {"value": "b"}, [], depends_on=[dep])
    assert submission_a.primer is not None
    assert submission_b.primer is not None
    await asyncio.wait_for(submission_a.primer, timeout=5.0)
    result_b = await asyncio.wait_for(submission_b.primer, timeout=5.0)
    await engine.close()

    assert result_b.ok is True
    assert result_b.output is not None
    assert result_b.output["payload"]["value"] == "repair"


@pytest.mark.asyncio
async def test_result_handler_resubmit_with_core_retry() -> None:
    calls: list[str] = []

    class FailOnceExecutor:
        def __init__(self) -> None:
            self._failed = False

        async def execute(self, job: Job, *, pool: str) -> JobResult:
            calls.append(job.kind)
            if not self._failed:
                self._failed = True
                raise RuntimeError("boom")
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=True,
                output={"payload": dict(job.payload)},
                group_id=job.group_id,
            )

    async def on_retry(group_id, attempt, error, payload, meta):
        return PrimerRetryDecision(payload=payload or {}, delay_s=0.0)

    async def handler(result: JobResult, ctx):
        if result.ok and ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"})
        return None

    engine = ActyEngine(
        executor=FailOnceExecutor(),
        config=EngineConfig(
            primer_failure_policy=PrimerFailurePolicy.RETRY,
            primer_retry_callback=on_retry,
            job_result_handler=handler,
        ),
    )
    submission = await engine.submit_group("g-core", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert result.meta is not None
    assert result.meta.get("attempt") == 2
    assert calls == [
        GroupTaskKind.PRIMER.value,
        GroupTaskKind.PRIMER.value,
        GroupTaskKind.PRIMER.value,
    ]


@pytest.mark.asyncio
async def test_result_handler_resubmit_with_tenacity_retry() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])

    class FailOnceExecutor:
        def __init__(self) -> None:
            self.calls: list[dict] = []
            self._failed = False

        async def execute(self, job: Job, *, pool: str) -> JobResult:
            _ = pool
            payload = dict(job.payload) if isinstance(job.payload, dict) else {}
            self.calls.append(payload)
            if not self._failed:
                self._failed = True
                raise RuntimeError("boom")
            return JobResult(
                job_id=job.id,
                kind=job.kind,
                ok=True,
                output={"payload": payload},
                group_id=job.group_id,
                follower_id=job.follower_id,
            )

    async def handler(result: JobResult, ctx):
        if result.ok and ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"})
        return None

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    executor = FailOnceExecutor()
    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(
            attempt_retry_policy=retry_policy,
            job_result_handler=handler,
        ),
    )
    submission = await engine.submit_group("g-tenacity", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert result.output is not None
    assert result.output["payload"]["value"] == "repair"
    assert len(executor.calls) == 3

    retry_events = [
        event for event in sink.events if event.type == "job_retrying" and event.payload
    ]
    sources = [event.payload.get("source") for event in retry_events]
    assert sources.count("tenacity") == 1
    assert sources.count("result_handler") == 1

    original_job_id = next(
        event.job_id
        for event in sink.events
        if event.type == "job_queued"
        and event.payload
        and event.payload.get("attempt") == 1
        and event.payload.get("original_job_id") == event.job_id
    )
    assert result.meta is not None
    assert result.meta.get("original_job_id") == original_job_id


@pytest.mark.asyncio
async def test_result_handler_resubmit_follower() -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        if ctx.kind == GroupTaskKind.FOLLOWER.value and ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "retry"})
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(job_result_handler=handler),
    )
    submission = await engine.submit_group("g-follow", {"value": "primer"}, [{"value": "orig"}])
    assert submission.primer is not None
    await asyncio.wait_for(submission.primer, timeout=5.0)
    assert len(submission.followers) == 1
    follower_result = await asyncio.wait_for(submission.followers[0], timeout=5.0)
    await asyncio.wait_for(engine._controller.wait_group_done("g-follow"), timeout=5.0)
    await engine.close()

    assert follower_result.ok is True
    assert follower_result.output is not None
    assert follower_result.output["payload"]["value"] == "retry"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy,expected_ok,expected_error",
    [
        (ResultHandlerErrorPolicy.FAIL, False, "result_handler_failed"),
        (ResultHandlerErrorPolicy.PASS_THROUGH, True, None),
    ],
)
async def test_result_handler_error_policy(policy, expected_ok, expected_error) -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_error_policy=policy,
        ),
    )
    submission = await engine.submit_group("g-error-policy", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is expected_ok
    assert result.error == expected_error


@pytest.mark.asyncio
async def test_result_handler_retry_logs_attempts() -> None:
    executor = RecordingExecutor()
    calls: list[int] = []
    original_job_id: list[str] = []

    async def handler(result: JobResult, ctx):
        if not original_job_id:
            original_job_id.append(str(ctx.original_job_id))
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("handler boom")
        return None

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    with capture_logs() as logs:
        engine = ActyEngine(
            executor=executor,
            config=EngineConfig(
                job_result_handler=handler,
                result_handler_retry_policy=retry_policy,
            ),
        )
        submission = await engine.submit_group("g-handler-log", {"value": "orig"}, [])
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
        await engine.close()

    assert result.ok is True
    retry_logs = [entry for entry in logs if entry.get("event") == "result_handler_retry"]
    assert [entry.get("attempt") for entry in retry_logs] == [1, 2]
    for entry in retry_logs:
        assert entry.get("handler") == "handler"
        assert entry.get("job_id") is not None
        assert entry.get("original_job_id") == original_job_id[0]


@pytest.mark.asyncio
async def test_result_handler_retry_logs_attempts_on_exhaustion() -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    with capture_logs() as logs:
        engine = ActyEngine(
            executor=executor,
            config=EngineConfig(
                job_result_handler=handler,
                result_handler_retry_policy=retry_policy,
            ),
        )
        submission = await engine.submit_group("g-handler-log-exhaust", {"value": "orig"}, [])
        assert submission.primer is not None
        await asyncio.wait_for(submission.primer, timeout=5.0)
        await engine.close()

    retry_logs = [entry for entry in logs if entry.get("event") == "result_handler_retry"]
    assert [entry.get("attempt") for entry in retry_logs] == [1, 2]


@pytest.mark.asyncio
async def test_result_handler_retry_error_callback_action_used() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    def retry_error_callback(retry_state):
        _ = retry_state
        return FailResult(error="handler_retry_exhausted", reason="exhausted")

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
        retry_error_callback=retry_error_callback,
    )
    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_retry_policy=retry_policy,
        ),
    )
    submission = await engine.submit_group("g-handler-retry-callback", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is False
    assert result.error == "handler_retry_exhausted"
    assert all(event.type != "job_result_handler_failed" for event in sink.events)


@pytest.mark.asyncio
async def test_result_handler_retry_error_attempts_in_failure_payload() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_retry_policy=retry_policy,
        ),
    )
    submission = await engine.submit_group("g-handler-fail", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is False
    assert result.meta is not None
    assert result.meta.get("result_handler_error_attempts") == 3

    failed = next(event for event in sink.events if event.type == "job_result_handler_failed")
    payload = failed.payload or {}
    assert payload.get("result_handler_error_attempts") == 3


@pytest.mark.asyncio
async def test_result_handler_failed_payload_includes_error_info_and_attempts() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_retry_policy=retry_policy,
        ),
    )
    submission = await engine.submit_group("g-handler-fail-payload", {"value": "orig"}, [])
    assert submission.primer is not None
    await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    failed = next(event for event in sink.events if event.type == "job_result_handler_failed")
    payload = failed.payload or {}
    assert payload.get("result_handler_error_attempts") == 2
    error_info = payload.get("error_info")
    assert error_info is not None
    assert error_info.get("message") == "handler boom"
    assert error_info.get("type") == "RuntimeError"


@pytest.mark.asyncio
async def test_result_handler_retry_inherits_attempt_policy() -> None:
    executor = RecordingExecutor()
    calls: list[int] = []

    async def handler(result: JobResult, ctx):
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("handler boom")
        return None

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            attempt_retry_policy=retry_policy,
            job_result_handler=handler,
        ),
    )
    submission = await engine.submit_group("g-handler-retry", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_result_handler_retry_wraps_result_policy() -> None:
    executor = RecordingExecutor()
    calls: list[int] = []

    async def handler(result: JobResult, ctx):
        calls.append(1)
        return None

    def result_predicate(result) -> bool:
        return result.ok  # would raise if invoked with None

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(1),
        wait=wait_fixed(0.0),
        retry=retry_if_result(result_predicate),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_retry_policy=retry_policy,
        ),
    )
    submission = await engine.submit_group("g-handler-retry-result", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_result_handler_retry_disabled() -> None:
    executor = RecordingExecutor()
    calls: list[int] = []

    async def handler(result: JobResult, ctx):
        calls.append(1)
        raise RuntimeError("handler boom")

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            attempt_retry_policy=retry_policy,
            job_result_handler=handler,
            result_handler_retry_policy="disabled",
        ),
    )
    submission = await engine.submit_group("g-handler-disabled", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert len(calls) == 1
    assert result.ok is False
    assert result.error == "result_handler_failed"


@pytest.mark.asyncio
async def test_result_handler_retry_custom_policy_overrides_inherit() -> None:
    executor = RecordingExecutor()
    calls: list[int] = []

    async def handler(result: JobResult, ctx):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("handler boom")
        return None

    attempt_policy = AsyncRetrying(
        stop=stop_after_attempt(1),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    handler_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            attempt_retry_policy=attempt_policy,
            result_handler_retry_policy=handler_policy,
            job_result_handler=handler,
        ),
    )
    submission = await engine.submit_group("g-handler-custom", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is True
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_result_handler_retry_error_callback_jobresult_is_wrapped() -> None:
    sink = RecordingEventSink()
    bus = EventBus([sink])
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        raise RuntimeError("handler boom")

    def retry_error_callback(retry_state):
        _ = retry_state
        return JobResult(
            job_id="job-x",
            kind=GroupTaskKind.PRIMER.value,
            ok=False,
            error="handler_retry_exhausted",
        )

    retry_policy = AsyncRetrying(
        stop=stop_after_attempt(2),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
        retry_error_callback=retry_error_callback,
    )
    engine = ActyEngine(
        executor=executor,
        event_bus=bus,
        config=EngineConfig(
            job_result_handler=handler,
            result_handler_retry_policy=retry_policy,
        ),
    )
    submission = await engine.submit_group("g-handler-retry-jobresult", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is False
    assert result.error == "handler_retry_exhausted"
    handled = [
        event
        for event in sink.events
        if event.type == "job_result_handled" and event.payload
    ]
    assert any(event.payload.get("action") == "replace" for event in handled)
    assert all(event.type != "job_result_handler_failed" for event in sink.events)


@pytest.mark.asyncio
async def test_result_handler_max_attempts_exhaustion() -> None:
    executor = RecordingExecutor()

    async def handler(result: JobResult, ctx):
        return ResubmitJob(payload={"value": "retry"})

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            job_result_handler=handler,
            max_result_handler_attempts=1,
        ),
    )
    submission = await engine.submit_group("g-exhaust", {"value": "orig"}, [])
    assert submission.primer is not None
    result = await asyncio.wait_for(submission.primer, timeout=5.0)
    await engine.close()

    assert result.ok is False
    assert result.error == "result_handler_max_attempts_exceeded"
    assert len(executor.calls) == 2


@pytest.mark.asyncio
async def test_result_handler_resubmit_drop_cache_omits_cache_context() -> None:
    provider = DummyProvider()
    registry = CacheRegistry(provider, storage=InMemoryStorage())
    executor = ContextRecordingExecutor()

    async def handler(result: JobResult, ctx):
        if ctx.result_handler_attempt == 1:
            return ResubmitJob(payload={"value": "repair"}, context_policy="drop_cache")
        return None

    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(job_result_handler=handler),
        cache_registry=registry,
    )
    try:
        submission = await engine.submit_group(
            "g-drop-cache",
            {"value": "orig"},
            [],
            group_context={"g": "ctx"},
        )
        assert submission.primer is not None
        result = await asyncio.wait_for(submission.primer, timeout=5.0)
    finally:
        await engine.close()

    assert result.ok is True
    assert len(executor.contexts) == 2
    first_context = executor.contexts[0]
    second_context = executor.contexts[1]
    assert first_context is not None
    assert second_context is not None
    assert "cache" in first_context
    assert "cache" not in second_context
    assert second_context.get("group_context") == {"g": "ctx"}
    assert executor.cache_handles[0] is not None
    assert executor.cache_handles[1] is None


def test_result_handler_requires_bind_config_for_lifecycle_executor() -> None:
    class LifecycleExecutor:
        handles_lifecycle = True

        async def execute(self, job: Job, *, pool: str) -> JobResult:
            return JobResult(job_id=job.id, kind=job.kind, ok=True)

    async def handler(result: JobResult, ctx):
        return None

    with pytest.raises(ValueError, match="bind_result_handler_config.*error_policy.*max_attempts"):
        ActyEngine(
            executor=LifecycleExecutor(),
            config=EngineConfig(job_result_handler=handler),
        )


def test_result_handler_binds_config_on_lifecycle_executor() -> None:
    class LifecycleExecutor:
        handles_lifecycle = True

        def __init__(self) -> None:
            self.bound = None

        def bind_result_handler_config(self, handler, *, error_policy, max_attempts) -> None:
            self.bound = (handler, error_policy, max_attempts)

        async def execute(self, job: Job, *, pool: str) -> JobResult:
            return JobResult(job_id=job.id, kind=job.kind, ok=True)

    async def handler(result: JobResult, ctx):
        return None

    executor = LifecycleExecutor()
    config = EngineConfig(
        job_result_handler=handler,
        result_handler_error_policy=ResultHandlerErrorPolicy.PASS_THROUGH,
        max_result_handler_attempts=3,
    )
    ActyEngine(executor=executor, config=config)

    assert executor.bound == (handler, ResultHandlerErrorPolicy.PASS_THROUGH, 3)
