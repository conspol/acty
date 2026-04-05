"""Executor helpers, including tenacity-based retry wrapper."""

from __future__ import annotations

import inspect
from dataclasses import replace
from typing import Any, Awaitable, Callable, Mapping, cast

import structlog
from tenacity import AsyncRetrying, RetryCallState, RetryError

from acty_core.core.types import Job, JobResult
from acty_core.result_handlers import (
    AcceptResult,
    FailResult,
    JobResultAction,
    JobResultHandler,
    JobResultHandlerContext,
    ReplaceResult,
    ResubmitJob,
    ResultHandlerRetryError,
)
from acty_core.executors import (
    EchoExecutor,
    LifecycleExecutorAdapter,
    NoopExecutor,
    SimulatedExecutor,
    SimulatedExecutorConfig,
    ensure_lifecycle_executor,
)
from acty_core.events.bus import EventBus
from acty_core.scheduler.work_stealing import TaggedExecutor

from .exec_resolver import ExecResolver, resolve_payload_with_exec_id

RetryFactory = Callable[[Job], AsyncRetrying | Awaitable[AsyncRetrying]]
RetryingOrFactory = AsyncRetrying | RetryFactory
RetryUpdate = Job | Mapping[str, Any] | None
RetryCallback = Callable[[RetryCallState, Job], RetryUpdate | Awaitable[RetryUpdate]]
RetryPayloadFn = Callable[
    [int, Exception, Mapping[str, Any]],
    Mapping[str, Any] | Awaitable[Mapping[str, Any]] | None,
]

logger = structlog.get_logger(__name__)


async def build_tenacity_retrying(
    retrying_or_factory: RetryingOrFactory,
    job: Job,
    *,
    on_retry_hook: Callable[[RetryCallState], Awaitable[None]],
    on_retry_error: Callable[[RetryCallState, Any], Awaitable[None]] | None = None,
    retry_transform: Callable[[Any], Any] | None = None,
    retry_error_callback_transform: Callable[[Any], Any] | None = None,
) -> AsyncRetrying:
    """Build a per-job AsyncRetrying instance.

    Note: We always call .copy() on the provided retrying/factory result to
    isolate per-job state. If the factory already returns a fresh instance,
    the extra copy is redundant but safe.
    """
    if isinstance(retrying_or_factory, AsyncRetrying):
        base_retrying = retrying_or_factory
    else:
        base_retrying = retrying_or_factory(job)
        if inspect.isawaitable(base_retrying):
            base_retrying = await base_retrying
        if not isinstance(base_retrying, AsyncRetrying):
            raise ValueError("retry_factory must return a tenacity.AsyncRetrying instance")

    base_retrying = base_retrying.copy()
    before_sleep = base_retrying.before_sleep
    retry_error_callback = base_retrying.retry_error_callback
    retry_policy = base_retrying.retry
    if retry_transform is not None:
        retry_policy = retry_transform(retry_policy)
    if retry_error_callback_transform is not None:
        retry_error_callback = retry_error_callback_transform(retry_error_callback)

    async def chained_before_sleep(retry_state: RetryCallState) -> None:
        if before_sleep is not None:
            maybe = before_sleep(retry_state)
            if inspect.isawaitable(maybe):
                await maybe
        await on_retry_hook(retry_state)

    async def chained_retry_error_callback(retry_state: RetryCallState) -> Any:
        result = None
        if retry_error_callback is not None:
            maybe = retry_error_callback(retry_state)
            if inspect.isawaitable(maybe):
                maybe = await maybe
            result = maybe
        if on_retry_error is not None:
            await on_retry_error(retry_state, result)
        return result

    return base_retrying.copy(
        before_sleep=chained_before_sleep,
        retry_error_callback=chained_retry_error_callback if retry_error_callback is not None else None,
        retry=retry_policy,
    )


def _set_supports_executor_retry(executor: object, supported: bool) -> None:
    """Best-effort propagation for executor retry capability."""

    seen: set[int] = set()

    def looks_like_executor(target: object) -> bool:
        return bool(
            hasattr(target, "execute")
            or hasattr(target, "supports_executor_retry")
            or hasattr(target, "handles_lifecycle")
        )

    def apply(target: object | None) -> None:
        if target is None:
            return
        target_id = id(target)
        if target_id in seen:
            return
        seen.add(target_id)
        try:
            setattr(target, "supports_executor_retry", supported)
        except Exception:
            pass
        for attr in ("_executor", "_inner", "inner", "executor"):
            try:
                inner = getattr(target, attr)
            except Exception:
                continue
            # Only propagate to objects that resemble executors to avoid mutating unrelated objects.
            if not looks_like_executor(inner):
                continue
            apply(inner)

    apply(executor)


def _error_message_from_exception(exc: BaseException | None) -> str | None:
    if exc is None:
        return None
    message = str(exc)
    if message:
        return message
    return exc.__class__.__name__


def _select_retry_payload(
    payload: Mapping[str, Any],
    *,
    payload_key: str,
    retry_payload_key: str,
) -> tuple[Mapping[str, Any], bool]:
    inner = payload.get(payload_key)
    if isinstance(inner, Mapping):
        if retry_payload_key in inner:
            return inner, True
        if retry_payload_key not in payload and "input" in inner and "input" not in payload:
            return inner, True
    return payload, False


async def resolver_retry_payload_on_retry(
    retry_state: RetryCallState,
    job: Job,
    *,
    resolver: ExecResolver | None,
    exec_id_key: str = "exec_id",
    payload_key: str = "payload",
    runnable_key: str = "runnable",
    retry_payload_key: str = "retry_payload_fn",
    retry_key: str = "retry",
) -> RetryUpdate:
    """Resolve exec_id payloads before applying retry_payload_fn updates."""

    outcome = retry_state.outcome
    if outcome is None or not outcome.failed:
        return None
    exc = outcome.exception()
    if exc is None or not isinstance(exc, Exception):
        return None
    if not isinstance(job.payload, Mapping):
        return None

    payload: Any = job.payload
    if resolver is not None:
        payload = resolve_payload_with_exec_id(
            payload,
            resolver,
            job=job,
            exec_id_key=exec_id_key,
            payload_key=payload_key,
            runnable_key=runnable_key,
        )
        if inspect.isawaitable(payload):
            payload = await payload
    if not isinstance(payload, Mapping):
        return None

    call_payload, _ = _select_retry_payload(
        payload,
        payload_key=payload_key,
        retry_payload_key=retry_payload_key,
    )
    retry_payload_fn = call_payload.get(retry_payload_key)
    if retry_payload_fn is None:
        return None
    if not callable(retry_payload_fn):
        raise ValueError("retry_payload_fn must be callable")

    maybe_new = retry_payload_fn(retry_state.attempt_number, exc, call_payload)
    if inspect.isawaitable(maybe_new):
        maybe_new = await maybe_new
    if maybe_new is None:
        return None
    if not isinstance(maybe_new, Mapping):
        raise ValueError("retry_payload_fn must return a mapping")

    new_payload = dict(maybe_new)
    if retry_key not in new_payload and retry_key in call_payload:
        new_payload[retry_key] = call_payload[retry_key]
    if retry_payload_key not in new_payload and retry_payload_key in call_payload:
        new_payload[retry_payload_key] = call_payload[retry_payload_key]

    original_payload = job.payload
    _, wrap_outer = _select_retry_payload(
        original_payload,
        payload_key=payload_key,
        retry_payload_key=retry_payload_key,
    )
    if exec_id_key not in new_payload:
        if exec_id_key in call_payload:
            new_payload[exec_id_key] = call_payload[exec_id_key]
        elif not wrap_outer and exec_id_key in original_payload:
            new_payload[exec_id_key] = original_payload[exec_id_key]

    if wrap_outer and isinstance(original_payload.get(payload_key), Mapping) and runnable_key not in original_payload:
        merged = dict(original_payload)
        merged[payload_key] = new_payload
        return merged
    return new_payload


class ResolverExecutor:
    """Resolve exec_id payloads before delegating to an inner executor.

    exec_id lookup prefers the top-level key over nested payloads.
    """

    handles_lifecycle = False

    def __init__(
        self,
        executor: TaggedExecutor,
        resolver: ExecResolver | None,
        *,
        exec_id_key: str = "exec_id",
        payload_key: str = "payload",
        runnable_key: str = "runnable",
    ) -> None:
        self._executor = executor
        self._resolver = resolver
        self._exec_id_key = exec_id_key
        self._payload_key = payload_key
        self._runnable_key = runnable_key
        self._supports_executor_retry = False
        self.handles_lifecycle = bool(getattr(executor, "handles_lifecycle", False))

    @property
    def supports_executor_retry(self) -> bool:
        try:
            return bool(getattr(self._executor, "supports_executor_retry"))
        except Exception:
            return self._supports_executor_retry

    @supports_executor_retry.setter
    def supports_executor_retry(self, value: bool) -> None:
        self._supports_executor_retry = bool(value)
        _set_supports_executor_retry(self._executor, value)

    def bind(self, controller) -> None:
        if hasattr(self._executor, "bind"):
            try:
                self._executor.bind(controller)  # type: ignore[attr-defined]
            except TypeError:
                return

    def bind_result_handler(self, handler) -> None:
        if hasattr(self._executor, "bind_result_handler"):
            self._executor.bind_result_handler(handler)  # type: ignore[attr-defined]

    def bind_result_handler_config(self, handler, *, error_policy, max_attempts) -> None:
        if hasattr(self._executor, "bind_result_handler_config"):
            self._executor.bind_result_handler_config(  # type: ignore[attr-defined]
                handler,
                error_policy=error_policy,
                max_attempts=max_attempts,
            )
            return
        raise ValueError(
            "job_result_handler requires bind_result_handler_config(handler, error_policy, max_attempts) "
            "on lifecycle executor"
        )

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        payload = job.payload
        if not isinstance(payload, Mapping):
            return await self._executor.execute(job, pool=pool)
        if self._payload_has_runnable(payload):
            return await self._executor.execute(job, pool=pool)
        exec_id = self._extract_exec_id(payload)
        if exec_id is None:
            return await self._executor.execute(job, pool=pool)
        if self._resolver is None:
            logger.error(
                "exec_id provided but no resolver configured",
                exec_id=exec_id,
                job_id=str(job.id),
                group_id=str(job.group_id) if job.group_id is not None else None,
                kind=job.kind,
                payload_keys=list(payload.keys()),
            )
            return await self._executor.execute(job, pool=pool)
        resolved_payload = resolve_payload_with_exec_id(
            payload,
            self._resolver,
            job=job,
            exec_id_key=self._exec_id_key,
            payload_key=self._payload_key,
            runnable_key=self._runnable_key,
        )
        if inspect.isawaitable(resolved_payload):
            resolved_payload = await resolved_payload
        if resolved_payload is payload:
            return await self._executor.execute(job, pool=pool)
        resolved_job = replace(job, payload=resolved_payload)
        return await self._executor.execute(resolved_job, pool=pool)

    async def retry_payload_on_retry(self, retry_state: RetryCallState, job: Job) -> RetryUpdate:
        """Resolve exec_id before invoking retry_payload_fn, if configured."""
        return await resolver_retry_payload_on_retry(
            retry_state,
            job,
            resolver=self._resolver,
            exec_id_key=self._exec_id_key,
            payload_key=self._payload_key,
            runnable_key=self._runnable_key,
        )

    def _payload_has_runnable(self, payload: Mapping[str, Any]) -> bool:
        if self._runnable_key in payload:
            return True
        inner = payload.get(self._payload_key)
        return isinstance(inner, Mapping) and self._runnable_key in inner

    def _extract_exec_id(self, payload: Mapping[str, Any]) -> str | None:
        exec_id = payload.get(self._exec_id_key)
        if exec_id is None and isinstance(payload.get(self._payload_key), Mapping):
            exec_id = payload[self._payload_key].get(self._exec_id_key)
        if exec_id is None:
            return None
        if not isinstance(exec_id, str):
            raise ValueError(f"{self._exec_id_key} must be a string")
        return exec_id


def _handler_name(handler: JobResultHandler) -> str:
    name = getattr(handler, "__name__", None)
    if isinstance(name, str) and name:
        return name
    return handler.__class__.__name__


_HANDLER_ACTION_UNSET = object()


class ResultHandlerRetryAdapter:
    """Retry a job result handler via tenacity."""

    def __init__(
        self,
        handler: JobResultHandler,
        retrying_or_factory: RetryingOrFactory,
    ) -> None:
        self._handler = handler
        self._retrying_or_factory = retrying_or_factory

    async def __call__(
        self,
        result: JobResult,
        context: JobResultHandlerContext,
    ) -> JobResultAction | None:
        handler_name = _handler_name(self._handler)
        attempt_count = 0
        last_error: BaseException | None = None
        last_action: object = _HANDLER_ACTION_UNSET
        retry_error_action_unset = object()
        retry_error_action: object = retry_error_action_unset

        async def on_retry(_: RetryCallState) -> None:
            return None

        async def record_retry_error_action(_: RetryCallState, value: Any) -> None:
            nonlocal retry_error_action
            retry_error_action = _coerce_retry_error_action(value)

        warned = False

        def handler_retry_filter(base_retry) -> Callable[[RetryCallState], bool]:
            def _filter(retry_state: RetryCallState) -> bool:
                outcome = retry_state.outcome
                if outcome is None or not outcome.failed:
                    return False
                try:
                    return bool(base_retry(retry_state))
                except Exception as exc:
                    nonlocal warned
                    if not warned:
                        warned = True
                        logger.info(
                            "result_handler_retry_policy_wrapped",
                            handler=handler_name,
                            job_id=str(context.job_id),
                            original_job_id=str(context.original_job_id),
                            kind=context.kind,
                            group_id=str(context.group_id) if context.group_id is not None else None,
                            error_type=exc.__class__.__name__,
                        )
                    return True

            return _filter

        retrying = await build_tenacity_retrying(
            self._retrying_or_factory,
            context.job,
            on_retry_hook=on_retry,
            on_retry_error=record_retry_error_action,
            retry_transform=handler_retry_filter,
        )

        try:
            async for attempt in retrying:
                attempt_count = attempt.retry_state.attempt_number
                with attempt:
                    action = self._handler(result, context)
                    if inspect.isawaitable(action):
                        action = await action
                outcome = attempt.retry_state.outcome
                if outcome is not None and outcome.failed:
                    exc = outcome.exception()
                    error_type = exc.__class__.__name__ if exc is not None else None
                    logger.info(
                        "result_handler_retry",
                        attempt=attempt.retry_state.attempt_number,
                        error_type=error_type,
                        handler=handler_name,
                        job_id=str(context.job_id),
                        original_job_id=str(context.original_job_id),
                        kind=context.kind,
                        group_id=str(context.group_id) if context.group_id is not None else None,
                        follower_id=str(context.job.follower_id) if context.job.follower_id is not None else None,
                    )
                elif outcome is not None:
                    attempt.retry_state.set_result(action)
                    last_action = action
        except RetryError as exc:
            if retry_error_action is not retry_error_action_unset:
                return cast(JobResultAction | None, retry_error_action)
            last_attempt = exc.last_attempt
            attempt_count = getattr(last_attempt, "attempt_number", attempt_count)
            last_error = last_attempt.exception() or last_error
            raise ResultHandlerRetryError(attempt_count or 1, last_error or exc) from last_error
        except Exception as exc:
            last_error = exc
            raise ResultHandlerRetryError(attempt_count or 1, last_error) from last_error

        if last_action is not _HANDLER_ACTION_UNSET:
            return cast(JobResultAction | None, last_action)
        if retry_error_action is not retry_error_action_unset:
            return cast(JobResultAction | None, retry_error_action)
        if last_error is not None:
            raise ResultHandlerRetryError(attempt_count or 1, last_error) from last_error
        raise ResultHandlerRetryError(attempt_count or 1, None)


def _coerce_retry_error_action(value: Any) -> JobResultAction | None:
    if value is None:
        return None
    if isinstance(value, (AcceptResult, ReplaceResult, ResubmitJob, FailResult)):
        return value
    if isinstance(value, JobResult):
        return ReplaceResult(value, reason="handler_retry_error_callback")
    logger.info(
        "result_handler_retry_error_callback_ignored",
        value_type=value.__class__.__name__ if value is not None else None,
    )
    return None


class TenacityRetryAdapter:
    """Retry executor attempts via tenacity (intra-attempt retries).

    This adapter must be used *inside* lifecycle handling (i.e. wrapped by
    `LifecycleExecutorAdapter` / `ensure_lifecycle_executor`) so lifecycle
    transitions happen once per job attempt after tenacity is exhausted.
    """

    handles_lifecycle = False
    supports_executor_retry = True

    def __init__(
        self,
        executor: TaggedExecutor,
        retrying_or_factory: RetryingOrFactory,
        *,
        on_retry: RetryCallback | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        if getattr(executor, "handles_lifecycle", False):
            raise ValueError("TenacityRetryAdapter must wrap a non-lifecycle executor")
        self._executor = executor
        self._retrying_or_factory = retrying_or_factory
        self._on_retry = on_retry
        self._event_bus = event_bus
        _set_supports_executor_retry(self._executor, True)

    def bind(self, controller) -> None:
        if hasattr(self._executor, "bind"):
            try:
                self._executor.bind(controller)  # type: ignore[attr-defined]
            except TypeError:
                return

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        current_job = job
        attempt_count = 0
        last_error: BaseException | None = None
        last_result: JobResult | None = None
        retry_error_result_unset = object()
        retry_error_result = retry_error_result_unset

        def apply_update(update: RetryUpdate) -> None:
            nonlocal current_job
            if update is None:
                return
            if isinstance(update, Job):
                if update.id != current_job.id:
                    raise ValueError("on_retry cannot change job_id")
                current_job = update
                return
            if isinstance(update, Mapping):
                current_job = replace(current_job, payload=update)
                return
            raise TypeError("on_retry must return Job, Mapping, or None")

        async def run_on_retry(retry_state: RetryCallState) -> None:
            outcome = retry_state.outcome
            if outcome is not None and outcome.failed:
                exc = outcome.exception()
                if exc is not None:
                    last_error_type = exc.__class__.__name__
                else:
                    last_error_type = None
                if self._event_bus is not None:
                    await self._event_bus.emit(
                        type="job_retrying",
                        job_id=str(current_job.id),
                        group_id=str(current_job.group_id) if current_job.group_id is not None else None,
                        follower_id=str(current_job.follower_id) if current_job.follower_id is not None else None,
                        pool=pool,
                        kind=current_job.kind,
                        payload={
                            "attempt": retry_state.attempt_number,
                            "error_type": last_error_type,
                            "source": "tenacity",
                        },
                    )
                logger.info(
                    "tenacity_retry",
                    attempt=retry_state.attempt_number,
                    error_type=last_error_type,
                    job_id=str(current_job.id),
                    kind=current_job.kind,
                    group_id=str(current_job.group_id) if current_job.group_id is not None else None,
                    follower_id=str(current_job.follower_id)
                    if current_job.follower_id is not None
                    else None,
                )
            if self._on_retry is None:
                return
            maybe_update = self._on_retry(retry_state, current_job)
            if inspect.isawaitable(maybe_update):
                maybe_update = await maybe_update
            apply_update(maybe_update)

        async def record_retry_error_result(retry_state: RetryCallState, value: Any) -> None:
            nonlocal retry_error_result
            retry_error_result = value

        retrying = await build_tenacity_retrying(
            self._retrying_or_factory,
            current_job,
            on_retry_hook=run_on_retry,
            on_retry_error=record_retry_error_result,
        )

        def attach_meta(result: JobResult, *, force_ok: bool | None = None, error: str | None = None) -> JobResult:
            meta = dict(result.meta) if result.meta else {}
            meta["tenacity_attempts"] = attempt_count
            last_error_message = _error_message_from_exception(last_error)
            if not last_error_message and result.error:
                last_error_message = result.error
            meta["tenacity_last_error"] = last_error_message
            return JobResult(
                job_id=result.job_id,
                kind=result.kind,
                ok=result.ok if force_ok is None else force_ok,
                output=result.output,
                error=error if error is not None else result.error,
                group_id=result.group_id,
                follower_id=result.follower_id,
                meta=meta,
            )

        try:
            async for attempt in retrying:
                attempt_count = attempt.retry_state.attempt_number
                result: JobResult | None = None
                with attempt:
                    result = await self._executor.execute(current_job, pool=pool)
                outcome = attempt.retry_state.outcome
                if outcome is not None:
                    if outcome.failed:
                        exc = outcome.exception()
                        if exc is not None:
                            last_error = exc
                    else:
                        attempt.retry_state.set_result(result)
                        last_result = result
        except RetryError as exc:
            last_attempt = exc.last_attempt
            attempt_count = getattr(last_attempt, "attempt_number", attempt_count)
            last_exc = last_attempt.exception()
            if last_exc is not None:
                last_error = last_exc
            elif not last_attempt.failed:
                try:
                    attempt_value = last_attempt.result()
                except Exception as inner_exc:
                    last_error = inner_exc
                else:
                    if isinstance(attempt_value, JobResult):
                        last_result = attempt_value
            logger.info(
                "tenacity_exhausted",
                attempts=attempt_count,
                error_type=last_error.__class__.__name__ if last_error is not None else None,
                job_id=str(current_job.id),
                kind=current_job.kind,
                group_id=str(current_job.group_id) if current_job.group_id is not None else None,
                follower_id=str(current_job.follower_id)
                if current_job.follower_id is not None
                else None,
            )
            error_message = _error_message_from_exception(last_error)
            if not error_message and last_result is not None and last_result.error:
                error_message = last_result.error
            if not error_message:
                error_message = "tenacity retry exhausted"
            if last_result is not None:
                return attach_meta(last_result, force_ok=False, error=error_message)
            return attach_meta(
                JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    output=None,
                    error=error_message,
                    group_id=job.group_id,
                    follower_id=job.follower_id,
                )
            )
        except Exception as exc:
            if last_error is None:
                last_error = exc
            logger.info(
                "tenacity_reraised",
                attempts=attempt_count,
                error_type=last_error.__class__.__name__ if last_error is not None else None,
                job_id=str(current_job.id),
                kind=current_job.kind,
                group_id=str(current_job.group_id) if current_job.group_id is not None else None,
                follower_id=str(current_job.follower_id)
                if current_job.follower_id is not None
                else None,
            )
            error_message = _error_message_from_exception(last_error) or _error_message_from_exception(exc)
            if not error_message:
                error_message = "tenacity retry exhausted"
            if last_result is not None:
                return attach_meta(last_result, force_ok=False, error=error_message)
            return attach_meta(
                JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    output=None,
                    error=error_message,
                    group_id=job.group_id,
                    follower_id=job.follower_id,
                )
            )

        if last_result is None:
            if retry_error_result is not retry_error_result_unset:
                if isinstance(retry_error_result, JobResult):
                    return attach_meta(retry_error_result)
                error_message = "tenacity retry_error_callback returned non-JobResult"
                output: Mapping[str, Any] | None = None
                if isinstance(retry_error_result, Mapping):
                    output = dict(retry_error_result)
                elif retry_error_result is not None:
                    error_message = f"{error_message}: {retry_error_result!r}"
                else:
                    error_message = f"{error_message}: None"
                return attach_meta(
                    JobResult(
                        job_id=job.id,
                        kind=job.kind,
                        ok=False,
                        output=output,
                        error=error_message,
                        group_id=job.group_id,
                        follower_id=job.follower_id,
                    )
                )
            return attach_meta(
                JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    output=None,
                    error="tenacity retry loop terminated without a result",
                    group_id=job.group_id,
                    follower_id=job.follower_id,
                )
            )
        return attach_meta(last_result)

    async def _build_retrying(
        self,
        job: Job,
        on_retry_hook: Callable[[RetryCallState], Awaitable[None]],
        on_retry_error: Callable[[RetryCallState, Any], Awaitable[None]] | None = None,
    ) -> AsyncRetrying:
        return await build_tenacity_retrying(
            self._retrying_or_factory,
            job,
            on_retry_hook=on_retry_hook,
            on_retry_error=on_retry_error,
        )

__all__ = [
    "NoopExecutor",
    "EchoExecutor",
    "SimulatedExecutor",
    "SimulatedExecutorConfig",
    "LifecycleExecutorAdapter",
    "ensure_lifecycle_executor",
    "resolver_retry_payload_on_retry",
    "ResolverExecutor",
    "TenacityRetryAdapter",
]
