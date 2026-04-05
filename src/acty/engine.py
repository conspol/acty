"""High-level Acty engine facade."""

from __future__ import annotations

import asyncio
import math
import threading
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Literal, Mapping, Sequence

import structlog

from acty_core.cache.registry import CacheRegistry
from acty_core.context import ContextPropagator, ContextVarsPropagator
from acty_core.core.types import FollowerId, GroupId, Job, JobId, JobResult
from acty_core.executors import ensure_lifecycle_executor
from acty_core.events import ErrorPayloadPolicy
from acty_core.events.bus import EventBus
from acty_core.events.sinks.base import EventSink
from acty_core.lifecycle import (
    BurstController,
    FollowersStartMode,
    FollowerFailurePolicy,
    FollowerRetryCallback,
    FollowerDispatchPolicy,
    GroupAdmissionScheduler,
    GroupDependency,
    GroupDependencyGate,
    GroupDependencyPolicy,
    GroupLifecycleController,
    GroupSpec,
    GroupTaskKind,
    PrimerFailurePolicy,
    PrimerRetryCallback,
)
from acty_core.results import (
    AsyncQueueResultSink,
    ResultFanout,
    ResultSink,
    RetryAwareAwaitableResultSink,
)
from acty_core.result_handlers import (
    JobResultHandler,
    ResultHandlerErrorPolicy,
    ResultHandlerTelemetryConfig,
)
from acty_core.telemetry import TelemetryPrivacyConfig
from acty_core.scheduler import (
    LaneConfig,
    PoolConfig,
    TaggedExecutor,
    WorkStealingReport,
    WorkStealingScheduler,
)

from .executors import RetryCallback, ResultHandlerRetryAdapter, RetryingOrFactory, TenacityRetryAdapter
from .lane import Lane

logger = structlog.get_logger(__name__)

UNSET: object = object()


@dataclass(frozen=True)
class EngineConfig:
    primer_workers: int = 1
    follower_workers: int = 2
    warm_delay_s: float = 0.0
    max_followers_inflight: int | None = None
    followers_start_mode: FollowersStartMode = FollowersStartMode.AFTER_WARMUP
    follower_dispatch_policy: FollowerDispatchPolicy | None = None
    # Admission settings (wired in Chapter 3).
    max_active_groups: int | None = None
    burst_allowance: int = 0
    low_watermark_factor: int = 1
    streams: int | None = None
    # Safety-only; unbounded by default.
    queue_maxsize: int | None = None
    # Result stream configuration (used when results() is called).
    result_queue_maxsize: int = 0
    result_drop_policy: Literal["block", "drop_newest", "drop_oldest"] = "block"
    admission_tick_s: float = 0.1
    stats_emit_interval_s: float | None = 5.0
    auto_primer_from_first_payload: bool = True
    run_primer_on_cache_hit: bool = True
    enable_dependency_gates: bool = True
    allow_unsafe_ignore_dependencies: bool = False
    max_deferrals: int = 2
    auto_wrap_executor: bool = True
    context_propagator: ContextPropagator | None = field(
        default_factory=ContextVarsPropagator,
    )
    attempt_retry_policy: RetryingOrFactory | None = None
    attempt_retry_hook: RetryCallback | None = None
    primer_failure_policy: PrimerFailurePolicy = PrimerFailurePolicy.FAIL_GROUP
    follower_failure_policy: FollowerFailurePolicy = FollowerFailurePolicy.FAIL_GROUP
    primer_retry_callback: PrimerRetryCallback | None = None
    follower_retry_callback: FollowerRetryCallback | None = None
    job_result_handler: JobResultHandler | None = None
    max_result_handler_attempts: int = 1
    result_handler_retry_policy: RetryingOrFactory | Literal["inherit", "disabled"] = "inherit"
    result_handler_error_policy: ResultHandlerErrorPolicy = ResultHandlerErrorPolicy.FAIL
    result_handler_telemetry: ResultHandlerTelemetryConfig = field(
        default_factory=ResultHandlerTelemetryConfig
    )
    telemetry_privacy: TelemetryPrivacyConfig = field(default_factory=TelemetryPrivacyConfig)
    # Controls whether raw error details are emitted in event payloads.
    error_payload_policy: ErrorPayloadPolicy = field(default_factory=ErrorPayloadPolicy)
    lane_configs: Mapping[str, LaneConfig] | None = None
    lane_default: LaneConfig | None = None

    def __post_init__(self) -> None:
        if self.follower_dispatch_policy is None:
            target_total = max(1, self.follower_workers)
            object.__setattr__(
                self,
                "follower_dispatch_policy",
                FollowerDispatchPolicy.target(target_total),
            )
        if not isinstance(self.followers_start_mode, FollowersStartMode):
            raise ValueError("followers_start_mode must be a FollowersStartMode value")
        if self.stats_emit_interval_s is not None and self.stats_emit_interval_s < 0:
            raise ValueError("stats_emit_interval_s must be >= 0")
        if self.max_result_handler_attempts < 1:
            raise ValueError("max_result_handler_attempts must be >= 1")
        if isinstance(self.result_handler_retry_policy, str) and self.result_handler_retry_policy not in {
            "inherit",
            "disabled",
        }:
            raise ValueError("result_handler_retry_policy must be 'inherit', 'disabled', or a retry policy")


@dataclass(frozen=True)
class GroupSubmission:
    primer: asyncio.Future[JobResult] | None
    followers: Sequence[asyncio.Future[JobResult]]


@dataclass
class _PendingJob:
    job_id: JobId
    kind: str
    follower_id: str | None
    enqueued: bool = False


@dataclass(frozen=True)
class _GroupFailureInfo:
    error: str | None
    kind: str | None
    job_id: JobId | None
    follower_id: str | None
    dependency_reason: str | None = None


class _GroupResultTrackerSink:
    def __init__(self, engine: "ActyEngine") -> None:
        self._engine = engine

    async def handle(self, result: JobResult) -> None:
        self._engine._record_job_result(result)


class GroupHandle:
    """Awaitable + async context manager for open groups."""

    def __init__(
        self,
        engine: "ActyEngine",
        *,
        group_id: str,
        primer_payload: Any | None,
        follower_payloads: Iterable[Any] | None,
        follower_ids: Iterable[str | None] | None,
        depends_on: Sequence[GroupDependency] | None,
        group_context: Mapping[str, Any] | None,
        cache_key: str | None,
        cache_context: Mapping[str, Any] | None,
    ) -> None:
        self._engine = engine
        self._group_id = group_id
        self._primer_payload = primer_payload
        self._follower_payloads = list(follower_payloads) if follower_payloads is not None else None
        self._follower_ids = list(follower_ids) if follower_ids is not None else None
        self._depends_on = list(depends_on) if depends_on is not None else None
        self._group_context = group_context
        self._cache_key = cache_key
        self._cache_context = cache_context
        self._opened = False
        self._open_lock = asyncio.Lock()
        self.primer: asyncio.Future[JobResult] | None = None
        self.followers: Sequence[asyncio.Future[JobResult]] = ()

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def group_context(self) -> Mapping[str, Any] | None:
        return self._group_context

    def __await__(self):
        return self._ensure_open().__await__()

    async def __aenter__(self) -> "GroupHandle":
        await self._ensure_open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _ensure_open(self) -> "GroupHandle":
        if self._opened:
            return self
        async with self._open_lock:
            if self._opened:
                return self
            submission = await self._engine._open_group(
                self._group_id,
                primer_payload=self._primer_payload,
                follower_payloads=self._follower_payloads,
                follower_ids=self._follower_ids,
                depends_on=self._depends_on,
                group_context=self._group_context,
                cache_key=self._cache_key,
                cache_context=self._cache_context,
            )
            self.primer = submission.primer
            self.followers = submission.followers
            self._opened = True
            return self

    async def submit_primer(
        self,
        payload: Any,
        *,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
    ) -> asyncio.Future[JobResult] | None:
        await self._ensure_open()
        future = await self._engine.submit_primer(
            self._group_id,
            payload,
            cache_key=cache_key,
            cache_context=cache_context,
        )
        if future is not None and self.primer is None:
            self.primer = future
        return future

    async def add_follower(
        self,
        payload: Any,
        *,
        follower_id: str | None = None,
    ) -> asyncio.Future[JobResult]:
        await self._ensure_open()
        return await self._engine.submit_follower(self._group_id, payload, follower_id=follower_id)

    async def submit(
        self,
        payload: Any,
        *,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
        follower_id: str | None = None,
    ) -> asyncio.Future[JobResult] | None:
        await self._ensure_open()
        future = await self._engine.submit(
            self._group_id,
            payload,
            cache_key=cache_key,
            cache_context=cache_context,
            follower_id=follower_id,
        )
        if future is not None and self.primer is None:
            # If submit auto-promoted to primer, capture the future.
            self.primer = future
        return future

    async def close(self) -> None:
        await self._ensure_open()
        await self._engine.close_group(self._group_id)

    def producer_lease(self) -> "GroupLease":
        return GroupLease(self)


class GroupLease:
    """Lease that auto-closes the group when the last lease is released."""

    def __init__(self, group: GroupHandle) -> None:
        self._group = group
        self._acquired = False

    def __await__(self):
        return self.acquire().__await__()

    async def __aenter__(self) -> "GroupLease":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()

    async def acquire(self) -> "GroupLease":
        if self._acquired:
            return self
        await self._group._ensure_open()
        await self._group._engine._acquire_lease(self._group.group_id)
        self._acquired = True
        return self

    async def release(self) -> None:
        if not self._acquired:
            return
        self._acquired = False
        await self._group._engine._release_lease(self._group.group_id)

    async def done(self) -> None:
        await self.release()


class ActyEngine:
    UNSET = UNSET
    """Primary runtime facade for acty (group-only)."""

    def __init__(
        self,
        *,
        executor: TaggedExecutor,
        config: EngineConfig | None = None,
        event_bus: EventBus | None = None,
        event_sinks: Sequence[EventSink] | None = None,
        result_sink: ResultSink | None = None,
        result_sinks: Sequence[ResultSink] | None = None,
        cache_registry: CacheRegistry | None = None,
    ) -> None:
        if event_bus is not None and event_sinks:
            raise ValueError("Provide either event_bus or event_sinks, not both.")
        if result_sink is not None and result_sinks:
            raise ValueError("Provide either result_sink or result_sinks, not both.")

        self._config = config or EngineConfig()
        self._executor = executor
        self._cache_registry = cache_registry
        self._context_propagator = self._config.context_propagator

        if event_bus is None and event_sinks:
            event_bus = EventBus(event_sinks)
        self._event_bus = event_bus
        self._owns_event_bus = event_sinks is not None

        self._result_fanout = ResultFanout()
        if result_sink is not None:
            self._result_fanout.add_sink(result_sink)
        if result_sinks:
            for sink in result_sinks:
                self._result_fanout.add_sink(sink)
        self._awaitable_sink = RetryAwareAwaitableResultSink()
        self._result_fanout.add_sink(self._awaitable_sink)
        self._pending_lock = threading.Lock()
        self._pending_by_group: dict[str, dict[JobId, _PendingJob]] = {}
        self._job_to_group: dict[JobId, str] = {}
        self._group_failures: dict[str, _GroupFailureInfo] = {}
        self._finalizer_tasks: dict[str, asyncio.Task[None]] = {}
        self._result_fanout.add_sink(_GroupResultTrackerSink(self))
        self._result_stream_sink: AsyncQueueResultSink | None = None

        queue_maxsize = 0
        if self._config.queue_maxsize is not None:
            if self._config.queue_maxsize < 0:
                raise ValueError("queue_maxsize must be >= 0")
            queue_maxsize = self._config.queue_maxsize
        if self._config.result_queue_maxsize < 0:
            raise ValueError("result_queue_maxsize must be >= 0")
        if self._config.admission_tick_s <= 0:
            raise ValueError("admission_tick_s must be > 0")
        self._controller = GroupLifecycleController(
            event_bus=self._event_bus,
            follower_dispatch_policy=self._config.follower_dispatch_policy,
            cache_registry=self._cache_registry,
            primer_failure_policy=self._config.primer_failure_policy,
            follower_failure_policy=self._config.follower_failure_policy,
            primer_retry_callback=self._config.primer_retry_callback,
            follower_retry_callback=self._config.follower_retry_callback,
            error_payload_policy=self._config.error_payload_policy,
            on_job_enqueued=self._mark_job_enqueued,
            on_group_completed=self._on_controller_group_completed,
        )
        if self._config.attempt_retry_policy is not None:
            if not self._config.auto_wrap_executor:
                raise ValueError("attempt_retry_policy requires auto_wrap_executor=True to preserve lifecycle order")
            if getattr(self._executor, "handles_lifecycle", False):
                raise ValueError("attempt_retry_policy cannot wrap executors that handle lifecycle")
            self._executor = TenacityRetryAdapter(
                self._executor,
                self._config.attempt_retry_policy,
                on_retry=self._config.attempt_retry_hook,
                event_bus=self._event_bus,
            )
        job_result_handler = self._config.job_result_handler
        handler_retry_policy = self._resolve_result_handler_retry_policy()
        if job_result_handler is not None and handler_retry_policy is not None:
            job_result_handler = ResultHandlerRetryAdapter(job_result_handler, handler_retry_policy)
        if self._config.auto_wrap_executor:
            self._executor = ensure_lifecycle_executor(
                self._executor,
                self._controller,
                context_propagator=self._context_propagator,
                result_handler=job_result_handler,
                result_handler_error_policy=self._config.result_handler_error_policy,
                max_result_handler_attempts=self._config.max_result_handler_attempts,
                result_handler_telemetry=self._config.result_handler_telemetry,
                telemetry_privacy=self._config.telemetry_privacy,
            )
        else:
            if hasattr(self._executor, "bind_telemetry_privacy"):
                try:
                    self._executor.bind_telemetry_privacy(self._config.telemetry_privacy)  # type: ignore[attr-defined]
                except TypeError:
                    self._executor.bind_telemetry_privacy()  # type: ignore[attr-defined]
            if job_result_handler is not None:
                if not getattr(self._executor, "handles_lifecycle", False):
                    raise ValueError("job_result_handler requires handles_lifecycle=True when auto_wrap_executor=False")
                if hasattr(self._executor, "bind_result_handler_config"):
                    self._executor.bind_result_handler_config(  # type: ignore[attr-defined]
                        job_result_handler,
                        error_policy=self._config.result_handler_error_policy,
                        max_attempts=self._config.max_result_handler_attempts,
                    )
                else:
                    raise ValueError(
                        "job_result_handler requires bind_result_handler_config(handler, error_policy, max_attempts) "
                        "on lifecycle executor"
                    )
            if hasattr(self._executor, "bind"):
                try:
                    self._executor.bind(self._controller)  # type: ignore[attr-defined]
                except TypeError:
                    pass

        self._scheduler = WorkStealingScheduler(
            pools=[
                PoolConfig(
                    name="primer",
                    preferred_kinds=[GroupTaskKind.PRIMER.value],
                    workers=self._config.primer_workers,
                ),
                PoolConfig(
                    name="follower",
                    preferred_kinds=[GroupTaskKind.FOLLOWER.value],
                    workers=self._config.follower_workers,
                ),
            ],
            event_bus=self._event_bus,
            result_sink=self._result_fanout,
            queue_maxsize=queue_maxsize,
            max_deferrals=self._config.max_deferrals,
            error_payload_policy=self._config.error_payload_policy,
            lane_configs=self._config.lane_configs,
            lane_default=self._config.lane_default,
        )

        self._admission: GroupAdmissionScheduler | None = None
        if self._config.max_active_groups is not None:
            if self._config.max_active_groups <= 0:
                raise ValueError("max_active_groups must be > 0")
            streams = self._config.streams or (self._config.primer_workers + self._config.follower_workers)
            burst_controller = None
            if self._config.burst_allowance > 0:
                burst_controller = BurstController(
                    streams=streams,
                    low_watermark_factor=self._config.low_watermark_factor,
                    burst_allowance=self._config.burst_allowance,
                    event_bus=self._event_bus,
                )
            self._admission = GroupAdmissionScheduler(
                controller=self._controller,
                max_active_groups=self._config.max_active_groups,
                event_bus=self._event_bus,
                burst_controller=burst_controller,
                runnable_provider=self._scheduler.runnable_depth,
                tick_interval_s=self._config.admission_tick_s,
            )

        self._gate = GroupDependencyGate(
            controller=self._controller,
            admission=self._admission,
            event_bus=self._event_bus,
            on_dependency_failed=self._record_dependency_failure,
        )

        self._scheduler_task: asyncio.Task[WorkStealingReport] | None = None
        self._stats_task: asyncio.Task[None] | None = None
        self._closed = False
        self._started = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._state_lock = asyncio.Lock()
        self._group_handles: dict[str, GroupHandle] = {}
        self._lease_counts: dict[str, int] = {}
        self._lease_lock = asyncio.Lock()
        self._cleanup_tasks: dict[str, asyncio.Task[None]] = {}

    def _resolve_result_handler_retry_policy(self) -> RetryingOrFactory | None:
        policy = self._config.result_handler_retry_policy
        if policy is None:
            return None
        if policy == "inherit":
            return self._config.attempt_retry_policy
        if policy == "disabled":
            return None
        return policy

    async def __aenter__(self) -> "ActyEngine":
        await self._ensure_started()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _resolve_dependencies(
        self,
        group_id: str,
        depends_on: Sequence[GroupDependency] | None,
    ) -> Sequence[GroupDependency] | None:
        if not depends_on:
            return None
        if self._config.enable_dependency_gates:
            return depends_on
        if self._config.allow_unsafe_ignore_dependencies:
            logger.warning(
                "Dependency gates disabled; ignoring depends_on",
                group_id=group_id,
                dependencies=len(depends_on),
            )
            return None
        raise ValueError(
            "Dependency gates are disabled; enable_dependency_gates "
            "or allow_unsafe_ignore_dependencies to use depends_on"
        )

    async def submit_group(
        self,
        group_id: str,
        primer_payload: Any,
        follower_payloads: Iterable[Any] | None = None,
        follower_ids: Iterable[str | None] | None = None,
        depends_on: Sequence[GroupDependency] | None = None,
        *,
        group_context: Mapping[str, Any] | None = None,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
    ) -> GroupSubmission:
        """Create a group with a primer and optional followers.

        If follower_payloads is None, the group remains open for streaming followers.
        If follower_payloads is provided, the group is closed after enqueueing them.
        Optional follower_ids may be provided to tag batch followers.
        depends_on blocks this group until dependencies complete.
        group_context is attached to the group and exposed via Job.context.
        """
        await self._ensure_started()
        if self._closed:
            raise RuntimeError("ActyEngine is closed")

        depends_on = self._resolve_dependencies(group_id, depends_on)
        exec_context = self._capture_exec_context()
        payloads = list(follower_payloads) if follower_payloads is not None else None
        follower_id_list = list(follower_ids) if follower_ids is not None else None
        if payloads is None and follower_id_list:
            raise ValueError("follower_ids requires follower_payloads")
        if payloads is not None and follower_id_list is not None and len(follower_id_list) != len(payloads):
            raise ValueError("follower_ids length must match follower_payloads length")

        primer_job_id = self._controller.reserve_job_id(group_id, GroupTaskKind.PRIMER)
        primer_future = self._register_future(
            group_id,
            primer_job_id,
            kind=GroupTaskKind.PRIMER.value,
            follower_id=None,
        )

        def _cancel_primer_future() -> None:
            if not primer_future.done():
                primer_future.cancel()
            self._drop_pending_job(primer_job_id)

        follower_futures: list[asyncio.Future[JobResult]] = []
        follower_job_ids: list[JobId] = []
        follower_payload_list: list[Mapping[str, Any]] | None = None
        follower_exec_contexts: list[Any | None] | None = None
        if payloads is not None:
            follower_payload_list = [_wrap_payload(p) for p in payloads]
            if payloads:
                follower_job_ids = [
                    self._controller.reserve_job_id(group_id, GroupTaskKind.FOLLOWER)
                    for _ in payloads
                ]
                follower_futures = [
                    self._register_future(
                        group_id,
                        job_id,
                        kind=GroupTaskKind.FOLLOWER.value,
                        follower_id=follower_id_list[idx] if follower_id_list else None,
                    )
                    for idx, job_id in enumerate(follower_job_ids)
                ]
                if exec_context is not None:
                    follower_exec_contexts = [exec_context] * len(payloads)

        spec = GroupSpec(
            group_id=group_id,
            followers_total=None,
            warm_delay_s=self._config.warm_delay_s,
            max_followers_inflight=self._config.max_followers_inflight,
            followers_start_mode=self._config.followers_start_mode,
            payload=_wrap_payload(primer_payload),
            group_context=group_context,
            exec_context=exec_context,
            cache_group_id=cache_key,
            primer_content=primer_payload,
            cache_context=cache_context,
            run_primer_on_cache_hit=self._config.run_primer_on_cache_hit,
            depends_on=depends_on,
        )
        try:
            await self._gate.submit_group(
                spec,
                primer_job_id=primer_job_id,
                primer_cancel=_cancel_primer_future,
                follower_payloads=follower_payload_list,
                follower_job_ids=follower_job_ids if follower_payload_list is not None else None,
                follower_ids=follower_id_list,
                follower_exec_contexts=follower_exec_contexts,
                close_after_admit=payloads is not None,
            )
        except Exception:
            if not primer_future.done():
                primer_future.cancel()
            self._drop_pending_job(primer_job_id)
            for job_id, future in zip(follower_job_ids, follower_futures):
                future.cancel()
                self._drop_pending_job(job_id)
            raise
        return GroupSubmission(primer=primer_future, followers=tuple(follower_futures))

    def lane(self, lane_id: str, *, weight: float | None = None) -> Lane:
        return Lane(self, lane_id, weight=weight)

    def update_lane_config(
        self,
        *,
        lane_id: str,
        weight: float | object = UNSET,
        max_inflight: int | None | object = UNSET,
        ensure: bool = True,
    ) -> None:
        """Merge a single lane config into the scheduler configuration."""
        if not isinstance(lane_id, str) or not lane_id:
            raise ValueError("lane_id must be a non-empty string")
        if not ensure and weight is UNSET and max_inflight is UNSET:
            return
        loop = self._loop
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if loop is None or running_loop is loop:
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_config_now(
                lane_id=lane_id,
                weight=weight,
                max_inflight=max_inflight,
                ensure=ensure,
            )
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._apply_lane_config(
                    lane_id=lane_id,
                    weight=weight,
                    max_inflight=max_inflight,
                    ensure=ensure,
                ),
                loop,
            )
            future.result()
        except AttributeError:
            # Engine not started; apply directly without event loop coordination.
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_config_now(
                lane_id=lane_id,
                weight=weight,
                max_inflight=max_inflight,
                ensure=ensure,
            )

    def update_lane_configs(
        self,
        *,
        lane_configs: Mapping[str, LaneConfig] | None,
        default_config: LaneConfig | None = None,
    ) -> None:
        """Update lane configs at runtime and forward to the scheduler.

        If the engine is not started yet, updates apply immediately without needing an event loop.
        When called from the engine's running loop, updates are applied immediately.
        When called from another thread, this method blocks until the update is applied.
        """
        self._validate_lane_configs(lane_configs, default_config)
        loop = self._loop
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if loop is None or running_loop is loop:
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_configs_now(lane_configs, default_config)
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._apply_lane_configs(lane_configs, default_config),
                loop,
            )
            future.result()
        except AttributeError:
            # Engine not started; apply directly without event loop coordination.
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_configs_now(lane_configs, default_config)

    def open_group(
        self,
        group_id: str,
        primer_payload: Any | None = None,
        follower_payloads: Iterable[Any] | None = None,
        follower_ids: Iterable[str | None] | None = None,
        depends_on: Sequence[GroupDependency] | None = None,
        *,
        group_context: Mapping[str, Any] | None = None,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
    ) -> GroupHandle:
        if group_id in self._group_handles:
            raise ValueError(f"group already open: {group_id}")
        depends_on = self._resolve_dependencies(group_id, depends_on)
        handle = GroupHandle(
            self,
            group_id=group_id,
            primer_payload=primer_payload,
            follower_payloads=follower_payloads,
            follower_ids=follower_ids,
            depends_on=depends_on,
            group_context=group_context,
            cache_key=cache_key,
            cache_context=cache_context,
        )
        self._group_handles[group_id] = handle
        return handle

    def get_group(self, group_id: str) -> GroupHandle:
        handle = self._group_handles.get(group_id)
        if handle is None:
            raise ValueError(f"unknown group handle: {group_id}")
        return handle

    async def submit_primer(
        self,
        group_id: str,
        payload: Any,
        *,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
    ) -> asyncio.Future[JobResult] | None:
        await self._ensure_started()
        if self._closed:
            raise RuntimeError("ActyEngine is closed")

        exec_context = self._capture_exec_context()
        primer_job_id = self._controller.reserve_job_id(group_id, GroupTaskKind.PRIMER)
        primer_future = self._register_future(
            group_id,
            primer_job_id,
            kind=GroupTaskKind.PRIMER.value,
            follower_id=None,
        )

        def _cancel_future() -> None:
            if not primer_future.done():
                primer_future.cancel()
            self._drop_pending_job(primer_job_id)

        try:
            result = await self._gate.submit_primer(
                group_id,
                _wrap_payload(payload),
                job_id=primer_job_id,
                cache_group_id=cache_key,
                cache_context=cache_context,
                primer_cancel=_cancel_future,
                exec_context=exec_context,
            )
        except Exception:
            _cancel_future()
            raise
        if result is None:
            _cancel_future()
            return None
        return primer_future

    async def submit(
        self,
        group_id: str,
        payload: Any,
        *,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
        follower_id: str | None = None,
    ) -> asyncio.Future[JobResult] | None:
        await self._ensure_started()
        if self._closed:
            raise RuntimeError("ActyEngine is closed")

        has_primer = await self._has_primer(group_id)
        if not has_primer and not self._config.auto_primer_from_first_payload:
            raise ValueError(f"group has no primer: {group_id}")

        if not has_primer:
            return await self.submit_primer(
                group_id,
                payload,
                cache_key=cache_key,
                cache_context=cache_context,
            )
        return await self.submit_follower(group_id, payload, follower_id=follower_id)

    async def submit_follower(
        self,
        group_id: str,
        payload: Any,
        *,
        follower_id: str | None = None,
    ) -> asyncio.Future[JobResult]:
        """Submit a follower payload to an existing open group."""
        await self._ensure_started()
        if self._closed:
            raise RuntimeError("ActyEngine is closed")
        exec_context = self._capture_exec_context()
        job_id = self._controller.reserve_job_id(group_id, GroupTaskKind.FOLLOWER)
        future = self._register_future(
            group_id,
            job_id,
            kind=GroupTaskKind.FOLLOWER.value,
            follower_id=follower_id,
        )
        try:
            await self._gate.submit_followers(
                group_id,
                [_wrap_payload(payload)],
                job_ids=[job_id],
                follower_ids=[follower_id] if follower_id is not None else None,
                exec_contexts=[exec_context] if exec_context is not None else None,
            )
        except Exception:
            future.cancel()
            self._drop_pending_job(job_id)
            raise
        return future

    async def _open_group(
        self,
        group_id: str,
        *,
        primer_payload: Any | None,
        follower_payloads: Iterable[Any] | None,
        follower_ids: Iterable[str | None] | None,
        depends_on: Sequence[GroupDependency] | None,
        group_context: Mapping[str, Any] | None,
        cache_key: str | None,
        cache_context: Mapping[str, Any] | None,
    ) -> GroupSubmission:
        await self._ensure_started()
        if self._closed:
            raise RuntimeError("ActyEngine is closed")

        exec_context = self._capture_exec_context()
        payloads = list(follower_payloads) if follower_payloads is not None else []
        follower_id_list = list(follower_ids) if follower_ids is not None else None
        if follower_payloads is None and follower_id_list:
            raise ValueError("follower_ids requires follower_payloads")
        if follower_id_list is not None and len(follower_id_list) != len(payloads):
            raise ValueError("follower_ids length must match follower_payloads length")
        defer_primer = primer_payload is None
        primer_future: asyncio.Future[JobResult] | None = None
        primer_job_id: JobId | None = None

        if not defer_primer:
            primer_job_id = self._controller.reserve_job_id(group_id, GroupTaskKind.PRIMER)
            primer_future = self._register_future(
                group_id,
                primer_job_id,
                kind=GroupTaskKind.PRIMER.value,
                follower_id=None,
            )

        def _cancel_primer_future() -> None:
            if primer_future is not None and not primer_future.done():
                primer_future.cancel()
            if primer_job_id is not None:
                self._drop_pending_job(primer_job_id)

        follower_futures: list[asyncio.Future[JobResult]] = []
        follower_job_ids: list[JobId] = []
        follower_payload_list: list[Mapping[str, Any]] = []
        follower_exec_contexts: list[Any | None] | None = None
        if payloads:
            follower_payload_list = [_wrap_payload(p) for p in payloads]
            follower_job_ids = [
                self._controller.reserve_job_id(group_id, GroupTaskKind.FOLLOWER)
                for _ in payloads
            ]
            follower_futures = [
                self._register_future(
                    group_id,
                    job_id,
                    kind=GroupTaskKind.FOLLOWER.value,
                    follower_id=follower_id_list[idx] if follower_id_list else None,
                )
                for idx, job_id in enumerate(follower_job_ids)
            ]
            if exec_context is not None:
                follower_exec_contexts = [exec_context] * len(payloads)

        spec = GroupSpec(
            group_id=group_id,
            followers_total=None,
            warm_delay_s=self._config.warm_delay_s,
            max_followers_inflight=self._config.max_followers_inflight,
            followers_start_mode=self._config.followers_start_mode,
            payload=_wrap_payload(primer_payload) if primer_payload is not None else {},
            group_context=group_context,
            exec_context=exec_context,
            cache_group_id=cache_key,
            primer_content=primer_payload,
            cache_context=cache_context,
            defer_primer=defer_primer,
            run_primer_on_cache_hit=self._config.run_primer_on_cache_hit,
            depends_on=depends_on,
        )
        try:
            await self._gate.submit_group(
                spec,
                primer_job_id=primer_job_id,
                primer_cancel=None if defer_primer else _cancel_primer_future,
                follower_payloads=follower_payload_list,
                follower_job_ids=follower_job_ids if payloads else None,
                follower_ids=follower_id_list,
                follower_exec_contexts=follower_exec_contexts,
                close_after_admit=False,
            )
        except Exception:
            if primer_future is not None and not primer_future.done():
                primer_future.cancel()
            if primer_job_id is not None:
                self._drop_pending_job(primer_job_id)
            self._group_handles.pop(group_id, None)
            for job_id, future in zip(follower_job_ids, follower_futures):
                future.cancel()
                self._drop_pending_job(job_id)
            raise
        return GroupSubmission(primer=primer_future, followers=tuple(follower_futures))

    async def close_group(self, group_id: str) -> None:
        await self._gate.close_group(group_id)
        self._ensure_cleanup_task(group_id)

    async def close_all_groups(
        self,
        *,
        mode: Literal["close", "drop_pending"] = "close",
        timeout_s: float | None = None,
        on_timeout: Literal["drop_pending"] = "drop_pending",
    ) -> None:
        await self._ensure_started()
        if mode not in {"close", "drop_pending"}:
            raise ValueError(f"unknown close_all_groups mode: {mode}")
        if on_timeout not in {"drop_pending"}:
            raise ValueError(f"unknown close_all_groups on_timeout: {on_timeout}")
        if mode == "drop_pending" and timeout_s is not None:
            raise ValueError("timeout_s is only supported when mode='close'")

        if mode == "close":
            pending_ids, active_ids = await self._gate.list_group_ids()
            await self._gate.close_all_groups()
            for gid in [*pending_ids, *active_ids]:
                self._ensure_cleanup_task(gid)

            if timeout_s is None:
                return

            try:
                await asyncio.wait_for(self._wait_all_groups_closed(), timeout=timeout_s)
                return
            except asyncio.TimeoutError:
                logger.debug(
                    "Timed out waiting for groups to close; applying fallback",
                    timeout_s=timeout_s,
                    on_timeout=on_timeout,
                    mode=mode,
                )
                # Fall back to forced closure.
                if on_timeout == "drop_pending":
                    await self.close_all_groups(mode="drop_pending")
                return

        # mode == "drop_pending"
        pending_ids, active_ids = await self._gate.list_group_ids()
        group_ids = [*pending_ids, *active_ids]
        job_ids = await self._gate.drop_all_groups(reason="close_all_drop_pending")
        if job_ids:
            self._scheduler.cancel_jobs(job_ids, reason="explicit_cancel")
            self._awaitable_sink.cancel_jobs(job_ids)
        for gid in group_ids:
            self._group_handles.pop(gid, None)
        async with self._lease_lock:
            self._lease_counts.clear()

    async def results(self) -> AsyncIterator[JobResult]:
        await self._ensure_started()
        if self._result_stream_sink is None:
            self._result_stream_sink = AsyncQueueResultSink(
                maxsize=self._config.result_queue_maxsize,
                drop_policy=self._config.result_drop_policy,
            )
            self._result_fanout.add_sink(self._result_stream_sink)
        queue = self._result_stream_sink.queue
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            try:
                yield item
            finally:
                queue.task_done()

    async def close(self) -> WorkStealingReport | None:
        started = False
        async with self._state_lock:
            if self._closed:
                if self._scheduler_task is not None:
                    return await self._scheduler_task
                return None
            started = self._started
            self._closed = True

        if self._stats_task is not None:
            self._stats_task.cancel()
            await asyncio.gather(self._stats_task, return_exceptions=True)
            self._stats_task = None

        if started:
            # Ensure open groups are closed before shutting down admission.
            try:
                await self.close_all_groups(mode="close")
            except Exception:
                logger.exception("Failed to close all groups during shutdown")
            await self._gate.shutdown()
        else:
            await self._gate.shutdown()

        if self._admission is not None:
            await self._admission.close()

        await self._controller.close()
        report: WorkStealingReport | None = None
        if self._scheduler_task is not None:
            report = await self._scheduler_task

        if self._result_stream_sink is not None:
            await self._result_stream_sink.close()
        pending_futures = self._awaitable_sink.pending_count
        if pending_futures:
            logger.info("Cancelling pending futures during engine close", pending=pending_futures)
        self._awaitable_sink.cancel_pending()
        for task in self._finalizer_tasks.values():
            task.cancel()
        if self._finalizer_tasks:
            await asyncio.gather(*self._finalizer_tasks.values(), return_exceptions=True)
        self._finalizer_tasks.clear()
        with self._pending_lock:
            self._pending_by_group.clear()
            self._job_to_group.clear()
            self._group_failures.clear()

        async with self._lease_lock:
            self._lease_counts.clear()
        for task in self._cleanup_tasks.values():
            task.cancel()
        if self._cleanup_tasks:
            await asyncio.gather(*self._cleanup_tasks.values(), return_exceptions=True)
        self._cleanup_tasks.clear()
        self._group_handles.clear()

        if self._owns_event_bus and self._event_bus is not None:
            await self._event_bus.flush()
            await self._event_bus.stop()

        self._loop = None
        return report

    async def _wait_all_groups_closed(self) -> None:
        while True:
            pending_ids, active_ids = await self._gate.list_group_ids()
            if not pending_ids and not active_ids:
                return
            await asyncio.sleep(0.05)

    async def _has_primer(self, group_id: str) -> bool:
        return await self._gate.has_primer(group_id)

    def _capture_exec_context(self) -> Any | None:
        propagator = self._context_propagator
        if propagator is None:
            return None
        try:
            return propagator.capture()
        except Exception:
            logger.exception("Failed to capture execution context")
            return None

    def _register_future(
        self,
        group_id: str,
        job_id: JobId,
        *,
        kind: str,
        follower_id: str | None,
    ) -> asyncio.Future[JobResult]:
        future = self._awaitable_sink.register(job_id)
        with self._pending_lock:
            pending = self._pending_by_group.setdefault(group_id, {})
            pending[job_id] = _PendingJob(job_id=job_id, kind=kind, follower_id=follower_id)
            self._job_to_group[job_id] = group_id
        self._ensure_finalizer_task(group_id)
        return future

    def _drop_pending_job(self, job_id: JobId) -> None:
        with self._pending_lock:
            group_id = self._job_to_group.pop(job_id, None)
            if group_id is None:
                return
            pending = self._pending_by_group.get(group_id)
            if pending is None:
                return
            pending.pop(job_id, None)
            if not pending:
                self._pending_by_group.pop(group_id, None)

    def _mark_job_enqueued(self, job_id: JobId, original_job_id: JobId) -> None:
        target_ids = (original_job_id, job_id)
        with self._pending_lock:
            group_id = None
            for candidate in target_ids:
                group_id = self._job_to_group.get(candidate)
                if group_id is not None:
                    break
            if group_id is None:
                return
            pending = self._pending_by_group.get(group_id)
            if pending is None:
                return
            for candidate in target_ids:
                info = pending.get(candidate)
                if info is not None:
                    info.enqueued = True

    def _record_job_result(self, result: JobResult) -> None:
        original_job_id = self._extract_original_job_id(result)
        retry_scheduled = None
        if result.meta is not None and (
            "core_retry_scheduled" in result.meta or "result_handler_retry_scheduled" in result.meta
        ):
            retry_scheduled = bool(
                result.meta.get("core_retry_scheduled") or result.meta.get("result_handler_retry_scheduled")
            )

        if retry_scheduled:
            return

        group_id = None
        if result.group_id is not None:
            group_id = str(result.group_id)
        if group_id is None:
            group_id = self._job_to_group.get(original_job_id)
        if group_id is None:
            return

        with self._pending_lock:
            pending = self._pending_by_group.get(group_id)
            if pending is not None:
                pending.pop(original_job_id, None)
                self._job_to_group.pop(original_job_id, None)
                if not pending:
                    self._pending_by_group.pop(group_id, None)

            if not result.ok and group_id not in self._group_failures:
                self._group_failures[group_id] = _GroupFailureInfo(
                    error=result.error,
                    kind=result.kind,
                    job_id=result.job_id,
                    follower_id=str(result.follower_id) if result.follower_id is not None else None,
                    dependency_reason=None,
                )

    async def _on_controller_group_completed(self, group_id: str, ok: bool) -> None:
        await self._gate.on_group_completed(group_id, ok)

    async def _record_dependency_failure(
        self,
        group_id: str,
        dependency_id: str,
        policy: GroupDependencyPolicy,
        reason: str | None = None,
    ) -> None:
        with self._pending_lock:
            if group_id in self._group_failures:
                return
            self._group_failures[group_id] = _GroupFailureInfo(
                error=f"dependency_failed({policy.value}): {dependency_id}",
                kind="dependency_failed",
                job_id=None,
                follower_id=dependency_id,
                dependency_reason=reason,
            )

    def _ensure_finalizer_task(self, group_id: str) -> None:
        task = self._finalizer_tasks.get(group_id)
        if task is not None and not task.done():
            return
        self._finalizer_tasks[group_id] = asyncio.create_task(
            self._finalize_pending_futures(group_id)
        )

    async def _finalize_pending_futures(self, group_id: str) -> None:
        try:
            await self._wait_group_finished(group_id)
        except Exception:
            logger.exception("Failed while waiting to finalize group futures", group_id=group_id)
            return

        # Allow in-flight result handlers to record failures before finalizing.
        await asyncio.sleep(0)
        self._notify_executor_group_closed(group_id)

        to_finalize: dict[JobId, _PendingJob] = {}
        with self._pending_lock:
            pending = self._pending_by_group.get(group_id, {})
            failure = self._group_failures.pop(group_id, None)
            if pending:
                remaining: dict[JobId, _PendingJob] = {}
                for job_id, info in pending.items():
                    if info.enqueued:
                        remaining[job_id] = info
                    else:
                        to_finalize[job_id] = info
                if to_finalize:
                    for job_id in to_finalize:
                        self._job_to_group.pop(job_id, None)
                    if remaining:
                        self._pending_by_group[group_id] = remaining
                    else:
                        self._pending_by_group.pop(group_id, None)

        if not to_finalize:
            self._finalizer_tasks.pop(group_id, None)
            return

        for info in to_finalize.values():
            result = self._build_synthetic_result(group_id, info, failure)
            self._awaitable_sink.resolve(info.job_id, result)

        self._finalizer_tasks.pop(group_id, None)

    @staticmethod
    def _extract_original_job_id(result: JobResult) -> JobId:
        if result.meta is not None and result.meta.get("original_job_id") is not None:
            return JobId(str(result.meta.get("original_job_id")))
        return result.job_id

    @staticmethod
    def _build_synthetic_result(
        group_id: str,
        info: _PendingJob,
        failure: _GroupFailureInfo | None,
    ) -> JobResult:
        finalizer_reason = "group_failed" if failure is not None else "group_done"
        base_error = "group failed before job was enqueued" if failure is not None else (
            "group completed before job was enqueued"
        )
        error = base_error
        if failure is not None and failure.error:
            error = f"{base_error}: {failure.error}"
        meta: dict[str, Any] = {
            "finalized_without_execution": True,
            "group_finalized": True,
            "finalizer_reason": finalizer_reason,
            "original_job_id": str(info.job_id),
        }
        if failure is not None:
            meta.update(
                {
                    "finalizer_error": failure.error,
                    "finalizer_failed_kind": failure.kind,
                    "finalizer_failed_job_id": str(failure.job_id) if failure.job_id is not None else None,
                    "finalizer_failed_follower_id": failure.follower_id,
                }
            )
            if failure.dependency_reason is not None:
                meta["dependency_failure_reason"] = failure.dependency_reason

        return JobResult(
            job_id=info.job_id,
            kind=info.kind,
            ok=False,
            output=None,
            error=error,
            group_id=GroupId(group_id),
            follower_id=FollowerId(info.follower_id) if info.follower_id is not None else None,
            meta=meta,
        )

    def _ensure_cleanup_task(self, group_id: str) -> None:
        if group_id not in self._group_handles:
            return
        task = self._cleanup_tasks.get(group_id)
        if task is not None and not task.done():
            return
        self._cleanup_tasks[group_id] = asyncio.create_task(self._cleanup_group_handle(group_id))

    async def _cleanup_group_handle(self, group_id: str) -> None:
        await self._wait_group_finished(group_id)
        self._notify_executor_group_closed(group_id)
        self._group_handles.pop(group_id, None)
        async with self._lease_lock:
            self._lease_counts.pop(group_id, None)
        self._cleanup_tasks.pop(group_id, None)

    def _notify_executor_group_closed(self, group_id: str) -> None:
        self._call_executor_hook("clear_group_session", group_id)

    def _call_executor_hook(self, hook: str, *args) -> None:
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
            method = getattr(target, hook, None)
            if callable(method):
                try:
                    method(*args)
                except Exception:
                    logger.debug(
                        "Executor hook failed",
                        hook=hook,
                        group_id=str(args[0]) if args else None,
                    )
            for attr in ("_executor", "_inner", "inner", "executor"):
                try:
                    inner = getattr(target, attr)
                except Exception:
                    continue
                if not looks_like_executor(inner):
                    continue
                apply(inner)

        apply(self._executor)

    async def _wait_group_finished(self, group_id: str) -> None:
        try:
            await self._gate.wait_group_done(group_id)
        except ValueError:
            logger.debug("Group already removed while waiting; skipping", group_id=group_id)
            return

    async def _acquire_lease(self, group_id: str) -> None:
        async with self._lease_lock:
            self._lease_counts[group_id] = self._lease_counts.get(group_id, 0) + 1

    async def _release_lease(self, group_id: str) -> None:
        should_close = False
        async with self._lease_lock:
            count = self._lease_counts.get(group_id, 0)
            if count <= 1:
                self._lease_counts.pop(group_id, None)
                should_close = True
            else:
                self._lease_counts[group_id] = count - 1
        if should_close:
            try:
                await self.close_group(group_id)
            except ValueError:
                logger.debug("Group already closed while releasing lease", group_id=group_id)
                # Group already completed/removed.
                return

    async def _ensure_started(self) -> None:
        async with self._state_lock:
            if self._started:
                return
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            if self._admission is not None:
                await self._admission.start()
            self._loop = asyncio.get_running_loop()
            self._scheduler_task = asyncio.create_task(
                self._scheduler.run(self._controller, self._executor)
            )
            self._start_stats_emitter()
            self._started = True

    async def _apply_lane_configs(
        self,
        lane_configs: Mapping[str, LaneConfig] | None,
        default_config: LaneConfig | None,
    ) -> None:
        async with self._state_lock:
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_configs_now(lane_configs, default_config)

    async def _apply_lane_config(
        self,
        *,
        lane_id: str,
        weight: float | object,
        max_inflight: int | None | object,
        ensure: bool,
    ) -> None:
        async with self._state_lock:
            if self._closed:
                raise RuntimeError("ActyEngine is closed")
            self._apply_lane_config_now(
                lane_id=lane_id,
                weight=weight,
                max_inflight=max_inflight,
                ensure=ensure,
            )

    def _apply_lane_configs_now(
        self,
        lane_configs: Mapping[str, LaneConfig] | None,
        default_config: LaneConfig | None,
    ) -> None:
        self._scheduler.update_lane_configs(
            lane_configs=lane_configs,
            default_config=default_config,
        )
        lane_summary = None
        if lane_configs:
            lane_summary = {lane_id: config.weight for lane_id, config in lane_configs.items()}
        logger.debug(
            "Lane configs updated",
            lanes=lane_summary,
            default_weight=default_config.weight if default_config is not None else None,
        )

    def _apply_lane_config_now(
        self,
        *,
        lane_id: str,
        weight: float | object,
        max_inflight: int | None | object,
        ensure: bool,
    ) -> None:
        snapshot = getattr(self._scheduler, "_lane_snapshot", None)
        current_configs = None
        default_config = None
        if snapshot is not None:
            current_configs = getattr(snapshot, "lane_configs", None)
            default_config = getattr(snapshot, "default_config", None)
        if not isinstance(current_configs, Mapping):
            current_configs = getattr(self._scheduler, "_lane_configs", None)
        if not isinstance(default_config, LaneConfig):
            default_config = getattr(self._scheduler, "_lane_default", None)
        if not isinstance(default_config, LaneConfig):
            default_config = LaneConfig()
        lane_configs = dict(current_configs) if isinstance(current_configs, Mapping) else {}
        current = lane_configs.get(lane_id)
        if current is None and not ensure:
            return
        base_config = current if current is not None else default_config
        new_weight = base_config.weight if weight is UNSET else weight
        new_max_inflight = (
            base_config.max_inflight if max_inflight is UNSET else max_inflight
        )
        new_config = LaneConfig(weight=new_weight, max_inflight=new_max_inflight)
        ActyEngine._validate_lane_config(new_config, lane_id=lane_id)
        if current is not None and new_config == current and not ensure:
            return
        lane_configs[lane_id] = new_config
        ActyEngine._validate_lane_configs(lane_configs, None)
        self._apply_lane_configs_now(lane_configs, None)

    @staticmethod
    def _validate_lane_configs(
        lane_configs: Mapping[str, LaneConfig] | None,
        default_config: LaneConfig | None,
    ) -> None:
        if lane_configs is not None and not isinstance(lane_configs, Mapping):
            raise ValueError("lane_configs must be a mapping or None")
        if lane_configs is not None:
            for lane_id, config in lane_configs.items():
                if not isinstance(lane_id, str) or not lane_id:
                    raise ValueError("lane id must be a non-empty string")
                ActyEngine._validate_lane_config(config, lane_id=lane_id)
        if default_config is not None:
            ActyEngine._validate_lane_config(default_config, lane_id="default")

    @staticmethod
    def _validate_lane_config(config: LaneConfig, *, lane_id: str) -> None:
        if not isinstance(config, LaneConfig):
            raise ValueError(f"lane config for {lane_id} must be a LaneConfig")
        weight = config.weight
        if (
            not isinstance(weight, (int, float))
            or isinstance(weight, bool)
            or not math.isfinite(weight)
            or weight <= 0
        ):
            raise ValueError(f"lane {lane_id} weight must be a positive number")
        max_inflight = config.max_inflight
        if max_inflight is not None and (
            not isinstance(max_inflight, int)
            or isinstance(max_inflight, bool)
            or max_inflight <= 0
        ):
            raise ValueError(f"lane {lane_id} max_inflight must be a positive integer or None")

    def _start_stats_emitter(self) -> None:
        interval = self._config.stats_emit_interval_s
        if interval is None or interval <= 0:
            return
        if self._event_bus is None:
            return
        if self._stats_task is not None and not self._stats_task.done():
            return
        self._stats_task = asyncio.create_task(self._emit_runtime_stats_loop(interval))

    async def _emit_runtime_stats_loop(self, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await self._emit_runtime_stats()
        except asyncio.CancelledError:
            return

    async def _emit_runtime_stats(self) -> None:
        if self._event_bus is None:
            return
        scheduler_stats = self._scheduler.stats()
        controller_stats = await self._controller.get_stats()
        payload = {
            "queue_depth": scheduler_stats.queue_depth,
            "pending_by_kind": scheduler_stats.pending_by_kind,
            "inflight_by_kind": scheduler_stats.inflight_by_kind,
            "pending_by_lane": scheduler_stats.pending_by_lane,
            "inflight_by_lane": scheduler_stats.inflight_by_lane,
            "inflight_workers": scheduler_stats.inflight_workers,
            "groups_by_state": controller_stats.groups_by_state,
            "warm_tasks": controller_stats.warm_tasks,
            "counts": scheduler_stats.counts,
        }
        await self._event_bus.emit(type="runtime_stats", payload=payload)


def _wrap_payload(payload: Any) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return payload
    return {"payload": payload}
