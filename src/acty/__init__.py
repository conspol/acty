"""acty: group-only runtime with TUI (depends on acty-core)."""

from .client import ActyClient
from .engine import ActyEngine, EngineConfig, GroupHandle, GroupLease, GroupSubmission
from .exec_resolver import (
    CallableExecResolver,
    CompositeExecResolver,
    ExecResolver,
    MappingExecResolver,
    resolve_payload_with_exec_id,
)
from acty_core.context import ContextPropagator, ContextVarsPropagator
from acty_core.events import ErrorPayloadPolicy
from acty_core.lifecycle import GroupDependency, GroupDependencyPolicy
from acty_core.result_handlers import (
    AcceptResult,
    FailResult,
    JobResultAction,
    JobResultHandler,
    JobResultHandlerContext,
    ReplaceResult,
    ResubmitJob,
    ResultHandlerErrorPolicy,
    ResultHandlerRetryError,
    ResultHandlerTelemetryConfig,
)
from acty_core.telemetry import TelemetryPrivacyConfig
from .executors import (
    EchoExecutor,
    LifecycleExecutorAdapter,
    NoopExecutor,
    ResolverExecutor,
    SimulatedExecutor,
    SimulatedExecutorConfig,
    TenacityRetryAdapter,
    ensure_lifecycle_executor,
    resolver_retry_payload_on_retry,
)
from .groups import (
    CacheKeyResolver,
    GroupCloseHook,
    GroupContextBuilder,
    GroupIdResolver,
    GroupKey,
    GroupRegistry,
)
from .lane import Lane
from .logging import HandlerKind, configure_logging
from .result_handlers import compose_handlers
from .retry_budget import retry_factory_from_payload
from .tui import TuiState, create_tui_app

__all__ = [
    "ActyClient",
    "ActyEngine",
    "EngineConfig",
    "GroupHandle",
    "GroupLease",
    "GroupSubmission",
    "ContextPropagator",
    "ContextVarsPropagator",
    "GroupDependency",
    "GroupDependencyPolicy",
    "AcceptResult",
    "FailResult",
    "JobResultAction",
    "JobResultHandler",
    "JobResultHandlerContext",
    "ReplaceResult",
    "ResubmitJob",
    "ResultHandlerErrorPolicy",
    "ResultHandlerRetryError",
    "ResultHandlerTelemetryConfig",
    "TelemetryPrivacyConfig",
    "ErrorPayloadPolicy",
    "GroupKey",
    "GroupIdResolver",
    "CacheKeyResolver",
    "GroupContextBuilder",
    "GroupCloseHook",
    "GroupRegistry",
    "Lane",
    "ExecResolver",
    "MappingExecResolver",
    "CallableExecResolver",
    "CompositeExecResolver",
    "resolve_payload_with_exec_id",
    "NoopExecutor",
    "EchoExecutor",
    "SimulatedExecutor",
    "SimulatedExecutorConfig",
    "LifecycleExecutorAdapter",
    "ResolverExecutor",
    "resolver_retry_payload_on_retry",
    "TenacityRetryAdapter",
    "ensure_lifecycle_executor",
    "HandlerKind",
    "configure_logging",
    "compose_handlers",
    "retry_factory_from_payload",
    "TuiState",
    "create_tui_app",
]
