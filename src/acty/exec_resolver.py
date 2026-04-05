"""ExecResolver protocol and helpers for resolving exec_id payloads."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence

import structlog

from acty_core.core.types import Job

logger = structlog.get_logger(__name__)


class ExecResolver(Protocol):
    """Resolve exec_id values into runnable objects."""

    def resolve(self, exec_id: str, *, job: Job) -> Any | Awaitable[Any]: ...


@dataclass(frozen=True)
class MappingExecResolver:
    """Resolve exec_id values from a mapping."""

    mapping: Mapping[str, Any]

    def resolve(self, exec_id: str, *, job: Job) -> Any:
        if exec_id not in self.mapping:
            raise KeyError(f"exec_id '{exec_id}' not found in mapping")
        return self.mapping[exec_id]


@dataclass(frozen=True)
class CallableExecResolver:
    """Resolve exec_id values via a callable."""

    fn: Callable[[str, Job], Any | Awaitable[Any]]

    def resolve(self, exec_id: str, *, job: Job) -> Any | Awaitable[Any]:
        return self.fn(exec_id, job)


@dataclass(frozen=True)
class CompositeExecResolver:
    """Resolve exec_id values by querying a sequence of resolvers."""

    resolvers: Sequence[ExecResolver]

    async def resolve(self, exec_id: str, *, job: Job) -> Any:
        last_error: KeyError | None = None
        for resolver in self.resolvers:
            try:
                result = resolver.resolve(exec_id, job=job)
            except KeyError as exc:
                last_error = exc
                continue
            if inspect.isawaitable(result):
                try:
                    return await result
                except KeyError as exc:
                    last_error = exc
                    continue
            return result
        message = f"exec_id '{exec_id}' not resolved"
        if last_error is None:
            raise KeyError(message)
        raise KeyError(message) from last_error


def resolve_payload_with_exec_id(
    payload: Any,
    resolver: ExecResolver | None,
    *,
    job: Job,
    exec_id_key: str = "exec_id",
    payload_key: str = "payload",
    runnable_key: str = "runnable",
) -> Any | Awaitable[Any]:
    """Return a payload copy with runnable injected when exec_id resolution succeeds."""

    if not isinstance(payload, Mapping):
        return payload
    if _payload_has_runnable(payload, payload_key, runnable_key):
        return payload
    exec_id = _extract_exec_id(payload, exec_id_key, payload_key)
    if exec_id is None:
        return payload
    if resolver is None:
        logger.warning(
            "exec_id provided but no resolver configured",
            exec_id=exec_id,
            payload_keys=list(payload.keys()),
        )
        return payload
    resolved = resolver.resolve(exec_id, job=job)
    if inspect.isawaitable(resolved):
        async def _await_and_apply() -> Any:
            runnable = await resolved
            return _inject_runnable(payload, runnable, payload_key, runnable_key)

        return _await_and_apply()
    return _inject_runnable(payload, resolved, payload_key, runnable_key)


def _payload_has_runnable(payload: Mapping[str, Any], payload_key: str, runnable_key: str) -> bool:
    if runnable_key in payload:
        return True
    inner = payload.get(payload_key)
    return isinstance(inner, Mapping) and runnable_key in inner


def _extract_exec_id(payload: Mapping[str, Any], exec_id_key: str, payload_key: str) -> str | None:
    exec_id = payload.get(exec_id_key)
    if exec_id is None and isinstance(payload.get(payload_key), Mapping):
        exec_id = payload[payload_key].get(exec_id_key)
    if exec_id is None:
        return None
    if not isinstance(exec_id, str):
        raise ValueError(f"{exec_id_key} must be a string")
    return exec_id


def _inject_runnable(
    payload: Mapping[str, Any],
    runnable: Any,
    payload_key: str,
    runnable_key: str,
) -> dict[str, Any]:
    updated = dict(payload)
    inner = payload.get(payload_key)
    if isinstance(inner, Mapping):
        inner_copy = dict(inner)
        inner_copy[runnable_key] = runnable
        updated[payload_key] = inner_copy
    else:
        updated[runnable_key] = runnable
    return updated
