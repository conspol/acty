"""Group registry helper for memoizing open group handles."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import structlog

from acty_core.lifecycle.groups import GroupDependency, normalize_dependencies

logger = structlog.get_logger(__name__)

GroupKey = tuple[str, str]
GroupIdResolver = Callable[[GroupKey], str]
CacheKeyResolver = Callable[[GroupKey], str | None]
GroupContextBuilder = Callable[[GroupKey], Mapping[str, Any] | None]
GroupCloseHook = Callable[[str], None]


@dataclass(frozen=True)
class _GroupEntry:
    handle: Any
    dependencies: tuple[tuple[str, Any, Any], ...]


class GroupRegistry:
    def __init__(
        self,
        engine: Any,
        *,
        group_id_resolver: GroupIdResolver,
        cache_key_resolver: CacheKeyResolver | None = None,
        group_context_builder: GroupContextBuilder | None = None,
        on_group_close: GroupCloseHook | None = None,
    ) -> None:
        self._engine = engine
        self._group_id_resolver = group_id_resolver
        self._cache_key_resolver = cache_key_resolver
        self._group_context_builder = group_context_builder
        self._on_group_close = on_group_close
        self._groups: dict[GroupKey, _GroupEntry] = {}
        self._lock = asyncio.Lock()

    async def ensure(
        self,
        key: GroupKey,
        *,
        depends_on: Sequence[GroupDependency] | None = None,
    ) -> Any:
        has_depends = depends_on is not None
        normalized = normalize_dependencies(depends_on) if has_depends else ()
        async with self._lock:
            entry = self._groups.get(key)
            if entry is not None:
                if has_depends and entry.dependencies != normalized:
                    raise ValueError(
                        "Group dependencies already set for "
                        f"key={key!r}: existing={entry.dependencies!r} requested={normalized!r}"
                    )
                return entry.handle
            group_id = self._group_id_resolver(key)
            cache_key = self._cache_key_resolver(key) if self._cache_key_resolver is not None else None
            group_context = None
            if self._group_context_builder is not None:
                group_context = self._group_context_builder(key)
            logger.debug("group_registry_open", group_id=group_id)
            handle = self._engine.open_group(
                group_id,
                group_context=group_context,
                cache_key=cache_key,
                depends_on=depends_on,
            )
            self._groups[key] = _GroupEntry(handle=handle, dependencies=normalized)
        try:
            await handle
        except Exception:
            async with self._lock:
                entry = self._groups.get(key)
                if entry is not None and entry.handle is handle:
                    self._groups.pop(key, None)
            raise
        return handle

    async def get(self, key: GroupKey) -> Any:
        async with self._lock:
            entry = self._groups.get(key)
            if entry is not None:
                return entry.handle
        raise KeyError(f"Group {key!r} is not open")

    async def close(self, key: GroupKey) -> None:
        await self._close_key(key)

    async def close_namespace(self, namespace: str) -> None:
        async with self._lock:
            keys = [key for key in self._groups if key[0] == namespace]
        for key in keys:
            await self._close_key(key)

    async def close_all(self) -> None:
        async with self._lock:
            keys = list(self._groups.keys())
        for key in keys:
            await self._close_key(key)

    async def _close_key(self, key: GroupKey) -> None:
        async with self._lock:
            entry = self._groups.pop(key, None)
        if entry is None:
            return
        handle = entry.handle
        group_id = getattr(handle, "group_id", None)
        logger.debug("group_registry_close", group_id=group_id)
        try:
            closer = getattr(handle, "close", None)
            if closer is not None:
                result = closer()
                if inspect.isawaitable(result):
                    await result
        except Exception:
            logger.exception("Failed to close group handle", group_id=group_id)
        if group_id and self._on_group_close is not None:
            try:
                self._on_group_close(group_id)
            except Exception:
                logger.exception("Failed to run on_group_close hook", group_id=group_id)


__all__ = [
    "CacheKeyResolver",
    "GroupCloseHook",
    "GroupContextBuilder",
    "GroupIdResolver",
    "GroupKey",
    "GroupRegistry",
]
