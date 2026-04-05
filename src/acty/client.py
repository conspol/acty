"""Thin client wrapper for ActyEngine (async call-style usage)."""

from __future__ import annotations

import asyncio
from typing import Any, Iterable, Mapping, Sequence

from acty_core.core.types import JobResult
from acty_core.lifecycle import GroupDependency

from acty.engine import ActyEngine
from acty.engine import GroupHandle


class ActyClient:
    """Convenience wrapper around ActyEngine for call-style APIs."""

    def __init__(self, engine: ActyEngine) -> None:
        self._engine = engine

    @property
    def engine(self) -> ActyEngine:
        return self._engine

    async def call(
        self,
        group_id: str,
        primer_payload: Any,
        *,
        depends_on: Sequence[GroupDependency] | None = None,
        group_context: Mapping[str, Any] | None = None,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
    ) -> JobResult | None:
        """Create a group and await the primer result (if executed)."""
        submission = await self._engine.submit_group(
            group_id,
            primer_payload,
            follower_payloads=None,
            depends_on=depends_on,
            group_context=group_context,
            cache_key=cache_key,
            cache_context=cache_context,
        )
        if submission.primer is None:
            return None
        try:
            return await submission.primer
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            return None

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
        return self._engine.open_group(
            group_id,
            primer_payload,
            follower_payloads,
            follower_ids,
            depends_on=depends_on,
            group_context=group_context,
            cache_key=cache_key,
            cache_context=cache_context,
        )

    def get_group(self, group_id: str) -> GroupHandle:
        return self._engine.get_group(group_id)

    async def call_follower(
        self,
        group_id: str,
        payload: Any,
        *,
        follower_id: str | None = None,
    ) -> JobResult:
        """Submit a follower payload to an open group and await the result."""
        future = await self._engine.submit_follower(group_id, payload, follower_id=follower_id)
        return await future

    async def close_group(self, group_id: str) -> None:
        await self._engine.close_group(group_id)

    async def submit(
        self,
        group_id: str,
        payload: Any,
        *,
        cache_key: str | None = None,
        cache_context: Mapping[str, Any] | None = None,
        follower_id: str | None = None,
    ) -> JobResult | None:
        future = await self._engine.submit(
            group_id,
            payload,
            cache_key=cache_key,
            cache_context=cache_context,
            follower_id=follower_id,
        )
        if future is None:
            return None
        return await future

    async def close_all_groups(
        self,
        *,
        mode: str = "close",
        timeout_s: float | None = None,
        on_timeout: str = "drop_pending",
    ) -> None:
        await self._engine.close_all_groups(
            mode=mode,
            timeout_s=timeout_s,
            on_timeout=on_timeout,
        )

    async def close(self) -> None:
        await self._engine.close()
