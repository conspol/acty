from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable


def _default_interval(timeout: float) -> float:
    return min(0.01, timeout / 10) if timeout > 0 else 0.0


async def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 1.0,
    interval: float | None = None,
    message: str | None = None,
    on_timeout: Callable[[], str] | None = None,
) -> None:
    """Wait until predicate returns True or timeout expires."""
    if message is not None and on_timeout is not None:
        raise ValueError("provide either message or on_timeout, not both")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    sleep_interval = _default_interval(timeout) if interval is None else interval
    while True:
        if predicate():
            return
        if loop.time() >= deadline:
            if on_timeout is not None:
                detail = on_timeout()
                raise TimeoutError(detail)
            if message is not None:
                raise TimeoutError(message)
            raise TimeoutError("condition not met before timeout")
        await asyncio.sleep(sleep_interval)


async def wait_until_async(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float = 1.0,
    interval: float | None = None,
    message: str | None = None,
    on_timeout: Callable[[], str] | None = None,
) -> None:
    """Wait until an async predicate returns True or timeout expires."""
    if message is not None and on_timeout is not None:
        raise ValueError("provide either message or on_timeout, not both")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    sleep_interval = _default_interval(timeout) if interval is None else interval
    while True:
        if await predicate():
            return
        if loop.time() >= deadline:
            if on_timeout is not None:
                detail = on_timeout()
                raise TimeoutError(detail)
            if message is not None:
                raise TimeoutError(message)
            raise TimeoutError("condition not met before timeout")
        await asyncio.sleep(sleep_interval)


async def wait_for_tasks(
    tasks: Iterable[Awaitable[object]],
    *,
    timeout: float = 1.0,
) -> list[object]:
    """Wait for a collection of awaitables with a shared timeout."""
    return await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)


async def drain_loop(iterations: int = 1) -> None:
    """Give the event loop a chance to process pending tasks."""
    for _ in range(iterations):
        await asyncio.sleep(0)
