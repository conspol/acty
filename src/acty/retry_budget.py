"""Retry budget helpers for acty backends."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

from tenacity import AsyncRetrying, stop_after_attempt, wait_fixed

from acty_core.core import Job


def retry_factory_from_payload(
    payload_key: str = "retry_attempts",
    max_attempts: int = 3,
) -> Callable[[Job], AsyncRetrying]:
    """Return a retry factory that maps payload retry budgets to attempt limits."""

    def factory(job: Job) -> AsyncRetrying:
        attempts = 1
        payload = job.payload
        if isinstance(payload, Mapping):
            try:
                attempts = int(payload.get(payload_key) or 0) + 1
            except (TypeError, ValueError):
                attempts = 1
        attempts = max(1, min(attempts, max_attempts))
        return AsyncRetrying(stop=stop_after_attempt(attempts), wait=wait_fixed(0.0))

    return factory


__all__ = ["retry_factory_from_payload"]
