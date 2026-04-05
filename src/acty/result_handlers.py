"""Result handler composition helpers."""

from __future__ import annotations

import inspect

from acty_core.result_handlers import AcceptResult, JobResultHandler


def compose_handlers(*handlers: JobResultHandler) -> JobResultHandler:
    """Compose result handlers so later handlers run only after accept/None."""

    async def handler(result, context):
        action = None
        for inner in handlers:
            action = inner(result, context)
            if inspect.isawaitable(action):
                action = await action
            if action is None or isinstance(action, AcceptResult):
                continue
            return action
        return action

    handler.__name__ = "composed_result_handler"
    return handler


__all__ = ["compose_handlers"]
