"""Logging facade for acty (delegates to acty-core)."""

from __future__ import annotations

from typing import Iterable

import structlog

from acty_core.logging import HandlerKind
from acty_core.logging import configure_logging as _configure_logging


def configure_logging(
    *,
    level: int | str = "INFO",
    handler: HandlerKind = "rich",
    logger_name: str = "acty",
    reset_root: bool = False,
    show_locals: bool = True,
    rich_tracebacks: bool = True,
    install_traceback: bool = True,
    traceback_suppress: Iterable[object] | None = None,
) -> structlog.stdlib.BoundLogger:
    """Configure structlog + stdlib logging for acty.

    This is a thin wrapper over acty-core's configure_logging with a different
    default logger name.
    """

    return _configure_logging(
        level=level,
        handler=handler,
        logger_name=logger_name,
        reset_root=reset_root,
        show_locals=show_locals,
        rich_tracebacks=rich_tracebacks,
        install_traceback=install_traceback,
        traceback_suppress=traceback_suppress,
    )
