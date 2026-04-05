"""Console capture guardrails for the TUI.

These constants document the scope decisions for console capture so future
implementation stays in-process and TUI-local without requiring changes
outside acty.tui.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ConsoleCaptureOutputMode = Literal["tui_only", "tee"]


@dataclass(frozen=True)
class ConsoleCaptureGuardrails:
    """Guardrails for in-process console capture within the TUI."""

    # In-process only: do not wrap subprocess/PTY/curses execution.
    in_process_only: bool = True
    # "Regular logs" are stdout/stderr prints plus logging handlers.
    regular_log_sources: tuple[str, ...] = ("stdout", "stderr", "logging")
    # Keep scheduler logs as the default view; console logs are opt-in.
    console_pane_enabled_by_default: bool = False
    # When enabled, capture output goes to the TUI only unless explicitly tee'd.
    output_mode: ConsoleCaptureOutputMode = "tui_only"


CONSOLE_CAPTURE_GUARDRAILS = ConsoleCaptureGuardrails()


__all__ = [
    "CONSOLE_CAPTURE_GUARDRAILS",
    "ConsoleCaptureGuardrails",
    "ConsoleCaptureOutputMode",
]
