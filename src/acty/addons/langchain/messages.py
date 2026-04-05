"""Compatibility shim for acty-langchain message helpers."""

try:
    from acty_langchain import (
        from_acty_messages,
        messages_from_acty_payload,
        messages_to_acty_payload,
        to_acty_messages,
    )
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ImportError(
        "acty.addons.langchain requires acty-langchain to be installed"
    ) from exc

__all__ = [
    "from_acty_messages",
    "messages_from_acty_payload",
    "messages_to_acty_payload",
    "to_acty_messages",
]
