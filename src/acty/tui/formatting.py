"""Formatting helpers for TUI display."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from acty_core.events.snapshot import Snapshot
from acty_core.events.types import Event

_MAX_PAYLOAD_VALUE_CHARS = 160


def _clean_value(value: Any, *, max_len: int = _MAX_PAYLOAD_VALUE_CHARS) -> str:
    text = str(value)
    text = " ".join(text.split())
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def _format_kv(key: str, value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_value(value)
    if not text:
        return None
    if any(ch.isspace() for ch in text):
        text = text.replace('"', "'")
        text = f"\"{text}\""
    return f"{key}={text}"


def _error_summary(payload: Mapping[str, Any]) -> Any:
    if payload.get("error") is not None:
        return payload.get("error")
    info = payload.get("error_info")
    if isinstance(info, Mapping):
        return info.get("message") or info.get("type")
    return None


def _payload_summary(event: Event) -> list[str]:
    payload: Mapping[str, Any] = event.payload if isinstance(event.payload, Mapping) else {}
    extras: list[str] = []

    def add(key: str, value: Any) -> None:
        item = _format_kv(key, value)
        if item:
            extras.append(item)

    if event.type == "job_failed":
        err = _error_summary(payload)
        add("error", err)
        add("attempt", payload.get("attempt"))
        if err == "result_handler_resubmit":
            add("action", "resubmit")
    elif event.type == "job_retrying":
        add("attempt", payload.get("attempt"))
        add("delay_s", payload.get("delay_s"))
        add("source", payload.get("source"))
        add("error_type", payload.get("error_type"))
    elif event.type in {"primer_retrying", "follower_retrying"}:
        add("attempt", payload.get("attempt"))
        add("delay_s", payload.get("delay_s"))
    elif event.type == "job_result_handled":
        add("action", payload.get("action"))
        add("reason", payload.get("reason"))
        add("handler", payload.get("handler"))
        add("rh_attempt", payload.get("result_handler_attempt"))
    elif event.type == "job_result_handler_failed":
        add("handler", payload.get("handler"))
        add("error_type", payload.get("error_type"))
        add("policy", payload.get("policy"))
    elif event.type == "job_resubmitted":
        add("reason", payload.get("reason"))
        add("handler", payload.get("handler"))
        add("rh_attempt", payload.get("result_handler_attempt"))
        add("repair_attempt", payload.get("repair_attempt"))
    elif event.type == "job_cancelled":
        add("reason", payload.get("reason"))
        add("phase", payload.get("phase"))
    elif event.type == "job_ignored":
        add("reason", payload.get("reason"))
        add("group_state", payload.get("group_state"))
    elif event.type == "group_failed":
        add("reason", payload.get("reason"))
        add("failed_kind", payload.get("failed_kind"))
        add("failed_job_id", payload.get("failed_job_id"))
    elif event.type in {"follower_failed", "primer_failed"}:
        err = _error_summary(payload)
        add("error", err)
        add("attempt", payload.get("attempt"))

    return extras


def format_event(event: Event) -> str:
    """Format event as a string (for storage and copy)."""
    dt = datetime.fromtimestamp(event.ts)
    timestamp = dt.strftime("%H:%M:%S") + f".{dt.microsecond // 1000:03d}"

    parts = [event.type]
    if event.job_id:
        parts.append(f"job={event.job_id}")
    if event.group_id:
        parts.append(f"group={event.group_id}")
    if event.follower_id:
        parts.append(f"follower={event.follower_id}")
    if event.kind:
        parts.append(f"kind={event.kind}")
    if event.pool:
        parts.append(f"pool={event.pool}")
    parts.extend(_payload_summary(event))
    log_text = " ".join(parts)
    return f"{log_text}\t{timestamp}"


def format_log_for_display(log_line: str) -> str:
    """Format a log line for TUI display with timestamp at the beginning."""
    if "\t" in log_line:
        log_text, timestamp = log_line.rsplit("\t", 1)
    else:
        log_text = log_line
        timestamp = ""

    if timestamp:
        return f"[dim]{timestamp}[/] {log_text}"
    return log_text


def format_snapshot(snapshot: Snapshot) -> str:
    active_groups = (
        snapshot.groups_primer_ready
        + snapshot.groups_primer_inflight
        + snapshot.groups_warm_delay
        + snapshot.groups_followers_ready
    )
    done_groups = snapshot.groups_done + snapshot.groups_failed
    lines = [
        f"queued: {snapshot.queued}",
        f"started: {snapshot.started}",
        f"finished: {snapshot.finished}",
        f"failed: {snapshot.failed}",
        f"resubmitted: {snapshot.resubmitted}",
        f"cancelled: {snapshot.cancelled}",
        f"ignored: {snapshot.ignored}",
        f"rejected: {snapshot.rejected}",
        f"requeued: {snapshot.requeued}",
        f"inflight: {snapshot.inflight}",
        f"queue_depth: {snapshot.queue_depth}",
        f"inflight_workers: {snapshot.inflight_workers}",
        f"calls_rate: {snapshot.calls_rate:.2f}/s",
        f"avg_precached: {snapshot.avg_precached:.2f}",
        "groups: "
        f"active={active_groups} "
        f"pending={snapshot.groups_pending} "
        f"done={done_groups} "
        f"failed={snapshot.groups_failed}",
        f"dropped_events: {snapshot.dropped_events}",
    ]
    return "\n".join(lines)
