import asyncio
import json
from dataclasses import dataclass

import pytest

import acty.tui.sources.replay as replay_mod
from acty.tui.sources import FileEventSource, QueueEventSource, ReplayEventSource
from acty_core.events.types import Event
from tests.support.asyncio_tools import wait_until


def _write_events(path, events: list[dict]) -> None:
    payload = "\n".join(json.dumps(evt) for evt in events) + "\n"
    path.write_text(payload, encoding="utf-8")


def _event_dict(seq: int, ts: float) -> dict:
    return {
        "schema_version": 1,
        "run_id": "run",
        "seq": seq,
        "ts": ts,
        "type": "job_queued",
        "job_id": f"j{seq}",
        "group_id": "g1",
        "kind": "primer",
    }


@dataclass
class _ListSource:
    events: list[Event]

    async def stream(self):
        for event in self.events:
            yield event


@pytest.mark.asyncio
async def test_file_event_source_last_n(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    events = [_event_dict(1, 1.0), _event_dict(2, 2.0), _event_dict(3, 3.0)]
    _write_events(path, events)

    source = FileEventSource(path, follow=False, last_n=2)
    seen = [event async for event in source.stream()]
    assert [event.seq for event in seen] == [2, 3]


@pytest.mark.asyncio
async def test_file_event_source_start_from_ts(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    events = [_event_dict(1, 1.0), _event_dict(2, 2.0), _event_dict(3, 3.0)]
    _write_events(path, events)

    source = FileEventSource(path, follow=False, start_from_ts=2.0)
    seen = [event async for event in source.stream()]
    assert [event.seq for event in seen] == [2, 3]


@pytest.mark.asyncio
async def test_file_event_source_follow_skips_existing_when_last_n_zero(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    _write_events(path, [_event_dict(1, 1.0)])
    source = FileEventSource(path, follow=True, poll_interval=0.01, last_n=0)

    async def next_event():
        async for event in source.stream():
            return event
        return None

    task = asyncio.create_task(next_event())
    with pytest.raises(TimeoutError):
        await wait_until(lambda: task.done(), timeout=0.05)

    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_event_dict(2, 2.0)) + "\n")
        handle.flush()

    event = await asyncio.wait_for(task, timeout=0.5)
    assert event is not None
    assert event.seq == 2


@pytest.mark.asyncio
async def test_queue_event_source_stops_on_none() -> None:
    queue: asyncio.Queue[Event | None] = asyncio.Queue()
    queue.put_nowait(Event(1, "run", 1, 1.0, "job_queued"))
    queue.put_nowait(None)
    source = QueueEventSource(queue, stop_on_none=True)

    seen = [event async for event in source.stream()]
    assert len(seen) == 1
    assert seen[0].type == "job_queued"


@pytest.mark.asyncio
async def test_replay_event_source_passthrough() -> None:
    events = [
        Event(1, "run", 1, 1.0, "job_queued"),
        Event(1, "run", 2, 2.0, "job_started"),
    ]
    source = ReplayEventSource(_ListSource(events), speed=0.0)
    seen = [event async for event in source.stream()]
    assert [event.seq for event in seen] == [1, 2]


@pytest.mark.asyncio
async def test_replay_event_source_pause_resume() -> None:
    events = [
        Event(1, "run", 1, 1.0, "job_queued"),
        Event(1, "run", 2, 2.0, "job_started"),
    ]
    source = ReplayEventSource(_ListSource(events), speed=0.0)
    source.set_paused(True)

    async def collect():
        return [event async for event in source.stream()]

    task = asyncio.create_task(collect())
    with pytest.raises(TimeoutError):
        await wait_until(lambda: task.done(), timeout=0.05)

    source.set_paused(False)
    seen = await asyncio.wait_for(task, timeout=0.5)
    assert [event.seq for event in seen] == [1, 2]


@pytest.mark.asyncio
async def test_file_event_source_timestamp_fallback(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    event = _event_dict(1, 1.0)
    event.pop("ts")
    event["timestamp"] = 5.0
    _write_events(path, [event])

    source = FileEventSource(path, follow=False)
    seen = [event async for event in source.stream()]
    assert len(seen) == 1
    assert seen[0].ts == 5.0


@pytest.mark.asyncio
async def test_file_event_source_time_fallback(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    event = _event_dict(1, 1.0)
    event.pop("ts")
    event["time"] = "6.25"
    _write_events(path, [event])

    source = FileEventSource(path, follow=False)
    seen = [event async for event in source.stream()]
    assert len(seen) == 1
    assert seen[0].ts == 6.25


@pytest.mark.asyncio
async def test_replay_event_source_respects_speed(monkeypatch) -> None:
    events = [
        Event(1, "run", 1, 1.0, "job_queued"),
        Event(1, "run", 2, 2.5, "job_started"),
    ]
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(replay_mod.asyncio, "sleep", fake_sleep)
    source = ReplayEventSource(_ListSource(events), speed=1.0)
    seen = [event async for event in source.stream()]

    assert [event.seq for event in seen] == [1, 2]
    assert sleeps == [1.5]


@pytest.mark.asyncio
async def test_replay_event_source_clamps_negative(monkeypatch) -> None:
    events = [
        Event(1, "run", 1, 2.0, "job_queued"),
        Event(1, "run", 2, 1.0, "job_started"),
        Event(1, "run", 3, 3.0, "job_succeeded"),
    ]
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(replay_mod.asyncio, "sleep", fake_sleep)
    source = ReplayEventSource(_ListSource(events), speed=1.0)
    seen = [event async for event in source.stream()]

    assert [event.seq for event in seen] == [1, 2, 3]
    assert sleeps == [1.0]


def test_file_event_source_invalid_args(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    with pytest.raises(ValueError):
        FileEventSource(path, last_n=-1)
    with pytest.raises(ValueError):
        FileEventSource(path, last_n=1, start_from_ts=1.0)
    with pytest.raises(ValueError):
        FileEventSource(path, poll_interval=0)
