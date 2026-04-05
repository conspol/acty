import asyncio
import json

import pytest

import acty.tui.cli as tui_cli
from acty.tui.sources import FileEventSource
from tests.support.asyncio_tools import drain_loop, wait_until


@pytest.mark.asyncio
async def test_file_event_source_replay_parses_events(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    data = {
        "schema_version": 1,
        "run_id": "run",
        "seq": 1,
        "ts": 1.0,
        "type": "job_queued",
        "job_id": "j1",
        "group_id": "g1",
        "kind": "primer",
    }
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")

    source = FileEventSource(path, follow=False)
    events = [event async for event in source.stream()]
    assert len(events) == 1
    assert events[0].type == "job_queued"
    assert events[0].job_id == "j1"
    assert events[0].group_id == "g1"


@pytest.mark.asyncio
async def test_file_event_source_follow_handles_partial_lines(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")
    source = FileEventSource(path, follow=True, poll_interval=0.01, last_n=0)

    async def next_event():
        async for event in source.stream():
            return event
        return None

    task = asyncio.create_task(next_event())
    # Allow the stream task to start tailing the file before appending.
    await drain_loop(2)
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"type": "job_started"')
        handle.flush()

    with pytest.raises(TimeoutError):
        await wait_until(lambda: task.done(), timeout=0.05)

    with path.open("a", encoding="utf-8") as handle:
        handle.write("}\n")
        handle.flush()

    event = await asyncio.wait_for(task, timeout=0.5)
    assert event is not None
    assert event.type == "job_started"


def test_cli_replay_wraps_with_replay_source(tmp_path, monkeypatch) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")

    captured = {}

    class DummyApp:
        async def run_async(self) -> None:
            return None

    def fake_create_tui_app(source, **kwargs):
        captured["source"] = source
        return DummyApp()

    class FakeReplaySource:
        def __init__(self, source, *, speed: float = 1.0) -> None:
            self.source = source
            self.speed = speed

        async def stream(self):
            if False:
                yield None

    monkeypatch.setattr(tui_cli, "create_tui_app", fake_create_tui_app)
    monkeypatch.setattr(tui_cli, "ReplayEventSource", FakeReplaySource)

    result = tui_cli.main(["replay", str(path)])

    assert result == 0
    assert isinstance(captured.get("source"), FakeReplaySource)
    assert captured["source"].speed == 1.0
