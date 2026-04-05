import pytest

from acty.tui.state import TuiState
from acty.tui.sources import FileEventSource
from acty_core.events import EventBus, JsonlEventSink


@pytest.mark.asyncio
async def test_jsonl_replay_updates_snapshot(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    sink = JsonlEventSink(path)
    bus = EventBus([sink])

    await bus.emit(type="group_activated", group_id="g1")
    await bus.emit(type="job_queued", job_id="j1", group_id="g1", kind="primer")
    await bus.emit(
        type="job_started",
        job_id="j1",
        group_id="g1",
        kind="primer",
        pool="primer",
        payload={"worker": "primer-0"},
    )
    await bus.emit(type="job_succeeded", job_id="j1", group_id="g1", kind="primer")
    await bus.emit(type="group_done", group_id="g1")
    sink.close()

    state = TuiState()
    source = FileEventSource(path, follow=False)
    async for event in source.stream():
        await state.handle(event)

    snapshot = state.snapshot()
    assert snapshot.queued == 1
    assert snapshot.started == 1
    assert snapshot.finished == 1
    assert snapshot.groups_done == 1
