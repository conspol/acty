import pytest

from acty.tui.formatting import format_snapshot
from acty.tui.state import TuiState
from acty_core.events.types import Event


@pytest.mark.asyncio
async def test_tui_state_updates_snapshot_and_logs() -> None:
    state = TuiState()
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=1,
            ts=1.0,
            type="group_activated",
            group_id="g1",
        )
    )
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=2,
            ts=2.0,
            type="job_queued",
            job_id="j1",
            group_id="g1",
            kind="primer",
        )
    )
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=3,
            ts=3.0,
            type="job_started",
            job_id="j1",
            group_id="g1",
            kind="primer",
            pool="p1",
            payload={"worker": "p1-0"},
        )
    )

    snapshot = state.snapshot()
    rendered = format_snapshot(snapshot)
    assert "queued: 1" in rendered
    assert snapshot.groups_primer_inflight == 1
    assert snapshot.runnable_by_kind.get("primer") == 0

    lines = state.log_lines()
    assert lines
    assert "job=j1" in lines[-1]


@pytest.mark.asyncio
async def test_tui_state_lane_targets_refresh_on_update() -> None:
    state = TuiState()
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=1,
            ts=1.0,
            type="run_started",
            payload={
                "pool_configs": [{"name": "p", "workers": 4}],
                "lane_configs": [
                    {"lane": "alpha", "weight": 1, "max_inflight": None},
                    {"lane": "beta", "weight": 1, "max_inflight": None},
                ],
                "lane_default": {"weight": 1, "max_inflight": None},
            },
        )
    )
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=2,
            ts=2.0,
            type="job_queued",
            job_id="j-alpha",
            kind="seed",
            payload={"job_context": {"lane": "alpha"}},
        )
    )
    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=3,
            ts=2.0,
            type="job_queued",
            job_id="j-beta",
            kind="seed",
            payload={"job_context": {"lane": "beta"}},
        )
    )

    snapshot = state.snapshot()
    assert snapshot.pending_by_lane == {"alpha": 1, "beta": 1}
    assert snapshot.lane_targets == {"alpha": 2, "beta": 2}

    await state.handle(
        Event(
            schema_version=1,
            run_id="run",
            seq=4,
            ts=3.0,
            type="lane_configs_updated",
            payload={
                "lane_configs": [
                    {"lane": "alpha", "weight": 3, "max_inflight": None},
                    {"lane": "beta", "weight": 1, "max_inflight": None},
                ],
                "default_config": {"weight": 1, "max_inflight": None},
            },
        )
    )

    updated = state.snapshot()
    assert updated.pending_by_lane == {"alpha": 1, "beta": 1}
    assert updated.lane_targets == {"alpha": 3, "beta": 1}
