from acty.tui.history import MetricsHistory
from acty_core.events.snapshot import Snapshot


def make_snapshot(
    *,
    calls_rate: float = 0.0,
    queue_depth: int = 0,
    inflight_workers: int = 0,
    finished: int = 0,
    failed: int = 0,
    rejected: int = 0,
    runnable_by_kind: dict[str, int] | None = None,
    groups_primer_ready: int = 0,
    groups_primer_inflight: int = 0,
    groups_warm_delay: int = 0,
    groups_followers_ready: int = 0,
    groups_done: int = 0,
    groups_failed: int = 0,
) -> Snapshot:
    return Snapshot(
        queued=0,
        started=0,
        finished=finished,
        failed=failed,
        rejected=rejected,
        requeued=0,
        inflight=0,
        runnable_by_kind=runnable_by_kind or {},
        queue_depth=queue_depth,
        inflight_workers=inflight_workers,
        inflight_by_kind={},
        calls_rate=calls_rate,
        avg_precached=0.0,
        dropped_events=0,
        pool_stats={},
        groups_primer_ready=groups_primer_ready,
        groups_primer_inflight=groups_primer_inflight,
        groups_warm_delay=groups_warm_delay,
        groups_followers_ready=groups_followers_ready,
        groups_done=groups_done,
        groups_failed=groups_failed,
        followers_by_group={},
    )


def test_history_records_points_and_breakdown() -> None:
    history = MetricsHistory()
    history.record(100.0, make_snapshot(calls_rate=1.0, queue_depth=2, runnable_by_kind={"primer": 2}))
    history.record(101.0, make_snapshot(calls_rate=2.0, queue_depth=3, runnable_by_kind={"follower": 3}))

    assert history.timestamps() == [0.0, 1.0]
    assert history.calls_rates() == [1.0, 2.0]
    assert history.queue_depths() == [2, 3]
    breakdown = history.runnable_breakdown()
    assert breakdown["primer"] == [2, 0]
    assert breakdown["follower"] == [0, 3]
