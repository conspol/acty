from acty.tui.formatting import format_event, format_log_for_display, format_snapshot
from acty_core.events.snapshot import Snapshot
from acty_core.events.types import Event


def test_format_event_includes_fields() -> None:
    event = Event(
        schema_version=1,
        run_id="run",
        seq=1,
        ts=0.0,
        type="job_started",
        job_id="job-1",
        group_id="group-1",
        follower_id="dep-1",
        pool="pool-a",
        kind="primer",
    )
    line = format_event(event)
    assert "job_started" in line
    assert "job=job-1" in line
    assert "group=group-1" in line
    assert "follower=dep-1" in line
    assert "kind=primer" in line
    assert "pool=pool-a" in line
    assert "\t" in line


def test_format_log_for_display_prefixes_timestamp() -> None:
    line = "job_started job=job-1\t12:34:56.789"
    formatted = format_log_for_display(line)
    assert formatted.startswith("[dim]12:34:56.789[/] ")
    assert "job_started" in formatted


def test_format_event_includes_failure_payload() -> None:
    event = Event(
        schema_version=1,
        run_id="run",
        seq=1,
        ts=0.0,
        type="job_failed",
        job_id="job-1",
        group_id="group-1",
        kind="primer",
        payload={"error": "boom", "attempt": 2},
    )
    line = format_event(event)
    assert "error=boom" in line
    assert "attempt=2" in line


def test_format_snapshot_includes_group_counts() -> None:
    snapshot = Snapshot(
        queued=1,
        started=1,
        finished=0,
        failed=0,
        rejected=0,
        requeued=0,
        inflight=1,
        runnable_by_kind={"primer": 1},
        queue_depth=1,
        inflight_workers=1,
        inflight_by_kind={"primer": 1},
        calls_rate=0.0,
        avg_precached=0.0,
        dropped_events=0,
        pool_stats={},
        groups_primer_ready=1,
        groups_primer_inflight=0,
        groups_warm_delay=0,
        groups_followers_ready=0,
        groups_pending=2,
        groups_done=0,
        groups_failed=0,
        followers_by_group={},
    )
    rendered = format_snapshot(snapshot)
    assert "queued: 1" in rendered
    assert "inflight: 1" in rendered
    assert "groups: active=1" in rendered
    assert "pending=2" in rendered
