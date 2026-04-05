from acty.tui.widgets import DiagnosticWidget, WorkStructureWidget
from acty_core.events.snapshot import PoolStats


def test_diagnostic_widget_runnable_render() -> None:
    widget = DiagnosticWidget()
    widget.update_data(
        runnable_state={
            "queue_depth": 3,
            "inflight": 2,
            "runnable_by_kind": {"primer": 1},
            "inflight_by_kind": {"primer": 1},
        }
    )
    output = widget.render()
    assert "Queue vs Execution" in output
    assert "Queued" in output
    assert "Inflight" in output


def test_diagnostic_widget_bottleneck_render() -> None:
    widget = DiagnosticWidget()
    widget.view = widget.view.BOTTLENECK
    widget.update_data(
        runnable_state={
            "queue_depth": 0,
            "inflight": 0,
            "runnable_by_kind": {},
            "inflight_by_kind": {},
        },
        pool_stats={
            "p1": PoolStats(
                name="p1",
                total_workers=4,
                idle_workers=3,
                working_workers=1,
                stealing_workers=0,
            ),
        },
    )
    output = widget.render()
    assert "Bottleneck" in output
    assert "Idle workers" in output


def test_work_structure_widget_pipeline_render() -> None:
    widget = WorkStructureWidget()
    widget.update_state(
        groups_primer_ready=1,
        groups_primer_inflight=1,
        groups_warm_delay=0,
        groups_followers_ready=2,
        groups_pending=4,
        groups_done=3,
        groups_failed=1,
        runnable_by_kind={"primer": 2, "follower": 5},
        inflight_by_kind={"primer": 1, "follower": 2},
        followers_by_group={"g1": {"done": 2}, "g2": {"done": 1}},
    )
    output = widget.render()
    assert "Groups active" in output
    assert "Groups pending" in output
    assert "Primers queued" in output
    assert "Followers done" in output


def test_work_structure_widget_lane_summary_render() -> None:
    widget = WorkStructureWidget()
    widget.update_state(
        pending_by_lane={"alpha-lane": 2, "beta": 1},
        inflight_by_lane={"alpha-lane": 1},
        lane_targets={"alpha-lane": 2, "beta": 1},
    )
    output = widget.render()
    assert "Lanes" in output
    assert "alpha-lane" in output
    assert "P=2" in output


def test_work_structure_widget_hides_default_lane() -> None:
    widget = WorkStructureWidget()
    widget.update_state(
        pending_by_lane={"default": 1},
        inflight_by_lane={"default": 0},
        lane_targets={"default": 1},
    )
    output = widget.render()
    assert "Lanes" not in output
