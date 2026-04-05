import asyncio
import threading

import pytest

from acty import ActyEngine, EngineConfig, Lane
from acty_core.core.types import Job, JobResult
from acty_core.events.types import Event
from acty_core.scheduler import LaneConfig

from tests.support.asyncio_tools import wait_until


class _DummyHandle:
    def __init__(self, group_id: str) -> None:
        self.group_id = group_id

    def __await__(self):
        async def _wait() -> "_DummyHandle":
            return self

        return _wait().__await__()


class _DummyEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []
        self.lane_updates: list[dict] = []
        self.UNSET = object()

    def open_group(self, group_id, *, group_context=None, **_kwargs):
        self.calls.append((group_id, group_context))
        return _DummyHandle(group_id)

    def update_lane_config(self, **kwargs) -> None:
        self.lane_updates.append(kwargs)


class _DummyEngineNoUnset:
    def __init__(self) -> None:
        self.lane_updates: list[dict] = []

    def update_lane_config(self, **kwargs) -> None:
        self.lane_updates.append(kwargs)


class _EventRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []

    async def handle(self, event: Event) -> None:
        self.events.append(event)


class _BlockingExecutor:
    def __init__(self) -> None:
        self._release = asyncio.Event()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        await self._release.wait()
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )

    def release(self) -> None:
        self._release.set()


@pytest.mark.asyncio
async def test_lane_registry_injects_lane_context() -> None:
    engine = _DummyEngine()
    lane = Lane(engine, "lane-1")
    registry = lane.registry(group_id_resolver=lambda key: f"{key[0]}:{key[1]}")

    handle = await registry.ensure(("ns", "key"))

    assert handle.group_id == "ns:key"
    assert engine.calls == [("ns:key", {"lane": "lane-1"})]


@pytest.mark.asyncio
async def test_lane_registry_merges_group_context() -> None:
    engine = _DummyEngine()
    lane = Lane(engine, "lane-2")
    registry = lane.registry(
        group_id_resolver=lambda key: f"{key[0]}:{key[1]}",
        group_context_builder=lambda key: {"namespace": key[0]},
    )

    await registry.ensure(("ns", "key"))

    group_context = engine.calls[0][1]
    assert group_context == {"namespace": "ns", "lane": "lane-2"}


@pytest.mark.asyncio
async def test_lane_registry_preserves_existing_lane() -> None:
    engine = _DummyEngine()
    lane = Lane(engine, "lane-3")
    registry = lane.registry(
        group_id_resolver=lambda key: f"{key[0]}:{key[1]}",
        group_context_builder=lambda key: {"lane": "custom", "tag": "x"},
    )

    await registry.ensure(("ns", "key"))

    group_context = engine.calls[0][1]
    assert group_context == {"lane": "custom", "tag": "x"}


@pytest.mark.asyncio
async def test_lane_registry_injects_when_lane_is_none() -> None:
    engine = _DummyEngine()
    lane = Lane(engine, "lane-4")
    registry = lane.registry(
        group_id_resolver=lambda key: f"{key[0]}:{key[1]}",
        group_context_builder=lambda key: {"lane": None, "tag": "x"},
    )

    await registry.ensure(("ns", "key"))

    group_context = engine.calls[0][1]
    assert group_context == {"lane": "lane-4", "tag": "x"}


@pytest.mark.asyncio
async def test_engine_lane_configs_flow_to_scheduler() -> None:
    config = EngineConfig(
        primer_workers=1,
        follower_workers=1,
        lane_configs={"lane-a": LaneConfig(weight=2, max_inflight=1)},
        lane_default=LaneConfig(weight=3),
    )
    engine = ActyEngine(executor=_BlockingExecutor(), config=config)
    try:
        scheduler = engine._scheduler
        assert scheduler._lane_configs == config.lane_configs
        assert scheduler._lane_default == config.lane_default
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_lane_deferral_with_engine_configured_lanes() -> None:
    recorder = _EventRecorder()
    executor = _BlockingExecutor()
    config = EngineConfig(
        primer_workers=2,
        follower_workers=1,
        lane_configs={
            "lane-a": LaneConfig(weight=10, max_inflight=1),
            "lane-b": LaneConfig(weight=1),
        },
        stats_emit_interval_s=None,
    )
    engine = ActyEngine(executor=executor, config=config, event_sinks=[recorder])
    try:
        lane_a = engine.lane("lane-a")
        lane_b = engine.lane("lane-b")
        registry_a = lane_a.registry(group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
        registry_b = lane_b.registry(group_id_resolver=lambda key: f"{key[0]}:{key[1]}")

        handle_a1 = await registry_a.ensure(("lane-a", "g1"))
        handle_a2 = await registry_a.ensure(("lane-a", "g2"))
        handle_b1 = await registry_b.ensure(("lane-b", "g1"))
        handle_b2 = await registry_b.ensure(("lane-b", "g2"))

        await handle_a1.submit_primer({"p": 1})
        await handle_a2.submit_primer({"p": 2})
        await handle_b1.submit_primer({"p": 3})
        await handle_b2.submit_primer({"p": 4})

        await wait_until(
            lambda: any(
                event.type == "job_requeued"
                and (event.payload or {}).get("reason") == "lane_balance_deferral"
                for event in recorder.events
            ),
            timeout=1.0,
            message="lane deferral event not observed",
        )

        requeued = next(
            event
            for event in recorder.events
            if event.type == "job_requeued"
            and (event.payload or {}).get("reason") == "lane_balance_deferral"
        )
        payload = requeued.payload or {}
        assert payload.get("lane") == "lane-a"
        assert payload.get("lane_target") == 1
        assert payload.get("lane_inflight") == 1
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_engine_update_lane_configs_forwards_to_scheduler() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))
    try:
        engine.update_lane_configs(
            lane_configs={"lane-a": LaneConfig(weight=2)},
            default_config=LaneConfig(weight=1, max_inflight=2),
        )

        await wait_until(
            lambda: engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=2)
            and engine._scheduler._lane_default == LaneConfig(weight=1, max_inflight=2),
            timeout=1.0,
            message="lane configs not forwarded to scheduler",
        )
    finally:
        executor.release()
        await engine.close()


def test_engine_update_lane_configs_before_start_does_not_break_start() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))

    engine.update_lane_configs(
        lane_configs={"lane-a": LaneConfig(weight=2)},
        default_config=LaneConfig(weight=1),
    )

    async def _run() -> None:
        async with engine:
            await asyncio.sleep(0)

    asyncio.run(_run())


@pytest.mark.asyncio
async def test_lane_update_preserves_other_lane_configs() -> None:
    executor = _BlockingExecutor()
    config = EngineConfig(
        stats_emit_interval_s=None,
        lane_configs={
            "lane-a": LaneConfig(weight=1),
            "lane-b": LaneConfig(weight=2),
        },
    )
    engine = ActyEngine(executor=executor, config=config)
    try:
        lane_a = engine.lane("lane-a")
        lane_a.update(weight=3)

        await wait_until(
            lambda: engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=3),
            timeout=1.0,
            message="lane-a update not applied",
        )

        assert engine._scheduler._lane_configs.get("lane-b") == LaneConfig(weight=2)
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_engine_update_lane_configs_applies_immediately_in_loop() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))
    try:
        async with engine:
            engine.update_lane_configs(lane_configs={"lane-a": LaneConfig(weight=4)})
            assert engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=4)
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lane_registers_default_weight_and_emits_event() -> None:
    recorder = _EventRecorder()
    executor = _BlockingExecutor()
    config = EngineConfig(stats_emit_interval_s=None, lane_default=LaneConfig(weight=2))
    engine = ActyEngine(executor=executor, config=config, event_sinks=[recorder])
    try:
        async with engine:
            await wait_until(
                lambda: engine._scheduler._loop is not None,
                timeout=1.0,
                message="scheduler loop not started",
            )
            engine.lane("lane-default", weight=2)

            await wait_until(
                lambda: engine._scheduler._lane_configs.get("lane-default")
                == LaneConfig(weight=2),
                timeout=1.0,
                message="default lane config not registered",
            )

            await wait_until(
                lambda: any(
                    event.type == "lane_configs_updated"
                    and any(
                        config.get("lane") == "lane-default"
                        for config in (event.payload or {}).get("lane_configs", [])
                    )
                    for event in recorder.events
                ),
                timeout=1.0,
                message="lane_configs_updated not observed for default lane",
            )
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lane_uses_engine_default_when_weight_omitted() -> None:
    recorder = _EventRecorder()
    executor = _BlockingExecutor()
    config = EngineConfig(stats_emit_interval_s=None, lane_default=LaneConfig(weight=0.5))
    engine = ActyEngine(executor=executor, config=config, event_sinks=[recorder])
    try:
        async with engine:
            await wait_until(
                lambda: engine._scheduler._loop is not None,
                timeout=1.0,
                message="scheduler loop not started",
            )
            engine.lane("lane-default")

            await wait_until(
                lambda: engine._scheduler._lane_configs.get("lane-default")
                == LaneConfig(weight=0.5),
                timeout=1.0,
                message="default lane config not registered with engine default weight",
            )
    finally:
        executor.release()
        await engine.close()


def test_lane_registers_non_default_weight_immediately() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))
    try:
        engine.lane("lane-float", weight=2.5)
        assert engine._scheduler._lane_configs.get("lane-float") == LaneConfig(weight=2.5)
    finally:
        executor.release()
        asyncio.run(engine.close())


def test_lane_update_without_unset_uses_explicit_fields() -> None:
    engine = _DummyEngineNoUnset()
    lane = Lane(engine, "lane-no-unset")
    lane.update(weight=2)

    assert engine.lane_updates
    last_update = engine.lane_updates[-1]
    assert last_update["lane_id"] == "lane-no-unset"
    assert last_update["weight"] == 2
    assert "max_inflight" not in last_update


@pytest.mark.asyncio
async def test_engine_update_lane_config_unset_vs_none() -> None:
    executor = _BlockingExecutor()
    config = EngineConfig(
        stats_emit_interval_s=None,
        lane_configs={"lane-a": LaneConfig(weight=1, max_inflight=3)},
    )
    engine = ActyEngine(executor=executor, config=config)
    try:
        engine.update_lane_config(lane_id="lane-a", max_inflight=engine.UNSET)
        assert engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=1, max_inflight=3)

        engine.update_lane_config(lane_id="lane-a", max_inflight=None)
        assert engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=1, max_inflight=None)
    finally:
        executor.release()
        await engine.close()

@pytest.mark.asyncio
async def test_lane_update_rejects_invalid_values() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))
    try:
        lane = engine.lane("lane-bad")
        with pytest.raises(ValueError):
            lane.update(weight=0)
        with pytest.raises(ValueError):
            lane.update(max_inflight=0)
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_engine_update_lane_configs_rejects_invalid_values() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(stats_emit_interval_s=None))
    try:
        with pytest.raises(ValueError):
            engine.update_lane_configs(lane_configs={"lane-a": LaneConfig(weight=0)})
        with pytest.raises(ValueError):
            engine.update_lane_configs(
                lane_configs={"lane-a": LaneConfig(weight=1, max_inflight=0)}
            )
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lane_updates_concurrent_different_lanes() -> None:
    executor = _BlockingExecutor()
    config = EngineConfig(
        stats_emit_interval_s=None,
        lane_configs={
            "lane-a": LaneConfig(weight=1),
            "lane-b": LaneConfig(weight=1),
        },
    )
    engine = ActyEngine(executor=executor, config=config)
    try:
        async with engine:
            await wait_until(
                lambda: engine._scheduler._loop is not None,
                timeout=1.0,
                message="scheduler loop not started",
            )
            lane_a = engine.lane("lane-a")
            lane_b = engine.lane("lane-b")
            start_event = threading.Event()
            done_a = threading.Event()
            done_b = threading.Event()
            errors: list[BaseException] = []

            def _update_a() -> None:
                start_event.wait()
                try:
                    lane_a.update(weight=3)
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    done_a.set()

            def _update_b() -> None:
                start_event.wait()
                try:
                    lane_b.update(weight=4)
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    done_b.set()

            thread_a = threading.Thread(target=_update_a)
            thread_b = threading.Thread(target=_update_b)
            thread_a.start()
            thread_b.start()
            start_event.set()
            await wait_until(
                lambda: done_a.is_set() and done_b.is_set(),
                timeout=2.0,
                message="lane update threads did not complete",
            )
            thread_a.join(timeout=1.0)
            thread_b.join(timeout=1.0)
            assert not thread_a.is_alive()
            assert not thread_b.is_alive()
            assert not errors

            await wait_until(
                lambda: engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=3)
                and engine._scheduler._lane_configs.get("lane-b") == LaneConfig(weight=4),
                timeout=1.0,
                message="concurrent lane updates not applied",
            )
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lane_updates_concurrent_same_lane_last_write_wins() -> None:
    executor = _BlockingExecutor()
    config = EngineConfig(
        stats_emit_interval_s=None,
        lane_configs={
            "lane-a": LaneConfig(weight=1),
            "lane-b": LaneConfig(weight=2),
        },
    )
    engine = ActyEngine(executor=executor, config=config)
    try:
        async with engine:
            await wait_until(
                lambda: engine._scheduler._loop is not None,
                timeout=1.0,
                message="scheduler loop not started",
            )
            lane_first = engine.lane("lane-a")
            lane_second = engine.lane("lane-a")
            start_event = threading.Event()
            first_done = threading.Event()
            done_first = threading.Event()
            done_second = threading.Event()
            errors: list[BaseException] = []

            def _update_first() -> None:
                start_event.wait()
                try:
                    lane_first.update(weight=2)
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    first_done.set()
                    done_first.set()

            def _update_second() -> None:
                start_event.wait()
                first_done.wait(timeout=1.0)
                try:
                    lane_second.update(weight=5)
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    done_second.set()

            thread_first = threading.Thread(target=_update_first)
            thread_second = threading.Thread(target=_update_second)
            thread_first.start()
            thread_second.start()
            start_event.set()
            await wait_until(
                lambda: done_first.is_set() and done_second.is_set(),
                timeout=2.0,
                message="lane update threads did not complete",
            )
            thread_first.join(timeout=1.0)
            thread_second.join(timeout=1.0)
            assert not thread_first.is_alive()
            assert not thread_second.is_alive()
            assert not errors

            await wait_until(
                lambda: engine._scheduler._lane_configs.get("lane-a") == LaneConfig(weight=5),
                timeout=1.0,
                message="lane-a last-write update not applied",
            )
            assert engine._scheduler._lane_configs.get("lane-b") == LaneConfig(weight=2)
    finally:
        executor.release()
        await engine.close()
