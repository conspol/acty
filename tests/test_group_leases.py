import asyncio

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import GroupTaskKind
from tests.support.asyncio_tools import wait_until_async


class _Executor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None

    def bind(self, controller) -> None:
        self._controller = controller

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            await controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await controller.mark_follower_done(group_id)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


class _BlockingExecutor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None
        self._event = asyncio.Event()

    def bind(self, controller) -> None:
        self._controller = controller

    def release(self) -> None:
        self._event.set()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        await self._event.wait()
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            await controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await controller.mark_follower_done(group_id)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


async def _wait_group_gone(controller, group_id: str, timeout: float = 1.0) -> None:
    async def _missing() -> bool:
        return group_id not in await controller.list_group_ids()

    await wait_until_async(
        _missing,
        timeout=timeout,
        message=f"group {group_id} still present after timeout",
    )


async def _group_present(controller, group_id: str) -> bool:
    return group_id in await controller.list_group_ids()


@pytest.mark.asyncio
async def test_get_group_returns_same_handle() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        handle = engine.open_group("g1", primer_payload={"p": 1})
        assert engine.get_group("g1") is handle
        with pytest.raises(ValueError, match="group already open"):
            engine.open_group("g1", primer_payload={"p": 2})
        with pytest.raises(ValueError, match="unknown group handle"):
            engine.get_group("missing")
        await handle
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_single_lease_auto_closes_group() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g2", primer_payload={"p": 1})
        lease = await group.producer_lease()
        follower = await group.add_follower({"f": 1})
        await asyncio.wait_for(follower, timeout=1.0)
        await lease.release()
        await _wait_group_gone(engine._controller, "g2", timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_multiple_leases_hold_group_open_until_last_release() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g3", primer_payload={"p": 1})
        lease1 = await group.producer_lease()
        lease2 = await group.producer_lease()
        follower = await group.add_follower({"f": 1})
        await asyncio.wait_for(follower, timeout=1.0)

        await lease1.release()
        await asyncio.sleep(0)
        assert "g3" in await engine._controller.list_group_ids()

        await lease2.release()
        await _wait_group_gone(engine._controller, "g3", timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_close_group_with_outstanding_lease_is_safe() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g4", primer_payload={"p": 1})
        lease = await group.producer_lease()
        follower = await group.add_follower({"f": 1})
        await asyncio.wait_for(follower, timeout=1.0)
        await group.close()
        await lease.release()
        await _wait_group_gone(engine._controller, "g4", timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_no_lease_behavior_unchanged() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g5", primer_payload={"p": 1})
        follower = await group.add_follower({"f": 1})
        await asyncio.wait_for(follower, timeout=1.0)

        # Without explicit close, group remains open.
        assert "g5" in await engine._controller.list_group_ids()
        await group.close()
        await _wait_group_gone(engine._controller, "g5", timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_lease_with_admission_pending_group() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
    )
    executor.bind(engine._controller)

    try:
        g1 = await engine.open_group("g6", primer_payload={"p": 1})
        g2 = await engine.open_group("g7", primer_payload={"p": 2})

        lease = await g2.producer_lease()
        await lease.release()

        # g7 should still be pending while g6 is active.
        with pytest.raises(TimeoutError):
            await wait_until_async(
                lambda: _group_present(engine._controller, "g7"),
                timeout=0.05,
            )
        assert "g7" not in await engine._controller.list_group_ids()

        await g1.close()
        executor.release()
        await _wait_group_gone(engine._controller, "g6", timeout=1.0)
        await _wait_group_gone(engine._controller, "g7", timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_lease_drop_pending_cancels_and_allows_reopen() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g_drop", primer_payload={"p": 1})
        lease = await group.producer_lease()
        follower = await group.add_follower({"f": 1})

        await engine.close_all_groups(mode="drop_pending")

        assert group.primer is not None
        assert group.primer.cancelled()
        assert follower.cancelled()
        with pytest.raises(ValueError, match="unknown group handle"):
            engine.get_group("g_drop")

        # Releasing after drop_pending is safe.
        await lease.release()

        # Reopen the same group id while stale work is still pending.
        group2 = await engine.open_group("g_drop", primer_payload={"p": 2})
        await group2.add_follower({"f": 2})
        await group2.close()
        # Release executor so any stale jobs would run now if they weren't canceled.
        executor.release()
        await _wait_group_gone(engine._controller, "g_drop", timeout=1.0)
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lease_with_result_streaming() -> None:
    executor = _Executor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)

    async def _collect_results():
        collected = []
        async for item in engine.results():
            collected.append(item)
        return collected

    collector_task = asyncio.create_task(_collect_results())
    try:
        group = await engine.open_group("g_stream", primer_payload={"p": 1})
        lease = await group.producer_lease()
        follower = await group.add_follower({"f": 1})
        await asyncio.wait_for(follower, timeout=1.0)
        await lease.release()
        await _wait_group_gone(engine._controller, "g_stream", timeout=1.0)
    finally:
        await engine.close()

    results = await asyncio.wait_for(collector_task, timeout=1.0)
    kinds = [result.kind for result in results]
    assert kinds.count(GroupTaskKind.PRIMER.value) == 1
    assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1


@pytest.mark.asyncio
async def test_lease_drop_pending_with_admission_pending_group() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            max_active_groups=1,
            admission_tick_s=0.01,
        ),
    )
    executor.bind(engine._controller)

    try:
        g1 = await engine.open_group("g_drop_a", primer_payload={"p": 1})
        g2 = await engine.open_group("g_drop_b", primer_payload={"p": 2})

        lease = await g2.producer_lease()
        await g2.add_follower({"f": 2})

        with pytest.raises(TimeoutError):
            await wait_until_async(
                lambda: _group_present(engine._controller, "g_drop_b"),
                timeout=0.05,
            )
        assert "g_drop_b" not in await engine._controller.list_group_ids()

        await engine.close_all_groups(mode="drop_pending")

        assert g1.primer is None or g1.primer.cancelled()
        assert g2.primer is None or g2.primer.cancelled()
        with pytest.raises(ValueError, match="unknown group handle"):
            engine.get_group("g_drop_a")
        with pytest.raises(ValueError, match="unknown group handle"):
            engine.get_group("g_drop_b")

        await lease.release()
    finally:
        executor.release()
        await engine.close()


@pytest.mark.asyncio
async def test_lease_close_all_timeout_drops_pending() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
    )
    executor.bind(engine._controller)

    try:
        group = await engine.open_group("g_timeout", primer_payload={"p": 1})
        lease = await group.producer_lease()
        follower = await group.add_follower({"f": 1})

        await engine.close_all_groups(timeout_s=0.01, on_timeout="drop_pending")

        assert group.primer is not None
        assert group.primer.cancelled()
        assert follower.cancelled()
        with pytest.raises(ValueError, match="unknown group handle"):
            engine.get_group("g_timeout")

        await lease.release()
    finally:
        executor.release()
        await engine.close()
