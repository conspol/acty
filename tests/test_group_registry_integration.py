import asyncio

import pytest

from acty import ActyEngine, EngineConfig, GroupRegistry
from acty_core.core.types import Job, JobResult
from acty_core.events.types import Event
from acty_core.lifecycle import GroupDependency, GroupDependencyPolicy, GroupTaskKind


class EventRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self._cond = asyncio.Condition()

    async def handle(self, event: Event) -> None:
        async with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def count(self, event_type: str) -> int:
        return sum(1 for event in self.events if event.type == event_type)

    async def wait_for(self, event_type: str, count: int, timeout: float = 1.0) -> None:
        async with self._cond:
            await asyncio.wait_for(
                self._cond.wait_for(lambda: self.count(event_type) >= count),
                timeout=timeout,
            )


class _BlockingExecutor:
    def __init__(self) -> None:
        self._primer_events: dict[str, asyncio.Event] = {}
        self._blocked: set[str] = set()
        self.executed: list[tuple[str, str]] = []

    def block_primer(self, group_id: str) -> None:
        self._blocked.add(group_id)
        self._primer_events.setdefault(group_id, asyncio.Event())

    def allow_primer(self, group_id: str) -> None:
        self._blocked.discard(group_id)
        event = self._primer_events.setdefault(group_id, asyncio.Event())
        event.set()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        group_id = str(job.group_id) if job.group_id is not None else ""
        if job.kind == GroupTaskKind.PRIMER.value and job.group_id is not None:
            if group_id in self._blocked:
                event = self._primer_events.setdefault(group_id, asyncio.Event())
                await event.wait()
            self.executed.append((group_id, job.kind))
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_group_registry_dependency_gate_blocks_until_dependency_done() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        registry = GroupRegistry(engine, group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
        dep_key = ("trace", "dep")
        dep_group_id = "trace:dep"
        executor.block_primer(dep_group_id)

        dep_handle = await registry.ensure(dep_key)
        dep_future = await dep_handle.submit_primer({"p": 1})

        dependent_key = ("trace", "dependent")
        dep = GroupDependency(group_id=dep_group_id, policy=GroupDependencyPolicy.ON_DONE)
        dependent_handle = await registry.ensure(dependent_key, depends_on=[dep])
        dependent_future = await dependent_handle.submit_primer({"p": 2})

        await recorder.wait_for("group_blocked", count=1, timeout=1.0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(dependent_future), timeout=0.1)

        executor.allow_primer(dep_group_id)
        await asyncio.wait_for(dep_future, timeout=1.0)
        await dep_handle.close()
        await recorder.wait_for("group_unblocked", count=1, timeout=1.0)
        await asyncio.wait_for(dependent_future, timeout=1.0)
        await dependent_handle.close()

        assert executor.executed == [
            (dep_group_id, GroupTaskKind.PRIMER.value),
            ("trace:dependent", GroupTaskKind.PRIMER.value),
        ]
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_group_registry_dedupes_dependency_events() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        registry = GroupRegistry(engine, group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
        dep_group_id = "trace:dep"
        executor.block_primer(dep_group_id)
        dep_handle = await registry.ensure(("trace", "dep"))
        dep_future = await dep_handle.submit_primer({"p": 1})

        dep = GroupDependency(group_id=dep_group_id, policy=GroupDependencyPolicy.ON_DONE)
        dependent_handle = await registry.ensure(("trace", "dup"), depends_on=[dep, dep])
        await dependent_handle.submit_primer({"p": 2})

        await recorder.wait_for("group_blocked", count=1, timeout=1.0)
        assert recorder.count("group_blocked") == 1

        executor.allow_primer(dep_group_id)
        await asyncio.wait_for(dep_future, timeout=1.0)
        await dep_handle.close()
    finally:
        await engine.close()
