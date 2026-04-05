import asyncio

import pytest

from acty import GroupRegistry
from acty_core.lifecycle import GroupDependency, GroupDependencyPolicy


class _DummyHandle:
    def __init__(self, group_id: str, ready: asyncio.Event | None = None) -> None:
        self.group_id = group_id
        self.closed = False
        self._ready = ready or asyncio.Event()
        if ready is None:
            self._ready.set()

    def __await__(self):
        async def _wait() -> "_DummyHandle":
            await self._ready.wait()
            return self

        return _wait().__await__()

    async def close(self) -> None:
        self.closed = True


class _DummyEngine:
    def __init__(self, ready: asyncio.Event | None = None) -> None:
        self.calls: list[tuple[str, dict | None, str | None, list | None]] = []
        self._ready = ready

    def open_group(self, group_id, *, group_context=None, cache_key=None, depends_on=None):
        self.calls.append((group_id, group_context, cache_key, depends_on))
        return _DummyHandle(group_id, ready=self._ready)


@pytest.mark.asyncio
async def test_group_registry_ensure_reuses_handle_and_validates_dependencies() -> None:
    engine = _DummyEngine()
    registry = GroupRegistry(
        engine,
        group_id_resolver=lambda key: f"{key[0]}:{key[1]}",
        cache_key_resolver=lambda key: f"cache:{key[0]}:{key[1]}",
        group_context_builder=lambda key: {"namespace": key[0], "key": key[1]},
    )
    dep = GroupDependency(group_id="dep-1", policy=GroupDependencyPolicy.ON_DONE)
    handle = await registry.ensure(("trace-1", "core"), depends_on=[dep])
    assert handle.group_id == "trace-1:core"
    assert len(engine.calls) == 1
    assert engine.calls[0] == (
        "trace-1:core",
        {"namespace": "trace-1", "key": "core"},
        "cache:trace-1:core",
        [dep],
    )

    handle_again = await registry.ensure(("trace-1", "core"), depends_on=[dep])
    assert handle_again is handle
    assert len(engine.calls) == 1

    other_dep = GroupDependency(group_id="dep-2", policy=GroupDependencyPolicy.ON_DONE)
    with pytest.raises(ValueError, match="dependencies already set"):
        await registry.ensure(("trace-1", "core"), depends_on=[other_dep])


@pytest.mark.asyncio
async def test_group_registry_concurrent_ensure_single_open() -> None:
    ready = asyncio.Event()
    engine = _DummyEngine(ready=ready)
    registry = GroupRegistry(engine, group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
    dep = GroupDependency(group_id="dep", policy=GroupDependencyPolicy.ON_DONE)

    async def _ensure_and_wait():
        handle = await registry.ensure(("trace", "core"), depends_on=[dep])
        await handle
        return handle

    task_one = asyncio.create_task(_ensure_and_wait())
    task_two = asyncio.create_task(_ensure_and_wait())
    await asyncio.sleep(0)
    assert len(engine.calls) == 1
    ready.set()
    handle_one, handle_two = await asyncio.gather(task_one, task_two)
    assert handle_one is handle_two


@pytest.mark.asyncio
async def test_group_registry_close_namespace_and_all_runs_hook() -> None:
    engine = _DummyEngine()
    closed: list[str] = []
    registry = GroupRegistry(
        engine,
        group_id_resolver=lambda key: f"{key[0]}:{key[1]}",
        on_group_close=closed.append,
    )
    handle_a = await registry.ensure(("ns-a", "one"))
    handle_b = await registry.ensure(("ns-a", "two"))
    handle_c = await registry.ensure(("ns-b", "three"))

    await registry.close_namespace("ns-a")
    assert handle_a.closed is True
    assert handle_b.closed is True
    assert handle_c.closed is False
    assert "ns-a:one" in closed
    assert "ns-a:two" in closed

    await registry.close_all()
    assert handle_c.closed is True
    assert "ns-b:three" in closed


@pytest.mark.asyncio
async def test_group_registry_get_requires_existing_group() -> None:
    engine = _DummyEngine()
    registry = GroupRegistry(engine, group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
    dep = GroupDependency(group_id="dep-1", policy=GroupDependencyPolicy.ON_DONE)

    with pytest.raises(KeyError, match="not open"):
        await registry.get(("trace-1", "core"))
    assert engine.calls == []

    handle = await registry.ensure(("trace-1", "core"), depends_on=[dep])
    assert handle.group_id == "trace-1:core"
    assert len(engine.calls) == 1
    assert engine.calls[0][3] == [dep]

    handle_again = await registry.get(("trace-1", "core"))
    assert handle_again is handle


@pytest.mark.asyncio
async def test_group_registry_ensure_keeps_dependencies_when_reaccessed() -> None:
    engine = _DummyEngine()
    registry = GroupRegistry(engine, group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
    dep = GroupDependency(group_id="dep-keep", policy=GroupDependencyPolicy.ON_DONE)

    handle = await registry.ensure(("trace-1", "core"), depends_on=[dep])
    assert len(engine.calls) == 1

    handle_again = await registry.ensure(("trace-1", "core"), depends_on=None)
    assert handle_again is handle
    assert len(engine.calls) == 1
