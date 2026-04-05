import pytest

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.core.types import JobId
import acty.engine as engine_module


@pytest.mark.asyncio
async def test_engine_cleanup_on_exception() -> None:
    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
    )
    executor.bind(engine._controller)

    with pytest.raises(RuntimeError, match="Simulated error"):
        async with engine:
            await engine.submit_group("g1", {"p": 1}, [{"f": 1}])
            raise RuntimeError("Simulated error")

    assert engine._closed


@pytest.mark.asyncio
async def test_engine_close_cancels_pending_futures_and_logs(monkeypatch) -> None:
    executor = NoopExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
    )
    executor.bind(engine._controller)

    logged: list[tuple[str, dict]] = []

    def record(event, **kwargs):
        logged.append((event, dict(kwargs)))

    monkeypatch.setattr(engine_module.logger, "info", record)

    pending = engine._awaitable_sink.register(JobId("pending-1"))
    await engine.close()

    assert pending.cancelled()
    assert any(
        event == "Cancelling pending futures during engine close" and payload.get("pending") == 1
        for event, payload in logged
    )
