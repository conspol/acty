"""Lane-aware scheduling demo for Acty."""

from __future__ import annotations

import asyncio

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.scheduler import LaneConfig


async def main() -> None:
    config = EngineConfig(
        primer_workers=2,
        follower_workers=2,
        lane_configs={
            "run-a": LaneConfig(weight=2),
            "run-b": LaneConfig(weight=1),
        },
    )
    engine = ActyEngine(executor=NoopExecutor(), config=config)
    try:
        lane_a = engine.lane("run-a")
        lane_b = engine.lane("run-b")
        registry_a = lane_a.registry(group_id_resolver=lambda key: f"{key[0]}:{key[1]}")
        registry_b = lane_b.registry(group_id_resolver=lambda key: f"{key[0]}:{key[1]}")

        handle_a = await registry_a.ensure(("lane-a", "group-1"))
        handle_b = await registry_b.ensure(("lane-b", "group-1"))

        await handle_a.submit_primer({"p": "a"})
        await handle_b.submit_primer({"p": "b"})
    finally:
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
