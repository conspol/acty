# acty

Async grouped-job runtime for Python, AI agents, and LLM systems, with retries, fair scheduling, JSONL event logs, and a live terminal TUI.

Acty is a good fit when one setup step unlocks a family of related async jobs. That can be an AI agent that establishes session state, a coordinator that fans work out to specialist agents, an LLM request with shared context, a cache-backed warmup step, or any workload where several follow-up jobs should stay tied to the same unit of work.

It builds on `acty-core` and adds the high-level engine API, the TUI app, and the `acty-tui` CLI for following or replaying JSONL event streams.

## Why use acty?

- Keep related jobs together instead of manually stitching queues, futures, and lifecycle state.
- Run one `primer` job and then fan out `follower` jobs behind it.
- Retry work without bolting retry logic onto every executor.
- Split worker capacity fairly across runs, tenants, or pipelines with lanes.
- Write JSONL events and inspect long runs live in a terminal TUI.

## Core ideas

- `group`: one logical unit of work
- `primer`: the first job in a group, usually the setup or context-building step
- `follower`: jobs that belong to the same group and usually benefit from the primer
- `lane`: a fairness label that gives a run or tenant its own share of worker capacity

If you have ten noisy runs and one important run, lanes let you give the important run a bigger share instead of letting everything fight in one flat queue. Idle lanes do not have to waste capacity; other lanes can still use spare workers.

## Install

```bash
pip install acty
```

Optional LangChain integration:

```bash
pip install "acty[langchain]"
```

Local development:

```bash
pip install -e .[dev]
```

## Quick Start

This is the smallest useful example. It submits one group with one primer and one follower, then waits for the results.

```python
import asyncio

from acty import ActyEngine, EngineConfig, EchoExecutor


async def main() -> None:
    engine = ActyEngine(executor=EchoExecutor(), config=EngineConfig())
    try:
        submission = await engine.submit_group(
            "demo",
            {"role": "primer", "message": "set things up"},
            [{"role": "follower", "message": "do the next step"}],
        )
        if submission.primer is not None:
            print("primer:", (await submission.primer).output)
        for future in submission.followers:
            print("follower:", (await future).output)
    finally:
        await engine.close()


asyncio.run(main())
```

## Watch A Run Live

Acty works well when you want both a programmatic API and a live view of what the runtime is doing.

Write JSONL events while your code runs:

```python
import asyncio
from pathlib import Path

from acty import ActyEngine, EngineConfig, EchoExecutor
from acty_core.events.sinks.jsonl import JsonlEventSink


async def main() -> None:
    sink = JsonlEventSink(Path("/tmp/acty_events.jsonl"))
    engine = ActyEngine(
        executor=EchoExecutor(),
        config=EngineConfig(),
        event_sinks=[sink],
    )
    try:
        submission = await engine.submit_group("demo-events", {"primer": 1}, [{"follower": 1}])
        if submission.primer is not None:
            await submission.primer
        await asyncio.gather(*submission.followers)
    finally:
        await engine.close()
        sink.close()


asyncio.run(main())
```

Then watch the event stream:

```bash
acty-tui follow /tmp/acty_events.jsonl
```

Or replay a finished run:

```bash
acty-tui replay /tmp/acty_events.jsonl --speed 2.0
```

## Open Groups

Use open groups when different parts of your app need to contribute work to the same group over time.

```python
import asyncio

from acty import ActyEngine, EngineConfig, EchoExecutor


async def main() -> None:
    engine = ActyEngine(executor=EchoExecutor(), config=EngineConfig())
    try:
        async with engine.open_group("g-open") as group:
            await group.submit_primer({"step": "primer"})
            await group.add_follower({"step": "follower-1"})
            await group.add_follower({"step": "follower-2"})
    finally:
        await engine.close()


asyncio.run(main())
```

`open_group(...)` is both awaitable and an async context manager, which makes it useful for multi-producer code paths.

## Lanes

Lanes are how you give different workloads a fair share of the same worker pool.

Typical uses:

- separate traffic by tenant
- protect one evaluation run from another
- keep background batch work from swallowing all follower workers

You configure lane weights on the engine, then submit work through a lane handle:

```python
import asyncio

from acty import ActyEngine, EngineConfig, NoopExecutor
from acty_core.scheduler import LaneConfig


async def main() -> None:
    engine = ActyEngine(
        executor=NoopExecutor(),
        config=EngineConfig(
            primer_workers=2,
            follower_workers=2,
            lane_configs={
                "priority": LaneConfig(weight=2),
                "background": LaneConfig(weight=1),
            },
        ),
    )
    try:
        priority_lane = engine.lane("priority")
        background_lane = engine.lane("background")

        priority_registry = priority_lane.registry(
            group_id_resolver=lambda key: f"{key[0]}:{key[1]}"
        )
        background_registry = background_lane.registry(
            group_id_resolver=lambda key: f"{key[0]}:{key[1]}"
        )

        priority_group = await priority_registry.ensure(("priority", "g1"))
        background_group = await background_registry.ensure(("background", "g1"))

        await priority_group.submit_primer({"payload": "important"})
        await background_group.submit_primer({"payload": "less-important"})
    finally:
        await engine.close()


asyncio.run(main())
```

The important part is the relative weight:

- `weight=2` means "this lane should get about twice the share"
- `weight=1` means "normal share"

That gives you fairness without hard-partitioning the workers into isolated pools.

## Examples

- `python examples/group_tui_demo.py --groups 3 --followers-per-group 2`
- `python examples/lane_demo.py`
- `python examples/tenacity_retry_demo.py --event-jsonl /tmp/acty_tenacity_events.jsonl`

## Package Map

- `acty-core`: low-level scheduler, lifecycle, event, and cache primitives
- `acty`: engine, client, TUI, and CLI
- `acty-langchain`: LangChain payload helpers and runnable executor support
- `acty-openai`: OpenAI executor integration
- `acty-gigachat`: GigaChat executor integration

If you only need the lower-level runtime pieces, use `acty-core`. If you want the higher-level API and the live terminal tooling, use `acty`.

## Development

- tests live under `tests/`
- example programs live under `examples/`
- adapter-specific integration coverage belongs in the adapter packages
